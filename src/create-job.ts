import { assertUniqueQueueNames } from './define-queues.js'
import { compact } from './options.js'
import type { AddJobsJobSpec, TaskSpec } from 'graphile-worker'
import {
	DEFAULT_GRAPHILE_WORKER_SCHEMA,
	type EnqueueAdapter
} from './client.js'
import {
	CRON_INIT_SUFFIX,
	getQueueType,
	resolveSerialQueueName,
	type DerivedJobOptions,
	type QueueContract
} from './queue.js'
import { JobValidationError, UnknownQueueError } from './errors.js'
import type { BetterWorkerHooks, JobSpan } from './hooks.js'
import type { BatchJobOptions, JobOptions } from './job-options.js'
import { injectTraceContext, isPlainObject } from './payload.js'
import {
	getOtel,
	otelStatusCodes,
	withActiveSpan,
	type OtelApi
} from './otel.js'
import { observe } from './observers.js'
import { assertInteger, assertJsonValue } from './validation.js'
import type {
	CreateJobFn,
	CreateJobsFn,
	JobsApi,
	PreparedJob,
	PrepareJobFn,
	PrepareJobsFn
} from './types.js'
import { addJobSql, addJobsSql } from './enqueue-sql.js'
import { assertValidSchemaName } from './schema-name.js'

export const DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS = 4
export type BindCreateJobOptions<T extends readonly QueueContract[]> = {
	queues: T
	enqueue: EnqueueAdapter
	hooks?: BetterWorkerHooks
	validateOnEnqueue?: boolean
	defaultMaxAttempts?: number
	otel?: OtelApi | null
	/** Schema used by prepared SQL; defaults to `graphile_worker`. */
	schema?: string
}

function skippedJob(queue: string): PreparedJob {
	return {
		queue,
		text: 'SELECT NULL::text AS id WHERE false',
		values: [],
		skipped: true
	}
}

export async function parseQueuePayload(
	queue: QueueContract,
	data: unknown,
	index?: number
): Promise<unknown> {
	if (!queue.inputSchema) return undefined
	const result = await queue.inputSchema.safeParseAsync(data)
	if (!result.success)
		throw new JobValidationError(
			queue.name,
			result.error.issues.map((issue) => ({
				path: index === undefined ? issue.path : [index, ...issue.path],
				message: issue.message
			}))
		)
	return result.data
}

function deriveOptions(
	queue: QueueContract,
	input: unknown
): DerivedJobOptions | undefined {
	const derived = queue.deriveJobOptions?.(input as never)
	if (derived === undefined) return undefined
	if (!isPlainObject(derived))
		throw new TypeError(
			`deriveJobOptions for "${queue.name}" must return a plain object synchronously`
		)
	for (const key of ['queueName', 'jobKey'] as const)
		if (
			derived[key] !== undefined &&
			(typeof derived[key] !== 'string' || !derived[key])
		)
			throw new TypeError(
				`deriveJobOptions for "${queue.name}" returned an empty or non-string ${key}`
			)
	if (
		derived.jobKeyMode !== undefined &&
		!['replace', 'preserve_run_at', 'unsafe_dedupe'].includes(
			derived.jobKeyMode
		)
	)
		throw new TypeError(
			`deriveJobOptions for "${queue.name}" returned an invalid jobKeyMode`
		)
	if (
		derived.flags !== undefined &&
		(!Array.isArray(derived.flags) ||
			derived.flags.some((flag) => typeof flag !== 'string'))
	)
		throw new TypeError(
			`deriveJobOptions for "${queue.name}" returned invalid flags`
		)
	return compact({
		queueName: derived.queueName,
		jobKey: derived.jobKey,
		jobKeyMode: derived.jobKeyMode,
		priority: derived.priority,
		flags: derived.flags
	})
}

export function resolveEnqueueSpec(
	queue: QueueContract,
	explicit: JobOptions | undefined,
	defaultMaxAttempts: number,
	cron = false,
	derived?: DerivedJobOptions
): TaskSpec {
	// Precedence: explicit call options, derived options, then queue defaults.
	const options: JobOptions | undefined = derived
		? { ...derived, ...compact(explicit ?? {}) }
		: explicit
	const cronOptions = cron ? queue.cronOptions : undefined
	const maxAttempts =
		options?.maxAttempts ??
		cronOptions?.maxAttempts ??
		queue.maxAttempts ??
		defaultMaxAttempts
	const priority = options?.priority ?? cronOptions?.priority ?? queue.priority
	assertInteger(maxAttempts, 'maxAttempts', 1, 32767)
	if (priority !== undefined) assertInteger(priority, 'priority', -32768, 32767)
	if (options?.runAt && !Number.isFinite(new Date(options.runAt).getTime()))
		throw new RangeError('runAt must be a valid timestamp')
	return compact({
		maxAttempts,
		priority,
		flags: options?.flags
			? [...options.flags]
			: queue.flags
				? [...queue.flags]
				: undefined,
		queueName:
			options?.queueName ??
			cronOptions?.queueName ??
			resolveSerialQueueName(queue.serial, queue.name),
		runAt: options?.runAt,
		jobKey: options?.jobKey ?? cronOptions?.jobKey,
		jobKeyMode: options?.jobKeyMode ?? cronOptions?.jobKeyMode
	})
}

export function bindCreateJob<const T extends readonly QueueContract[]>(
	options: BindCreateJobOptions<T>
) {
	assertUniqueQueueNames(options.queues)
	const hooks = options.hooks ?? {}
	const api = options.otel === undefined ? getOtel() : options.otel
	const defaultMaxAttempts =
		options.defaultMaxAttempts ?? DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS
	assertInteger(defaultMaxAttempts, 'defaultMaxAttempts', 1, 32767)
	const schema = assertValidSchemaName(
		options.schema ?? DEFAULT_GRAPHILE_WORKER_SCHEMA
	)
	const registry = new Map(options.queues.map((queue) => [queue.name, queue]))
	for (const queue of registry.values()) {
		if (!queue.inputSchema && queue.cron === undefined)
			throw new TypeError(
				`Queue "${queue.name}" requires an inputSchema or cron schedule`
			)
		resolveEnqueueSpec(queue, undefined, defaultMaxAttempts)
		if (queue.cron !== undefined) {
			resolveEnqueueSpec(queue, undefined, defaultMaxAttempts, true)
			if (queue.cronOptions?.backfillPeriod !== undefined)
				assertInteger(queue.cronOptions.backfillPeriod, 'backfillPeriod', 0)
		}
	}
	function find(name: string) {
		const queue = registry.get(name)
		if (!queue) throw new UnknownQueueError(name, [...registry.keys()])
		return queue
	}
	async function prepare(
		queue: QueueContract,
		data: unknown,
		jobOptions?: JobOptions,
		index?: number
	) {
		assertJsonValue(data, true)
		// Validate the wire input; transforms are applied again by the consumer.
		const snapshot =
			data === undefined ? undefined : JSON.parse(JSON.stringify(data))
		if (jobOptions?.validateOnEnqueue ?? options.validateOnEnqueue ?? true)
			await parseQueuePayload(queue, snapshot, index)
		return snapshot
	}
	async function observed<R>(
		name: string,
		operation: () => Promise<R>
	): Promise<R> {
		try {
			return await operation()
		} catch (error) {
			await observe(hooks.onEnqueueFail, { queue: name, error })
			throw error
		}
	}
	async function producerSpan<R>(
		name: string,
		fn: (span: JobSpan) => Promise<R>
	): Promise<R> {
		return withActiveSpan(
			'worker',
			name,
			{ kind: 'producer' },
			async (span) => {
				const codes = otelStatusCodes(api)
				try {
					const result = await fn(span)
					span.setStatus({ code: codes.OK })
					return result
				} catch (error) {
					const message = error instanceof Error ? error.message : String(error)
					span.setStatus({ code: codes.ERROR, message })
					span.recordException(
						error instanceof Error ? error : new Error(message)
					)
					throw error
				}
			},
			api
		)
	}
	async function resolveOne(
		name: string,
		data: unknown,
		jobOptions: JobOptions | undefined,
		cron: boolean
	) {
		const queue = find(name)
		if (cron && queue.cron === undefined)
			throw new Error(`"${name}" is not a cron queue`)
		const type = getQueueType(queue)
		const task =
			cron && type === 'cron-init' ? `${name}${CRON_INIT_SUFFIX}` : name
		const noInput = cron || type === 'cron'
		const payload = noInput ? undefined : await prepare(queue, data, jobOptions)
		const spec = resolveEnqueueSpec(
			queue,
			jobOptions,
			defaultMaxAttempts,
			noInput,
			noInput ? undefined : deriveOptions(queue, payload)
		)
		return { task, payload, spec }
	}
	async function resolveMany(
		name: string,
		data: readonly unknown[],
		jobOptions: BatchJobOptions | undefined
	) {
		const queue = find(name)
		if (!queue.inputSchema)
			throw new Error(
				`createJobs cannot enqueue cron queue "${name}"; use createJob or triggerCron`
			)
		if (
			jobOptions?.jobKeyMode !== undefined ||
			jobOptions?.jobKey !== undefined
		)
			throw new Error(
				'createJobs cannot use jobKey or jobKeyMode; derive per-item keys with deriveJobOptions'
			)
		const specs: AddJobsJobSpec[] = []
		resolveEnqueueSpec(queue, jobOptions, defaultMaxAttempts)
		const keys = new Map<string, number>()
		let keyMode: string | undefined
		for (const [index, item] of data.entries()) {
			const payload = await prepare(queue, item, jobOptions, index)
			const { jobKeyMode, ...spec } = resolveEnqueueSpec(
				queue,
				jobOptions,
				defaultMaxAttempts,
				false,
				deriveOptions(queue, payload)
			)
			if (spec.jobKey !== undefined) {
				// add_jobs has one preserve_run_at flag per call and no unsafe_dedupe.
				const mode = jobKeyMode ?? 'replace'
				if (mode === 'unsafe_dedupe' || (keyMode && keyMode !== mode))
					throw new Error(
						`createJobs item ${index} uses jobKeyMode "${mode}"; a batch needs one of "replace" or "preserve_run_at" for every keyed item`
					)
				keyMode = mode
				const first = keys.get(spec.jobKey)
				if (first !== undefined)
					throw new Error(
						`createJobs items ${first} and ${index} share jobKey "${spec.jobKey}"`
					)
				keys.set(spec.jobKey, index)
			}
			specs.push({ identifier: name, ...spec, payload })
		}
		return { specs, preserveRunAt: keyMode === 'preserve_run_at' }
	}
	async function enqueueOne(
		name: string,
		data?: unknown,
		jobOptions?: JobOptions,
		cron = false
	): Promise<string | null> {
		return observed(name, async () => {
			if (hooks.shouldSkipEnqueue?.()) return null
			const { task, payload, spec } = await resolveOne(
				name,
				data,
				jobOptions,
				cron
			)
			return producerSpan(`createJob: ${task}`, async (span) => {
				span.setAttributes({
					'job.queue': task,
					'job.batch': false,
					'job.max_attempts': spec.maxAttempts ?? defaultMaxAttempts
				})
				const job = await options.enqueue.addJob(
					task,
					injectTraceContext(payload, api),
					spec
				)
				span.setAttribute('job.id', String(job.id))
				return String(job.id)
			})
		})
	}
	async function enqueueMany(
		name: string,
		data: readonly unknown[],
		jobOptions?: BatchJobOptions
	): Promise<string[]> {
		return observed(name, async () => {
			if (hooks.shouldSkipEnqueue?.()) return []
			const { specs, preserveRunAt } = await resolveMany(name, data, jobOptions)
			return producerSpan(`createJobs: ${name}`, async (span) => {
				span.setAttributes({
					'job.queue': name,
					'job.batch': true,
					'job.batch_size': data.length
				})
				if (!specs.length) return []
				const jobs = await options.enqueue.addJobs(
					specs.map((spec) => ({
						...spec,
						payload: injectTraceContext(spec.payload, api)
					})),
					preserveRunAt
				)
				const ids = jobs.map((job) => String(job.id))
				span.setAttribute('job.created_count', ids.length)
				if (ids.length <= 50) span.setAttribute('job.ids', ids.join(','))
				return ids
			})
		})
	}
	async function prepareOne(
		name: string,
		data?: unknown,
		jobOptions?: JobOptions
	): Promise<PreparedJob> {
		return observed(name, async () => {
			if (hooks.shouldSkipEnqueue?.()) return skippedJob(name)
			const { task, payload, spec } = await resolveOne(
				name,
				data,
				jobOptions,
				false
			)
			return producerSpan(`prepareJob: ${task}`, async (span) => {
				span.setAttributes({ 'job.queue': task, 'job.batch': false })
				return {
					queue: task,
					skipped: false,
					...addJobSql(schema, task, injectTraceContext(payload, api), spec)
				}
			})
		})
	}
	async function prepareMany(
		name: string,
		data: readonly unknown[],
		jobOptions?: BatchJobOptions
	): Promise<PreparedJob> {
		return observed(name, async () => {
			if (hooks.shouldSkipEnqueue?.()) return skippedJob(name)
			const { specs, preserveRunAt } = await resolveMany(name, data, jobOptions)
			if (!specs.length) return { ...skippedJob(name), skipped: false }
			return producerSpan(`prepareJobs: ${name}`, async (span) => {
				span.setAttributes({
					'job.queue': name,
					'job.batch': true,
					'job.batch_size': data.length
				})
				return {
					queue: name,
					skipped: false,
					...addJobsSql(
						schema,
						specs.map((spec) => ({
							...spec,
							payload: injectTraceContext(spec.payload, api)
						})),
						preserveRunAt
					)
				}
			})
		})
	}
	// All erased calls pass through this registry/validation boundary.
	const createJob = enqueueOne as CreateJobFn<T>
	const createJobs = enqueueMany as CreateJobsFn<T>
	return {
		createJob,
		createJobs,
		prepareJob: prepareOne as PrepareJobFn<T>,
		prepareJobs: prepareMany as PrepareJobsFn<T>,
		enqueueOne,
		enqueueMany,
		triggerCron: (name: string, opts?: JobOptions) =>
			enqueueOne(name, undefined, opts, true)
	}
}

export function createJobsApi<T extends readonly QueueContract[]>(
	createJob: CreateJobFn<T>,
	queues: T
): JobsApi<T> {
	const jobs: Record<
		string,
		(data?: unknown, options?: JobOptions) => Promise<string | null>
	> = Object.create(null)
	const enqueue = createJob as (
		name: string,
		data?: unknown,
		opts?: JobOptions
	) => Promise<string | null>
	for (const { name } of queues)
		jobs[name] = (data, opts) => enqueue(name, data, opts)
	return Object.freeze(jobs) as JobsApi<T>
}
