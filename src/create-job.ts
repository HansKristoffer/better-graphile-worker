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
import { assertInteger, assertJsonValue, parseDuration } from './validation.js'
import type {
	CreateJobFn,
	CreateJobsFn,
	EnqueueInTransactionFn,
	JobsApi,
	PreparedJob,
	PrepareJobFn,
	PrepareJobRequestFn,
	PrepareJobsFn
} from './types.js'
import {
	addIdempotentJobsSql,
	addJobSql,
	addJobsSql,
	type IdempotentJobSpec
} from './enqueue-sql.js'
import { assertValidSchemaName } from './schema-name.js'
import {
	enqueueInTransaction,
	type EnqueueInTransactionOptions,
	type RawQueryClient
} from './prisma.js'

export const DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS = 4
const DEFAULT_IDEMPOTENCY_TTL = 30 * 86_400_000
/** A Graphile task spec plus an idempotency key and its TTL in milliseconds. */
type EnqueueSpec = TaskSpec &
	Pick<IdempotentJobSpec, 'idempotencyKey' | 'idempotencyTtl'>
/**
 * `strict` rejects wire inputs JSON would change. `json-compat` stringifies them first, as
 * producers before version 2 did, dropping undefined fields and turning dates into strings.
 */
export type PayloadMode = 'strict' | 'json-compat'

export type BindCreateJobOptions<T extends readonly QueueContract[]> = {
	queues: T
	enqueue: EnqueueAdapter
	hooks?: BetterWorkerHooks
	validateOnEnqueue?: boolean
	defaultMaxAttempts?: number
	otel?: OtelApi | null
	/** Schema used by prepared SQL; defaults to `graphile_worker`. */
	schema?: string
	payloadMode?: PayloadMode
	/** Mode for `enqueueInTransaction` jobs with a `jobKey` but no mode of their own. */
	transactionalJobKeyMode?: JobKeyMode | undefined
}

export type JobKeyMode = NonNullable<JobOptions['jobKeyMode']>
/** The erased shape of a `JobRequest`. */
type JobRequestObject = { task: string; payload?: unknown } & JobOptions
const JOB_KEY_MODES: readonly JobKeyMode[] = [
	'replace',
	'preserve_run_at',
	'unsafe_dedupe'
]

function toJsonCompat(data: unknown): unknown {
	const text = JSON.stringify(data)
	return text === undefined ? undefined : JSON.parse(text)
}

function skippedJob(queue: string): PreparedJob {
	return {
		queue,
		text: 'SELECT NULL::text AS id WHERE false',
		values: [],
		count: 0,
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
	for (const key of ['queueName', 'jobKey', 'idempotencyKey'] as const)
		if (
			derived[key] !== undefined &&
			(typeof derived[key] !== 'string' || !derived[key])
		)
			throw new TypeError(
				`deriveJobOptions for "${queue.name}" returned an empty or non-string ${key}`
			)
	if (
		derived.jobKeyMode !== undefined &&
		!JOB_KEY_MODES.includes(derived.jobKeyMode)
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
		flags: derived.flags,
		idempotencyKey: derived.idempotencyKey,
		idempotencyTtl: derived.idempotencyTtl
	})
}

/** The idempotent path calls add_job per item, so each keyed spec carries the batch mode. */
function withBatchKeyMode(
	specs: readonly IdempotentJobSpec[],
	preserveRunAt: boolean
): IdempotentJobSpec[] {
	return specs.map((spec) =>
		preserveRunAt && spec.jobKey !== undefined
			? { ...spec, jobKeyMode: 'preserve_run_at' }
			: spec
	)
}

export function resolveEnqueueSpec(
	queue: QueueContract,
	explicit: JobOptions | undefined,
	defaultMaxAttempts: number,
	cron = false,
	derived?: DerivedJobOptions
): EnqueueSpec {
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
	const idempotencyKey = options?.idempotencyKey
	if (
		idempotencyKey !== undefined &&
		(typeof idempotencyKey !== 'string' || !idempotencyKey)
	)
		throw new TypeError('idempotencyKey must be a non-empty string')
	// A TTL without a key is validated but has nothing to hold.
	const idempotencyTtl =
		options?.idempotencyTtl === undefined
			? DEFAULT_IDEMPOTENCY_TTL
			: parseDuration(options.idempotencyTtl, 'idempotencyTtl')
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
		jobKeyMode: options?.jobKeyMode ?? cronOptions?.jobKeyMode,
		idempotencyKey,
		idempotencyTtl: idempotencyKey === undefined ? undefined : idempotencyTtl
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
	const payloadMode = options.payloadMode ?? 'strict'
	const { transactionalJobKeyMode } = options
	if (
		transactionalJobKeyMode !== undefined &&
		!JOB_KEY_MODES.includes(transactionalJobKeyMode)
	)
		throw new TypeError(
			'transactionalJobKeyMode must be "replace", "preserve_run_at" or "unsafe_dedupe"'
		)
	if (payloadMode !== 'strict' && payloadMode !== 'json-compat')
		throw new TypeError('payloadMode must be "strict" or "json-compat"')
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
	function enqueueIdempotent(specs: readonly IdempotentJobSpec[]) {
		if (!options.enqueue.addIdempotentJobs)
			throw new TypeError(
				'idempotencyKey requires an enqueue adapter with addIdempotentJobs'
			)
		return options.enqueue.addIdempotentJobs(specs)
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
		const wire = payloadMode === 'json-compat' ? toJsonCompat(data) : data
		assertJsonValue(wire, true)
		// Validate the wire input; transforms are applied again by the consumer.
		const snapshot =
			wire === undefined ? undefined : JSON.parse(JSON.stringify(wire))
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
		cron: boolean,
		defaultJobKeyMode?: JobKeyMode
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
		// The default only fills a mode no call, derived or cron option chose.
		if (defaultJobKeyMode && spec.jobKey !== undefined)
			spec.jobKeyMode ??= defaultJobKeyMode
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
			jobOptions?.jobKey !== undefined ||
			jobOptions?.idempotencyKey !== undefined
		)
			throw new Error(
				'createJobs cannot use jobKey, jobKeyMode or idempotencyKey; derive per-item keys with deriveJobOptions'
			)
		const specs: (AddJobsJobSpec & EnqueueSpec)[] = []
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
		return {
			specs,
			preserveRunAt: keyMode === 'preserve_run_at',
			idempotent: specs.some((spec) => spec.idempotencyKey !== undefined)
		}
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
				const traced = injectTraceContext(payload, api)
				if (spec.idempotencyKey !== undefined) {
					const [job] = await enqueueIdempotent([
						{ identifier: task, payload: traced, ...spec }
					])
					if (!job) throw new Error('Idempotent enqueue returned no job')
					span.setAttributes({
						'job.id': job.id,
						'job.deduplicated': job.deduplicated
					})
					return job.id
				}
				const job = await options.enqueue.addJob(task, traced, spec)
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
			const { specs, preserveRunAt, idempotent } = await resolveMany(
				name,
				data,
				jobOptions
			)
			return producerSpan(`createJobs: ${name}`, async (span) => {
				span.setAttributes({
					'job.queue': name,
					'job.batch': true,
					'job.batch_size': data.length
				})
				if (!specs.length) return []
				const traced = specs.map((spec) => ({
					...spec,
					payload: injectTraceContext(spec.payload, api)
				}))
				let ids: string[]
				if (idempotent) {
					const jobs = await enqueueIdempotent(
						withBatchKeyMode(traced, preserveRunAt)
					)
					ids = jobs.map((job) => job.id)
					span.setAttribute(
						'job.deduplicated_count',
						jobs.filter((job) => job.deduplicated).length
					)
				} else {
					const jobs = await options.enqueue.addJobs(traced, preserveRunAt)
					ids = jobs.map((job) => String(job.id))
				}
				span.setAttribute('job.created_count', ids.length)
				if (ids.length <= 50) span.setAttribute('job.ids', ids.join(','))
				return ids
			})
		})
	}
	async function prepareOne(
		name: string,
		data?: unknown,
		jobOptions?: JobOptions,
		defaultJobKeyMode?: JobKeyMode
	): Promise<PreparedJob> {
		return observed(name, async () => {
			if (hooks.shouldSkipEnqueue?.()) return skippedJob(name)
			const { task, payload, spec } = await resolveOne(
				name,
				data,
				jobOptions,
				false,
				defaultJobKeyMode
			)
			return producerSpan(`prepareJob: ${task}`, async (span) => {
				span.setAttributes({ 'job.queue': task, 'job.batch': false })
				const traced = injectTraceContext(payload, api)
				return {
					queue: task,
					count: 1,
					skipped: false,
					...(spec.idempotencyKey === undefined
						? addJobSql(schema, task, traced, spec)
						: addIdempotentJobsSql(schema, [
								{ identifier: task, payload: traced, ...spec }
							]))
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
			const { specs, preserveRunAt, idempotent } = await resolveMany(
				name,
				data,
				jobOptions
			)
			if (!specs.length) return { ...skippedJob(name), skipped: false }
			return producerSpan(`prepareJobs: ${name}`, async (span) => {
				span.setAttributes({
					'job.queue': name,
					'job.batch': true,
					'job.batch_size': data.length
				})
				const traced = specs.map((spec) => ({
					...spec,
					payload: injectTraceContext(spec.payload, api)
				}))
				return {
					queue: name,
					count: specs.length,
					skipped: false,
					...(idempotent
						? addIdempotentJobsSql(
								schema,
								withBatchKeyMode(traced, preserveRunAt)
							)
						: addJobsSql(schema, traced, preserveRunAt))
				}
			})
		})
	}
	async function enqueueOneInTransaction(
		tx: RawQueryClient,
		job: JobRequestObject,
		transactionOptions?: EnqueueInTransactionOptions
	): Promise<string | null> {
		const { task, payload, ...jobOptions } = job
		const prepared = await prepareOne(
			task,
			payload,
			jobOptions,
			transactionalJobKeyMode
		)
		const [id] = await enqueueInTransaction(tx, prepared, transactionOptions)
		return id ?? null
	}
	/** `prepareJob` takes a queue name and arguments, or one `JobRequest` object. */
	function prepareJob(
		first: string | JobRequestObject,
		data?: unknown,
		jobOptions?: JobOptions
	): Promise<PreparedJob> {
		if (typeof first === 'string') return prepareOne(first, data, jobOptions)
		if (!isPlainObject(first) || typeof first.task !== 'string')
			return Promise.reject(
				new TypeError('prepareJob takes a queue name or a { task } request')
			)
		const { task, payload, ...requestOptions } = first
		return prepareOne(task, payload, requestOptions)
	}
	// All erased calls pass through this registry/validation boundary.
	const createJob = enqueueOne as CreateJobFn<T>
	const createJobs = enqueueMany as CreateJobsFn<T>
	return {
		createJob,
		createJobs,
		prepareJob: prepareJob as PrepareJobRequestFn<T> & PrepareJobFn<T>,
		prepareJobs: prepareMany as PrepareJobsFn<T>,
		enqueueInTransaction:
			enqueueOneInTransaction as unknown as EnqueueInTransactionFn<T>,
		enqueueOne,
		enqueueMany,
		prepareOne,
		prepareMany,
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
