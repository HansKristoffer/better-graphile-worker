import { assertUniqueQueueNames } from './define-queues.js'
import { compact } from './options.js'
import type { AddJobsJobSpec, TaskSpec } from 'graphile-worker'
import type { EnqueueAdapter } from './client.js'
import {
	CRON_INIT_SUFFIX,
	getQueueType,
	resolveSerialQueueName,
	type QueueContract
} from './queue.js'
import { JobValidationError, UnknownQueueError } from './errors.js'
import type { BetterWorkerHooks, JobSpan } from './hooks.js'
import type { BatchJobOptions, JobOptions } from './job-options.js'
import { injectTraceContext } from './payload.js'
import {
	getOtel,
	otelStatusCodes,
	withActiveSpan,
	type OtelApi
} from './otel.js'
import { observe } from './observers.js'
import { assertInteger, assertJsonValue } from './validation.js'
import type { CreateJobFn, CreateJobsFn, JobsApi } from './types.js'

export const DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS = 4
export type BindCreateJobOptions<T extends readonly QueueContract[]> = {
	queues: T
	enqueue: EnqueueAdapter
	hooks?: BetterWorkerHooks
	validateOnEnqueue?: boolean
	defaultMaxAttempts?: number
	otel?: OtelApi | null
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

export function resolveEnqueueSpec(
	queue: QueueContract,
	options: JobOptions | undefined,
	defaultMaxAttempts: number,
	cron = false
): TaskSpec {
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
	async function enqueueOne(
		name: string,
		data?: unknown,
		jobOptions?: JobOptions,
		cron = false
	): Promise<string | null> {
		return observed(name, async () => {
			if (hooks.shouldSkipEnqueue?.()) return null
			const queue = find(name)
			if (cron && queue.cron === undefined)
				throw new Error(`"${name}" is not a cron queue`)
			const type = getQueueType(queue)
			const task =
				cron && type === 'cron-init' ? `${name}${CRON_INIT_SUFFIX}` : name
			const payload =
				cron || type === 'cron'
					? undefined
					: await prepare(queue, data, jobOptions)
			const spec = resolveEnqueueSpec(
				queue,
				jobOptions,
				defaultMaxAttempts,
				cron || type === 'cron'
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
					'createJobs cannot use jobKey or jobKeyMode; batch items must have independent keys'
				)
			const spec = resolveEnqueueSpec(queue, jobOptions, defaultMaxAttempts)
			return producerSpan(`createJobs: ${name}`, async (span) => {
				span.setAttributes({
					'job.queue': name,
					'job.batch': true,
					'job.batch_size': data.length
				})
				const { jobKeyMode: _mode, jobKey: _key, ...batchSpec } = spec
				const specs: AddJobsJobSpec[] = []
				for (const [index, item] of data.entries())
					specs.push({
						identifier: name,
						...batchSpec,
						payload: injectTraceContext(
							await prepare(queue, item, jobOptions, index),
							api
						)
					})
				if (!specs.length) return []
				const jobs = await options.enqueue.addJobs(specs)
				const ids = jobs.map((job) => String(job.id))
				span.setAttribute('job.created_count', ids.length)
				if (ids.length <= 50) span.setAttribute('job.ids', ids.join(','))
				return ids
			})
		})
	}
	// All erased calls pass through this registry/validation boundary.
	const createJob = enqueueOne as CreateJobFn<T>
	const createJobs = enqueueMany as CreateJobsFn<T>
	return {
		createJob,
		createJobs,
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
