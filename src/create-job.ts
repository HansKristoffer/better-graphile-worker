import type {
	AddJobsJobSpec,
	Job,
	TaskSpec,
	WorkerUtils
} from 'graphile-worker'
import {
	hasInputSchema,
	isCronQueue,
	resolveSerialQueueName,
	type QueueAny
} from './create-queue'
import { JobValidationError, UnknownQueueError } from './errors'
import type { BetterWorkerHooks, JobSpan } from './hooks'
import type { JobOptions } from './job-options'
import { injectTraceContext } from './payload'
import { otelStatusCodes, withActiveSpan } from './otel'
import type { CreateJobFn, CreateJobsFn } from './types'

/** Applied when `createJob` is called without `maxAttempts` (Graphile's own default is 25). */
export const DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS = 4

const MAX_JOB_IDS_SPAN_ATTRIBUTE_COUNT = 50

export {
	TRACE_CONTEXT_KEY,
	TRACEPARENT_KEY,
	BGW_ENVELOPE_KEY,
	type JobTraceContext,
	injectTraceContext
} from './payload'

type AddJobFn = (
	identifier: string,
	payload: unknown,
	spec?: TaskSpec
) => Promise<Job>

export type BindCreateJobOptions<TQueues extends readonly QueueAny[]> = {
	getWorkerUtils: () => Promise<WorkerUtils>
	queues: TQueues
	hooks?: BetterWorkerHooks
	validateOnEnqueue?: boolean
	defaultMaxAttempts?: number
}

function assertBatchJobKeyMode(options: JobOptions | undefined): void {
	if (options?.jobKeyMode !== undefined) {
		throw new Error(
			'createJobs cannot use jobKeyMode; Graphile addJobs does not support it'
		)
	}
}

function setJobOptionSpanAttributes(
	span: JobSpan,
	options: JobOptions | undefined,
	resolvedMaxAttempts: number
): void {
	if (options?.jobKey) {
		span.setAttribute('job.key', options.jobKey)
	}
	if (options?.priority !== undefined) {
		span.setAttribute('job.priority', options.priority)
	}
	if (options?.runAt) {
		span.setAttribute(
			'job.run_at',
			options.runAt instanceof Date
				? options.runAt.toISOString()
				: String(options.runAt)
		)
	}
	span.setAttribute('job.max_attempts', resolvedMaxAttempts)
}

function findQueue(queues: readonly QueueAny[], queueName: string): QueueAny {
	const queue = queues.find((item) => item.name === queueName)
	if (!queue) {
		throw new UnknownQueueError(
			queueName,
			queues.map((item) => item.name)
		)
	}
	return queue
}

function resolveEnqueueSpec(
	queue: QueueAny,
	options: JobOptions | undefined,
	defaultMaxAttempts: number
): {
	maxAttempts: number
	priority: number | undefined
	flags: string[] | undefined
	queueName: string | undefined
} {
	return {
		maxAttempts:
			options?.maxAttempts ?? queue.maxAttempts ?? defaultMaxAttempts,
		priority: options?.priority ?? queue.priority,
		flags: options?.flags ?? queue.flags,
		queueName:
			options?.queueName ?? resolveSerialQueueName(queue.serial, queue.name)
	}
}

function validatePayload(
	queue: QueueAny,
	queueName: string,
	data: unknown,
	shouldValidate: boolean
): unknown {
	if (!shouldValidate) return data
	if (isCronQueue(queue)) return data
	if (!hasInputSchema(queue)) return data

	const result = queue.inputSchema.safeParse(data)
	if (!result.success) {
		throw new JobValidationError(
			queueName,
			result.error.issues.map((issue) => ({
				path: issue.path,
				message: issue.message
			}))
		)
	}
	return data
}

function toTaskSpec(
	resolved: ReturnType<typeof resolveEnqueueSpec>,
	options: JobOptions | undefined
): TaskSpec {
	return {
		maxAttempts: resolved.maxAttempts,
		priority: resolved.priority,
		flags: resolved.flags,
		queueName: resolved.queueName,
		runAt: options?.runAt,
		jobKey: options?.jobKey,
		jobKeyMode: options?.jobKeyMode
	}
}

async function enqueueOne(
	addJob: AddJobFn,
	queueName: string,
	payload: unknown,
	spec: TaskSpec
): Promise<string> {
	const job = await addJob(queueName, injectTraceContext(payload), spec)
	return String(job.id)
}

export function bindCreateJob<TQueues extends readonly QueueAny[]>(
	options: BindCreateJobOptions<TQueues>
): {
	createJob: CreateJobFn<TQueues>
	createJobs: CreateJobsFn<TQueues>
} {
	const hooks = options.hooks ?? {}
	const validateOnEnqueue = options.validateOnEnqueue ?? true
	const defaultMaxAttempts =
		options.defaultMaxAttempts ?? DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS

	async function createJob(
		queueName: string,
		data?: unknown,
		jobOptions?: JobOptions
	): Promise<string | null> {
		if (hooks.shouldSkipEnqueue?.()) {
			return null
		}

		const queue = findQueue(options.queues, queueName)
		const payload = validatePayload(
			queue,
			queueName,
			data,
			jobOptions?.validateOnEnqueue ?? validateOnEnqueue
		)
		const resolved = resolveEnqueueSpec(queue, jobOptions, defaultMaxAttempts)
		const statusCodes = otelStatusCodes()

		return withActiveSpan(
			'worker',
			`createJob: ${queueName}`,
			{ kind: 'producer' },
			async (span) => {
				try {
					span.setAttribute('job.queue', queueName)
					span.setAttribute('job.batch', false)
					setJobOptionSpanAttributes(span, jobOptions, resolved.maxAttempts)

					const workerUtils = await options.getWorkerUtils()
					const addJob = workerUtils.addJob as AddJobFn
					const result = await enqueueOne(
						addJob,
						queueName,
						isCronQueue(queue) ? {} : payload,
						toTaskSpec(resolved, jobOptions)
					)
					span.setAttribute('job.id', result)
					span.setStatus({ code: statusCodes.OK })
					return result
				} catch (error) {
					const errorMessage =
						error instanceof Error ? error.message : String(error)
					hooks.onEnqueueFail?.({ queue: queueName, error })
					span.setStatus({
						code: statusCodes.ERROR,
						message: errorMessage
					})
					span.recordException(
						error instanceof Error ? error : new Error(errorMessage)
					)
					throw error
				}
			}
		)
	}

	async function createJobs(
		queueName: string,
		data: unknown[],
		jobOptions?: JobOptions
	): Promise<string[]> {
		if (hooks.shouldSkipEnqueue?.()) {
			return []
		}

		const queue = findQueue(options.queues, queueName)
		if (isCronQueue(queue)) {
			throw new Error(
				`createJobs cannot enqueue cron queue "${queueName}"; use createJob or triggerCron`
			)
		}

		assertBatchJobKeyMode(jobOptions)

		const shouldValidate = jobOptions?.validateOnEnqueue ?? validateOnEnqueue
		const payloads = data.map((item) =>
			validatePayload(queue, queueName, item, shouldValidate)
		)
		const resolved = resolveEnqueueSpec(queue, jobOptions, defaultMaxAttempts)
		const statusCodes = otelStatusCodes()

		return withActiveSpan(
			'worker',
			`createJobs: ${queueName}`,
			{ kind: 'producer' },
			async (span) => {
				try {
					span.setAttribute('job.queue', queueName)
					span.setAttribute('job.batch', true)
					span.setAttribute('job.batch_size', payloads.length)
					setJobOptionSpanAttributes(span, jobOptions, resolved.maxAttempts)

					if (payloads.length === 0) {
						span.setAttribute('job.created_count', 0)
						span.setStatus({ code: statusCodes.OK })
						return []
					}

					const workerUtils = await options.getWorkerUtils()
					const specs: AddJobsJobSpec[] = payloads.map((item) => ({
						identifier: queueName,
						payload: injectTraceContext(item),
						maxAttempts: resolved.maxAttempts,
						priority: resolved.priority,
						flags: resolved.flags,
						queueName: resolved.queueName,
						runAt: jobOptions?.runAt,
						jobKey: jobOptions?.jobKey
					}))
					const jobs = await workerUtils.addJobs(specs)
					const result = jobs.map((job) => String(job.id))

					span.setAttribute('job.created_count', result.length)
					if (result.length <= MAX_JOB_IDS_SPAN_ATTRIBUTE_COUNT) {
						span.setAttribute('job.ids', result.join(','))
					}
					span.setStatus({ code: statusCodes.OK })
					return result
				} catch (error) {
					const errorMessage =
						error instanceof Error ? error.message : String(error)
					hooks.onEnqueueFail?.({ queue: queueName, error })
					span.setStatus({
						code: statusCodes.ERROR,
						message: errorMessage
					})
					span.recordException(
						error instanceof Error ? error : new Error(errorMessage)
					)
					throw error
				}
			}
		)
	}

	return {
		createJob: createJob as CreateJobFn<TQueues>,
		createJobs: createJobs as CreateJobsFn<TQueues>
	}
}

export function createJobsApi<TQueues extends readonly QueueAny[]>(
	createJob: CreateJobFn<TQueues>
): import('./types').JobsApi<TQueues> {
	return new Proxy({} as import('./types').JobsApi<TQueues>, {
		get(_target, prop) {
			if (typeof prop !== 'string') return undefined
			return (data?: unknown, options?: JobOptions) =>
				createJob(prop as never, data as never, options)
		}
	})
}
