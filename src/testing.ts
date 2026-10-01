import type { JobHelpers } from 'graphile-worker'
import {
	type JobContext,
	type QueueContract,
	type HandlerFreeQueueContract,
	type InlineQueues,
	type RunnableQueue,
	type QueueHandlers,
	CRON_INIT_SUFFIX
} from './queue.js'
import type { JobLogger, LogAttributes } from './hooks.js'
import { createNoopSpan } from './otel.js'
import type {
	QueueInput,
	QueueName,
	CronInitQueueName,
	InferInput
} from './types.js'
import {
	createStepRunner,
	createMemoryStepStore,
	type StepStore
} from './steps.js'
import { extractProducerLink } from './payload.js'
import { bindCreateJob, parseQueuePayload } from './create-job.js'
import { normalizeWorkerQueues, type NormalizedQueue } from './registry.js'
import type { JobOptions } from './job-options.js'

export type CapturedLog = {
	level: keyof JobLogger
	message: string
	attributes?: LogAttributes | undefined
}

export type CapturedEnqueue<
	T extends readonly QueueContract[] = readonly QueueContract[]
> = T[number] extends infer Q
	? Q extends QueueContract
		? {
				id: string
				queue: Q['name']
				payload: InferInput<Q>
				options?: JobOptions | undefined
			}
		: never
	: never

type ProcessArgs<T extends readonly QueueContract[]> = T[number] extends infer Q
	? Q extends QueueContract
		? [
				queueName: Q['name'],
				payload: InferInput<Q>,
				extras?: Partial<JobContext<T, Q['name']>>
			]
		: never
	: never

export type TestHarness<T extends readonly QueueContract[]> = {
	logs: CapturedLog[]
	enqueued: CapturedEnqueue<T>[]
	clearLogs(): void
	process<const Args extends ProcessArgs<T>>(
		...args: Args
	): Promise<{ ctx: JobContext<T, Args[0]>; logs: CapturedLog[] }>
	init<N extends CronInitQueueName<T>>(
		queueName: N,
		extras?: Partial<JobContext<T, `${NoInfer<N>}_cron-init`>>
	): Promise<readonly QueueInput<N, T>[]>
	context<const N extends QueueName<T>>(
		queueName: N,
		extras?: Partial<JobContext<T, NoInfer<N>>>
	): JobContext<T, N>
}

function createCapturingLogger(logs: CapturedLog[]): JobLogger {
	const write =
		(level: keyof JobLogger) =>
		(message: string, attributes?: LogAttributes | undefined) => {
			logs.push({ level, message, attributes })
		}
	return {
		debug: write('debug'),
		info: write('info'),
		warn: write('warn'),
		error: write('error')
	}
}

function fakeHelpers(
	queueName: string,
	extras?: Partial<JobContext>
): JobHelpers {
	const abortController = new AbortController()
	return {
		job: {
			id: extras?.jobId ?? 'test-job',
			job_queue_id: null,
			task_id: 1,
			task_identifier: queueName,
			payload: {},
			priority: 0,
			run_at: new Date(),
			attempts: extras?.attempt ?? 1,
			max_attempts: extras?.maxAttempts ?? 4,
			last_error: null,
			created_at: new Date(),
			updated_at: new Date(),
			key: null,
			revision: 0,
			flags: null,
			is_available: true,
			locked_at: null,
			locked_by: null
		},
		logger: {
			debug() {},
			error() {},
			info() {},
			warn() {},
			scope() {
				return this
			}
		},
		async addJob() {
			throw new Error('addJob is not available in the test harness')
		},
		async addJobs() {
			throw new Error('addJobs is not available in the test harness')
		},
		async withPgClient() {
			throw new Error('withPgClient is not available in the test harness')
		},
		async query() {
			throw new Error('query is not available in the test harness')
		},
		async getQueueName() {
			return null
		},
		abortSignal: extras?.signal ?? abortController.signal,
		abortPromise: new Promise(() => {})
	} as unknown as JobHelpers
}

export function createTestHarness<
	const TQueues extends readonly RunnableQueue[]
>(queues: TQueues & NoInfer<InlineQueues<TQueues>>): TestHarness<TQueues>
export function createTestHarness<
	const TQueues extends readonly QueueContract[]
>(
	contracts: TQueues & readonly HandlerFreeQueueContract[],
	handlers: NoInfer<QueueHandlers<TQueues>>
): TestHarness<TQueues>
export function createTestHarness<
	const TQueues extends readonly QueueContract[]
>(contracts: TQueues, handlers?: QueueHandlers<TQueues>): TestHarness<TQueues> {
	const queues = normalizeWorkerQueues(contracts, handlers)
	const logs: CapturedLog[] = []
	const logger = createCapturingLogger(logs)
	const stepCaches = new Map<string, StepStore>()
	let invocation = 0
	const enqueued: CapturedEnqueue[] = []
	const producer = bindCreateJob({
		queues: contracts,
		otel: null,
		enqueue: {
			async addJob(queue, payload, options) {
				const id = `child-${enqueued.length + 1}`
				enqueued.push({
					id,
					queue,
					payload: extractProducerLink(payload).cleanPayload,
					options
				})
				return { ...fakeHelpers(queue).job, id }
			},
			async addJobs(specs) {
				const result = []
				for (const spec of specs)
					result.push(await this.addJob(spec.identifier, spec.payload, spec))
				return result
			}
		}
	})

	function context(
		queueName: string,
		extras?: Partial<JobContext>
	): JobContext {
		const jobId = extras?.jobId ?? `test-job-${++invocation}`
		const cacheId = JSON.stringify([queueName, jobId])
		if (!stepCaches.has(cacheId))
			stepCaches.set(cacheId, createMemoryStepStore())
		const span = extras?.span ?? createNoopSpan()
		const signal =
			extras?.signal ??
			extras?.helpers?.abortSignal ??
			new AbortController().signal
		const helpers =
			extras?.helpers ?? fakeHelpers(queueName, { ...extras, jobId, signal })
		return {
			jobId,
			queue: extras?.queue ?? queueName,
			attempt: extras?.attempt ?? 1,
			maxAttempts: extras?.maxAttempts ?? 4,
			logger: extras?.logger ?? logger,
			span,
			helpers,
			signal,
			createJob: extras?.createJob ?? producer.enqueueOne,
			createJobs: extras?.createJobs ?? producer.enqueueMany,
			cron: extras?.cron,
			step:
				extras?.step ??
				createStepRunner({
					store: stepCaches.get(cacheId)!,
					span
				})
		}
	}

	function getQueue(queueName: string): NormalizedQueue {
		const queue = queues.find((item) => item.name === queueName)
		if (!queue) {
			throw new Error(`Unknown queue "${queueName}"`)
		}
		return queue
	}

	const harness = {
		logs,
		enqueued,
		clearLogs() {
			logs.length = 0
		},
		async process(
			queueName: string,
			payload: unknown,
			extras?: Partial<JobContext>
		) {
			const queue = getQueue(String(queueName))
			const ctx = context(String(queueName), extras)
			const parsed = await parseQueuePayload(queue, payload)
			await queue.processFn(parsed, ctx)
			return { ctx, logs: [...logs] }
		},
		async init(queueName: string, extras?: Partial<JobContext>) {
			const queue = getQueue(String(queueName))
			if (!queue.initFn) {
				throw new Error(`Queue "${String(queueName)}" is not a cron-init queue`)
			}
			return queue.initFn(
				context(`${String(queueName)}${CRON_INIT_SUFFIX}`, extras)
			)
		},
		context
	}
	return harness as unknown as TestHarness<TQueues>
}
