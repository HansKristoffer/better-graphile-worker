import type { JobHelpers } from 'graphile-worker'
import {
	hasInputSchema,
	isCronInitQueue,
	type JobContext,
	type QueueAny
} from './create-queue'
import type { JobLogger, LogAttributes } from './hooks'
import { createNoopSpan } from './otel'
import type { QueueInput, QueueName } from './types'
import { createStepRunner, type StepStore } from './steps'
import type { StepCache } from './payload'

export type CapturedLog = {
	level: keyof JobLogger
	message: string
	attributes?: LogAttributes
}

export type TestHarness<TQueues extends readonly QueueAny[]> = {
	logs: CapturedLog[]
	clearLogs(): void
	process<T extends QueueName<TQueues>>(
		queueName: T,
		payload: QueueInput<T, TQueues>,
		extras?: Partial<JobContext>
	): Promise<{ ctx: JobContext; logs: CapturedLog[] }>
	init<T extends QueueName<TQueues>>(
		queueName: T,
		extras?: Partial<JobContext>
	): Promise<unknown[]>
	context(queueName: string, extras?: Partial<JobContext>): JobContext
}

function createCapturingLogger(logs: CapturedLog[]): JobLogger {
	const write =
		(level: keyof JobLogger) =>
		(message: string, attributes?: LogAttributes) => {
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

function createHarnessStepStore(
	jobId: string,
	caches: Map<string, StepCache>
): StepStore {
	if (!caches.has(jobId)) {
		caches.set(jobId, {})
	}
	return {
		get(id) {
			return caches.get(jobId)?.[id]
		},
		async set(id, output) {
			const cache = caches.get(jobId) ?? {}
			cache[id] = { output }
			caches.set(jobId, cache)
		}
	}
}

export function createTestHarness<const TQueues extends readonly QueueAny[]>(
	queues: TQueues
): TestHarness<TQueues> {
	const logs: CapturedLog[] = []
	const logger = createCapturingLogger(logs)
	const stepCaches = new Map<string, StepCache>()

	function context(
		queueName: string,
		extras?: Partial<JobContext>
	): JobContext {
		const jobId = extras?.jobId ?? 'test-job'
		const span = extras?.span ?? createNoopSpan()
		return {
			jobId,
			queue: extras?.queue ?? queueName,
			attempt: extras?.attempt ?? 1,
			maxAttempts: extras?.maxAttempts ?? 4,
			logger: extras?.logger ?? logger,
			span,
			helpers: extras?.helpers ?? fakeHelpers(queueName, extras),
			signal: extras?.signal ?? new AbortController().signal,
			createJob: extras?.createJob ?? (async () => null),
			createJobs: extras?.createJobs ?? (async () => []),
			cron: extras?.cron,
			step:
				extras?.step ??
				createStepRunner({
					store: createHarnessStepStore(jobId, stepCaches),
					span
				})
		}
	}

	function getQueue(queueName: string): QueueAny {
		const queue = queues.find((item) => item.name === queueName)
		if (!queue) {
			throw new Error(`Unknown queue "${queueName}"`)
		}
		return queue
	}

	return {
		logs,
		clearLogs() {
			logs.length = 0
		},
		async process(queueName, payload, extras) {
			const queue = getQueue(String(queueName))
			const ctx = context(String(queueName), extras)
			const parsed = hasInputSchema(queue)
				? queue.inputSchema.parse(payload)
				: undefined
			await queue.processFn(parsed, ctx)
			return { ctx, logs: [...logs] }
		},
		async init(queueName, extras) {
			const queue = getQueue(String(queueName))
			if (!isCronInitQueue(queue)) {
				throw new Error(`Queue "${String(queueName)}" is not a cron-init queue`)
			}
			return queue.initFn(context(String(queueName), extras))
		},
		context
	}
}
