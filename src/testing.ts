import type { JobHelpers } from 'graphile-worker'
import {
	type JobContext,
	type QueueContract,
	type HandlerFreeQueueContract,
	type InlineQueues,
	type RunnableQueue,
	type QueueHandlers,
	CRON_INIT_SUFFIX,
	parseContinueRunAt
} from './queue.js'
import type { JobLogger, LogAttributes } from './hooks.js'
import { createNoopSpan } from './otel.js'
import type {
	CreateJobFn,
	CreateJobsFn,
	CronInitQueueName,
	CronQueueName,
	EnqueueInTransactionFn,
	InferInput,
	QueueInput,
	QueueName
} from './types.js'
import {
	createStepRunner,
	createMemoryStepStore,
	type StepStore,
	type WaitBackend
} from './steps.js'
import { encodeQueueOutput, type ChildReport } from './waits.js'
import { extractProducerLink } from './payload.js'
import {
	bindCreateJob,
	DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS,
	parseQueuePayload
} from './create-job.js'
import { normalizeWorkerQueues, type NormalizedQueue } from './registry.js'
import type { JobOptions } from './job-options.js'
import { JobContinuedSignal, UnknownQueueError } from './errors.js'
import { createProgressRunner, type ProgressRunner } from './progress.js'

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

export type CapturedContinue = {
	queue: string
	jobId: string
	runAt: Date | undefined
}

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
	/** Runs that ended with `ctx.continue()`; `process()` resolves normally for them. */
	continued: CapturedContinue[]
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
	/**
	 * Enqueue as the worker would, validated and with derived options, into `enqueued`.
	 * Hand these to the code under test in place of the app's producer.
	 */
	createJob: CreateJobFn<T>
	createJobs: CreateJobsFn<T>
	/** Like `createJob`; the transaction is not used. */
	enqueueInTransaction: EnqueueInTransactionFn<T>
	/** Capture a cron queue's run: its task, or its `{name}_cron-init` task. */
	triggerCron<N extends CronQueueName<T>>(
		queueName: N,
		options?: JobOptions
	): Promise<string | null>
	/**
	 * Run each enqueued job that has not run yet, and the jobs those enqueue, in order
	 * until none is left, ignoring `runAt`. A cron-init trigger runs its `initFn` and
	 * enqueues its items. Continuations stay in `continued`. Throws the first job's error,
	 * or after `maxJobs` (default 1000) runs. Returns how many ran.
	 */
	drain(options?: { maxJobs?: number }): Promise<number>
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
	const progressValues = new Map<string, unknown>()
	let invocation = 0
	const enqueued: CapturedEnqueue[] = []
	const continued: CapturedContinue[] = []
	/** Captured jobs that already ran: drained ones and waited children. */
	const ran = new Set<string>()
	function capture(queue: string, payload: unknown, options?: JobOptions) {
		const id = `child-${enqueued.length + 1}`
		enqueued.push({
			id,
			queue,
			payload: extractProducerLink(payload).cleanPayload,
			options
		})
		return { ...fakeHelpers(queue).job, id }
	}
	const producer = bindCreateJob({
		queues: contracts,
		otel: null,
		enqueue: {
			async addJob(queue, payload, options) {
				return capture(queue, payload, options)
			},
			async addJobs(specs) {
				return specs.map((spec) =>
					capture(spec.identifier, spec.payload, spec as JobOptions)
				)
			},
			// The harness has no database, so it records keys but never deduplicates.
			async addIdempotentJobs(specs) {
				return specs.map((spec) => ({
					id: capture(spec.identifier, spec.payload, spec as JobOptions).id,
					deduplicated: false
				}))
			}
		}
	})

	/** Children run inline, once and in order; a thrown error is their final failure. */
	const waits: WaitBackend = {
		async wait(_id, queue, inputs, batch) {
			if (!contracts.includes(queue))
				throw new UnknownQueueError(
					queue.name,
					contracts.map((item) => item.name)
				)
			const enqueued = batch
				? await producer.enqueueMany(queue.name, inputs)
				: [await producer.enqueueOne(queue.name, inputs[0])]
			// The harness has no shouldSkipEnqueue hook, so every child has an id.
			const ids = enqueued.filter((id) => id !== null)
			for (const id of ids) ran.add(id)

			const child = getQueue(queue.name)
			const reports: (ChildReport & { id: string })[] = []
			for (const [index, id] of ids.entries()) {
				const report: ChildReport = await invoke(
					queue.name,
					{ jobId: id },
					async (ctx) => {
						const payload = await parseQueuePayload(child, inputs[index])
						return encodeQueueOutput(child, await child.processFn(payload, ctx))
					}
				).then(
					(run) => {
						if (run.continued)
							throw new Error(
								`Child ${id} of "${queue.name}" called ctx.continue(); the test harness runs waited children once`
							)
						return run.result === undefined
							? { ok: true }
							: { ok: true, output: run.result }
					},
					(error: unknown) => {
						const failure =
							error instanceof Error ? error : new Error(String(error))
						return {
							ok: false,
							error: { name: failure.name, message: failure.message }
						}
					}
				)
				reports.push({ id, ...report })
			}
			return { checkpoint: reports, persisted: false }
		}
	}

	/** The queue's own maxAttempts, as Graphile would store it on the job. */
	function maxAttemptsOf(queueName: string): number {
		const name = queueName.endsWith(CRON_INIT_SUFFIX)
			? queueName.slice(0, -CRON_INIT_SUFFIX.length)
			: queueName
		const queue = queues.find((item) => item.name === name)
		return (
			(queue?.cron === undefined
				? undefined
				: queue.cronOptions?.maxAttempts) ??
			queue?.maxAttempts ??
			DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS
		)
	}

	function createContext(
		queueName: string,
		extras?: Partial<JobContext>
	): { ctx: JobContext; progress: ProgressRunner } {
		const jobId = extras?.jobId ?? `test-job-${++invocation}`
		const maxAttempts = extras?.maxAttempts ?? maxAttemptsOf(queueName)
		const cacheId = JSON.stringify([queueName, jobId])
		if (!stepCaches.has(cacheId))
			stepCaches.set(cacheId, createMemoryStepStore())
		// Like the worker, progress persists per job id; init jobs are untyped.
		const progress = createProgressRunner({
			initial: progressValues.get(cacheId),
			schema: queues.find((queue) => queue.name === queueName)?.progressSchema,
			write: async (value) => {
				progressValues.set(cacheId, value)
			}
		})
		const span = extras?.span ?? createNoopSpan()
		const signal =
			extras?.signal ??
			extras?.helpers?.abortSignal ??
			new AbortController().signal
		const helpers =
			extras?.helpers ??
			fakeHelpers(queueName, { ...extras, jobId, signal, maxAttempts })
		let called = false
		const ctx: JobContext = {
			jobId,
			queue: extras?.queue ?? queueName,
			attempt: extras?.attempt ?? 1,
			maxAttempts,
			logger: extras?.logger ?? logger,
			span,
			helpers,
			signal,
			createJob: extras?.createJob ?? producer.enqueueOne,
			createJobs: extras?.createJobs ?? producer.enqueueMany,
			cron: extras?.cron,
			continue:
				extras?.continue ??
				(async (options) => {
					if (called) throw new Error('ctx.continue() was already called')
					called = true
					continued.push({
						queue: queueName,
						jobId,
						runAt: parseContinueRunAt(options)
					})
					throw new JobContinuedSignal()
				}),
			step:
				extras?.step ??
				createStepRunner({
					store: stepCaches.get(cacheId)!,
					span,
					waits
				}),
			progress: extras?.progress ?? (progress as JobContext['progress'])
		}
		return { ctx, progress }
	}

	function context(queueName: string, extras?: Partial<JobContext>) {
		return createContext(queueName, extras).ctx
	}

	/** Persist the latest progress like a retried or continued worker run would see it. */
	async function invoke<T>(
		queueName: string,
		extras: Partial<JobContext> | undefined,
		fn: (ctx: JobContext) => Promise<T>
	): Promise<{ ctx: JobContext; result: T | undefined; continued: boolean }> {
		const { ctx, progress } = createContext(queueName, extras)
		try {
			return { ctx, result: await fn(ctx), continued: false }
		} catch (error) {
			if (!(error instanceof JobContinuedSignal)) throw error
			return { ctx, result: undefined, continued: true }
		} finally {
			await progress.close(true)
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
		continued,
		clearLogs() {
			logs.length = 0
		},
		async process(
			queueName: string,
			payload: unknown,
			extras?: Partial<JobContext>
		) {
			const queue = getQueue(String(queueName))
			const { ctx } = await invoke(String(queueName), extras, async (ctx) => {
				await encodeQueueOutput(
					queue,
					await queue.processFn(await parseQueuePayload(queue, payload), ctx)
				)
			})
			return { ctx, logs: [...logs] }
		},
		async init(queueName: string, extras?: Partial<JobContext>) {
			const queue = getQueue(String(queueName))
			if (!queue.initFn) {
				throw new Error(`Queue "${String(queueName)}" is not a cron-init queue`)
			}
			const initFn = queue.initFn
			const { result } = await invoke(
				`${String(queueName)}${CRON_INIT_SUFFIX}`,
				extras,
				async (ctx) => initFn(ctx)
			)
			return result ?? []
		},
		context,
		createJob: producer.enqueueOne,
		createJobs: producer.enqueueMany,
		triggerCron: producer.triggerCron,
		enqueueInTransaction(
			_tx: unknown,
			{
				task,
				payload,
				...jobOptions
			}: { task: string; payload?: unknown } & JobOptions
		) {
			return producer.enqueueOne(task, payload, jobOptions)
		},
		async drain({ maxJobs = 1000 }: { maxJobs?: number } = {}) {
			let count = 0
			for (;;) {
				const next = enqueued.find((job) => !ran.has(job.id))
				if (!next) return count
				if (++count > maxJobs)
					throw new Error(
						`drain() ran ${maxJobs} jobs and more are enqueued; a job may enqueue itself`
					)
				ran.add(next.id)
				if (next.queue.endsWith(CRON_INIT_SUFFIX)) {
					const name = next.queue.slice(0, -CRON_INIT_SUFFIX.length)
					const items = await harness.init(name)
					if (items.length) await producer.enqueueMany(name, items)
				} else await harness.process(next.queue, next.payload)
			}
		}
	}
	return harness as unknown as TestHarness<TQueues>
}
