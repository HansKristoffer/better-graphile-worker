import type { CronItem, Runner, TaskList, WorkerUtils } from 'graphile-worker'
import type { Pool } from 'pg'
import {
	CRON_INIT_SUFFIX,
	isCronInitQueue,
	isCronQueue,
	type QueueAny
} from './create-queue'
import type { BetterWorkerHooks, JobLogger } from './hooks'
import type { CompletedJob, CompletedJobStats } from './completed-jobs-store'
import {
	bindCreateJob,
	createJobsApi,
	DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS
} from './create-job'
import { createWorkerClient, DEFAULT_GRAPHILE_WORKER_SCHEMA } from './client'
import {
	createCompletedJobsStore,
	createNoopCompletedJobsStore
} from './completed-jobs-store'
import {
	getQueueDefinitions,
	queryJobCounts,
	queryRecentJobs,
	type JobCountRow,
	type ListedJob,
	type ListJobsOptions,
	type QueueDefinition
} from './admin'
import {
	buildCronItems,
	buildTaskList,
	DEFAULT_CONCURRENCY,
	DEFAULT_POLL_INTERVAL,
	logRegisteredQueues,
	runOnceTasks,
	startRunner,
	type GraphileRunnerOverrides
} from './worker'
import { createConsoleLogger } from './default-logger'
import { createNoopSpan, setOtelApi, type OtelApi } from './otel'
import { assertUniqueQueueNames } from './define-queues'
import type {
	CreateJobFn,
	CreateJobsFn,
	CronQueueName,
	JobsApi,
	QueueName
} from './types'
import type { JobOptions, StopOptions } from './job-options'

export type CompletedJobsOption = false | { maxPerQueue?: number }

export type BetterWorkerOptions<TQueues extends readonly QueueAny[]> = {
	pgPool: Pool
	queues: TQueues
	schema?: string
	concurrency?: number
	pollInterval?: number
	handleSignals?: boolean
	hooks?: BetterWorkerHooks
	validateOnEnqueue?: boolean
	defaultMaxAttempts?: number
	completedJobs?: CompletedJobsOption
	graphile?: GraphileRunnerOverrides
	otel?: { api: OtelApi | null }
}

export type BetterWorker<TQueues extends readonly QueueAny[]> = {
	readonly schema: string
	readonly queues: TQueues
	readonly createJob: CreateJobFn<TQueues>
	readonly createJobs: CreateJobsFn<TQueues>
	readonly jobs: JobsApi<TQueues>
	readonly promise: Promise<void>
	migrate(): Promise<void>
	start(): Promise<void>
	stop(options?: StopOptions): Promise<void>
	waitUntilStopped(): Promise<void>
	runOnce(): Promise<void>
	triggerCron<T extends CronQueueName<TQueues>>(
		queueName: T,
		options?: JobOptions
	): Promise<string | null>
	getWorkerUtils(): Promise<WorkerUtils>
	buildTaskList(): TaskList
	buildCronItems(): CronItem[]
	getCompletedJobs(): CompletedJob[]
	getCompletedJobsStats(): CompletedJobStats
	getJobStats(): Promise<JobCountRow[]>
	listJobs(options?: ListJobsOptions): Promise<ListedJob[]>
	retryJobs(ids: string[]): Promise<string[]>
	failJobs(ids: string[], reason?: string): Promise<string[]>
	getQueueDefinitions(): QueueDefinition[]
}

function createStartupLogger(hooks?: BetterWorkerHooks): JobLogger {
	if (hooks?.createLogger) {
		return hooks.createLogger({
			queue: 'worker',
			jobId: 'startup',
			attempt: 0,
			span: createNoopSpan()
		})
	}
	return createConsoleLogger({
		queue: 'worker',
		jobId: 'startup',
		attempt: 0,
		span: createNoopSpan()
	})
}

function resolveCompletedJobsStore(option: CompletedJobsOption | undefined) {
	if (option === false || option === undefined) {
		return createNoopCompletedJobsStore()
	}
	return createCompletedJobsStore(option.maxPerQueue)
}

export function createBetterWorker<TQueues extends readonly QueueAny[]>(
	options: BetterWorkerOptions<TQueues>
): BetterWorker<TQueues> {
	assertUniqueQueueNames(options.queues)
	if (options.otel) {
		setOtelApi(options.otel.api)
	}

	const schema = options.schema ?? DEFAULT_GRAPHILE_WORKER_SCHEMA
	const hooks = options.hooks ?? {}
	const client = createWorkerClient({
		pgPool: options.pgPool,
		schema
	})
	const completedJobs = resolveCompletedJobsStore(options.completedJobs)
	const { createJob, createJobs } = bindCreateJob<TQueues>({
		getWorkerUtils: () => client.getUtils(),
		queues: options.queues,
		hooks,
		validateOnEnqueue: options.validateOnEnqueue,
		defaultMaxAttempts: options.defaultMaxAttempts
	})
	const jobs = createJobsApi(createJob)
	const logger = createStartupLogger(hooks)
	const runtime = {
		hooks,
		completedJobs,
		createJob,
		createJobs,
		logger
	}

	let runner: Runner | null = null
	let signalHandlers: (() => void) | null = null

	function installSignalHandlers(stop: () => Promise<void>) {
		const onSignal = () => {
			void stop()
		}
		process.on('SIGINT', onSignal)
		process.on('SIGTERM', onSignal)
		signalHandlers = () => {
			process.off('SIGINT', onSignal)
			process.off('SIGTERM', onSignal)
		}
	}

	async function stop(stopOptions?: StopOptions): Promise<void> {
		if (signalHandlers) {
			signalHandlers()
			signalHandlers = null
		}
		if (runner) {
			const stopPromise = runner.stop()
			if (stopOptions?.timeout !== undefined) {
				await Promise.race([
					stopPromise,
					new Promise<void>((resolve) => {
						setTimeout(resolve, stopOptions.timeout)
					})
				])
			} else {
				await stopPromise
			}
			runner = null
		}
		await client.release()
	}

	return {
		schema,
		queues: options.queues,
		createJob,
		createJobs,
		jobs,
		get promise() {
			return runner ? runner.promise : Promise.resolve()
		},
		async migrate() {
			await client.migrate()
		},
		async start() {
			if (runner) {
				throw new Error('Worker is already running')
			}
			logger.info('Starting graphile-worker')
			const taskList = buildTaskList(options.queues, runtime)
			const cronItems = buildCronItems(options.queues)
			logRegisteredQueues(options.queues, logger)

			if (options.handleSignals) {
				installSignalHandlers(() => stop())
			}

			runner = await startRunner({
				pgPool: options.pgPool,
				schema,
				taskList,
				cronItems,
				concurrency: options.concurrency ?? DEFAULT_CONCURRENCY,
				pollInterval: options.pollInterval ?? DEFAULT_POLL_INTERVAL,
				noHandleSignals: true,
				graphile: options.graphile
			})
		},
		stop,
		async waitUntilStopped() {
			if (runner) {
				await runner.promise
			}
		},
		async runOnce() {
			await runOnceTasks({
				pgPool: options.pgPool,
				schema,
				taskList: buildTaskList(options.queues, runtime),
				noHandleSignals: true,
				graphile: options.graphile
			})
		},
		async triggerCron(queueName, jobOptions) {
			const queue = options.queues.find((item) => item.name === queueName)
			if (!queue || (!isCronQueue(queue) && !isCronInitQueue(queue))) {
				throw new Error(`"${String(queueName)}" is not a cron queue`)
			}
			const taskName = isCronInitQueue(queue)
				? `${queue.name}${CRON_INIT_SUFFIX}`
				: queue.name
			if (hooks.shouldSkipEnqueue?.()) {
				return null
			}
			const workerUtils = await client.getUtils()
			const job = await workerUtils.addJob(
				taskName,
				{},
				{
					maxAttempts:
						jobOptions?.maxAttempts ??
						queue.maxAttempts ??
						options.defaultMaxAttempts ??
						DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS,
					priority: jobOptions?.priority ?? queue.priority,
					flags: jobOptions?.flags ?? queue.flags,
					runAt: jobOptions?.runAt,
					jobKey: jobOptions?.jobKey,
					jobKeyMode: jobOptions?.jobKeyMode
				}
			)
			return String(job.id)
		},
		getWorkerUtils() {
			return client.getUtils()
		},
		buildTaskList() {
			return buildTaskList(options.queues, runtime)
		},
		buildCronItems() {
			return buildCronItems(options.queues)
		},
		getCompletedJobs() {
			return completedJobs.getAll()
		},
		getCompletedJobsStats() {
			return completedJobs.getStats()
		},
		async getJobStats() {
			const utils = await client.getUtils()
			return queryJobCounts(utils, schema)
		},
		async listJobs(listOptions) {
			const utils = await client.getUtils()
			return queryRecentJobs(utils, schema, listOptions)
		},
		async retryJobs(ids) {
			const utils = await client.getUtils()
			const jobs = await utils.rescheduleJobs(ids, {
				attempts: 0,
				runAt: new Date()
			})
			return jobs.map((job) => String(job.id))
		},
		async failJobs(ids, reason) {
			const utils = await client.getUtils()
			const jobs = await utils.permanentlyFailJobs(ids, reason)
			return jobs.map((job) => String(job.id))
		},
		getQueueDefinitions() {
			return getQueueDefinitions(options.queues)
		}
	}
}

export {
	DEFAULT_GRAPHILE_WORKER_SCHEMA,
	DEFAULT_CONCURRENCY,
	DEFAULT_POLL_INTERVAL
}

export type EnqueueableQueueName<TQueues extends readonly QueueAny[]> =
	QueueName<TQueues>
