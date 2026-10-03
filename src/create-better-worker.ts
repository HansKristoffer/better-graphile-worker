import { compact } from './options.js'
import type { CronItem, TaskList, WorkerUtils } from 'graphile-worker'
import type { Pool } from 'pg'
import {
	CRON_INIT_SUFFIX,
	getQueueType,
	type QueueContract,
	type HandlerFreeQueueContract,
	type InlineQueues,
	type RunnableQueue,
	type QueueHandlers
} from './queue.js'
import type { BetterWorkerHooks, JobLogger } from './hooks.js'
import type { CompletedJob, CompletedJobStats } from './completed-jobs-store.js'
import { bindCreateJob, createJobsApi } from './create-job.js'
import { createWorkerClient, DEFAULT_GRAPHILE_WORKER_SCHEMA } from './client.js'
import {
	createCompletedJobsStore,
	createNoopCompletedJobsStore
} from './completed-jobs-store.js'
import {
	getQueueDefinitions,
	mergeJobStats,
	queryJobCounts,
	queryRecentJobs,
	type ListedJob,
	type ListJobsOptions,
	type QueueDefinition,
	type WorkerJobStatsRow
} from './admin.js'
import {
	buildCronItems,
	buildTaskList,
	DEFAULT_CONCURRENCY,
	DEFAULT_POLL_INTERVAL,
	logRegisteredQueues,
	runOnceTasks,
	startRunner,
	type GraphileRunnerOverrides
} from './worker.js'
import { jobLogger } from './observers.js'
import { createLifecycle } from './lifecycle.js'
import { normalizeWorkerQueues, type NormalizedQueue } from './registry.js'
import { assertInteger } from './validation.js'
import { createNoopSpan, getOtel, type OtelApi } from './otel.js'
import { assertUniqueQueueNames } from './define-queues.js'
import type {
	CreateJobFn,
	CreateJobsFn,
	PrepareJobFn,
	PrepareJobsFn,
	CronQueueName,
	JobsApi,
	QueueName
} from './types.js'
import type { JobOptions, StopOptions } from './job-options.js'

export type CompletedJobsOption = false | { maxPerQueue?: number }

type BaseWorkerOptions<TQueues extends readonly QueueContract[]> = {
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
	graphile?: GraphileRunnerOverrides | undefined
	otel?: { api: OtelApi | null }
	/** Retain failures in PostgreSQL by default; discard acknowledges and deletes them. */
	permanentFailure?: 'discard' | 'retain' | undefined
	/**
	 * Run only these queues (and their cron schedules); defaults to all. Enqueueing still
	 * covers every queue, so separate instances can give workloads their own concurrency.
	 */
	process?: readonly NoInfer<QueueName<TQueues>>[]
}

export type BetterWorkerOptions<TQueues extends readonly QueueContract[]> =
	BaseWorkerOptions<TQueues> &
		(TQueues extends readonly RunnableQueue[]
			? { queues: TQueues & NoInfer<InlineQueues<TQueues>>; handlers?: never }
			: {
					queues: TQueues & readonly HandlerFreeQueueContract[]
					handlers: NoInfer<QueueHandlers<TQueues>>
				})

export type BetterWorker<TQueues extends readonly QueueContract[]> = {
	readonly schema: string
	readonly queues: TQueues
	readonly createJob: CreateJobFn<TQueues>
	readonly createJobs: CreateJobsFn<TQueues>
	/** Build enqueue SQL to execute inside your own transaction. */
	readonly prepareJob: PrepareJobFn<TQueues>
	readonly prepareJobs: PrepareJobsFn<TQueues>
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
	getJobStats(): Promise<WorkerJobStatsRow[]>
	listJobs(options?: ListJobsOptions): Promise<ListedJob[]>
	retryJobs(ids: string[]): Promise<string[]>
	failJobs(ids: string[], reason?: string): Promise<string[]>
	getQueueDefinitions(): QueueDefinition[]
}

function createStartupLogger(hooks?: BetterWorkerHooks): JobLogger {
	return jobLogger(hooks ?? {}, {
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

function selectProcessed(
	queues: NormalizedQueue[],
	names: readonly string[] | undefined
): NormalizedQueue[] {
	if (names === undefined) return queues
	const known = new Set(queues.map((queue) => queue.name))
	if (!names.length) throw new TypeError('process must list at least one queue')
	const selected = new Set<string>()
	for (const name of names) {
		if (!known.has(name))
			throw new TypeError(`process lists unknown queue "${name}"`)
		if (selected.has(name))
			throw new TypeError(`process lists queue "${name}" twice`)
		selected.add(name)
	}
	return queues.filter((queue) => selected.has(queue.name))
}

export function createBetterWorker<
	const TQueues extends readonly QueueContract[]
>(options: BetterWorkerOptions<TQueues>): BetterWorker<TQueues> {
	assertUniqueQueueNames(options.queues)
	const queues = selectProcessed(
		normalizeWorkerQueues(options.queues, options.handlers),
		options.process
	)
	const otel = options.otel ? options.otel.api : getOtel()
	assertInteger(options.concurrency ?? DEFAULT_CONCURRENCY, 'concurrency', 1)
	assertInteger(
		options.pollInterval ?? DEFAULT_POLL_INTERVAL,
		'pollInterval',
		1,
		2147483647
	)

	const schema = options.schema ?? DEFAULT_GRAPHILE_WORKER_SCHEMA
	const hooks = options.hooks ?? {}
	const client = createWorkerClient({
		pgPool: options.pgPool,
		schema
	})
	const completedJobs = resolveCompletedJobsStore(options.completedJobs)
	const producer = bindCreateJob<TQueues>({
		enqueue: client.enqueue,
		otel,
		queues: options.queues,
		schema,
		...compact({
			hooks,
			validateOnEnqueue: options.validateOnEnqueue,
			defaultMaxAttempts: options.defaultMaxAttempts
		})
	})
	const { createJob, createJobs, prepareJob, prepareJobs } = producer
	const jobs = createJobsApi(createJob, options.queues)
	const logger = createStartupLogger(hooks)
	const runtime = {
		hooks,
		completedJobs,
		createJob: producer.enqueueOne,
		createJobs: producer.enqueueMany,
		logger,
		schema,
		otel,
		permanentFailure: options.permanentFailure
	}

	const lifecycle = createLifecycle({
		start: async () => {
			logger.info('Starting graphile-worker')
			logRegisteredQueues(queues, logger)
			return startRunner({
				pgPool: options.pgPool,
				schema,
				taskList: buildTaskList(queues, runtime),
				cronItems: buildCronItems(queues, options.defaultMaxAttempts),
				concurrency: options.concurrency ?? DEFAULT_CONCURRENCY,
				pollInterval: options.pollInterval ?? DEFAULT_POLL_INTERVAL,
				noHandleSignals: true,
				graphile: options.graphile
			})
		},
		release: () => client.release(),
		onError: (error) =>
			logger.error('worker.lifecycle.failed', { error: String(error) }),
		handleSignals: options.handleSignals
	})

	return {
		schema,
		queues: options.queues,
		createJob,
		createJobs,
		prepareJob,
		prepareJobs,
		jobs,
		get promise() {
			return lifecycle.promise
		},
		migrate: () => client.migrate(),
		start: () => lifecycle.start(),
		stop: (stopOptions) => lifecycle.stop(stopOptions?.timeout),
		waitUntilStopped: () => lifecycle.waitUntilStopped(),
		runOnce: () =>
			lifecycle.runOnce(() =>
				runOnceTasks({
					pgPool: options.pgPool,
					schema,
					taskList: buildTaskList(queues, runtime),
					noHandleSignals: true,
					graphile: options.graphile
				})
			),
		triggerCron: (name, opts) => producer.triggerCron(name, opts),

		getWorkerUtils() {
			return client.getUtils()
		},
		buildTaskList() {
			return buildTaskList(queues, runtime)
		},
		buildCronItems() {
			return buildCronItems(queues, options.defaultMaxAttempts)
		},
		getCompletedJobs() {
			return completedJobs.getAll()
		},
		getCompletedJobsStats() {
			return completedJobs.getStats()
		},
		async getJobStats() {
			const utils = client
			const rows = await queryJobCounts(utils, schema)
			const taskIdentifiers = options.queues.flatMap((queue) =>
				getQueueType(queue) === 'cron-init'
					? [queue.name, `${queue.name}${CRON_INIT_SUFFIX}`]
					: [queue.name]
			)
			return mergeJobStats(rows, completedJobs.getStats(), taskIdentifiers)
		},
		async listJobs(listOptions) {
			const utils = client
			return queryRecentJobs(utils, schema, listOptions)
		},
		retryJobs: (ids) => client.retryJobs(ids),
		async failJobs(ids, reason) {
			const utils = await client.getUtils()
			const jobs = await utils.permanentlyFailJobs(ids, reason)
			return jobs.map((job) => String(job.id))
		},
		getQueueDefinitions() {
			return getQueueDefinitions(options.queues, options.defaultMaxAttempts)
		}
	}
}

export {
	DEFAULT_GRAPHILE_WORKER_SCHEMA,
	DEFAULT_CONCURRENCY,
	DEFAULT_POLL_INTERVAL
}

export type EnqueueableQueueName<TQueues extends readonly QueueContract[]> =
	QueueName<TQueues>
