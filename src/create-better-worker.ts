import { compact } from './options.js'
import type { CronItem, TaskList, WorkerUtils } from 'graphile-worker'
import type { Pool } from 'pg'
import {
	CRON_INIT_SUFFIX,
	getQueueType,
	type QueueContract,
	isCronQueue,
	resolveSerialQueueName,
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
	createNoopCompletedJobsStore,
	type CompletedJobsStore
} from './completed-jobs-store.js'
import {
	deleteFailedJobs,
	getQueueDefinitions,
	mergeJobStats,
	queryJobCounts,
	queryRecentJobs,
	queryJobProgress,
	removeJobByKey,
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
	DEFAULT_SHUTDOWN_GRACE,
	DEFAULT_TIMEOUT,
	logRegisteredQueues,
	reportShutdownFailures,
	runOnceTasks,
	startRunner,
	type GraphileRunnerOverrides,
	type TaskListRuntime,
	type RunningJob
} from './worker.js'
import { jobLogger, observe, reportPermanentFailure } from './observers.js'
import {
	queryJob,
	resolveOutcomeRetention,
	type JobOutcomesOption,
	type JobState
} from './job-outcomes.js'
import {
	createLifecycle,
	resolveRestart,
	type RestartOptions
} from './lifecycle.js'
import { normalizeWorkerQueues, type NormalizedQueue } from './registry.js'
import {
	resolveDatabaseProbe,
	thenEndPool,
	whileReachable
} from './shutdown.js'
import { assertInteger, parseDuration } from './validation.js'
import { createNoopSpan, getOtel, type OtelOption } from './otel.js'
import { withQueueMetrics } from './metrics.js'
import type { JobKeyMode, PayloadMode } from './create-job.js'
import { assertUniqueQueueNames } from './define-queues.js'
import {
	createMaintenance,
	maintainOnce,
	releaseLocks,
	resolveCrashRecovery,
	type CrashRecoveryOptions,
	type Maintenance,
	type ReleaseLocksOptions
} from './maintenance.js'
import { permanentlyFailJobs, type ReleasedJob } from './private-jobs.js'
import { extractProducerLink } from './payload.js'
import type { PermanentFailureReason } from './hooks.js'
import type {
	CreateJobFn,
	CreateJobsFn,
	EnqueueInTransactionFn,
	PrepareJobFn,
	PrepareJobRequestFn,
	PrepareJobsFn,
	CronQueueName,
	GetJobProgressFn,
	JobsApi,
	QueueName
} from './types.js'
import type { Duration, JobOptions, StopOptions } from './job-options.js'

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
	/**
	 * Fail attempts that run longer than this many milliseconds; queues override it with
	 * `timeout`. Defaults to 3 h 50 min, below Graphile's 4-hour reclaim; `false` means no
	 * limit.
	 */
	defaultTimeout?: number | false
	completedJobs?: CompletedJobsOption
	/** Record final job states in PostgreSQL so `getJob` can read them after completion. */
	jobOutcomes?: JobOutcomesOption
	graphile?: GraphileRunnerOverrides | undefined
	/** `metrics: true` records queue metrics through the same API's meter. */
	otel?: OtelOption
	/** Retain failures in PostgreSQL by default; discard acknowledges and deletes them. */
	permanentFailure?: 'discard' | 'retain' | undefined
	/**
	 * Heartbeat this worker and release jobs (and lanes) held by workers that stopped
	 * heartbeating, instead of waiting for Graphile's 4-hour stale-lock scan. On by
	 * default; `false` turns it off.
	 */
	crashRecovery?: boolean | CrashRecoveryOptions | undefined
	/**
	 * Start a new runner when Graphile stops one on its own, for example after losing the
	 * database, and keep trying while it is unreachable. On by default; `false` makes
	 * `promise` reject with `WorkerStoppedError` instead.
	 */
	restart?: boolean | RestartOptions | undefined
	/**
	 * Fetch once more this many milliseconds after each new-job notification. Workers that
	 * share a database skip each other's locked lanes, so a first fetch can miss a job.
	 */
	secondLookOnNotify?: number | undefined
	/**
	 * Fetch and lock up to `concurrency + prefetch` jobs at once, so the next job is ready
	 * when a slot frees (Graphile's `localQueue.size`). Off by default. Graphile fetches
	 * the batch in one statement, which can take several jobs of one serial lane, so they
	 * run at the same time: use it only on runners without lanes. Locked jobs wait
	 * locally, so another worker cannot take them meanwhile.
	 */
	prefetch?: number | undefined
	/**
	 * `json-compat` passes wire inputs through `JSON.stringify` first, as producers before
	 * version 2 did: undefined fields are dropped and dates become strings.
	 */
	payloadMode?: PayloadMode | undefined
	/**
	 * `jobKeyMode` for `enqueueInTransaction` jobs that set a `jobKey` but no mode; other
	 * enqueue paths keep Graphile's `replace`. Defaults to `replace`.
	 */
	transactionalJobKeyMode?: JobKeyMode | undefined
	/**
	 * How long running jobs get after `stop()`, in milliseconds; defaults to 25 seconds.
	 * `ctx.signal` aborts when it ends; jobs still running 500 ms later are failed (their
	 * attempt spent) and unlocked, and `stop()` resolves. `false` leaves Graphile's own
	 * shutdown: `ctx.signal` aborts after 5 seconds and `stop()` waits with no limit.
	 */
	shutdownGrace?: number | false | undefined
	/**
	 * Run `migrate()` once before the first enqueue, prepare or admin read, for processes
	 * that enqueue without starting a worker. Off by default: enqueue-only roles that
	 * cannot create tables keep working.
	 */
	autoMigrate?: boolean | undefined
}

/** How handlers are registered: inline `processFn`s or a contract handler registry. */
export type QueueRegistration<TQueues extends readonly QueueContract[]> =
	TQueues extends readonly RunnableQueue[]
		? { queues: TQueues & NoInfer<InlineQueues<TQueues>>; handlers?: never }
		: {
				queues: TQueues & readonly HandlerFreeQueueContract[]
				handlers: NoInfer<QueueHandlers<TQueues>>
			}

export type BetterWorkerOptions<TQueues extends readonly QueueContract[]> =
	BaseWorkerOptions<TQueues> & QueueRegistration<TQueues>

/** Options each runner of a worker pool sets for itself. */
export type WorkerRunnerOptions = Pick<
	BaseWorkerOptions<readonly QueueContract[]>,
	| 'concurrency'
	| 'pollInterval'
	| 'secondLookOnNotify'
	| 'graphile'
	| 'prefetch'
>

/** Worker options a pool shares across its runners; `prefetch` is a default runners override. */
export type SharedWorkerOptions<TQueues extends readonly QueueContract[]> =
	Omit<
		BaseWorkerOptions<TQueues>,
		Exclude<keyof WorkerRunnerOptions, 'prefetch'>
	>

export type BetterWorker<TQueues extends readonly QueueContract[]> = {
	readonly schema: string
	readonly queues: TQueues
	readonly createJob: CreateJobFn<TQueues>
	readonly createJobs: CreateJobsFn<TQueues>
	/**
	 * Build enqueue SQL to execute inside your own transaction, from a queue name and
	 * arguments or one `JobRequest` object.
	 */
	readonly prepareJob: PrepareJobRequestFn<TQueues> & PrepareJobFn<TQueues>
	readonly prepareJobs: PrepareJobsFn<TQueues>
	/** Prepare one job and run it in a Prisma transaction, so it commits with the transaction. */
	readonly enqueueInTransaction: EnqueueInTransactionFn<TQueues>
	readonly jobs: JobsApi<TQueues>
	/**
	 * Enqueue by a name known only at runtime, validated like `createJob`. A cron queue, or
	 * a cron-init queue without a payload, triggers its cron run.
	 */
	enqueueByName(
		name: string,
		payload?: unknown,
		options?: JobOptions
	): Promise<string | null>
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
	getJob(id: string): Promise<JobState>
	/** The live job with this `jobKey`, payload included; null when there is none. */
	getJobByKey(key: string): Promise<ListedJob | null>
	/**
	 * Graphile's `removeJob`: delete the unlocked job with this key, or stop a running one
	 * from retrying and free its key. Returns its id, or null when there was none.
	 */
	removeJob(key: string): Promise<string | null>
	/**
	 * Release jobs and lanes whose lock owner looks dead (see `ReleaseLocksOptions`), with
	 * crash recovery's `staleAfter`. Each attempt stays spent. Returns the released job ids.
	 */
	releaseLocks(options?: ReleaseLocksOptions): Promise<string[]>
	/** Delete permanently failed jobs not updated for `olderThan`. Returns their ids. */
	deleteFailedJobs(olderThan: Duration): Promise<string[]>
	/** Read a job's `ctx.progress` value; null once the job no longer exists. */
	readonly getJobProgress: GetJobProgressFn<TQueues>
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

export function resolveCompletedJobsStore(
	option: CompletedJobsOption | undefined
) {
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
	return createWorker(options, resolveCompletedJobsStore(options.completedJobs))
}

/**
 * The connections a worker or pool's started runners hold for good: one LISTEN each and
 * one shared maintenance connection. Without one more, no job can ever be fetched.
 */
export function assertPoolSize(pgPool: Pool, runners: number): void {
	const max = (pgPool as { options?: { max?: unknown } }).options?.max
	const needed = runners + 2
	if (typeof max === 'number' && max < needed)
		throw new TypeError(
			`pgPool max is ${max}, but ${runners} started runner(s) hold ${runners + 1} connections and need one more to fetch jobs; set max to at least ${needed}, or use pgPoolSize()`
		)
}

/**
 * Graphile's prefetching fetch can take several jobs of one serial lane in one batch.
 * Lanes from `deriveJobOptions` are only known per job, so a queue that derives options
 * counts too.
 */
function warnPrefetchWithLanes(
	queues: readonly QueueContract[],
	prefetch: number,
	logger: JobLogger,
	runner: string | undefined
) {
	if (!prefetch) return
	const laned = queues
		.filter(
			(queue) =>
				resolveSerialQueueName(queue.serial, queue.name, isCronQueue(queue)) !==
					undefined || queue.deriveJobOptions !== undefined
		)
		.map((queue) => queue.name)
	if (laned.length)
		logger.warn('worker.prefetch.lanes', {
			runner,
			queues: laned.join(','),
			message:
				'prefetch can run several jobs of one serial lane at once (Graphile fetches them in one batch); set prefetch: 0 on runners whose queues use lanes'
		})
}

/**
 * A worker pool's runners share one history, so any of them reports all its jobs, and
 * one maintenance loop.
 */
export function createWorker<const TQueues extends readonly QueueContract[]>(
	options: BetterWorkerOptions<TQueues>,
	completedJobs: CompletedJobsStore,
	runnerName?: string,
	maintenanceLoop: Maintenance = createMaintenance(),
	/** A pool runner's queues; a single worker runs them all. */
	processed?: readonly string[]
): BetterWorker<TQueues> {
	assertUniqueQueueNames(options.queues)
	const registered = normalizeWorkerQueues(options.queues, options.handlers)
	const queues = selectProcessed(registered, processed)
	const failureHooks = new Map(
		registered.map((queue) => [queue.name, queue.onPermanentFailure])
	)
	const otel = options.otel ? options.otel.api : getOtel()
	assertInteger(options.concurrency ?? DEFAULT_CONCURRENCY, 'concurrency', 1)
	assertInteger(
		options.pollInterval ?? DEFAULT_POLL_INTERVAL,
		'pollInterval',
		1,
		2147483647
	)
	const defaultTimeout =
		options.defaultTimeout === false
			? undefined
			: (options.defaultTimeout ?? DEFAULT_TIMEOUT)
	if (defaultTimeout !== undefined)
		assertInteger(defaultTimeout, 'defaultTimeout', 1, 2147483647)
	const prefetch = options.prefetch ?? 0
	assertInteger(prefetch, 'prefetch', 0)
	const shutdownGrace =
		options.shutdownGrace === false
			? undefined
			: (options.shutdownGrace ?? DEFAULT_SHUTDOWN_GRACE)
	if (options.secondLookOnNotify !== undefined)
		assertInteger(
			options.secondLookOnNotify,
			'secondLookOnNotify',
			0,
			2147483647
		)
	if (shutdownGrace !== undefined)
		assertInteger(shutdownGrace, 'shutdownGrace', 0, 2147483647)

	const crashRecovery = resolveCrashRecovery(options.crashRecovery)
	const restart = resolveRestart(options.restart)

	const schema = options.schema ?? DEFAULT_GRAPHILE_WORKER_SCHEMA
	const hooks = withQueueMetrics(
		options.hooks ?? {},
		options.otel?.metrics ? options.otel.api : null
	)
	const client = createWorkerClient({
		pgPool: options.pgPool,
		schema
	})
	const ready = options.autoMigrate ? () => client.ensureMigrated() : undefined
	/** Admin reads and writes run after `autoMigrate`'s migration. */
	const admin = <A extends unknown[], R>(fn: (...args: A) => Promise<R>) =>
		ready
			? async (...args: A) => {
					await ready()
					return fn(...args)
				}
			: fn
	const producer = bindCreateJob<TQueues>({
		enqueue: client.enqueue,
		otel,
		queues: options.queues,
		schema,
		...compact({
			hooks,
			validateOnEnqueue: options.validateOnEnqueue,
			defaultMaxAttempts: options.defaultMaxAttempts,
			payloadMode: options.payloadMode,
			transactionalJobKeyMode: options.transactionalJobKeyMode,
			ready
		})
	})
	const { createJob, createJobs, prepareJob, prepareJobs } = producer
	const jobs = createJobsApi(createJob, options.queues)
	const logger = createStartupLogger(hooks)
	const outcomeRetention = resolveOutcomeRetention(options.jobOutcomes)
	let maintenance: ReturnType<Maintenance['register']> | undefined
	const running = new Map<string, RunningJob>()
	/** Report jobs that failed for good without a handler finishing them. */
	async function reportFailed(
		jobs: readonly ReleasedJob[],
		reason: PermanentFailureReason
	) {
		for (const job of jobs) {
			const init = job.task.endsWith(CRON_INIT_SUFFIX)
			await reportPermanentFailure(
				hooks,
				init ? undefined : failureHooks.get(job.task),
				{
					reason,
					error: new Error(job.lastError),
					queue: job.task,
					jobId: job.id,
					operation: init ? 'init' : 'process',
					attempts: job.attempts,
					maxAttempts: job.maxAttempts,
					payload: extractProducerLink(job.payload).cleanPayload
				}
			)
		}
	}
	const runtime: TaskListRuntime<TQueues> = {
		hooks,
		completedJobs,
		createJob: producer.enqueueOne,
		createJobs: producer.enqueueMany,
		logger,
		schema,
		otel,
		permanentFailure: options.permanentFailure,
		defaultTimeout,
		jobOutcomes: outcomeRetention !== undefined,
		heartbeatReady: () => maintenance?.ready,
		queues: options.queues,
		prepareJob: producer.prepareOne,
		prepareJobs: producer.prepareMany,
		running
	}

	/**
	 * Create this library's tables if they are missing, so enabling an option cannot fail
	 * every job. False when the role cannot and nothing asked for them explicitly: the
	 * default must not break a locked-down role that upgraded without migrate().
	 */
	async function prepareTables() {
		if (await client.hasTables()) return true
		try {
			await client.migrate()
			return true
		} catch (error) {
			const cannotCreate = (error as { code?: string }).code === '42501'
			const required =
				options.crashRecovery !== undefined || outcomeRetention !== undefined
			if (required || !cannotCreate) throw error
			logger.warn('worker.maintenance.disabled', {
				reason:
					"This library's tables are missing and this role cannot create them; run migrate()"
			})
			return false
		}
	}
	const maintenanceOptions = {
		pgPool: options.pgPool,
		schema,
		logger,
		recovery: crashRecovery,
		outcomeRetention,
		onExhausted: (jobs: ReleasedJob[]) => reportFailed(jobs, 'worker_lost')
	}

	const lifecycle = createLifecycle({
		start: async (restarting) => {
			if (!restarting) {
				if (runnerName === undefined) assertPoolSize(options.pgPool, 1)
				logger.info('Starting graphile-worker')
				logRegisteredQueues(queues, logger, defaultTimeout)
				warnPrefetchWithLanes(queues, prefetch, logger, runnerName)
			}
			const maintained = await prepareTables()
			return startRunner({
				pgPool: options.pgPool,
				schema,
				taskList: buildTaskList(queues, runtime),
				cronItems: buildCronItems(queues, options.defaultMaxAttempts),
				concurrency: options.concurrency ?? DEFAULT_CONCURRENCY,
				pollInterval: options.pollInterval ?? DEFAULT_POLL_INTERVAL,
				noHandleSignals: true,
				graphile: options.graphile,
				secondLookOnNotify: options.secondLookOnNotify,
				prefetch: options.prefetch,
				runnerName,
				shutdownGrace,
				onNudge: (nudge) => {
					runtime.nudge = nudge
				},
				onForcedShutdown: () => reportShutdownFailures(runtime),
				maintain: maintained
					? (workerId) =>
							(maintenance = maintenanceLoop.register(
								workerId,
								maintenanceOptions
							))
					: undefined
			})
		},
		release: () => client.release(),
		onError: (error) =>
			logger.error('worker.lifecycle.failed', { error: String(error) }),
		handleSignals: options.handleSignals,
		restart: restart && {
			...restart,
			onStopped: (error, next) => {
				logger.error('worker.runner.stopped', {
					runner: runnerName,
					error: String(error),
					restart_attempt: next.attempt,
					restart_in_ms: next.delay
				})
				// Not awaited: the restart's backoff must not wait on app code.
				void observe(hooks.onRunnerStopped, {
					runner: runnerName,
					error,
					restartAttempt: next.attempt,
					restartInMs: next.delay
				})
			},
			onRestartFailed: (error, next) =>
				logger.warn('worker.runner.restart_failed', {
					runner: runnerName,
					error: String(error),
					restart_attempt: next.attempt,
					restart_in_ms: next.delay
				}),
			onRestarted: (attempt) =>
				logger.info('worker.runner.restarted', {
					runner: runnerName,
					restart_attempt: attempt
				})
		}
	})

	return {
		schema,
		queues: options.queues,
		createJob,
		createJobs,
		prepareJob,
		prepareJobs,
		enqueueInTransaction: producer.enqueueInTransaction,
		jobs,
		enqueueByName: producer.enqueueByName,
		get promise() {
			return lifecycle.promise
		},
		migrate: () => client.migrate(),
		start: () => lifecycle.start(),
		stop(stopOptions) {
			const probe = resolveDatabaseProbe(stopOptions?.probeDatabase)
			const lifecycleStop = lifecycle.stop(stopOptions?.timeout)
			const stopping = probe
				? whileReachable(lifecycleStop, options.pgPool, probe)
				: lifecycleStop
			return stopOptions?.endPool
				? thenEndPool(stopping, options.pgPool)
				: stopping
		},
		waitUntilStopped: () => lifecycle.waitUntilStopped(),
		runOnce: () =>
			lifecycle.runOnce(async () => {
				const maintained = await prepareTables()
				await runOnceTasks({
					pgPool: options.pgPool,
					schema,
					taskList: buildTaskList(queues, runtime),
					noHandleSignals: true,
					graphile: options.graphile
				})
				if (maintained) await maintainOnce(maintenanceOptions)
			}),
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
		getJobStats: admin(async () => {
			const rows = await queryJobCounts(client, schema)
			const taskIdentifiers = options.queues.flatMap((queue) =>
				getQueueType(queue) === 'cron-init'
					? [queue.name, `${queue.name}${CRON_INIT_SUFFIX}`]
					: [queue.name]
			)
			return mergeJobStats(rows, completedJobs.getStats(), taskIdentifiers)
		}),
		listJobs: admin((listOptions?: ListJobsOptions) =>
			queryRecentJobs(client, schema, listOptions)
		),
		getJob: admin((id: string) => queryJob(client, schema, id)),
		getJobByKey: admin(async (key: string) => {
			const [job] = await queryRecentJobs(client, schema, {
				key,
				includePayload: true,
				limit: 1
			})
			return job ?? null
		}),
		removeJob: admin((key: string) => removeJobByKey(client, schema, key)),
		releaseLocks: admin(async (releaseOptions?: ReleaseLocksOptions) => {
			const released = await client.withPgClient((pgClient) =>
				releaseLocks(
					pgClient,
					schema,
					crashRecovery?.staleAfter ?? resolveCrashRecovery(true)!.staleAfter,
					releaseOptions
				)
			)
			await reportFailed(
				released.filter((job) => job.attempts >= job.maxAttempts),
				'worker_lost'
			)
			return released.map((job) => job.id)
		}),
		deleteFailedJobs: admin((olderThan: Duration) =>
			deleteFailedJobs(client, schema, parseDuration(olderThan, 'olderThan'))
		),
		getJobProgress: admin((queue: string, jobId: string) =>
			queryJobProgress(client, schema, options.queues, queue, jobId)
		) as GetJobProgressFn<TQueues>,
		retryJobs: admin((ids: string[]) => client.retryJobs(ids)),
		failJobs: admin(async (ids: string[], reason?: string) => {
			const failed = await client.withPgClient((pgClient) =>
				permanentlyFailJobs(pgClient, schema, ids, reason)
			)
			await reportFailed(failed, 'failJobs')
			return failed.map((job) => job.id)
		}),
		getQueueDefinitions() {
			return getQueueDefinitions(
				options.queues,
				options.defaultMaxAttempts,
				defaultTimeout
			)
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
