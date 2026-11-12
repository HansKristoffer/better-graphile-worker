import { compact } from './options.js'
import type { Pool } from 'pg'
import type { WorkerUtils } from 'graphile-worker'
import type { QueueContract } from './queue.js'
import type { BetterWorkerHooks } from './hooks.js'
import {
	bindCreateJob,
	createJobsApi,
	type JobKeyMode,
	type PayloadMode
} from './create-job.js'
import { createWorkerClient, DEFAULT_GRAPHILE_WORKER_SCHEMA } from './client.js'
import { assertUniqueQueueNames } from './define-queues.js'
import { getOtel, type OtelOption } from './otel.js'
import { withQueueMetrics } from './metrics.js'
import { queryJobProgress, queryRecentJobs, type ListedJob } from './admin.js'
import { queryJob, type JobState } from './job-outcomes.js'
import type { JobOptions } from './job-options.js'
import type {
	CreateJobFn,
	CreateJobsFn,
	EnqueueInTransactionFn,
	GetJobProgressFn,
	JobsApi,
	PrepareJobFn,
	PrepareJobRequestFn,
	PrepareJobsFn
} from './types.js'

export type JobClientOptions<TQueues extends readonly QueueContract[]> = {
	pgPool: Pool
	queues: TQueues
	schema?: string
	hooks?: Pick<BetterWorkerHooks, 'onEnqueueFail' | 'shouldSkipEnqueue'>
	validateOnEnqueue?: boolean
	defaultMaxAttempts?: number
	/** `metrics: true` counts failed enqueues through the same API's meter. */
	otel?: OtelOption
	/** Match the workers' `payloadMode`. */
	payloadMode?: PayloadMode
	/** `jobKeyMode` for `enqueueInTransaction` jobs with a `jobKey` but no mode. */
	transactionalJobKeyMode?: JobKeyMode
	/** Run `migrate()` once before the first enqueue, prepare or read. Off by default. */
	autoMigrate?: boolean
}

export type JobClient<TQueues extends readonly QueueContract[]> = {
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
	/** Read a job's `ctx.progress` value; null once the job no longer exists. */
	readonly getJobProgress: GetJobProgressFn<TQueues>
	/** The live job with this `jobKey`, payload included; null when there is none. */
	getJobByKey(key: string): Promise<ListedJob | null>
	migrate(): Promise<void>
	release(): Promise<void>
	getWorkerUtils(): Promise<WorkerUtils>
	/** Live state, or the recorded outcome if a worker has `jobOutcomes` enabled. */
	getJob(id: string): Promise<JobState>
}

export function createJobClient<const TQueues extends readonly QueueContract[]>(
	options: JobClientOptions<TQueues>
): JobClient<TQueues> {
	assertUniqueQueueNames(options.queues)

	const schema = options.schema ?? DEFAULT_GRAPHILE_WORKER_SCHEMA
	const client = createWorkerClient({
		pgPool: options.pgPool,
		schema
	})
	const ready = options.autoMigrate ? () => client.ensureMigrated() : undefined
	const {
		createJob,
		createJobs,
		prepareJob,
		prepareJobs,
		enqueueInTransaction,
		enqueueByName
	} = bindCreateJob<TQueues>({
		enqueue: client.enqueue,
		otel: options.otel ? options.otel.api : getOtel(),
		queues: options.queues,
		schema,
		hooks: withQueueMetrics(
			options.hooks ?? {},
			options.otel?.metrics ? options.otel.api : null
		),
		...compact({
			validateOnEnqueue: options.validateOnEnqueue,
			defaultMaxAttempts: options.defaultMaxAttempts,
			payloadMode: options.payloadMode,
			transactionalJobKeyMode: options.transactionalJobKeyMode,
			ready
		})
	})

	return {
		schema,
		queues: options.queues,
		createJob,
		createJobs,
		prepareJob,
		prepareJobs,
		enqueueInTransaction,
		jobs: createJobsApi(createJob, options.queues),
		enqueueByName,
		getJobProgress: (async (queue: string, jobId: string) => {
			await ready?.()
			return queryJobProgress(client, schema, options.queues, queue, jobId)
		}) as GetJobProgressFn<TQueues>,
		async getJobByKey(key) {
			await ready?.()
			const [job] = await queryRecentJobs(client, schema, {
				key,
				includePayload: true,
				limit: 1
			})
			return job ?? null
		},
		migrate() {
			return client.migrate()
		},
		release() {
			return client.release()
		},
		getWorkerUtils() {
			return client.getUtils()
		},
		async getJob(id) {
			await ready?.()
			return queryJob(client, schema, id)
		}
	}
}
