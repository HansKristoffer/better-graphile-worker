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
import { queryJobProgress } from './admin.js'
import { queryJob, type JobState } from './job-outcomes.js'
import type {
	CreateJobFn,
	CreateJobsFn,
	EnqueueInTransactionFn,
	GetJobProgressFn,
	JobsApi,
	PrepareJobFn,
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
}

export type JobClient<TQueues extends readonly QueueContract[]> = {
	readonly schema: string
	readonly queues: TQueues
	readonly createJob: CreateJobFn<TQueues>
	readonly createJobs: CreateJobsFn<TQueues>
	/** Build enqueue SQL to execute inside your own transaction. */
	readonly prepareJob: PrepareJobFn<TQueues>
	readonly prepareJobs: PrepareJobsFn<TQueues>
	/** Prepare one job and run it in a Prisma transaction, so it commits with the transaction. */
	readonly enqueueInTransaction: EnqueueInTransactionFn<TQueues>
	readonly jobs: JobsApi<TQueues>
	/** Read a job's `ctx.progress` value; null once the job no longer exists. */
	readonly getJobProgress: GetJobProgressFn<TQueues>
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
	const {
		createJob,
		createJobs,
		prepareJob,
		prepareJobs,
		enqueueInTransaction
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
			transactionalJobKeyMode: options.transactionalJobKeyMode
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
		getJobProgress: ((queue: string, jobId: string) =>
			queryJobProgress(
				client,
				schema,
				options.queues,
				queue,
				jobId
			)) as GetJobProgressFn<TQueues>,
		migrate() {
			return client.migrate()
		},
		release() {
			return client.release()
		},
		getWorkerUtils() {
			return client.getUtils()
		},
		getJob(id) {
			return queryJob(client, schema, id)
		}
	}
}
