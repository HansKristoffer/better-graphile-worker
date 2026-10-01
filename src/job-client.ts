import { compact } from './options.js'
import type { Pool } from 'pg'
import type { WorkerUtils } from 'graphile-worker'
import type { QueueContract } from './queue.js'
import type { BetterWorkerHooks } from './hooks.js'
import { bindCreateJob, createJobsApi } from './create-job.js'
import { createWorkerClient, DEFAULT_GRAPHILE_WORKER_SCHEMA } from './client.js'
import { assertUniqueQueueNames } from './define-queues.js'
import { getOtel, type OtelApi } from './otel.js'
import type { CreateJobFn, CreateJobsFn, JobsApi } from './types.js'

export type JobClientOptions<TQueues extends readonly QueueContract[]> = {
	pgPool: Pool
	queues: TQueues
	schema?: string
	hooks?: Pick<BetterWorkerHooks, 'onEnqueueFail' | 'shouldSkipEnqueue'>
	validateOnEnqueue?: boolean
	defaultMaxAttempts?: number
	otel?: { api: OtelApi | null }
}

export type JobClient<TQueues extends readonly QueueContract[]> = {
	readonly schema: string
	readonly queues: TQueues
	readonly createJob: CreateJobFn<TQueues>
	readonly createJobs: CreateJobsFn<TQueues>
	readonly jobs: JobsApi<TQueues>
	migrate(): Promise<void>
	release(): Promise<void>
	getWorkerUtils(): Promise<WorkerUtils>
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
	const { createJob, createJobs } = bindCreateJob<TQueues>({
		enqueue: client.enqueue,
		otel: options.otel ? options.otel.api : getOtel(),
		queues: options.queues,
		...compact({
			hooks: options.hooks,
			validateOnEnqueue: options.validateOnEnqueue,
			defaultMaxAttempts: options.defaultMaxAttempts
		})
	})

	return {
		schema,
		queues: options.queues,
		createJob,
		createJobs,
		jobs: createJobsApi(createJob, options.queues),
		migrate() {
			return client.migrate()
		},
		release() {
			return client.release()
		},
		getWorkerUtils() {
			return client.getUtils()
		}
	}
}
