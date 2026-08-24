import type { Pool } from 'pg'
import type { WorkerUtils } from 'graphile-worker'
import type { QueueAny } from './create-queue'
import type { BetterWorkerHooks } from './hooks'
import { bindCreateJob, createJobsApi } from './create-job'
import { createWorkerClient, DEFAULT_GRAPHILE_WORKER_SCHEMA } from './client'
import { assertUniqueQueueNames } from './define-queues'
import { setOtelApi, type OtelApi } from './otel'
import type { CreateJobFn, CreateJobsFn, JobsApi } from './types'

export type JobClientOptions<TQueues extends readonly QueueAny[]> = {
	pgPool: Pool
	queues: TQueues
	schema?: string
	hooks?: Pick<BetterWorkerHooks, 'onEnqueueFail' | 'shouldSkipEnqueue'>
	validateOnEnqueue?: boolean
	defaultMaxAttempts?: number
	otel?: { api: OtelApi | null }
}

export type JobClient<TQueues extends readonly QueueAny[]> = {
	readonly schema: string
	readonly queues: TQueues
	readonly createJob: CreateJobFn<TQueues>
	readonly createJobs: CreateJobsFn<TQueues>
	readonly jobs: JobsApi<TQueues>
	migrate(): Promise<void>
	release(): Promise<void>
	getWorkerUtils(): Promise<WorkerUtils>
}

export function createJobClient<TQueues extends readonly QueueAny[]>(
	options: JobClientOptions<TQueues>
): JobClient<TQueues> {
	assertUniqueQueueNames(options.queues)
	if (options.otel) {
		setOtelApi(options.otel.api)
	}

	const schema = options.schema ?? DEFAULT_GRAPHILE_WORKER_SCHEMA
	const client = createWorkerClient({
		pgPool: options.pgPool,
		schema
	})
	const { createJob, createJobs } = bindCreateJob<TQueues>({
		getWorkerUtils: () => client.getUtils(),
		queues: options.queues,
		hooks: options.hooks,
		validateOnEnqueue: options.validateOnEnqueue,
		defaultMaxAttempts: options.defaultMaxAttempts
	})

	return {
		schema,
		queues: options.queues,
		createJob,
		createJobs,
		jobs: createJobsApi(createJob),
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
