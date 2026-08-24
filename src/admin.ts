import type { WorkerUtils } from 'graphile-worker'
import type { QueueAny } from './create-queue'
import {
	formatCronSchedule,
	getQueueType,
	hasInputSchema
} from './create-queue'
import { DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS } from './create-job'
import { assertValidSchemaName } from './schema-name'

/**
 * Queries Graphile Worker's internal `_private_jobs` / `_private_tasks` tables.
 * These names are not a public API and may change between graphile-worker versions.
 */
export type QueueDefinition = {
	name: string
	type: 'regular' | 'cron' | 'cron-init'
	cron: string | null
	serial: boolean | string | null
	maxAttempts: number
	priority: number | null
	hasInputSchema: boolean
}

export type JobCountRow = {
	taskIdentifier: string
	pending: number
	running: number
	failed: number
}

export type ListedJob = {
	id: string
	queueName: string
	payload: unknown
	priority: number
	attempts: number
	maxAttempts: number
	runAt: string
	createdAt: string
	lockedAt: string | null
	lockedBy: string | null
	lastError: string | null
}

export type JobListState = 'pending' | 'running' | 'failed'

export type ListJobsOptions = {
	limit?: number
	offset?: number
	queue?: string
	state?: JobListState
}

const DEFAULT_LIST_LIMIT = 100
const MAX_LIST_LIMIT = 1000

export function getQueueDefinitions(
	queues: readonly QueueAny[]
): QueueDefinition[] {
	return queues.map((queue) => ({
		name: queue.name,
		type: getQueueType(queue),
		cron: formatCronSchedule(queue.cron),
		serial: queue.serial ?? null,
		maxAttempts: queue.maxAttempts ?? DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS,
		priority: queue.priority ?? null,
		hasInputSchema: hasInputSchema(queue)
	}))
}

export async function queryJobCounts(
	utils: WorkerUtils,
	schema: string
): Promise<JobCountRow[]> {
	const safeSchema = assertValidSchemaName(schema)
	const result = await utils.withPgClient(async (pgClient) => {
		return pgClient.query<{
			task_identifier: string
			pending: string
			running: string
			failed: string
		}>(`
			SELECT
				tasks.identifier as task_identifier,
				COUNT(*) FILTER (
					WHERE jobs.locked_at IS NULL AND jobs.attempts < jobs.max_attempts
				) as pending,
				COUNT(*) FILTER (WHERE jobs.locked_at IS NOT NULL) as running,
				COUNT(*) FILTER (
					WHERE jobs.locked_at IS NULL AND jobs.attempts >= jobs.max_attempts
				) as failed
			FROM ${safeSchema}._private_jobs jobs
			INNER JOIN ${safeSchema}._private_tasks tasks ON tasks.id = jobs.task_id
			GROUP BY tasks.identifier
		`)
	})

	return result.rows.map((row) => ({
		taskIdentifier: row.task_identifier,
		pending: Number.parseInt(row.pending, 10),
		running: Number.parseInt(row.running, 10),
		failed: Number.parseInt(row.failed, 10)
	}))
}

export async function queryRecentJobs(
	utils: WorkerUtils,
	schema: string,
	options: ListJobsOptions = {}
): Promise<ListedJob[]> {
	const safeSchema = assertValidSchemaName(schema)
	const limit = Math.min(
		Math.max(options.limit ?? DEFAULT_LIST_LIMIT, 0),
		MAX_LIST_LIMIT
	)
	const offset = Math.max(options.offset ?? 0, 0)
	const queue = options.queue ?? null
	const state = options.state ?? null

	const result = await utils.withPgClient(async (pgClient) => {
		return pgClient.query<{
			id: string
			task_identifier: string
			payload: unknown
			priority: number
			attempts: number
			max_attempts: number
			run_at: Date
			created_at: Date
			locked_at: Date | null
			locked_by: string | null
			last_error: string | null
		}>(
			`
			SELECT
				jobs.id,
				tasks.identifier as task_identifier,
				jobs.payload,
				jobs.priority,
				jobs.attempts,
				jobs.max_attempts,
				jobs.run_at,
				jobs.created_at,
				jobs.locked_at,
				jobs.locked_by,
				jobs.last_error
			FROM ${safeSchema}._private_jobs jobs
			INNER JOIN ${safeSchema}._private_tasks tasks ON tasks.id = jobs.task_id
			WHERE
				($2::text IS NULL OR tasks.identifier = $2)
				AND (
					$3::text IS NULL
					OR (
						$3 = 'pending'
						AND jobs.locked_at IS NULL
						AND jobs.attempts < jobs.max_attempts
					)
					OR (
						$3 = 'running'
						AND jobs.locked_at IS NOT NULL
					)
					OR (
						$3 = 'failed'
						AND jobs.locked_at IS NULL
						AND jobs.attempts >= jobs.max_attempts
					)
				)
			ORDER BY jobs.created_at DESC
			LIMIT $1 OFFSET $4
		`,
			[limit, queue, state, offset]
		)
	})

	return result.rows.map((row) => ({
		id: row.id,
		queueName: row.task_identifier,
		payload: row.payload,
		priority: row.priority,
		attempts: row.attempts,
		maxAttempts: row.max_attempts,
		runAt: row.run_at.toISOString(),
		createdAt: row.created_at.toISOString(),
		lockedAt: row.locked_at?.toISOString() ?? null,
		lockedBy: row.locked_by,
		lastError: row.last_error
	}))
}
