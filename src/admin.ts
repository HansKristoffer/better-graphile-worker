import { payloadDebugJoin } from './private-jobs.js'
import { assertInteger } from './validation.js'
import type { WorkerUtils } from 'graphile-worker'
import type z from 'zod'
import type { QueueContract } from './queue.js'
import type { CompletedJobStats } from './completed-jobs-store.js'
import { formatCronSchedule, getQueueType, hasInputSchema } from './queue.js'
import { DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS } from './create-job.js'
import { quoteSchemaName } from './schema-name.js'
import { extractProducerLink } from './payload.js'

export type QueueDefinition = {
	name: string
	type: 'regular' | 'cron' | 'cron-init'
	cron: string | null
	serial: boolean | string | null
	maxAttempts: number
	priority: number | null
	hasInputSchema: boolean
	inputSchema: z.ZodType | null
}

export type JobCountRow = {
	taskIdentifier: string
	pending: number
	running: number
	failed: number
}

export type WorkerJobStatsRow = JobCountRow & {
	/** Completed entries in this instance’s bounded history. */
	recentCompleted: number
	/** Failed entries in this instance’s bounded history; separate from database failures. */
	recentFailed: number
}

export type ListedJob = {
	id: string
	/** Preserve PostgreSQL timestamp precision when paginating. */
	cursor: { createdAt: string; id: string }
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
	/** Defaults to false. Opt in to the private-table payload debugging join. */
	includePayload?: boolean
	before?: { createdAt: string; id: string }
}

const DEFAULT_LIST_LIMIT = 100
const MAX_LIST_LIMIT = 1000

export function getQueueDefinitions(
	queues: readonly QueueContract[],
	defaultMaxAttempts = DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS
): QueueDefinition[] {
	return queues.map((queue) => ({
		name: queue.name,
		type: getQueueType(queue),
		cron: formatCronSchedule(queue.cron),
		serial: queue.serial ?? null,
		maxAttempts: queue.maxAttempts ?? defaultMaxAttempts,
		priority: queue.priority ?? null,
		hasInputSchema: hasInputSchema(queue),
		inputSchema: hasInputSchema(queue) ? queue.inputSchema : null
	}))
}

export function mergeJobStats(
	rows: readonly JobCountRow[],
	ring: CompletedJobStats,
	taskIdentifiers: readonly string[] = []
): WorkerJobStatsRow[] {
	const names = new Set<string>([
		...taskIdentifiers,
		...rows.map((row) => row.taskIdentifier),
		...Object.keys(ring)
	])
	const byTask = new Map(rows.map((row) => [row.taskIdentifier, row]))

	return [...names].map((taskIdentifier) => {
		const pg = byTask.get(taskIdentifier)
		const mem = ring[taskIdentifier]
		return {
			taskIdentifier,
			pending: pg?.pending ?? 0,
			running: pg?.running ?? 0,
			recentCompleted: mem?.completed ?? 0,
			failed: pg?.failed ?? 0,
			recentFailed: mem?.failed ?? 0
		}
	})
}

export async function queryJobCounts(
	utils: Pick<WorkerUtils, 'withPgClient'>,
	schema: string
): Promise<JobCountRow[]> {
	const safeSchema = quoteSchemaName(schema)
	const result = await utils.withPgClient(async (pgClient) => {
		return pgClient.query<{
			task_identifier: string
			pending: string
			running: string
			failed: string
		}>(`
			SELECT
				jobs.task_identifier,
				COUNT(*) FILTER (
					WHERE jobs.locked_at IS NULL AND jobs.attempts < jobs.max_attempts
				) as pending,
				COUNT(*) FILTER (WHERE jobs.locked_at IS NOT NULL) as running,
				COUNT(*) FILTER (
					WHERE jobs.locked_at IS NULL AND jobs.attempts >= jobs.max_attempts
				) as failed
			FROM ${safeSchema}.jobs jobs
			GROUP BY jobs.task_identifier
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
	utils: Pick<WorkerUtils, 'withPgClient'>,
	schema: string,
	options: ListJobsOptions = {}
): Promise<ListedJob[]> {
	const safeSchema = quoteSchemaName(schema)
	const limit = options.limit ?? DEFAULT_LIST_LIMIT
	const offset = options.offset ?? 0
	assertInteger(limit, 'limit', 0, MAX_LIST_LIMIT)
	assertInteger(offset, 'offset', 0)
	if (
		options.state !== undefined &&
		!['pending', 'running', 'failed'].includes(options.state)
	)
		throw new RangeError('Invalid job state')
	if (options.before && options.offset !== undefined)
		throw new RangeError('before and offset cannot be combined')
	if (
		options.before &&
		(!Number.isFinite(Date.parse(options.before.createdAt)) ||
			!/^[1-9][0-9]*$/.test(options.before.id))
	)
		throw new RangeError('Invalid job cursor')

	const debug =
		options.includePayload === true
			? payloadDebugJoin(safeSchema)
			: { column: 'NULL::json', join: '' }
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
			cursor_created_at: string
			locked_at: Date | null
			locked_by: string | null
			last_error: string | null
		}>(
			`
			SELECT
				jobs.id,
				jobs.task_identifier,
				${debug.column} as payload,
				jobs.priority,
				jobs.attempts,
				jobs.max_attempts,
				jobs.run_at,
				jobs.created_at,
				to_char(jobs.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as cursor_created_at,
				jobs.locked_at,
				jobs.locked_by,
				jobs.last_error
			FROM ${safeSchema}.jobs jobs
			${debug.join}
			WHERE
				($2::text IS NULL OR jobs.task_identifier = $2)
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
			AND ($5::timestamptz IS NULL OR (jobs.created_at, jobs.id) < ($5::timestamptz, $6::bigint))
			ORDER BY jobs.created_at DESC, jobs.id DESC
			LIMIT $1 OFFSET $4
		`,
			[
				limit,
				queue,
				state,
				offset,
				options.before?.createdAt ?? null,
				options.before?.id ?? null
			]
		)
	})

	return result.rows.map((row) => ({
		id: row.id,
		cursor: { createdAt: row.cursor_created_at, id: row.id },
		queueName: row.task_identifier,
		payload:
			options.includePayload === true
				? extractProducerLink(row.payload).cleanPayload
				: null,
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
