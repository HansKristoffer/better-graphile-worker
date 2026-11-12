import { payloadDebugJoin, readProgress } from './private-jobs.js'
import { UnknownQueueError } from './errors.js'
import type { JobProgressSnapshot } from './progress.js'
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
	/** Attempt timeout in milliseconds, including the worker default; null when unlimited. */
	timeout: number | null
	/** The worker pool runner that executes the queue, including the pool's `defaultRunner`. */
	runner: string | null
	priority: number | null
	hasInputSchema: boolean
	inputSchema: z.ZodType | null
}

export type JobCountRow = {
	/** The Graphile task identifier; cron-init runs use `{name}_cron-init`. */
	queue: string
	/** @deprecated Use `queue`. */
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
	/** The Graphile task identifier; cron-init runs use `{name}_cron-init`. */
	queue: string
	/** @deprecated Use `queue`; job options use `queueName` for the lane. */
	queueName: string
	/** The serial lane (Graphile's job queue), if any. */
	lane: string | null
	key: string | null
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
	/** Only the job with this `jobKey`. */
	key?: string
	/** Defaults to false. Opt in to the private-table payload debugging join. */
	includePayload?: boolean
	before?: { createdAt: string; id: string }
}

export async function queryJobProgress(
	utils: Pick<WorkerUtils, 'withPgClient'>,
	schema: string,
	queues: readonly QueueContract[],
	queue: string,
	jobId: string
): Promise<JobProgressSnapshot | null> {
	if (!queues.some((item) => item.name === queue))
		throw new UnknownQueueError(
			queue,
			queues.map((item) => item.name)
		)
	if (typeof jobId !== 'string' || !/^[1-9][0-9]{0,18}$/.test(jobId))
		throw new RangeError('Invalid job id')
	return readProgress(utils.withPgClient, schema, queue, jobId)
}

const DEFAULT_LIST_LIMIT = 100
const MAX_LIST_LIMIT = 1000

export function getQueueDefinitions(
	queues: readonly QueueContract[],
	defaultMaxAttempts = DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS,
	defaultTimeout?: number
): QueueDefinition[] {
	return queues.map((queue) => ({
		name: queue.name,
		type: getQueueType(queue),
		cron: formatCronSchedule(queue.cron),
		serial: queue.serial ?? null,
		maxAttempts: queue.maxAttempts ?? defaultMaxAttempts,
		timeout: queue.timeout ?? defaultTimeout ?? null,
		runner: queue.runner ?? null,
		priority: queue.priority ?? null,
		hasInputSchema: hasInputSchema(queue),
		inputSchema: hasInputSchema(queue) ? queue.inputSchema : null
	}))
}

export function mergeJobStats(
	rows: readonly Omit<JobCountRow, 'queue'>[],
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
			queue: taskIdentifier,
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
		queue: row.task_identifier,
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
	if (
		options.key !== undefined &&
		(typeof options.key !== 'string' || !options.key)
	)
		throw new RangeError('Invalid job key')

	const result = await utils.withPgClient(async (pgClient) => {
		return pgClient.query<{
			id: string
			task_identifier: string
			queue_name: string | null
			key: string | null
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
				jobs.queue_name,
				jobs.key,
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
			AND ($7::text IS NULL OR jobs.key = $7)
			ORDER BY jobs.created_at DESC, jobs.id DESC
			LIMIT $1 OFFSET $4
		`,
			[
				limit,
				queue,
				state,
				offset,
				options.before?.createdAt ?? null,
				options.before?.id ?? null,
				options.key ?? null
			]
		)
	})

	return result.rows.map((row) => ({
		id: row.id,
		cursor: { createdAt: row.cursor_created_at, id: row.id },
		queue: row.task_identifier,
		queueName: row.task_identifier,
		lane: row.queue_name,
		key: row.key,
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

/**
 * Delete permanently failed jobs last updated longer ago than `olderThan` milliseconds,
 * through Graphile's `complete_jobs`, which skips locked jobs. Returns their ids.
 */
export async function deleteFailedJobs(
	utils: Pick<WorkerUtils, 'withPgClient'>,
	schema: string,
	olderThan: number
): Promise<string[]> {
	const quoted = quoteSchemaName(schema)
	const result = await utils.withPgClient((client) =>
		client.query<{ id: string }>(
			`SELECT id::text FROM ${quoted}.complete_jobs(ARRAY(
	SELECT id FROM ${quoted}.jobs
	WHERE attempts >= max_attempts AND locked_at IS NULL
		AND updated_at < now() - $1::float8 * interval '1 millisecond'
))`,
			[olderThan]
		)
	)
	return result.rows.map((row) => row.id)
}

/** Graphile's `remove_job`: the removed (or, when running, disarmed) job's id. */
export async function removeJobByKey(
	utils: Pick<WorkerUtils, 'withPgClient'>,
	schema: string,
	key: string
): Promise<string | null> {
	const result = await utils.withPgClient((client) =>
		client.query<{ id: string | null }>(
			`SELECT id::text FROM ${quoteSchemaName(schema)}.remove_job($1::text)`,
			[key]
		)
	)
	return result.rows[0]?.id ?? null
}
