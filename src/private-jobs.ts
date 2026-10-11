/** Version-specific SQL for Graphile Worker 0.17.x; keep private-table access here. */
import type { JobHelpers, WorkerUtils } from 'graphile-worker'
import { addJobSql } from './enqueue-sql.js'
import { StepPersistenceError } from './errors.js'
import { compact } from './options.js'
import { quoteSchemaName } from './schema-name.js'
export async function retainPermanentFailure(
	helpers: JobHelpers,
	schema: string
): Promise<void> {
	const result = await helpers.withPgClient((client) =>
		client.query(
			`UPDATE ${quoteSchemaName(schema)}._private_jobs SET max_attempts = attempts WHERE id = $1::bigint AND locked_by = $2::text AND locked_at IS NOT NULL`,
			[helpers.job.id, helpers.job.locked_by]
		)
	)
	if (result.rowCount !== 1)
		throw new StepPersistenceError(String(helpers.job.id))
	// Throwing the original error lets Graphile persist last_error and release its lock.
}

/** Patch the locked job's envelope; `$1` is the job id, `$2` the lock owner. */
async function patchEnvelope(
	helpers: JobHelpers,
	schema: string,
	jobId: string,
	patched: string,
	values: unknown[]
): Promise<void> {
	const quoted = quoteSchemaName(schema)
	const result = await helpers.withPgClient((client) =>
		client.query(
			`WITH owned AS (
		SELECT id, payload::jsonb AS data FROM ${quoted}._private_jobs
		WHERE id = $1::bigint AND locked_by = $2::text AND locked_at IS NOT NULL FOR UPDATE
	) UPDATE ${quoted}._private_jobs jobs SET payload = ${patched}, updated_at = now()
		FROM owned WHERE jobs.id = owned.id AND owned.data->>'__bgw' = '2' AND owned.data ? 'payload'`,
			[jobId, helpers.job.locked_by, ...values]
		)
	)
	if (result.rowCount !== 1) throw new StepPersistenceError(jobId)
}

export function writeCheckpoint(
	helpers: JobHelpers,
	schema: string,
	jobId: string,
	id: string,
	checkpoint: unknown
): Promise<void> {
	return patchEnvelope(
		helpers,
		schema,
		jobId,
		`jsonb_set(owned.data, '{steps}', CASE WHEN jsonb_typeof(owned.data->'steps') = 'object' THEN owned.data->'steps' ELSE '{}'::jsonb END || jsonb_build_object($3::text, $4::jsonb), true)`,
		[id, JSON.stringify(checkpoint)]
	)
}

export function writeProgress(
	helpers: JobHelpers,
	schema: string,
	jobId: string,
	value: unknown
): Promise<void> {
	return patchEnvelope(
		helpers,
		schema,
		jobId,
		`jsonb_set(owned.data, '{progress}', $3::jsonb, true)`,
		[JSON.stringify(value)]
	)
}

/** Null when no job of this task exists; completed jobs are deleted by Graphile. */
export async function readProgress(
	withPgClient: WorkerUtils['withPgClient'],
	schema: string,
	identifier: string,
	jobId: string
): Promise<{ progress: unknown } | null> {
	const quoted = quoteSchemaName(schema)
	const result = await withPgClient((client) =>
		client.query<{ progress: unknown; has_progress: boolean }>(
			`SELECT jobs.payload::jsonb->'progress' AS progress, jobs.payload::jsonb ? 'progress' AS has_progress
FROM ${quoted}._private_jobs jobs
JOIN ${quoted}._private_tasks tasks ON tasks.id = jobs.task_id
WHERE jobs.id = $1::bigint AND tasks.identifier = $2::text`,
			[jobId, identifier]
		)
	)
	const row = result.rows[0]
	if (!row) return null
	return { progress: row.has_progress ? row.progress : undefined }
}

export function payloadDebugJoin(quotedSchema: string): {
	column: string
	join: string
} {
	return {
		column: 'private_job.payload',
		join: `JOIN ${quotedSchema}._private_jobs private_job ON private_job.id = jobs.id`
	}
}

/**
 * Enqueue the same job (lane, key, priority, flags, max_attempts, steps, progress) and
 * stop the current one from retrying, in one transaction. Returns the continuation's id.
 * `progress` carries the latest in-memory value, which may not be written yet.
 */
export async function continueJob(
	helpers: JobHelpers,
	schema: string,
	runAt: Date | undefined,
	traceparent: string | undefined,
	progress?: unknown
): Promise<string> {
	const quoted = quoteSchemaName(schema)
	// pg decodes locked_at to Date, which only retains milliseconds.
	const params = [helpers.job.id, helpers.job.locked_by, helpers.job.locked_at]
	return helpers.withPgClient(async (client) => {
		await client.query('BEGIN')
		try {
			const current = await client.query<{
				identifier: string
				payload: Record<string, unknown>
				queue_name: string | null
				key: string | null
				priority: number
				max_attempts: number
				flags: string[] | null
			}>(
				`SELECT tasks.identifier, jobs.payload, queues.queue_name, jobs.key, jobs.priority, jobs.max_attempts,
	(SELECT array_agg(flag) FROM jsonb_object_keys(jobs.flags) AS flag) AS flags
FROM ${quoted}._private_jobs jobs
JOIN ${quoted}._private_tasks tasks ON tasks.id = jobs.task_id
LEFT JOIN ${quoted}._private_job_queues queues ON queues.id = jobs.job_queue_id
WHERE jobs.id = $1::bigint AND jobs.locked_by = $2::text AND jobs.locked_at IS NOT NULL
AND date_trunc('milliseconds', jobs.locked_at) = $3::timestamptz
FOR UPDATE OF jobs`,
				params
			)
			const job = current.rows[0]
			if (!job) throw new StepPersistenceError(String(helpers.job.id))
			// Same logical job: keep input and steps; a continuation is not a cron fire.
			const { _cron, traceparent: _previous, ...envelope } = job.payload
			if (progress !== undefined) envelope.progress = progress
			const { text, values } = addJobSql(
				schema,
				job.identifier,
				traceparent ? { ...envelope, traceparent } : envelope,
				compact({
					queueName: job.queue_name ?? undefined,
					runAt,
					maxAttempts: job.max_attempts,
					jobKey: job.key ?? undefined,
					priority: job.priority,
					flags: job.flags ?? undefined,
					jobKeyMode: 'replace' as const
				})
			)
			const added = await client.query<{ id: string }>(text, values)
			const id = added.rows[0]?.id
			if (!id) throw new Error('Graphile add_job returned no continuation')
			// Keyed jobs are already exhausted by add_job; this covers unkeyed ones.
			await client.query(
				`UPDATE ${quoted}._private_jobs SET max_attempts = attempts WHERE id = $1::bigint AND locked_by = $2::text`,
				params.slice(0, 2)
			)
			await client.query('COMMIT')
			return id
		} catch (error) {
			await client.query('ROLLBACK').catch(() => {})
			throw error
		}
	})
}
