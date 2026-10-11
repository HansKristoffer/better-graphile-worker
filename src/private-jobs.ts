/** Version-specific SQL for Graphile Worker 0.17.x; keep private-table access here. */
import type { JobHelpers, WorkerUtils } from 'graphile-worker'
import type { PoolClient } from 'pg'
import { addJobSql } from './enqueue-sql.js'
import { StepPersistenceError } from './errors.js'
import { JOB_OUTCOMES_TABLE } from './job-outcomes.js'
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

/**
 * Upserts the locked job's outcome. With `finish`, also completes the job the way
 * Graphile's complete_jobs does (delete it, release its lane) in the same statement;
 * Graphile's own completion then finds nothing to delete. Returns false without a lock.
 */
async function writeOutcome(
	client: Pick<PoolClient, 'query'>,
	helpers: JobHelpers,
	schema: string,
	outcome: {
		state: 'completed' | 'failed' | 'continued'
		error?: string | undefined
		continuedTo?: string
		finish: boolean
	}
): Promise<boolean> {
	const quoted = quoteSchemaName(schema)
	const finishSql = outcome.finish
		? `, deleted AS (
	DELETE FROM ${quoted}._private_jobs jobs USING owned WHERE jobs.id = owned.id RETURNING jobs.job_queue_id
), unlocked AS (
	UPDATE ${quoted}._private_job_queues queues SET locked_by = NULL, locked_at = NULL
	FROM deleted WHERE queues.id = deleted.job_queue_id AND queues.locked_by = $2::text
)`
		: ''
	const result = await client.query<{ recorded: number }>(
		`WITH owned AS (
	SELECT jobs.id, tasks.identifier, jobs.attempts, jobs.max_attempts, jobs.created_at
	FROM ${quoted}._private_jobs jobs
	JOIN ${quoted}._private_tasks tasks ON tasks.id = jobs.task_id
	WHERE jobs.id = $1::bigint AND jobs.locked_by = $2::text AND jobs.locked_at IS NOT NULL
	FOR UPDATE OF jobs
), recorded AS (
	INSERT INTO ${quoted}.${JOB_OUTCOMES_TABLE}
		(job_id, task_identifier, state, continued_to, attempts, max_attempts, last_error, created_at)
	SELECT id, identifier, $3::text, $5::bigint, attempts, max_attempts, $4::text, created_at FROM owned
	ON CONFLICT (job_id) DO UPDATE SET state = excluded.state, continued_to = excluded.continued_to,
		attempts = excluded.attempts, max_attempts = excluded.max_attempts,
		last_error = excluded.last_error, finished_at = now()
	RETURNING job_id
)${finishSql}
SELECT count(*)::int AS recorded FROM recorded`,
		[
			helpers.job.id,
			helpers.job.locked_by,
			outcome.state,
			outcome.error ?? null,
			outcome.continuedTo ?? null
		]
	)
	return result.rows[0]?.recorded === 1
}

export function recordJobOutcome(
	helpers: JobHelpers,
	schema: string,
	outcome: {
		state: 'completed' | 'failed'
		error?: string | undefined
		finish: boolean
	}
): Promise<boolean> {
	return helpers.withPgClient((client) =>
		writeOutcome(client, helpers, schema, outcome)
	)
}

/**
 * Patch the locked job's envelope; `$1` is the job id, `$2` the lock owner and `$3` the
 * lock time, which tells this attempt apart from a later one by the same worker pool.
 */
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
		WHERE id = $1::bigint AND locked_by = $2::text AND date_trunc('milliseconds', locked_at) = $3::timestamptz FOR UPDATE
	) UPDATE ${quoted}._private_jobs jobs SET payload = ${patched}, updated_at = now()
		FROM owned WHERE jobs.id = owned.id AND owned.data->>'__bgw' = '2' AND owned.data ? 'payload'`,
			[jobId, helpers.job.locked_by, helpers.job.locked_at, ...values]
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
		`jsonb_set(owned.data, '{steps}', CASE WHEN jsonb_typeof(owned.data->'steps') = 'object' THEN owned.data->'steps' ELSE '{}'::jsonb END || jsonb_build_object($4::text, $5::jsonb), true)`,
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
		`jsonb_set(owned.data, '{progress}', $4::jsonb, true)`,
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
	recordOutcome = false,
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
			if (
				recordOutcome &&
				!(await writeOutcome(client, helpers, schema, {
					state: 'continued',
					continuedTo: id,
					finish: true
				}))
			)
				throw new StepPersistenceError(String(helpers.job.id))
			await client.query('COMMIT')
			return id
		} catch (error) {
			await client.query('ROLLBACK').catch(() => {})
			throw error
		}
	})
}

/**
 * Release jobs and lanes locked by dead worker pools, like Graphile's failJobs: record
 * the error and back off. The attempt taken when the job was locked stays spent, so a
 * job that keeps killing its worker exhausts max_attempts and fails permanently.
 */
export async function releaseJobsLockedBy(
	client: PoolClient,
	schema: string,
	workerIds: readonly string[]
): Promise<string[]> {
	const quoted = quoteSchemaName(schema)
	const result = await client.query<{ id: string }>(
		`WITH jobs AS (
	UPDATE ${quoted}._private_jobs SET
		last_error = 'Worker ' || locked_by || ' stopped sending heartbeats (crashed or blocked); released by crash recovery',
		run_at = greatest(now(), run_at) + exp(least(attempts, 10)) * interval '1 second',
		locked_by = NULL, locked_at = NULL
	WHERE locked_by = ANY($1::text[])
	RETURNING id
), queues AS (
	UPDATE ${quoted}._private_job_queues SET locked_by = NULL, locked_at = NULL
	WHERE locked_by = ANY($1::text[])
)
SELECT id::text FROM jobs`,
		[workerIds]
	)
	return result.rows.map((row) => row.id)
}
