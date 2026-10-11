/** Version-specific SQL for Graphile Worker 0.17.x; keep private-table access here. */
import type { JobHelpers, WorkerUtils } from 'graphile-worker'
import type { PoolClient } from 'pg'
import { addJobSql } from './enqueue-sql.js'
import { StepPersistenceError } from './errors.js'
import { JOB_OUTCOMES_TABLE } from './job-outcomes.js'
import { compact } from './options.js'
import { quoteSchemaName } from './schema-name.js'
import type { PreparedJob } from './types.js'
import {
	WAIT_GROUPS_TABLE,
	WAITS_TABLE,
	type ChildReport,
	type WaitCheckpoint
} from './waits.js'
/**
 * The locked job as this attempt owns it: `$1` id, `$2` lock owner, `$3` lock time.
 * Graphile 0.17 sets `locked_by` to the worker pool's id, which every worker in the
 * process shares, so only the lock time tells this attempt apart from a later one.
 */
function owned(alias?: string): string {
	const column = (name: string) => (alias ? `${alias}.${name}` : name)
	return `${column('id')} = $1::bigint AND ${column('locked_by')} = $2::text AND date_trunc('milliseconds', ${column('locked_at')}) = $3::timestamptz`
}

/** Values for `owned()`; pg decodes locked_at to a Date, which keeps milliseconds only. */
function ownerValues(helpers: JobHelpers): unknown[] {
	return [helpers.job.id, helpers.job.locked_by, helpers.job.locked_at]
}

/** The delay Graphile 0.17's failure update adds to run_at, for a number of attempts. */
function graphileBackoff(attempts: string): string {
	return `exp(least(${attempts}, 10)) * interval '1 second'`
}

/**
 * Graphile's failure update sets `run_at = greatest(now(), run_at) + exp(least(attempts, 10))s`.
 * Shifting run_at back by that backoff while we hold the lock makes the retry land on
 * `runAt`, or on the default backoff when that is later. Returns the expected retry time.
 */
export async function scheduleRetry(
	helpers: JobHelpers,
	schema: string,
	runAt: Date
): Promise<Date> {
	const backoff = graphileBackoff('attempts')
	const result = await helpers.withPgClient((client) =>
		client.query<{ retry_at: Date }>(
			`UPDATE ${quoteSchemaName(schema)}._private_jobs SET run_at = $4::timestamptz - ${backoff} WHERE ${owned()} RETURNING greatest(now(), run_at) + ${backoff} AS retry_at`,
			[...ownerValues(helpers), runAt]
		)
	)
	const row = result.rows[0]
	if (!row) throw new StepPersistenceError(String(helpers.job.id))
	// Throwing the original error lets Graphile record it, spend the attempt and unlock.
	return row.retry_at
}

/**
 * Settle the locked job in one statement: upsert its outcome, report its final state to
 * waiting parents (waking a parent whose last child this was), and handle its `row`:
 * - `delete` completes it the way Graphile's complete_jobs does (delete it, release its
 *   lane), so Graphile's own completion finds nothing to delete;
 * - `exhaust` spends its attempts, so the error Graphile then records is permanent;
 * - `keep` leaves it for Graphile's failure update.
 * Returns false without the attempt's lock.
 */
async function settleJob(
	client: Pick<PoolClient, 'query'>,
	helpers: JobHelpers,
	schema: string,
	settle: {
		outcome?:
			| {
					state: 'completed' | 'failed' | 'continued'
					error?: string | undefined
					continuedTo?: string
			  }
			| undefined
		report?: ChildReport | undefined
		row: 'delete' | 'exhaust' | 'keep'
	}
): Promise<boolean> {
	const quoted = quoteSchemaName(schema)
	const values = ownerValues(helpers)
	const param = (value: unknown) => `$${values.push(value)}`
	const ctes = [
		`owned AS (
	SELECT jobs.id, tasks.identifier, jobs.attempts, jobs.max_attempts, jobs.created_at
	FROM ${quoted}._private_jobs jobs
	JOIN ${quoted}._private_tasks tasks ON tasks.id = jobs.task_id
	WHERE ${owned('jobs')}
	FOR UPDATE OF jobs
)`
	]
	const { outcome, report, row } = settle
	if (outcome)
		ctes.push(`recorded AS (
	INSERT INTO ${quoted}.${JOB_OUTCOMES_TABLE}
		(job_id, task_identifier, state, continued_to, attempts, max_attempts, last_error, created_at)
	SELECT id, identifier, ${param(outcome.state)}::text, ${param(outcome.continuedTo ?? null)}::bigint,
		attempts, max_attempts, ${param(outcome.error ?? null)}::text, created_at FROM owned
	ON CONFLICT (job_id) DO UPDATE SET state = excluded.state, continued_to = excluded.continued_to,
		attempts = excluded.attempts, max_attempts = excluded.max_attempts,
		last_error = excluded.last_error, finished_at = now()
)`)
	if (report)
		ctes.push(
			`reported AS (
	UPDATE ${quoted}.${WAITS_TABLE} waits SET result = ${param(JSON.stringify(report))}::jsonb, finished_at = now()
	FROM owned WHERE waits.child_job_id = owned.id AND waits.result IS NULL
	RETURNING waits.parent_job_id, waits.wait_id
)`,
			...wakeParents(quoted)
		)
	if (row === 'exhaust')
		ctes.push(`exhausted AS (
	UPDATE ${quoted}._private_jobs jobs SET max_attempts = jobs.attempts FROM owned WHERE jobs.id = owned.id
)`)
	if (row === 'delete')
		ctes.push(`deleted AS (
	DELETE FROM ${quoted}._private_jobs jobs USING owned WHERE jobs.id = owned.id RETURNING jobs.job_queue_id
), unlocked AS (
	UPDATE ${quoted}._private_job_queues queues SET locked_by = NULL, locked_at = NULL
	FROM deleted WHERE queues.id = deleted.job_queue_id AND queues.locked_by = $2::text
)`)
	const result = await client.query<{ owned: number }>(
		`WITH ${ctes.join(', ')}
SELECT count(*)::int AS owned FROM owned`,
		values
	)
	return result.rows[0]?.owned === 1
}

/**
 * Count `reported` waits against their groups and wake each parent whose group has no
 * children left. Updating the group row waits for a parent that is suspending on it.
 */
function wakeParents(quoted: string): string[] {
	return [
		`counted AS (
	UPDATE ${quoted}.${WAIT_GROUPS_TABLE} groups SET remaining = groups.remaining - reported.n
	FROM (SELECT parent_job_id, wait_id, count(*)::int AS n FROM reported GROUP BY 1, 2) reported
	WHERE groups.parent_job_id = reported.parent_job_id AND groups.wait_id = reported.wait_id
	RETURNING groups.parent_job_id, groups.remaining
)`,
		// Match on id only: a filter on run_at would read the parent's row from this
		// statement's snapshot, from before a suspend that committed while we waited.
		`woken AS (
	UPDATE ${quoted}._private_jobs parents SET run_at = least(parents.run_at, now())
	FROM counted WHERE parents.id = counted.parent_job_id AND counted.remaining = 0
)`
	]
}

export function settleLockedJob(
	helpers: JobHelpers,
	schema: string,
	settle: Parameters<typeof settleJob>[3]
): Promise<boolean> {
	return helpers.withPgClient((client) =>
		settleJob(client, helpers, schema, settle)
	)
}

/** Patch the locked job's envelope while this attempt owns it. */
async function patchEnvelope(
	helpers: JobHelpers,
	schema: string,
	jobId: string,
	patched: string,
	values: unknown[],
	client?: Pick<PoolClient, 'query'>
): Promise<void> {
	const quoted = quoteSchemaName(schema)
	const run = (client: Pick<PoolClient, 'query'>) =>
		client.query(
			`WITH owned AS (
		SELECT id, payload::jsonb AS data FROM ${quoted}._private_jobs
		WHERE ${owned()} FOR UPDATE
	) UPDATE ${quoted}._private_jobs jobs SET payload = ${patched}, updated_at = now()
		FROM owned WHERE jobs.id = owned.id AND owned.data->>'__bgw' = '2' AND owned.data ? 'payload'`,
			[jobId, helpers.job.locked_by, helpers.job.locked_at, ...values]
		)
	const result = await (client ? run(client) : helpers.withPgClient(run))
	if (result.rowCount !== 1) throw new StepPersistenceError(jobId)
}

export function writeCheckpoint(
	helpers: JobHelpers,
	schema: string,
	jobId: string,
	id: string,
	checkpoint: unknown,
	client?: Pick<PoolClient, 'query'>
): Promise<void> {
	return patchEnvelope(
		helpers,
		schema,
		jobId,
		`jsonb_set(owned.data, '{steps}', CASE WHEN jsonb_typeof(owned.data->'steps') = 'object' THEN owned.data->'steps' ELSE '{}'::jsonb END || jsonb_build_object($4::text, $5::jsonb), true)`,
		[id, JSON.stringify(checkpoint)],
		client
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
	const params = ownerValues(helpers)
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
WHERE ${owned('jobs')}
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
				`UPDATE ${quoted}._private_jobs SET max_attempts = attempts WHERE ${owned()}`,
				params
			)
			// Waiting parents follow the job to its continuation.
			if (job.payload.waited === true)
				await client.query(
					`UPDATE ${quoted}.${WAITS_TABLE} SET child_job_id = $2::bigint WHERE child_job_id = $1::bigint AND result IS NULL`,
					[helpers.job.id, id]
				)
			if (
				recordOutcome &&
				!(await settleJob(client, helpers, schema, {
					outcome: { state: 'continued', continuedTo: id },
					row: 'delete'
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
		run_at = greatest(now(), run_at) + ${graphileBackoff('attempts')},
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

/** Where a suspended job's run_at points when its wait has no timeout. */
const NO_DEADLINE = `'9999-01-01T00:00:00Z'::timestamptz`

/**
 * One wait of the locked job, in one transaction. The first call enqueues the children
 * with their group and wait rows, so a crash cannot leave children without a group; a
 * replay finds the group and enqueues nothing. Then, under the group row's lock: when
 * every child reported or the deadline passed, the results are checkpointed in the
 * envelope and the group is deleted, so a group without children left exists only
 * while its parent must wake. Otherwise the job suspends: it keeps the lock and sets
 * the row up for Graphile's failure update, which unlocks the job and its lane without
 * deleting it. The attempt is given back and `run_at` is moved back by the backoff
 * Graphile adds (as in `scheduleRetry`), so it lands on the deadline.
 * `suspended` is set when an earlier wait of this run already suspended it.
 */
export async function enterWait(
	helpers: JobHelpers,
	schema: string,
	wait: {
		waitId: string
		count: number
		timeout: number | undefined
		suspended: boolean
		prepare: () => Promise<PreparedJob>
	}
) {
	const quoted = quoteSchemaName(schema)
	const groups = `${quoted}.${WAIT_GROUPS_TABLE}`
	const key = [helpers.job.id, wait.waitId]
	return helpers.withPgClient(async (client) => {
		await client.query('BEGIN')
		try {
			const own = await client.query(
				`SELECT 1 FROM ${quoted}._private_jobs WHERE ${owned()} FOR UPDATE`,
				ownerValues(helpers)
			)
			if (own.rowCount !== 1)
				throw new StepPersistenceError(String(helpers.job.id))

			const existing = await client.query<{
				remaining: number
				expired: boolean
			}>(
				`SELECT remaining, coalesce(deadline <= now(), false) AS expired FROM ${groups}
	WHERE parent_job_id = $1::bigint AND wait_id = $2::text FOR UPDATE`,
				key
			)
			let group = existing.rows[0]
			if (!group) {
				const prepared = await wait.prepare()
				if (prepared.skipped)
					throw new Error(
						`shouldSkipEnqueue skipped the children of wait "${wait.waitId}"`
					)
				await client.query(
					`INSERT INTO ${groups} (parent_job_id, wait_id, remaining, deadline)
	VALUES ($1::bigint, $2::text, $3::int, now() + $4::float8 * interval '1 millisecond')`,
					[...key, wait.count, wait.timeout ?? null]
				)
				const added = await client.query<{ id: string | null }>(
					prepared.text,
					prepared.values
				)
				const ids = added.rows.map((row) => row.id)
				if (ids.length !== wait.count || ids.some((id) => !id))
					throw new Error(
						`Wait "${wait.waitId}" enqueued ${ids.length} of ${wait.count} children; a jobKey may have been claimed concurrently`
					)
				await client.query(
					`INSERT INTO ${quoted}.${WAITS_TABLE} (parent_job_id, wait_id, idx, child_job_id)
	SELECT $1::bigint, $2::text, child.idx - 1, child.id FROM unnest($3::bigint[]) WITH ORDINALITY AS child(id, idx)`,
					[...key, ids]
				)
				// Their workers report a final state only for jobs carrying this mark.
				await client.query(
					`UPDATE ${quoted}._private_jobs SET payload = (payload::jsonb || '{"waited": true}'::jsonb)::json
	WHERE id = ANY($1::bigint[]) AND json_typeof(payload) = 'object'`,
					[ids]
				)
				group = { remaining: wait.count, expired: false }
			}

			if (group.remaining === 0 || group.expired) {
				const checkpoint = await finishWait(
					client,
					helpers,
					schema,
					wait.waitId,
					group.remaining === 0
				)
				await client.query('COMMIT')
				return { state: 'finished' as const, checkpoint }
			}

			// Suspend: give the attempt back once per run and aim at the deadline.
			await client.query(
				`UPDATE ${quoted}._private_jobs jobs SET
	attempts = jobs.attempts - $3::int,
	run_at = least(
		CASE WHEN $3::int = 1 THEN 'infinity'::timestamptz ELSE jobs.run_at END,
		coalesce(groups.deadline, ${NO_DEADLINE}) - ${graphileBackoff('jobs.attempts - $3::int')}
	)
FROM ${groups} groups
WHERE jobs.id = $1::bigint AND groups.parent_job_id = $1::bigint AND groups.wait_id = $2::text`,
				[...key, wait.suspended ? 0 : 1]
			)
			await client.query('COMMIT')
			return { state: 'suspended' as const }
		} catch (error) {
			await client.query('ROLLBACK').catch(() => {})
			if ((error as { code?: unknown }).code === '42P01')
				throw new Error(
					`ctx.step.triggerAndWait needs the ${WAIT_GROUPS_TABLE} and ${WAITS_TABLE} tables; run migrate()`,
					{ cause: error }
				)
			throw error
		}
	})
}

/**
 * Checkpoint a wait's results in the locked job's envelope, or the unfinished children
 * after a timeout, and delete its group. Children still running after a timeout then
 * have no wait to report to.
 */
async function finishWait(
	client: Pick<PoolClient, 'query'>,
	helpers: JobHelpers,
	schema: string,
	waitId: string,
	complete: boolean
) {
	const quoted = quoteSchemaName(schema)
	const key = [helpers.job.id, waitId]
	const rows = await client.query<{ id: string; result: ChildReport | null }>(
		`SELECT child_job_id::text AS id, result FROM ${quoted}.${WAITS_TABLE}
	WHERE parent_job_id = $1::bigint AND wait_id = $2::text ORDER BY idx`,
		key
	)
	const checkpoint: WaitCheckpoint = complete
		? rows.rows.map((row) => ({ id: row.id, ...row.result! }))
		: {
				timedOut: rows.rows.filter((row) => !row.result).map((row) => row.id)
			}

	await writeCheckpoint(
		helpers,
		schema,
		String(helpers.job.id),
		waitId,
		{ output: checkpoint },
		client
	)
	await client.query(
		`DELETE FROM ${quoted}.${WAIT_GROUPS_TABLE} WHERE parent_job_id = $1::bigint AND wait_id = $2::text`,
		key
	)
	return checkpoint
}

/**
 * Fail waits whose child finished without reporting (released by crash recovery on its
 * last attempt, `failJobs`, deleted, or run by a worker that does not report), waking
 * their parents. SKIP LOCKED lets every worker sweep at once; a child that reports
 * deletes or finishes its row in the same statement, so a reported child never looks
 * lost. Then wake parents whose wakeup was swallowed and drop groups of parents that no
 * longer exist.
 */
export async function sweepWaits(
	client: Pick<PoolClient, 'query'>,
	schema: string
): Promise<string[]> {
	const quoted = quoteSchemaName(schema)
	const waits = `${quoted}.${WAITS_TABLE}`
	const lost = await client.query<{ child_job_id: string }>(
		`WITH lost AS (
	SELECT waits.parent_job_id, waits.wait_id, waits.idx, jsonb_build_object('ok', false, 'error', jsonb_build_object(
		'name', 'ChildJobLostError',
		'message', CASE WHEN jobs.id IS NULL
			THEN 'The child job finished without reporting a result (deleted, or run by a worker without wait support)'
			ELSE coalesce(jobs.last_error, 'The child job failed permanently') END
	)) AS result
	FROM ${waits} waits
	LEFT JOIN ${quoted}._private_jobs jobs ON jobs.id = waits.child_job_id
	WHERE waits.result IS NULL
	AND (jobs.id IS NULL OR (jobs.locked_at IS NULL AND jobs.attempts >= jobs.max_attempts))
	FOR UPDATE OF waits SKIP LOCKED
), reported AS (
	UPDATE ${waits} waits SET result = lost.result, finished_at = now() FROM lost
	WHERE waits.parent_job_id = lost.parent_job_id AND waits.wait_id = lost.wait_id
	AND waits.idx = lost.idx AND waits.result IS NULL
	RETURNING waits.parent_job_id, waits.wait_id, waits.child_job_id
), ${wakeParents(quoted).join(', ')}
SELECT child_job_id::text FROM reported`
	)
	// A child that reports before Graphile's batched failure update unlocks its suspending
	// parent has that update add its backoff to the wakeup (about a second at attempt 0).
	await client.query(
		`UPDATE ${quoted}._private_jobs jobs SET run_at = now()
FROM ${quoted}.${WAIT_GROUPS_TABLE} groups
WHERE groups.remaining = 0 AND jobs.id = groups.parent_job_id AND jobs.locked_at IS NULL
AND jobs.run_at > now() AND jobs.attempts < jobs.max_attempts`
	)
	// ponytail: one unbounded delete per sweep; batch it if orphaned groups ever pile up.
	await client.query(
		`DELETE FROM ${quoted}.${WAIT_GROUPS_TABLE} groups
WHERE NOT EXISTS (SELECT 1 FROM ${quoted}._private_jobs jobs WHERE jobs.id = groups.parent_job_id)`
	)
	return lost.rows.map((row) => row.child_job_id)
}
