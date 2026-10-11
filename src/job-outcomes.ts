import type { JobHelpers, WorkerUtils } from 'graphile-worker'
import type { Pool } from 'pg'
import { quoteSchemaName } from './schema-name.js'
import { assertInteger } from './validation.js'

/** Off by default. `retention` is in milliseconds and defaults to 7 days. */
export type JobOutcomesOption = false | { retention?: number }

type JobFields = {
	id: string
	/** Graphile task identifier; cron-init runs use `{name}_cron-init`. */
	queue: string
	attempts: number
	maxAttempts: number
	createdAt: string
	lastError: string | null
}

/**
 * A job's current state. Live jobs come from Graphile's jobs view; finished jobs come
 * from the outcome table while `jobOutcomes` retains them.
 */
export type JobState =
	| { state: 'not_found'; id: string }
	| (JobFields & { state: 'pending'; runAt: string })
	| (JobFields & { state: 'running'; runAt: string; lockedAt: string })
	/** Failed an attempt and waits for `runAt` to retry. */
	| (JobFields & { state: 'retrying'; runAt: string })
	/** Permanently failed: attempts exhausted, `NonRetriableError`, or `failJobs`. */
	| (JobFields & { state: 'failed'; finishedAt: string })
	| (JobFields & { state: 'completed'; finishedAt: string })
	/** Ended with `ctx.continue()`; `continuedTo` is the next run's job id. */
	| (JobFields & {
			state: 'continued'
			finishedAt: string
			continuedTo: string
	  })

export const JOB_OUTCOMES_TABLE = 'bgw_job_outcomes'
const DEFAULT_RETENTION = 7 * 24 * 60 * 60 * 1000
const PRUNE_INTERVAL = 60_000

export type JobOutcomes = {
	/** Deletes expired outcomes at most once per interval; never throws. */
	prune(helpers: JobHelpers): Promise<void>
}

export function resolveJobOutcomes(
	option: JobOutcomesOption | undefined,
	schema: string,
	onError: (error: unknown) => void
): JobOutcomes | undefined {
	if (option === false || option === undefined) return undefined
	const retention = option.retention ?? DEFAULT_RETENTION
	assertInteger(retention, 'jobOutcomes.retention', 1)
	const table = `${quoteSchemaName(schema)}.${JOB_OUTCOMES_TABLE}`
	let lastPrune = 0
	return {
		async prune(helpers) {
			const now = Date.now()
			if (now - lastPrune < Math.min(retention, PRUNE_INTERVAL)) return
			lastPrune = now
			// ponytail: one unbounded delete per interval; batch it if a backlog ever locks too long.
			await helpers
				.withPgClient((client) =>
					client.query(
						`DELETE FROM ${table} WHERE job_id IN (
	SELECT job_id FROM ${table}
	WHERE finished_at < now() - make_interval(secs => $1::double precision / 1000)
	FOR UPDATE SKIP LOCKED
)`,
						[retention]
					)
				)
				.catch(onError)
		}
	}
}

export async function migrateJobOutcomes(
	pgPool: Pool,
	schema: string
): Promise<void> {
	const quoted = quoteSchemaName(schema)
	const exists = await pgPool.query<{ exists: boolean }>(
		'SELECT to_regclass($1) IS NOT NULL AS exists',
		[`${quoted}.${JOB_OUTCOMES_TABLE}`]
	)
	if (exists.rows[0]?.exists) return
	const client = await pgPool.connect()
	try {
		await client.query('BEGIN')
		// Concurrent CREATE ... IF NOT EXISTS can still collide on the catalog.
		await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [
			`${schema}.${JOB_OUTCOMES_TABLE}`
		])
		await client.query(`CREATE TABLE IF NOT EXISTS ${quoted}.${JOB_OUTCOMES_TABLE} (
	job_id bigint PRIMARY KEY,
	task_identifier text NOT NULL,
	state text NOT NULL CHECK (state IN ('completed', 'failed', 'continued')),
	continued_to bigint,
	attempts integer NOT NULL,
	max_attempts integer NOT NULL,
	last_error text,
	created_at timestamptz NOT NULL,
	finished_at timestamptz NOT NULL DEFAULT now()
)`)
		await client.query(
			`CREATE INDEX IF NOT EXISTS ${JOB_OUTCOMES_TABLE}_finished_at ON ${quoted}.${JOB_OUTCOMES_TABLE} (finished_at)`
		)
		await client.query('COMMIT')
	} catch (error) {
		await client.query('ROLLBACK').catch(() => {})
		throw error
	} finally {
		client.release()
	}
}

type LiveRow = {
	id: string
	task_identifier: string
	attempts: number
	max_attempts: number
	created_at: Date
	last_error: string | null
	run_at: Date
	updated_at: Date
	locked_at: Date | null
}

type OutcomeRow = Omit<LiveRow, 'run_at' | 'updated_at' | 'locked_at'> & {
	state: 'completed' | 'failed' | 'continued'
	continued_to: string | null
	finished_at: Date
}

export async function queryJob(
	utils: Pick<WorkerUtils, 'withPgClient'>,
	schema: string,
	id: string
): Promise<JobState> {
	if (typeof id !== 'string' || !/^[1-9][0-9]*$/.test(id))
		throw new RangeError('Invalid job id')
	const quoted = quoteSchemaName(schema)
	return utils.withPgClient(async (client) => {
		const live = await client.query<LiveRow>(
			`SELECT id::text, task_identifier, attempts, max_attempts, created_at, last_error, run_at, updated_at, locked_at
FROM ${quoted}.jobs WHERE id = $1::bigint`,
			[id]
		)
		if (live.rows[0]) return liveState(live.rows[0])
		// Completed and continued outcomes commit with the job's deletion, so no lookup falls between the two.
		let outcome: OutcomeRow | undefined
		try {
			const result = await client.query<OutcomeRow>(
				`SELECT job_id::text AS id, task_identifier, state, continued_to::text, attempts, max_attempts, last_error, created_at, finished_at
FROM ${quoted}.${JOB_OUTCOMES_TABLE} WHERE job_id = $1::bigint`,
				[id]
			)
			outcome = result.rows[0]
		} catch (error) {
			// undefined_table: outcomes were never enabled for this schema.
			if ((error as { code?: unknown }).code !== '42P01') throw error
		}
		return outcome ? outcomeState(outcome) : { state: 'not_found', id }
	})
}

function fields(row: LiveRow | OutcomeRow): JobFields {
	return {
		id: row.id,
		queue: row.task_identifier,
		attempts: row.attempts,
		maxAttempts: row.max_attempts,
		createdAt: row.created_at.toISOString(),
		lastError: row.last_error
	}
}

function liveState(row: LiveRow): JobState {
	const base = fields(row)
	const runAt = row.run_at.toISOString()
	if (row.locked_at)
		return {
			...base,
			state: 'running',
			runAt,
			lockedAt: row.locked_at.toISOString()
		}
	if (row.attempts >= row.max_attempts)
		return {
			...base,
			state: 'failed',
			finishedAt: row.updated_at.toISOString()
		}
	return { ...base, state: row.attempts > 0 ? 'retrying' : 'pending', runAt }
}

function outcomeState(row: OutcomeRow): JobState {
	const base = fields(row)
	const finishedAt = row.finished_at.toISOString()
	if (row.state === 'continued')
		return {
			...base,
			state: 'continued',
			finishedAt,
			continuedTo: row.continued_to!
		}
	return { ...base, state: row.state, finishedAt }
}
