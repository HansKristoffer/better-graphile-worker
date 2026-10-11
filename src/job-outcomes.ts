import type { WorkerUtils } from 'graphile-worker'
import type { PoolClient } from 'pg'
import { quoteSchemaName } from './schema-name.js'
import { assertInteger } from './validation.js'
import { WAIT_GROUPS_TABLE, WAITS_TABLE } from './waits.js'

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
	/** Suspended at `ctx.step.triggerAndWait` until these child jobs finish. */
	| (JobFields & { state: 'waiting'; waitingOn: string[] })
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

/** The retention in milliseconds, or undefined when outcomes are off. */
export function resolveOutcomeRetention(
	option: JobOutcomesOption | undefined
): number | undefined {
	if (option === false || option === undefined) return undefined
	const retention = option.retention ?? DEFAULT_RETENTION
	assertInteger(retention, 'jobOutcomes.retention', 1)
	return retention
}

export function outcomeTablesSql(schema: string): string {
	const table = `${quoteSchemaName(schema)}.${JOB_OUTCOMES_TABLE}`
	return `CREATE TABLE IF NOT EXISTS ${table} (
	job_id bigint PRIMARY KEY,
	task_identifier text NOT NULL,
	state text NOT NULL CHECK (state IN ('completed', 'failed', 'continued')),
	continued_to bigint CHECK ((state = 'continued') = (continued_to IS NOT NULL)),
	attempts integer NOT NULL,
	max_attempts integer NOT NULL,
	last_error text,
	created_at timestamptz NOT NULL,
	finished_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ${JOB_OUTCOMES_TABLE}_finished_at ON ${table} (finished_at);`
}

// ponytail: one unbounded delete per maintenance pass; batch it if a backlog ever locks too long.
export async function pruneJobOutcomes(
	client: Pick<PoolClient, 'query'>,
	schema: string,
	retention: number
): Promise<void> {
	const table = `${quoteSchemaName(schema)}.${JOB_OUTCOMES_TABLE}`
	await client.query(
		`DELETE FROM ${table} WHERE job_id IN (
	SELECT job_id FROM ${table}
	WHERE finished_at < now() - make_interval(secs => $1::double precision / 1000)
	FOR UPDATE SKIP LOCKED
)`,
		[retention]
	)
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
	scheduled: boolean
}

type OutcomeRow = Omit<
	LiveRow,
	'run_at' | 'updated_at' | 'locked_at' | 'scheduled'
> & {
	state: 'completed' | 'failed' | 'continued'
	continued_to: string | null
	finished_at: Date
}

export async function queryJob(
	utils: Pick<WorkerUtils, 'withPgClient'>,
	schema: string,
	id: string
): Promise<JobState> {
	if (!/^[1-9][0-9]*$/.test(id)) throw new RangeError('Invalid job id')
	const quoted = quoteSchemaName(schema)
	return utils.withPgClient(async (client) => {
		const live = await client.query<LiveRow>(
			`SELECT id::text, task_identifier, attempts, max_attempts, created_at, last_error, run_at, updated_at, locked_at,
	run_at > now() AS scheduled
FROM ${quoted}.jobs WHERE id = $1::bigint`,
			[id]
		)
		const row = live.rows[0]
		if (row) {
			const state = liveState(row)
			if (state.state !== 'pending' && state.state !== 'retrying') return state
			if (!row.scheduled) return state
			// A suspended parent is not runnable until its children wake it.
			const children = await client.query<{ id: string }>(
				`SELECT waits.child_job_id::text AS id FROM ${quoted}.${WAITS_TABLE} waits
JOIN ${quoted}.${WAIT_GROUPS_TABLE} groups USING (parent_job_id, wait_id)
WHERE waits.parent_job_id = $1::bigint AND waits.result IS NULL AND groups.remaining > 0
ORDER BY groups.created_at, waits.idx`,
				[id]
			)
			const waitingOn = children.rows.map((child) => child.id)
			if (!waitingOn.length) return state
			const { runAt: _runAt, ...fields } = state
			return { ...fields, state: 'waiting', waitingOn }
		}
		// Completed and continued outcomes commit with the job's deletion, so no lookup falls between the two.
		const outcomes = await client.query<OutcomeRow>(
			`SELECT job_id::text AS id, task_identifier, state, continued_to::text, attempts, max_attempts, last_error, created_at, finished_at
FROM ${quoted}.${JOB_OUTCOMES_TABLE} WHERE job_id = $1::bigint`,
			[id]
		)
		const outcome = outcomes.rows[0]
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
