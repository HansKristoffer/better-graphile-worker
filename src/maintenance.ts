import type { Pool, PoolClient } from 'pg'
import { pruneIdempotencyKeys } from './enqueue-sql.js'
import type { JobLogger } from './hooks.js'
import { pruneJobOutcomes } from './job-outcomes.js'
import { releaseJobsLockedBy, sweepWaits } from './private-jobs.js'
import { quoteSchemaName } from './schema-name.js'
import { assertInteger } from './validation.js'

export type CrashRecoveryOptions = {
	/** How often each started worker records that it is alive, in ms. Default 30 seconds. */
	heartbeatInterval?: number
	/**
	 * How old a heartbeat must be before its worker counts as dead, in ms. Default 5
	 * minutes; at least three heartbeat intervals. A worker whose event loop is blocked
	 * for longer looks dead and its jobs can run twice.
	 */
	staleAfter?: number
}

export const HEARTBEATS_TABLE = 'bgw_worker_heartbeats'
/** How often maintenance runs without crash recovery, which sets its own interval. */
const MAINTENANCE_INTERVAL = 30_000
const MAX_TIMER = 2147483647

/** On unless explicitly disabled. */
export function resolveCrashRecovery(
	option: boolean | CrashRecoveryOptions | undefined
) {
	if (option === false) return undefined
	const { heartbeatInterval = 30_000, staleAfter = 300_000 } =
		option === true || option === undefined ? {} : option
	assertInteger(
		heartbeatInterval,
		'crashRecovery.heartbeatInterval',
		1,
		MAX_TIMER
	)
	assertInteger(staleAfter, 'crashRecovery.staleAfter', 3 * heartbeatInterval)
	return { heartbeatInterval, staleAfter }
}

export function heartbeatTableSql(schema: string): string {
	return `CREATE TABLE IF NOT EXISTS ${quoteSchemaName(schema)}.${HEARTBEATS_TABLE} (
	worker_id text PRIMARY KEY,
	last_seen_at timestamptz NOT NULL
);`
}

/**
 * Release jobs held by workers whose heartbeat is older than staleAfter. SKIP LOCKED
 * lets every live worker sweep without two of them releasing the same worker.
 * `sweeperId` is never released: a worker running this is alive, and a stall between
 * its own heartbeat and this sweep must not make it release its own jobs.
 */
export async function sweepDeadWorkers(
	client: PoolClient,
	schema: string,
	staleAfter: number,
	sweeperId?: string
) {
	const table = `${quoteSchemaName(schema)}.${HEARTBEATS_TABLE}`
	try {
		await client.query('BEGIN')
		const dead = await client.query<{ worker_id: string }>(
			`DELETE FROM ${table} WHERE worker_id IN (
	SELECT worker_id FROM ${table}
	WHERE last_seen_at < now() - $1::float8 * interval '1 millisecond'
		AND worker_id IS DISTINCT FROM $2
	FOR UPDATE SKIP LOCKED
) RETURNING worker_id`,
			[staleAfter, sweeperId ?? null]
		)
		const workerIds = dead.rows.map((row) => row.worker_id)
		const jobIds = workerIds.length
			? await releaseJobsLockedBy(client, schema, workerIds)
			: []
		await client.query('COMMIT')
		return { workerIds, jobIds }
	} catch (error) {
		await client.query('ROLLBACK').catch(() => {})
		throw error
	}
}

type MaintenanceOptions = {
	pgPool: Pool
	schema: string
	logger: JobLogger
	/** Crash recovery's heartbeat and dead-worker sweep; off when undefined. */
	recovery?: Required<CrashRecoveryOptions> | undefined
	/** Delete job outcomes older than this many milliseconds; off when undefined. */
	outcomeRetention?: number | undefined
}

/**
 * One maintenance pass: release jobs of dead workers, fail waits on children that ended
 * without reporting (such as those just released), and delete expired outcomes and
 * idempotency keys. Every step uses SKIP LOCKED, so any number of workers can run it at
 * once, and a failing step does not stop the others.
 */
async function sweep(
	options: MaintenanceOptions & { workerId?: string },
	withConnection: <T>(work: (client: PoolClient) => Promise<T>) => Promise<T>
) {
	const { schema, logger, recovery, outcomeRetention } = options
	const attempt = async (
		failedEvent: string,
		work: (client: PoolClient) => Promise<void>
	) => {
		try {
			await withConnection(work)
		} catch (error) {
			logger.warn(failedEvent, { error: String(error) })
		}
	}

	if (recovery)
		await attempt('worker.crash_recovery.sweep_failed', async (client) => {
			const swept = await sweepDeadWorkers(
				client,
				schema,
				recovery.staleAfter,
				options.workerId
			)
			// Stopped workers leave their row behind; only released jobs are news.
			if (swept.jobIds.length)
				logger.warn('worker.crash_recovery.released', {
					dead_worker_ids: swept.workerIds.join(','),
					job_ids: swept.jobIds.join(','),
					job_count: swept.jobIds.length
				})
		})
	await attempt('worker.waits.sweep_failed', async (client) => {
		const lost = await sweepWaits(client, schema)
		if (lost.length)
			logger.warn('worker.waits.lost_children', {
				job_ids: lost.join(','),
				job_count: lost.length
			})
	})
	if (outcomeRetention !== undefined)
		await attempt('job.outcome.prune_failed', (client) =>
			pruneJobOutcomes(client, schema, outcomeRetention)
		)
	await attempt('worker.idempotency.prune_failed', (client) =>
		pruneIdempotencyKeys(client, schema)
	)
}

/** The single maintenance pass of `runOnce()`, on an ordinary pool connection. */
export async function maintainOnce(options: MaintenanceOptions) {
	await sweep(options, async (work) => {
		const client = await options.pgPool.connect()
		try {
			return await work(client)
		} finally {
			client.release()
		}
	})
}

/**
 * Maintenance for one started Graphile worker pool, every heartbeat interval. With crash
 * recovery it first heartbeats the pool (its id is the jobs' locked_by). It runs on the
 * worker's own event loop, beside its jobs, but holds its own pool connection so jobs
 * holding every other one cannot starve it. `ready` settles after the first beat; jobs
 * wait for it so none can do work (and crash the process) before its worker is
 * registered.
 */
export function startMaintenance(
	options: MaintenanceOptions & { workerId: string }
) {
	const { pgPool, schema, workerId, recovery, logger } = options
	const table = `${quoteSchemaName(schema)}.${HEARTBEATS_TABLE}`
	let registered = false
	let stopped = false
	let timer: ReturnType<typeof setTimeout> | undefined
	let held: PoolClient | undefined
	async function connection() {
		if (held) return held
		const client = await pgPool.connect()
		// A held client has no pool listener; an unhandled 'error' would crash the process.
		client.on('error', (error) => discard(client, error))
		held = client
		return client
	}
	function discard(client: PoolClient, error: Error) {
		if (held !== client) return
		held = undefined
		client.release(error)
	}
	// Reconnect on the next pass after any failure rather than reusing a suspect client.
	async function withConnection<T>(work: (client: PoolClient) => Promise<T>) {
		const client = await connection()
		try {
			return await work(client)
		} catch (error) {
			discard(client, error instanceof Error ? error : new Error(String(error)))
			throw error
		}
	}
	async function beat() {
		const result = await withConnection((client) =>
			client.query<{ inserted: boolean }>(
				`INSERT INTO ${table} (worker_id, last_seen_at) VALUES ($1, now())
ON CONFLICT (worker_id) DO UPDATE SET last_seen_at = excluded.last_seen_at
RETURNING xmax = 0 AS inserted`,
				[workerId]
			)
		)
		if (registered && result.rows[0]?.inserted)
			logger.error('worker.crash_recovery.presumed_dead', {
				worker_id: workerId,
				message:
					'Other workers treated this worker as dead after missed heartbeats; its running jobs may have been released and run twice'
			})
		registered = true
	}
	async function beatSafely() {
		if (!recovery) return
		try {
			await beat()
		} catch (error) {
			logger.warn('worker.crash_recovery.heartbeat_failed', {
				worker_id: workerId,
				error: String(error)
			})
		}
	}
	const pass = () => sweep(options, withConnection)
	const ready = beatSafely()
	let pending = ready.then(pass)
	function schedule() {
		if (stopped) return
		timer = setTimeout(() => {
			pending = beatSafely().then(pass).then(schedule)
		}, recovery?.heartbeatInterval ?? MAINTENANCE_INTERVAL)
		timer.unref()
	}
	void pending.then(schedule)
	return {
		ready,
		async stop() {
			stopped = true
			clearTimeout(timer)
			await pending
			held?.release()
			held = undefined
		}
	}
}
