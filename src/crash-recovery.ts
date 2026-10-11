import type { Pool, PoolClient } from 'pg'
import type { JobLogger } from './hooks.js'
import { releaseJobsLockedBy } from './private-jobs.js'
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

const HEARTBEATS = '_bgw_worker_heartbeats'
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

/** Library-owned, so it lives beside Graphile's tables but outside its migrations. */
export async function ensureHeartbeatTable(
	pgPool: Pool,
	schema: string
): Promise<void> {
	const table = `${quoteSchemaName(schema)}.${HEARTBEATS}`
	// Check first so a worker role without CREATE can start against a migrated schema.
	const existing = await pgPool.query<{ exists: boolean }>(
		'SELECT to_regclass($1) IS NOT NULL AS exists',
		[table]
	)
	if (existing.rows[0]?.exists) return
	await pgPool
		.query(
			`CREATE TABLE IF NOT EXISTS ${table} (worker_id text PRIMARY KEY, last_seen_at timestamptz NOT NULL)`
		)
		.catch((error: { code?: string }) => {
			// Concurrent creators race on the catalog; the loser's table already exists.
			if (error.code !== '23505' && error.code !== '42P07') throw error
		})
}

/**
 * Release jobs held by workers whose heartbeat is older than staleAfter. SKIP LOCKED
 * lets every live worker sweep without two of them releasing the same worker.
 */
export async function sweepDeadWorkers(
	client: PoolClient,
	schema: string,
	staleAfter: number
) {
	const table = `${quoteSchemaName(schema)}.${HEARTBEATS}`
	try {
		await client.query('BEGIN')
		const dead = await client.query<{ worker_id: string }>(
			`DELETE FROM ${table} WHERE worker_id IN (
	SELECT worker_id FROM ${table}
	WHERE last_seen_at < now() - $1::float8 * interval '1 millisecond'
	FOR UPDATE SKIP LOCKED
) RETURNING worker_id`,
			[staleAfter]
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

/**
 * Heartbeat for one Graphile worker pool (its id is the jobs' locked_by), sweeping dead
 * workers after each beat. It runs on the worker's own event loop, beside its jobs, but
 * holds its own pool connection so jobs holding every other one cannot starve it.
 * `ready` settles after the first beat; jobs wait for it so none can do work (and crash
 * the process) before its worker is registered.
 */
export function startHeartbeat(options: {
	pgPool: Pool
	schema: string
	workerId: string
	config: Required<CrashRecoveryOptions>
	logger: JobLogger
}) {
	const { pgPool, schema, workerId, config, logger } = options
	const table = `${quoteSchemaName(schema)}.${HEARTBEATS}`
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
	// Reconnect on the next beat after any failure rather than reusing a suspect client.
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
		try {
			await beat()
		} catch (error) {
			logger.warn('worker.crash_recovery.heartbeat_failed', {
				worker_id: workerId,
				error: String(error)
			})
		}
	}
	async function sweep() {
		try {
			const { workerIds, jobIds } = await withConnection((client) =>
				sweepDeadWorkers(client, schema, config.staleAfter)
			)
			// Stopped workers leave their row behind; only released jobs are news.
			if (jobIds.length)
				logger.warn('worker.crash_recovery.released', {
					dead_worker_ids: workerIds.join(','),
					job_ids: jobIds.join(','),
					job_count: jobIds.length
				})
		} catch (error) {
			logger.warn('worker.crash_recovery.sweep_failed', {
				error: String(error)
			})
		}
	}
	const ready = beatSafely()
	let pending = ready.then(sweep)
	function schedule() {
		if (stopped) return
		timer = setTimeout(() => {
			pending = beatSafely().then(sweep).then(schedule)
		}, config.heartbeatInterval)
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
