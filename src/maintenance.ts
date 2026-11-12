import type { Pool, PoolClient } from 'pg'
import { pruneIdempotencyKeys } from './enqueue-sql.js'
import type { JobLogger } from './hooks.js'
import { pruneJobOutcomes } from './job-outcomes.js'
import {
	releaseJobsLockedBy,
	sweepWaits,
	type ReleasedJob
} from './private-jobs.js'
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
 * `ownIds` are never released: a worker running this is alive, and a stall between its
 * own heartbeat and this sweep must not make it release its own jobs.
 */
export async function sweepDeadWorkers(
	client: PoolClient,
	schema: string,
	staleAfter: number,
	ownIds: string | readonly string[] = []
) {
	const table = `${quoteSchemaName(schema)}.${HEARTBEATS_TABLE}`
	try {
		await client.query('BEGIN')
		const dead = await client.query<{ worker_id: string }>(
			`DELETE FROM ${table} WHERE worker_id IN (
	SELECT worker_id FROM ${table}
	WHERE last_seen_at < now() - $1::float8 * interval '1 millisecond'
		AND worker_id <> ALL($2::text[])
	FOR UPDATE SKIP LOCKED
) RETURNING worker_id`,
			[staleAfter, typeof ownIds === 'string' ? [ownIds] : ownIds]
		)
		const workerIds = dead.rows.map((row) => row.worker_id)
		const released = workerIds.length
			? await releaseJobsLockedBy(client, schema, workerIds)
			: []
		await client.query('COMMIT')
		return { workerIds, jobIds: released.map((job) => job.id), released }
	} catch (error) {
		await client.query('ROLLBACK').catch(() => {})
		throw error
	}
}

export type ReleaseLocksOptions = {
	/**
	 * Release every lock, live workers' included. Only for when nothing else runs on the
	 * database, such as before starting a development server.
	 */
	all?: boolean | undefined
}

/**
 * Release the jobs and lanes of lock owners that look dead: a heartbeat older than
 * `staleAfter`, or no heartbeat and no lock newer than `staleAfter` (a killed `runOnce()`,
 * a worker with `crashRecovery: false` or one that died before its first beat). Like
 * crash recovery, each attempt stays spent. Returns what it released.
 */
export async function releaseLocks(
	client: PoolClient,
	schema: string,
	staleAfter: number,
	options: ReleaseLocksOptions = {}
): Promise<ReleasedJob[]> {
	const quoted = quoteSchemaName(schema)
	const all = options.all === true
	try {
		await client.query('BEGIN')
		const locks = `SELECT locked_by, locked_at FROM ${quoted}._private_jobs WHERE locked_by IS NOT NULL
	UNION ALL SELECT locked_by, locked_at FROM ${quoted}._private_job_queues WHERE locked_by IS NOT NULL`
		const owners = await client.query<{ locked_by: string }>(
			all
				? `SELECT DISTINCT locked_by FROM (${locks}) AS locks`
				: `SELECT locks.locked_by FROM (${locks}) AS locks
LEFT JOIN ${quoted}.${HEARTBEATS_TABLE} beats ON beats.worker_id = locks.locked_by
GROUP BY locks.locked_by, beats.last_seen_at
HAVING coalesce(beats.last_seen_at, max(locks.locked_at)) < now() - $1::float8 * interval '1 millisecond'`,
			all ? [] : [staleAfter]
		)
		const ownerIds = owners.rows.map((row) => row.locked_by)
		const released = ownerIds.length
			? await releaseJobsLockedBy(client, schema, ownerIds)
			: []
		await client.query('COMMIT')
		return released
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
	/** Jobs crash recovery released on their last attempt, which failed them for good. */
	onExhausted?: ((jobs: ReleasedJob[]) => Promise<void>) | undefined
}

/**
 * One maintenance pass: release jobs of dead workers, fail waits on children that ended
 * without reporting (such as those just released), and delete expired outcomes and
 * idempotency keys. Every step uses SKIP LOCKED, so any number of workers can run it at
 * once, and a failing step does not stop the others.
 */
async function sweep(
	options: MaintenanceOptions,
	ownIds: readonly string[],
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
				ownIds
			)
			// Stopped workers leave their row behind; only released jobs are news.
			if (!swept.jobIds.length) return
			logger.warn('worker.crash_recovery.released', {
				dead_worker_ids: swept.workerIds.join(','),
				job_ids: swept.jobIds.join(','),
				job_count: swept.jobIds.length
			})
			const exhausted = swept.released.filter(
				(job) => job.attempts >= job.maxAttempts
			)
			if (exhausted.length) await options.onExhausted?.(exhausted)
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
	await sweep(options, [], async (work) => {
		const client = await options.pgPool.connect()
		try {
			return await work(client)
		} finally {
			client.release()
		}
	})
}

/**
 * Maintenance for the started Graphile worker pools of one worker or worker pool: every
 * heartbeat interval, one statement heartbeats all of them (their ids are the jobs'
 * locked_by), then one pass sweeps. It runs on the workers' event loop, beside their
 * jobs, but holds its own pool connection so jobs holding every other one cannot starve
 * it. `register(id).ready` settles after that pool's first beat; its jobs wait for it so
 * none can do work (and crash the process) before its worker is registered.
 */
export function createMaintenance() {
	const ids = new Set<string>()
	const beaten = new Set<string>()
	let loop:
		| {
				stopped: boolean
				timer?: ReturnType<typeof setTimeout>
				pending: Promise<void>
		  }
		| undefined
	let held: Promise<PoolClient> | undefined
	let options: MaintenanceOptions | undefined

	/** Runners register at once, so they share one pending connect. */
	function connection(pgPool: Pool): Promise<PoolClient> {
		if (!held) {
			const connecting = pgPool.connect().then((client) => {
				// A held client has no pool listener; an unhandled 'error' would crash the process.
				client.on('error', (error) => discard(connecting, client, error))
				return client
			})
			held = connecting
			connecting.catch(() => {
				if (held === connecting) held = undefined
			})
		}
		return held
	}
	function discard(
		connecting: Promise<PoolClient>,
		client: PoolClient,
		error: Error
	) {
		if (held !== connecting) return
		held = undefined
		client.release(error)
	}
	// Reconnect on the next pass after any failure rather than reusing a suspect client.
	async function withConnection<T>(work: (client: PoolClient) => Promise<T>) {
		const connecting = connection(options!.pgPool)
		const client = await connecting
		try {
			return await work(client)
		} catch (error) {
			discard(
				connecting,
				client,
				error instanceof Error ? error : new Error(String(error))
			)
			throw error
		}
	}
	async function beat(workerIds: readonly string[]) {
		const { recovery, logger, schema } = options!
		if (!recovery || !workerIds.length) return
		try {
			const result = await withConnection((client) =>
				client.query<{ worker_id: string; inserted: boolean }>(
					`INSERT INTO ${quoteSchemaName(schema)}.${HEARTBEATS_TABLE} (worker_id, last_seen_at)
SELECT worker_id, now() FROM unnest($1::text[]) AS worker_id
ON CONFLICT (worker_id) DO UPDATE SET last_seen_at = excluded.last_seen_at
RETURNING worker_id, xmax = 0 AS inserted`,
					[workerIds]
				)
			)
			for (const row of result.rows) {
				if (beaten.has(row.worker_id) && row.inserted)
					logger.error('worker.crash_recovery.presumed_dead', {
						worker_id: row.worker_id,
						message:
							'Other workers treated this worker as dead after missed heartbeats; its running jobs may have been released and run twice'
					})
				beaten.add(row.worker_id)
			}
		} catch (error) {
			logger.warn('worker.crash_recovery.heartbeat_failed', {
				worker_id: workerIds.join(','),
				error: String(error)
			})
		}
	}
	function startLoop(first: Promise<void>) {
		const current: NonNullable<typeof loop> = { stopped: false, pending: first }
		loop = current
		const pass = () => sweep(options!, [...ids], withConnection)
		const schedule = () => {
			if (current.stopped) return
			current.timer = setTimeout(() => {
				current.pending = beat([...ids])
					.then(pass)
					.then(schedule)
			}, options!.recovery?.heartbeatInterval ?? MAINTENANCE_INTERVAL)
			current.timer.unref()
		}
		current.pending = first.then(pass).then(schedule)
	}
	return {
		/** Maintain one started Graphile worker pool; the first one configures the loop. */
		register(workerId: string, configure: MaintenanceOptions) {
			options ??= configure
			ids.add(workerId)
			const ready = beat([workerId])
			if (!loop) startLoop(ready)
			return {
				ready,
				async stop() {
					ids.delete(workerId)
					beaten.delete(workerId)
					const current = loop
					if (ids.size || !current) return
					current.stopped = true
					clearTimeout(current.timer)
					// A runner restarting meanwhile starts a new loop on the same connection.
					loop = undefined
					await current.pending
					if (!loop) {
						const connecting = held
						held = undefined
						const client = await connecting?.catch(() => undefined)
						client?.release()
					}
				}
			}
		}
	}
}

export type Maintenance = ReturnType<typeof createMaintenance>
