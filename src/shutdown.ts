import pg, { type ClientConfig, type Pool } from 'pg'
import { DatabaseUnreachableError } from './errors.js'
import type { DatabaseProbeOptions } from './job-options.js'
import { assertInteger } from './validation.js'

export type DatabaseProbe = Required<DatabaseProbeOptions>

/** Off unless asked for. */
export function resolveDatabaseProbe(
	option: boolean | DatabaseProbeOptions | undefined
): DatabaseProbe | undefined {
	if (option === undefined || option === false) return undefined
	const { interval = 1000, timeout = 2000 } = option === true ? {} : option
	assertInteger(interval, 'probeDatabase.interval', 0, 2147483647)
	assertInteger(timeout, 'probeDatabase.timeout', 1, 2147483647)
	return { interval, timeout }
}

/**
 * Whether the database answers `SELECT 1` within `timeout` on a fresh connection; the
 * pool's own may be exhausted by the very queries that hang. An error the server sends,
 * such as too many connections, is an answer too: only silence counts as unreachable.
 * An aborted probe ends early and counts as an answer.
 */
export async function databaseAnswers(
	pgPool: Pool,
	timeout: number,
	signal?: AbortSignal
): Promise<boolean> {
	const Client = (pgPool.options.Client ?? pg.Client) as new (
		config: ClientConfig
	) => pg.Client
	const client = new Client({
		...pgPool.options,
		connectionTimeoutMillis: timeout,
		query_timeout: timeout
	})
	client.on('error', () => {})
	let timer: ReturnType<typeof setTimeout> | undefined
	let onAbort = () => {}
	const answered = client.connect().then(() => client.query('SELECT 1'))
	void answered.catch(() => {})
	try {
		await Promise.race([
			answered,
			new Promise<void>((resolve, reject) => {
				timer = setTimeout(() => reject(new Error('probe timed out')), timeout)
				onAbort = resolve
				signal?.addEventListener('abort', onAbort, { once: true })
			})
		])
		return true
	} catch (error) {
		return error instanceof Error && 'severity' in error
	} finally {
		clearTimeout(timer)
		signal?.removeEventListener('abort', onAbort)
		void client.end().catch(() => {})
	}
}

/** Resolves after `ms`, or as soon as `signal` aborts. */
function pause(ms: number, signal: AbortSignal) {
	return new Promise<void>((resolve) => {
		const done = () => {
			clearTimeout(timer)
			signal.removeEventListener('abort', done)
			resolve()
		}
		const timer = setTimeout(done, ms)
		signal.addEventListener('abort', done, { once: true })
	})
}

/**
 * Settles like `work`, or rejects with `DatabaseUnreachableError` once a probe gets no
 * answer. Probing ends as soon as `work` settles. A pool without connection options is
 * never probed.
 */
export async function whileReachable(
	work: Promise<void>,
	pgPool: Pool,
	probe: DatabaseProbe
): Promise<void> {
	if (!pgPool.options) return work
	const settling = new AbortController()
	const { signal } = settling
	const settled = work.finally(() => settling.abort())
	const unreachable = (async () => {
		for (;;) {
			await pause(probe.interval, signal)
			if (signal.aborted) return settled
			const answered = await databaseAnswers(pgPool, probe.timeout, signal)
			if (signal.aborted) return settled
			if (!answered) throw new DatabaseUnreachableError()
		}
	})()
	return Promise.race([settled, unreachable])
}
