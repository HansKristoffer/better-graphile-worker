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
 */
export async function databaseAnswers(
	pgPool: Pool,
	timeout: number
): Promise<boolean> {
	const Client = (pgPool.options.Client ?? pg.Client) as new (
		config: ClientConfig
	) => pg.ClientBase
	const client = new Client({
		...pgPool.options,
		connectionTimeoutMillis: timeout,
		query_timeout: timeout
	})
	client.on('error', () => {})
	let timer: ReturnType<typeof setTimeout> | undefined
	const answered = client.connect().then(() => client.query('SELECT 1'))
	void answered.catch(() => {})
	try {
		await Promise.race([
			answered,
			new Promise((_, reject) => {
				timer = setTimeout(() => reject(new Error('probe timed out')), timeout)
			})
		])
		return true
	} catch (error) {
		return error instanceof Error && 'severity' in error
	} finally {
		clearTimeout(timer)
		void (client as pg.Client).end().catch(() => {})
	}
}

/**
 * Settles like `work`, or rejects with `DatabaseUnreachableError` once a probe gets no
 * answer. A pool without connection options is never probed.
 */
export async function whileReachable(
	work: Promise<void>,
	pgPool: Pool,
	probe: DatabaseProbe
): Promise<void> {
	if (!pgPool.options) return work
	let done = false
	let wake = () => {}
	const settled = work.finally(() => {
		done = true
		wake()
	})
	const unreachable = (async () => {
		for (;;) {
			await new Promise<void>((resolve) => {
				const timer = setTimeout(resolve, probe.interval)
				wake = () => {
					clearTimeout(timer)
					resolve()
				}
			})
			if (done) return settled
			if (!(await databaseAnswers(pgPool, probe.timeout))) break
		}
		if (done) return settled
		throw new DatabaseUnreachableError()
	})()
	return Promise.race([settled, unreachable])
}
