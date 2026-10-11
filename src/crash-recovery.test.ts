import {
	afterAll,
	afterEach,
	beforeAll,
	describe,
	expect,
	test
} from 'bun:test'
import { Pool } from 'pg'
import { z } from 'zod'
import { defineQueue } from './queue.js'
import { defineQueues } from './define-queues.js'
import { createBetterWorker } from './create-better-worker.js'
import { resolveCrashRecovery, sweepDeadWorkers } from './crash-recovery.js'

const enabled = Boolean(process.env.DATABASE_URL)
const suite = describe.skipIf(!enabled)
const schema = `BGW_crash_${process.pid}`
const quiet = { debug() {}, info() {}, warn() {}, error() {} }
const fast = { heartbeatInterval: 50, staleAfter: 150 }
const released = (workerId: string) =>
	`Worker ${workerId} stopped sending heartbeats (crashed or blocked); released by crash recovery`

// A separate process that claims a `sync` job and dies on it, like an OOM kill.
const crashingWorker = `import pg from 'pg'
import { z } from 'zod'
import { createBetterWorker, defineQueue, defineQueues } from ${JSON.stringify(`${import.meta.dir}/index.ts`)}
const pgPool = new pg.Pool({ connectionString: process.env.DATABASE_URL })
const queues = defineQueues([defineQueue({ name: 'sync', inputSchema: z.string(), serial: true, maxAttempts: 3 })])
await createBetterWorker({
	pgPool, schema: ${JSON.stringify(schema)}, queues, crashRecovery: ${JSON.stringify(fast)},
	handlers: { sync: () => process.kill(process.pid, 'SIGKILL') }
}).start()`

describe('crash recovery options', () => {
	test('is off unless enabled and defaults conservatively', () => {
		expect(resolveCrashRecovery(undefined)).toBeUndefined()
		expect(resolveCrashRecovery(false)).toBeUndefined()
		expect(resolveCrashRecovery(true)).toEqual({
			heartbeatInterval: 30_000,
			staleAfter: 300_000
		})
	})

	test('rejects a threshold that tolerates fewer than two missed heartbeats', () => {
		expect(() =>
			resolveCrashRecovery({ heartbeatInterval: 10_000, staleAfter: 29_999 })
		).toThrow('crashRecovery.staleAfter')
		expect(() => resolveCrashRecovery({ heartbeatInterval: 0 })).toThrow(
			'crashRecovery.heartbeatInterval'
		)
	})
})

suite('crash recovery in PostgreSQL', () => {
	const pool = new Pool({ connectionString: process.env.DATABASE_URL })
	pool.on('error', () => {})
	const contracts = defineQueues([
		defineQueue({
			name: 'sync',
			inputSchema: z.string(),
			serial: true,
			maxAttempts: 3
		})
	])
	const worker = createBetterWorker({
		pgPool: pool,
		schema,
		queues: contracts,
		hooks: { createLogger: () => quiet },
		handlers: { sync: async () => {} }
	})
	const q = `"${schema}"`
	const lockAs = async (id: string, workerId: string) => {
		await pool.query(
			`UPDATE ${q}._private_jobs SET locked_by = $2, locked_at = now(), attempts = attempts + 1 WHERE id = $1`,
			[id, workerId]
		)
		await pool.query(
			`UPDATE ${q}._private_job_queues SET locked_by = $2, locked_at = now()
			WHERE id = (SELECT job_queue_id FROM ${q}._private_jobs WHERE id = $1)`,
			[id, workerId]
		)
	}
	const beatAs = (workerId: string, age: string) =>
		pool.query(
			`INSERT INTO ${q}._bgw_worker_heartbeats VALUES ($1, now() - $2::interval)`,
			[workerId, age]
		)
	const job = async (id: string) =>
		(
			await pool.query<{
				locked_by: string | null
				attempts: number
				last_error: string | null
				queue_locked_by: string | null
			}>(
				`SELECT j.locked_by, j.attempts, j.last_error, jq.locked_by AS queue_locked_by
				FROM ${q}._private_jobs j JOIN ${q}._private_job_queues jq ON jq.id = j.job_queue_id
				WHERE j.id = $1`,
				[id]
			)
		).rows[0]
	beforeAll(() => worker.migrate())
	afterEach(async () => {
		await pool.query(`DELETE FROM ${q}._private_jobs`)
		await pool.query(
			`UPDATE ${q}._private_job_queues SET locked_by = NULL, locked_at = NULL`
		)
		await pool.query(`DELETE FROM ${q}._bgw_worker_heartbeats`)
	})
	afterAll(async () => {
		await worker.stop()
		await pool.query(`DROP SCHEMA ${q} CASCADE`)
		await pool.end()
	})

	test('releases jobs and lanes of dead workers only, keeping the attempt', async () => {
		const dead = await worker.createJob('sync', 'dead')
		const alive = await worker.createJob('sync', 'alive', {
			queueName: 'other-lane'
		})
		await lockAs(dead!, 'pool-dead')
		await lockAs(alive!, 'pool-alive')
		await beatAs('pool-dead', '1 hour')
		await beatAs('pool-alive', '0 seconds')

		const swept = await sweepDeadWorkers(pool, schema, 60_000)

		expect(swept).toEqual({ workerIds: ['pool-dead'], jobIds: [dead!] })
		expect(await job(dead!)).toEqual({
			locked_by: null,
			attempts: 1,
			last_error: released('pool-dead'),
			queue_locked_by: null
		})
		expect(await job(alive!)).toMatchObject({
			locked_by: 'pool-alive',
			queue_locked_by: 'pool-alive'
		})
		const rows = await pool.query(
			`SELECT worker_id FROM ${q}._bgw_worker_heartbeats`
		)
		expect(rows.rows).toEqual([{ worker_id: 'pool-alive' }])
	})

	test('a job that keeps killing its worker fails permanently', async () => {
		const id = await worker.createJob('sync', 'oom')
		await pool.query(
			`UPDATE ${q}._private_jobs SET attempts = max_attempts - 1 WHERE id = $1`,
			[id]
		)
		await lockAs(id!, 'pool-oom')
		await beatAs('pool-oom', '1 hour')

		await sweepDeadWorkers(pool, schema, 60_000)

		const [failed] = await worker.listJobs({ state: 'failed' })
		expect(failed?.id).toBe(id!)
		expect(failed?.lastError).toBe(released('pool-oom'))
	})

	test('a live worker retries the job and lane of a SIGKILLed worker', async () => {
		const id = await worker.createJob('sync', 'killed')
		const child = Bun.spawn([process.execPath, '-e', crashingWorker], {
			cwd: `${import.meta.dir}/..`,
			stdout: 'ignore',
			stderr: 'inherit'
		})
		await child.exited
		expect(child.signalCode).toBe('SIGKILL')
		const orphan = await job(id!)
		expect(orphan?.locked_by).toMatch(/^pool-/)
		expect(orphan?.queue_locked_by).toBe(orphan!.locked_by)

		const retried = Promise.withResolvers<{
			attempt: number
			lastError: string | null
			registered: boolean
		}>()
		const rescuer = createBetterWorker({
			pgPool: pool,
			schema,
			queues: contracts,
			pollInterval: 20,
			crashRecovery: fast,
			hooks: { createLogger: () => quiet },
			handlers: {
				sync: async (_payload, ctx) => {
					const claimed = ctx.helpers.job
					const own = await pool.query(
						`SELECT 1 FROM ${q}._bgw_worker_heartbeats WHERE worker_id = $1`,
						[claimed.locked_by]
					)
					retried.resolve({
						attempt: ctx.attempt,
						lastError: claimed.last_error,
						registered: own.rowCount === 1
					})
				}
			}
		})
		await rescuer.start()
		try {
			expect(await retried.promise).toEqual({
				attempt: 2,
				lastError: released(orphan!.locked_by!),
				registered: true
			})
		} finally {
			await rescuer.stop()
		}
	}, 20_000)
})
