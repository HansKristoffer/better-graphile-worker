import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import { Pool } from 'pg'
import { z } from 'zod'
import type { JobHelpers } from 'graphile-worker'
import { defineQueue, type JobContext } from './queue.js'
import { defineQueues } from './define-queues.js'
import { createBetterWorker } from './create-better-worker.js'
import { createJobClient } from './job-client.js'
import { continueJob } from './private-jobs.js'
import { StepPersistenceError } from './errors.js'
import { createWorkerClient } from './client.js'
import { addJobsSql } from './enqueue-sql.js'
import type { JobFinishedEvent } from './hooks.js'

const enabled = Boolean(process.env.DATABASE_URL)
const suite = describe.skipIf(!enabled)
const schema = `BGW_long_${process.pid}`
const quiet = { debug() {}, info() {}, warn() {}, error() {} }
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

suite('long-running, per-tenant workloads', () => {
	const pool = new Pool({ connectionString: process.env.DATABASE_URL })
	pool.on('error', () => {})
	const contracts = defineQueues([
		defineQueue({
			name: 'lane',
			inputSchema: z.object({ shop: z.string(), n: z.number() }),
			deriveJobOptions: ({ shop }) => ({ queueName: `shop:${shop}` })
		}),
		defineQueue({
			name: 'keyed',
			inputSchema: z.object({ id: z.string().optional() }),
			deriveJobOptions: ({ id }) => (id ? { jobKey: `k:${id}` } : {})
		}),
		defineQueue({
			name: 'slice',
			inputSchema: z.object({ run: z.string(), mode: z.string() }),
			maxAttempts: 3,
			priority: 4,
			flags: ['slow'],
			deriveJobOptions: ({ run }) => ({
				queueName: `run:${run}`,
				jobKey: `slice:${run}`
			})
		}),
		defineQueue({ name: 'other', inputSchema: z.string() })
	])
	const active = new Map<string, number>()
	const overlaps: string[] = []
	let peak = 0
	let sliceRuns = 0
	let stepRuns = 0
	let afterContinue = false
	const finished: JobFinishedEvent[] = []
	const handlers = {
		lane: async ({ shop }: { shop: string }) => {
			const now = (active.get(shop) ?? 0) + 1
			active.set(shop, now)
			if (now > 1) overlaps.push(shop)
			peak = Math.max(
				peak,
				[...active.values()].reduce((a, b) => a + b, 0)
			)
			await sleep(30)
			active.set(shop, now - 1)
		},
		keyed: async () => {},
		slice: async ({ mode }: { mode: string }, ctx: JobContext) => {
			sliceRuns++
			await ctx.step.run('once', () => {
				stepRuns++
				return 1
			})
			if (sliceRuns > 1) return
			if (mode === 'fail-after') {
				try {
					await ctx.continue({ runAt: new Date(Date.now() + 3_600_000) })
				} catch {}
				throw new Error('after continue')
			}
			await ctx.continue({ runAt: new Date(Date.now() + 3_600_000) })
			afterContinue = true
		},
		other: async () => {}
	}
	const worker = createBetterWorker({
		pgPool: pool,
		schema,
		queues: contracts,
		concurrency: 6,
		pollInterval: 20,
		hooks: {
			createLogger: () => quiet,
			onJobFinished: (event) => void finished.push(event)
		},
		handlers: handlers as never
	})
	const jobs = async (task: string) =>
		(
			await pool.query(
				`SELECT j.id::text, j.key, j.priority, j.flags, j.max_attempts, j.attempts, j.run_at, j.payload, q.queue_name
				FROM "${schema}"._private_jobs j
				JOIN "${schema}"._private_tasks t ON t.id = j.task_id
				LEFT JOIN "${schema}"._private_job_queues q ON q.id = j.job_queue_id
				WHERE t.identifier = $1 ORDER BY j.id`,
				[task]
			)
		).rows
	beforeAll(() => worker.migrate())
	afterAll(async () => {
		await worker.stop()
		await pool.query(`DROP SCHEMA "${schema}" CASCADE`)
		await pool.end()
	})

	test('a derived queueName serializes per tenant, not globally', async () => {
		await worker.createJobs(
			'lane',
			['a', 'b', 'c'].flatMap((shop) => [1, 2, 3].map((n) => ({ shop, n })))
		)
		await worker.start()
		for (let i = 0; i < 100 && (await jobs('lane')).length; i++) await sleep(20)
		await worker.stop()
		expect(await jobs('lane')).toHaveLength(0)
		expect(overlaps).toEqual([])
		expect(peak).toBeGreaterThan(1)
	})

	test('createJobs returns ids aligned with inputs, including existing keys', async () => {
		const [existing] = await worker.createJobs('keyed', [{ id: 'x' }])
		const ids = await worker.createJobs('keyed', [
			{},
			{ id: 'x' },
			{ id: 'y' },
			{}
		])
		const rows = await jobs('keyed')
		const byId = new Map(rows.map((row) => [row.id, row.key]))
		expect(ids[1]).toBe(existing!)
		expect(ids.map((id) => byId.get(id!))).toEqual([null, 'k:x', 'k:y', null])
		await pool.query(`DELETE FROM "${schema}"._private_jobs`)
	})

	test('prepareJob commits with the caller transaction and rolls back with it', async () => {
		const client = createJobClient({ pgPool: pool, schema, queues: contracts })
		const prepared = await client.prepareJob('keyed', { id: 'tx' })
		const batch = await client.prepareJobs('keyed', [
			{ id: 'b1' },
			{ id: 'b2' }
		])
		const connection = await pool.connect()
		// Prisma-style $queryRawUnsafe(text, ...values) on top of pg.
		const queryRawUnsafe = async (text: string, ...values: unknown[]) =>
			(await connection.query(text, values)).rows
		try {
			await connection.query('BEGIN')
			await queryRawUnsafe(prepared.text, ...prepared.values)
			await connection.query('ROLLBACK')
			expect(await jobs('keyed')).toHaveLength(0)
			await connection.query('BEGIN')
			const [row] = await queryRawUnsafe(prepared.text, ...prepared.values)
			const rows = await queryRawUnsafe(batch.text, ...batch.values)
			await connection.query('COMMIT')
			expect(row).toEqual({ id: expect.any(String) })
			expect(rows).toHaveLength(2)
		} finally {
			connection.release()
		}
		expect((await jobs('keyed')).map((job) => job.key)).toEqual([
			'k:tx',
			'k:b1',
			'k:b2'
		])
		await worker.runOnce()
		expect(await jobs('keyed')).toHaveLength(0)
		await client.release()
	})

	test('unkeyed batch ordering survives merge joins and labels advanced adapter tasks correctly', async () => {
		const connection = await pool.connect()
		try {
			await connection.query('BEGIN')
			await connection.query('SET LOCAL enable_hashjoin = off')
			await connection.query('SET LOCAL enable_nestloop = off')
			const specs = Array.from({ length: 12 }, (_, n) => ({
				identifier: `ordering:${11 - n}`,
				queueName: `ordering:${11 - n}`,
				payload: { n },
				...(n === 4 ? { jobKey: 'ordering:key' } : {})
			}))
			const client = createWorkerClient({
				pgPool: { query: connection.query.bind(connection) } as unknown as Pool,
				schema
			})
			const rows = await client.enqueue.addJobs(specs)
			expect(rows.map((job) => job.payload)).toEqual(
				specs.map((spec) => spec.payload)
			)
			expect(rows.map((job) => job.task_identifier)).toEqual(
				specs.map((spec) => spec.identifier)
			)
			const prepared = addJobsSql(schema, specs)
			const ids = (await connection.query(prepared.text, prepared.values)).rows
			const actual = await connection.query(
				`SELECT id::text, payload FROM "${schema}"._private_jobs`
			)
			const byId = new Map(actual.rows.map((job) => [job.id, job.payload]))
			expect(ids.map((job) => byId.get(job.id))).toEqual(
				specs.map((spec) => spec.payload)
			)
		} finally {
			await connection.query('ROLLBACK')
			connection.release()
		}
	})

	test('keyed batches preserve run_at and replace envelope payloads', async () => {
		const future = new Date(Date.now() + 3_600_000)
		const [id] = await worker.createJobs('keyed', [{ id: 'preserve' }], {
			runAt: future
		})
		const statement = addJobsSql(
			schema,
			[
				{
					identifier: 'keyed',
					jobKey: 'k:preserve',
					payload: { __bgw: 2, payload: { id: 'changed' } }
				}
			],
			true
		)
		const rows = (await pool.query(statement.text, statement.values)).rows
		expect(rows).toEqual([{ id }])
		const [job] = await jobs('keyed')
		expect(job.run_at).toEqual(future)
		expect(job.payload.payload).toEqual({ id: 'changed' })
		await pool.query(`DELETE FROM "${schema}"._private_jobs`)
	})

	test('a SQL failure rolls back earlier unkeyed inserts in the same batch', async () => {
		const statement = addJobsSql(schema, [
			{ identifier: 'keyed', payload: { n: 1 } },
			{ identifier: 'keyed', payload: { n: 2 }, maxAttempts: 0 }
		])
		await expect(pool.query(statement.text, statement.values)).rejects.toThrow()
		expect(await jobs('keyed')).toHaveLength(0)
	})

	test('immediate enqueue rejects Graphile results lost to a concurrent claim', async () => {
		const adapter = createWorkerClient({ pgPool: pool, schema })
		for (const batch of [false, true]) {
			const id = await worker.createJob('keyed', { id: 'race' })
			await pool.query(`CREATE FUNCTION "${schema}".block_insert() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN PERFORM pg_advisory_xact_lock(${process.pid}, 912345); RETURN NEW; END$$`)
			await pool.query(
				`CREATE TRIGGER block_insert BEFORE INSERT ON "${schema}"._private_jobs FOR EACH ROW EXECUTE FUNCTION "${schema}".block_insert()`
			)
			const gate = await pool.connect()
			await gate.query('BEGIN')
			await gate.query('SELECT pg_advisory_xact_lock($1, 912345)', [
				process.pid
			])
			const pending = (
				batch
					? adapter.enqueue.addJobs([
							{ identifier: 'keyed', payload: {}, jobKey: 'k:race' }
						])
					: adapter.enqueue.addJob('keyed', {}, { jobKey: 'k:race' })
			).then(
				() => undefined,
				(error: unknown) => error
			)
			try {
				let waiting = false
				for (let i = 0; i < 100; i++) {
					const result = await pool.query(
						`SELECT 1 FROM pg_stat_activity WHERE wait_event = 'advisory' AND query LIKE $1`,
						[`%${schema}%add_jobs%`]
					)
					// add_job delegates to add_jobs inside the function.
					const single = batch
						? false
						: (
								await pool.query(
									`SELECT 1 FROM pg_stat_activity WHERE wait_event = 'advisory' AND query LIKE $1`,
									[`%${schema}%add_job(%`]
								)
							).rowCount
					if (result.rowCount || single) {
						waiting = true
						break
					}
					await sleep(5)
				}
				expect(waiting).toBe(true)
				await pool.query(
					`UPDATE "${schema}"._private_jobs SET locked_by = 'owner', locked_at = now(), attempts = 1 WHERE id = $1`,
					[id]
				)
			} finally {
				await gate.query('COMMIT')
				gate.release()
				await pending
				await pool.query(
					`DROP TRIGGER block_insert ON "${schema}"._private_jobs`
				)
				await pool.query(`DROP FUNCTION "${schema}".block_insert()`)
			}
			expect(await pending).toBeInstanceOf(Error)
			expect(await jobs('keyed')).toHaveLength(1)
			await pool.query(`DELETE FROM "${schema}"._private_jobs`)
		}
	})

	test('continue keeps lane, key, priority, flags, steps and max_attempts', async () => {
		await worker.createJob('slice', { run: '1', mode: 'once' })
		await worker.runOnce()
		expect(afterContinue).toBe(false)
		const [next, ...rest] = await jobs('slice')
		expect(rest).toHaveLength(0)
		expect(next).toMatchObject({
			key: 'slice:1',
			queue_name: 'run:1',
			priority: 4,
			flags: { slow: true },
			max_attempts: 3,
			attempts: 0
		})
		expect(next.run_at.getTime()).toBeGreaterThan(Date.now() + 3_000_000)
		expect(next.payload).toMatchObject({
			payload: { run: '1', mode: 'once' },
			steps: { once: { output: 1 } }
		})
		expect(finished.find((event) => event.queue === 'slice')).toMatchObject({
			status: 'success',
			continued: true
		})
		await pool.query(
			`UPDATE "${schema}"._private_jobs SET run_at = now() WHERE id = $1`,
			[next.id]
		)
		await worker.runOnce()
		expect(sliceRuns).toBe(2)
		expect(stepRuns).toBe(1)
		expect(await jobs('slice')).toHaveLength(0)
	})

	test('a failure after continue never retries the original job', async () => {
		await worker.createJob('slice', { run: '2', mode: 'fail-after' })
		sliceRuns = 0
		await worker.runOnce()
		const rows = await jobs('slice')
		expect(rows).toHaveLength(2)
		const [original, continuation] = rows
		expect(original.key).toBeNull()
		expect(original.attempts).toBe(original.max_attempts)
		expect(continuation).toMatchObject({ key: 'slice:2', attempts: 0 })
		await pool.query(`DELETE FROM "${schema}"._private_jobs`)
	})

	test('continue without the lock rolls back and is retriable', async () => {
		const [id] = await worker.createJobs('keyed', [{}])
		const utils = await worker.getWorkerUtils()
		await expect(
			continueJob(
				{
					job: { id, locked_by: 'someone-else' },
					withPgClient: utils.withPgClient
				} as unknown as JobHelpers,
				schema,
				undefined,
				undefined
			)
		).rejects.toBeInstanceOf(StepPersistenceError)
		expect(await jobs('keyed')).toHaveLength(1)
		await pool.query(`DELETE FROM "${schema}"._private_jobs`)
	})

	test('unkeyed continuation strips cron metadata, refreshes tracing, and exhausts the original', async () => {
		const id = await worker.createJob('keyed', {})
		const original = (
			await pool.query(
				`UPDATE "${schema}"._private_jobs SET locked_by = 'owner', locked_at = now(), attempts = 1,
			payload = payload::jsonb || '{"_cron":{"ts":"2030-01-01T00:00:00Z"},"traceparent":"old"}'::jsonb WHERE id = $1 RETURNING *`,
				[id]
			)
		).rows[0]
		const utils = await worker.getWorkerUtils()
		const helpers = {
			job: original,
			withPgClient: utils.withPgClient
		} as JobHelpers
		// Same pool can reacquire a stale lock; its old task must not continue it.
		await expect(
			continueJob(
				{ ...helpers, job: { ...original, locked_at: new Date(0) } },
				schema,
				undefined,
				undefined
			)
		).rejects.toBeInstanceOf(StepPersistenceError)
		const next = await continueJob(
			helpers,
			schema,
			new Date(Date.now() + 3_600_000),
			'fresh'
		)
		const rows = await jobs('keyed')
		expect(rows).toHaveLength(2)
		expect(rows[0]).toMatchObject({ id, attempts: 1, max_attempts: 1 })
		expect(rows[1]).toMatchObject({
			id: next,
			attempts: 0,
			max_attempts: 4,
			payload: { traceparent: 'fresh' }
		})
		expect(rows[1].payload._cron).toBeUndefined()
		await pool.query(`DELETE FROM "${schema}"._private_jobs`)
	})

	test('enqueue failure rolls back continuation and leaves the original retriable', async () => {
		const id = await worker.createJob('keyed', { id: 'rollback' })
		const original = (
			await pool.query(
				`UPDATE "${schema}"._private_jobs SET locked_by = 'owner', locked_at = now(), attempts = 1 WHERE id = $1 RETURNING *`,
				[id]
			)
		).rows[0]
		const utils = await worker.getWorkerUtils()
		const helpers = {
			job: original,
			withPgClient: (fn) =>
				utils.withPgClient((client) =>
					fn({
						query: async (text: string, values: unknown[]) => {
							const result = await client.query(text, values)
							if (text.includes('.add_job('))
								throw new Error('connection failed after insert')
							return result
						}
					} as never)
				)
		} as JobHelpers
		await expect(
			continueJob(helpers, schema, undefined, undefined)
		).rejects.toThrow('connection failed after insert')
		expect(await jobs('keyed')).toMatchObject([
			{ id, key: 'k:rollback', attempts: 1, max_attempts: 4 }
		])
		await pool.query(`DELETE FROM "${schema}"._private_jobs`)
	})

	test('continuation does not steal a key replaced by another producer', async () => {
		const id = await worker.createJob('keyed', { id: 'replaced' })
		const original = (
			await pool.query(
				`UPDATE "${schema}"._private_jobs SET locked_by = 'owner', locked_at = now(), attempts = 1 WHERE id = $1 RETURNING *`,
				[id]
			)
		).rows[0]
		const replacement = await worker.createJob('keyed', { id: 'replaced' })
		const utils = await worker.getWorkerUtils()
		const next = await continueJob(
			{ job: original, withPgClient: utils.withPgClient } as JobHelpers,
			schema,
			new Date(Date.now() + 3_600_000),
			undefined
		)
		expect(await jobs('keyed')).toMatchObject([
			{ id, key: null, attempts: 4, max_attempts: 4 },
			{ id: replacement, key: 'k:replaced' },
			{ id: next, key: null, attempts: 0 }
		])
		await pool.query(`DELETE FROM "${schema}"._private_jobs`)
	})

	test('process runs a subset; other queues stay pending', async () => {
		const narrow = createBetterWorker({
			pgPool: pool,
			schema,
			queues: contracts,
			process: ['other'],
			hooks: { createLogger: () => quiet },
			handlers: handlers as never
		})
		await narrow.createJob('other', 'x')
		await narrow.createJob('keyed', {})
		await narrow.runOnce()
		expect(await jobs('other')).toHaveLength(0)
		expect(await jobs('keyed')).toHaveLength(1)
		await worker.runOnce()
		for (let i = 0; i < 100 && (await jobs('keyed')).length; i++) await sleep(5)
		expect(await jobs('keyed')).toHaveLength(0)
		await narrow.stop()
	})
})
