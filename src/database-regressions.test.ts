import { beforeAll, afterAll, describe, expect, test } from 'bun:test'
import { Pool } from 'pg'
import { z } from 'zod'
import { defineQueue } from './queue.js'
import { defineQueues } from './define-queues.js'
import { createBetterWorker } from './create-better-worker.js'
import { createJobClient } from './job-client.js'
import { createWorkerClient } from './client.js'
import { NonRetriableError, StepPersistenceError } from './errors.js'
import { createPgStepStore } from './steps.js'
import type { JobHelpers } from 'graphile-worker'
import type { OtelApi } from './otel.js'
import { extractProducerLink } from './payload.js'

const enabled = Boolean(process.env.DATABASE_URL)
const suite = describe.skipIf(!enabled)
const schema = `BGW_tests_${process.pid}`
const quiet = { debug() {}, info() {}, warn() {}, error() {} }
suite('PostgreSQL regression cases', () => {
	const pool = new Pool({ connectionString: process.env.DATABASE_URL })
	pool.on('error', () => {})
	const contracts = defineQueues([
		defineQueue({ name: 'steps', inputSchema: z.string(), maxAttempts: 2 }),
		defineQueue({ name: 'permanent', inputSchema: z.string() }),
		defineQueue({ name: 'null', inputSchema: z.null() }),
		defineQueue({ name: 'array', inputSchema: z.array(z.string()) }),
		defineQueue({ name: 'cron', cron: '* * * * *', serial: true, priority: 7 })
	])
	let stepRuns = 0
	const worker = createBetterWorker({
		pgPool: pool,
		schema,
		queues: contracts,
		defaultMaxAttempts: 9,
		hooks: { createLogger: () => quiet },
		handlers: {
			steps: async (_payload, ctx) => {
				await Promise.all(
					Array.from({ length: 10 }, (_, i) =>
						ctx.step.run(`parallel-${i}`, async () => {
							stepRuns++
							return { i }
						})
					)
				)
				await ctx.step.run('void', () => undefined)
				throw new Error('retry after checkpoints')
			},
			permanent: async () => {
				throw new NonRetriableError('invalid business state')
			},
			null: async () => {},
			array: async () => {},
			cron: async () => {}
		}
	})
	beforeAll(() => worker.migrate())
	afterAll(async () => {
		await worker.stop()
		await pool.query(`DROP SCHEMA "${schema}" CASCADE`)
		await pool.end()
	})
	test('utilities initialize once under concurrency and release listeners', async () => {
		const client = createWorkerClient({ pgPool: pool, schema })
		const before = pool.listenerCount('error')
		const results = await Promise.all(
			Array.from({ length: 20 }, () => client.getUtils())
		)
		expect(new Set(results).size).toBe(1)
		await Promise.all([client.release(), client.release()])
		expect(pool.listenerCount('error')).toBe(before)
	})
	test('atomic parallel checkpoints survive retry, preserve void, and reject lost locks', async () => {
		const id = await worker.createJob('steps', 'input')
		await worker.runOnce()
		const row = await pool.query<{
			payload: { steps: Record<string, unknown> }
		}>(`SELECT payload FROM "${schema}"._private_jobs WHERE id = $1`, [id])
		expect(Object.keys(row.rows[0]!.payload.steps)).toHaveLength(11)
		expect(row.rows[0]!.payload.steps.void).toEqual({
			output: null,
			isVoid: true
		})

		const utils = await worker.getWorkerUtils()
		const store = createPgStepStore({
			helpers: {
				job: { locked_by: 'wrong-owner' },
				withPgClient: utils.withPgClient
			} as unknown as JobHelpers,
			schema,
			jobId: id!,
			rawPayload: row.rows[0]!.payload
		})
		await expect(store.set('lost', 1)).rejects.toBeInstanceOf(
			StepPersistenceError
		)
		await worker.retryJobs([id!])
		const reset = await pool.query(
			`SELECT attempts FROM "${schema}".jobs WHERE id = $1`,
			[id]
		)
		expect(reset.rows[0]?.attempts).toBe(0)
		await worker.runOnce()
		expect(stepRuns).toBe(10)
		await worker.failJobs([id!])
	})
	test('retained permanent failures survive a new instance and can be retried', async () => {
		const id = await worker.createJob('permanent', 'input')
		await worker.runOnce()
		await worker.stop()
		const fresh = createBetterWorker({
			pgPool: pool,
			schema,
			queues: contracts,
			handlers: {
				steps: async () => {},
				permanent: async () => {},
				null: async () => {},
				array: async () => {},
				cron: async () => {}
			},
			hooks: { createLogger: () => quiet }
		})
		const stats = await fresh.getJobStats()
		expect(
			stats.find((row) => row.taskIdentifier === 'permanent')?.failed
		).toBe(1)
		const jobs = await fresh.listJobs({
			queue: 'permanent',
			includePayload: false
		})
		expect(jobs[0]).toMatchObject({
			id,
			lastError: expect.stringContaining('invalid business state'),
			lockedAt: null
		})
		await fresh.stop()
		await worker.retryJobs([id!])
		expect(
			(await worker.listJobs({ queue: 'permanent', state: 'pending' }))[0]
				?.attempts
		).toBe(0)
		await worker.failJobs([id!])
	})
	test('null round trips and keyed arrays replace consistently', async () => {
		const producer = createJobClient({
			pgPool: pool,
			schema,
			queues: contracts,
			otel: { api: null }
		})
		const nullId = await producer.createJob('null', null)
		expect(
			(await worker.listJobs({ queue: 'null', includePayload: true })).find(
				(job) => job.id === nullId
			)?.payload
		).toBeNull()
		const first = await producer.createJob('array', ['a'], {
			jobKey: 'array-key',
			runAt: new Date(Date.now() + 60000)
		})
		const second = await producer.createJob('array', ['b'], {
			jobKey: 'array-key',
			runAt: new Date(Date.now() + 60000)
		})
		expect(second).toBe(first)
		expect(
			(await worker.listJobs({ queue: 'array', includePayload: true }))[0]
				?.payload
		).toEqual(['b'])
		await producer.release()
	})
	test('metadata-only listing is the default; payload debugging is explicit', async () => {
		await worker.createJob('array', ['inspect'], {
			runAt: new Date(Date.now() + 60000)
		})
		const metadata = await worker.listJobs({ queue: 'array' })
		expect(metadata.length).toBeGreaterThan(0)
		expect(metadata.every((job) => job.payload === null)).toBe(true)
		const debug = await worker.listJobs({
			queue: 'array',
			includePayload: true
		})
		expect(debug[0]?.payload).toEqual(['inspect'])
	})
	test('unsupported old payloads are retained without invoking handlers', async () => {
		let calls = 0
		const queue = defineQueue({ name: 'unsupported', inputSchema: z.string() })
		const strict = createBetterWorker({
			pgPool: pool,
			schema,
			queues: [queue],
			handlers: {
				unsupported: () => {
					calls++
				}
			},
			hooks: { createLogger: () => quiet }
		})
		try {
			const job = await pool.query(
				`SELECT * FROM "${schema}".add_job('unsupported', $1::json)`,
				[JSON.stringify({ __bgw: 1, payload: 'old' })]
			)
			await strict.runOnce()
			expect(calls).toBe(0)
			const stored = await strict.listJobs({
				queue: 'unsupported',
				state: 'failed'
			})
			expect(stored[0]).toMatchObject({
				id: String(job.rows[0].id),
				lastError: expect.stringContaining('version-2 envelope')
			})
			expect(
				(await strict.getJobStats()).find(
					(row) => row.taskIdentifier === 'unsupported'
				)?.failed
			).toBe(1)
		} finally {
			await strict.stop()
		}
	})
	test('cron triggers inherit serial, priority and worker attempts', async () => {
		const id = await worker.triggerCron('cron')
		const result = await pool.query(
			`SELECT max_attempts, priority, queue_name FROM "${schema}".jobs WHERE id = $1`,
			[id]
		)
		expect(result.rows[0]).toEqual({
			max_attempts: 9,
			priority: 7,
			queue_name: 'cron'
		})
	})
	test('instances retain their tracing adapter independently', async () => {
		const trace = {
			traceId: 'a'.repeat(32),
			spanId: 'b'.repeat(16),
			traceFlags: 0
		}
		const span = {
			setAttribute() {},
			setAttributes() {},
			setStatus() {},
			recordException() {},
			end() {},
			spanContext: () => trace
		}
		const api: OtelApi = {
			trace: {
				getTracer: () => ({
					startActiveSpan: async (_name, _opts, fn) => fn(span)
				}),
				getActiveSpan: () => span
			},
			isSpanContextValid: () => true,
			SpanStatusCode: { OK: 1, ERROR: 2 },
			SpanKind: { INTERNAL: 0, CONSUMER: 1, PRODUCER: 2 },
			TraceFlags: { SAMPLED: 1 }
		}
		const traced = createJobClient({
			pgPool: pool,
			schema,
			queues: contracts,
			otel: { api }
		})
		const untraced = createJobClient({
			pgPool: pool,
			schema,
			queues: contracts,
			otel: { api: null }
		})
		const a = await traced.createJob('null', null),
			b = await untraced.createJob('null', null)
		const rows = await pool.query(
			`SELECT id, payload FROM "${schema}"._private_jobs WHERE id = ANY($1::bigint[]) ORDER BY id`,
			[[a, b]]
		)
		expect(
			extractProducerLink(rows.rows[0].payload).link?.context.traceId
		).toBe(trace.traceId)
		expect(extractProducerLink(rows.rows[1].payload).link).toBeNull()
		await traced.release()
		await untraced.release()
	})
	test('producer role can enqueue without DDL permissions or migration queries', async () => {
		const role = `${schema}_producer`
		const password = crypto.randomUUID()
		const url = new URL(process.env.DATABASE_URL!)
		url.username = role
		url.password = password
		await pool.query(`CREATE ROLE "${role}" LOGIN PASSWORD '${password}'`)
		const restricted = new Pool({ connectionString: url.toString() })
		restricted.on('error', () => {})
		try {
			await pool.query(`GRANT USAGE ON SCHEMA "${schema}" TO "${role}"`)
			await pool.query(
				`GRANT SELECT, INSERT, UPDATE ON "${schema}"._private_jobs, "${schema}"._private_tasks, "${schema}"._private_job_queues TO "${role}"`
			)
			for (const table of [
				'_private_jobs',
				'_private_tasks',
				'_private_job_queues'
			])
				await pool.query(
					`CREATE POLICY producer_access ON "${schema}".${table} TO "${role}" USING (true) WITH CHECK (true)`
				)
			await pool.query(
				`GRANT USAGE ON ALL SEQUENCES IN SCHEMA "${schema}" TO "${role}"`
			)
			const privileges = await restricted.query(
				`SELECT has_schema_privilege(current_user, $1, 'CREATE') AS ddl`,
				[schema]
			)
			expect(privileges.rows[0].ddl).toBe(false)
			let requests = 0
			const originalQuery = restricted.query.bind(restricted)
			restricted.query = ((...args: unknown[]) => {
				requests++
				return (originalQuery as (...args: unknown[]) => unknown)(...args)
			}) as typeof restricted.query
			const producer = createJobClient({
				pgPool: restricted,
				schema,
				queues: contracts
			})
			expect(await producer.createJob('null', null)).toMatch(/^\d+$/)
			await producer.createJobs('array', [['a'], ['b']] as const)
			expect(requests).toBe(2)
			await producer.release()
		} finally {
			await restricted.end()
			for (const table of [
				'_private_jobs',
				'_private_tasks',
				'_private_job_queues'
			])
				await pool.query(
					`DROP POLICY IF EXISTS producer_access ON "${schema}".${table}`
				)
			await pool.query(`DROP OWNED BY "${role}"`)
			await pool.query(`DROP ROLE "${role}"`)
		}
	})
	test('cursor pagination retains timestamp precision across equal-time batches', async () => {
		const ids = await worker.createJobs(
			'array',
			Array.from({ length: 10 }, (_, i) => [String(i)])
		)
		await pool.query(
			`UPDATE "${schema}"._private_jobs SET created_at='2100-01-01T00:00:00.123456Z' WHERE id=ANY($1::bigint[])`,
			[ids]
		)
		const found: string[] = []
		let before: { createdAt: string; id: string } | undefined
		for (;;) {
			const page = await worker.listJobs({
				queue: 'array',
				limit: 3,
				includePayload: false,
				...(before ? { before } : {})
			})
			if (!page.length) break
			found.push(...page.map((job) => job.id))
			before = page.at(-1)!.cursor
		}
		expect(new Set(found).size).toBe(found.length)
		expect(found.filter((id) => ids.includes(id))).toHaveLength(10)
		expect(await worker.listJobs({ queue: 'array', limit: 0 })).toEqual([])
		for (const limit of [-1, NaN, Infinity, 1.5, 1001])
			await expect(worker.listJobs({ limit })).rejects.toThrow('integer')
	})
	test('concurrent worker starts and shutdown leave the caller pool open', async () => {
		await Promise.all([worker.start(), worker.start()])
		await Promise.all([worker.stop(), worker.stop()])
		expect((await pool.query('SELECT 1 AS alive')).rows[0]).toEqual({
			alive: 1
		})
	})
})
