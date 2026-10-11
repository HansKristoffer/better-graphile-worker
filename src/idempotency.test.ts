import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import { Pool } from 'pg'
import { z } from 'zod'
import { bindCreateJob } from './create-job.js'
import type { EnqueueAdapter } from './client.js'
import type { IdempotentJobSpec } from './enqueue-sql.js'
import { defineQueue } from './queue.js'
import { defineQueues } from './define-queues.js'
import { createBetterWorker } from './create-better-worker.js'
import { createJobClient } from './job-client.js'
import { createTestHarness } from './testing.js'
import { parseDuration } from './validation.js'

const sync = defineQueue({
	name: 'sync',
	inputSchema: z.object({ user: z.string() }),
	deriveJobOptions: ({ user }) => ({
		idempotencyKey: `sync:${user}`,
		idempotencyTtl: '2h'
	}),
	processFn: () => {}
})

function recorder() {
	const specs: IdempotentJobSpec[] = []
	const enqueue: EnqueueAdapter = {
		addJob: async () => {
			throw new Error('unexpected addJob')
		},
		addJobs: async () => {
			throw new Error('unexpected addJobs')
		},
		async addIdempotentJobs(batch) {
			specs.push(...batch)
			return batch.map((_, i) => ({ id: String(i + 1), deduplicated: false }))
		}
	}
	return { specs, enqueue }
}

describe('idempotency options', () => {
	test('durations are milliseconds or whole units', () => {
		expect(parseDuration(1500, 'ttl')).toBe(1500)
		expect(parseDuration('90s', 'ttl')).toBe(90_000)
		expect(parseDuration('2h', 'ttl')).toBe(7_200_000)
		expect(parseDuration('7d', 'ttl')).toBe(604_800_000)
		for (const bad of [0, -1, 1.5, '2 hours', '1.5h', 'h', ''])
			expect(() => parseDuration(bad, 'ttl')).toThrow(RangeError)
	})

	test('derived keys reach the adapter; explicit options win; the default TTL is 30 days', async () => {
		const { specs, enqueue } = recorder()
		const producer = bindCreateJob({ queues: [sync] as const, enqueue })
		await producer.createJob('sync', { user: 'a' })
		await producer.createJob(
			'sync',
			{ user: 'a' },
			{ idempotencyKey: 'manual', idempotencyTtl: 1000 }
		)
		await producer.createJobs('sync', [{ user: 'b' }], {
			idempotencyTtl: '1m'
		})
		const plain = defineQueue({ name: 'plain', inputSchema: z.string() })
		const other = recorder()
		await bindCreateJob({
			queues: [plain] as const,
			enqueue: other.enqueue
		}).createJob('plain', 'x', { idempotencyKey: 'k' })
		expect(
			specs.map(({ idempotencyKey, idempotencyTtl }) => [
				idempotencyKey,
				idempotencyTtl
			])
		).toEqual([
			['sync:a', 7_200_000],
			['manual', 1000],
			['sync:b', 60_000]
		])
		expect(other.specs[0]?.idempotencyTtl).toBe(30 * 86_400_000)
	})

	test('invalid keys, shared batch keys and adapters without support are rejected', async () => {
		const { enqueue } = recorder()
		const producer = bindCreateJob({ queues: [sync] as const, enqueue })
		await expect(
			producer.createJob('sync', { user: 'a' }, { idempotencyKey: '' })
		).rejects.toThrow('idempotencyKey must be a non-empty string')
		await expect(
			producer.createJob(
				'sync',
				{ user: 'a' },
				{ idempotencyTtl: '1y' as '1d' }
			)
		).rejects.toThrow(RangeError)
		await expect(
			producer.createJobs('sync', [{ user: 'a' }], {
				idempotencyKey: 'shared'
			} as never)
		).rejects.toThrow(
			'createJobs cannot use jobKey, jobKeyMode or idempotencyKey'
		)
		const legacy = bindCreateJob({
			queues: [sync] as const,
			enqueue: { addJob: enqueue.addJob, addJobs: enqueue.addJobs }
		})
		await expect(legacy.createJob('sync', { user: 'a' })).rejects.toThrow(
			'idempotencyKey requires an enqueue adapter with addIdempotentJobs'
		)
	})

	test('the harness records keys without deduplicating', async () => {
		const parent = defineQueue({
			name: 'parent',
			inputSchema: z.string(),
			processFn: async (_payload, ctx) => {
				await ctx.createJob(sync, { user: 'a' })
				await ctx.createJob(sync, { user: 'a' })
			}
		})
		const harness = createTestHarness([parent, sync])
		await harness.process('parent', 'go')
		expect(harness.enqueued.map((job) => job.options?.idempotencyKey)).toEqual([
			'sync:a',
			'sync:a'
		])
	})
})

const enabled = Boolean(process.env.DATABASE_URL)
const schema = `BGW_idempotency_${process.pid}`

describe.skipIf(!enabled)('idempotency keys in PostgreSQL', () => {
	const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 30 })
	pool.on('error', () => {})
	let fannedOut = 0
	const queues = defineQueues([
		sync,
		defineQueue({
			name: 'other',
			inputSchema: z.string(),
			processFn: () => {}
		}),
		defineQueue({
			name: 'keyed',
			inputSchema: z.object({ n: z.number() }),
			deriveJobOptions: () => ({ jobKey: 'pending', idempotencyKey: 'once' }),
			processFn: () => {}
		}),
		defineQueue({
			name: 'fanout',
			cron: '0 0 1 1 *',
			inputSchema: z.object({ id: z.string() }),
			deriveJobOptions: ({ id }) => ({ idempotencyKey: id }),
			initFn: () => [{ id: 'a' }, { id: 'b' }],
			processFn: () => {
				fannedOut++
			}
		})
	])
	const worker = createBetterWorker({
		pgPool: pool,
		schema,
		queues,
		hooks: {
			createLogger: () => ({ debug() {}, info() {}, warn() {}, error() {} })
		}
	})
	const count = async (task: string) =>
		Number(
			(
				await pool.query(
					`SELECT count(*) FROM "${schema}"._private_jobs j JOIN "${schema}"._private_tasks t ON t.id = j.task_id WHERE t.identifier = $1`,
					[task]
				)
			).rows[0].count
		)
	const expire = (key: string) =>
		pool.query(
			`UPDATE "${schema}".bgw_idempotency_keys SET expires_at = now() - interval '1 second' WHERE key = $1`,
			[key]
		)

	beforeAll(async () => {
		// Concurrent migrations from several processes must not collide.
		await Promise.all([worker.migrate(), worker.migrate(), worker.migrate()])
	})
	afterAll(async () => {
		await worker.stop()
		await pool.query(`DROP SCHEMA "${schema}" CASCADE`)
		await pool.end()
	})

	test('a key returns the original job after it completed, until the key expires', async () => {
		const first = await worker.createJob('sync', { user: 'u1' })
		await worker.runOnce()
		expect(await count('sync')).toBe(0)
		expect(await worker.jobs.sync({ user: 'u1' })).toBe(first)
		expect(await count('sync')).toBe(0)
		await expire('sync:u1')
		const second = await worker.createJob('sync', { user: 'u1' })
		expect(second).not.toBe(first)
		expect(await count('sync')).toBe(1)
		await worker.runOnce()
	})

	test('keys are scoped per queue', async () => {
		const a = await worker.createJob('other', 'x', { idempotencyKey: 'shared' })
		const b = await worker.createJob(
			'sync',
			{ user: 'x' },
			{
				idempotencyKey: 'shared'
			}
		)
		expect(a).not.toBe(b)
		await worker.runOnce()
	})

	test('concurrent producers create exactly one job', async () => {
		const ids = await Promise.all(
			Array.from({ length: 25 }, () =>
				worker.createJob('sync', { user: 'race' })
			)
		)
		expect(new Set(ids).size).toBe(1)
		expect(await count('sync')).toBe(1)
		await worker.runOnce()
	})

	test('a duplicate leaves a pending jobKey job untouched', async () => {
		const id = await worker.createJob('keyed', { n: 1 })
		expect(await worker.createJob('keyed', { n: 2 })).toBe(id)
		const { rows } = await pool.query(
			`SELECT payload FROM "${schema}"._private_jobs WHERE id = $1`,
			[id]
		)
		expect(rows[0].payload.payload).toEqual({ n: 1 })
		await worker.runOnce()
	})

	test('batches dedupe against earlier jobs and within the batch, in input order', async () => {
		const [earlier] = await worker.createJobs('sync', [{ user: 'b1' }])
		const ids = await worker.createJobs('sync', [
			{ user: 'b2' },
			{ user: 'b1' },
			{ user: 'b2' },
			{ user: 'b3' }
		])
		expect(ids[1]).toBe(earlier!)
		expect(ids[2]).toBe(ids[0]!)
		expect(new Set(ids).size).toBe(3)
		expect(await count('sync')).toBe(3)
		await worker.runOnce()
	})

	test('cron-init fan-out runs each derived key once per TTL', async () => {
		await worker.triggerCron('fanout')
		await worker.runOnce()
		await worker.triggerCron('fanout')
		await worker.runOnce()
		expect(fannedOut).toBe(2)
	})

	test('prepared SQL reports duplicates and releases the key on rollback', async () => {
		const client = createJobClient({ pgPool: pool, schema, queues })
		const job = await client.prepareJob('sync', { user: 'tx' })
		const connection = await pool.connect()
		try {
			await connection.query('BEGIN')
			await connection.query(job.text, job.values)
			await connection.query('ROLLBACK')
			await connection.query('BEGIN')
			const [created] = (await connection.query(job.text, job.values)).rows
			await connection.query('COMMIT')
			const [duplicate] = (await connection.query(job.text, job.values)).rows
			expect(created).toEqual({ id: expect.any(String), deduplicated: false })
			expect(duplicate).toEqual({ id: created.id, deduplicated: true })
		} finally {
			connection.release()
		}
		expect(await count('sync')).toBe(1)
		await worker.runOnce()
	})

	test('worker maintenance prunes expired keys', async () => {
		await worker.createJob('other', 'p', { idempotencyKey: 'stale' })
		await expire('stale')
		await worker.createJob('other', 'p', { idempotencyKey: 'fresh' })
		await worker.runOnce()
		const { rows } = await pool.query(
			`SELECT key FROM "${schema}".bgw_idempotency_keys WHERE key IN ('stale', 'fresh')`
		)
		expect(rows).toEqual([{ key: 'fresh' }])
	})
})
