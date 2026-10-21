import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import { Pool, type PoolClient } from 'pg'
import { z } from 'zod'
import { createJobClient } from './job-client.js'
import { defineQueue } from './queue.js'
import { enqueueInTransaction, type PrismaRawClient } from './prisma.js'
import type { JobRequest, PreparedJob } from './types.js'

const prepared = (count: number): PreparedJob => ({
	queue: 'q',
	text: 'SELECT',
	values: [],
	count,
	skipped: false
})
const rows = (...ids: (string | null)[]): PrismaRawClient => ({
	$queryRawUnsafe: async <T>() => ids.map((id) => ({ id })) as T
})

describe('enqueueInTransaction', () => {
	test('returns the job ids in order', async () => {
		expect(await enqueueInTransaction(rows('1', '2'), prepared(2))).toEqual([
			'1',
			'2'
		])
	})

	test('a skipped or empty statement runs nothing', async () => {
		const tx: PrismaRawClient = {
			$queryRawUnsafe: () => {
				throw new Error('ran')
			}
		}
		expect(
			await enqueueInTransaction(tx, { ...prepared(0), skipped: true })
		).toEqual([])
	})

	test('throws, rolling the transaction back, when a job is missing', async () => {
		await expect(
			enqueueInTransaction(rows('1', null), prepared(2))
		).rejects.toThrow('1 of 2 jobs')
		await expect(enqueueInTransaction(rows('1'), prepared(2))).rejects.toThrow(
			'1 of 2 jobs'
		)
	})
})

describe('enqueueInTransaction on a producer', () => {
	const charge = defineQueue({
		name: 'charge',
		inputSchema: z.object({ id: z.string() })
	})
	const nightly = defineQueue({ name: 'nightly', cron: '0 3 * * *' })
	const queues = [charge, nightly] as const
	const producer = (transactionalJobKeyMode?: 'unsafe_dedupe') =>
		createJobClient({
			pgPool: new Pool(),
			queues,
			...(transactionalJobKeyMode ? { transactionalJobKeyMode } : {})
		})
	/** Records each statement's values and answers with one job id. */
	const recording = () => {
		const calls: unknown[][] = []
		const tx: PrismaRawClient = {
			$queryRawUnsafe: async <T>(_text: string, ...values: unknown[]) => {
				calls.push(values)
				return [{ id: '7' }] as T
			}
		}
		return { tx, calls }
	}

	test('a union of jobs across queues enqueues without casts', async () => {
		const { tx, calls } = recording()
		const jobs: JobRequest<typeof queues>[] = [
			{ task: 'charge', payload: { id: 'a' }, jobKey: 'charge:a' },
			{ task: 'nightly' }
		]
		const client = producer()
		for (const job of jobs)
			expect(await client.enqueueInTransaction(tx, job)).toBe('7')
		expect(calls.map((values) => values[0])).toEqual(['charge', 'nightly'])
		// Type checks only; never called.
		void (() => {
			void client.enqueueInTransaction(tx, {
				task: 'charge',
				// @ts-expect-error the payload must match the task's input
				payload: { id: 1 }
			})
			// @ts-expect-error the task must name a queue
			void client.enqueueInTransaction(tx, { task: 'missing' })
		})
	})

	test('transactionalJobKeyMode fills a keyed job without a mode', async () => {
		const { tx, calls } = recording()
		const client = producer('unsafe_dedupe')
		await client.enqueueInTransaction(tx, {
			task: 'charge',
			payload: { id: 'a' },
			jobKey: 'k'
		})
		await client.enqueueInTransaction(tx, {
			task: 'charge',
			payload: { id: 'a' },
			jobKey: 'k',
			jobKeyMode: 'replace'
		})
		const prepared = await client.prepareJob(
			'charge',
			{ id: 'a' },
			{ jobKey: 'k' }
		)
		expect(calls.map((values) => values.at(-1))).toEqual([
			'unsafe_dedupe',
			'replace'
		])
		// Other enqueue paths keep Graphile's default.
		expect(prepared.values.at(-1)).toBe('replace')
	})

	test('rejects an unknown transactionalJobKeyMode', () => {
		expect(() =>
			createJobClient({
				pgPool: new Pool(),
				queues,
				transactionalJobKeyMode: 'keep' as never
			})
		).toThrow('transactionalJobKeyMode')
	})
})

const suite = describe.skipIf(!process.env.DATABASE_URL)
const schema = `bgw_prisma_${process.pid}`

suite('enqueueInTransaction on PostgreSQL', () => {
	const pool = new Pool({ connectionString: process.env.DATABASE_URL })
	pool.on('error', () => {})
	const queue = defineQueue({
		name: 'charge',
		inputSchema: z.object({ id: z.string() })
	})
	const client = createJobClient({ pgPool: pool, schema, queues: [queue] })
	/** The same `$queryRawUnsafe` contract Prisma's transaction client has. */
	const asPrisma = (connection: PoolClient): PrismaRawClient => ({
		$queryRawUnsafe: async <T>(text: string, ...values: unknown[]) =>
			(await connection.query(text, values)).rows as T
	})
	const count = async () =>
		Number(
			(await pool.query(`SELECT count(*) FROM "${schema}".jobs`)).rows[0].count
		)

	beforeAll(() => client.migrate())
	afterAll(async () => {
		await client.release()
		await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
		await pool.end()
	})

	test('jobs commit and roll back with the transaction', async () => {
		const one = await client.prepareJob('charge', { id: 'a' })
		const many = await client.prepareJobs('charge', [{ id: 'b' }, { id: 'c' }])
		const connection = await pool.connect()
		try {
			await connection.query('BEGIN')
			expect(
				await enqueueInTransaction(asPrisma(connection), one)
			).toHaveLength(1)
			await connection.query('ROLLBACK')
			expect(await count()).toBe(0)

			await connection.query('BEGIN')
			const ids = await enqueueInTransaction(asPrisma(connection), many)
			await connection.query('COMMIT')
			expect(ids).toHaveLength(2)
			expect(await count()).toBe(2)
		} finally {
			connection.release()
		}
	})

	test('a typed transactional job keeps the pending job with unsafe_dedupe', async () => {
		const dedupe = createJobClient({
			pgPool: pool,
			schema,
			queues: [queue],
			transactionalJobKeyMode: 'unsafe_dedupe'
		})
		const connection = await pool.connect()
		try {
			await connection.query('BEGIN')
			const tx = asPrisma(connection)
			const first = await dedupe.enqueueInTransaction(tx, {
				task: 'charge',
				payload: { id: 'first' },
				jobKey: 'charge:dedupe',
				runAt: new Date('2100-01-01T00:00:00Z')
			})
			const second = await dedupe.enqueueInTransaction(tx, {
				task: 'charge',
				payload: { id: 'second' },
				jobKey: 'charge:dedupe',
				runAt: new Date('2200-01-01T00:00:00Z')
			})
			await connection.query('COMMIT')
			expect(second).toBe(first)
			// `replace` would have moved the pending job to the second run_at.
			const { rows } = await pool.query(
				`SELECT run_at FROM "${schema}".jobs WHERE key = 'charge:dedupe'`
			)
			expect(rows).toHaveLength(1)
			expect(rows[0].run_at.getUTCFullYear()).toBe(2100)
		} finally {
			connection.release()
			await dedupe.release()
		}
	})
})
