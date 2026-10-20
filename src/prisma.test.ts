import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import { Pool, type PoolClient } from 'pg'
import { z } from 'zod'
import { createJobClient } from './job-client.js'
import { defineQueue } from './queue.js'
import { enqueueInTransaction, type PrismaRawClient } from './prisma.js'
import type { PreparedJob } from './types.js'

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
})
