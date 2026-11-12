import { afterAll, describe, expect, test } from 'bun:test'
import { Pool } from 'pg'
import { z } from 'zod'
import { createJobClient } from './job-client.js'
import { defineQueue } from './queue.js'

const suite = describe.skipIf(!process.env.DATABASE_URL)
const schema = `bgw_client_${process.pid}`

suite('job client on PostgreSQL', () => {
	const pool = new Pool({ connectionString: process.env.DATABASE_URL })
	pool.on('error', () => {})
	afterAll(async () => {
		await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
		await pool.end()
	})

	test('autoMigrate creates the schema before the first enqueue, once', async () => {
		const client = createJobClient({
			pgPool: pool,
			schema,
			autoMigrate: true,
			queues: [
				defineQueue({
					name: 'keyed',
					inputSchema: z.string(),
					deriveJobOptions: (id) => ({ jobKey: `keyed:${id}` })
				}),
				defineQueue({ name: 'hourly', cron: '0 * * * *' })
			]
		})
		const [first, second] = await Promise.all([
			client.createJob('keyed', 'a'),
			client.enqueueByName('hourly')
		])
		expect(first).toBeString()
		expect(second).toBeString()
		expect(await client.getJobByKey('keyed:a')).toMatchObject({
			id: first,
			payload: 'a'
		})
		await client.release()
	})
})
