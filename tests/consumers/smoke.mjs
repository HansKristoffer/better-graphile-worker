import assert from 'node:assert/strict'
import * as root from 'better-graphile-worker'
import * as client from 'better-graphile-worker/client'
import * as advanced from 'better-graphile-worker/advanced'
import { createTestHarness } from 'better-graphile-worker/testing'
import { createCli } from 'better-graphile-worker/cli'
import pg from 'pg'
import { z } from 'zod'

assert.equal(root.createJobClient, client.createJobClient)
assert.equal(typeof advanced.createWorkerClient, 'function')
for (const name of ['createQueue', 'setOtelApi', 'bindCreateJob', 'createJobsApi', 'buildTaskList', 'createWorkerClient', 'getOtel']) assert.equal(Object.hasOwn(root, name), false)
assert.equal(advanced.getOtel(), null)
assert.equal(typeof createTestHarness, 'function')
assert.equal(typeof createCli, 'function')
if (process.env.DATABASE_URL) {
	const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL })
	pool.on('error', () => {})
	pool.on('connect', connection => connection.on('error', () => {}))
	const schema = `bgw_consumer_${process.pid}`
	let calls = 0
	let children = 0
	const quiet = { debug() {}, info() {}, warn() {}, error() {} }
	const child = root.defineQueue({
		name: 'child', inputSchema: z.number(),
		processFn: length => { assert.equal(length, 4); children++ }
	})
	const contracts = root.defineQueues([
		root.defineQueue({
			name: 'work',
			inputSchema: z.string().refine(async value => value.length > 0).transform(value => value.length),
			processFn: async (length, ctx) => {
				assert.equal(length, 4)
				const [a, b] = await Promise.all([ctx.step.run('a', () => ({ length })), ctx.step.run('b', () => undefined)])
				assert.deepEqual(a, { length: 4 }); assert.equal(b, undefined); calls++
				await ctx.createJob(child, length)
			}
		}),
		root.defineQueue({
			name: 'fail', inputSchema: z.null(),
			processFn: payload => { assert.equal(payload, null); throw new root.NonRetriableError('retained') }
		}),
		child
	])
	const worker = root.createBetterWorker({ pgPool: pool, schema, queues: contracts, hooks: { createLogger: () => quiet } })
	try {
		await worker.migrate()
		const producer = client.createJobClient({ pgPool: pool, schema, queues: contracts })
		await producer.createJobs('work', ['text', 'text'])
		const id = await producer.createJob('fail', null)
		await worker.runOnce()
		assert.equal(calls, 2)
		assert.equal(children, 2)
		assert.equal((await worker.getJobStats()).find(row => row.taskIdentifier === 'fail').failed, 1)
		await worker.retryJobs([id])
		assert.equal((await worker.listJobs({ queue: 'fail', state: 'pending' }))[0].attempts, 0)
		await producer.release()
	} finally {
		await worker.stop()
		await pool.query(`DROP SCHEMA "${schema}" CASCADE`)
		await pool.end()
	}
}
console.log(`Packed Node ${process.version} entry points and runtime passed without OpenTelemetry`)
