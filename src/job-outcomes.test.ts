import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import { Pool } from 'pg'
import { z } from 'zod'
import { defineQueue } from './queue.js'
import { defineQueues } from './define-queues.js'
import { createBetterWorker } from './create-better-worker.js'
import { createJobClient } from './job-client.js'
import { NonRetriableError } from './errors.js'
import type { JobState } from './job-outcomes.js'

const enabled = Boolean(process.env.DATABASE_URL)
const suite = describe.skipIf(!enabled)
const schema = `BGW_outcomes_${process.pid}`
const quiet = { debug() {}, info() {}, warn() {}, error() {} }

suite('durable job outcomes', () => {
	const pool = new Pool({ connectionString: process.env.DATABASE_URL })
	pool.on('error', () => {})
	const seenWhileRunning: JobState[] = []
	let lookup: (id: string) => Promise<JobState> = async () => {
		throw new Error('not ready')
	}
	let continued = false
	const queues = defineQueues([
		defineQueue({
			name: 'ok',
			inputSchema: z.string(),
			serial: true,
			processFn: async (_input, ctx) => {
				seenWhileRunning.push(await lookup(ctx.jobId))
			}
		}),
		defineQueue({
			name: 'slice',
			inputSchema: z.string(),
			processFn: async (_input, ctx) => {
				if (continued) return
				continued = true
				await ctx.continue({ runAt: new Date(Date.now() + 3_600_000) })
			}
		}),
		defineQueue({
			name: 'flaky',
			inputSchema: z.string(),
			maxAttempts: 2,
			processFn: () => {
				throw new Error('flaked')
			}
		}),
		defineQueue({
			name: 'fatal',
			inputSchema: z.string(),
			processFn: () => {
				throw new NonRetriableError('bad input')
			}
		})
	])
	const options = {
		pgPool: pool,
		schema,
		queues,
		hooks: { createLogger: () => quiet }
	}
	const plain = createBetterWorker(options)
	const worker = createBetterWorker({
		...options,
		jobOutcomes: {},
		permanentFailure: 'discard'
	})
	const client = createJobClient({ pgPool: pool, schema, queues })
	const table = `"${schema}".bgw_job_outcomes`
	const outcome = async (id: string) =>
		(
			await pool.query(
				`SELECT state, continued_to::text, last_error FROM ${table} WHERE job_id = $1`,
				[id]
			)
		).rows[0]

	beforeAll(async () => {
		await plain.migrate()
		lookup = (id) => worker.getJob(id)
	})
	afterAll(async () => {
		await client.release()
		await plain.stop()
		await worker.stop()
		await pool.query(`DROP SCHEMA "${schema}" CASCADE`)
		await pool.end()
	})

	test('is off by default: no table and nothing to look up after completion', async () => {
		const id = (await plain.createJob('ok', 'plain'))!
		expect(await plain.getJob(id)).toMatchObject({ state: 'pending', id })
		await plain.runOnce()
		expect(await plain.getJob(id)).toEqual({ state: 'not_found', id })
		const exists = await pool.query('SELECT to_regclass($1) AS name', [table])
		expect(exists.rows[0].name).toBeNull()
	})

	test('records completions atomically, releases serial lanes, and prunes expired rows', async () => {
		await worker.migrate()
		seenWhileRunning.length = 0
		await pool.query(
			`INSERT INTO ${table} (job_id, task_identifier, state, attempts, max_attempts, created_at, finished_at)
			VALUES (1, 'ok', 'completed', 1, 1, now(), now() - interval '30 days')`
		)
		const later = new Date(Date.now() + 3_600_000)
		const scheduled = (await worker.createJob('ok', 'later', { runAt: later }))!
		const [first, second] = (await worker.createJobs('ok', ['a', 'b'])) as [
			string,
			string
		]
		expect(await client.getJob(scheduled)).toMatchObject({
			state: 'pending',
			queue: 'ok',
			attempts: 0,
			runAt: later.toISOString()
		})

		await worker.runOnce()

		expect(seenWhileRunning.map((job) => [job.id, job.state])).toEqual([
			[first, 'running'],
			[second, 'running']
		])
		for (const id of [first, second])
			expect(await client.getJob(id)).toMatchObject({
				state: 'completed',
				id,
				queue: 'ok',
				attempts: 1,
				lastError: null,
				finishedAt: expect.any(String)
			})
		expect(await outcome('1')).toBeUndefined()
		expect((await client.getJob(scheduled)).state).toBe('pending')
	})

	test('a continued job points at its continuation', async () => {
		const id = (await worker.createJob('slice', 'run'))!
		await worker.runOnce()
		const state = await worker.getJob(id)
		expect(state).toMatchObject({ state: 'continued', id, attempts: 1 })
		const next = state.state === 'continued' ? state.continuedTo : ''
		expect(await worker.getJob(next)).toMatchObject({
			state: 'pending',
			queue: 'slice',
			attempts: 0
		})
	})

	test('retrying, exhausted and discarded failures', async () => {
		const flaky = (await worker.createJob('flaky', 'x'))!
		const fatal = (await worker.createJob('fatal', 'x'))!
		await worker.runOnce()

		expect(await worker.getJob(flaky)).toMatchObject({
			state: 'retrying',
			attempts: 1,
			maxAttempts: 2,
			lastError: expect.stringContaining('flaked')
		})
		// Discarded jobs leave the jobs table; only the outcome remembers them.
		expect(await worker.getJob(fatal)).toMatchObject({
			state: 'failed',
			attempts: 1,
			lastError: 'bad input'
		})
		expect(await outcome(fatal)).toMatchObject({ state: 'failed' })

		await pool.query(
			`UPDATE "${schema}"._private_jobs SET run_at = now() WHERE id = $1`,
			[flaky]
		)
		await worker.runOnce()
		expect(await worker.getJob(flaky)).toMatchObject({
			state: 'failed',
			attempts: 2,
			lastError: expect.stringContaining('flaked')
		})
		expect(await outcome(flaky)).toMatchObject({
			state: 'failed',
			last_error: 'flaked'
		})

		// A retried failure is live again and wins over its old outcome.
		await worker.retryJobs([flaky])
		expect((await worker.getJob(flaky)).state).toBe('pending')
		await pool.query(`DELETE FROM "${schema}"._private_jobs WHERE id = $1`, [
			flaky
		])
		expect((await worker.getJob(flaky)).state).toBe('failed')
	})

	test('unknown ids are not found and malformed ids are rejected', async () => {
		expect(await client.getJob('987654321')).toEqual({
			state: 'not_found',
			id: '987654321'
		})
		expect(client.getJob('1; DROP')).rejects.toThrow(RangeError)
	})
})
