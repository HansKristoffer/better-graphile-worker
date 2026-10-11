import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import { Pool } from 'pg'
import { z } from 'zod'
import { defineQueue } from './queue.js'
import { defineQueues } from './define-queues.js'
import { createBetterWorker } from './create-better-worker.js'
import { RetryAtError } from './errors.js'
import type { JobFinishedEvent, PermanentFailureEvent } from './hooks.js'

describe('RetryAtError', () => {
	test('takes its message from the cause and rejects invalid timestamps', () => {
		const at = new Date('2030-01-01T00:00:00Z')
		expect(new RetryAtError(at, { cause: new Error('429') }).message).toBe(
			'429'
		)
		expect(new RetryAtError(at.toISOString()).runAt).toEqual(at)
		expect(() => new RetryAtError('soon')).toThrow(RangeError)
	})
})

const suite = describe.skipIf(!process.env.DATABASE_URL)
const schema = `BGW_retry_at_${process.pid}`
const quiet = { debug() {}, info() {}, warn() {}, error() {} }

suite('RetryAtError in PostgreSQL', () => {
	const pool = new Pool({ connectionString: process.env.DATABASE_URL })
	pool.on('error', () => {})
	let stepRuns = 0
	const finished: JobFinishedEvent[] = []
	const permanent: PermanentFailureEvent[] = []
	const queues = defineQueues([
		defineQueue({
			name: 'sync',
			inputSchema: z.object({ delayMs: z.number() }),
			serial: 'bank',
			maxAttempts: 2,
			processFn: async ({ delayMs }, ctx) => {
				await ctx.step.run('fetch', () => {
					stepRuns++
				})
				throw new RetryAtError(new Date(Date.now() + delayMs), {
					cause: new TypeError('rate limited')
				})
			}
		}),
		defineQueue({
			name: 'mate',
			inputSchema: z.string(),
			serial: 'bank',
			processFn: () => {}
		})
	])
	const worker = createBetterWorker({
		pgPool: pool,
		schema,
		queues,
		hooks: {
			createLogger: () => quiet,
			onJobFinished: (event) => void finished.push(event),
			onPermanentFailure: (event) => void permanent.push(event)
		}
	})
	const row = async (id: string | null) =>
		(
			await pool.query<{
				attempts: number
				last_error: string | null
				wait_s: number
				payload: { steps?: Record<string, unknown> }
			}>(
				`SELECT attempts, last_error, extract(epoch FROM run_at - now())::float8 AS wait_s, payload
				FROM "${schema}"._private_jobs WHERE id = $1`,
				[id]
			)
		).rows[0]
	beforeAll(() => worker.migrate())
	afterAll(async () => {
		await worker.stop()
		await pool.query(`DROP SCHEMA "${schema}" CASCADE`)
		await pool.end()
	})

	test('spends an attempt, waits until runAt and keeps steps, without blocking its lane', async () => {
		const id = await worker.createJob('sync', { delayMs: 3_600_000 })
		const mate = await worker.createJob('mate', 'after')
		await worker.runOnce()

		const job = await row(id)
		expect(job).toMatchObject({ attempts: 1, last_error: 'rate limited' })
		expect(job!.wait_s).toBeGreaterThan(3_590)
		expect(job!.wait_s).toBeLessThan(3_601)
		expect(Object.keys(job!.payload.steps ?? {})).toEqual(['fetch'])
		const event = finished.find((e) => e.jobId === id)
		expect(event).toMatchObject({
			status: 'failed',
			permanentlyFailed: false,
			errorType: 'TypeError',
			errorMessage: 'rate limited'
		})
		expect(event!.retryAt!.getTime()).toBeGreaterThan(Date.now() + 3_590_000)

		// The lane is free while the retry waits.
		await worker.runOnce()
		expect(await row(mate)).toBeUndefined()
		expect((await row(id))!.attempts).toBe(1)

		// The retry replays the checkpoint, and the last attempt fails permanently.
		await pool.query(
			`UPDATE "${schema}"._private_jobs SET run_at = now() WHERE id = $1`,
			[id]
		)
		finished.length = 0
		await worker.runOnce()
		expect(stepRuns).toBe(1)
		expect(await row(id)).toMatchObject({
			attempts: 2,
			last_error: 'rate limited'
		})
		expect(permanent).toHaveLength(1)
		expect(permanent[0]!.error).toBeInstanceOf(TypeError)
		expect(finished[0]).toMatchObject({ permanentlyFailed: true })
		expect(finished[0]!.retryAt).toBeUndefined()
	})

	test("a runAt sooner than Graphile's default backoff waits for the backoff", async () => {
		const id = await worker.createJob('sync', { delayMs: -60_000 })
		await worker.runOnce()
		const job = await row(id)
		expect(job!.attempts).toBe(1)
		// First retry backoff is exp(1) ≈ 2.7 seconds.
		expect(job!.wait_s).toBeGreaterThan(1)
		expect(job!.wait_s).toBeLessThan(3)
	})
})
