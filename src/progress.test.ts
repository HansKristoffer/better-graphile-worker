import {
	afterAll,
	beforeAll,
	describe,
	expect,
	expectTypeOf,
	test
} from 'bun:test'
import { Pool } from 'pg'
import { z } from 'zod'
import type { JobHelpers } from 'graphile-worker'
import { createProgressRunner } from './progress.js'
import { defineQueue, type QueueHandlers } from './queue.js'
import { defineQueues } from './define-queues.js'
import { createBetterWorker } from './create-better-worker.js'
import { createJobClient } from './job-client.js'
import { createTestHarness } from './testing.js'
import { writeProgress } from './private-jobs.js'
import {
	NonRetriableError,
	StepPersistenceError,
	UnknownQueueError
} from './errors.js'
import type { JsonValue } from './validation.js'

function recordingRunner(
	options: { schema?: z.ZodType; fail?: () => boolean } = {}
) {
	const writes: unknown[] = []
	const runner = createProgressRunner({
		initial: undefined,
		schema: options.schema,
		write: async (value) => {
			if (options.fail?.()) throw new Error('db down')
			writes.push(value)
		}
	})
	return { runner, writes }
}

describe('createProgressRunner', () => {
	test('coalesces a tight loop into a leading and a final write', async () => {
		const { runner, writes } = recordingRunner()
		for (let i = 0; i < 100; i++) await runner.set({ processed: i })
		expect(runner.get()).toEqual({ processed: 99 })
		await runner.flush()
		expect(writes).toHaveLength(2)
		expect(writes.at(-1)).toEqual({ processed: 99 })
	})

	test('validates with the schema and rejects non-JSON values', async () => {
		const { runner, writes } = recordingRunner({
			schema: z.object({ phase: z.string(), done: z.number().default(0) })
		})
		await expect(runner.set({ phase: 1 })).rejects.toBeInstanceOf(z.ZodError)
		await runner.set({ phase: 'fetch' })
		expect(runner.get()).toEqual({ phase: 'fetch', done: 0 })
		const untyped = recordingRunner().runner
		await expect(untyped.set(new Date())).rejects.toBeInstanceOf(
			NonRetriableError
		)
		await runner.flush()
		expect(writes).toEqual([{ phase: 'fetch', done: 0 }])
	})

	test('stores a copy, so later mutation does not leak into writes', async () => {
		const { runner, writes } = recordingRunner()
		const value = { items: [1] }
		await runner.set(value)
		value.items.push(2)
		await runner.flush()
		expect(writes).toEqual([{ items: [1] }])
	})

	test('a failed write rejects the next call once and is retried', async () => {
		let failing = true
		const { runner, writes } = recordingRunner({ fail: () => failing })
		await runner.set(1)
		await expect(runner.flush()).rejects.toThrow('db down')
		failing = false
		await runner.flush()
		expect(writes).toEqual([1])
	})

	test('close persists on request and then refuses new values', async () => {
		const { runner, writes } = recordingRunner()
		await runner.set('a')
		await runner.set('b')
		await runner.close(true)
		expect(writes).toEqual(['a', 'b'])
		await expect(runner.set('c')).rejects.toThrow('after the job finished')
		expect(runner.get()).toBe('b')
	})

	test('close without persisting drops the pending trailing write', async () => {
		const { runner, writes } = recordingRunner()
		await runner.set('a')
		await runner.set('b')
		await runner.close(false)
		expect(writes).toEqual(['a'])
	})
})

describe('progress types', () => {
	test('progressSchema types inline handlers, registries and readers', () => {
		const typed = defineQueue({
			name: 'typed',
			inputSchema: z.object({ id: z.string() }),
			progressSchema: z.object({ phase: z.string(), retriesLeft: z.number() }),
			processFn: async (_payload, ctx) => {
				expectTypeOf(ctx.progress.get()).toEqualTypeOf<
					{ phase: string; retriesLeft: number } | undefined
				>()
				// @ts-expect-error Progress must match progressSchema input
				await ctx.progress.set({ phase: 1 })
			}
		})
		const untyped = defineQueue({
			name: 'untyped',
			inputSchema: z.string(),
			processFn: (_payload, ctx) => {
				expectTypeOf(ctx.progress.get()).toEqualTypeOf<JsonValue | undefined>()
			}
		})
		const worker = createBetterWorker({
			pgPool: {} as Pool,
			queues: defineQueues([typed, untyped])
		})
		expectTypeOf<
			Awaited<ReturnType<typeof worker.getJobProgress<'typed'>>>
		>().toEqualTypeOf<{
			progress: { phase: string; retriesLeft: number } | undefined
		} | null>()

		const contract = defineQueue({
			name: 'contract',
			inputSchema: z.string(),
			progressSchema: z.number()
		})
		const handlers = {
			contract: async (_payload, ctx) => {
				expectTypeOf(ctx.progress.get()).toEqualTypeOf<number | undefined>()
			}
		} satisfies QueueHandlers<readonly [typeof contract]>
		const client = createJobClient({
			pgPool: {} as Pool,
			queues: defineQueues([contract])
		})
		expectTypeOf<
			Awaited<ReturnType<typeof client.getJobProgress<'contract'>>>
		>().toEqualTypeOf<{ progress: number | undefined } | null>()
		// @ts-expect-error Unknown queue
		void (() => client.getJobProgress('missing', '1'))
		void handlers
	})
})

describe('test harness progress', () => {
	test('validates, and a retry with the same jobId sees the latest value', async () => {
		const seen: unknown[] = []
		const budget = defineQueue({
			name: 'budget',
			inputSchema: z.object({}),
			progressSchema: z.object({ retriesLeft: z.number().int().min(0) }),
			processFn: async (_payload, ctx) => {
				const left = ctx.progress.get()?.retriesLeft ?? 2
				seen.push(left)
				await ctx.progress.set({ retriesLeft: left - 1 })
				if (left > 1) throw new Error('try again')
			}
		})
		const harness = createTestHarness([budget])
		const extras = { jobId: 'run-1' }
		await expect(harness.process('budget', {}, extras)).rejects.toThrow(
			'try again'
		)
		const { ctx } = await harness.process('budget', {}, extras)
		expect(seen).toEqual([2, 1])
		expect(ctx.progress.get()).toEqual({ retriesLeft: 0 })
		await expect(harness.process('budget', {}, extras)).rejects.toBeInstanceOf(
			z.ZodError
		)
	})
})

const enabled = Boolean(process.env.DATABASE_URL)
const suite = describe.skipIf(!enabled)
const schema = `BGW_progress_${process.pid}`
const quiet = { debug() {}, info() {}, warn() {}, error() {} }

suite('progress in PostgreSQL', () => {
	const pool = new Pool({ connectionString: process.env.DATABASE_URL })
	pool.on('error', () => {})
	let release: () => void = () => {}
	let reached: () => void = () => {}
	const attempts: Array<unknown> = []
	const contracts = defineQueues([
		defineQueue({
			name: 'sync',
			inputSchema: z.object({ mode: z.enum(['fail', 'wait', 'continue']) }),
			progressSchema: z.object({ phase: z.string(), done: z.number() }),
			maxAttempts: 3
		})
	])
	const worker = createBetterWorker({
		pgPool: pool,
		schema,
		queues: contracts,
		hooks: { createLogger: () => quiet },
		// Outcomes complete jobs in our own transaction; progress must not interfere.
		jobOutcomes: {},
		handlers: {
			sync: async ({ mode }, ctx) => {
				attempts.push(ctx.progress.get())
				if (mode === 'wait') {
					await ctx.progress.set({ phase: 'fetch', done: 1 })
					await ctx.progress.flush()
					reached()
					await new Promise<void>((resolve) => {
						release = resolve
					})
					return
				}
				const done = (ctx.progress.get()?.done ?? 0) + 1
				// A throttled, unflushed value must still reach the retry or continuation.
				await ctx.progress.set({ phase: 'start', done })
				await ctx.progress.set({ phase: mode, done })
				if (mode === 'fail' && ctx.attempt === 1) throw new Error('retry me')
				if (mode === 'continue' && ctx.attempt === 1 && done === 1)
					await ctx.continue()
			}
		}
	})
	const client = createJobClient({ pgPool: pool, schema, queues: contracts })
	const lockFor = (id: string) =>
		pool.query(
			`UPDATE "${schema}"._private_jobs SET locked_by = 'tester', locked_at = now(), attempts = attempts + 1 WHERE id = $1 RETURNING *`,
			[id]
		)
	beforeAll(() => worker.migrate())
	afterAll(async () => {
		await worker.stop()
		await client.release()
		await pool.query(`DROP SCHEMA "${schema}" CASCADE`)
		await pool.end()
	})

	test('a retry sees the latest progress, readable by job id', async () => {
		attempts.length = 0
		const id = (await worker.createJob('sync', { mode: 'fail' }))!
		expect(await client.getJobProgress('sync', id)).toEqual({
			progress: undefined
		})
		await worker.runOnce()
		expect(await client.getJobProgress('sync', id)).toEqual({
			progress: { phase: 'fail', done: 1 }
		})
		await pool.query(`UPDATE "${schema}"._private_jobs SET run_at = now()`)
		await worker.runOnce()
		expect(attempts).toEqual([undefined, { phase: 'fail', done: 1 }])
		// Completed jobs are deleted by Graphile, progress included.
		expect(await worker.getJobProgress('sync', id)).toBeNull()
	})

	test('progress is visible while the job runs', async () => {
		const id = (await worker.createJob('sync', { mode: 'wait' }))!
		const running = new Promise<void>((resolve) => {
			reached = resolve
		})
		const run = worker.runOnce()
		await running
		expect(await worker.getJobProgress('sync', id)).toEqual({
			progress: { phase: 'fetch', done: 1 }
		})
		release()
		await run
	})

	test('a continuation carries the latest progress', async () => {
		attempts.length = 0
		const id = (await worker.createJob('sync', { mode: 'continue' }))!
		await worker.runOnce()
		expect(attempts).toEqual([undefined, { phase: 'continue', done: 1 }])
		const job = await worker.getJob(id)
		if (job.state !== 'continued') throw new Error(`job is ${job.state}`)
		expect((await worker.getJob(job.continuedTo)).state).toBe('completed')
	})

	test('writes require the current lock', async () => {
		const id = (await worker.createJob('sync', { mode: 'fail' }))!
		const job = (await lockFor(id)).rows[0]
		const utils = await worker.getWorkerUtils()
		const owner = { job, withPgClient: utils.withPgClient } as JobHelpers
		await writeProgress(owner, schema, id, { phase: 'owner', done: 1 })
		const stale = { ...owner, job: { ...job, locked_by: 'stale' } }
		await expect(
			writeProgress(stale as JobHelpers, schema, id, {
				phase: 'stale',
				done: 2
			})
		).rejects.toBeInstanceOf(StepPersistenceError)
		expect(await worker.getJobProgress('sync', id)).toEqual({
			progress: { phase: 'owner', done: 1 }
		})
		await pool.query(`DELETE FROM "${schema}"._private_jobs`)
	})

	test('rejects unknown queues and malformed ids before querying', async () => {
		await expect(
			worker.getJobProgress('nope' as 'sync', '1')
		).rejects.toBeInstanceOf(UnknownQueueError)
		await expect(worker.getJobProgress('sync', '1; drop')).rejects.toThrow(
			RangeError
		)
		expect(await worker.getJobProgress('sync', '999999')).toBeNull()
	})
})
