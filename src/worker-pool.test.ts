import { afterAll, describe, expect, expectTypeOf, test } from 'bun:test'
import * as otel from '@opentelemetry/api'
import { Pool } from 'pg'
import { z } from 'zod'
import { defineQueue } from './queue.js'
import { defineQueues } from './define-queues.js'
import { createBetterWorkerPool, pgPoolSize } from './worker-pool.js'
import { secondLookListener } from './worker.js'
import type { OtelApi, OtelMeter } from './otel.js'

const quiet = { debug() {}, info() {}, warn() {}, error() {} }
const reply = defineQueue({
	name: 'reply',
	runner: 'turn',
	inputSchema: z.object({ id: z.string() })
})
const digest = defineQueue({
	name: 'digest',
	runner: 'background',
	inputSchema: z.object({ id: z.string() })
})
const loose = defineQueue({ name: 'loose', inputSchema: z.string() })
const contracts = defineQueues([reply, digest])
const handlers = { reply: () => {}, digest: () => {} }

describe('createBetterWorkerPool', () => {
	test('every runner a queue names must be configured, and every queue needs a runner', () => {
		// Type checks only; both calls would throw.
		void (() =>
			createBetterWorkerPool({
				pgPool: {} as Pool,
				queues: contracts,
				handlers,
				// @ts-expect-error `background` has no runner options.
				runners: { turn: {} }
			}))
		void (() =>
			createBetterWorkerPool({
				pgPool: {} as Pool,
				// @ts-expect-error A pooled queue must name its runner.
				queues: [loose],
				handlers: { loose: () => {} },
				runners: {}
			}))
		expectTypeOf(reply.runner).toEqualTypeOf<'turn'>()
	})

	test('rejects queues without a configured runner and runners without queues', () => {
		const create = (runners: Record<string, object>, queues = contracts) =>
			createBetterWorkerPool({
				pgPool: {} as Pool,
				queues,
				handlers,
				runners
			} as never)
		expect(() => create({ turn: {} })).toThrow(
			'Queue "digest" uses unknown runner "background"'
		)
		expect(() => create({ turn: {}, background: {}, idle: {} })).toThrow(
			'Runner "idle" has no queues'
		)
		expect(() => create({})).toThrow('at least one runner')
		expect(() =>
			create({ turn: {} }, [
				defineQueue({ name: 'reply', inputSchema: z.string() })
			] as never)
		).toThrow('Queue "reply" needs a runner')
	})

	test('pgPoolSize covers each runner and its LISTEN and maintenance connections', () => {
		expect(
			pgPoolSize({ turn: { concurrency: 10 }, background: { concurrency: 4 } })
		).toBe(18)
		expect(pgPoolSize({ default: {} })).toBe(12)
	})
})

describe('secondLookOnNotify', () => {
	test('nudges the pool once after a new-job notification, unless it is shutting down', async () => {
		const listener = secondLookListener(0)
		expect(secondLookListener(undefined)).toBeUndefined()
		const nudges: number[] = []
		const workerPool = {
			_active: true,
			_shuttingDown: false,
			nudge: (n: number) => nudges.push(n)
		}
		const notify = (channel: string) =>
			listener?.({ workerPool, message: { channel } } as never)

		notify('jobs:insert')
		notify('worker:migrate')
		await new Promise((resolve) => setTimeout(resolve, 0))
		expect(nudges).toEqual([1])

		notify('jobs:insert')
		workerPool._shuttingDown = true
		await new Promise((resolve) => setTimeout(resolve, 0))
		expect(nudges).toEqual([1])
	})
})

const suite = describe.skipIf(!process.env.DATABASE_URL)
const schema = `bgw_pool_${process.pid}`

suite('worker pool on PostgreSQL', () => {
	const pool = new Pool({ connectionString: process.env.DATABASE_URL })
	pool.on('error', () => {})
	afterAll(async () => {
		await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
		await pool.end()
	})

	test('each runner executes only its queues; enqueue, history and metrics cover all', async () => {
		const lockedBy = new Map<string, string | null>()
		const done = new Map<string, () => void>()
		const finished = (queue: string) =>
			new Promise<void>((resolve) => done.set(queue, resolve))
		const counted: { name: string; attributes?: unknown }[] = []
		const meter: OtelMeter = {
			createCounter: (name) => ({
				add: (_, attributes) => counted.push({ name, attributes })
			}),
			createHistogram: () => ({ record() {} })
		}
		const api = {
			...otel,
			metrics: { getMeter: () => meter }
		} as unknown as OtelApi
		const record =
			(queue: string) =>
			(_: unknown, ctx: { helpers: { job: { locked_by: string | null } } }) => {
				lockedBy.set(queue, ctx.helpers.job.locked_by)
			}
		const workers = createBetterWorkerPool({
			pgPool: pool,
			schema,
			queues: contracts,
			handlers: { reply: record('reply'), digest: record('digest') },
			completedJobs: { maxPerQueue: 10 },
			otel: { api, metrics: true },
			hooks: {
				createLogger: () => quiet,
				onJobFinished: ({ queue }) => done.get(queue)?.()
			},
			runners: {
				turn: { concurrency: 2, pollInterval: 60_000, secondLookOnNotify: 10 },
				background: { concurrency: 1, pollInterval: 60_000 }
			}
		})
		await workers.migrate()
		await workers.start()
		try {
			const ran = Promise.all([finished('reply'), finished('digest')])
			await workers.jobs.reply({ id: 'r' })
			await workers.createJob('digest', { id: 'd' })
			await ran
		} finally {
			await workers.stop()
		}

		const reply = lockedBy.get('reply')
		const digest = lockedBy.get('digest')
		expect(reply).toBeString()
		expect(digest).toBeString()
		expect(reply).not.toBe(digest)
		const history = workers
			.getCompletedJobs()
			.map((job) => job.queueName)
			.sort()
		expect(history).toEqual(['digest', 'reply'])
		expect(workers.runners.turn.getCompletedJobs()).toHaveLength(2)
		expect(
			counted.filter((entry) => entry.name === 'graphile_job_total')
		).toHaveLength(2)
	})
})
