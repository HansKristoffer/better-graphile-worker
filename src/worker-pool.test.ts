import { afterAll, describe, expect, expectTypeOf, test } from 'bun:test'
import * as otel from '@opentelemetry/api'
import { Pool } from 'pg'
import { z } from 'zod'
import { defineQueue } from './queue.js'
import { defineQueues } from './define-queues.js'
import { createBetterWorkerPool, pgPoolSize } from './worker-pool.js'
import { createBetterWorker } from './create-better-worker.js'
import { WorkerStoppedError } from './errors.js'
import type { OtelMeter } from './otel.js'
import type { WorkerEvents, WorkerPool } from 'graphile-worker'
import { EventEmitter } from 'node:events'

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

	test('a typed runner record and a pool prefetch need no cast', () => {
		const lanes: Record<
			'turn' | 'background',
			{ concurrency: number; pollInterval: number }
		> = {
			turn: { concurrency: 10, pollInterval: 100 },
			background: { concurrency: 4, pollInterval: 1000 }
		}
		const workers = createBetterWorkerPool({
			pgPool: new Pool(),
			queues: contracts,
			handlers,
			runners: lanes,
			prefetch: 1
		})
		expectTypeOf(workers.enqueueInTransaction).toBeFunction()
		expect(() =>
			createBetterWorkerPool({
				pgPool: new Pool(),
				queues: contracts,
				handlers,
				runners: lanes,
				prefetch: -1
			})
		).toThrow('prefetch')
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
		const api = { ...otel, metrics: { getMeter: () => meter } }
		const workers = createBetterWorkerPool({
			pgPool: pool,
			schema,
			queues: contracts,
			handlers: {
				reply: (_, ctx) =>
					void lockedBy.set('reply', ctx.helpers.job.locked_by),
				digest: (_, ctx) =>
					void lockedBy.set('digest', ctx.helpers.job.locked_by)
			},
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

	test('secondLookOnNotify fetches once more after a new-job notification', async () => {
		const events: WorkerEvents = new EventEmitter()
		const nudges: number[] = []
		let secondNudge!: () => void
		const nudged = new Promise<void>((resolve) => {
			secondNudge = resolve
		})
		const listening = new Promise((resolve) =>
			events.once('pool:listen:success', resolve)
		)
		events.on('pool:create', ({ workerPool }) => {
			const nudge = workerPool.nudge.bind(workerPool)
			workerPool.nudge = (count) => {
				nudges.push(count)
				if (nudges.length === 2) secondNudge()
				nudge(count)
			}
		})
		const worker = createBetterWorker({
			pgPool: pool,
			schema,
			queues: [reply],
			handlers: { reply: () => {} },
			pollInterval: 60_000,
			secondLookOnNotify: 10,
			graphile: { events },
			hooks: { createLogger: () => quiet }
		})
		await worker.start()
		try {
			await listening
			await worker.createJob('reply', { id: 'notified' })
			await nudged
		} finally {
			await worker.stop()
		}
		// Graphile's own nudge for the insert, then the second look.
		expect(nudges).toEqual([1, 1])
	})

	test("prefetch sizes each runner's local queue; a runner overrides the pool", async () => {
		const localQueueSize = (events: WorkerEvents) =>
			new Promise<number | undefined>((resolve) =>
				events.once('localQueue:init', ({ ctx }) =>
					resolve(ctx.resolvedPreset.worker.localQueue?.size)
				)
			)
		const turn: WorkerEvents = new EventEmitter()
		const background: WorkerEvents = new EventEmitter()
		const sizes = Promise.all([
			localQueueSize(turn),
			localQueueSize(background)
		])
		const workers = createBetterWorkerPool({
			pgPool: pool,
			schema,
			queues: contracts,
			handlers,
			prefetch: 1,
			runners: {
				turn: { concurrency: 3, graphile: { events: turn } },
				background: {
					concurrency: 2,
					prefetch: 0,
					graphile: { events: background }
				}
			},
			hooks: { createLogger: () => quiet }
		})
		await workers.start()
		try {
			expect(await sizes).toEqual([4, 2])
		} finally {
			await workers.stop()
		}
	})

	test('without restarts, a runner that stops on its own stops the pool, which rejects and can start again', async () => {
		const turn: WorkerEvents = new EventEmitter()
		// A WorkerPool is thenable, so it is handed out boxed rather than awaited.
		const created = new Promise<{ workerPool: WorkerPool }>((resolve) =>
			turn.once('pool:create', resolve)
		)
		const workers = createBetterWorkerPool({
			pgPool: pool,
			schema,
			queues: contracts,
			handlers,
			runners: { turn: { graphile: { events: turn } }, background: {} },
			restart: false,
			hooks: { createLogger: () => quiet }
		})
		await workers.start()
		const { workerPool } = await created
		// What Graphile does by itself when it cannot complete jobs or return its local queue.
		void workerPool.gracefulShutdown('Connection terminated unexpectedly')
		const error = await workers.promise.catch((caught: unknown) => caught)
		expect(error).toBeInstanceOf(WorkerStoppedError)
		expect(error).toMatchObject({
			runner: 'turn',
			cause: { message: 'Connection terminated unexpectedly' }
		})
		// The pool stopped the healthy runner; that stop was requested.
		await workers.runners.background.promise

		await workers.start()
		await workers.stop()
		await workers.promise
	})
})
