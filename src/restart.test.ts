import { afterAll, describe, expect, spyOn, test } from 'bun:test'
import { EventEmitter } from 'node:events'
import net, { type AddressInfo } from 'node:net'
import type { WorkerEvents, WorkerPool } from 'graphile-worker'
import { Pool } from 'pg'
import { z } from 'zod'
import { createBetterWorker } from './create-better-worker.js'
import { defineQueue } from './queue.js'
import { createBetterWorkerPool } from './worker-pool.js'
import { WorkerStoppedError } from './errors.js'
import type { LogAttributes, RunnerStoppedEvent } from './hooks.js'

/** A TCP proxy in front of PostgreSQL that can drop every connection and refuse new ones. */
async function databaseProxy(target: URL) {
	const sockets = new Set<net.Socket>()
	let reachable = true
	const server = net.createServer((client) => {
		if (!reachable) return void client.destroy()
		const upstream = net.connect(Number(target.port || 5432), target.hostname)
		for (const socket of [client, upstream]) {
			sockets.add(socket)
			socket.on('error', () => {})
			socket.on('close', () => sockets.delete(socket))
		}
		client.pipe(upstream).pipe(client)
	})
	await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
	const url = new URL(target)
	url.hostname = '127.0.0.1'
	url.port = String((server.address() as AddressInfo).port)
	return {
		url: url.href,
		cut() {
			reachable = false
			for (const socket of sockets) socket.destroy()
		},
		restore() {
			reachable = true
		},
		close: () => new Promise<void>((resolve) => server.close(() => resolve()))
	}
}

/** Collects every log line and resolves once a message has been logged `count` times. */
function recordLogs() {
	const seen: { message: string; attributes: LogAttributes }[] = []
	const waiters: (() => void)[] = []
	const record = (message: string, attributes: LogAttributes = {}) => {
		seen.push({ message, attributes })
		for (const check of [...waiters]) check()
	}
	const matching = (message: string) =>
		seen.filter((line) => line.message === message).map((l) => l.attributes)
	return {
		logger: { debug: record, info: record, warn: record, error: record },
		matching,
		logged(message: string, count = 1) {
			return new Promise<LogAttributes[]>((resolve) => {
				const check = () => {
					const lines = matching(message)
					if (lines.length < count) return
					waiters.splice(waiters.indexOf(check), 1)
					resolve(lines)
				}
				waiters.push(check)
				check()
			})
		}
	}
}

function deferred() {
	let resolve!: () => void
	const promise = new Promise<void>((yes) => {
		resolve = yes
	})
	return { promise, resolve }
}

const suite = describe.skipIf(!process.env.DATABASE_URL)
const schema = `bgw_restart_${process.pid}`

suite('restart after Graphile stops a runner on its own', () => {
	const direct = new Pool({ connectionString: process.env.DATABASE_URL })
	direct.on('error', () => {})
	afterAll(async () => {
		await direct.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
		await direct.end()
	})

	test('a worker that loses its database mid-job keeps restarting until it is back, then runs jobs again', async () => {
		const proxy = await databaseProxy(new URL(process.env.DATABASE_URL!))
		const pgPool = new Pool({ connectionString: proxy.url })
		pgPool.on('error', () => {})
		const started = new Map<string, (lockedBy: string | null) => void>()
		const jobStarted = (id: string) =>
			new Promise<string | null>((resolve) => started.set(id, resolve))
		const finishFirst = deferred()
		const slow = defineQueue({
			name: 'slow',
			inputSchema: z.object({ id: z.string() }),
			processFn: async ({ id }, ctx) => {
				started.get(id)?.(ctx.helpers.job.locked_by)
				if (id === 'first') await finishFirst.promise
			}
		})
		const logs = recordLogs()
		const stops: RunnerStoppedEvent[] = []
		const worker = createBetterWorker({
			pgPool,
			schema,
			queues: [slow],
			restart: { delay: 10, maxDelay: 40 },
			hooks: {
				createLogger: () => logs.logger,
				onRunnerStopped: (event) => void stops.push(event)
			}
		})
		let settled = false
		try {
			await worker.migrate()
			await worker.start()
			void worker.promise.then(
				() => (settled = true),
				() => (settled = true)
			)
			const first = jobStarted('first')
			const firstId = await worker.createJob('slow', { id: 'first' })
			const deadRunner = await first

			// As in production: the job finishes while the database is unreachable, so
			// Graphile cannot record its completion and shuts its own pool down.
			proxy.cut()
			finishFirst.resolve()
			const [stopped] = await logs.logged('worker.runner.stopped')
			expect(stopped).toMatchObject({ restart_attempt: 1, restart_in_ms: 10 })
			expect(String(stopped?.error)).toContain('WorkerStoppedError')

			// The database is still down for the first restarts; each waits longer.
			const failed = await logs.logged('worker.runner.restart_failed', 2)
			expect(failed.slice(0, 2)).toMatchObject([
				{ restart_attempt: 2, restart_in_ms: 20 },
				{ restart_attempt: 3, restart_in_ms: 40 }
			])
			proxy.restore()
			const [restarted] = await logs.logged('worker.runner.restarted')
			expect(Number(restarted?.restart_attempt)).toBeGreaterThanOrEqual(3)
			expect(settled).toBe(false)
			// One event for the stop; the failed restart tries do not report again.
			expect(stops).toHaveLength(1)
			expect(stops[0]).toMatchObject({
				runner: undefined,
				restartAttempt: 1,
				restartInMs: 10
			})
			expect(stops[0]?.error).toBeInstanceOf(WorkerStoppedError)
			expect(stops[0]?.error).toMatchObject({ cause: expect.any(Error) })

			const second = jobStarted('second')
			await worker.createJob('slow', { id: 'second' })
			expect(await second).not.toBe(deadRunner)
			expect(settled).toBe(false)
			// Its completion was never recorded, so the first job stays locked by the dead
			// runner until crash recovery releases it.
			const running = await worker.listJobs({ state: 'running' })
			expect(running.find((job) => job.id === firstId)?.lockedBy).toBe(
				deadRunner
			)
		} finally {
			await worker.stop()
			await pgPool.end()
			await proxy.close()
		}
		await worker.promise
		expect(settled).toBe(true)
	}, 10_000)

	test('stop() during the backoff ends the worker without another attempt', async () => {
		const proxy = await databaseProxy(new URL(process.env.DATABASE_URL!))
		const pgPool = new Pool({ connectionString: proxy.url })
		pgPool.on('error', () => {})
		const events: WorkerEvents = new EventEmitter()
		const created = new Promise<{ workerPool: WorkerPool }>((resolve) =>
			events.once('pool:create', resolve)
		)
		const logs = recordLogs()
		const worker = createBetterWorker({
			pgPool,
			schema,
			queues: [
				defineQueue({
					name: 'idle',
					inputSchema: z.object({}),
					processFn: async () => {}
				})
			],
			restart: { delay: 60_000 },
			graphile: { events },
			hooks: { createLogger: () => logs.logger }
		})
		try {
			await worker.migrate()
			await worker.start()
			const { workerPool } = await created
			void workerPool.gracefulShutdown('Connection terminated unexpectedly')
			await logs.logged('worker.runner.stopped')
			await worker.stop()
			await worker.promise
			expect(logs.matching('Starting graphile-worker')).toHaveLength(1)
			expect(logs.matching('worker.runner.restarted')).toHaveLength(0)
		} finally {
			await pgPool.end()
			await proxy.close()
		}
	})

	test('stop() while Graphile is still tearing down a runner it stopped itself resolves', async () => {
		const events: WorkerEvents = new EventEmitter()
		const created = new Promise<{ workerPool: WorkerPool }>((resolve) =>
			events.once('pool:create', resolve)
		)
		const worker = createBetterWorker({
			pgPool: direct,
			schema,
			queues: [
				defineQueue({
					name: 'idle',
					inputSchema: z.object({}),
					processFn: async () => {}
				})
			],
			graphile: { events },
			hooks: { createLogger: () => recordLogs().logger }
		})
		await worker.migrate()
		await worker.start()
		const { workerPool } = await created
		void workerPool.gracefulShutdown('Connection terminated unexpectedly')
		// Graphile's runner has now marked itself stopped and is releasing its resources.
		await workerPool.promise
		await worker.stop()
		await worker.promise
	})

	test('in a pool, only the runner that stopped is restarted', async () => {
		const turnEvents: WorkerEvents = new EventEmitter()
		const backgroundEvents: WorkerEvents = new EventEmitter()
		const poolIds = { turn: [] as string[], background: [] as string[] }
		turnEvents.on('pool:create', ({ workerPool }) =>
			poolIds.turn.push(workerPool.id)
		)
		backgroundEvents.on('pool:create', ({ workerPool }) =>
			poolIds.background.push(workerPool.id)
		)
		const firstTurn = new Promise<{ workerPool: WorkerPool }>((resolve) =>
			turnEvents.once('pool:create', resolve)
		)
		const ran = new Map<string, (lockedBy: string | null) => void>()
		const runs = (queue: string) =>
			new Promise<string | null>((resolve) => ran.set(queue, resolve))
		const logs = recordLogs()
		const stops: RunnerStoppedEvent[] = []
		const workers = createBetterWorkerPool({
			pgPool: direct,
			schema,
			queues: [
				defineQueue({
					name: 'reply',
					runner: 'turn',
					inputSchema: z.object({}),
					processFn: async (_, ctx) =>
						ran.get('reply')?.(ctx.helpers.job.locked_by)
				}),
				defineQueue({
					name: 'digest',
					runner: 'background',
					inputSchema: z.object({}),
					processFn: async (_, ctx) =>
						ran.get('digest')?.(ctx.helpers.job.locked_by)
				})
			],
			restart: { delay: 0 },
			runners: {
				turn: { graphile: { events: turnEvents } },
				background: { graphile: { events: backgroundEvents } }
			},
			hooks: {
				createLogger: () => logs.logger,
				onRunnerStopped: (event) => void stops.push(event)
			}
		})
		let settled = false
		try {
			await workers.migrate()
			await workers.start()
			void workers.promise.then(
				() => (settled = true),
				() => (settled = true)
			)
			const { workerPool } = await firstTurn
			void workerPool.gracefulShutdown('Connection terminated unexpectedly')
			const [restarted] = await logs.logged('worker.runner.restarted')
			expect(restarted).toMatchObject({ runner: 'turn' })

			const reply = runs('reply')
			const digest = runs('digest')
			await workers.jobs.reply({})
			await workers.jobs.digest({})
			expect(await reply).toBe(poolIds.turn[1]!)
			expect(await digest).toBe(poolIds.background[0]!)
			expect(poolIds.turn).toHaveLength(2)
			expect(poolIds.background).toHaveLength(1)
			expect(settled).toBe(false)
			expect(stops.map((event) => event.runner)).toEqual(['turn'])
		} finally {
			await workers.stop()
		}
		await workers.promise
	})

	/** A started worker on its own events, so a test can make Graphile stop it. */
	async function stoppable(options: {
		restart: false | { delay: number }
		onRunnerStopped: (event: RunnerStoppedEvent) => void | Promise<void>
	}) {
		const events: WorkerEvents = new EventEmitter()
		const created = () =>
			new Promise<{ workerPool: WorkerPool }>((resolve) =>
				events.once('pool:create', resolve)
			)
		const ran = new Map<string, (lockedBy: string | null) => void>()
		const logs = recordLogs()
		const worker = createBetterWorker({
			pgPool: direct,
			schema,
			queues: [
				defineQueue({
					name: 'echo',
					inputSchema: z.object({ id: z.string() }),
					processFn: async ({ id }, ctx) =>
						ran.get(id)?.(ctx.helpers.job.locked_by)
				})
			],
			restart: options.restart,
			graphile: { events },
			hooks: {
				createLogger: () => logs.logger,
				onRunnerStopped: options.onRunnerStopped
			}
		})
		await worker.migrate()
		const first = created()
		await worker.start()
		return {
			worker,
			logs,
			/** Graphile shuts the current runner's pool down by itself. */
			async stopByGraphile() {
				const { workerPool } = await first
				void workerPool.gracefulShutdown('Connection terminated unexpectedly')
			},
			runs(id: string) {
				return new Promise<string | null>((resolve) => ran.set(id, resolve))
			}
		}
	}

	for (const [kind, onRunnerStopped] of [
		[
			'throws',
			() => {
				throw new Error('hook failed')
			}
		],
		['rejects', () => Promise.reject(new Error('hook failed'))]
	] as const)
		test(`a hook that ${kind} is reported and the runner still restarts`, async () => {
			const reported = spyOn(console, 'error').mockImplementation(() => {})
			const { worker, logs, stopByGraphile, runs } = await stoppable({
				restart: { delay: 0 },
				onRunnerStopped
			})
			try {
				await stopByGraphile()
				await logs.logged('worker.runner.restarted')
				const ran = runs('after')
				await worker.createJob('echo', { id: 'after' })
				expect(await ran).toBeString()
				expect(reported).toHaveBeenCalledWith(
					'better-graphile-worker observer failed',
					expect.objectContaining({ message: 'hook failed' })
				)
			} finally {
				await worker.stop()
				reported.mockRestore()
			}
		})

	test('with restart: false the hook is not called and promise rejects', async () => {
		const stops: RunnerStoppedEvent[] = []
		const { worker, stopByGraphile } = await stoppable({
			restart: false,
			onRunnerStopped: (event) => void stops.push(event)
		})
		await stopByGraphile()
		const error = await worker.promise.catch((caught: unknown) => caught)
		expect(error).toBeInstanceOf(WorkerStoppedError)
		expect(stops).toEqual([])
		await worker.stop()
	})
})
