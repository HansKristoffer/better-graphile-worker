import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import type { JobHelpers } from 'graphile-worker'
import { Pool } from 'pg'
import { z } from 'zod'
import { createBetterWorker } from './create-better-worker.js'
import { createCompletedJobsStore } from './completed-jobs-store.js'
import { defineQueue } from './queue.js'
import { defineQueues } from './define-queues.js'
import { NonRetriableError } from './errors.js'
import type {
	BetterWorkerHooks,
	JobFinishedEvent,
	JobLogger,
	PermanentFailureEvent
} from './hooks.js'
import { injectTraceContext } from './payload.js'
import { normalizeWorkerQueues } from './registry.js'
import { buildTaskList, type TaskListRuntime } from './worker.js'

const quiet: JobLogger = { debug() {}, info() {}, warn() {}, error() {} }

function deferred<T = void>() {
	let resolve!: (value: T) => void
	const promise = new Promise<T>((done) => {
		resolve = done
	})
	return { promise, resolve }
}

type Seen = { by: string; input?: unknown; event: PermanentFailureEvent }

describe('permanent failures from a handler', () => {
	const contracts = defineQueues([
		defineQueue({
			name: 'sync',
			inputSchema: z.object({ id: z.string() }),
			timeout: 20,
			retryDelay: ['1m', '5m']
		}),
		defineQueue({
			name: 'tick',
			cron: '* * * * *',
			inputSchema: z.object({ id: z.string() })
		})
	])

	function setup(
		behavior: (payload: { id: string }) => Promise<void>,
		hooks: BetterWorkerHooks = {}
	) {
		const seen: Seen[] = []
		const finished: JobFinishedEvent[] = []
		const queries: { text: string; values: unknown[] }[] = []
		const runtime: TaskListRuntime = {
			hooks: {
				createLogger: () => quiet,
				onJobFinished: (event) => void finished.push(event),
				onPermanentFailure: (event) =>
					void seen.push({ by: 'instance', event }),
				...hooks
			},
			completedJobs: createCompletedJobsStore(),
			createJob: (async () => 'child') as TaskListRuntime['createJob'],
			createJobs: (async () => []) as TaskListRuntime['createJobs'],
			logger: quiet,
			schema: 'graphile_worker'
		}
		const tasks = buildTaskList(
			normalizeWorkerQueues<typeof contracts>(contracts, {
				sync: {
					processFn: (payload) => behavior(payload),
					onPermanentFailure: (input, event) => {
						// The wire input, typed by the queue's schema.
						const id: string = input.id
						seen.push({ by: 'queue', input: id, event })
					}
				},
				tick: {
					processFn: async () => {},
					initFn: async () => {
						throw new Error('init failed')
					},
					onPermanentFailure: (_input, event) =>
						void seen.push({ by: 'tick', event })
				}
			}),
			runtime
		)
		const run = (task: string, attempts: number, maxAttempts = 3) =>
			tasks[task]!(injectTraceContext({ id: 'a' }, null), {
				job: {
					id: '9',
					locked_by: 'pool',
					locked_at: new Date(),
					attempts,
					max_attempts: maxAttempts,
					task_identifier: task,
					created_at: new Date(),
					run_at: new Date(Date.now() - 1000)
				},
				withPgClient: async (fn: (client: unknown) => Promise<unknown>) =>
					fn({
						query: async (text: string, values: unknown[]) => {
							queries.push({ text, values })
							return { rowCount: 1, rows: [{ retry_at: values[3], owned: 1 }] }
						}
					}),
				abortSignal: new AbortController().signal
			} as unknown as JobHelpers)
		return { run, seen, finished, queries }
	}

	test('the queue hook gets the wire input before the instance hook, with the reason', async () => {
		const { run, seen } = setup(async () => {
			throw new Error('down')
		})
		await expect(run('sync', 3)).rejects.toThrow('down')
		expect(seen.map((entry) => entry.by)).toEqual(['queue', 'instance'])
		expect(seen[0]).toMatchObject({
			input: 'a',
			event: {
				reason: 'error',
				queue: 'sync',
				jobId: '9',
				payload: { id: 'a' }
			}
		})
	})

	test('each way the last attempt ends has its reason', async () => {
		const nonRetriable = setup(async () => {
			throw new NonRetriableError('bad')
		})
		// Retained: rethrown so Graphile records it.
		await expect(nonRetriable.run('sync', 1)).rejects.toThrow('bad')
		expect(nonRetriable.seen[0]?.event.reason).toBe('non_retriable')

		const hanging = setup(() => new Promise(() => {}))
		await expect(hanging.run('sync', 3)).rejects.toThrow('timeout')
		expect(hanging.seen[0]?.event.reason).toBe('timeout')
	})

	test('a failing queue hook changes nothing and the instance hook still runs', async () => {
		const failing = setup(
			async () => {
				throw new Error('down')
			},
			{
				onPermanentFailure: () => {
					throw new Error('hook down')
				}
			}
		)
		await expect(failing.run('sync', 3)).rejects.toThrow('down')
		expect(failing.seen.map((entry) => entry.by)).toEqual(['queue'])
	})

	test('an initFn failure reaches the instance hook only', async () => {
		const { run, seen } = setup(async () => {})
		await expect(run('tick_cron-init', 4, 4)).rejects.toThrow('init failed')
		expect(seen.map((entry) => entry.by)).toEqual(['instance'])
		expect(seen[0]?.event.operation).toBe('init')
	})

	test('retryDelay schedules each retry from its entry, and the last entry repeats', async () => {
		const { run, finished, queries } = setup(async () => {
			throw new Error('down')
		})
		const before = Date.now()
		await expect(run('sync', 1)).rejects.toThrow('down')
		await expect(run('sync', 2)).rejects.toThrow('down')
		await expect(run('sync', 2, 9)).rejects.toThrow('down')
		const delays = queries.map(
			(query) => new Date(query.values[3] as Date).getTime() - before
		)
		expect(delays[0]).toBeGreaterThanOrEqual(60_000)
		expect(delays[0]).toBeLessThan(61_000)
		expect(delays[1]).toBeGreaterThanOrEqual(300_000)
		expect(delays[2]).toBeGreaterThanOrEqual(300_000)
		expect(finished[0]?.retryAt).toBeInstanceOf(Date)
	})

	test('jobAttributes reach the span, the logger and onJobFinished; waitMs is measured', async () => {
		const loggers: unknown[] = []
		const { run, finished } = setup(async () => {}, {
			jobAttributes: ({ payload }) => ({
				tenant: (payload as { id: string }).id
			}),
			createLogger: (options) => {
				loggers.push(options.attributes)
				return quiet
			}
		})
		await run('sync', 1)
		expect(loggers).toEqual([{ tenant: 'a' }])
		expect(finished[0]?.attributes).toEqual({ tenant: 'a' })
		expect(finished[0]?.waitMs).toBeGreaterThanOrEqual(1000)
	})

	test('a throwing jobAttributes hook adds nothing', async () => {
		const { run, finished } = setup(async () => {}, {
			jobAttributes: () => {
				throw new Error('attributes down')
			}
		})
		await run('sync', 1)
		expect(finished[0]?.status).toBe('success')
		expect(finished[0]?.attributes).toBeUndefined()
	})
})

describe('queue options', () => {
	test('retryDelay must be a duration list with entries', () => {
		const queue = (retryDelay: unknown) =>
			normalizeWorkerQueues([
				defineQueue({
					name: 'q',
					inputSchema: z.string(),
					retryDelay: retryDelay as '1m',
					processFn: async () => {}
				})
			])
		expect(queue(['1m', 500])[0]?.retryDelays).toEqual([60_000, 500])
		expect(() => queue([])).toThrow('must not be empty')
		expect(() => queue('soon')).toThrow('retryDelay')
	})
})

const enabled = Boolean(process.env.DATABASE_URL)
const suite = describe.skipIf(!enabled)
const schema = `bgw_failure_${process.pid}`

suite('permanent failures without a handler, in PostgreSQL', () => {
	const pool = new Pool({ connectionString: process.env.DATABASE_URL })
	pool.on('error', () => {})
	const q = `"${schema}"`
	const reports: Seen[] = []
	let reported = deferred()
	const contracts = defineQueues([
		defineQueue({
			name: 'sync',
			inputSchema: z.object({ id: z.string() }),
			maxAttempts: 1,
			deriveJobOptions: ({ id }) => ({ jobKey: `sync:${id}` })
		})
	])
	const handlers = {
		sync: {
			processFn: async () => {},
			onPermanentFailure: (
				input: { id: string },
				event: PermanentFailureEvent
			) => {
				reports.push({ by: 'queue', input: input.id, event })
				reported.resolve()
			}
		}
	}
	const worker = createBetterWorker({
		pgPool: pool,
		schema,
		queues: contracts,
		handlers,
		hooks: { createLogger: () => quiet }
	})
	const lockAs = (id: string, workerId: string) =>
		pool.query(
			`UPDATE ${q}._private_jobs SET locked_by = $2, locked_at = now(), attempts = attempts + 1 WHERE id = $1`,
			[id, workerId]
		)
	const row = async (id: string) =>
		(
			await pool.query<{
				locked_by: string | null
				attempts: number
				last_error: string | null
			}>(
				`SELECT locked_by, attempts, last_error FROM ${q}._private_jobs WHERE id = $1`,
				[id]
			)
		).rows[0]

	beforeAll(() => worker.migrate())
	afterAll(async () => {
		await pool.query(`DROP SCHEMA IF EXISTS ${q} CASCADE`)
		await pool.end()
	})
	const reset = async () => {
		reports.length = 0
		reported = deferred()
		await pool.query(`DELETE FROM ${q}._private_jobs`)
		await pool.query(`DELETE FROM ${q}.bgw_worker_heartbeats`)
	}

	test('crash recovery reports a job it releases on its last attempt', async () => {
		await reset()
		const id = (await worker.createJob('sync', { id: 'lost' }))!
		await lockAs(id, 'pool-dead')
		await pool.query(
			`INSERT INTO ${q}.bgw_worker_heartbeats VALUES ('pool-dead', now() - interval '1 hour')`
		)
		const sweeper = createBetterWorker({
			pgPool: pool,
			schema,
			queues: contracts,
			handlers,
			crashRecovery: { heartbeatInterval: 20, staleAfter: 60 },
			hooks: { createLogger: () => quiet }
		})
		await sweeper.start()
		try {
			await reported.promise
		} finally {
			await sweeper.stop()
		}
		expect(reports[0]).toMatchObject({
			input: 'lost',
			event: { reason: 'worker_lost', jobId: id, attempts: 1, maxAttempts: 1 }
		})
		expect(await row(id)).toMatchObject({ locked_by: null, attempts: 1 })
	})

	test('releaseLocks releases dead or ownerless locks only, and all of them with all', async () => {
		await reset()
		const dead = (await worker.createJob('sync', { id: 'dead' }))!
		const alive = (await worker.createJob('sync', { id: 'alive' }))!
		const orphan = (await worker.createJob('sync', { id: 'orphan' }))!
		const fresh = (await worker.createJob('sync', { id: 'fresh' }))!
		await lockAs(dead, 'pool-dead')
		await lockAs(alive, 'pool-alive')
		await lockAs(orphan, 'pool-orphan')
		await lockAs(fresh, 'pool-fresh')
		await pool.query(
			`UPDATE ${q}._private_jobs SET locked_at = now() - interval '1 hour' WHERE id = $1`,
			[orphan]
		)
		await pool.query(
			`INSERT INTO ${q}.bgw_worker_heartbeats VALUES ('pool-dead', now() - interval '1 hour'), ('pool-alive', now())`
		)
		expect((await worker.releaseLocks()).sort()).toEqual([dead, orphan].sort())
		expect(reports.map((report) => report.input).sort()).toEqual([
			'dead',
			'orphan'
		])
		expect(await row(alive)).toMatchObject({ locked_by: 'pool-alive' })
		expect(await row(fresh)).toMatchObject({ locked_by: 'pool-fresh' })

		expect((await worker.releaseLocks({ all: true })).sort()).toEqual(
			[alive, fresh].sort()
		)
	})

	test('the bin releases every lock from a connection string alone', async () => {
		await reset()
		const id = (await worker.createJob('sync', { id: 'bin' }))!
		await lockAs(id, 'pool-anyone')
		const bin = Bun.spawn(
			[
				process.execPath,
				`${import.meta.dir}/bin.ts`,
				'release-locks',
				'--all',
				'--schema',
				schema,
				process.env.DATABASE_URL!
			],
			{ stdout: 'pipe', stderr: 'inherit' }
		)
		const output = await new Response(bin.stdout).text()
		expect(await bin.exited).toBe(0)
		expect(JSON.parse(output)).toEqual({ released: [id] })
		expect(await row(id)).toMatchObject({ locked_by: null })
	})

	test('failJobs reports what it failed', async () => {
		await reset()
		const id = (await worker.createJob('sync', { id: 'manual' }))!
		expect(await worker.failJobs([id], 'stop it')).toEqual([id])
		expect(reports[0]).toMatchObject({
			input: 'manual',
			event: { reason: 'failJobs', error: new Error('stop it') }
		})
	})

	test('shutdownGrace aborts the signal, fails what still runs and reports its last attempt', async () => {
		await reset()
		const started = deferred()
		let abortedAfter: number | undefined
		const stopping = createBetterWorker({
			pgPool: pool,
			schema,
			queues: contracts,
			shutdownGrace: 100,
			handlers: {
				sync: {
					processFn: (_payload, ctx) => {
						const since = Date.now()
						ctx.signal.addEventListener('abort', () => {
							abortedAfter = Date.now() - since
						})
						started.resolve()
						// Ignores its signal, so only the forced shutdown ends it.
						return new Promise<void>(() => {})
					},
					onPermanentFailure: handlers.sync.onPermanentFailure
				}
			},
			hooks: { createLogger: () => quiet }
		})
		const id = (await stopping.createJob('sync', { id: 'cut' }))!
		await stopping.start()
		await started.promise
		const stopAt = Date.now()
		await stopping.stop()
		expect(Date.now() - stopAt).toBeLessThan(5000)
		expect(abortedAfter).toBeGreaterThanOrEqual(90)
		expect(reports[0]).toMatchObject({
			input: 'cut',
			event: { reason: 'shutdown', jobId: id }
		})
		expect(await row(id)).toMatchObject({ locked_by: null, attempts: 1 })
	})

	test('a job is found, listed and removed by its key', async () => {
		await reset()
		const id = (await worker.createJob('sync', { id: 'k' }))!
		expect(await worker.getJobByKey('sync:k')).toMatchObject({
			id,
			queue: 'sync',
			key: 'sync:k',
			payload: { id: 'k' }
		})
		expect(
			(await worker.listJobs({ key: 'sync:k' })).map((job) => job.id)
		).toEqual([id])
		expect(await worker.getJobByKey('sync:none')).toBeNull()
		expect(await worker.removeJob('sync:k')).toBe(id)
		expect(await worker.getJobByKey('sync:k')).toBeNull()
		expect(await worker.removeJob('sync:k')).toBeNull()
	})

	test('deleteFailedJobs deletes only old permanent failures', async () => {
		await reset()
		const old = (await worker.createJob('sync', { id: 'old' }))!
		const recent = (await worker.createJob('sync', { id: 'recent' }))!
		const pending = (await worker.createJob('sync', { id: 'pending' }))!
		await worker.failJobs([old, recent])
		await pool.query(
			`UPDATE ${q}._private_jobs SET updated_at = now() - interval '40 days' WHERE id = ANY($1::bigint[])`,
			[[old, pending]]
		)
		expect(await worker.deleteFailedJobs('30d')).toEqual([old])
		expect(await row(recent)).toBeDefined()
		expect(await row(pending)).toBeDefined()
	})
})
