import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import type { JobHelpers } from 'graphile-worker'
import { Pool } from 'pg'
import { z } from 'zod'
import { createBetterWorker } from './create-better-worker.js'
import { createCompletedJobsStore } from './completed-jobs-store.js'
import { defineQueue } from './queue.js'
import { defineQueues } from './define-queues.js'
import { JobTimeoutError, StepPersistenceError } from './errors.js'
import type { JobFinishedEvent, JobLogger, LogAttributes } from './hooks.js'
import { injectTraceContext } from './payload.js'
import { normalizeWorkerQueues } from './registry.js'
import { createPgStepStore } from './steps.js'
import { buildTaskList, type TaskListRuntime } from './worker.js'

const quiet: JobLogger = { debug() {}, info() {}, warn() {}, error() {} }

function deferred<T = void>() {
	let resolve!: (value: T) => void
	const promise = new Promise<T>((done) => {
		resolve = done
	})
	return { promise, resolve }
}

const contracts = defineQueues([
	defineQueue({ name: 'hang', inputSchema: z.string(), timeout: 20 }),
	defineQueue({ name: 'defaulted', inputSchema: z.string() })
])

function setup(
	handlers: Parameters<typeof normalizeWorkerQueues<typeof contracts>>[1],
	options: {
		query?: (text: string) => Promise<unknown>
		abortSignal?: AbortSignal
		defaultTimeout?: number
	} = {}
) {
	const finished: JobFinishedEvent[] = []
	const warnings: LogAttributes[] = []
	const warned = deferred()
	const runtime: TaskListRuntime = {
		hooks: {
			createLogger: () => ({
				...quiet,
				warn: (_message, attributes) => {
					warnings.push(attributes ?? {})
					warned.resolve()
				}
			}),
			onJobFinished: (event) => void finished.push(event)
		},
		completedJobs: createCompletedJobsStore(),
		createJob: (async () => 'child') as TaskListRuntime['createJob'],
		createJobs: (async () => []) as TaskListRuntime['createJobs'],
		logger: quiet,
		schema: 'graphile_worker',
		defaultTimeout: options.defaultTimeout
	}
	const tasks = buildTaskList(
		normalizeWorkerQueues(contracts, handlers),
		runtime
	)
	const helpers = (task: string) =>
		({
			job: {
				id: '1',
				locked_by: 'pool',
				locked_at: new Date(),
				attempts: 1,
				max_attempts: 4,
				task_identifier: task,
				created_at: new Date()
			},
			withPgClient: async (fn: (client: unknown) => Promise<unknown>) =>
				fn({
					query: async (text: string) =>
						(await options.query?.(text)) ?? { rowCount: 1, rows: [] }
				}),
			abortSignal: options.abortSignal ?? new AbortController().signal
		}) as unknown as JobHelpers
	const run = (task: 'hang' | 'defaulted') =>
		tasks[task]!(injectTraceContext('x', null), helpers(task))
	return { run, finished, warnings, warned }
}

describe('attempt timeout', () => {
	test('fails a handler that ignores its signal and refuses its later work', async () => {
		const release = deferred()
		let reason: unknown
		let late: PromiseSettledResult<unknown>[] = []
		const { run, finished, warnings, warned } = setup({
			hang: async (_payload, ctx) => {
				ctx.signal.addEventListener('abort', () => {
					reason = ctx.signal.reason
				})
				await release.promise
				late = await Promise.allSettled([
					ctx.createJob('defaulted', 'child'),
					ctx.createJobs('defaulted', ['child']),
					ctx.step.run('late', () => 1),
					ctx.progress.set(1),
					ctx.progress.flush(),
					ctx.continue()
				])
			},
			defaulted: () => {}
		})

		await expect(run('hang')).rejects.toBeInstanceOf(JobTimeoutError)
		expect(reason).toBeInstanceOf(JobTimeoutError)
		expect(finished[0]).toMatchObject({
			status: 'failed',
			errorType: 'JobTimeoutError',
			errorMessage: 'Job attempt exceeded its 20ms timeout'
		})

		release.resolve()
		// Logged once the detached handler finally returns.
		await warned.promise
		expect(late.map((result) => result.status)).toEqual(
			Array(6).fill('rejected')
		)
		for (const result of late)
			expect((result as PromiseRejectedResult).reason).toBeInstanceOf(
				JobTimeoutError
			)
		expect(warnings).toContainEqual(
			expect.objectContaining({ event: 'job.timed_out_handler_settled' })
		)
	})

	test('the worker default applies to queues without their own timeout', async () => {
		const { run, finished } = setup(
			{
				hang: () => {},
				defaulted: (_payload, ctx) =>
					new Promise((_resolve, reject) =>
						ctx.signal.addEventListener('abort', () =>
							reject(ctx.signal.reason)
						)
					)
			},
			{ defaultTimeout: 10 }
		)
		await expect(run('defaulted')).rejects.toBeInstanceOf(JobTimeoutError)
		expect(finished[0]?.errorType).toBe('JobTimeoutError')
	})

	test('a continuation committing when the timeout fires wins', async () => {
		const aborted = deferred()
		const { run, finished } = setup(
			{
				hang: async (_payload, ctx) => {
					ctx.signal.addEventListener('abort', () => aborted.resolve())
					await ctx.continue()
				},
				defaulted: () => {}
			},
			{
				query: async (text) => {
					if (text.includes('SELECT tasks.identifier'))
						return {
							rows: [
								{
									identifier: 'hang',
									payload: injectTraceContext('x', null),
									queue_name: null,
									key: null,
									priority: 0,
									max_attempts: 4,
									flags: null
								}
							]
						}
					if (text.includes('add_job')) return { rows: [{ id: '2' }] }
					if (text === 'COMMIT') await aborted.promise
					return undefined
				}
			}
		)
		await expect(run('hang')).resolves.toBeUndefined()
		expect(finished[0]).toMatchObject({ status: 'success', continued: true })
	})

	test('shutdown aborts with its own reason and still allows continue', async () => {
		const shutdown = new AbortController()
		let reason: unknown
		const { run, finished } = setup(
			{
				hang: async (_payload, ctx) => {
					shutdown.abort()
					reason = ctx.signal.reason
					await ctx.continue()
				},
				defaulted: () => {}
			},
			{
				abortSignal: shutdown.signal,
				query: async (text) =>
					text.includes('SELECT tasks.identifier')
						? { rows: [{ identifier: 'hang', payload: {}, priority: 0 }] }
						: text.includes('add_job')
							? { rows: [{ id: '2' }] }
							: undefined
			}
		)
		await run('hang')
		expect(reason).not.toBeInstanceOf(JobTimeoutError)
		expect((reason as Error).name).toBe('AbortError')
		expect(finished[0]).toMatchObject({ status: 'success', continued: true })
	})

	test('rejects invalid timeouts when the worker is built', () => {
		const pgPool = {} as Pool
		const bad = defineQueues([
			defineQueue({ name: 'bad', inputSchema: z.string(), timeout: 0 })
		])
		expect(() =>
			createBetterWorker({ pgPool, queues: bad, handlers: { bad: () => {} } })
		).toThrow('timeout')
		expect(() =>
			createBetterWorker({
				pgPool,
				queues: contracts,
				defaultTimeout: 1.5,
				handlers: { hang: () => {}, defaulted: () => {} }
			})
		).toThrow('defaultTimeout')
		const worker = createBetterWorker({
			pgPool,
			queues: contracts,
			defaultTimeout: 60_000,
			handlers: { hang: () => {}, defaulted: () => {} }
		})
		expect(
			worker.getQueueDefinitions().map(({ name, timeout }) => ({
				name,
				timeout
			}))
		).toEqual([
			{ name: 'hang', timeout: 20 },
			{ name: 'defaulted', timeout: 60_000 }
		])
	})
})

const enabled = Boolean(process.env.DATABASE_URL)
const schema = `BGW_timeout_${process.pid}`

describe.skipIf(!enabled)('attempt timeout in PostgreSQL', () => {
	const pool = new Pool({ connectionString: process.env.DATABASE_URL })
	pool.on('error', () => {})
	const queues = defineQueues([
		defineQueue({
			name: 'stuck',
			inputSchema: z.object({ hang: z.boolean() }),
			serial: true,
			timeout: 100
		})
	])
	const finished: JobFinishedEvent[] = []
	let started = deferred()
	let release = deferred()
	let late = deferred<PromiseSettledResult<unknown>[]>()
	const ranNext = deferred()
	const worker = createBetterWorker({
		pgPool: pool,
		schema,
		queues,
		pollInterval: 20,
		hooks: {
			createLogger: () => quiet,
			onJobFinished: (event) => void finished.push(event)
		},
		handlers: {
			stuck: async ({ hang }, ctx) => {
				if (!hang) return ranNext.resolve()
				started.resolve()
				await release.promise
				late.resolve(
					await Promise.allSettled([
						ctx.step.run('late', () => 1),
						ctx.progress.set({ late: true }),
						ctx.createJob('stuck', { hang: false })
					])
				)
			}
		}
	})
	const rows = async () =>
		(
			await pool.query(
				`SELECT id::text, attempts, last_error, locked_by, payload FROM "${schema}"._private_jobs ORDER BY id`
			)
		).rows
	beforeAll(() => worker.migrate())
	afterAll(async () => {
		release.resolve()
		await worker.stop()
		await pool.query(`DROP SCHEMA "${schema}" CASCADE`)
		await pool.end()
	})

	test('a hung job fails its attempt, frees its lane and cannot write afterwards', async () => {
		const [hung] = await worker.createJobs('stuck', [
			{ hang: true },
			{ hang: false }
		])
		await worker.start()
		// The second job in the same lane runs once the timeout releases the first.
		await ranNext.promise
		await worker.stop()
		expect(await rows()).toMatchObject([
			{ id: hung, attempts: 1, locked_by: null }
		])
		const [failed] = await rows()
		expect(failed.last_error).toContain('100ms timeout')
		expect(finished.map((event) => event.errorType ?? 'ok')).toEqual([
			'JobTimeoutError',
			'ok'
		])

		release.resolve()
		await late.promise
		// The detached handler's step, progress and child enqueue never reached the database.
		const after = await rows()
		expect(after).toHaveLength(1)
		expect(after[0].payload.steps).toBeUndefined()
		expect(after[0].payload.progress).toBeUndefined()
		await pool.query(`DELETE FROM "${schema}"._private_jobs`)
	})

	test('graceful shutdown is bounded by the timeout of a hung job', async () => {
		started = deferred()
		release = deferred()
		late = deferred()
		await worker.createJob('stuck', { hang: true })
		await worker.start()
		await started.promise
		await worker.stop()
		const [failed] = await rows()
		expect(failed).toMatchObject({ attempts: 1, locked_by: null })
		expect(failed.last_error).toContain('timeout')
		release.resolve()
		await late.promise
		await pool.query(`DELETE FROM "${schema}"._private_jobs`)
	})

	test('a checkpoint from an earlier lock by the same pool is rejected', async () => {
		const id = await worker.createJob('stuck', { hang: false })
		const job = (
			await pool.query(
				`UPDATE "${schema}"._private_jobs SET locked_by = 'pool', locked_at = now() WHERE id = $1 RETURNING *`,
				[id]
			)
		).rows[0]
		const utils = await worker.getWorkerUtils()
		const store = createPgStepStore({
			helpers: {
				job: { ...job, locked_at: new Date(0) },
				withPgClient: utils.withPgClient
			} as unknown as JobHelpers,
			schema,
			jobId: String(id),
			rawPayload: job.payload
		})
		await expect(store.set('stale', 1)).rejects.toBeInstanceOf(
			StepPersistenceError
		)
		await pool.query(`DELETE FROM "${schema}"._private_jobs`)
	})
})
