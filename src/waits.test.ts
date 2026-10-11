import {
	afterAll,
	afterEach,
	beforeAll,
	describe,
	expect,
	test
} from 'bun:test'
import type { JobHelpers } from 'graphile-worker'
import { Pool, type PoolClient } from 'pg'
import { z } from 'zod'
import { createBetterWorker } from './create-better-worker.js'
import { sweepDeadWorkers } from './crash-recovery.js'
import { settleLockedJob, sweepWaits } from './private-jobs.js'
import { defineQueue } from './queue.js'
import { defineQueues } from './define-queues.js'
import { createTestHarness } from './testing.js'
import {
	ChildJobFailedError,
	JobValidationError,
	NonRetriableError,
	UnknownQueueError
} from './errors.js'
import type { WaitResult } from './waits.js'

describe('waiting on child jobs in the test harness', () => {
	let fetches = 0
	const fetchAccount = defineQueue({
		name: 'fetchAccount',
		inputSchema: z.object({ accountId: z.string() }),
		outputSchema: z.object({ upserted: z.number() }),
		processFn: ({ accountId }) => {
			fetches += 1
			if (accountId === 'broken') throw new Error('bank said no')
			return { upserted: accountId.length }
		}
	})
	const badOutput = defineQueue({
		name: 'badOutput',
		inputSchema: z.string(),
		outputSchema: z.object({ n: z.number() }),
		// Typed handlers cannot return this; untyped producers of output can.
		processFn: (() => ({ n: 'x' })) as never
	})
	const unregistered = defineQueue({
		name: 'unregistered',
		inputSchema: z.string(),
		processFn: () => {}
	})
	const seen: WaitResult<{ upserted: number }>[][] = []
	let beforeWait = 0
	const syncAll = defineQueue({
		name: 'syncAll',
		inputSchema: z.array(z.string()),
		outputSchema: z.number(),
		processFn: async (accountIds, ctx) => {
			beforeWait += 1
			const results = await ctx.step.batchTriggerAndWait(
				'fetch-all-v1',
				fetchAccount,
				accountIds.map((accountId) => ({ accountId }))
			)
			seen.push(results)
			return results.reduce(
				(total, result) => total + (result.ok ? result.output.upserted : 0),
				0
			)
		}
	})
	const nested = defineQueue({
		name: 'nested',
		inputSchema: z.array(z.string()),
		processFn: async (accountIds, ctx) => {
			const result = await ctx.step.triggerAndWait(
				'sync-v1',
				syncAll,
				accountIds
			)
			if (!result.ok) throw result.error
			ctx.logger.info('total', { total: result.output })
		}
	})
	const misuse = defineQueue({
		name: 'misuse',
		inputSchema: z.enum(['unregistered', 'badOutput']),
		processFn: async (which, ctx) => {
			if (which === 'unregistered')
				await ctx.step.triggerAndWait('x', unregistered, 'a')
			else {
				const result = await ctx.step.triggerAndWait('x', badOutput, 'a')
				if (!result.ok) throw result.error
			}
		}
	})
	const queues = defineQueues([
		fetchAccount,
		badOutput,
		syncAll,
		nested,
		misuse
	])

	test('runs children inline and returns typed results in input order', async () => {
		const harness = createTestHarness(queues)
		await harness.process('syncAll', ['ab', 'broken', 'abcd'])
		const [results] = seen.splice(0)
		expect(results?.map((result) => result.ok)).toEqual([true, false, true])
		const [first, failed] = results!
		expect(first).toEqual({ ok: true, id: 'child-1', output: { upserted: 2 } })
		if (failed?.ok !== false) throw new Error('expected a failure')
		expect(failed.error).toBeInstanceOf(ChildJobFailedError)
		expect(failed.error).toBeInstanceOf(NonRetriableError)
		expect(failed.error).toMatchObject({
			jobId: 'child-2',
			queueName: 'fetchAccount'
		})
		expect(failed.error.message).toContain('bank said no')
		expect((failed.error.cause as Error).message).toBe('bank said no')
		expect(harness.enqueued.map((job) => job.payload)).toEqual([
			{ accountId: 'ab' },
			{ accountId: 'broken' },
			{ accountId: 'abcd' }
		])
	})

	test('a retry with the same job id replays the checkpoint without rerunning children', async () => {
		const harness = createTestHarness(queues)
		fetches = 0
		beforeWait = 0
		await harness.process('syncAll', ['a'], { jobId: 'retry-1' })
		await harness.process('syncAll', ['a'], { jobId: 'retry-1' })
		expect(fetches).toBe(1)
		expect(beforeWait).toBe(2)
		expect(seen.splice(0).map((results) => results[0])).toEqual([
			{ ok: true, id: 'child-1', output: { upserted: 1 } },
			{ ok: true, id: 'child-1', output: { upserted: 1 } }
		])
	})

	test('children can wait on their own children', async () => {
		const harness = createTestHarness(queues)
		const { logs } = await harness.process('nested', ['abc', 'de'])
		expect(logs.find((log) => log.message === 'total')?.attributes).toEqual({
			total: 5
		})
		expect(harness.enqueued.map((job) => job.queue)).toEqual([
			'syncAll',
			'fetchAccount',
			'fetchAccount'
		])
		seen.length = 0
	})

	test('rejects unregistered queues and child outputs that fail outputSchema', async () => {
		const harness = createTestHarness(queues)
		await expect(harness.process('misuse', 'unregistered')).rejects.toThrow(
			UnknownQueueError
		)
		const error = await harness.process('misuse', 'badOutput').catch((e) => e)
		expect(error).toBeInstanceOf(ChildJobFailedError)
		expect(error.message).toContain('Invalid output for queue "badOutput"')
		// The harness validates outputs of directly processed jobs too.
		await expect(harness.process('badOutput', 'a')).rejects.toThrow(
			JobValidationError
		)
	})

	test('types outputs from outputSchema and checks handler returns', () => {
		void defineQueue({
			name: 'typed',
			inputSchema: z.string(),
			outputSchema: z.object({
				at: z.string().transform((value) => new Date(value))
			}),
			// Handlers return the schema input; parents receive its output.
			processFn: () => ({ at: '2026-01-01' })
		})
		void defineQueue({
			name: 'dated',
			inputSchema: z.string(),
			outputSchema: z.date(),
			// @ts-expect-error Outputs are stored as JSON; a Date is not
			processFn: () => new Date()
		})
		void defineQueue({
			name: 'wrong',
			inputSchema: z.string(),
			outputSchema: z.number(),
			// @ts-expect-error The handler must return outputSchema input
			processFn: () => 'one'
		})
		void defineQueue({
			name: 'parent',
			inputSchema: z.string(),
			processFn: async (_input, ctx) => {
				const one = await ctx.step.triggerAndWait('one', fetchAccount, {
					accountId: 'a'
				})
				if (one.ok) {
					const upserted: number = one.output.upserted
					void upserted
				} else {
					const error: ChildJobFailedError = one.error
					void error
				}
				const none = await ctx.step.triggerAndWait('none', unregistered, 'a')
				if (none.ok) {
					const output: undefined = none.output
					void output
				}
				// @ts-expect-error Input must match the child's inputSchema
				await ctx.step.triggerAndWait('bad', fetchAccount, { accountId: 1 })
				// @ts-expect-error Input is required
				await ctx.step.triggerAndWait('missing', fetchAccount)
				await ctx.step.batchTriggerAndWait('many', fetchAccount, [], {
					timeout: 60_000
				})
				// A tuple of inputs returns a tuple of results.
				const [a, b] = await ctx.step.batchTriggerAndWait(
					'pair',
					fetchAccount,
					[{ accountId: 'a' }, { accountId: 'b' }]
				)
				const pair: [boolean, boolean] = [a.ok, b.ok]
				void pair
				// @ts-expect-error The pair has no third result
				const [, , third] = await ctx.step.batchTriggerAndWait(
					'pair',
					fetchAccount,
					[{ accountId: 'a' }, { accountId: 'b' }]
				)
				void third
				const dynamic = await ctx.step.batchTriggerAndWait(
					'dynamic',
					fetchAccount,
					['a'].map((accountId) => ({ accountId }))
				)
				const maybe: WaitResult<{ upserted: number }> | undefined = dynamic[0]
				void maybe
				await ctx.step.batchTriggerAndWait('typo', fetchAccount, [
					// @ts-expect-error Batch items keep excess-property checks
					{ accountId: 'a', acountId: 'typo' }
				])
			}
		})
	})
})

const enabled = Boolean(process.env.DATABASE_URL)
const suite = describe.skipIf(!enabled)
const schema = `BGW_waits_${process.pid}`
const quiet = { debug() {}, info() {}, warn() {}, error() {} }

suite('waiting on child jobs in PostgreSQL', () => {
	const pool = new Pool({ connectionString: process.env.DATABASE_URL })
	pool.on('error', () => {})
	const q = `"${schema}"`
	const parentRuns = new Map<string, number>()
	const beforeWait = new Map<string, number>()
	const results = new Map<string, unknown>()
	let sliced = false
	let continuedOnce = false
	const count = (map: Map<string, number>, id: string) =>
		map.set(id, (map.get(id) ?? 0) + 1)

	const fetch = defineQueue({
		name: 'fetch',
		inputSchema: z.object({ accountId: z.string() }),
		outputSchema: z.object({ upserted: z.number() }),
		serial: 'bank',
		maxAttempts: 1,
		processFn: ({ accountId }) => {
			if (accountId.startsWith('fail')) throw new Error('bank said no')
			return { upserted: accountId.length }
		}
	})
	const slicer = defineQueue({
		name: 'slicer',
		inputSchema: z.string(),
		outputSchema: z.string(),
		processFn: async (input, ctx) => {
			if (!sliced) {
				sliced = true
				await ctx.continue()
			}
			return input.toUpperCase()
		}
	})
	// Shares the children's serial lane, and has a single attempt.
	const sync = defineQueue({
		name: 'sync',
		inputSchema: z.array(z.string()),
		outputSchema: z.number(),
		serial: 'bank',
		maxAttempts: 1,
		processFn: async (accountIds, ctx) => {
			count(parentRuns, ctx.jobId)
			await ctx.step.run('before-v1', () => {
				count(beforeWait, ctx.jobId)
			})
			const fetched = await ctx.step.batchTriggerAndWait(
				'fetch-v1',
				fetch,
				accountIds.map((accountId) => ({ accountId }))
			)
			results.set(ctx.jobId, fetched)
			return fetched.reduce(
				(total, result) => total + (result.ok ? result.output.upserted : 0),
				0
			)
		}
	})
	const timed = defineQueue({
		name: 'timed',
		inputSchema: z.string(),
		processFn: async (accountId, ctx) => {
			await ctx.step.triggerAndWait(
				'slow-v1',
				fetch,
				{ accountId },
				{ timeout: 3_600_000 }
			)
		}
	})
	const grand = defineQueue({
		name: 'grand',
		inputSchema: z.string(),
		processFn: async (accountId, ctx) => {
			count(parentRuns, ctx.jobId)
			const total = await ctx.step.triggerAndWait('sync-v1', sync, [accountId])
			const single = await ctx.step.triggerAndWait('fetch-v1', fetch, {
				accountId: `${accountId}!`
			})
			results.set(ctx.jobId, [total, single])
		}
	})
	const carry = defineQueue({
		name: 'carry',
		inputSchema: z.string(),
		processFn: async (input, ctx) => {
			const result = await ctx.step.triggerAndWait('slice-v1', slicer, input)
			if (!continuedOnce) {
				continuedOnce = true
				await ctx.continue()
			}
			results.set('carry', result)
		}
	})
	const queues = defineQueues([fetch, slicer, sync, timed, grand, carry])
	const base = {
		pgPool: pool,
		schema,
		queues,
		hooks: { createLogger: () => quiet }
	}
	const worker = createBetterWorker(base)
	const only = (...process: (typeof queues)[number]['name'][]) =>
		createBetterWorker({ ...base, process })
	const parents = only('sync', 'timed', 'carry')
	const grands = only('grand')
	// Children record durable outcomes too, in the same statement as their report.
	const children = createBetterWorker({
		...base,
		process: ['fetch', 'slicer'],
		jobOutcomes: {}
	})

	const row = async (id: string) =>
		(
			await pool.query<{
				attempts: number
				run_at: Date
				locked_by: string | null
				payload: { waited?: boolean }
			}>(
				`SELECT attempts, run_at, locked_by, payload FROM ${q}._private_jobs WHERE id = $1`,
				[id]
			)
		).rows[0]
	const childIds = async (parent: string) =>
		(
			await pool.query<{ id: string }>(
				`SELECT child_job_id::text AS id FROM ${q}.bgw_waits WHERE parent_job_id = $1 ORDER BY wait_id, idx`,
				[parent]
			)
		).rows.map((child) => child.id)
	const jobCount = async (task: string) =>
		(
			await pool.query<{ count: number }>(
				`SELECT count(*)::int FROM ${q}.jobs WHERE task_identifier = $1`,
				[task]
			)
		).rows[0]!.count
	const sweep = async () => {
		const client = await pool.connect()
		try {
			return await sweepWaits(client, schema)
		} finally {
			client.release()
		}
	}
	const far = (date: Date | undefined) => (date?.getUTCFullYear() ?? 0) >= 9999

	beforeAll(() => worker.migrate())
	afterEach(async () => {
		await pool.query(`DELETE FROM ${q}._private_jobs`)
		await pool.query(`DELETE FROM ${q}.bgw_wait_groups`)
		await pool.query(
			`UPDATE ${q}._private_job_queues SET locked_by = NULL, locked_at = NULL`
		)
		results.clear()
	})
	afterAll(async () => {
		await pool.query(`DROP SCHEMA ${q} CASCADE`)
		await pool.end()
	})

	test('a parent suspends without spending an attempt or holding its lane, then resumes', async () => {
		const id = (await worker.createJob('sync', ['ab', 'fail-1']))!
		await parents.runOnce()

		const suspended = await row(id)
		expect(suspended).toMatchObject({ attempts: 0, locked_by: null })
		expect(far(suspended?.run_at)).toBe(true)
		const lane = await pool.query(
			`SELECT locked_by FROM ${q}._private_job_queues WHERE queue_name = 'bank'`
		)
		expect(lane.rows[0].locked_by).toBeNull()
		const [ok, failed] = await childIds(id)
		expect((await row(ok!))?.payload.waited).toBe(true)
		expect(await worker.getJob(id)).toMatchObject({
			state: 'waiting',
			waitingOn: [ok, failed],
			attempts: 0
		})

		// Children run in the parent's serial lane; the last one wakes the parent.
		await children.runOnce()
		expect((await row(id))!.run_at.getTime()).toBeLessThanOrEqual(Date.now())
		expect(await worker.getJob(ok!)).toMatchObject({ state: 'completed' })

		await parents.runOnce()
		expect(await worker.getJob(id)).toMatchObject({ state: 'not_found' })
		expect(parentRuns.get(id)).toBe(2)
		expect(beforeWait.get(id)).toBe(1)
		const [first, second] = results.get(id) as WaitResult<unknown>[]
		expect(first).toEqual({ ok: true, id: ok!, output: { upserted: 2 } })
		expect(second).toMatchObject({ ok: false, id: failed })
		if (second?.ok !== false) throw new Error('expected a failure')
		expect(second.error.message).toBe(
			`Child job ${failed} of "fetch" failed: bank said no`
		)
		// The wait consumed its group when it checkpointed the results.
		expect(
			(await pool.query(`SELECT 1 FROM ${q}.bgw_wait_groups`)).rowCount
		).toBe(0)
	})

	test('a replay reuses the enqueued children, also after a crash past the suspend', async () => {
		const id = (await worker.createJob('sync', ['abc']))!
		await parents.runOnce()
		// A replay before the children finish, e.g. a crash between enqueue and suspend.
		await pool.query(
			`UPDATE ${q}._private_jobs SET run_at = now() WHERE id = $1`,
			[id]
		)
		await parents.runOnce()
		expect(parentRuns.get(id)).toBe(2)
		expect(await jobCount('fetch')).toBe(1)
		expect((await row(id))?.attempts).toBe(0)

		// A worker that died after committing the suspend (which gave its attempt back)
		// but before Graphile's failure update unlocked the job.
		await pool.query(
			`UPDATE ${q}._private_jobs SET locked_by = 'dead-pool', locked_at = now() WHERE id = $1`,
			[id]
		)
		await pool.query(
			`INSERT INTO ${q}._bgw_worker_heartbeats VALUES ('dead-pool', now() - interval '1 hour')`
		)
		const client = await pool.connect()
		try {
			await sweepDeadWorkers(client, schema, 60_000)
		} finally {
			client.release()
		}
		// Released, it still sleeps until its children wake it.
		expect(far((await row(id))?.run_at)).toBe(true)
		await children.runOnce()
		await parents.runOnce()
		expect((results.get(id) as WaitResult<unknown>[])[0]).toMatchObject({
			ok: true,
			output: { upserted: 3 }
		})
	})

	test('a child that reports while its parent suspends still wakes it', async () => {
		const id = (await worker.createJob('sync', ['abcd']))!
		await parents.runOnce()
		const [child] = await childIds(id)
		await pool.query(
			`UPDATE ${q}._private_jobs SET locked_by = 'child-pool', locked_at = now(), attempts = attempts + 1 WHERE id = $1`,
			[child]
		)
		// Woken early (e.g. by its deadline or crash recovery), the parent replays. It
		// holds the group lock and has not committed its suspend yet, so the child's
		// statement snapshot still shows it runnable.
		await pool.query(
			`UPDATE ${q}._private_jobs SET run_at = now() WHERE id = $1`,
			[id]
		)
		const suspending = await pool.connect()
		await suspending.query('BEGIN')
		await suspending.query(
			`SELECT 1 FROM ${q}.bgw_wait_groups WHERE parent_job_id = $1 FOR UPDATE`,
			[id]
		)
		const reporting = settleLockedJob(
			{
				job: { id: child, locked_by: 'child-pool' },
				withPgClient: async (fn: (client: PoolClient) => unknown) => {
					const client = await pool.connect()
					try {
						return await fn(client)
					} finally {
						client.release()
					}
				}
			} as unknown as JobHelpers,
			schema,
			{ report: { ok: true, output: { upserted: 4 } }, finish: true }
		)
		// The child's statement takes its snapshot, then blocks on the group row.
		for (;;) {
			const waiting = await pool.query(
				`SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`
			)
			if (waiting.rowCount) break
		}
		await suspending.query(
			`UPDATE ${q}._private_jobs SET run_at = '9999-01-01' WHERE id = $1`,
			[id]
		)
		await suspending.query('COMMIT')
		suspending.release()

		expect(await reporting).toBe(true)
		expect((await row(id))!.run_at.getTime()).toBeLessThanOrEqual(Date.now())
		expect(await row(child!)).toBeUndefined()
	})

	test('the sweep fails waits on children that ended without reporting', async () => {
		const id = (await worker.createJob('sync', ['gone', 'failed']))!
		await parents.runOnce()
		const [gone, failed] = await childIds(id)
		await pool.query(`DELETE FROM ${q}._private_jobs WHERE id = $1`, [gone])
		await worker.failJobs([failed!], 'gave up')

		expect((await sweep()).sort()).toEqual([gone!, failed!].sort())
		expect(await sweep()).toEqual([])
		await parents.runOnce()
		const [first, second] = results.get(id) as WaitResult<unknown>[]
		if (first?.ok !== false || second?.ok !== false)
			throw new Error('expected failures')
		expect(first.error.message).toContain('finished without reporting')
		expect(second.error.message).toContain('gave up')
	})

	test('the sweep wakes a parent whose wakeup Graphile delayed, and drops groups of deleted parents', async () => {
		const id = (await worker.createJob('sync', ['abc']))!
		await parents.runOnce()
		await children.runOnce()
		// As if Graphile's batched failure update ran after the child's wakeup.
		await pool.query(
			`UPDATE ${q}._private_jobs SET run_at = now() + interval '1 hour' WHERE id = $1`,
			[id]
		)
		await sweep()
		expect((await row(id))!.run_at.getTime()).toBeLessThanOrEqual(Date.now())

		const orphan = (await worker.createJob('sync', ['x']))!
		await parents.runOnce()
		await pool.query(`DELETE FROM ${q}._private_jobs WHERE id = $1`, [orphan])
		await sweep()
		expect(await childIds(orphan)).toEqual([])
		expect(
			(
				await pool.query(
					`SELECT 1 FROM ${q}.bgw_wait_groups WHERE parent_job_id = $1`,
					[orphan]
				)
			).rowCount
		).toBe(0)
	})

	test('a timeout schedules the parent at its deadline and fails it on resume', async () => {
		const id = (await worker.createJob('timed', 'slow'))!
		const before = Date.now()
		await parents.runOnce()
		const runAt = (await row(id))!.run_at.getTime()
		expect(runAt).toBeGreaterThanOrEqual(before + 3_600_000)
		expect(runAt).toBeLessThan(Date.now() + 3_600_000 + 5_000)

		await pool.query(
			`UPDATE ${q}.bgw_wait_groups SET deadline = now() WHERE parent_job_id = $1`,
			[id]
		)
		await pool.query(
			`UPDATE ${q}._private_jobs SET run_at = now() WHERE id = $1`,
			[id]
		)
		await parents.runOnce()
		const state = await worker.getJob(id)
		expect(state).toMatchObject({ state: 'failed' })
		expect(state.state === 'failed' && state.lastError).toContain(
			'Wait "slow-v1" timed out with 1 unfinished child job(s)'
		)
		// The still-running child no longer has a wait to report to.
		await children.runOnce()
		expect(await jobCount('fetch')).toBe(0)
	})

	test('nested and sequential waits', async () => {
		const id = (await worker.createJob('grand', 'ab'))!
		await grands.runOnce() // waits on sync
		await parents.runOnce() // sync waits on fetch
		await children.runOnce()
		await parents.runOnce() // sync finishes and wakes grand
		await grands.runOnce() // waits on the second fetch
		await children.runOnce()
		await grands.runOnce()
		expect(parentRuns.get(id)).toBe(3)
		expect(results.get(id)).toMatchObject([
			{ ok: true, output: 2 },
			{ ok: true, output: { upserted: 3 } }
		])
	})

	test('a child continuation reports for its run, and a parent continuation keeps its waits', async () => {
		const id = (await worker.createJob('carry', 'hello'))!
		await parents.runOnce()
		await children.runOnce() // continues once, then the continuation reports
		await parents.runOnce() // resumes and continues
		await parents.runOnce() // the continuation replays the checkpointed wait
		expect(results.get('carry')).toMatchObject({ ok: true, output: 'HELLO' })
		expect(await jobCount('slicer')).toBe(0)
		const outcomes = await pool.query(
			`SELECT state FROM ${q}.bgw_job_outcomes WHERE task_identifier = 'slicer' ORDER BY job_id`
		)
		expect(outcomes.rows.map((outcome) => outcome.state)).toEqual([
			'continued',
			'completed'
		])
		void id
	})

	test('a started worker drives many parents to completion', async () => {
		const total = 20
		let finished = 0
		let done: () => void = () => {}
		const allDone = new Promise<void>((resolve) => {
			done = resolve
		})
		const started = createBetterWorker({
			...base,
			concurrency: 4,
			// Graphile's batched unlock makes each serial-lane hand-off wait one poll.
			pollInterval: 10,
			crashRecovery: { heartbeatInterval: 50, staleAfter: 150 },
			hooks: {
				createLogger: () => quiet,
				onJobFinished: (event) => {
					if (
						event.queue === 'sync' &&
						event.status === 'success' &&
						!event.waiting
					)
						if (++finished === total) done()
				}
			}
		})
		await started.createJobs(
			'sync',
			Array.from({ length: total }, (_, index) => [`a${index}`, `b${index}`])
		)
		await started.start()
		await allDone
		await started.stop()
		expect(finished).toBe(total)
		expect(await jobCount('fetch')).toBe(0)
	})
})
