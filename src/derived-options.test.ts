import { describe, expect, test } from 'bun:test'
import { z } from 'zod'
import type { AddJobsJobSpec, TaskSpec } from 'graphile-worker'
import { bindCreateJob } from './create-job.js'
import { addJobsSql } from './enqueue-sql.js'
import type { EnqueueAdapter } from './client.js'
import { defineQueue } from './queue.js'
import { buildTaskList } from './worker.js'
import { normalizeWorkerQueues } from './registry.js'
import { createTestHarness } from './testing.js'
import { createBetterWorker } from './create-better-worker.js'
import type { Pool } from 'pg'

const advance = defineQueue({
	name: 'advance',
	inputSchema: z.object({ shop: z.string(), run: z.string() }),
	serial: true,
	priority: 1,
	deriveJobOptions: ({ shop, run }) => ({
		queueName: `shop:${shop}`,
		jobKey: `run:${run}`,
		flags: ['derived']
	})
})
const preserve = defineQueue({
	name: 'preserve',
	inputSchema: z.object({ key: z.string().optional(), mode: z.string() }),
	deriveJobOptions: ({ key, mode }) => ({
		...(key ? { jobKey: key } : {}),
		jobKeyMode: mode as 'replace'
	})
})
const plain = defineQueue({
	name: 'plain',
	inputSchema: z.string(),
	serial: 'lane'
})
const queues = [advance, preserve, plain] as const

function recorder() {
	const single: TaskSpec[] = []
	const batches: { specs: AddJobsJobSpec[]; preserve: boolean | undefined }[] =
		[]
	const enqueue: EnqueueAdapter = {
		async addJob(_identifier, _payload, spec = {}) {
			single.push(spec)
			return { id: String(single.length) } as never
		},
		async addJobs(specs, preserveRunAt) {
			batches.push({ specs: [...specs], preserve: preserveRunAt })
			return specs.map((_, index) => ({ id: String(index + 1) })) as never
		}
	}
	return { single, batches, enqueue }
}

describe('deriveJobOptions', () => {
	test('explicit > derived > serial/static defaults', async () => {
		const { single, enqueue } = recorder()
		const producer = bindCreateJob({ queues, enqueue, otel: null })
		await producer.createJob('advance', { shop: 'a', run: '1' })
		await producer.createJob(
			'advance',
			{ shop: 'a', run: '2' },
			{ queueName: 'explicit', priority: 5 }
		)
		await producer.createJob('plain', 'x')
		expect(single[0]).toMatchObject({
			queueName: 'shop:a',
			jobKey: 'run:1',
			priority: 1,
			flags: ['derived']
		})
		expect(single[1]).toMatchObject({
			queueName: 'explicit',
			jobKey: 'run:2',
			priority: 5
		})
		expect(single[2]?.queueName).toBe('lane')
	})

	test('rejects invalid derived values through onEnqueueFail', async () => {
		const failures: unknown[] = []
		const bad = defineQueue({
			name: 'bad',
			inputSchema: z.string(),
			deriveJobOptions: (input) => ({ jobKey: input })
		})
		const producer = bindCreateJob({
			queues: [bad],
			enqueue: recorder().enqueue,
			otel: null,
			hooks: { onEnqueueFail: (event) => void failures.push(event.error) }
		})
		await expect(producer.createJob('bad', '')).rejects.toThrow(/empty/)
		expect(failures).toHaveLength(1)
	})

	test('createJobs derives per-item keys and lanes', async () => {
		const { batches, enqueue } = recorder()
		const producer = bindCreateJob({ queues, enqueue, otel: null })
		await producer.createJobs('advance', [
			{ shop: 'a', run: '1' },
			{ shop: 'b', run: '2' }
		])
		expect(
			batches[0]?.specs.map(({ queueName, jobKey }) => ({ queueName, jobKey }))
		).toEqual([
			{ queueName: 'shop:a', jobKey: 'run:1' },
			{ queueName: 'shop:b', jobKey: 'run:2' }
		])
		expect(batches[0]?.preserve).toBe(false)
	})

	test('rejects asynchronous options and invalid modes or flags before enqueue', async () => {
		for (const value of [
			Promise.resolve({ queueName: 'lane' }),
			{ jobKeyMode: 'bad' },
			{ flags: 'slow' },
			{ flags: [1] }
		]) {
			const { single, batches, enqueue } = recorder()
			const bad = defineQueue({
				name: 'bad',
				inputSchema: z.string(),
				deriveJobOptions: () => value as never
			})
			const producer = bindCreateJob({ queues: [bad], enqueue, otel: null })
			await expect(producer.createJob('bad', 'x')).rejects.toThrow(TypeError)
			await expect(producer.prepareJobs('bad', ['x'])).rejects.toThrow(
				TypeError
			)
			expect(single).toHaveLength(0)
			expect(batches).toHaveLength(0)
		}
	})

	test('derives from wire input and ignores unsupported derived fields', async () => {
		const { single, enqueue } = recorder()
		const transformed = defineQueue({
			name: 'transformed',
			inputSchema: z.string().transform(Number),
			deriveJobOptions: (input) => ({ jobKey: input, maxAttempts: 99 })
		})
		const producer = bindCreateJob({
			queues: [transformed],
			enqueue,
			otel: null
		})
		await producer.createJob('transformed', '12')
		expect(single[0]).toMatchObject({ jobKey: '12', maxAttempts: 4 })
	})

	test('empty batches still validate explicit options', async () => {
		const producer = bindCreateJob({
			queues,
			enqueue: recorder().enqueue,
			otel: null
		})
		await expect(
			producer.createJobs('plain', [], { priority: 32768 })
		).rejects.toThrow(RangeError)
		await expect(
			producer.prepareJobs('plain', [], { maxAttempts: 0 })
		).rejects.toThrow(RangeError)
	})

	test('createJobs rejects duplicate and conflicting keys before SQL', async () => {
		const { batches, enqueue } = recorder()
		const producer = bindCreateJob({ queues, enqueue, otel: null })
		await expect(
			producer.createJobs('advance', [
				{ shop: 'a', run: '1' },
				{ shop: 'b', run: '1' }
			])
		).rejects.toThrow('items 0 and 1 share jobKey "run:1"')
		await expect(
			producer.createJobs('preserve', [
				{ key: 'a', mode: 'replace' },
				{ key: 'b', mode: 'preserve_run_at' }
			])
		).rejects.toThrow('item 1')
		await expect(
			producer.createJobs('preserve', [{ key: 'a', mode: 'unsafe_dedupe' }])
		).rejects.toThrow('item 0')
		expect(batches).toHaveLength(0)
		await producer.createJobs('preserve', [
			{ key: 'a', mode: 'preserve_run_at' },
			{ mode: 'unsafe_dedupe' }
		])
		expect(batches[0]?.preserve).toBe(true)
	})

	test('cron-init fan-out goes through derived keys', async () => {
		const { batches, enqueue } = recorder()
		const tick = defineQueue({
			name: 'tick',
			cron: '* * * * *',
			inputSchema: z.string(),
			deriveJobOptions: (occurrence) => ({ jobKey: `occ:${occurrence}` }),
			initFn: () => ['a', 'b'],
			processFn: () => {}
		})
		const producer = bindCreateJob({ queues: [tick], enqueue, otel: null })
		const tasks = buildTaskList(normalizeWorkerQueues([tick]), {
			hooks: { createLogger: () => ({ ...console, info() {} }) },
			completedJobs: { add() {} } as never,
			createJob: producer.enqueueOne,
			createJobs: producer.enqueueMany,
			logger: console,
			schema: 'graphile_worker',
			otel: null
		})
		await tasks['tick_cron-init']!(
			{ __bgw: 2, payload: null, payloadUndefined: true },
			{
				job: {
					id: '1',
					attempts: 1,
					max_attempts: 1,
					task_identifier: 'tick_cron-init',
					created_at: new Date()
				},
				abortSignal: new AbortController().signal
			} as never
		)
		expect(batches[0]?.specs.map((spec) => spec.jobKey)).toEqual([
			'occ:a',
			'occ:b'
		])
	})
})

describe('prepareJob', () => {
	test('builds scalar-only add_job SQL with the derived spec', async () => {
		const producer = bindCreateJob({
			queues,
			enqueue: recorder().enqueue,
			otel: null,
			schema: 'Custom_Schema'
		})
		const job = await producer.prepareJob(
			'advance',
			{ shop: 'a', run: '1' },
			{ runAt: new Date('2030-01-01T00:00:00Z') }
		)
		expect(job.skipped).toBe(false)
		expect(job.text).toContain('"Custom_Schema".add_job(')
		for (const value of job.values)
			expect(['string', 'number', 'boolean']).toContain(typeof (value ?? ''))
		expect(job.values).toContain('shop:a')
		expect(job.values).toContain('run:1')
		expect(job.values).toContain('2030-01-01T00:00:00.000Z')
		expect(JSON.parse(job.values[1] as string)).toMatchObject({
			__bgw: 2,
			payload: { shop: 'a', run: '1' }
		})
	})

	test('validation errors throw at prepare time; skip yields a no-op', async () => {
		let skip = false
		const producer = bindCreateJob({
			queues,
			enqueue: recorder().enqueue,
			otel: null,
			hooks: { shouldSkipEnqueue: () => skip }
		})
		await expect(producer.prepareJob('plain', 1 as never)).rejects.toThrow()
		skip = true
		expect(await producer.prepareJob('plain', 'x')).toEqual({
			queue: 'plain',
			text: 'SELECT NULL::text AS id WHERE false',
			values: [],
			skipped: true
		})
	})

	test('prepareJobs builds one add_jobs statement', async () => {
		const producer = bindCreateJob({
			queues,
			enqueue: recorder().enqueue,
			otel: null
		})
		const job = await producer.prepareJobs('preserve', [
			{ key: 'a', mode: 'preserve_run_at' }
		])
		expect(job.text).toContain('add_jobs(')
		expect(job.values[1]).toBe(true)
	})
})

describe('process option', () => {
	const contracts = [
		defineQueue({ name: 'a', inputSchema: z.string() }),
		defineQueue({ name: 'b', cron: '* * * * *' })
	] as const
	test('runs a subset but keeps the full producer registry', () => {
		const worker = createBetterWorker({
			pgPool: {} as Pool,
			queues: contracts,
			handlers: { a: () => {}, b: () => {} },
			process: ['a']
		})
		expect(Object.keys(worker.buildTaskList())).toEqual(['a'])
		expect(worker.buildCronItems()).toEqual([])
		expect(Object.keys(worker.jobs)).toEqual(['a', 'b'])
	})
	test('rejects unknown, duplicate and empty lists', () => {
		for (const process of [['x'], ['a', 'a'], []])
			expect(() =>
				createBetterWorker({
					pgPool: {} as Pool,
					queues: contracts,
					handlers: { a: () => {}, b: () => {} },
					process: process as never
				})
			).toThrow(TypeError)
	})
})

describe('test harness continue', () => {
	test('cron init treats continuation as success without fan-out', async () => {
		const harness = createTestHarness([
			defineQueue({
				name: 'tick',
				cron: '* * * * *',
				inputSchema: z.string(),
				initFn: async (ctx) => ctx.continue(),
				processFn: () => {}
			})
		])
		expect(await harness.init('tick')).toEqual([])
		expect(harness.continued[0]?.queue).toBe('tick_cron-init')
		const ctx = harness.context('tick')
		await expect(ctx.continue()).rejects.toThrow('Job continued')
		await expect(ctx.continue()).rejects.toThrow('already called')
		expect(harness.continued).toHaveLength(2)
	})
	test('records the continuation and resolves process()', async () => {
		let after = false
		const harness = createTestHarness([
			defineQueue({
				name: 'slice',
				inputSchema: z.string(),
				processFn: async (_input, ctx) => {
					await ctx.continue({ runAt: '2030-01-01T00:00:00Z' })
					after = true
				}
			})
		])
		await harness.process('slice', 'x')
		expect(after).toBe(false)
		expect(harness.continued).toEqual([
			{
				queue: 'slice',
				jobId: expect.any(String),
				runAt: new Date('2030-01-01T00:00:00Z')
			}
		])
	})
})

describe('addJobsSql', () => {
	test('rejects identical unkeyed specs it could not tell apart', () => {
		const spec = { identifier: 'a', payload: { n: 1 } }
		expect(() =>
			addJobsSql('graphile_worker', [spec, { ...spec, identifier: 'b' }])
		).toThrow('differ only by identifier or queueName')
		expect(() => addJobsSql('graphile_worker', [spec, spec])).not.toThrow()
	})
})
