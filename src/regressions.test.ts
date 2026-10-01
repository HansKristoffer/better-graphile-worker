import { normalizeWorkerQueues } from './registry.js'
import type { QueueHandlers } from './queue.js'
import { describe, expect, test } from 'bun:test'
import { z } from 'zod'
import type { JobHelpers } from 'graphile-worker'
import { defineQueue } from './queue.js'
import { defineQueues } from './define-queues.js'
import { bindCreateJob, createJobsApi } from './create-job.js'
import { createTestHarness } from './testing.js'
import {
	createStepRunner,
	createMemoryStepStore,
	createPgStepStore,
	serializeStepOutput
} from './steps.js'
import { createNoopSpan, parseTraceparent, type OtelApi } from './otel.js'
import { injectTraceContext, extractProducerLink } from './payload.js'
import { buildTaskList, buildCronItems } from './worker.js'
import { createNoopCompletedJobsStore } from './completed-jobs-store.js'
import { StepPersistenceError } from './errors.js'
import type { EnqueueAdapter } from './client.js'

const logger = { debug() {}, info() {}, warn() {}, error() {} }
function producer() {
	const recorded: unknown[] = []
	const queues = defineQueues([
		defineQueue({
			name: 'object',
			inputSchema: z.record(z.string(), z.unknown())
		}),
		defineQueue({
			name: 'async',
			inputSchema: z.string().refine(async (value) => value === 'valid')
		}),
		defineQueue({ name: 'null', inputSchema: z.null() }),
		defineQueue({
			name: 'default',
			inputSchema: z.string().default('fallback')
		})
	])
	const enqueue: EnqueueAdapter = {
		async addJob(_name, payload) {
			recorded.push(payload)
			return { id: '1' } as Awaited<ReturnType<EnqueueAdapter['addJob']>>
		},
		async addJobs(specs) {
			recorded.push(...specs.map((s) => s.payload))
			return []
		}
	}
	return {
		...bindCreateJob({ queues, enqueue, otel: null }),
		recorded,
		queues,
		enqueue
	}
}

describe('producer contracts and wire values', () => {
	test('preserves reserved business fields including nested envelopes', async () => {
		const p = producer()
		const input = {
			__bgw: 2,
			payload: 'business',
			__trace: { note: 'business' },
			traceparent: 'business'
		}
		await p.createJob('object', input)
		expect(extractProducerLink(p.recorded[0]).cleanPayload).toEqual(input)
	})
	test('supports async schemas, null and root undefined', async () => {
		const p = producer()
		await p.createJob('async', 'valid')
		await expect(p.createJob('async', 'invalid')).rejects.toThrow(
			'Invalid payload'
		)
		await p.createJob('null', null)
		await p.createJob('default')
		expect(
			p.recorded.map((raw) => extractProducerLink(raw).cleanPayload)
		).toEqual(['valid', null, undefined])
	})
	test('batch validation reports the index and writes nothing on failure', async () => {
		const p = producer()
		await expect(
			p.createJobs('async', ['valid', 'invalid'])
		).rejects.toMatchObject({
			issues: [{ path: [1], message: 'Invalid input' }]
		})
		expect(p.recorded).toHaveLength(0)
	})
	test('named jobs are stable, enumerable and safe to await', async () => {
		const p = producer()
		const jobs = createJobsApi(p.createJob, p.queues)
		expect(jobs.object).toBe(jobs.object)
		expect(Object.keys(jobs)).toEqual(p.queues.map((q) => q.name))
		expect(await Promise.resolve(jobs)).toBe(jobs)
		expect(() =>
			// @ts-expect-error Reserved literal names also fail at compilation.
			defineQueues([defineQueue({ name: 'then', inputSchema: z.string() })])
		).toThrow('reserved')
	})
	test('enqueue observer rejection preserves the original error', async () => {
		const p = producer()
		const failure = new Error('database down')
		const core = bindCreateJob({
			queues: p.queues,
			enqueue: {
				...p.enqueue,
				async addJob() {
					throw failure
				}
			},
			otel: null,
			hooks: {
				onEnqueueFail: async () => {
					throw new Error('observer down')
				}
			}
		})
		await expect(core.createJob('null', null)).rejects.toBe(failure)
	})
})

describe('step result integrity', () => {
	test('deduplicates concurrent IDs and handles prototype names', async () => {
		const step = createStepRunner({
			store: createMemoryStepStore(),
			span: createNoopSpan()
		})
		let count = 0
		const results = await Promise.all(
			Array.from({ length: 20 }, () =>
				step.run('toString', async () => {
					count++
					await Promise.resolve()
					return { count }
				})
			)
		)
		expect(count).toBe(1)
		expect(results).toEqual(Array.from({ length: 20 }, () => ({ count: 1 })))
		expect(await step.run('__proto__', () => 2)).toBe(2)
	})
	test('codec restores Date on both fresh execution and replay', async () => {
		const store = createMemoryStepStore()
		const step = createStepRunner({ store, span: createNoopSpan() })
		const codec = {
			encode: (date: Date) => date.toISOString(),
			decode: (wire: unknown) => new Date(z.string().parse(wire))
		}
		const first = await step.run('date-v1', () => new Date('2026-01-01'), codec)
		const replay = await step.run(
			'date-v1',
			() => {
				throw new Error('must be cached')
			},
			codec
		)
		expect(first).toBeInstanceOf(Date)
		expect(replay).toEqual(first)
	})
	test('rejects lossy JSON outputs', () => {
		for (const value of [
			NaN,
			Infinity,
			{ value: undefined },
			[undefined],
			new Map(),
			() => {},
			1n
		])
			expect(() => serializeStepOutput('invalid', value)).toThrow(
				'JSON-serializable'
			)
	})
	test('failed checkpoints are retried rather than cached; lock loss is visible', async () => {
		let writes = 0
		const helpers = {
			job: { locked_by: 'owner' },
			withPgClient: async (
				fn: (client: { query(): Promise<unknown> }) => Promise<unknown>
			) =>
				fn({
					async query() {
						writes++
						return { rowCount: writes === 1 ? 0 : 1 }
					}
				})
		} as unknown as JobHelpers
		const store = createPgStepStore({
			helpers,
			schema: 'UpperCase',
			jobId: '1',
			rawPayload: injectTraceContext({}, null)
		})
		const step = createStepRunner({ store, span: createNoopSpan() })
		let runs = 0
		await expect(step.run('work', () => ++runs)).rejects.toBeInstanceOf(
			StepPersistenceError
		)
		expect(store.get('work')).toBeUndefined()
		expect(await step.run('work', () => ++runs)).toBe(2)
	})
})

describe('worker observers and validation', () => {
	test('throwing loggers and observers never retry a successful handler', async () => {
		let runs = 0
		const queue = defineQueue({
			name: 'success',
			inputSchema: z.string()
		})
		const queueHandler: QueueHandlers<readonly [typeof queue]>['success'] =
			async () => {
				runs++
			}

		const harness = createTestHarness([queue], { [queue.name]: queueHandler })
		const ctx = harness.context('success')
		const tasks = buildTaskList(
			normalizeWorkerQueues([queue], { [queue.name]: queueHandler }),
			{
				hooks: {
					createLogger: () => {
						throw new Error('logger')
					},
					onJobFinished: async () => {
						throw new Error('metrics')
					}
				},
				completedJobs: createNoopCompletedJobsStore(),
				createJob: async () => null,
				createJobs: async () => [],
				logger,
				schema: 'graphile_worker',
				otel: null
			}
		)
		await tasks.success!(injectTraceContext('input', null), ctx.helpers)
		expect(runs).toBe(1)
	})
	test('downstream Zod errors remain retriable', async () => {
		const failure = new z.ZodError([
			{ code: 'custom', path: [], message: 'response' }
		])
		const queue = defineQueue({
			name: 'downstream',
			inputSchema: z.string()
		})
		const queueHandler: QueueHandlers<readonly [typeof queue]>['downstream'] =
			async () => {
				throw failure
			}

		const harness = createTestHarness([queue], { [queue.name]: queueHandler })
		const tasks = buildTaskList(
			normalizeWorkerQueues([queue], { [queue.name]: queueHandler }),
			{
				hooks: {},
				completedJobs: createNoopCompletedJobsStore(),
				createJob: async () => null,
				createJobs: async () => [],
				logger,
				schema: 'graphile_worker',
				otel: null
			}
		)
		await expect(
			Promise.resolve(
				tasks.downstream!(
					injectTraceContext('input', null),
					harness.context('downstream').helpers
				)
			)
		).rejects.toBe(failure)
	})
	test('cron-init validates each item once on enqueue', async () => {
		let parses = 0
		const queue = defineQueue({
			name: 'gather',
			cron: '* * * * *',
			inputSchema: z.string().transform((v) => {
				parses++
				return v
			})
		})
		const queueHandler: QueueHandlers<readonly [typeof queue]>['gather'] = {
			initFn: async () => ['a', 'b'],
			processFn: async () => {}
		}

		const harness = createTestHarness([queue], { [queue.name]: queueHandler })
		const core = bindCreateJob({
			queues: [queue],
			enqueue: producer().enqueue,
			otel: null
		})
		const tasks = buildTaskList(
			normalizeWorkerQueues([queue], { [queue.name]: queueHandler }),
			{
				hooks: {},
				completedJobs: createNoopCompletedJobsStore(),
				createJob: core.enqueueOne,
				createJobs: core.enqueueMany,
				logger,
				schema: 'graphile_worker',
				otel: null
			}
		)
		await tasks['gather_cron-init']!(
			injectTraceContext(undefined, null),
			harness.context('gather').helpers
		)
		expect(parses).toBe(2)
	})
	test('cron schedules inherit defaults and get unique identifiers', () => {
		const queue = defineQueue({
			name: 'cron',
			cron: ['* * * * *', '0 * * * *'],
			serial: true,
			priority: 7,
			cronOptions: { identifier: 'custom' }
		})
		const items = buildCronItems([queue], 8)
		expect(items.map((i) => i.identifier)).toEqual(['custom:0', 'custom:1'])
		expect(items[0]?.options).toMatchObject({
			maxAttempts: 8,
			priority: 7,
			queueName: 'cron'
		})
	})
})

describe('harness invocation isolation', () => {
	test('fresh jobs do not share steps, explicit retries do, and enqueues are captured', async () => {
		let runs = 0
		const queues = defineQueues([
			defineQueue({ name: 'parent', inputSchema: z.string() }),
			defineQueue({ name: 'child', inputSchema: z.number() })
		])
		const harness = createTestHarness(queues, {
			parent: async (_payload, ctx) => {
				const result = await ctx.step.run('count', () => ++runs)
				await ctx.createJob('child', result)
			},
			child: async () => {}
		})
		await harness.process('parent', 'a')
		await harness.process('parent', 'b')
		await harness.process('parent', 'c', { jobId: 'retry' })
		await harness.process('parent', 'c', { jobId: 'retry' })
		expect(runs).toBe(3)
		expect(harness.enqueued.map((job) => job.payload)).toEqual([1, 2, 3, 3])
	})
})

test('traceparent rejects malformed IDs and retains unsampled flags', () => {
	for (const trace of [
		`00-${'z'.repeat(32)}-${'a'.repeat(16)}-01`,
		`00-${'0'.repeat(32)}-${'a'.repeat(16)}-01`,
		`ff-${'a'.repeat(32)}-${'a'.repeat(16)}-01`
	])
		expect(parseTraceparent(trace)).toBeNull()
	const ctx = { traceId: 'a'.repeat(32), spanId: 'b'.repeat(16), traceFlags: 0 }
	const api = {
		trace: { getActiveSpan: () => ({ spanContext: () => ctx }) },
		isSpanContextValid: () => true
	} as unknown as OtelApi
	expect(injectTraceContext({}, api).traceparent).toBe(
		`00-${ctx.traceId}-${ctx.spanId}-00`
	)
})
