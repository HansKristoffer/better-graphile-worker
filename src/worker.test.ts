import type { QueueHandlers } from './queue.js'
import { describe, test, expect, expectTypeOf } from 'bun:test'
import { z } from 'zod'
import type { JobHelpers } from 'graphile-worker'
import type { Pool } from 'pg'
import {
	buildTaskList,
	buildCronItems,
	type TaskListRuntime
} from './worker.js'
import { DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS } from './create-job.js'
import { injectTraceContext, extractProducerLink } from './payload.js'
import { normalizeWorkerQueues } from './registry.js'
import { defineQueue, CRON_INIT_SUFFIX, getQueueType } from './queue.js'
import {
	createBetterWorker,
	DEFAULT_GRAPHILE_WORKER_SCHEMA
} from './create-better-worker.js'
import { createCompletedJobsStore } from './completed-jobs-store.js'
import type {
	JobFinishedEvent,
	JobLogger,
	PermanentFailureEvent
} from './hooks.js'
import { NonRetriableError } from './errors.js'

const silentLogger: JobLogger = {
	debug() {},
	info() {},
	warn() {},
	error() {}
}

const regularQueue = defineQueue({
	name: 'testRegular',
	inputSchema: z.object({ id: z.string() })
})
const regularQueueHandler: QueueHandlers<
	readonly [typeof regularQueue]
>['testRegular'] = async () => {}

const cronQueue = defineQueue({
	name: 'testCron',
	cron: '0 * * * *'
})
const cronQueueHandler: QueueHandlers<readonly [typeof cronQueue]>['testCron'] =
	async () => {}

const cronInitQueue = defineQueue({
	name: 'testCronInit',
	cron: '0 * * * *',
	inputSchema: z.object({ itemId: z.string() })
})
const cronInitQueueHandler: QueueHandlers<
	readonly [typeof cronInitQueue]
>['testCronInit'] = {
	initFn: async () => [{ itemId: 'test' }],
	processFn: async () => {}
}

const testQueues = [regularQueue, cronQueue, cronInitQueue] as const
const handlers: QueueHandlers<typeof testQueues> = {
	testRegular: regularQueueHandler,
	testCron: cronQueueHandler,
	testCronInit: cronInitQueueHandler
}
const normalized = normalizeWorkerQueues(testQueues, handlers)

function testRuntime(): TaskListRuntime<typeof testQueues> {
	return {
		hooks: { createLogger: () => silentLogger },
		completedJobs: createCompletedJobsStore(),
		createJob: (async () => null) as TaskListRuntime<
			typeof testQueues
		>['createJob'],
		createJobs: (async () => []) as TaskListRuntime<
			typeof testQueues
		>['createJobs'],
		logger: silentLogger,
		schema: DEFAULT_GRAPHILE_WORKER_SCHEMA
	}
}

describe('current payload protocol', () => {
	test.each([
		undefined,
		null,
		'text',
		12,
		['a'],
		{ __trace: 'business', traceparent: 'business', steps: 'business' }
	])('round-trips business values: %p', (value) => {
		const result = extractProducerLink(injectTraceContext(value, null))
		expect(result.link).toBeNull()
		expect(result.cleanPayload).toEqual(value)
	})
	test.each([
		{},
		null,
		'raw',
		{ __bgw: 1, payload: {} },
		{ __bgw: 3, payload: {} },
		{ __bgw: 2 },
		{ __bgw: 2, payload: 'not null', payloadUndefined: true }
	])('rejects unsupported payloads: %p', (value) => {
		expect(() => extractProducerLink(value)).toThrow(NonRetriableError)
	})
	test('reads a valid W3C producer link, including unsampled flags', () => {
		const envelope = {
			...injectTraceContext({ id: '1' }, null),
			traceparent: `00-${'a'.repeat(32)}-${'b'.repeat(16)}-00`
		}
		expect(extractProducerLink(envelope).link?.context).toEqual({
			traceId: 'a'.repeat(32),
			spanId: 'b'.repeat(16),
			traceFlags: 0
		})
	})
	test.each([
		'invalid',
		`00-${'0'.repeat(32)}-${'b'.repeat(16)}-01`,
		`00-${'a'.repeat(32)}-${'0'.repeat(16)}-01`
	])('ignores invalid trace metadata: %s', (traceparent) => {
		const envelope = { ...injectTraceContext({ id: '1' }, null), traceparent }
		expect(extractProducerLink(envelope).link).toBeNull()
		expect(extractProducerLink(envelope).cleanPayload).toEqual({ id: '1' })
	})
})

describe('getQueueType', () => {
	test('returns "regular" for regular queue', () => {
		expect(getQueueType(regularQueue)).toBe('regular')
	})

	test('returns "cron" for simple cron queue', () => {
		expect(getQueueType(cronQueue)).toBe('cron')
	})

	test('returns "cron-init" for cron init queue', () => {
		expect(getQueueType(cronInitQueue)).toBe('cron-init')
	})

	test('return type is union of valid types', () => {
		type QueueTypeReturn = ReturnType<typeof getQueueType>
		expectTypeOf<QueueTypeReturn>().toEqualTypeOf<
			'regular' | 'cron' | 'cron-init'
		>()
	})
})

describe('buildTaskList', () => {
	test('returns a TaskList object', () => {
		const taskList = buildTaskList(normalized, testRuntime())
		expect(typeof taskList).toBe('object')
		expect(taskList).not.toBeNull()
	})

	test('contains all registered regular queues', () => {
		const taskList = buildTaskList(normalized, testRuntime())
		expect(taskList.testRegular).toBeDefined()
		expect(typeof taskList.testRegular).toBe('function')
	})

	test('contains both init and process tasks for cron-init queues', () => {
		const taskList = buildTaskList(normalized, testRuntime())
		expect(taskList.testCronInit).toBeDefined()
		expect(taskList[`testCronInit${CRON_INIT_SUFFIX}`]).toBeDefined()
	})

	test('task functions are callable', () => {
		const taskList = buildTaskList(normalized, testRuntime())
		for (const taskName of Object.keys(taskList)) {
			expect(typeof taskList[taskName]).toBe('function')
		}
	})

	test('number of tasks matches expected based on queue types', () => {
		const taskList = buildTaskList(normalized, testRuntime())
		let expectedCount = 0
		for (const queue of testQueues) {
			if (getQueueType(queue) === 'cron-init') {
				expectedCount += 2
			} else {
				expectedCount += 1
			}
		}
		expect(Object.keys(taskList).length).toBe(expectedCount)
	})
})

describe('buildCronItems', () => {
	test('returns an array', () => {
		expect(Array.isArray(buildCronItems(testQueues))).toBe(true)
	})

	test('only includes queues with cron schedules', () => {
		const cronItems = buildCronItems(testQueues)
		const cronQueues = testQueues.filter((q) => 'cron' in q && q.cron)
		expect(cronItems.length).toBe(cronQueues.length)
	})

	test('cron items have correct structure', () => {
		for (const item of buildCronItems(testQueues)) {
			expect(item).toHaveProperty('task')
			expect(item).toHaveProperty('match')
			expect(item).toHaveProperty('payload')
			expect(typeof item.task).toBe('string')
			expect(typeof item.match).toBe('string')
			expect(typeof item.payload).toBe('object')
		}
	})

	test('cron-init queues use suffixed task name', () => {
		const matchingItem = buildCronItems(testQueues).find(
			(item) => item.task === `testCronInit${CRON_INIT_SUFFIX}`
		)
		expect(matchingItem).toBeDefined()
		expect(matchingItem?.match).toBe('0 * * * *')
	})

	test('simple cron queues use base task name', () => {
		const matchingItem = buildCronItems(testQueues).find(
			(item) => item.task === 'testCron'
		)
		expect(matchingItem).toBeDefined()
	})

	test('cron items use a versioned empty payload', () => {
		for (const item of buildCronItems(testQueues)) {
			expect(item.payload).toEqual({
				__bgw: 2,
				payload: null,
				payloadUndefined: true
			})
		}
	})

	test('cron items set maxAttempts from queue or default', () => {
		for (const queue of testQueues) {
			if (!('cron' in queue) || !queue.cron) continue
			const taskName =
				getQueueType(queue) === 'cron-init'
					? `${queue.name}${CRON_INIT_SUFFIX}`
					: queue.name
			const matchingItem = buildCronItems(testQueues).find(
				(item) => item.task === taskName
			)
			expect(matchingItem?.options?.maxAttempts).toBe(
				queue.maxAttempts ?? DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS
			)
		}
	})
})

describe('simple cron queue handling', () => {
	const simpleCronQueue = defineQueue({
		name: 'testSimpleCron',
		cron: '*/5 * * * *'
	})

	test('getQueueType correctly identifies simple cron vs cron-init', () => {
		expect(getQueueType(simpleCronQueue)).toBe('cron')
		expect(getQueueType(cronInitQueue)).toBe('cron-init')
	})

	test('simple cron queue creates only one task (no suffix)', () => {
		const taskList = buildTaskList(normalized, testRuntime())
		expect(taskList.testCron).toBeDefined()
		expect(taskList[`testCron${CRON_INIT_SUFFIX}`]).toBeUndefined()
	})
})

describe('start / stop', () => {
	test('stop can be called without starting (no-op)', async () => {
		const worker = createBetterWorker({
			pgPool: {} as Pool,
			queues: testQueues,
			handlers,
			hooks: { createLogger: () => silentLogger }
		})
		await expect(worker.stop()).resolves.toBeUndefined()
	})
})

describe('type exports', () => {
	test('extractProducerLink return type is correct', () => {
		type ExtractResult = ReturnType<typeof extractProducerLink>
		expectTypeOf<ExtractResult>().toMatchTypeOf<{
			link: unknown
			cleanPayload: unknown
		}>()
	})

	test('buildTaskList returns TaskList compatible type', () => {
		const taskList = buildTaskList(normalized, testRuntime())
		expectTypeOf(taskList).toMatchTypeOf<Record<string, unknown>>()
	})

	test('buildCronItems returns array of CronItem compatible objects', () => {
		const cronItems = buildCronItems(testQueues)
		expectTypeOf(cronItems).toBeArray()
		for (const item of cronItems) {
			expectTypeOf(item.task).toMatchTypeOf<string>()
			expectTypeOf(item.match).toMatchTypeOf<string | object>()
		}
	})
})

function fakeJobHelpers(): JobHelpers {
	return {
		job: {
			id: '1',
			locked_by: 'test-worker',
			attempts: 1,
			max_attempts: 4,
			task_identifier: 'testRegular',
			created_at: new Date()
		},
		withPgClient: async (fn: (client: unknown) => Promise<unknown>) =>
			fn({ query: async () => ({ rowCount: 1 }) }),
		abortSignal: new AbortController().signal
	} as unknown as JobHelpers
}

describe('non-retriable failures', () => {
	test('invalid input is retained as a permanent failure', async () => {
		let permanent = 0
		const runtime = testRuntime()
		runtime.hooks.onPermanentFailure = () => {
			permanent += 1
		}
		const taskList = buildTaskList(normalized, runtime)
		await expect(
			taskList.testRegular?.(
				injectTraceContext({ not: 'valid' }, null),
				fakeJobHelpers()
			)
		).rejects.toBeInstanceOf(NonRetriableError)
		expect(permanent).toBe(1)
		expect(runtime.completedJobs.getStats().testRegular?.failed).toBe(1)
	})

	test('NonRetriableError is retained by default', async () => {
		const failing = defineQueue({
			name: 'failOnce',
			inputSchema: z.object({ id: z.string() })
		})
		const failingHandler: QueueHandlers<readonly [typeof failing]>['failOnce'] =
			async () => {
				throw new NonRetriableError('nope')
			}

		const queues = [failing] as const
		const runtime: TaskListRuntime<typeof queues> = {
			...testRuntime(),
			createJob: (async () => null) as TaskListRuntime<
				typeof queues
			>['createJob'],
			createJobs: (async () => []) as TaskListRuntime<
				typeof queues
			>['createJobs']
		}
		const taskList = buildTaskList(
			normalizeWorkerQueues(queues, { failOnce: failingHandler }),
			runtime
		)
		await expect(
			taskList.failOnce?.(
				injectTraceContext({ id: '1' }, null),
				fakeJobHelpers()
			)
		).rejects.toBeInstanceOf(NonRetriableError)
	})
})

describe('failure diagnostics', () => {
	test('a retried failure reports its error message, trimmed in the log', async () => {
		const longMessage = `boom ${'x'.repeat(1000)}`
		const queues = [
			defineQueue({ name: 'flaky', inputSchema: z.object({ id: z.string() }) })
		] as const
		const logged: Record<string, unknown>[] = []
		const finished: JobFinishedEvent[] = []
		let permanent = 0
		const runtime = testRuntime()
		runtime.hooks.createLogger = () => ({
			...silentLogger,
			error: (_message, meta) => {
				logged.push(meta ?? {})
			}
		})
		runtime.hooks.onJobFinished = (event) => {
			finished.push(event)
		}
		runtime.hooks.onPermanentFailure = () => {
			permanent++
		}
		const tasks = buildTaskList(
			normalizeWorkerQueues(queues, {
				flaky: () => {
					throw new TypeError(longMessage)
				}
			}),
			runtime
		)
		await expect(
			tasks.flaky!(injectTraceContext({ id: '1' }, null), fakeJobHelpers())
		).rejects.toThrow(TypeError)

		expect(permanent).toBe(0)
		expect(finished[0]).toMatchObject({
			status: 'failed',
			errorType: 'TypeError',
			errorMessage: longMessage
		})
		const completed = logged.find((meta) => meta.event === 'job.completed')
		expect(completed).toMatchObject({
			error_type: 'TypeError',
			error_message: longMessage.slice(0, 500)
		})
	})

	test('onPermanentFailure receives the payload without the envelope', async () => {
		const events: PermanentFailureEvent[] = []
		const runtime = testRuntime()
		runtime.hooks.onPermanentFailure = (event) => {
			events.push(event)
		}
		const tasks = buildTaskList(normalized, runtime)
		await expect(
			tasks.testRegular!(
				injectTraceContext({ not: 'valid' }, null),
				fakeJobHelpers()
			)
		).rejects.toBeInstanceOf(NonRetriableError)
		expect(events[0]?.payload).toEqual({ not: 'valid' })
	})
})

describe('explicit failure policy', () => {
	test('unsupported payloads are permanent failures and do not invoke handlers', async () => {
		let calls = 0
		const queues = [
			defineQueue({ name: 'strict', inputSchema: z.string() })
		] as const
		const normalized = normalizeWorkerQueues(queues, {
			strict: () => {
				calls++
			}
		})
		const runtime = testRuntime()
		let permanent = 0
		runtime.hooks.onPermanentFailure = () => {
			permanent++
		}
		const tasks = buildTaskList(normalized, runtime)
		for (const payload of ['raw', { __bgw: 1, payload: 'old' }]) {
			await expect(tasks.strict!(payload, fakeJobHelpers())).rejects.toThrow(
				'version-2 envelope'
			)
		}
		expect(calls).toBe(0)
		expect(permanent).toBe(2)
	})
	test('discard remains an explicit choice for permanent failures', async () => {
		const runtime = testRuntime()
		runtime.permanentFailure = 'discard'
		const tasks = buildTaskList(normalized, runtime)
		await expect(
			tasks.testRegular!(
				injectTraceContext({ not: 'valid' }, null),
				fakeJobHelpers()
			)
		).resolves.toBeUndefined()
	})
})
