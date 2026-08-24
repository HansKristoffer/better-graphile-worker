import { describe, test, expect, expectTypeOf } from 'bun:test'
import { z } from 'zod'
import type { JobHelpers } from 'graphile-worker'
import type { Pool } from 'pg'
import {
	extractProducerLink,
	buildTaskList,
	buildCronItems,
	type TaskListRuntime
} from './worker'
import {
	TRACE_CONTEXT_KEY,
	TRACEPARENT_KEY,
	BGW_ENVELOPE_KEY,
	DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS
} from './create-job'
import {
	createQueue,
	CRON_INIT_SUFFIX,
	getQueueType,
	type QueueAny
} from './create-queue'
import { createBetterWorker } from './create-better-worker'
import { createCompletedJobsStore } from './completed-jobs-store'
import type { JobLogger } from './hooks'
import { NonRetriableError } from './errors'

const silentLogger: JobLogger = {
	debug() {},
	info() {},
	warn() {},
	error() {}
}

const regularQueue = createQueue({
	name: 'testRegular',
	inputSchema: z.object({ id: z.string() }),
	processFn: async () => {}
})

const cronQueue = createQueue({
	name: 'testCron',
	cron: '0 * * * *',
	processFn: async () => {}
})

const cronInitQueue = createQueue({
	name: 'testCronInit',
	cron: '0 * * * *',
	inputSchema: z.object({ itemId: z.string() }),
	initFn: async () => [{ itemId: 'test' }],
	processFn: async () => {}
})

const testQueues = [regularQueue, cronQueue, cronInitQueue] as const

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
		logger: silentLogger
	}
}

describe('extractProducerLink', () => {
	test('returns null link for payload without trace context', () => {
		const payload = { userId: 'test', action: 'test' }
		const result = extractProducerLink(payload)

		expect(result.link).toBeNull()
		expect(result.cleanPayload).toEqual({ userId: 'test', action: 'test' })
	})

	test('returns null link for null payload', () => {
		const result = extractProducerLink(null)

		expect(result.link).toBeNull()
		expect(result.cleanPayload).toBeNull()
	})

	test('returns null link for non-object payload', () => {
		const result = extractProducerLink('string payload')

		expect(result.link).toBeNull()
		expect(result.cleanPayload).toBe('string payload')
	})

	test('returns null link for undefined trace context values', () => {
		const payload = { userId: 'test', [TRACE_CONTEXT_KEY]: {} }
		const result = extractProducerLink(payload)

		expect(result.link).toBeNull()
		expect(result.cleanPayload).toEqual({ userId: 'test' })
	})

	test('returns null link for partial trace context (missing spanId)', () => {
		const payload = {
			userId: 'test',
			[TRACE_CONTEXT_KEY]: { traceId: 'abc123' }
		}
		const result = extractProducerLink(payload)

		expect(result.link).toBeNull()
		expect(result.cleanPayload).toEqual({ userId: 'test' })
	})

	test('returns null link for partial trace context (missing traceId)', () => {
		const payload = {
			userId: 'test',
			[TRACE_CONTEXT_KEY]: { spanId: 'def456' }
		}
		const result = extractProducerLink(payload)

		expect(result.link).toBeNull()
		expect(result.cleanPayload).toEqual({ userId: 'test' })
	})

	test('extracts valid trace context and returns link', () => {
		const traceId = '0af7651916cd43dd8448eb211c80319c'
		const spanId = 'b7ad6b7169203331'
		const payload = {
			userId: 'test',
			action: 'process',
			[TRACE_CONTEXT_KEY]: { traceId, spanId }
		}

		const result = extractProducerLink(payload)

		expect(result.link).not.toBeNull()
		expect(result.link?.context.traceId).toBe(traceId)
		expect(result.link?.context.spanId).toBe(spanId)
		expect(result.link?.context.traceFlags).toBe(1)
		expect(result.link?.attributes).toEqual({ 'link.type': 'producer' })
		expect(result.cleanPayload).toEqual({ userId: 'test', action: 'process' })
		expect(
			(result.cleanPayload as Record<string, unknown>)[TRACE_CONTEXT_KEY]
		).toBeUndefined()
	})

	test('unwraps a non-object envelope payload', () => {
		const result = extractProducerLink({
			[BGW_ENVELOPE_KEY]: 1,
			payload: 'hello',
			[TRACE_CONTEXT_KEY]: {
				traceId: '0af7651916cd43dd8448eb211c80319c',
				spanId: 'b7ad6b7169203331'
			}
		})

		expect(result.cleanPayload).toBe('hello')
		expect(result.link?.context.traceId).toBe(
			'0af7651916cd43dd8448eb211c80319c'
		)
	})

	test('reads W3C traceparent from an object payload', () => {
		const result = extractProducerLink({
			userId: 'test',
			[TRACEPARENT_KEY]:
				'00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'
		})

		expect(result.cleanPayload).toEqual({ userId: 'test' })
		expect(result.link?.context.traceId).toBe(
			'0af7651916cd43dd8448eb211c80319c'
		)
		expect(result.link?.context.spanId).toBe('b7ad6b7169203331')
	})

	test('preserves all other payload fields when extracting trace context', () => {
		const payload = {
			id: '123',
			nested: { a: 1, b: { c: 2 } },
			array: [1, 2, 3],
			nullField: null,
			[TRACE_CONTEXT_KEY]: { traceId: 'abc', spanId: 'def' }
		}

		const result = extractProducerLink(payload)

		expect(result.cleanPayload).toEqual({
			id: '123',
			nested: { a: 1, b: { c: 2 } },
			array: [1, 2, 3],
			nullField: null
		})
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
		const taskList = buildTaskList(testQueues, testRuntime())
		expect(typeof taskList).toBe('object')
		expect(taskList).not.toBeNull()
	})

	test('contains all registered regular queues', () => {
		const taskList = buildTaskList(testQueues, testRuntime())
		expect(taskList.testRegular).toBeDefined()
		expect(typeof taskList.testRegular).toBe('function')
	})

	test('contains both init and process tasks for cron-init queues', () => {
		const taskList = buildTaskList(testQueues, testRuntime())
		expect(taskList.testCronInit).toBeDefined()
		expect(taskList[`testCronInit${CRON_INIT_SUFFIX}`]).toBeDefined()
	})

	test('task functions are callable', () => {
		const taskList = buildTaskList(testQueues, testRuntime())
		for (const taskName of Object.keys(taskList)) {
			expect(typeof taskList[taskName]).toBe('function')
		}
	})

	test('number of tasks matches expected based on queue types', () => {
		const taskList = buildTaskList(testQueues, testRuntime())
		let expectedCount = 0
		for (const queue of testQueues) {
			if ('initFn' in queue && 'cron' in queue && 'inputSchema' in queue) {
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

	test('cron items have empty payload', () => {
		for (const item of buildCronItems(testQueues)) {
			expect(item.payload).toEqual({})
		}
	})

	test('cron items set maxAttempts from queue or default', () => {
		for (const queue of testQueues) {
			if (!('cron' in queue) || !queue.cron) continue
			const taskName =
				'initFn' in queue && 'inputSchema' in queue
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
	const simpleCronQueue = createQueue({
		name: 'testSimpleCron',
		cron: '*/5 * * * *',
		processFn: async () => {}
	})

	test('getQueueType correctly identifies simple cron vs cron-init', () => {
		expect(getQueueType(simpleCronQueue)).toBe('cron')
		expect(getQueueType(cronInitQueue)).toBe('cron-init')
	})

	test('simple cron queue creates only one task (no suffix)', () => {
		const taskList = buildTaskList(testQueues, testRuntime())
		expect(taskList.testCron).toBeDefined()
		expect(taskList[`testCron${CRON_INIT_SUFFIX}`]).toBeUndefined()
	})
})

describe('start / stop', () => {
	test('stop can be called without starting (no-op)', async () => {
		const worker = createBetterWorker({
			pgPool: {} as Pool,
			queues: testQueues,
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
		const taskList = buildTaskList(testQueues, testRuntime())
		expectTypeOf(taskList).toMatchTypeOf<Record<string, unknown>>()
	})

	test('buildCronItems returns array of CronItem compatible objects', () => {
		const cronItems = buildCronItems(testQueues as readonly QueueAny[])
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
			attempts: 1,
			max_attempts: 4,
			task_identifier: 'testRegular',
			created_at: new Date()
		},
		abortSignal: new AbortController().signal
	} as unknown as JobHelpers
}

describe('non-retriable failures', () => {
	test('invalid payload is swallowed as a permanent failure', async () => {
		let permanent = 0
		const runtime = testRuntime()
		runtime.hooks.onPermanentFailure = () => {
			permanent += 1
		}
		const taskList = buildTaskList(testQueues, runtime)
		await expect(
			taskList.testRegular?.({ not: 'valid' }, fakeJobHelpers())
		).resolves.toBeUndefined()
		expect(permanent).toBe(1)
		expect(runtime.completedJobs.getStats().testRegular?.failed).toBe(1)
	})

	test('NonRetriableError from processFn is swallowed', async () => {
		const failing = createQueue({
			name: 'failOnce',
			inputSchema: z.object({ id: z.string() }),
			processFn: async () => {
				throw new NonRetriableError('nope')
			}
		})
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
		const taskList = buildTaskList(queues, runtime)
		await expect(
			taskList.failOnce?.({ id: '1' }, fakeJobHelpers())
		).resolves.toBeUndefined()
	})
})
