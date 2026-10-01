import type { QueueHandlers } from './queue.js'
import { extractProducerLink } from './payload.js'
import {
	describe,
	test,
	expect,
	expectTypeOf,
	beforeAll,
	afterAll
} from 'bun:test'
import { Pool } from 'pg'
import { z } from 'zod'
import { bindCreateJob } from './create-job.js'
import { BGW_ENVELOPE_KEY, injectTraceContext } from './payload.js'
import { defineQueue } from './queue.js'
import { createBetterWorker } from './create-better-worker.js'
import type { QueueName, QueueInput } from './types.js'
import { DEFAULT_GRAPHILE_WORKER_SCHEMA } from './client.js'
import type { JobLogger } from './hooks.js'
import { JobValidationError, UnknownQueueError } from './errors.js'
import type { OtelApi } from './otel.js'
import type { EnqueueAdapter } from './client.js'

const silentLogger: JobLogger = {
	debug() {},
	info() {},
	warn() {},
	error() {}
}

const testQueue = defineQueue({
	name: 'bgwTestQueue',
	inputSchema: z.object({
		organizationId: z.string(),
		websiteUrl: z.string()
	}),
	maxAttempts: 7
})

const arrayQueue = defineQueue({
	name: 'bgwArrayQueue',
	inputSchema: z.array(z.string())
})

const stringQueue = defineQueue({
	name: 'bgwStringQueue',
	inputSchema: z.string()
})

const cronQueue = defineQueue({
	name: 'bgwCronQueue',
	cron: '0 * * * *'
})

const testQueues = [testQueue, arrayQueue, stringQueue, cronQueue] as const
type TestQueues = typeof testQueues
type TestQueueName = QueueName<TestQueues>
type TestQueueInput = QueueInput<'bgwTestQueue', TestQueues>

const TEST_QUEUE_NAME = 'bgwTestQueue' satisfies TestQueueName

function testInput(suffix: string): TestQueueInput {
	return {
		organizationId: `org_test_${suffix}`,
		websiteUrl: `https://${suffix}.example.com`
	}
}

type RecordedJob = {
	identifier: string
	payload: unknown
	spec?: unknown
}

function mockUtils() {
	const added: RecordedJob[] = []
	const utils = {
		addJob: async (identifier: string, payload: unknown, spec?: unknown) => {
			added.push({ identifier, payload, spec })
			return { id: String(added.length) }
		},
		addJobs: async (specs: Array<{ identifier: string; payload: unknown }>) => {
			return specs.map((spec) => {
				added.push(spec)
				return { id: String(added.length) }
			})
		}
	} as unknown as EnqueueAdapter

	return { utils, added }
}

const hasRealDatabase = Boolean(process.env.DATABASE_URL)
const integrationDescribe = describe.skipIf(!hasRealDatabase)

const pool = hasRealDatabase
	? new Pool({ connectionString: process.env.DATABASE_URL })
	: ({} as Pool)

const worker = createBetterWorker({
	pgPool: pool,
	queues: testQueues,
	handlers: {
		bgwTestQueue: () => {},
		bgwArrayQueue: () => {},
		bgwStringQueue: () => {},
		bgwCronQueue: () => {}
	},
	hooks: { createLogger: () => silentLogger }
})

if (hasRealDatabase) {
	beforeAll(async () => {
		await worker.migrate()
	})

	afterAll(async () => {
		const utils = await worker.getWorkerUtils()
		await utils.withPgClient(async (client) => {
			await client.query(
				`DELETE FROM ${DEFAULT_GRAPHILE_WORKER_SCHEMA}._private_jobs WHERE task_id IN (
				SELECT id FROM ${DEFAULT_GRAPHILE_WORKER_SCHEMA}._private_tasks WHERE identifier LIKE 'bgw%'
			)`
			)
		})
		await worker.stop()
		await pool.end()
	})
}

describe('createJob type inference', () => {
	test('single job returns string | null', () => {
		type SingleReturn = ReturnType<typeof worker.createJob>
		type Resolved = Awaited<SingleReturn>
		expectTypeOf<Resolved>().toEqualTypeOf<string | null>()
	})

	test('queue name must be valid QueueName', () => {
		type ValidCall = Parameters<typeof worker.createJob>
		expectTypeOf<ValidCall[0]>().toEqualTypeOf<TestQueueName>()
	})

	test('input must match queue schema', () => {
		expectTypeOf<TestQueueInput>().toEqualTypeOf<{
			organizationId: string
			websiteUrl: string
		}>()
	})
})

describe('bindCreateJob unit behavior', () => {
	test('rejects unknown queue names', async () => {
		const { utils } = mockUtils()
		const { createJob } = bindCreateJob({
			enqueue: utils,
			queues: testQueues
		})

		await expect(
			createJob('missing' as never, {} as never)
		).rejects.toBeInstanceOf(UnknownQueueError)
	})

	test('rejects invalid payloads at enqueue time', async () => {
		const { utils } = mockUtils()
		const { createJob } = bindCreateJob({
			enqueue: utils,
			queues: testQueues
		})

		await expect(
			createJob('bgwTestQueue', { organizationId: 'x' } as never)
		).rejects.toBeInstanceOf(JobValidationError)
	})

	test('enqueues an array-schema payload as a single job', async () => {
		const { utils, added } = mockUtils()
		const { createJob } = bindCreateJob({
			enqueue: utils,
			queues: testQueues
		})

		const jobId = await createJob('bgwArrayQueue', ['a', 'b', 'c'])
		expect(jobId).toBe('1')
		expect(added).toHaveLength(1)
		expect(extractProducerLink(added[0]?.payload).cleanPayload).toEqual([
			'a',
			'b',
			'c'
		])
	})

	test('createJobs enqueues each item via addJobs', async () => {
		const { utils, added } = mockUtils()
		const { createJobs } = bindCreateJob({
			enqueue: utils,
			queues: testQueues
		})

		const ids = await createJobs('bgwTestQueue', [
			testInput('a'),
			testInput('b')
		])
		expect(ids).toEqual(['1', '2'])
		expect(added).toHaveLength(2)
	})

	test('honours queue-level maxAttempts', async () => {
		const { utils, added } = mockUtils()
		const { createJob } = bindCreateJob({
			enqueue: utils,
			queues: testQueues
		})

		await createJob('bgwTestQueue', testInput('attempts'))
		expect(
			(added[0]?.spec as { maxAttempts?: number } | undefined)?.maxAttempts
		).toBe(7)
	})

	test('wraps payloads consistently without tracing', () => {
		expect(injectTraceContext('hello', null)).toEqual({
			__bgw: 2,
			payload: 'hello'
		})
	})

	test('wraps a non-object payload in an envelope when a span is active', () => {
		const api: OtelApi = {
			trace: {
				getTracer() {
					return {
						startActiveSpan: async (_name, _opts, fn) =>
							fn({
								setAttribute() {},
								setAttributes() {},
								setStatus() {},
								recordException() {},
								end() {},
								spanContext() {
									return {
										traceId: '0af7651916cd43dd8448eb211c80319c',
										spanId: 'b7ad6b7169203331',
										traceFlags: 1
									}
								}
							})
					}
				},
				getActiveSpan() {
					return {
						setAttribute() {},
						setAttributes() {},
						setStatus() {},
						recordException() {},
						end() {},
						spanContext() {
							return {
								traceId: '0af7651916cd43dd8448eb211c80319c',
								spanId: 'b7ad6b7169203331',
								traceFlags: 1
							}
						}
					}
				}
			},
			isSpanContextValid: () => true,
			SpanStatusCode: { OK: 1, ERROR: 2 },
			SpanKind: { INTERNAL: 0, CONSUMER: 1, PRODUCER: 2 },
			TraceFlags: { SAMPLED: 1 }
		}

		const wrapped = injectTraceContext('hello', api)
		expect(wrapped).toMatchObject({
			[BGW_ENVELOPE_KEY]: 2,
			payload: 'hello'
		})
	})
})

describe('createJob - batch jobKeyMode guard', () => {
	test('throws when jobKeyMode is set on batch input', async () => {
		const { utils } = mockUtils()
		const { createJobs } = bindCreateJob({
			enqueue: utils,
			queues: testQueues
		})

		await expect(
			// @ts-expect-error Runtime defense for callers bypassing TypeScript
			createJobs('bgwTestQueue', [testInput('batch-key-2')], {
				jobKeyMode: 'replace'
			})
		).rejects.toThrow(/jobKeyMode/)
	})
})

describe('compile-time type guards', () => {
	test('invalid input shape causes type error', () => {
		type OnboardingInput = QueueInput<'bgwTestQueue', TestQueues>

		// @ts-expect-error missing required field 'websiteUrl'
		const _invalid: OnboardingInput = {
			organizationId: 'org_test_invalid'
		}
		void _invalid
	})
})

integrationDescribe('createJob - single job', () => {
	test('creates a job and returns job ID', async () => {
		const jobId = await worker.createJob(TEST_QUEUE_NAME, testInput('single'))
		expect(jobId).not.toBeNull()
		expect(typeof jobId).toBe('string')
		expect(jobId).toMatch(/^\d+$/)
	})

	test('creates job with optional payload field', async () => {
		const futureDate = new Date(Date.now() + 3600000)
		const input = testInput('payload')
		const jobId = await worker.createJob(TEST_QUEUE_NAME, input, {
			runAt: futureDate
		})
		expect(jobId).not.toBeNull()

		const utils = await worker.getWorkerUtils()
		const jobs = await utils.withPgClient(async (client) => {
			const result = await client.query<{ payload: unknown }>(
				`SELECT payload FROM ${DEFAULT_GRAPHILE_WORKER_SCHEMA}._private_jobs WHERE id = $1`,
				[jobId]
			)
			return result.rows
		})

		expect(jobs.length).toBe(1)
		const payload = extractProducerLink(jobs[0]!.payload)
			.cleanPayload as Record<string, unknown>
		expect(payload.organizationId).toBe(input.organizationId)
		expect(payload.websiteUrl).toBe(input.websiteUrl)
	})
})

integrationDescribe('createJobs - batch jobs', () => {
	test('creates multiple jobs and returns array of IDs', async () => {
		const jobIds = await worker.createJobs(TEST_QUEUE_NAME, [
			testInput('batch-1'),
			testInput('batch-2'),
			testInput('batch-3')
		])
		expect(Array.isArray(jobIds)).toBe(true)
		expect(jobIds).toHaveLength(3)
		for (const id of jobIds) {
			expect(typeof id).toBe('string')
			expect(id).toMatch(/^\d+$/)
		}
	})

	test('empty batch returns empty array', async () => {
		const jobIds = await worker.createJobs(TEST_QUEUE_NAME, [])
		expect(Array.isArray(jobIds)).toBe(true)
		expect(jobIds).toHaveLength(0)
	})
})

integrationDescribe('createJob - with options', () => {
	test('creates job with priority', async () => {
		const jobId = await worker.createJob(
			TEST_QUEUE_NAME,
			testInput('priority'),
			{ priority: 10 }
		)
		const utils = await worker.getWorkerUtils()
		const jobs = await utils.withPgClient(async (client) => {
			const result = await client.query<{ priority: number }>(
				`SELECT priority FROM ${DEFAULT_GRAPHILE_WORKER_SCHEMA}._private_jobs WHERE id = $1`,
				[jobId]
			)
			return result.rows
		})
		expect(jobs.length).toBe(1)
		expect(jobs[0]!.priority).toBe(10)
	})

	test('uses queue maxAttempts when omitted', async () => {
		const jobId = await worker.createJob(
			TEST_QUEUE_NAME,
			testInput('default-attempts')
		)
		const utils = await worker.getWorkerUtils()
		const jobs = await utils.withPgClient(async (client) => {
			const result = await client.query<{ max_attempts: number }>(
				`SELECT max_attempts FROM ${DEFAULT_GRAPHILE_WORKER_SCHEMA}._private_jobs WHERE id = $1`,
				[jobId]
			)
			return result.rows
		})
		expect(jobs.length).toBe(1)
		expect(jobs[0]!.max_attempts).toBe(7)
	})

	test('creates job with jobKey for deduplication', async () => {
		const uniqueKey = `test-key-${Date.now()}`
		const futureDate = new Date(Date.now() + 3600000)

		await worker.createJob(TEST_QUEUE_NAME, testInput('dedup-first'), {
			jobKey: uniqueKey,
			jobKeyMode: 'replace',
			runAt: futureDate
		})
		await worker.createJob(TEST_QUEUE_NAME, testInput('dedup-second'), {
			jobKey: uniqueKey,
			jobKeyMode: 'replace',
			runAt: futureDate
		})

		const utils = await worker.getWorkerUtils()
		const jobs = await utils.withPgClient(async (client) => {
			const result = await client.query<{ payload: unknown }>(
				`SELECT payload FROM ${DEFAULT_GRAPHILE_WORKER_SCHEMA}._private_jobs WHERE key = $1`,
				[uniqueKey]
			)
			return result.rows
		})
		expect(jobs.length).toBe(1)
		const payload = extractProducerLink(jobs[0]!.payload)
			.cleanPayload as Record<string, unknown>
		expect(payload.organizationId).toBe('org_test_dedup-second')
	})
})

integrationDescribe('step cache', () => {
	const counts = { fetch: 0, send: 0 }
	const stepQueue = defineQueue({
		name: 'bgwStepQueue',
		inputSchema: z.object({ userId: z.string() }),
		maxAttempts: 3
	})
	const stepQueueHandler: QueueHandlers<
		readonly [typeof stepQueue]
	>['bgwStepQueue'] = async (payload, ctx) => {
		await ctx.step.run('fetch-user', async () => {
			counts.fetch += 1
			return { id: payload.userId }
		})
		await ctx.step.run('send-email', async () => {
			counts.send += 1
			throw new Error('smtp down')
		})
	}

	const stepWorker = createBetterWorker({
		pgPool: pool,
		queues: [stepQueue],
		handlers: { bgwStepQueue: stepQueueHandler },
		hooks: { createLogger: () => silentLogger }
	})

	beforeAll(async () => {
		await stepWorker.migrate()
	})

	afterAll(async () => {
		await stepWorker.stop()
	})

	test('persists completed steps and skips them on retry', async () => {
		counts.fetch = 0
		counts.send = 0

		const jobId = await stepWorker.createJob('bgwStepQueue', { userId: 'u1' })
		expect(jobId).toBeTruthy()

		await stepWorker.runOnce()

		const utils = await stepWorker.getWorkerUtils()
		const stored = await utils.withPgClient(async (client) => {
			const result = await client.query<{ payload: unknown }>(
				`SELECT payload FROM ${DEFAULT_GRAPHILE_WORKER_SCHEMA}._private_jobs WHERE id = $1`,
				[jobId]
			)
			return result.rows[0]?.payload
		})

		expect(stored).toMatchObject({
			[BGW_ENVELOPE_KEY]: 2,
			payload: { userId: 'u1' },
			steps: { 'fetch-user': { output: { id: 'u1' } } }
		})

		const listed = await stepWorker.listJobs({
			queue: 'bgwStepQueue',
			includePayload: true
		})
		expect(listed[0]?.payload).toEqual({ userId: 'u1' })

		await utils.rescheduleJobs([jobId!], { runAt: new Date() })
		await stepWorker.runOnce()

		expect(counts.fetch).toBe(1)
		expect(counts.send).toBe(2)
	})
})
