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
import type { WorkerUtils } from 'graphile-worker'
import {
	TRACE_CONTEXT_KEY,
	BGW_ENVELOPE_KEY,
	injectTraceContext,
	bindCreateJob,
	type JobTraceContext
} from './create-job'
import { createQueue } from './create-queue'
import { createBetterWorker } from './create-better-worker'
import type { QueueName, QueueInput } from './types'
import { DEFAULT_GRAPHILE_WORKER_SCHEMA } from './client'
import type { JobLogger } from './hooks'
import { JobValidationError, UnknownQueueError } from './errors'
import { setOtelApi } from './otel'

const silentLogger: JobLogger = {
	debug() {},
	info() {},
	warn() {},
	error() {}
}

const testQueue = createQueue({
	name: 'bgwTestQueue',
	inputSchema: z.object({
		organizationId: z.string(),
		websiteUrl: z.string()
	}),
	maxAttempts: 7,
	processFn: async () => {}
})

const arrayQueue = createQueue({
	name: 'bgwArrayQueue',
	inputSchema: z.array(z.string()),
	processFn: async () => {}
})

const stringQueue = createQueue({
	name: 'bgwStringQueue',
	inputSchema: z.string(),
	processFn: async () => {}
})

const cronQueue = createQueue({
	name: 'bgwCronQueue',
	cron: '0 * * * *',
	processFn: async () => {}
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
	} as unknown as WorkerUtils

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

describe('TRACE_CONTEXT_KEY constant', () => {
	test('equals "__trace"', () => {
		expect(TRACE_CONTEXT_KEY).toBe('__trace')
	})

	test('is a string literal type', () => {
		expectTypeOf(TRACE_CONTEXT_KEY).toEqualTypeOf<'__trace'>()
	})
})

describe('JobTraceContext type', () => {
	test('has correct shape', () => {
		const ctx: JobTraceContext = {
			traceId: 'abc123',
			spanId: 'def456'
		}
		expectTypeOf(ctx).toEqualTypeOf<{ traceId: string; spanId: string }>()
	})
})

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
			getWorkerUtils: async () => utils,
			queues: testQueues
		})

		await expect(
			createJob('missing' as never, {} as never)
		).rejects.toBeInstanceOf(UnknownQueueError)
	})

	test('rejects invalid payloads at enqueue time', async () => {
		const { utils } = mockUtils()
		const { createJob } = bindCreateJob({
			getWorkerUtils: async () => utils,
			queues: testQueues
		})

		await expect(
			createJob('bgwTestQueue', { organizationId: 'x' } as never)
		).rejects.toBeInstanceOf(JobValidationError)
	})

	test('enqueues an array-schema payload as a single job', async () => {
		const { utils, added } = mockUtils()
		const { createJob } = bindCreateJob({
			getWorkerUtils: async () => utils,
			queues: testQueues
		})

		const jobId = await createJob('bgwArrayQueue', ['a', 'b', 'c'])
		expect(jobId).toBe('1')
		expect(added).toHaveLength(1)
		expect(added[0]?.payload).toEqual(['a', 'b', 'c'])
	})

	test('createJobs enqueues each item via addJobs', async () => {
		const { utils, added } = mockUtils()
		const { createJobs } = bindCreateJob({
			getWorkerUtils: async () => utils,
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
			getWorkerUtils: async () => utils,
			queues: testQueues
		})

		await createJob('bgwTestQueue', testInput('attempts'))
		expect(
			(added[0]?.spec as { maxAttempts?: number } | undefined)?.maxAttempts
		).toBe(7)
	})

	test('does not wrap a string payload unless a span is active', () => {
		setOtelApi(null)
		expect(injectTraceContext('hello')).toBe('hello')
	})

	test('wraps a non-object payload in an envelope when a span is active', () => {
		setOtelApi({
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
		})

		try {
			const wrapped = injectTraceContext('hello')
			expect(wrapped).toMatchObject({
				[BGW_ENVELOPE_KEY]: 1,
				payload: 'hello'
			})
		} finally {
			setOtelApi(null)
		}
	})
})

describe('createJob - batch jobKeyMode guard', () => {
	test('throws when jobKeyMode is set on batch input', async () => {
		const { utils } = mockUtils()
		const { createJobs } = bindCreateJob({
			getWorkerUtils: async () => utils,
			queues: testQueues
		})

		await expect(
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
		const payload = jobs[0]!.payload as Record<string, unknown>
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
		const payload = jobs[0]!.payload as Record<string, unknown>
		expect(payload.organizationId).toBe('org_test_dedup-second')
	})
})
