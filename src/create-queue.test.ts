import { describe, test, expect, expectTypeOf } from 'bun:test'
import { z } from 'zod'
import {
	createQueue,
	isCronInitQueue,
	isRegularQueue,
	isCronQueue,
	CRON_INIT_SUFFIX,
	type RegularQueueConfig,
	type CronQueueConfig,
	type CronInitQueueConfig,
	type JobContext
} from './create-queue'
import type { InferInput, InferPayload, QueueName, QueueInput } from './index'

// ═══════════════════════════════════════════════════════════════════════════
// Test Queue Definitions
// ═══════════════════════════════════════════════════════════════════════════

// Regular queue with input schema
const regularQueue = createQueue({
	name: 'testRegular',
	inputSchema: z.object({ userId: z.string(), count: z.number() }),
	maxAttempts: 5,
	serial: true,
	processFn: async (payload, ctx) => {
		ctx.logger.info('Processing', { userId: payload.userId })
	}
})

// Cron queue (no input schema)
const cronQueue = createQueue({
	name: 'testCron',
	cron: '0 * * * *',
	maxAttempts: 3,
	processFn: async (_payload, ctx) => {
		ctx.logger.info('Running cron job')
	}
})

// Cron init queue (with initFn and processFn)
const cronInitQueue = createQueue({
	name: 'testCronInit',
	cron: '0 * * * *',
	inputSchema: z.object({ orderId: z.string(), status: z.string() }),
	maxAttempts: 3,
	initFn: async (ctx) => {
		ctx.logger.info('Gathering items')
		return [
			{ orderId: '1', status: 'pending' },
			{ orderId: '2', status: 'processing' }
		]
	},
	processFn: async (payload, ctx) => {
		ctx.logger.info('Processing order', { orderId: payload.orderId })
	}
})

// Test queues array for type helper tests
const testQueues = [regularQueue, cronQueue, cronInitQueue] as const

// ═══════════════════════════════════════════════════════════════════════════
// Type Tests - createQueue Return Types
// ═══════════════════════════════════════════════════════════════════════════

describe('createQueue return types', () => {
	test('regular queue returns RegularQueueConfig', () => {
		expectTypeOf(regularQueue).toMatchTypeOf<
			RegularQueueConfig<
				z.ZodObject<{ userId: z.ZodString; count: z.ZodNumber }>
			>
		>()
	})

	test('cron queue returns CronQueueConfig', () => {
		expectTypeOf(cronQueue).toMatchTypeOf<CronQueueConfig>()
	})

	test('cron init queue returns CronInitQueueConfig', () => {
		expectTypeOf(cronInitQueue).toMatchTypeOf<
			CronInitQueueConfig<
				z.ZodObject<{ orderId: z.ZodString; status: z.ZodString }>
			>
		>()
	})

	test('regular queue has correct name literal type', () => {
		expectTypeOf(regularQueue.name).toEqualTypeOf<'testRegular'>()
	})

	test('cron queue has correct name literal type', () => {
		expectTypeOf(cronQueue.name).toEqualTypeOf<'testCron'>()
	})

	test('cron init queue has correct name literal type', () => {
		expectTypeOf(cronInitQueue.name).toEqualTypeOf<'testCronInit'>()
	})
})

// ═══════════════════════════════════════════════════════════════════════════
// Type Tests - Input Schema Inference
// ═══════════════════════════════════════════════════════════════════════════

describe('input schema inference', () => {
	test('regular queue processFn receives correctly typed payload', () => {
		type ProcessFnPayload = Parameters<typeof regularQueue.processFn>[0]
		expectTypeOf<ProcessFnPayload>().toEqualTypeOf<{
			userId: string
			count: number
		}>()
	})

	test('cron queue processFn receives undefined payload', () => {
		type ProcessFnPayload = Parameters<typeof cronQueue.processFn>[0]
		expectTypeOf<ProcessFnPayload>().toEqualTypeOf<undefined>()
	})

	test('cron init queue initFn returns correctly typed array', () => {
		type InitFnReturn = Awaited<ReturnType<typeof cronInitQueue.initFn>>
		expectTypeOf<InitFnReturn>().toEqualTypeOf<
			Array<{ orderId: string; status: string }>
		>()
	})

	test('cron init queue processFn receives correctly typed payload', () => {
		type ProcessFnPayload = Parameters<typeof cronInitQueue.processFn>[0]
		expectTypeOf<ProcessFnPayload>().toEqualTypeOf<{
			orderId: string
			status: string
		}>()
	})

	test('all processFn receive JobContext as second parameter', () => {
		type RegularCtx = Parameters<typeof regularQueue.processFn>[1]
		type CronCtx = Parameters<typeof cronQueue.processFn>[1]
		type CronInitCtx = Parameters<typeof cronInitQueue.processFn>[1]

		expectTypeOf<RegularCtx>().toEqualTypeOf<JobContext>()
		expectTypeOf<CronCtx>().toEqualTypeOf<JobContext>()
		expectTypeOf<CronInitCtx>().toEqualTypeOf<JobContext>()
	})

	test('cron init queue initFn receives JobContext', () => {
		type InitFnCtx = Parameters<typeof cronInitQueue.initFn>[0]
		expectTypeOf<InitFnCtx>().toEqualTypeOf<JobContext>()
	})

	test('JobContext includes step.run', () => {
		expectTypeOf<JobContext['step']['run']>().toBeFunction()
	})
})

// ═══════════════════════════════════════════════════════════════════════════
// Type Tests - QueueName and QueueInput Helpers
// ═══════════════════════════════════════════════════════════════════════════

describe('QueueName and QueueInput type helpers', () => {
	type TestQueueName = QueueName<typeof testQueues>
	type RegularInput = QueueInput<'testRegular', typeof testQueues>
	type CronInitInput = QueueInput<'testCronInit', typeof testQueues>
	type CronInput = QueueInput<'testCron', typeof testQueues>

	test('QueueName is union of all queue names', () => {
		expectTypeOf<TestQueueName>().toEqualTypeOf<
			'testRegular' | 'testCron' | 'testCronInit'
		>()
	})

	test('QueueInput extracts correct payload type for regular queue', () => {
		expectTypeOf<RegularInput>().toEqualTypeOf<{
			userId: string
			count: number
		}>()
	})

	test('QueueInput extracts correct payload type for cron init queue', () => {
		expectTypeOf<CronInitInput>().toEqualTypeOf<{
			orderId: string
			status: string
		}>()
	})

	test('QueueInput is undefined for cron queue (no input)', () => {
		expectTypeOf<CronInput>().toEqualTypeOf<undefined>()
	})

	test('InferInput uses z.input so defaults are optional', () => {
		const withDefault = createQueue({
			name: 'withDefault',
			inputSchema: z.object({
				to: z.string(),
				retries: z.number().default(3)
			}),
			processFn: async () => {}
		})
		expectTypeOf<InferInput<typeof withDefault>>().toEqualTypeOf<{
			to: string
			retries?: number | undefined
		}>()
		expectTypeOf<InferPayload<typeof withDefault>>().toEqualTypeOf<{
			to: string
			retries: number
		}>()
	})
})

// ═══════════════════════════════════════════════════════════════════════════
// Type Tests - Compile-Time Guards (Negative Tests)
// ═══════════════════════════════════════════════════════════════════════════

describe('compile-time type guards', () => {
	test('regular queue processFn payload must match inputSchema', () => {
		createQueue({
			name: 'typeGuardTest1',
			inputSchema: z.object({ id: z.string() }),
			processFn: async (payload, _ctx) => {
				// payload.id should be string
				const _id: string = payload.id
				void _id
			}
		})
	})

	test('cron init queue processFn payload must match inputSchema', () => {
		createQueue({
			name: 'typeGuardTest2',
			cron: '* * * * *',
			inputSchema: z.object({ value: z.number() }),
			initFn: async (_ctx) => [{ value: 42 }],
			processFn: async (payload, _ctx) => {
				// payload.value should be number
				const _val: number = payload.value
				void _val
			}
		})
	})

	test('cron init queue initFn return type must match inputSchema', () => {
		createQueue({
			name: 'typeGuardTest3',
			cron: '* * * * *',
			inputSchema: z.object({ name: z.string(), active: z.boolean() }),
			initFn: async (_ctx) => {
				// Must return array matching schema
				return [{ name: 'test', active: true }]
			},
			processFn: async (payload, _ctx) => {
				const _name: string = payload.name
				const _active: boolean = payload.active
				void _name
				void _active
			}
		})
	})

	test('cron + inputSchema requires initFn (cron-init queue)', () => {
		// When you have both cron AND inputSchema, you need initFn to make it a valid cron-init queue
		// This is enforced by the type system - the config matches CronInitQueueConfig which requires initFn
		// The error manifests on processFn because without initFn the overload doesn't match
		const _queue = createQueue({
			name: 'cronInitNeedsInitFn',
			inputSchema: z.object({ id: z.string() }),
			cron: '* * * * *',
			initFn: async (_ctx) => [{ id: 'test' }], // Required for cron + inputSchema
			processFn: async (_payload, _ctx) => {}
		})
		// This compiles because we have initFn
		expect(_queue.initFn).toBeDefined()
	})

	test('cron queue with initFn requires inputSchema', () => {
		// initFn requires inputSchema to be present (to validate the items returned)
		// This would fail at runtime if initFn returns items that can't be validated
		// Valid cron-init queue:
		const _validQueue = createQueue({
			name: 'validCronInit',
			cron: '* * * * *',
			inputSchema: z.object({ id: z.string() }),
			initFn: async (_ctx) => [{ id: 'test' }],
			processFn: async (_payload, _ctx) => {}
		})
		expect(_validQueue.initFn).toBeDefined()
		expect(_validQueue.inputSchema).toBeDefined()
	})
})

// ═══════════════════════════════════════════════════════════════════════════
// Functional Tests - Type Guards
// ═══════════════════════════════════════════════════════════════════════════

describe('isCronInitQueue type guard', () => {
	test('returns true for cron init queue', () => {
		expect(isCronInitQueue(cronInitQueue)).toBe(true)
	})

	test('returns false for regular queue', () => {
		expect(isCronInitQueue(regularQueue)).toBe(false)
	})

	test('returns false for cron queue', () => {
		expect(isCronInitQueue(cronQueue)).toBe(false)
	})
})

describe('isRegularQueue type guard', () => {
	test('returns true for regular queue', () => {
		expect(isRegularQueue(regularQueue)).toBe(true)
	})

	test('returns false for cron queue', () => {
		expect(isRegularQueue(cronQueue)).toBe(false)
	})

	test('returns false for cron init queue', () => {
		expect(isRegularQueue(cronInitQueue)).toBe(false)
	})
})

describe('isCronQueue type guard', () => {
	test('returns true for cron queue', () => {
		expect(isCronQueue(cronQueue)).toBe(true)
	})

	test('returns false for regular queue', () => {
		expect(isCronQueue(regularQueue)).toBe(false)
	})

	test('returns false for cron init queue', () => {
		// Cron init queue has cron but also has initFn, so it's not a "simple" cron queue
		expect(isCronQueue(cronInitQueue)).toBe(false)
	})
})

// ═══════════════════════════════════════════════════════════════════════════
// Functional Tests - Queue Configuration
// ═══════════════════════════════════════════════════════════════════════════

describe('queue configuration properties', () => {
	test('regular queue has expected properties', () => {
		expect(regularQueue.name).toBe('testRegular')
		expect(regularQueue.inputSchema).toBeDefined()
		expect(regularQueue.processFn).toBeInstanceOf(Function)
		expect(regularQueue.maxAttempts).toBe(5)
		expect(regularQueue.serial).toBe(true)
		expect('cron' in regularQueue).toBe(false)
	})

	test('cron queue has expected properties', () => {
		expect(cronQueue.name).toBe('testCron')
		expect(cronQueue.cron).toBe('0 * * * *')
		expect(cronQueue.processFn).toBeInstanceOf(Function)
		expect(cronQueue.maxAttempts).toBe(3)
		expect('inputSchema' in cronQueue).toBe(false)
	})

	test('cron init queue has expected properties', () => {
		expect(cronInitQueue.name).toBe('testCronInit')
		expect(cronInitQueue.cron).toBe('0 * * * *')
		expect(cronInitQueue.inputSchema).toBeDefined()
		expect(cronInitQueue.initFn).toBeInstanceOf(Function)
		expect(cronInitQueue.processFn).toBeInstanceOf(Function)
		expect(cronInitQueue.maxAttempts).toBe(3)
	})

	test('optional fields are preserved when provided', () => {
		const queueWithOptions = createQueue({
			name: 'optionsTest',
			inputSchema: z.object({ id: z.string() }),
			serial: 'email',
			maxAttempts: 10,
			processFn: async (_payload, _ctx) => {}
		})

		expect(queueWithOptions.serial).toBe('email')
		expect(queueWithOptions.maxAttempts).toBe(10)
	})

	test('optional fields are undefined when not provided', () => {
		const queueWithoutOptions = createQueue({
			name: 'noOptionsTest',
			inputSchema: z.object({ id: z.string() }),
			processFn: async (_payload, _ctx) => {}
		})

		expect(queueWithoutOptions.serial).toBeUndefined()
		expect(queueWithoutOptions.maxAttempts).toBeUndefined()
	})
})

// ═══════════════════════════════════════════════════════════════════════════
// Functional Tests - CRON_INIT_SUFFIX
// ═══════════════════════════════════════════════════════════════════════════

describe('CRON_INIT_SUFFIX constant', () => {
	test('equals "_cron-init"', () => {
		expect(CRON_INIT_SUFFIX).toBe('_cron-init')
	})

	test('can be used to derive cron init task name', () => {
		const processingTaskName = 'syncOrders'
		const cronInitTaskName = `${processingTaskName}${CRON_INIT_SUFFIX}`

		expect(cronInitTaskName).toBe('syncOrders_cron-init')
	})

	test('can be used to derive processing task name from cron init name', () => {
		const cronInitTaskName = 'syncOrders_cron-init'
		const processingTaskName = cronInitTaskName.replace(CRON_INIT_SUFFIX, '')

		expect(processingTaskName).toBe('syncOrders')
	})
})

// ═══════════════════════════════════════════════════════════════════════════
// Functional Tests - Schema Validation
// ═══════════════════════════════════════════════════════════════════════════

describe('input schema validation', () => {
	test('regular queue inputSchema parses valid data', () => {
		const validData = { userId: 'user-123', count: 42 }
		const result = regularQueue.inputSchema.parse(validData)

		expect(result).toEqual(validData)
	})

	test('regular queue inputSchema rejects invalid data', () => {
		const invalidData = { userId: 123, count: 'not-a-number' }

		expect(() => regularQueue.inputSchema.parse(invalidData)).toThrow()
	})

	test('cron init queue inputSchema parses valid data', () => {
		const validData = { orderId: 'order-1', status: 'pending' }
		const result = cronInitQueue.inputSchema.parse(validData)

		expect(result).toEqual(validData)
	})

	test('cron init queue inputSchema.array() validates array of items', () => {
		const validItems = [
			{ orderId: '1', status: 'pending' },
			{ orderId: '2', status: 'processing' }
		]
		const result = cronInitQueue.inputSchema.array().parse(validItems)

		expect(result).toEqual(validItems)
	})
})

// ═══════════════════════════════════════════════════════════════════════════
// Complex Schema Tests
// ═══════════════════════════════════════════════════════════════════════════

describe('complex schema handling', () => {
	test('nested object schemas work correctly', () => {
		const complexQueue = createQueue({
			name: 'complexSchema',
			inputSchema: z.object({
				user: z.object({
					id: z.string(),
					profile: z.object({
						name: z.string(),
						email: z.string().email()
					})
				}),
				metadata: z.record(z.string(), z.unknown())
			}),
			processFn: async (payload, _ctx) => {
				// TypeScript should know the nested types
				const _userId: string = payload.user.id
				const _userName: string = payload.user.profile.name
				void _userId
				void _userName
			}
		})

		const validData = {
			user: {
				id: 'u-1',
				profile: { name: 'John', email: 'john@example.com' }
			},
			metadata: { key: 'value' }
		}

		expect(complexQueue.inputSchema.parse(validData)).toEqual(validData)
	})

	test('optional fields in schemas', () => {
		const optionalFieldQueue = createQueue({
			name: 'optionalFields',
			inputSchema: z.object({
				required: z.string(),
				optional: z.number().optional()
			}),
			processFn: async (payload, _ctx) => {
				const _required: string = payload.required
				const _optional: number | undefined = payload.optional
				void _required
				void _optional
			}
		})

		// Without optional field
		expect(optionalFieldQueue.inputSchema.parse({ required: 'test' })).toEqual({
			required: 'test'
		})

		// With optional field
		expect(
			optionalFieldQueue.inputSchema.parse({ required: 'test', optional: 42 })
		).toEqual({ required: 'test', optional: 42 })
	})

	test('enum types in schemas', () => {
		const enumQueue = createQueue({
			name: 'enumTypes',
			inputSchema: z.object({
				status: z.enum(['pending', 'active', 'completed']),
				priority: z.enum({ LOW: 0, MEDIUM: 1, HIGH: 2 } as const)
			}),
			processFn: async (payload, _ctx) => {
				const _status: 'pending' | 'active' | 'completed' = payload.status
				const _priority: 0 | 1 | 2 = payload.priority
				void _status
				void _priority
			}
		})

		expect(
			enumQueue.inputSchema.parse({ status: 'active', priority: 1 })
		).toEqual({ status: 'active', priority: 1 })
	})
})

// ═══════════════════════════════════════════════════════════════════════════
// Cron Queue (without initFn) Tests
// ═══════════════════════════════════════════════════════════════════════════

describe('cron queue (without initFn)', () => {
	test('creates a simple cron queue with no input schema', () => {
		const simpleCronQueue = createQueue({
			name: 'simpleCron',
			cron: '*/5 * * * *', // every 5 minutes
			processFn: async (_payload, ctx) => {
				ctx.logger.info('Running scheduled task')
			}
		})

		expect(simpleCronQueue.name).toBe('simpleCron')
		expect(simpleCronQueue.cron).toBe('*/5 * * * *')
		expect(simpleCronQueue.processFn).toBeInstanceOf(Function)
		expect('inputSchema' in simpleCronQueue).toBe(false)
		expect('initFn' in simpleCronQueue).toBe(false)
	})

	test('cron queue processFn receives undefined payload', () => {
		const cronQueueTest = createQueue({
			name: 'cronPayloadTest',
			cron: '0 0 * * *', // daily at midnight
			processFn: async (payload, _ctx) => {
				// payload should be undefined for simple cron queues
				expectTypeOf(payload).toEqualTypeOf<undefined>()
				expect(payload).toBeUndefined()
			}
		})

		// Verify the type at definition level
		type PayloadType = Parameters<typeof cronQueueTest.processFn>[0]
		expectTypeOf<PayloadType>().toEqualTypeOf<undefined>()
	})

	test('cron queue with various cron expressions', () => {
		// Every minute
		const everyMinute = createQueue({
			name: 'everyMinute',
			cron: '* * * * *',
			processFn: async (_payload, _ctx) => {}
		})
		expect(everyMinute.cron).toBe('* * * * *')

		// Every hour at minute 0
		const hourly = createQueue({
			name: 'hourly',
			cron: '0 * * * *',
			processFn: async (_payload, _ctx) => {}
		})
		expect(hourly.cron).toBe('0 * * * *')

		// Every day at 3:30 AM
		const daily = createQueue({
			name: 'daily',
			cron: '30 3 * * *',
			processFn: async (_payload, _ctx) => {}
		})
		expect(daily.cron).toBe('30 3 * * *')

		// Every Monday at 9 AM
		const weekly = createQueue({
			name: 'weekly',
			cron: '0 9 * * 1',
			processFn: async (_payload, _ctx) => {}
		})
		expect(weekly.cron).toBe('0 9 * * 1')
	})

	test('cron queue with optional config options', () => {
		const cronWithOptions = createQueue({
			name: 'cronWithOptions',
			cron: '0 */2 * * *', // every 2 hours
			maxAttempts: 5,
			serial: true,
			processFn: async (_payload, _ctx) => {}
		})

		expect(cronWithOptions.maxAttempts).toBe(5)
		expect(cronWithOptions.serial).toBe(true)
	})

	test('isCronQueue returns true for simple cron queue', () => {
		const simpleCron = createQueue({
			name: 'isCronTest',
			cron: '0 0 * * *',
			processFn: async (_payload, _ctx) => {}
		})

		expect(isCronQueue(simpleCron)).toBe(true)
		expect(isRegularQueue(simpleCron)).toBe(false)
		expect(isCronInitQueue(simpleCron)).toBe(false)
	})

	test('cron queue is distinct from cron init queue', () => {
		// Simple cron - no initFn, no inputSchema
		const simpleCron = createQueue({
			name: 'simpleCronDistinct',
			cron: '0 0 * * *',
			processFn: async (_payload, _ctx) => {}
		})

		// Cron init - has initFn and inputSchema
		const cronInit = createQueue({
			name: 'cronInitDistinct',
			cron: '0 0 * * *',
			inputSchema: z.object({ id: z.string() }),
			initFn: async (_ctx) => [{ id: '1' }],
			processFn: async (_payload, _ctx) => {}
		})

		// They should be identified differently by type guards
		expect(isCronQueue(simpleCron)).toBe(true)
		expect(isCronQueue(cronInit)).toBe(false)

		expect(isCronInitQueue(simpleCron)).toBe(false)
		expect(isCronInitQueue(cronInit)).toBe(true)
	})
})
