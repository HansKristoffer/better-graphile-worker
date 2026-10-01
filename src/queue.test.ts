import { normalizeWorkerQueues } from './registry.js'
import { describe, test, expect, expectTypeOf } from 'bun:test'
import { z } from 'zod'
import {
	defineQueue,
	isCronInitQueue,
	isRegularQueue,
	isCronQueue,
	hasInputSchema,
	getQueueType,
	formatCronSchedule,
	resolveSerialQueueName,
	type QueueHandlers,
	type QueueContract,
	type RegularQueueContract,
	type CronQueueContract,
	type CronInitQueueContract
} from './queue.js'
import { defineQueues } from './define-queues.js'
import { createTestHarness } from './testing.js'
import type {
	InferInput,
	InferPayload,
	QueueInput,
	QueuePayload
} from './types.js'

const regular = defineQueue({
	name: 'regular',
	inputSchema: z.string().transform((value) => value.length)
})
const cron = defineQueue({ name: 'cron', cron: '0 * * * *' })
const init = defineQueue({
	name: 'init',
	cron: '0 * * * *',
	inputSchema: z.object({ id: z.string() })
})
const contracts = defineQueues([regular, cron, init])

describe('queue contracts', () => {
	test('preserves names, schemas and contract variants', () => {
		expectTypeOf(regular.name).toEqualTypeOf<'regular'>()
		expectTypeOf(cron.name).toEqualTypeOf<'cron'>()
		expectTypeOf(init.name).toEqualTypeOf<'init'>()
		expectTypeOf(regular).toMatchTypeOf<
			RegularQueueContract<typeof regular.inputSchema, 'regular'>
		>()
		expectTypeOf(cron).toMatchTypeOf<CronQueueContract<'cron'>>()
		expectTypeOf(init).toMatchTypeOf<
			CronInitQueueContract<typeof init.inputSchema, 'init'>
		>()
		expectTypeOf<InferInput<typeof regular>>().toEqualTypeOf<string>()
		expectTypeOf<InferPayload<typeof regular>>().toEqualTypeOf<number>()
		expectTypeOf<QueueInput<'init', typeof contracts>>().toEqualTypeOf<{
			id: string
		}>()
		expectTypeOf<
			QueuePayload<'cron', typeof contracts>
		>().toEqualTypeOf<undefined>()
		expect(regular.inputSchema.parse('hello')).toBe(5)
	})
	test('describes handler-free cron-init contracts', () => {
		expect(getQueueType(regular)).toBe('regular')
		expect(getQueueType(cron)).toBe('cron')
		expect(getQueueType(init)).toBe('cron-init')
		expect(isCronInitQueue(init)).toBe(true)
		expect('initFn' in init).toBe(false)
	})
	test('retains enqueue defaults and readonly flags', () => {
		const contract = defineQueue({
			name: 'defaults',
			inputSchema: z.string(),
			maxAttempts: 7,
			priority: -2,
			serial: true,
			flags: ['one'] as const
		})
		expect(contract).toMatchObject({
			maxAttempts: 7,
			priority: -2,
			serial: true,
			flags: ['one']
		})
	})
	test('supports optional input, defaults and typed handler output', async () => {
		const defaulted = defineQueue({
			name: 'defaulted',
			inputSchema: z.object({
				id: z.string().default('fallback'),
				count: z.number().optional()
			})
		})
		let result: unknown
		const harness = createTestHarness([defaulted], {
			defaulted: (payload) => {
				result = payload
			}
		})
		await harness.process('defaulted', {})
		expect(result).toEqual({ id: 'fallback' })
	})
	test('supports enum and nested object inputs', async () => {
		const nested = defineQueue({
			name: 'nested',
			inputSchema: z.object({
				kind: z.enum(['a', 'b']),
				items: z.array(z.object({ id: z.number() }))
			})
		})
		const harness = createTestHarness([nested], {
			nested: (payload) => {
				expectTypeOf(payload.kind).toEqualTypeOf<'a' | 'b'>()
			}
		})
		await harness.process('nested', { kind: 'a', items: [{ id: 1 }] })
		await expect(
			harness.process('nested', {
				kind: 'a',
				items: [{ id: 'invalid' } as unknown as { id: number }]
			})
		).rejects.toThrow()
	})
})

describe('contract guards', () => {
	test.each([
		[regular, true, false, false, true],
		[cron, false, true, false, false],
		[init, false, false, true, true]
	] as const)(
		'classifies %p',
		(contract, regularExpected, cronExpected, initExpected, schemaExpected) => {
			expect(isRegularQueue(contract)).toBe(regularExpected)
			expect(isCronQueue(contract)).toBe(cronExpected)
			expect(isCronInitQueue(contract)).toBe(initExpected)
			expect(hasInputSchema(contract)).toBe(schemaExpected)
		}
	)
	test('preserves the exact variants and transformed schema during narrowing', () => {
		for (const contract of contracts) {
			if (isRegularQueue(contract)) {
				expectTypeOf(contract).toEqualTypeOf<typeof regular>()
				expect(contract.inputSchema.parse('hello')).toBe(5)
			} else if (isCronQueue(contract)) {
				expectTypeOf(contract).toEqualTypeOf<typeof cron>()
			} else if (isCronInitQueue(contract)) {
				expectTypeOf(contract).toEqualTypeOf<typeof init>()
			}
		}
	})
	test('narrows erased contracts without claiming handler functions', () => {
		const erased: QueueContract = {
			name: 'dynamic',
			cron: '* * * * *',
			inputSchema: z.string()
		}
		if (isCronInitQueue(erased)) {
			expect(erased.inputSchema.parse('text')).toBe('text')
			// @ts-expect-error A contract guard does not invent an initializer.
			void erased.initFn
		}
	})
	test('does not classify a present empty schedule as regular', () => {
		expect(
			isRegularQueue({ name: 'empty', cron: '', inputSchema: z.string() })
		).toBe(false)
	})
})

describe('typed handlers', () => {
	test('synchronous handlers infer payload and registry context', async () => {
		let length = 0
		const handlers: QueueHandlers<typeof contracts> = {
			regular: (payload, ctx) => {
				expectTypeOf(payload).toEqualTypeOf<number>()
				expectTypeOf(ctx.queue).toEqualTypeOf<'regular'>()
				length = payload
			},
			cron: (payload) => {
				expect(payload).toBeUndefined()
			},
			init: { initFn: () => [{ id: '1' }] as const, processFn: () => {} }
		}
		const harness = createTestHarness(contracts, handlers)
		await harness.process('regular', 'hello')
		await harness.process('cron', undefined)
		expect(length).toBe(5)
		expect(await harness.init('init')).toEqual([{ id: '1' }])
	})
	test('accepts asynchronous readonly initializers with producer input', async () => {
		const gather = defineQueue({
			name: 'gather',
			cron: '* * * * *',
			inputSchema: z.string().transform((value) => value.length)
		})
		const handlers: QueueHandlers<readonly [typeof gather]> = {
			gather: {
				initFn: async () => ['hello'] as const,
				processFn: (length) => {
					expectTypeOf(length).toEqualTypeOf<number>()
				}
			}
		}
		expect(await createTestHarness([gather], handlers).init('gather')).toEqual([
			'hello'
		])
	})
})

describe('contract display helpers', () => {
	test('formats all supported schedule variants', () => {
		expect(formatCronSchedule(undefined)).toBeNull()
		expect(formatCronSchedule('0 * * * *')).toBe('0 * * * *')
		expect(formatCronSchedule(['0 * * * *', '* * * * *'])).toBe(
			'0 * * * *, * * * * *'
		)
		expect(formatCronSchedule(() => true)).toBe('[function]')
	})
	test('resolves serial queue names', () => {
		expect(resolveSerialQueueName(true, 'queue')).toBe('queue')
		expect(resolveSerialQueueName('shared', 'queue')).toBe('shared')
		expect(resolveSerialQueueName(false, 'queue')).toBeUndefined()
	})
})

test('required handlers cannot come from Object.prototype', () => {
	const queues = [
		defineQueue({ name: 'toString', inputSchema: z.string() })
	] as const
	expect(() =>
		normalizeWorkerQueues(queues, {} as QueueHandlers<typeof queues>)
	).toThrow('requires a processFn')
	expect(normalizeWorkerQueues(queues, { toString: () => {} })).toHaveLength(1)
})
