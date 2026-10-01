import { describe, test, expect } from 'bun:test'
import type { JobHelpers } from 'graphile-worker'
import {
	createMemoryStepStore,
	createPgStepStore,
	createStepRunner,
	serializeStepOutput
} from './steps.js'
import {
	injectTraceContext,
	extractCronMeta,
	extractProducerLink,
	extractStepCache
} from './payload.js'
import { createNoopSpan } from './otel.js'
import { NonRetriableError, StepSerializationError } from './errors.js'
import type { JobSpan } from './hooks.js'

function recordingSpan() {
	const events: Array<{
		name: string
		attributes?: Record<string, unknown> | undefined
	}> = []
	const span: JobSpan = {
		...createNoopSpan(),
		addEvent(name, attributes) {
			events.push({ name, attributes })
		}
	}
	return { span, events }
}

describe('serializeStepOutput', () => {
	test('preserves void', () => {
		expect(serializeStepOutput('void', undefined)).toBeUndefined()
	})

	test('round-trips JSON-safe values', () => {
		expect(serializeStepOutput('n', 1)).toBe(1)
		expect(serializeStepOutput('obj', { id: 'u1' })).toEqual({ id: 'u1' })
	})

	test('rejects Date without an explicit codec', () => {
		const date = new Date('2020-01-02T03:04:05.000Z')
		expect(() => serializeStepOutput('when', date)).toThrow(
			StepSerializationError
		)
	})

	test('throws StepSerializationError for non-JSON values', () => {
		expect(() => serializeStepOutput('cycle', 1n)).toThrow(
			StepSerializationError
		)
	})
})

describe('createStepRunner', () => {
	test('runs fn on cache miss and returns the cached value on hit', async () => {
		const store = createMemoryStepStore()
		const { span, events } = recordingSpan()
		const step = createStepRunner({ store, span })
		let runs = 0

		const first = await step.run('fetch-user', async () => {
			runs += 1
			return { id: 'u1' }
		})
		const second = await step.run('fetch-user', async () => {
			runs += 1
			return { id: 'other' }
		})

		expect(first).toEqual({ id: 'u1' })
		expect(second).toEqual({ id: 'u1' })
		expect(runs).toBe(1)
		expect(events.map((event) => event.name)).toEqual([
			'step.completed',
			'step.cache_hit'
		])
	})

	test('restores void on fresh and cached execution', async () => {
		const step = createStepRunner({
			store: createMemoryStepStore(),
			span: createNoopSpan()
		})
		expect(await step.run('noop', async () => undefined)).toBeUndefined()
		expect(await step.run('noop', async () => undefined)).toBeUndefined()
	})

	test('rejects an empty step id', async () => {
		const step = createStepRunner({
			store: createMemoryStepStore(),
			span: createNoopSpan()
		})
		await expect(step.run('', async () => 1)).rejects.toBeInstanceOf(
			NonRetriableError
		)
	})

	test('records step.failed and rethrows', async () => {
		const { span, events } = recordingSpan()
		const step = createStepRunner({
			store: createMemoryStepStore(),
			span
		})
		await expect(
			step.run('boom', async () => {
				throw new Error('smtp down')
			})
		).rejects.toThrow('smtp down')
		expect(events).toEqual([
			{ name: 'step.failed', attributes: { 'step.id': 'boom' } }
		])
	})
})

describe('payload metadata', () => {
	test('keeps cron metadata and checkpoints outside the business payload', () => {
		const wrapped = {
			...injectTraceContext({ userId: 'u1', _cron: { ts: 'business' } }, null),
			_cron: { ts: '2020-01-01T00:00:00.000Z', backfilled: true },
			steps: { 'fetch-user': { output: { id: 'u1' } } }
		}
		expect(extractCronMeta(wrapped)).toEqual({
			ts: new Date('2020-01-01T00:00:00.000Z'),
			backfilled: true
		})
		expect(extractProducerLink(wrapped).cleanPayload).toEqual({
			userId: 'u1',
			_cron: { ts: 'business' }
		})
		expect(extractStepCache(wrapped)).toEqual({
			'fetch-user': { output: { id: 'u1' } }
		})
	})
	test('does not interpret business _cron fields as scheduler metadata', () => {
		expect(
			extractCronMeta(
				injectTraceContext({ _cron: { ts: '2021-02-03T04:05:06.000Z' } }, null)
			)
		).toBeUndefined()
	})
})

describe('createPgStepStore', () => {
	test('sends only the new checkpoint and verifies the lock owner', async () => {
		let params: unknown[] = []
		const helpers = {
			job: { locked_by: 'worker-1' },
			withPgClient: async (fn: (client: unknown) => Promise<unknown>) =>
				fn({
					query: async (_sql: string, values: unknown[]) => {
						params = values
						return { rows: [], rowCount: 1 }
					}
				})
		} as unknown as JobHelpers
		const store = createPgStepStore({
			helpers,
			schema: 'graphile_worker',
			jobId: '1',
			rawPayload: injectTraceContext({ userId: 'u1' }, null)
		})
		await store.set('fetch-user', { id: 'u1' })
		expect(params).toEqual([
			'1',
			'fetch-user',
			JSON.stringify({ output: { id: 'u1' } }),
			'worker-1'
		])
		expect(store.get('fetch-user')).toEqual({ output: { id: 'u1' } })
	})
	test('rejects old payloads instead of converting them during a checkpoint', () => {
		expect(() =>
			createPgStepStore({
				helpers: {} as JobHelpers,
				schema: 'graphile_worker',
				jobId: '1',
				rawPayload: { userId: 'u1' }
			})
		).toThrow(NonRetriableError)
	})
})
