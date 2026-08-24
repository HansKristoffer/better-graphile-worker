import { describe, test, expect } from 'bun:test'
import type { JobHelpers } from 'graphile-worker'
import {
	createMemoryStepStore,
	createPgStepStore,
	createStepRunner,
	serializeStepOutput
} from './steps'
import {
	BGW_ENVELOPE_KEY,
	TRACE_CONTEXT_KEY,
	TRACEPARENT_KEY,
	extractCronMeta,
	extractProducerLink,
	extractStepCache,
	withStepCache
} from './payload'
import { createNoopSpan } from './otel'
import { NonRetriableError, StepSerializationError } from './errors'
import type { JobSpan } from './hooks'

function recordingSpan() {
	const events: Array<{ name: string; attributes?: Record<string, unknown> }> =
		[]
	const span: JobSpan = {
		...createNoopSpan(),
		addEvent(name, attributes) {
			events.push({ name, attributes })
		}
	}
	return { span, events }
}

describe('serializeStepOutput', () => {
	test('stores undefined as null', () => {
		expect(serializeStepOutput('void', undefined)).toBeNull()
	})

	test('round-trips JSON-safe values', () => {
		expect(serializeStepOutput('n', 1)).toBe(1)
		expect(serializeStepOutput('obj', { id: 'u1' })).toEqual({ id: 'u1' })
	})

	test('turns Date into an ISO string', () => {
		const date = new Date('2020-01-02T03:04:05.000Z')
		expect(serializeStepOutput('when', date)).toBe(date.toISOString())
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

	test('stores undefined results as null', async () => {
		const step = createStepRunner({
			store: createMemoryStepStore(),
			span: createNoopSpan()
		})
		expect(await step.run('noop', async () => undefined)).toBeNull()
		expect(await step.run('noop', async () => 'again')).toBeNull()
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

describe('payload step envelope', () => {
	test('wraps a flat payload and keeps _cron plus trace fields', () => {
		const wrapped = withStepCache(
			{
				userId: 'u1',
				_cron: { ts: '2020-01-01T00:00:00.000Z', backfilled: true },
				[TRACE_CONTEXT_KEY]: {
					traceId: '0af7651916cd43dd8448eb211c80319c',
					spanId: 'b7ad6b7169203331'
				},
				[TRACEPARENT_KEY]:
					'00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'
			},
			{ 'fetch-user': { output: { id: 'u1' } } }
		)

		expect(wrapped).toMatchObject({
			[BGW_ENVELOPE_KEY]: 1,
			payload: {
				userId: 'u1',
				_cron: { ts: '2020-01-01T00:00:00.000Z', backfilled: true }
			},
			steps: { 'fetch-user': { output: { id: 'u1' } } },
			[TRACE_CONTEXT_KEY]: {
				traceId: '0af7651916cd43dd8448eb211c80319c',
				spanId: 'b7ad6b7169203331'
			}
		})
		expect(extractCronMeta(wrapped)).toEqual({
			ts: new Date('2020-01-01T00:00:00.000Z'),
			backfilled: true
		})
		expect(extractProducerLink(wrapped).cleanPayload).toEqual({
			userId: 'u1',
			_cron: { ts: '2020-01-01T00:00:00.000Z', backfilled: true }
		})
		expect(extractStepCache(wrapped)).toEqual({
			'fetch-user': { output: { id: 'u1' } }
		})
	})

	test('extractCronMeta reads _cron from clean payloads', () => {
		expect(
			extractCronMeta({
				_cron: { ts: '2021-02-03T04:05:06.000Z' }
			})
		).toEqual({ ts: new Date('2021-02-03T04:05:06.000Z') })
	})

	test('wraps a non-object payload', () => {
		const wrapped = withStepCache('hello', { a: { output: 1 } })
		expect(wrapped.payload).toBe('hello')
		expect(extractProducerLink(wrapped).cleanPayload).toBe('hello')
	})
})

describe('createPgStepStore', () => {
	test('writes an envelope with steps onto the job payload', async () => {
		let updated: unknown
		const helpers = {
			withPgClient: async (
				fn: (client: {
					query: (sql: string, params: unknown[]) => Promise<unknown>
				}) => Promise<unknown>
			) =>
				fn({
					query: async (_sql, params) => {
						updated = JSON.parse(String(params[1]))
						return { rows: [] }
					}
				})
		} as unknown as JobHelpers

		const store = createPgStepStore({
			helpers,
			schema: 'graphile_worker',
			jobId: '1',
			rawPayload: {
				userId: 'u1',
				_cron: { ts: '2020-01-01T00:00:00.000Z' },
				[TRACE_CONTEXT_KEY]: { traceId: 'abc', spanId: 'def' }
			}
		})

		await store.set('fetch-user', { id: 'u1' })

		expect(updated).toMatchObject({
			[BGW_ENVELOPE_KEY]: 1,
			payload: {
				userId: 'u1',
				_cron: { ts: '2020-01-01T00:00:00.000Z' }
			},
			steps: { 'fetch-user': { output: { id: 'u1' } } }
		})
		expect(extractCronMeta(updated)).toEqual({
			ts: new Date('2020-01-01T00:00:00.000Z')
		})
		expect(store.get('fetch-user')).toEqual({ output: { id: 'u1' } })
	})
})
