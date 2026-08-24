import { describe, test, expect } from 'bun:test'
import { createNoopSpan, wrapOtelSpan } from './otel'

describe('JobSpan.addEvent', () => {
	test('noop span accepts addEvent', () => {
		const span = createNoopSpan()
		expect(() => span.addEvent('info', { message: 'ok' })).not.toThrow()
	})

	test('wrapOtelSpan forwards compacted attributes', () => {
		const events: { name: string; attributes?: Record<string, unknown> }[] = []
		const span = wrapOtelSpan({
			setAttribute() {},
			setAttributes() {},
			setStatus() {},
			addEvent(name, attributes) {
				events.push({ name, attributes })
			},
			recordException() {},
			end() {},
			spanContext: () => ({ traceId: 't', spanId: 's', traceFlags: 1 })
		})

		span.addEvent('info', { message: 'ok', skip: null, empty: undefined })
		expect(events).toEqual([{ name: 'info', attributes: { message: 'ok' } }])
	})
})
