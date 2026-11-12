import { describe, test, expect, expectTypeOf } from 'bun:test'
import { z } from 'zod'
import { defineQueue } from './queue.js'
import { assertUniqueQueueNames, defineQueues } from './define-queues.js'
import { DuplicateQueueError, QueueNameCollisionError } from './errors.js'
import { manyQueues } from '../tests/fixtures/many-queues.js'

describe('defineQueues', () => {
	test('returns the same tuple and preserves name literals', () => {
		const sendEmail = defineQueue({
			name: 'sendEmail',
			inputSchema: z.object({ to: z.string() })
		})
		const sweep = defineQueue({
			name: 'sweep',
			cron: '0 * * * *'
		})
		const queues = defineQueues([sendEmail, sweep])
		expect(queues).toHaveLength(2)
		expectTypeOf(queues[0].name).toEqualTypeOf<'sendEmail'>()
		expectTypeOf(queues[1].name).toEqualTypeOf<'sweep'>()
	})

	test('rejects duplicate names', () => {
		const a = defineQueue({
			name: 'dup',
			inputSchema: z.object({ id: z.string() })
		})
		const b = defineQueue({
			name: 'dup',
			inputSchema: z.object({ id: z.string() })
		})
		expect(() => assertUniqueQueueNames([a, b])).toThrow(DuplicateQueueError)
	})

	test('rejects cron-init task name collisions', () => {
		const processor = defineQueue({
			name: 'sync_cron-init',
			inputSchema: z.object({ id: z.string() })
		})
		const cronInit = defineQueue({
			name: 'sync',
			cron: '0 * * * *',
			inputSchema: z.object({ id: z.string() })
		})
		expect(() => assertUniqueQueueNames([processor, cronInit])).toThrow(
			QueueNameCollisionError
		)
		expect(() => assertUniqueQueueNames([cronInit, processor])).toThrow(
			QueueNameCollisionError
		)
	})

	test('checks names from dynamic arrays at runtime', () => {
		const queues: import('./queue.js').QueueContract[] = [
			defineQueue({ name: 'then', inputSchema: z.string() })
		]
		expect(() => defineQueues(queues)).toThrow('reserved')
		queues[0] = defineQueue({ name: '', inputSchema: z.string() })
		expect(() => defineQueues(queues)).toThrow('non-empty')
	})
})

describe('defineQueues name checks at compile time', () => {
	test('duplicates, reserved names and cron-init collisions fail to compile', () => {
		const one = defineQueue({ name: 'one', inputSchema: z.string() })
		const otherOne = defineQueue({ name: 'one', inputSchema: z.number() })

		const processor = defineQueue({
			name: 'sync_cron-init',
			inputSchema: z.string()
		})
		const cronInit = defineQueue({
			name: 'sync',
			cron: '0 * * * *',
			inputSchema: z.number()
		})
		const empty = defineQueue({ name: '', inputSchema: z.string() })
		// Type checks only; each call would throw.
		void (() => {
			// @ts-expect-error Two queues named "one".
			defineQueues([one, otherOne])

			// @ts-expect-error The cron-init task collides with a queue name.
			defineQueues([processor, cronInit])
			// @ts-expect-error Empty names are invalid.
			defineQueues([empty])
		})
		expect(defineQueues([one, cronInit])).toHaveLength(2)
		// Same name, same type: only the runtime check can see it.
		const sameOne = defineQueue({ name: 'one', inputSchema: z.string() })
		expect(() => defineQueues([one, sameOne])).toThrow(DuplicateQueueError)
	})

	test('400 queues compile', () => {
		const queues = defineQueues(manyQueues)
		expectTypeOf(queues[399].name).toEqualTypeOf<'q399'>()
	})
})
