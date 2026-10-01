import { describe, test, expect, expectTypeOf } from 'bun:test'
import { z } from 'zod'
import { defineQueue } from './queue.js'
import { assertUniqueQueueNames, defineQueues } from './define-queues.js'
import { DuplicateQueueError, QueueNameCollisionError } from './errors.js'

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
