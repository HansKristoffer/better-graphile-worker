import { describe, test, expect, expectTypeOf } from 'bun:test'
import { z } from 'zod'
import { createQueue } from './create-queue'
import { assertUniqueQueueNames, defineQueues } from './define-queues'
import { DuplicateQueueError, QueueNameCollisionError } from './errors'

describe('defineQueues', () => {
	test('returns the same tuple and preserves name literals', () => {
		const sendEmail = createQueue({
			name: 'sendEmail',
			inputSchema: z.object({ to: z.string() }),
			processFn: async () => {}
		})
		const sweep = createQueue({
			name: 'sweep',
			cron: '0 * * * *',
			processFn: async () => {}
		})
		const queues = defineQueues([sendEmail, sweep])
		expect(queues).toHaveLength(2)
		expectTypeOf(queues[0].name).toEqualTypeOf<'sendEmail'>()
		expectTypeOf(queues[1].name).toEqualTypeOf<'sweep'>()
	})

	test('rejects duplicate names', () => {
		const a = createQueue({
			name: 'dup',
			inputSchema: z.object({ id: z.string() }),
			processFn: async () => {}
		})
		const b = createQueue({
			name: 'dup',
			inputSchema: z.object({ id: z.string() }),
			processFn: async () => {}
		})
		expect(() => assertUniqueQueueNames([a, b])).toThrow(DuplicateQueueError)
	})

	test('rejects cron-init task name collisions', () => {
		const processor = createQueue({
			name: 'sync_cron-init',
			inputSchema: z.object({ id: z.string() }),
			processFn: async () => {}
		})
		const cronInit = createQueue({
			name: 'sync',
			cron: '0 * * * *',
			inputSchema: z.object({ id: z.string() }),
			initFn: async () => [],
			processFn: async () => {}
		})
		expect(() => assertUniqueQueueNames([processor, cronInit])).toThrow(
			QueueNameCollisionError
		)
	})
})
