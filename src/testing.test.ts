import type { QueueHandlers } from './queue.js'
import { describe, test, expect } from 'bun:test'
import { z } from 'zod'
import { defineQueue } from './queue.js'
import { createTestHarness } from './testing.js'

const sendEmail = defineQueue({
	name: 'sendEmail',
	inputSchema: z.object({ to: z.string() })
})
const sendEmailHandler: QueueHandlers<
	readonly [typeof sendEmail]
>['sendEmail'] = async (payload, ctx) => {
	ctx.logger.info('sending', { to: payload.to })
}

const gather = defineQueue({
	name: 'gather',
	cron: '0 * * * *',
	inputSchema: z.object({ id: z.string() })
})
const gatherHandler: QueueHandlers<readonly [typeof gather]>['gather'] = {
	initFn: async () => [{ id: '1' }],
	processFn: async () => {}
}

describe('createTestHarness', () => {
	test('invokes processFn with a stub context', async () => {
		const harness = createTestHarness([sendEmail, gather], {
			sendEmail: sendEmailHandler,
			gather: gatherHandler
		})
		await harness.process('sendEmail', { to: 'a@b.com' })
		expect(harness.logs[0]).toMatchObject({
			level: 'info',
			message: 'sending',
			attributes: { to: 'a@b.com' }
		})
	})

	test('invokes initFn', async () => {
		const harness = createTestHarness([sendEmail, gather], {
			sendEmail: sendEmailHandler,
			gather: gatherHandler
		})
		expect(await harness.init('gather')).toEqual([{ id: '1' }])
	})

	test('caches step.run results across process calls with the same jobId', async () => {
		const counts = { fetch: 0, send: 0 }
		const checkout = defineQueue({
			name: 'checkout',
			inputSchema: z.object({ userId: z.string() })
		})
		const checkoutHandler: QueueHandlers<
			readonly [typeof checkout]
		>['checkout'] = async (payload, ctx) => {
			const user = await ctx.step.run('fetch-user', async () => {
				counts.fetch += 1
				return { id: payload.userId }
			})
			await ctx.step.run('send-email', async () => {
				counts.send += 1
				void user
				throw new Error('smtp down')
			})
		}

		const harness = createTestHarness([checkout], { checkout: checkoutHandler })
		await expect(
			harness.process('checkout', { userId: 'u1' }, { jobId: 'job-1' })
		).rejects.toThrow('smtp down')
		await expect(
			harness.process('checkout', { userId: 'u1' }, { jobId: 'job-1' })
		).rejects.toThrow('smtp down')

		expect(counts.fetch).toBe(1)
		expect(counts.send).toBe(2)
	})
})

test('handler-free contracts still require handlers', () => {
	expect(() =>
		// @ts-expect-error A handler-free contract cannot run by itself.
		createTestHarness([sendEmail])
	).toThrow('requires a processFn')
})
