import { describe, test, expect } from 'bun:test'
import { z } from 'zod'
import { createQueue } from './create-queue'
import { createTestHarness } from './testing'

const sendEmail = createQueue({
	name: 'sendEmail',
	inputSchema: z.object({ to: z.string() }),
	processFn: async (payload, ctx) => {
		ctx.logger.info('sending', { to: payload.to })
	}
})

const gather = createQueue({
	name: 'gather',
	cron: '0 * * * *',
	inputSchema: z.object({ id: z.string() }),
	initFn: async () => [{ id: '1' }],
	processFn: async () => {}
})

describe('createTestHarness', () => {
	test('invokes processFn with a stub context', async () => {
		const harness = createTestHarness([sendEmail, gather])
		await harness.process('sendEmail', { to: 'a@b.com' })
		expect(harness.logs[0]).toMatchObject({
			level: 'info',
			message: 'sending',
			attributes: { to: 'a@b.com' }
		})
	})

	test('invokes initFn', async () => {
		const harness = createTestHarness([sendEmail, gather])
		expect(await harness.init('gather')).toEqual([{ id: '1' }])
	})

	test('caches step.run results across process calls with the same jobId', async () => {
		const counts = { fetch: 0, send: 0 }
		const checkout = createQueue({
			name: 'checkout',
			inputSchema: z.object({ userId: z.string() }),
			processFn: async (payload, ctx) => {
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
		})

		const harness = createTestHarness([checkout])
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
