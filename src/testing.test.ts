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

describe('enqueueing through the harness', () => {
	const order = defineQueue({
		name: 'order',
		inputSchema: z.object({ id: z.string() }),
		maxAttempts: 2,
		deriveJobOptions: ({ id }) => ({ jobKey: `order:${id}` }),
		processFn: async (payload, ctx) => {
			attempts.push([payload.id, ctx.attempt, ctx.maxAttempts])
			await ctx.createJob(receipt, { orderId: payload.id })
		}
	})
	const receipt = defineQueue({
		name: 'receipt',
		inputSchema: z.object({ orderId: z.string() }),
		processFn: async (payload) => void sent.push(payload.orderId)
	})
	const digest = defineQueue({
		name: 'digest',
		cron: '0 * * * *',
		inputSchema: z.object({ id: z.string() }),
		initFn: async () => [{ id: 'nightly' }],
		processFn: async (payload) => void sent.push(payload.id)
	})
	const attempts: [string, number, number][] = []
	const sent: string[] = []

	test('createJob validates and derives like the worker; drain runs what follows', async () => {
		const harness = createTestHarness([order, receipt, digest])
		await harness.createJob('order', { id: 'a' })
		await harness.enqueueInTransaction(
			{ $queryRawUnsafe: async () => [] },
			{ task: 'order', payload: { id: 'b' } }
		)
		await expect(
			harness.createJob('order', { id: 1 } as never)
		).rejects.toThrow()
		expect(harness.enqueued[0]).toMatchObject({
			queue: 'order',
			options: { jobKey: 'order:a' }
		})
		expect(await harness.drain()).toBe(4)
		// The queue's own maxAttempts, not Graphile's default.
		expect(attempts).toEqual([
			['a', 1, 2],
			['b', 1, 2]
		])
		expect(sent).toEqual(['a', 'b'])
		expect(await harness.drain()).toBe(0)
	})

	test('drain runs a cron-init trigger and the items it returns', async () => {
		sent.length = 0
		const harness = createTestHarness([order, receipt, digest])
		await harness.triggerCron('digest')
		expect(await harness.drain()).toBe(2)
		expect(sent).toEqual(['nightly'])
	})

	test('drain stops a job that keeps enqueueing itself', async () => {
		const loop = defineQueue({
			name: 'loop',
			inputSchema: z.number(),
			processFn: async (n, ctx) => void (await ctx.createJob(loop, n + 1))
		})
		const harness = createTestHarness([loop])
		await harness.createJob('loop', 0)
		await expect(harness.drain({ maxJobs: 5 })).rejects.toThrow('ran 5 jobs')
	})
})
