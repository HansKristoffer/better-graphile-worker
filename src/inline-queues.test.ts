import { expect, expectTypeOf, test } from 'bun:test'
import { z } from 'zod'
import { defineQueue } from './queue.js'
import { defineQueues } from './define-queues.js'
import { createTestHarness } from './testing.js'
import { createBetterWorker } from './create-better-worker.js'
import { normalizeWorkerQueues } from './registry.js'
import type { Pool } from 'pg'

test('inline handlers infer transformed payloads and enqueue through queue references', async () => {
	const received: number[] = []
	const count = defineQueue({
		name: 'count',
		inputSchema: z.string().transform((text) => text.length),
		processFn: (length, ctx) => {
			expectTypeOf(length).toEqualTypeOf<number>()
			expectTypeOf(ctx.queue).toEqualTypeOf<'count'>()
			received.push(length)
		}
	})
	const email = defineQueue({
		name: 'email',
		inputSchema: z.object({ to: z.string() }),
		processFn: async (payload, ctx) => {
			await ctx.createJob(count, payload.to, { priority: 3 })
			await ctx.createJobs(count, [payload.to, 'next'] as const)
		}
	})
	const queues = defineQueues([email, count])
	const harness = createTestHarness(queues)
	const { ctx } = await harness.process('email', { to: 'hello' })
	expectTypeOf(ctx.queue).toEqualTypeOf<'email'>()
	expect(ctx.queue).toBe('email')
	expectTypeOf(harness.context('count').queue).toEqualTypeOf<'count'>()
	for (const job of harness.enqueued) {
		if (job.queue === 'count') {
			expectTypeOf(job.payload).toEqualTypeOf<string>()
		} else {
			expectTypeOf(job.payload).toEqualTypeOf<{ to: string }>()
		}
	}
	expect(
		harness.enqueued.map(({ queue, payload }) => ({ queue, payload }))
	).toEqual([
		{ queue: 'count', payload: 'hello' },
		{ queue: 'count', payload: 'hello' },
		{ queue: 'count', payload: 'next' }
	])
	expect(harness.enqueued[0]?.options?.priority).toBe(3)
	await harness.process('count', 'hello')
	expect(received).toEqual([5])
	const worker = createBetterWorker({ pgPool: {} as Pool, queues })
	expect(Object.keys(worker.buildTaskList()).sort()).toEqual(['count', 'email'])
})

test('mutually referring inline queues keep literal names and payload types', async () => {
	const first = defineQueue({
		name: 'first',
		inputSchema: z.string(),
		processFn: async (payload, ctx) => {
			await ctx.createJob(second, payload.length)
		}
	})
	const second = defineQueue({
		name: 'second',
		inputSchema: z.number(),
		processFn: async (payload, ctx) => {
			await ctx.createJob(first, String(payload))
		}
	})
	expectTypeOf(first.name).toEqualTypeOf<'first'>()
	expectTypeOf(second.name).toEqualTypeOf<'second'>()
	const harness = createTestHarness(defineQueues([first, second]))
	await harness.process('first', 'hello')
	await harness.process('second', 5)
	expect(
		harness.enqueued.map(({ queue, payload }) => ({ queue, payload }))
	).toEqual([
		{ queue: 'second', payload: 5 },
		{ queue: 'first', payload: '5' }
	])
})

test('inline cron and cron-init handlers preserve initializer inputs and context', async () => {
	const sweep = defineQueue({
		name: 'sweep',
		cron: '* * * * *',
		processFn: (payload, ctx) => {
			expectTypeOf(payload).toEqualTypeOf<undefined>()
			expectTypeOf(ctx.queue).toEqualTypeOf<'sweep'>()
		}
	})
	let received = 0
	const gather = defineQueue({
		name: 'gather',
		cron: '* * * * *',
		inputSchema: z.string().transform((text) => text.length),
		initFn: async (ctx) => {
			expectTypeOf(ctx.queue).toEqualTypeOf<'gather_cron-init'>()
			await ctx.createJob(sweep)
			return ['hello'] as const
		},
		processFn: (length) => {
			expectTypeOf(length).toEqualTypeOf<number>()
			received = length
		}
	})
	const harness = createTestHarness([sweep, gather])
	expect(await harness.init('gather')).toEqual(['hello'])
	expect(harness.enqueued[0]).toMatchObject({
		queue: 'sweep',
		payload: undefined
	})
	await harness.process('sweep', undefined)
	await harness.process('gather', 'hello')
	expect(received).toBe(5)
})

test('inline enqueue rejects unregistered queue references, even with matching names', async () => {
	const target = defineQueue({
		name: 'target',
		inputSchema: z.number(),
		processFn: () => {}
	})
	const imposter = defineQueue({ name: 'target', inputSchema: z.string() })
	const parent = defineQueue({
		name: 'parent',
		inputSchema: z.string(),
		processFn: async (payload, ctx) => {
			await ctx.createJob(imposter, payload)
		}
	})
	const harness = createTestHarness([parent, target])
	await expect(harness.process('parent', 'hello')).rejects.toThrow(
		'Unknown queue "target"'
	)
	expect(harness.enqueued).toEqual([])
})

test('inline definitions require valid initializers and cannot also register handlers', () => {
	const cronInit = {
		name: 'invalid',
		cron: '* * * * *',
		inputSchema: z.string(),
		processFn: () => {}
	}
	expect(() => normalizeWorkerQueues([cronInit])).toThrow('requires an initFn')
	const queue = defineQueue({
		name: 'inline',
		inputSchema: z.string(),
		processFn: () => {}
	})
	expect(() =>
		createBetterWorker({
			pgPool: {} as Pool,
			queues: [queue],
			// @ts-expect-error Choose inline handlers or a separate registry.
			handlers: { inline: () => {} }
		})
	).toThrow('not both')
})
