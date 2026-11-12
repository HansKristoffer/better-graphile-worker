import { describe, test, expect } from 'bun:test'
import { z } from 'zod'
import type { Pool } from 'pg'
import { defineQueue } from './queue.js'
import { createBetterWorker } from './create-better-worker.js'
import { lazyWorker } from './lazy.js'
import type { JobLogger } from './hooks.js'

const silentLogger: JobLogger = {
	debug() {},
	info() {},
	warn() {},
	error() {}
}

const sendEmail = defineQueue({
	name: 'sendEmail',
	inputSchema: z.object({ to: z.string() })
})

const dailySweep = defineQueue({
	name: 'dailySweep',
	cron: '0 3 * * *'
})

const queues = [sendEmail, dailySweep] as const

function createTestWorker(
	hooks?: Parameters<typeof createBetterWorker>[0]['hooks']
) {
	return createBetterWorker({
		pgPool: {} as Pool,
		queues,
		handlers: { sendEmail: () => {}, dailySweep: () => {} },
		hooks: { createLogger: () => silentLogger, ...hooks }
	})
}

describe('createBetterWorker', () => {
	test('getQueueDefinitions lists registered queues', () => {
		const worker = createTestWorker()
		const defs = worker.getQueueDefinitions()
		expect(defs.map((d) => d.name)).toEqual(['sendEmail', 'dailySweep'])
		expect(defs[0]?.type).toBe('regular')
		expect(defs[0]?.inputSchema).toBe(sendEmail.inputSchema)
		expect(defs[1]?.type).toBe('cron')
		expect(defs[1]?.cron).toBe('0 3 * * *')
		expect(defs[1]?.inputSchema).toBeNull()
	})

	test('shouldSkipEnqueue skips createJob and createJobs', async () => {
		const worker = createTestWorker({
			shouldSkipEnqueue: () => true
		})

		expect(await worker.createJob('sendEmail', { to: 'a@b.com' })).toBeNull()
		expect(await worker.createJobs('sendEmail', [{ to: 'a@b.com' }])).toEqual(
			[]
		)
	})

	test('onEnqueueFail is not called when enqueue is skipped', async () => {
		let called = false
		const worker = createTestWorker({
			shouldSkipEnqueue: () => true,
			onEnqueueFail: () => {
				called = true
			}
		})
		await worker.createJob('sendEmail', { to: 'a@b.com' })
		expect(called).toBe(false)
	})

	test('buildTaskList and buildCronItems are available on the instance', () => {
		const worker = createTestWorker()
		expect(worker.buildTaskList().sendEmail).toBeDefined()
		expect(worker.buildCronItems()).toHaveLength(1)
		expect(worker.buildCronItems()[0]?.task).toBe('dailySweep')
	})

	test('completed jobs store starts empty', () => {
		const worker = createBetterWorker({
			pgPool: {} as Pool,
			queues,
			handlers: { sendEmail: () => {}, dailySweep: () => {} },
			completedJobs: {},
			hooks: { createLogger: () => silentLogger }
		})
		expect(worker.getCompletedJobs()).toEqual([])
		expect(worker.getCompletedJobsStats()).toEqual({})
	})

	test('start can be followed by stop without awaiting runner.promise', async () => {
		const worker = createTestWorker()
		await expect(worker.stop()).resolves.toBeUndefined()
		expect(worker.promise).toBeInstanceOf(Promise)
		await expect(worker.waitUntilStopped()).resolves.toBeUndefined()
	})

	test('named jobs exposes named enqueue helpers', () => {
		const worker = createTestWorker()
		expect(typeof worker.jobs.sendEmail).toBe('function')
		expect(typeof worker.jobs.dailySweep).toBe('function')
	})
})

test('handler-free contracts require a handler registry', () => {
	expect(() =>
		// @ts-expect-error A handler-free queue cannot run by itself.
		createBetterWorker({ pgPool: {} as Pool, queues: [sendEmail] })
	).toThrow('requires a processFn')
})

describe('3.0 defaults', () => {
	const tick = defineQueue({ name: 'tick', cron: '* * * * *' })
	const overlapping = defineQueue({
		name: 'overlapping',
		cron: '* * * * *',
		serial: false
	})
	const unlimited = defineQueue({
		name: 'unlimited',
		inputSchema: z.string(),
		timeout: false
	})
	const make = (options: { defaultTimeout?: number | false } = {}) =>
		createBetterWorker({
			pgPool: {} as Pool,
			queues: [sendEmail, tick, overlapping, unlimited],
			handlers: {
				sendEmail: () => {},
				tick: () => {},
				overlapping: () => {},
				unlimited: () => {}
			},
			...options
		})

	test('plain cron queues get their own lane unless serial is false', () => {
		const items = make().buildCronItems()
		expect(items.map((item) => [item.task, item.options?.queueName])).toEqual([
			['tick', 'tick'],
			['overlapping', undefined]
		])
		expect(
			make()
				.getQueueDefinitions()
				.map(({ name, serial }) => [name, serial])
		).toEqual([
			['sendEmail', null],
			['tick', true],
			['overlapping', false],
			['unlimited', null]
		])
	})

	test('attempts time out below Graphile reclaim unless the worker or queue says false', () => {
		const timeouts = (worker: ReturnType<typeof make>) =>
			worker.getQueueDefinitions().map(({ timeout }) => timeout)
		expect(timeouts(make())).toEqual([13_800_000, 13_800_000, 13_800_000, null])
		expect(timeouts(make({ defaultTimeout: false }))).toEqual([
			null,
			null,
			null,
			null
		])
		expect(timeouts(make({ defaultTimeout: 60_000 }))).toEqual([
			60_000,
			60_000,
			60_000,
			null
		])
	})
})

test('lazyWorker builds the worker on first use, once', () => {
	let built = 0
	const worker = lazyWorker(() => {
		built++
		return createTestWorker()
	})
	expect(built).toBe(0)
	expect(worker.getQueueDefinitions().map(({ name }) => name)).toEqual([
		'sendEmail',
		'dailySweep'
	])
	expect(worker.schema).toBe('graphile_worker')
	expect('createJob' in worker).toBe(true)
	expect(built).toBe(1)
})
