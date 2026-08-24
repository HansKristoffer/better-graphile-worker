import { describe, test, expect } from 'bun:test'
import { z } from 'zod'
import type { Pool } from 'pg'
import { createQueue } from './create-queue'
import { createBetterWorker } from './create-better-worker'
import type { JobLogger } from './hooks'

const silentLogger: JobLogger = {
	debug() {},
	info() {},
	warn() {},
	error() {}
}

const sendEmail = createQueue({
	name: 'sendEmail',
	inputSchema: z.object({ to: z.string() }),
	processFn: async () => {}
})

const dailySweep = createQueue({
	name: 'dailySweep',
	cron: '0 3 * * *',
	processFn: async () => {}
})

const queues = [sendEmail, dailySweep] as const

function createTestWorker(
	hooks?: Parameters<typeof createBetterWorker>[0]['hooks']
) {
	return createBetterWorker({
		pgPool: {} as Pool,
		queues,
		hooks: { createLogger: () => silentLogger, ...hooks }
	})
}

describe('createBetterWorker', () => {
	test('getQueueDefinitions lists registered queues', () => {
		const worker = createTestWorker()
		const defs = worker.getQueueDefinitions()
		expect(defs.map((d) => d.name)).toEqual(['sendEmail', 'dailySweep'])
		expect(defs[0]?.type).toBe('regular')
		expect(defs[1]?.type).toBe('cron')
		expect(defs[1]?.cron).toBe('0 3 * * *')
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

	test('jobs proxy exposes named enqueue helpers', () => {
		const worker = createTestWorker()
		expect(typeof worker.jobs.sendEmail).toBe('function')
		expect(typeof worker.jobs.dailySweep).toBe('function')
	})
})
