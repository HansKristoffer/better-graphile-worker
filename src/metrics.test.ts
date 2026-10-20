import { describe, expect, test } from 'bun:test'
import type { Pool } from 'pg'
import { z } from 'zod'
import { createJobClient } from './job-client.js'
import { defineQueue } from './queue.js'
import type { JobFinishedEvent } from './hooks.js'
import type { OtelApi, OtelMeter } from './otel.js'
import { withQueueMetrics } from './metrics.js'

function fakeMeter() {
	const recorded: { name: string; value: number; attributes?: unknown }[] = []
	const meter: OtelMeter = {
		createCounter: (name) => ({
			add: (value, attributes) => recorded.push({ name, value, attributes })
		}),
		createHistogram: (name) => ({
			record: (value, attributes) => recorded.push({ name, value, attributes })
		})
	}
	const api = { metrics: { getMeter: () => meter } } as unknown as OtelApi
	return { api, recorded }
}

const finished: JobFinishedEvent = {
	queue: 'sync',
	status: 'failed',
	durationMs: 42,
	permanentlyFailed: true,
	jobId: '1',
	operation: 'process',
	attempt: 4,
	maxAttempts: 4
}

describe('queue metrics', () => {
	test('a finished job records its count, duration and permanent failure, then runs the hook', async () => {
		const { api, recorded } = fakeMeter()
		const seen: JobFinishedEvent[] = []
		const hooks = withQueueMetrics(
			{ onJobFinished: (event) => void seen.push(event) },
			api,
			{ api, metrics: true }
		)
		await hooks.onJobFinished?.(finished)
		expect(recorded).toEqual([
			{
				name: 'graphile_job_total',
				value: 1,
				attributes: { queue: 'sync', status: 'failed' }
			},
			{
				name: 'graphile_job_duration_ms',
				value: 42,
				attributes: { queue: 'sync', status: 'failed' }
			},
			{
				name: 'graphile_job_permanently_failed_total',
				value: 1,
				attributes: { queue: 'sync' }
			}
		])
		expect(seen).toEqual([finished])
	})

	test('metrics are opt-in and a failing meter never reaches the job', async () => {
		const { api, recorded } = fakeMeter()
		const hooks = {}
		expect(withQueueMetrics(hooks, api, { api })).toBe(hooks)
		expect(withQueueMetrics(hooks, null, { api: null, metrics: true })).toBe(
			hooks
		)

		const broken = {
			metrics: {
				getMeter: () => ({
					createCounter: () => ({
						add: () => {
							throw new Error('exporter')
						}
					}),
					createHistogram: () => ({
						record: () => {
							throw new Error('exporter')
						}
					})
				})
			}
		} as unknown as OtelApi
		await withQueueMetrics({}, broken, {
			api: broken,
			metrics: true
		}).onJobFinished?.(finished)
		expect(recorded).toEqual([])
	})

	test('a job client counts failed enqueues', async () => {
		const { api, recorded } = fakeMeter()
		const client = createJobClient({
			pgPool: {} as Pool,
			queues: [
				defineQueue({ name: 'sync', inputSchema: z.object({ id: z.string() }) })
			],
			otel: { api, metrics: true }
		})
		await expect(client.createJob('sync', { id: 1 } as never)).rejects.toThrow()
		expect(recorded).toEqual([
			{
				name: 'graphile_job_enqueue_fail_total',
				value: 1,
				attributes: { queue: 'sync' }
			}
		])
	})
})
