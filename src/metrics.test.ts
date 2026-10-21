import { describe, expect, test } from 'bun:test'
import type { Pool } from 'pg'
import * as otel from '@opentelemetry/api'
import { z } from 'zod'
import { createJobClient } from './job-client.js'
import { defineQueue } from './queue.js'
import type { JobFinishedEvent } from './hooks.js'
import type { OtelMeter } from './otel.js'
import {
	QUEUE_METRICS_METER,
	queueMetricDefinitions,
	withQueueMetrics
} from './metrics.js'

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
	return { api: { metrics: { getMeter: () => meter } }, recorded }
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
	test('the exported definitions are the instruments the meter creates', () => {
		const created: unknown[] = []
		let meterName: string | undefined
		const meter: OtelMeter = {
			createCounter: (name, options) => {
				created.push({ name, kind: 'counter', ...options })
				return { add() {} }
			},
			createHistogram: (name, options) => {
				created.push({ name, kind: 'histogram', ...options })
				return { record() {} }
			}
		}
		withQueueMetrics(
			{},
			{
				metrics: {
					getMeter: (name) => {
						meterName = name
						return meter
					}
				}
			}
		)
		expect(meterName).toBe(QUEUE_METRICS_METER)
		expect(created).toEqual(
			queueMetricDefinitions.map(({ attributes: _, ...metric }) => metric)
		)
	})

	test('a finished job records its count, duration and permanent failure, then runs the hook', async () => {
		const { api, recorded } = fakeMeter()
		const seen: JobFinishedEvent[] = []
		const hooks = withQueueMetrics(
			{ onJobFinished: (event) => void seen.push(event) },
			api
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

	test('a failing meter still runs the hook and never throws', async () => {
		const fail = () => {
			throw new Error('exporter')
		}
		const api = {
			metrics: {
				getMeter: () => ({
					createCounter: () => ({ add: fail }),
					createHistogram: () => ({ record: fail })
				})
			}
		}
		const seen: JobFinishedEvent[] = []
		const hooks = withQueueMetrics(
			{ onJobFinished: (event) => void seen.push(event) },
			api
		)
		await hooks.onJobFinished?.(finished)
		expect(seen).toEqual([finished])
	})

	test('a job client counts failed enqueues only with metrics: true', async () => {
		const { api, recorded } = fakeMeter()
		const sync = defineQueue({
			name: 'sync',
			inputSchema: z.object({ id: z.string() })
		})
		const enqueueInvalid = (metrics: boolean) =>
			createJobClient({
				pgPool: {} as Pool,
				queues: [sync],
				otel: { api: { ...otel, ...api }, metrics }
			}).createJob('sync', { id: 1 } as never)

		await expect(enqueueInvalid(false)).rejects.toThrow()
		expect(recorded).toEqual([])
		await expect(enqueueInvalid(true)).rejects.toThrow()
		expect(recorded).toEqual([
			{
				name: 'graphile_job_enqueue_fail_total',
				value: 1,
				attributes: { queue: 'sync' }
			}
		])
	})
})
