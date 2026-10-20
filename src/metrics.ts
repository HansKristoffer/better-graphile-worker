import type { BetterWorkerHooks, JobFinishedEvent } from './hooks.js'
import type { OtelApi, OtelOption } from './otel.js'

/**
 * Queue metrics recorded through the instance's OTel meter. Recording runs before the
 * caller's hook and never throws, so a failing exporter cannot change a job's outcome.
 */
function createQueueMetrics(api: OtelApi) {
	const meter = api.metrics?.getMeter('graphile-worker')
	if (!meter) return undefined
	return {
		jobs: meter.createCounter('graphile_job_total', {
			description: 'Graphile Worker jobs finished by queue and status'
		}),
		duration: meter.createHistogram('graphile_job_duration_ms', {
			description: 'Graphile Worker job duration by queue and status',
			unit: 'ms'
		}),
		permanentlyFailed: meter.createCounter(
			'graphile_job_permanently_failed_total',
			{ description: 'Graphile Worker jobs that exhausted max attempts' }
		),
		enqueueFailed: meter.createCounter('graphile_job_enqueue_fail_total', {
			description: 'Failed createJob / prepareJob enqueue attempts'
		})
	}
}

function record(fn: () => void) {
	try {
		fn()
	} catch {
		/* Metrics must not change job outcomes. */
	}
}

export function withQueueMetrics(
	hooks: BetterWorkerHooks,
	api: OtelApi | null,
	option: OtelOption | undefined
): BetterWorkerHooks {
	if (!option?.metrics || !api) return hooks
	let metrics: ReturnType<typeof createQueueMetrics>
	try {
		metrics = createQueueMetrics(api)
	} catch {
		return hooks
	}
	if (!metrics) return hooks
	const { jobs, duration, permanentlyFailed, enqueueFailed } = metrics
	return {
		...hooks,
		onJobFinished(event: JobFinishedEvent) {
			record(() => {
				const attributes = { queue: event.queue, status: event.status }
				jobs.add(1, attributes)
				duration.record(event.durationMs, attributes)
				if (event.permanentlyFailed)
					permanentlyFailed.add(1, { queue: event.queue })
			})
			return hooks.onJobFinished?.(event)
		},
		onEnqueueFail(event) {
			record(() => enqueueFailed.add(1, { queue: event.queue }))
			return hooks.onEnqueueFail?.(event)
		}
	}
}
