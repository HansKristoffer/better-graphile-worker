import type { BetterWorkerHooks, JobFinishedEvent } from './hooks.js'
import type { OtelApi } from './otel.js'

type MetricsApi = Pick<OtelApi, 'metrics'>

/** The meter `otel: { metrics: true }` records on. */
export const QUEUE_METRICS_METER = 'graphile-worker'

/**
 * The queue metrics this library records, for an app's metric catalog. `attributes` lists
 * each attribute key, with its values when the set is closed.
 */
export const queueMetricDefinitions = [
	{
		name: 'graphile_job_total',
		kind: 'counter',
		description: 'Graphile Worker jobs finished by queue and status',
		attributes: { queue: 'string', status: ['success', 'failed'] }
	},
	{
		name: 'graphile_job_duration_ms',
		kind: 'histogram',
		unit: 'ms',
		description: 'Graphile Worker job duration by queue and status',
		attributes: { queue: 'string', status: ['success', 'failed'] }
	},
	{
		name: 'graphile_job_permanently_failed_total',
		kind: 'counter',
		description: 'Graphile Worker jobs that exhausted max attempts',
		attributes: { queue: 'string' }
	},
	{
		name: 'graphile_job_enqueue_fail_total',
		kind: 'counter',
		description: 'Failed createJob / prepareJob enqueue attempts',
		attributes: { queue: 'string' }
	}
] as const satisfies readonly QueueMetricDefinition[]

export type QueueMetricDefinition = {
	readonly name: string
	readonly kind: 'counter' | 'histogram'
	readonly unit?: 'ms'
	readonly description: string
	readonly attributes: Readonly<Record<string, 'string' | readonly string[]>>
}

const [totalMetric, durationMetric, permanentMetric, enqueueFailMetric] =
	queueMetricDefinitions

/**
 * Queue metrics recorded through the instance's OTel meter. Recording runs before the
 * caller's hook and never throws, so a failing exporter cannot change a job's outcome.
 */
function createQueueMetrics(api: MetricsApi) {
	const meter = api.metrics?.getMeter(QUEUE_METRICS_METER)
	if (!meter) return undefined
	const options = (metric: QueueMetricDefinition) =>
		metric.unit === undefined
			? { description: metric.description }
			: { description: metric.description, unit: metric.unit }
	return {
		jobs: meter.createCounter(totalMetric.name, options(totalMetric)),
		duration: meter.createHistogram(
			durationMetric.name,
			options(durationMetric)
		),
		permanentlyFailed: meter.createCounter(
			permanentMetric.name,
			options(permanentMetric)
		),
		enqueueFailed: meter.createCounter(
			enqueueFailMetric.name,
			options(enqueueFailMetric)
		)
	}
}

function record(fn: () => void) {
	try {
		fn()
	} catch {
		/* Metrics must not change job outcomes. */
	}
}

/** `api` is the instance's OTel API when `otel.metrics` is on, otherwise null. */
export function withQueueMetrics(
	hooks: BetterWorkerHooks,
	api: MetricsApi | null
): BetterWorkerHooks {
	if (!api) return hooks
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
