import {
	OTEL_SAMPLED,
	getActiveTraceContext,
	getActiveTraceparent,
	parseTraceparent,
	type ProducerLink
} from './otel'

export type JobTraceContext = {
	traceId: string
	spanId: string
}

export const TRACE_CONTEXT_KEY = '__trace' as const
export const TRACEPARENT_KEY = 'traceparent' as const
export const BGW_ENVELOPE_KEY = '__bgw' as const
export const BGW_ENVELOPE_VERSION = 1 as const

export type StepCache = Record<string, { output: unknown }>

export type PayloadEnvelope = {
	[BGW_ENVELOPE_KEY]: typeof BGW_ENVELOPE_VERSION
	payload: unknown
	steps?: StepCache
	[TRACE_CONTEXT_KEY]?: JobTraceContext
	[TRACEPARENT_KEY]?: string
}

export function isPlainObject(
	value: unknown
): value is Record<string, unknown> {
	return typeof value === 'object' && value !== null && !Array.isArray(value)
}

export function isPayloadEnvelope(value: unknown): value is PayloadEnvelope {
	return (
		isPlainObject(value) &&
		value[BGW_ENVELOPE_KEY] === BGW_ENVELOPE_VERSION &&
		'payload' in value
	)
}

export function injectTraceContext(data: unknown): unknown {
	const traceCtx = getActiveTraceContext()
	const traceparent = getActiveTraceparent()
	if (!traceCtx && !traceparent) return data

	const extras: Record<string, unknown> = {}
	if (traceCtx) extras[TRACE_CONTEXT_KEY] = traceCtx
	if (traceparent) extras[TRACEPARENT_KEY] = traceparent

	if (isPlainObject(data)) {
		return { ...data, ...extras }
	}

	return {
		[BGW_ENVELOPE_KEY]: BGW_ENVELOPE_VERSION,
		payload: data,
		...extras
	} satisfies PayloadEnvelope
}

export function extractProducerLink(payload: unknown): {
	link: ProducerLink | null
	cleanPayload: unknown
} {
	if (!isPlainObject(payload)) {
		return { link: null, cleanPayload: payload }
	}

	const source = isPayloadEnvelope(payload) ? payload : payload
	const cleanPayload = isPayloadEnvelope(payload)
		? payload.payload
		: stripTraceFields(payload)

	const link = linkFromPayload(source)
	return { link, cleanPayload }
}

function stripTraceFields(
	payload: Record<string, unknown>
): Record<string, unknown> {
	const {
		[TRACE_CONTEXT_KEY]: _trace,
		[TRACEPARENT_KEY]: _traceparent,
		...rest
	} = payload
	return rest
}

function linkFromPayload(
	payload: Record<string, unknown>
): ProducerLink | null {
	const traceparent = payload[TRACEPARENT_KEY]
	if (typeof traceparent === 'string') {
		const parsed = parseTraceparent(traceparent)
		if (parsed) {
			return {
				context: {
					traceId: parsed.traceId,
					spanId: parsed.spanId,
					traceFlags: parsed.traceFlags
				},
				attributes: { 'link.type': 'producer' }
			}
		}
	}

	const traceCtx = payload[TRACE_CONTEXT_KEY] as JobTraceContext | undefined
	if (!traceCtx?.traceId || !traceCtx?.spanId) return null

	return {
		context: {
			traceId: traceCtx.traceId,
			spanId: traceCtx.spanId,
			traceFlags: OTEL_SAMPLED
		},
		attributes: { 'link.type': 'producer' }
	}
}

export function extractCronMeta(
	payload: unknown
): { ts: Date; backfilled?: boolean } | undefined {
	const source = isPayloadEnvelope(payload) ? payload.payload : payload
	if (!isPlainObject(source)) return undefined
	const cron = source._cron
	if (!isPlainObject(cron) || typeof cron.ts !== 'string') return undefined
	return {
		ts: new Date(cron.ts),
		backfilled: cron.backfilled === true ? true : undefined
	}
}

export function extractStepCache(payload: unknown): StepCache {
	if (!isPayloadEnvelope(payload) || !isPlainObject(payload.steps)) {
		return {}
	}

	const cache: StepCache = {}
	for (const [id, value] of Object.entries(payload.steps)) {
		if (isPlainObject(value) && 'output' in value) {
			cache[id] = { output: value.output }
		}
	}
	return cache
}

export function withStepCache(
	rawPayload: unknown,
	steps: StepCache
): PayloadEnvelope {
	if (isPayloadEnvelope(rawPayload)) {
		const envelope: PayloadEnvelope = {
			[BGW_ENVELOPE_KEY]: BGW_ENVELOPE_VERSION,
			payload: rawPayload.payload,
			steps
		}
		if (rawPayload[TRACE_CONTEXT_KEY]) {
			envelope[TRACE_CONTEXT_KEY] = rawPayload[TRACE_CONTEXT_KEY]
		}
		if (rawPayload[TRACEPARENT_KEY]) {
			envelope[TRACEPARENT_KEY] = rawPayload[TRACEPARENT_KEY]
		}
		return envelope
	}

	if (isPlainObject(rawPayload)) {
		const {
			[TRACE_CONTEXT_KEY]: trace,
			[TRACEPARENT_KEY]: traceparent,
			...rest
		} = rawPayload
		const envelope: PayloadEnvelope = {
			[BGW_ENVELOPE_KEY]: BGW_ENVELOPE_VERSION,
			payload: rest,
			steps
		}
		if (isPlainObject(trace) && typeof trace.traceId === 'string') {
			envelope[TRACE_CONTEXT_KEY] = trace as JobTraceContext
		}
		if (typeof traceparent === 'string') {
			envelope[TRACEPARENT_KEY] = traceparent
		}
		return envelope
	}

	return {
		[BGW_ENVELOPE_KEY]: BGW_ENVELOPE_VERSION,
		payload: rawPayload,
		steps
	}
}
