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

export type PayloadEnvelope = {
	[BGW_ENVELOPE_KEY]: typeof BGW_ENVELOPE_VERSION
	payload: unknown
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
	if (!isPlainObject(payload)) return undefined
	const cron = payload._cron
	if (!isPlainObject(cron) || typeof cron.ts !== 'string') return undefined
	return {
		ts: new Date(cron.ts),
		backfilled: cron.backfilled === true ? true : undefined
	}
}
