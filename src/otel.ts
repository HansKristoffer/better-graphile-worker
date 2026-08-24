import { createRequire } from 'node:module'
import type { JobSpan } from './hooks'

type OtelSpan = {
	setAttribute(key: string, value: string | number | boolean): unknown
	setAttributes(attributes: Record<string, string | number | boolean>): unknown
	setStatus(status: { code: number; message?: string }): unknown
	addEvent?(
		name: string,
		attributes?: Record<string, string | number | boolean>
	): unknown
	recordException(error: Error): unknown
	end(): unknown
	spanContext(): { traceId: string; spanId: string; traceFlags: number }
}

function compactSpanEventAttributes(
	attributes?: Record<string, string | number | boolean | null | undefined>
): Record<string, string | number | boolean> | undefined {
	if (!attributes) return undefined
	const compact: Record<string, string | number | boolean> = {}
	for (const [key, value] of Object.entries(attributes)) {
		if (value !== null && value !== undefined) {
			compact[key] = value
		}
	}
	return compact
}

type OtelLink = {
	context: { traceId: string; spanId: string; traceFlags: number }
	attributes?: Record<string, string>
}

type OtelContext = unknown

export type OtelApi = {
	trace: {
		getTracer(name: string): {
			startActiveSpan<T>(
				name: string,
				options: { kind?: number; links?: OtelLink[] },
				fn: (span: OtelSpan) => Promise<T>
			): Promise<T>
		}
		getActiveSpan(): OtelSpan | undefined
	}
	isSpanContextValid(ctx: { traceId: string; spanId: string }): boolean
	SpanStatusCode: { OK: number; ERROR: number }
	SpanKind: { INTERNAL: number; CONSUMER: number; PRODUCER: number }
	TraceFlags: { SAMPLED: number }
	context?: {
		active(): OtelContext
	}
	propagation?: {
		inject(
			context: OtelContext,
			carrier: Record<string, string>,
			setter?: {
				set(carrier: Record<string, string>, key: string, value: string): void
			}
		): void
		extract(
			context: OtelContext,
			carrier: Record<string, string>,
			getter?: {
				get(
					carrier: Record<string, string>,
					key: string
				): string | string[] | undefined
				keys(carrier: Record<string, string>): string[]
			}
		): OtelContext
	}
}

let injected: OtelApi | null | undefined
let cached: OtelApi | null | undefined

export function setOtelApi(api: OtelApi | null): void {
	injected = api
	cached = api
}

export function getOtel(): OtelApi | null {
	if (injected !== undefined) return injected
	if (cached !== undefined) return cached
	try {
		const require = createRequire(import.meta.url)
		cached = require('@opentelemetry/api') as OtelApi
	} catch {
		cached = null
	}
	return cached
}

export const OTEL_SAMPLED = 1

export function createNoopSpan(): JobSpan {
	return {
		setAttribute() {},
		setAttributes() {},
		setStatus() {},
		addEvent() {},
		recordException() {},
		end() {}
	}
}

export function wrapOtelSpan(span: OtelSpan): JobSpan {
	return {
		setAttribute(key, value) {
			span.setAttribute(key, value)
		},
		setAttributes(attributes) {
			span.setAttributes(attributes)
		},
		setStatus(status) {
			span.setStatus(status)
		},
		addEvent(name, attributes) {
			span.addEvent?.(name, compactSpanEventAttributes(attributes))
		},
		recordException(error) {
			span.recordException(error)
		},
		end() {
			span.end()
		}
	}
}

export type ProducerLink = {
	context: {
		traceId: string
		spanId: string
		traceFlags: number
	}
	attributes: { 'link.type': 'producer' }
}

export type SpanKindName = 'internal' | 'consumer' | 'producer'

export async function withActiveSpan<T>(
	tracerName: string,
	spanName: string,
	options: { kind: SpanKindName; links?: ProducerLink[] },
	fn: (span: JobSpan) => Promise<T>
): Promise<T> {
	const otel = getOtel()
	if (!otel) {
		const span = createNoopSpan()
		try {
			return await fn(span)
		} finally {
			span.end()
		}
	}

	const kind =
		options.kind === 'consumer'
			? otel.SpanKind.CONSUMER
			: options.kind === 'producer'
				? otel.SpanKind.PRODUCER
				: otel.SpanKind.INTERNAL

	return otel.trace.getTracer(tracerName).startActiveSpan(
		spanName,
		{
			kind,
			links: options.links
		},
		async (otelSpan) => {
			const span = wrapOtelSpan(otelSpan)
			try {
				return await fn(span)
			} finally {
				span.end()
			}
		}
	)
}

export function getActiveTraceContext(): {
	traceId: string
	spanId: string
} | null {
	const otel = getOtel()
	if (!otel) return null
	const span = otel.trace.getActiveSpan()
	if (!span) return null
	const ctx = span.spanContext()
	if (!otel.isSpanContextValid(ctx)) return null
	return { traceId: ctx.traceId, spanId: ctx.spanId }
}

export function getActiveTraceparent(): string | null {
	const otel = getOtel()
	if (otel?.propagation && otel.context) {
		const carrier: Record<string, string> = {}
		otel.propagation.inject(otel.context.active(), carrier)
		if (carrier.traceparent) return carrier.traceparent
	}

	const ctx = getActiveTraceContext()
	if (!ctx) return null
	return formatTraceparent(ctx.traceId, ctx.spanId, OTEL_SAMPLED)
}

export function formatTraceparent(
	traceId: string,
	spanId: string,
	traceFlags: number
): string {
	const flags = (traceFlags & 0xff).toString(16).padStart(2, '0')
	return `00-${traceId}-${spanId}-${flags}`
}

export function parseTraceparent(traceparent: string): {
	traceId: string
	spanId: string
	traceFlags: number
} | null {
	const parts = traceparent.split('-')
	if (parts.length !== 4 || parts[0] !== '00') return null
	const [, traceId, spanId, flags] = parts
	if (!traceId || !spanId || !flags) return null
	if (traceId.length !== 32 || spanId.length !== 16) return null
	const traceFlags = Number.parseInt(flags, 16)
	if (Number.isNaN(traceFlags)) return null
	return { traceId, spanId, traceFlags }
}

export function otelStatusCodes() {
	const otel = getOtel()
	return {
		OK: otel?.SpanStatusCode.OK ?? 1,
		ERROR: otel?.SpanStatusCode.ERROR ?? 2
	}
}
