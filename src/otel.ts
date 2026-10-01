import { createRequire } from 'node:module'
import type { JobSpan } from './hooks.js'

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
				options: { kind?: number; links?: OtelLink[] | undefined },
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

let cached: OtelApi | null | undefined

export function getOtel(): OtelApi | null {
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
	function safe(fn: () => unknown) {
		try {
			const result = fn()
			if (result && typeof (result as PromiseLike<unknown>).then === 'function')
				void Promise.resolve(result).catch(() => {})
		} catch {
			/* Observational tracing must not change job outcomes. */
		}
	}
	return {
		setAttribute: (key, value) => safe(() => span.setAttribute(key, value)),
		setAttributes: (attributes) => safe(() => span.setAttributes(attributes)),
		setStatus: (status) => safe(() => span.setStatus(status)),
		addEvent: (name, attributes) =>
			safe(() => span.addEvent?.(name, compactSpanEventAttributes(attributes))),
		recordException: (error) => safe(() => span.recordException(error)),
		end: () => safe(() => span.end())
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
	fn: (span: JobSpan) => Promise<T>,
	api: OtelApi | null = getOtel()
): Promise<T> {
	const otel = api
	if (!otel) {
		const span = createNoopSpan()
		try {
			return await fn(span)
		} finally {
			span.end()
		}
	}

	let execution: Promise<T> | undefined
	try {
		return await otel.trace.getTracer(tracerName).startActiveSpan(
			spanName,
			{
				kind:
					options.kind === 'consumer'
						? otel.SpanKind.CONSUMER
						: options.kind === 'producer'
							? otel.SpanKind.PRODUCER
							: otel.SpanKind.INTERNAL,
				links: options.links
			},
			async (rawSpan) => {
				const span = wrapOtelSpan(rawSpan)
				execution ??= Promise.resolve()
					.then(() => fn(span))
					.finally(() => span.end())
				return execution
			}
		)
	} catch {
		// Never invoke a handler again if an adapter throws after it started.
		if (execution) return execution
		return fn(createNoopSpan())
	}
}

export function getActiveTraceContext(api: OtelApi | null = getOtel()): {
	traceId: string
	spanId: string
	traceFlags: number
} | null {
	const otel = api
	if (!otel) return null
	const span = otel.trace.getActiveSpan()
	if (!span) return null
	const ctx = span.spanContext()
	if (!otel.isSpanContextValid(ctx)) return null
	return {
		traceId: ctx.traceId,
		spanId: ctx.spanId,
		traceFlags: ctx.traceFlags
	}
}

export function getActiveTraceparent(
	api: OtelApi | null = getOtel()
): string | null {
	const otel = api
	if (otel?.propagation && otel.context) {
		const carrier: Record<string, string> = {}
		otel.propagation.inject(otel.context.active(), carrier)
		if (carrier.traceparent) return carrier.traceparent
	}

	const ctx = getActiveTraceContext(api)
	if (!ctx) return null
	return formatTraceparent(ctx.traceId, ctx.spanId, ctx.traceFlags)
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
	if (!/^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/.test(traceparent))
		return null
	const [, traceId, spanId, flags] = traceparent.split('-')
	if (
		!traceId ||
		!spanId ||
		!flags ||
		/^0+$/.test(traceId) ||
		/^0+$/.test(spanId)
	)
		return null
	return { traceId, spanId, traceFlags: Number.parseInt(flags, 16) }
}

export function otelStatusCodes(api: OtelApi | null = getOtel()) {
	try {
		return {
			OK: api?.SpanStatusCode.OK ?? 1,
			ERROR: api?.SpanStatusCode.ERROR ?? 2
		}
	} catch {
		return { OK: 1, ERROR: 2 }
	}
}
