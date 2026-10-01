import { NonRetriableError } from './errors.js'
import {
	getOtel,
	getActiveTraceparent,
	parseTraceparent,
	type OtelApi,
	type ProducerLink
} from './otel.js'

export const TRACEPARENT_KEY = 'traceparent' as const
export const BGW_ENVELOPE_KEY = '__bgw' as const
export const BGW_ENVELOPE_VERSION = 2 as const
export type StepCacheEntry = { output: unknown; isVoid?: boolean }
export type StepCache = Record<string, StepCacheEntry>
type EnvelopeMetadata = {
	[BGW_ENVELOPE_KEY]: typeof BGW_ENVELOPE_VERSION
	steps?: StepCache
	_cron?: unknown
	[TRACEPARENT_KEY]?: string
}
/** Root undefined is persisted as null plus an explicit discriminator. */
export type PayloadEnvelope<T = unknown> = EnvelopeMetadata &
	(undefined extends T
		?
				| { payload: Exclude<T, undefined>; payloadUndefined?: false }
				| { payload: null; payloadUndefined: true }
		: { payload: T; payloadUndefined?: false })

export function isPlainObject(
	value: unknown
): value is Record<string, unknown> {
	return (
		typeof value === 'object' &&
		value !== null &&
		!Array.isArray(value) &&
		(Object.getPrototypeOf(value) === Object.prototype ||
			Object.getPrototypeOf(value) === null)
	)
}
export function isPayloadEnvelope(value: unknown): value is PayloadEnvelope {
	return (
		isPlainObject(value) &&
		value[BGW_ENVELOPE_KEY] === BGW_ENVELOPE_VERSION &&
		Object.hasOwn(value, 'payload') &&
		(value.payloadUndefined === undefined ||
			value.payloadUndefined === false ||
			(value.payloadUndefined === true && value.payload === null))
	)
}
export function assertPayloadEnvelope(
	value: unknown
): asserts value is PayloadEnvelope {
	if (!isPayloadEnvelope(value))
		throw new NonRetriableError(
			'Unsupported job payload: expected a version-2 envelope. Drain old jobs before upgrading and enqueue through the worker/client APIs.'
		)
}
/** Every job has the same envelope, with optional W3C trace metadata. */
export function injectTraceContext<T>(
	data: T,
	api?: OtelApi | null
): PayloadEnvelope<T>
export function injectTraceContext(
	data: unknown,
	api: OtelApi | null = getOtel()
): PayloadEnvelope {
	const envelope: PayloadEnvelope = {
		[BGW_ENVELOPE_KEY]: BGW_ENVELOPE_VERSION,
		...(data === undefined
			? { payload: null, payloadUndefined: true as const }
			: { payload: data })
	}
	try {
		const traceparent = getActiveTraceparent(api)
		if (traceparent && parseTraceparent(traceparent))
			envelope[TRACEPARENT_KEY] = traceparent
	} catch {
		/* Optional tracing cannot prevent enqueue. */
	}
	return envelope
}
export function extractProducerLink(payload: unknown): {
	link: ProducerLink | null
	cleanPayload: unknown
} {
	assertPayloadEnvelope(payload)
	const parsed =
		typeof payload.traceparent === 'string'
			? parseTraceparent(payload.traceparent)
			: null
	return {
		link: parsed
			? { context: parsed, attributes: { 'link.type': 'producer' } }
			: null,
		cleanPayload: payload.payloadUndefined ? undefined : payload.payload
	}
}
export function extractCronMeta(
	payload: unknown
): { ts: Date; backfilled?: boolean } | undefined {
	if (
		!isPayloadEnvelope(payload) ||
		!isPlainObject(payload._cron) ||
		typeof payload._cron.ts !== 'string'
	)
		return undefined
	const ts = new Date(payload._cron.ts)
	if (!Number.isFinite(ts.getTime())) return undefined
	return payload._cron.backfilled === true ? { ts, backfilled: true } : { ts }
}
export function extractStepCache(payload: unknown): StepCache {
	assertPayloadEnvelope(payload)
	const cache: StepCache = Object.create(null)
	if (!isPlainObject(payload.steps)) return cache
	for (const [id, entry] of Object.entries(payload.steps)) {
		if (isPlainObject(entry) && Object.hasOwn(entry, 'output'))
			cache[id] =
				entry.isVoid === true
					? { output: null, isVoid: true }
					: { output: entry.output }
	}
	return cache
}
