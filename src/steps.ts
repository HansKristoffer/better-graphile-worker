import type { JobHelpers } from 'graphile-worker'
import type { JobSpan } from './hooks.js'
import { NonRetriableError, StepSerializationError } from './errors.js'
import {
	extractStepCache,
	type StepCache,
	type StepCacheEntry
} from './payload.js'
import { writeCheckpoint } from './private-jobs.js'
import {
	assertJsonValue,
	type JsonCompatible,
	type JsonValue
} from './validation.js'

export type StepCodec<T> = {
	encode(value: T): JsonValue
	decode(value: unknown): T
}
export type JobStep = {
	run<T>(
		id: string,
		fn: () => Promise<T> | T,
		...check: [Exclude<T, void>] extends [JsonCompatible<Exclude<T, void>>]
			? []
			: [never]
	): Promise<T>
	run<T>(id: string, fn: () => Promise<T> | T, codec: StepCodec<T>): Promise<T>
}
export type StepStore = {
	get(id: string): StepCacheEntry | undefined
	set(id: string, output: unknown): Promise<void>
}
export function serializeStepOutput(stepId: string, value: unknown): unknown {
	try {
		assertJsonValue(value, true)
		return value === undefined ? undefined : JSON.parse(JSON.stringify(value))
	} catch (error) {
		throw new StepSerializationError(stepId, { cause: error })
	}
}
export function createStepRunner(options: {
	store: StepStore
	span: JobSpan
}): JobStep {
	const pending = new Map<string, Promise<unknown>>()
	function event(name: string, id: string) {
		try {
			options.span.addEvent(name, { 'step.id': id })
		} catch {
			/* Instrumentation is observational. */
		}
	}
	async function run<T>(
		id: string,
		fn: () => Promise<T> | T,
		codec?: StepCodec<T>
	): Promise<T> {
		if (typeof id !== 'string' || !id.length)
			throw new NonRetriableError('Step id must be a non-empty string')
		const decode = (output: unknown): T => {
			if (codec) return codec.decode(serializeStepOutput(id, output))
			return serializeStepOutput(id, output) as T
		}
		const cached = options.store.get(id)
		if (cached) {
			event('step.cache_hit', id)
			return decode(cached.isVoid ? undefined : cached.output)
		}
		let work = pending.get(id)
		if (!work) {
			work = (async () => {
				try {
					const result = await fn()
					const output = serializeStepOutput(
						id,
						codec ? codec.encode(result) : result
					)
					if (codec) decode(output)
					await options.store.set(id, output)
					event('step.completed', id)
					return output
				} catch (error) {
					event('step.failed', id)
					throw error
				}
			})()
			pending.set(id, work)
			void work.finally(() => pending.delete(id)).catch(() => {})
		}
		return decode(await work)
	}
	return { run }
}
function entry(output: unknown): StepCacheEntry {
	return output === undefined ? { output: null, isVoid: true } : { output }
}
export function createMemoryStepStore(initial: StepCache = {}): StepStore {
	const cache = new Map(Object.entries(initial))
	return {
		get: (id) => cache.get(id),
		async set(id, output) {
			cache.set(id, entry(output))
		}
	}
}
export function createPgStepStore(options: {
	helpers: JobHelpers
	schema: string
	jobId: string
	rawPayload: unknown
}): StepStore {
	const cache = new Map(Object.entries(extractStepCache(options.rawPayload)))
	let writes: Promise<void> = Promise.resolve()
	return {
		get: (id) => cache.get(id),
		set(id, output) {
			const checkpoint = entry(output)
			const write = async () => {
				await writeCheckpoint(
					options.helpers,
					options.schema,
					options.jobId,
					id,
					checkpoint
				)
				cache.set(id, checkpoint)
			}
			const result = writes.then(write, write)
			writes = result.catch(() => {})
			return result
		}
	}
}
