import type { JobHelpers } from 'graphile-worker'
import type z from 'zod'
import type { JobSpan } from './hooks.js'
import type { QueueContract } from './queue.js'
import type { InferOutput, ReferenceInput } from './types.js'
import {
	assertWaitOptions,
	decodeWaitCheckpoint,
	type WaitCheckpoint,
	type WaitOptions,
	type WaitResult
} from './waits.js'
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
type WaitArgs<Input> = undefined extends Input
	? [input?: Input, options?: WaitOptions]
	: [input: Input, options?: WaitOptions]
export type JobStep = {
	run<T>(
		id: string,
		fn: () => Promise<T> | T,
		...check: [Exclude<T, void>] extends [JsonCompatible<Exclude<T, void>>]
			? []
			: [never]
	): Promise<T>
	run<T>(id: string, fn: () => Promise<T> | T, codec: StepCodec<T>): Promise<T>
	/**
	 * Enqueue one child job and return its final state. Until the child finishes, the
	 * run ends and the job waits without spending an attempt; it then runs again from
	 * the top and this call returns the checkpointed result. Code before it repeats.
	 */
	triggerAndWait<const Q extends QueueContract>(
		id: string,
		queue: Q,
		...args: WaitArgs<NoInfer<ReferenceInput<Q>>>
	): Promise<WaitResult<InferOutput<Q>>>
	/** Like `triggerAndWait` for a batch, enqueued atomically; results keep input order. */
	batchTriggerAndWait<
		const Q extends QueueContract & { inputSchema: z.ZodType }
	>(
		id: string,
		queue: Q,
		inputs: readonly NoInfer<ReferenceInput<Q>>[],
		options?: WaitOptions
	): Promise<WaitResult<InferOutput<Q>>[]>
}
/**
 * Enqueues a wait's children once per id and ends the run while they are unfinished.
 * `persisted` means the backend already wrote the checkpoint.
 */
export type WaitBackend = {
	wait(
		id: string,
		queue: QueueContract,
		inputs: readonly unknown[],
		batch: boolean,
		options: WaitOptions | undefined
	): Promise<{ checkpoint: WaitCheckpoint; persisted: boolean }>
}
export type StepStore = {
	get(id: string): StepCacheEntry | undefined
	set(id: string, output: unknown): Promise<void>
	/** Cache a checkpoint that was already written, such as a wait's results. */
	remember?(id: string, output: unknown): void
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
	/** Throws when the attempt may no longer start steps, e.g. after its timeout. */
	assertActive?: () => void
	waits?: WaitBackend | undefined
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
		options.assertActive?.()
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
	async function wait(
		id: string,
		queue: QueueContract,
		inputs: readonly unknown[],
		batch: boolean,
		waitOptions: WaitOptions | undefined
	): Promise<WaitResult<unknown>[]> {
		options.assertActive?.()
		if (typeof id !== 'string' || !id.length)
			throw new NonRetriableError('Step id must be a non-empty string')
		assertWaitOptions(waitOptions)
		const cached = options.store.get(id)
		if (cached) {
			event('step.cache_hit', id)
			return decodeWaitCheckpoint(id, queue, cached.output)
		}
		if (!inputs.length) return []
		const backend = options.waits
		if (!backend)
			throw new Error(
				'ctx.step.triggerAndWait is not available in this context'
			)
		let work = pending.get(id)
		if (!work) {
			work = (async () => {
				const { checkpoint, persisted } = await backend.wait(
					id,
					queue,
					inputs,
					batch,
					waitOptions
				)
				if (persisted && options.store.remember)
					options.store.remember(id, checkpoint)
				else await options.store.set(id, checkpoint)
				event('step.completed', id)
				return checkpoint
			})()
			pending.set(id, work)
			void work.finally(() => pending.delete(id)).catch(() => {})
		}
		return decodeWaitCheckpoint(id, queue, await work)
	}
	return {
		run,
		async triggerAndWait(id, queue, ...[input, waitOptions]) {
			const [result] = await wait(id, queue, [input], false, waitOptions)
			return result as WaitResult<never>
		},
		batchTriggerAndWait: (id, queue, inputs, waitOptions) =>
			wait(id, queue, inputs, true, waitOptions) as Promise<WaitResult<never>[]>
	}
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
		remember: (id, output) => cache.set(id, entry(output)),
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
