import type z from 'zod'
import { assertJsonValue, type JsonValue } from './validation.js'

/** Progress of the running job, persisted in its PostgreSQL row. */
export type JobProgress<TValue = JsonValue, TInput = TValue> = {
	/** The latest value set in this run, or the value persisted by an earlier attempt. */
	get(): TValue | undefined
	/** Validate and keep a value; writes are throttled and coalesced. */
	set(value: TInput): Promise<void>
	/** Persist the latest value now. */
	flush(): Promise<void>
}

type ProgressTypes<P, S = Exclude<P, undefined>> = [S] extends [never]
	? [JsonValue, JsonValue]
	: z.ZodType extends S
		? [JsonValue, JsonValue]
		: S extends z.ZodType
			? [z.output<S>, z.input<S>]
			: [JsonValue, JsonValue]
/** Progress for a queue's optional `progressSchema`; untyped queues use JSON values. */
export type ProgressOf<P> = JobProgress<
	ProgressTypes<P>[0],
	ProgressTypes<P>[1]
>
export type ProgressValueOf<P> = ProgressTypes<P>[0]

/** `getJobProgress` result; progress is undefined until the job sets it. */
export type JobProgressSnapshot<T = unknown> = { progress: T | undefined }

/** The interval between persisted writes; `flush()` writes immediately. */
const PROGRESS_WRITE_INTERVAL_MS = 1000

export type ProgressRunner = JobProgress<unknown, unknown> & {
	/** Stop accepting values, optionally persist the latest one, and await writes. */
	close(persist: boolean): Promise<void>
}

/**
 * The first value is written at once; later values within the interval coalesce into one
 * trailing write. A failed write rejects the next `set` or `flush` and is retried.
 */
export function createProgressRunner(options: {
	write: (value: unknown) => Promise<void>
	initial: unknown
	schema?: z.ZodType | undefined
}): ProgressRunner {
	let value = options.initial
	let dirty = false
	let queued = false
	let closed = false
	let lastWrite = Number.NEGATIVE_INFINITY
	let timer: ReturnType<typeof setTimeout> | undefined
	let writes: Promise<void> = Promise.resolve()
	let failure: { error: unknown } | undefined

	function write(): Promise<void> {
		clearTimeout(timer)
		timer = undefined
		if (!dirty || queued) return writes
		queued = true
		lastWrite = Date.now()
		writes = writes
			.then(async () => {
				queued = false
				if (!dirty) return
				dirty = false
				await options.write(value)
			})
			.catch((error: unknown) => {
				failure = { error }
				dirty = true
			})
		return writes
	}
	function rethrow() {
		if (!failure) return
		const { error } = failure
		failure = undefined
		throw error
	}
	function assertOpen() {
		if (closed) throw new Error('ctx.progress was used after the job finished')
	}

	return {
		get: () => value,
		async set(input) {
			assertOpen()
			rethrow()
			const next = options.schema
				? await options.schema.parseAsync(input)
				: input
			assertJsonValue(next)
			assertOpen()
			value = JSON.parse(JSON.stringify(next))
			dirty = true
			if (timer) return
			const wait = lastWrite + PROGRESS_WRITE_INTERVAL_MS - Date.now()
			if (wait <= 0) void write()
			else {
				timer = setTimeout(write, wait)
				timer.unref?.()
			}
		},
		async flush() {
			assertOpen()
			await write()
			rethrow()
		},
		async close(persist) {
			closed = true
			if (persist) await write()
			clearTimeout(timer)
			timer = undefined
			await writes
			rethrow()
		}
	}
}
