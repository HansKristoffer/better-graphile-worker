import type { CronItemOptions, CronMatcher, JobHelpers } from 'graphile-worker'
import type z from 'zod'
import type { JobLogger, JobSpan } from './hooks.js'
import type { ProgressOf } from './progress.js'
import type { JobStep } from './steps.js'
import type { Duration } from './job-options.js'
import type {
	CreateJobFn,
	CreateJobsFn,
	CreateJobForQueueFn,
	CreateJobsForQueueFn,
	InferInput,
	InferPayload
} from './types.js'

export type { JobLogger, JobSpan }

export type CronSchedule = string | readonly string[] | CronMatcher

export type QueueCronOptions = Pick<
	CronItemOptions,
	| 'backfillPeriod'
	| 'maxAttempts'
	| 'queueName'
	| 'priority'
	| 'jobKey'
	| 'jobKeyMode'
> & {
	identifier?: string
}

/** Per-job options derived from the wire input; explicit call options take precedence. */
export type DerivedJobOptions = {
	queueName?: string
	jobKey?: string
	jobKeyMode?: 'replace' | 'preserve_run_at' | 'unsafe_dedupe'
	priority?: number
	flags?: readonly string[]
	idempotencyKey?: string
	idempotencyTtl?: Duration
}

export type JobCronMeta = {
	ts: Date
	backfilled?: boolean
}

export type ContinueOptions = {
	/** When the continuation may start; defaults to now. */
	runAt?: Date | string
}

export function parseContinueRunAt(
	options: ContinueOptions | undefined
): Date | undefined {
	if (options?.runAt === undefined) return undefined
	const runAt = new Date(options.runAt)
	if (!Number.isFinite(runAt.getTime()))
		throw new RangeError('runAt must be a valid timestamp')
	return runAt
}

/** Context object passed to process and init functions */
export type JobContext<
	TQueues extends readonly QueueContract[] = readonly QueueContract[],
	TName extends string = string
> = {
	jobId: string
	queue: TName
	attempt: number
	maxAttempts: number
	logger: JobLogger
	span: JobSpan
	helpers: JobHelpers
	/**
	 * Aborts on graceful shutdown, or with a `JobTimeoutError` reason when the attempt
	 * exceeds its queue's `timeout`.
	 */
	signal: AbortSignal
	createJob: CreateJobFn<TQueues>
	createJobs: CreateJobsFn<TQueues>
	cron?: JobCronMeta | undefined
	step: JobStep
	/** Progress readable from outside the worker; typed by the queue's `progressSchema`. */
	progress: ProgressOf<
		ProgressSchemaOf<Extract<TQueues[number], { name: TName }>>
	>
	/**
	 * End this run successfully and enqueue the same job again (same input, lane, key,
	 * priority, flags, steps and progress, fresh attempts). Code after it never runs.
	 */
	continue(options?: ContinueOptions): Promise<never>
}

export type QueueContract<
	TName extends string = string,
	TSchema extends z.ZodType | undefined = z.ZodType | undefined,
	TProgress extends z.ZodType | undefined = z.ZodType | undefined
> = {
	readonly name: TName
	readonly inputSchema?: TSchema
	/** Validates `ctx.progress.set()` values; its output must be JSON. */
	readonly progressSchema?: TProgress
	readonly maxAttempts?: number
	readonly priority?: number
	readonly flags?: readonly string[]
	/**
	 * Fail an attempt that runs longer than this many milliseconds. Each retry and
	 * continuation starts a fresh clock. Overrides the worker's `defaultTimeout`.
	 */
	readonly timeout?: number
	/** Serialize jobs: `true` uses the queue name, a string is a custom Graphile `queueName`. */
	readonly serial?: boolean | string
	readonly cron?: CronSchedule
	readonly cronOptions?: Readonly<QueueCronOptions>
	/** Synchronous, pure: derive a lane, key, priority or flags from each job's wire input. */
	deriveJobOptions?(
		input: TSchema extends z.ZodType ? z.input<NoInfer<TSchema>> : never
	): DerivedJobOptions
}

/** A queue's `progressSchema`, or undefined when it has none. */
export type ProgressSchemaOf<Q> = Q extends {
	readonly progressSchema?: infer P extends z.ZodType | undefined
}
	? P
	: undefined

/** Shared contracts cannot carry competing inline handler implementations. */
export type HandlerFreeQueueContract<Q extends QueueContract = QueueContract> =
	Q & { readonly processFn?: never; readonly initFn?: never }

export type RegularQueueContract<
	S extends z.ZodType,
	N extends string = string,
	P extends z.ZodType | undefined = undefined
> = QueueContract<N, S, P> & {
	readonly inputSchema: S
	readonly cron?: never
	readonly cronOptions?: never
}
export type CronQueueContract<
	N extends string = string,
	P extends z.ZodType | undefined = undefined
> = QueueContract<N, undefined, P> & {
	readonly cron: CronSchedule
	readonly inputSchema?: never
	readonly deriveJobOptions?: never
}
export type CronInitQueueContract<
	S extends z.ZodType,
	N extends string = string,
	P extends z.ZodType | undefined = undefined
> = QueueContract<N, S, P> & {
	readonly cron: CronSchedule
	readonly inputSchema: S
}

/** Inline handlers enqueue through queue references to preserve schema inference. */
export type InlineJobContext<
	TName extends string = string,
	TProgress = ProgressOf<undefined>
> = Omit<
	JobContext<readonly QueueContract[], TName>,
	'createJob' | 'createJobs' | 'progress'
> & {
	createJob: CreateJobForQueueFn
	createJobs: CreateJobsForQueueFn
	progress: TProgress
}

type InlineProcessHandler<
	S extends z.ZodType | undefined,
	N extends string,
	P extends z.ZodType | undefined
> = (
	payload: S extends z.ZodType ? z.output<S> : undefined,
	ctx: InlineJobContext<N, ProgressOf<P>>
) => Promise<void> | void

export type RegularQueue<
	S extends z.ZodType,
	N extends string = string,
	P extends z.ZodType | undefined = undefined
> = RegularQueueContract<S, N, P> & {
	readonly processFn: InlineProcessHandler<NoInfer<S>, NoInfer<N>, NoInfer<P>>
	readonly initFn?: never
}
export type CronQueue<
	N extends string = string,
	P extends z.ZodType | undefined = undefined
> = CronQueueContract<N, P> & {
	readonly processFn: InlineProcessHandler<undefined, NoInfer<N>, NoInfer<P>>
	readonly initFn?: never
}
export type CronInitQueue<
	S extends z.ZodType,
	N extends string = string,
	P extends z.ZodType | undefined = undefined
> = CronInitQueueContract<S, N, P> & {
	readonly processFn: InlineProcessHandler<NoInfer<S>, NoInfer<N>, NoInfer<P>>
	readonly initFn: (
		ctx: InlineJobContext<`${NoInfer<N>}_cron-init`>
	) => Promise<readonly z.input<NoInfer<S>>[]> | readonly z.input<NoInfer<S>>[]
}

/** Handler presence only; concrete definitions retain their schema and context. */
export type RunnableQueue = QueueContract & {
	readonly processFn: (...args: never[]) => unknown
}

/** Validate schema/handler agreement even for manually constructed definitions. */
type InlineQueueDefinition<Q extends QueueContract> = Q extends {
	cron: CronSchedule
	inputSchema: infer S extends z.ZodType
}
	? CronInitQueue<S, Q['name'], ProgressSchemaOf<Q>>
	: Q extends { inputSchema: infer S extends z.ZodType }
		? RegularQueue<S, Q['name'], ProgressSchemaOf<Q>>
		: Q extends { cron: CronSchedule }
			? CronQueue<Q['name'], ProgressSchemaOf<Q>>
			: never

export type InlineQueues<T extends readonly QueueContract[]> = {
	[K in keyof T]: InlineQueueDefinition<T[K]>
}

/** Define a queue with inferred inline handlers, or a shared handler-free contract. */
export function defineQueue<
	S extends z.ZodType,
	const N extends string,
	P extends z.ZodType | undefined = undefined
>(config: RegularQueue<S, N, P>): RegularQueue<S, N, P>
export function defineQueue<
	const N extends string,
	P extends z.ZodType | undefined = undefined
>(config: CronQueue<N, P>): CronQueue<N, P>
export function defineQueue<
	S extends z.ZodType,
	const N extends string,
	P extends z.ZodType | undefined = undefined
>(config: CronInitQueue<S, N, P>): CronInitQueue<S, N, P>
export function defineQueue<
	S extends z.ZodType,
	const N extends string,
	P extends z.ZodType | undefined = undefined
>(
	config: HandlerFreeQueueContract<RegularQueueContract<S, N, P>>
): RegularQueueContract<S, N, P>
export function defineQueue<
	const N extends string,
	P extends z.ZodType | undefined = undefined
>(
	config: HandlerFreeQueueContract<CronQueueContract<N, P>>
): CronQueueContract<N, P>
export function defineQueue<
	S extends z.ZodType,
	const N extends string,
	P extends z.ZodType | undefined = undefined
>(
	config: HandlerFreeQueueContract<CronInitQueueContract<S, N, P>>
): CronInitQueueContract<S, N, P>
export function defineQueue(config: QueueContract): QueueContract {
	return config
}

type ProcessHandler<
	Q extends QueueContract,
	T extends readonly QueueContract[]
> = (
	payload: InferPayload<Q>,
	ctx: JobContext<T, Q['name']>
) => Promise<void> | void
export type QueueHandlers<T extends readonly QueueContract[]> = {
	[Q in T[number] as Q['name']]: Q extends {
		cron: CronSchedule
		inputSchema: z.ZodType
	}
		? {
				processFn: ProcessHandler<Q, T>
				initFn: (
					ctx: JobContext<T, `${Q['name']}_cron-init`>
				) => Promise<readonly InferInput<Q>[]> | readonly InferInput<Q>[]
			}
		: ProcessHandler<Q, T> | { processFn: ProcessHandler<Q, T> }
}

/** Suffix automatically appended to cron init task names. */
export const CRON_INIT_SUFFIX = '_cron-init' as const

/** Preserve concrete schemas and names when narrowing contract variants. */
type NarrowQueue<Q, Shape> = Q extends Shape ? Q : Q & Readonly<Shape>

export function isCronInitQueue<Q extends QueueContract>(
	queue: Q
): queue is NarrowQueue<Q, { cron: CronSchedule; inputSchema: z.ZodType }> {
	return queue.cron !== undefined && queue.inputSchema !== undefined
}

export function isRegularQueue<Q extends QueueContract>(
	queue: Q
): queue is NarrowQueue<Q, { inputSchema: z.ZodType; cron?: undefined }> {
	return (
		'inputSchema' in queue &&
		queue.inputSchema !== undefined &&
		queue.cron === undefined
	)
}

export function isCronQueue<Q extends QueueContract>(
	queue: Q
): queue is NarrowQueue<Q, { cron: CronSchedule; inputSchema?: undefined }> {
	return queue.cron !== undefined && queue.inputSchema === undefined
}

export function getQueueType(
	queue: QueueContract
): 'regular' | 'cron' | 'cron-init' {
	if (queue.cron !== undefined && queue.inputSchema !== undefined)
		return 'cron-init'
	if (isCronQueue(queue)) return 'cron'
	return 'regular'
}

export function hasInputSchema<Q extends QueueContract>(
	queue: Q
): queue is NarrowQueue<Q, { inputSchema: z.ZodType }> {
	return 'inputSchema' in queue && queue.inputSchema !== undefined
}

export function formatCronSchedule(
	cron: CronSchedule | undefined
): string | null {
	if (cron === undefined) return null
	if (typeof cron === 'string') return cron
	if (typeof cron === 'function') return '[function]'
	return cron.join(', ')
}

export function resolveSerialQueueName(
	serial: boolean | string | undefined,
	queueName: string
): string | undefined {
	if (serial === true) return queueName
	if (typeof serial === 'string') return serial
	return undefined
}
