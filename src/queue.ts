import type { CronItemOptions, CronMatcher, JobHelpers } from 'graphile-worker'
import type z from 'zod'
import type { JobLogger, JobSpan } from './hooks.js'
import type { JobStep } from './steps.js'
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
	signal: AbortSignal
	createJob: CreateJobFn<TQueues>
	createJobs: CreateJobsFn<TQueues>
	cron?: JobCronMeta | undefined
	step: JobStep
	/**
	 * End this run successfully and enqueue the same job again (same input, lane, key,
	 * priority, flags and steps, fresh attempts). Code after it never runs.
	 */
	continue(options?: ContinueOptions): Promise<never>
}

export type QueueContract<
	TName extends string = string,
	TSchema extends z.ZodType | undefined = z.ZodType | undefined
> = {
	readonly name: TName
	readonly inputSchema?: TSchema
	readonly maxAttempts?: number
	readonly priority?: number
	readonly flags?: readonly string[]
	/** Serialize jobs: `true` uses the queue name, a string is a custom Graphile `queueName`. */
	readonly serial?: boolean | string
	readonly cron?: CronSchedule
	readonly cronOptions?: Readonly<QueueCronOptions>
	/** Synchronous, pure: derive a lane, key, priority or flags from each job's wire input. */
	deriveJobOptions?(
		input: TSchema extends z.ZodType ? z.input<NoInfer<TSchema>> : never
	): DerivedJobOptions
}

/** Shared contracts cannot carry competing inline handler implementations. */
export type HandlerFreeQueueContract<Q extends QueueContract = QueueContract> =
	Q & { readonly processFn?: never; readonly initFn?: never }

export type RegularQueueContract<
	S extends z.ZodType,
	N extends string = string
> = QueueContract<N, S> & {
	readonly inputSchema: S
	readonly cron?: never
	readonly cronOptions?: never
}
export type CronQueueContract<N extends string = string> = QueueContract<
	N,
	undefined
> & {
	readonly cron: CronSchedule
	readonly inputSchema?: never
	readonly deriveJobOptions?: never
}
export type CronInitQueueContract<
	S extends z.ZodType,
	N extends string = string
> = QueueContract<N, S> & {
	readonly cron: CronSchedule
	readonly inputSchema: S
}

/** Inline handlers enqueue through queue references to preserve schema inference. */
export type InlineJobContext<TName extends string = string> = Omit<
	JobContext<readonly QueueContract[], TName>,
	'createJob' | 'createJobs'
> & {
	createJob: CreateJobForQueueFn
	createJobs: CreateJobsForQueueFn
}

type InlineProcessHandler<S extends z.ZodType | undefined, N extends string> = (
	payload: S extends z.ZodType ? z.output<S> : undefined,
	ctx: InlineJobContext<N>
) => Promise<void> | void

export type RegularQueue<
	S extends z.ZodType,
	N extends string = string
> = RegularQueueContract<S, N> & {
	readonly processFn: InlineProcessHandler<NoInfer<S>, NoInfer<N>>
	readonly initFn?: never
}
export type CronQueue<N extends string = string> = CronQueueContract<N> & {
	readonly processFn: InlineProcessHandler<undefined, NoInfer<N>>
	readonly initFn?: never
}
export type CronInitQueue<
	S extends z.ZodType,
	N extends string = string
> = CronInitQueueContract<S, N> & {
	readonly processFn: InlineProcessHandler<NoInfer<S>, NoInfer<N>>
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
	? CronInitQueue<S, Q['name']>
	: Q extends { inputSchema: infer S extends z.ZodType }
		? RegularQueue<S, Q['name']>
		: Q extends { cron: CronSchedule }
			? CronQueue<Q['name']>
			: never

export type InlineQueues<T extends readonly QueueContract[]> = {
	[K in keyof T]: InlineQueueDefinition<T[K]>
}

/** Define a queue with inferred inline handlers, or a shared handler-free contract. */
export function defineQueue<S extends z.ZodType, const N extends string>(
	config: RegularQueue<S, N>
): RegularQueue<S, N>
export function defineQueue<const N extends string>(
	config: CronQueue<N>
): CronQueue<N>
export function defineQueue<S extends z.ZodType, const N extends string>(
	config: CronInitQueue<S, N>
): CronInitQueue<S, N>
export function defineQueue<S extends z.ZodType, const N extends string>(
	config: HandlerFreeQueueContract<RegularQueueContract<S, N>>
): RegularQueueContract<S, N>
export function defineQueue<const N extends string>(
	config: HandlerFreeQueueContract<CronQueueContract<N>>
): CronQueueContract<N>
export function defineQueue<S extends z.ZodType, const N extends string>(
	config: HandlerFreeQueueContract<CronInitQueueContract<S, N>>
): CronInitQueueContract<S, N>
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
