import type { CronItemOptions, CronMatcher, JobHelpers } from 'graphile-worker'
import type z from 'zod'
import type { JobLogger, JobSpan } from './hooks'
import type { JobOptions } from './job-options'

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

export type JobCronMeta = {
	ts: Date
	backfilled?: boolean
}

export type CreateJobFnLike = (
	queueName: string,
	data?: unknown,
	options?: JobOptions
) => Promise<string | null>

export type CreateJobsFnLike = (
	queueName: string,
	data: unknown[],
	options?: JobOptions
) => Promise<string[]>

/** Context object passed to process and init functions */
export type JobContext = {
	jobId: string
	queue: string
	attempt: number
	maxAttempts: number
	logger: JobLogger
	span: JobSpan
	helpers: JobHelpers
	signal: AbortSignal
	createJob: CreateJobFnLike
	createJobs: CreateJobsFnLike
	cron?: JobCronMeta
}

export type QueueContract<TName extends string = string> = {
	name: TName
	inputSchema?: z.ZodType
	maxAttempts?: number
	priority?: number
	flags?: string[]
	/** Serialize jobs: `true` uses the queue name, a string is a custom Graphile `queueName`. */
	serial?: boolean | string
}

type BaseQueueConfig<TName extends string = string> = QueueContract<TName>

export type RegularQueueConfig<
	TInput extends z.ZodType,
	TName extends string = string
> = BaseQueueConfig<TName> & {
	inputSchema: TInput
	cron?: never
	cronOptions?: never
	initFn?: never
	processFn: (payload: z.output<TInput>, ctx: JobContext) => Promise<void>
}

export type CronQueueConfig<TName extends string = string> =
	BaseQueueConfig<TName> & {
		inputSchema?: never
		cron: CronSchedule
		cronOptions?: QueueCronOptions
		initFn?: never
		processFn: (payload: undefined, ctx: JobContext) => Promise<void>
	}

/** Suffix automatically appended to cron init task names */
export const CRON_INIT_SUFFIX = '_cron-init' as const

export type CronInitQueueConfig<
	TInput extends z.ZodType,
	TName extends string = string
> = BaseQueueConfig<TName> & {
	inputSchema: TInput
	cron: CronSchedule
	cronOptions?: QueueCronOptions
	initFn: (ctx: JobContext) => Promise<z.input<TInput>[]>
	processFn: (payload: z.output<TInput>, ctx: JobContext) => Promise<void>
}

export type QueueConfig<
	TInput extends z.ZodType = z.ZodType,
	TName extends string = string
> =
	| RegularQueueConfig<TInput, TName>
	| CronQueueConfig<TName>
	| CronInitQueueConfig<TInput, TName>

/**
 * Create a typed queue/task configuration for regular jobs (with input schema)
 */
export function createQueue<TInput extends z.ZodType, TName extends string>(
	config: RegularQueueConfig<TInput, TName>
): RegularQueueConfig<TInput, TName>

/**
 * Create a queue/task configuration for cron jobs (no input schema)
 */
export function createQueue<TName extends string>(
	config: CronQueueConfig<TName>
): CronQueueConfig<TName>

/**
 * Create a queue/task configuration for cron init jobs (initFn returns items for processFn)
 */
export function createQueue<TInput extends z.ZodType, TName extends string>(
	config: CronInitQueueConfig<TInput, TName>
): CronInitQueueConfig<TInput, TName>

/**
 * Create a queue/task configuration
 */
export function createQueue<TInput extends z.ZodType, TName extends string>(
	config: QueueConfig<TInput, TName>
): QueueConfig<TInput, TName> {
	return config
}

export type QueueAny = {
	name: string
	maxAttempts?: number
	priority?: number
	flags?: string[]
	serial?: boolean | string
	cron?: CronSchedule
	cronOptions?: QueueCronOptions
	inputSchema?: z.ZodType
	initFn?: (ctx: JobContext) => Promise<unknown[]>
	// biome-ignore lint/suspicious/noExplicitAny: Required for type-erased collections
	processFn: (payload: any, ctx: JobContext) => Promise<void>
}

export function isCronInitQueue(queue: QueueAny): queue is QueueAny & {
	cron: CronSchedule
	inputSchema: z.ZodType
	initFn: (ctx: JobContext) => Promise<unknown[]>
} {
	return (
		'initFn' in queue &&
		'cron' in queue &&
		queue.cron !== undefined &&
		queue.initFn !== undefined
	)
}

export function isRegularQueue(
	queue: QueueAny
): queue is QueueAny & { inputSchema: z.ZodType; cron?: undefined } {
	return (
		'inputSchema' in queue &&
		queue.inputSchema !== undefined &&
		!queue.cron &&
		!('initFn' in queue && queue.initFn !== undefined)
	)
}

export function isCronQueue(
	queue: QueueAny
): queue is QueueAny & { cron: CronSchedule; inputSchema?: undefined } {
	return 'cron' in queue && queue.cron !== undefined && !queue.inputSchema
}

export function getQueueType(
	queue: QueueAny
): 'regular' | 'cron' | 'cron-init' {
	if (isCronInitQueue(queue)) return 'cron-init'
	if (isCronQueue(queue)) return 'cron'
	return 'regular'
}

export function hasInputSchema(
	queue: QueueAny
): queue is QueueAny & { inputSchema: z.ZodType } {
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
