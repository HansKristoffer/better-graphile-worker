import type {
	CronQueueConfig,
	QueueAny,
	RegularQueueConfig,
	CronInitQueueConfig
} from './create-queue'
import type z from 'zod'
import type { JobOptions } from './job-options'

export type { JobOptions }

export type QueueName<TQueues extends readonly QueueAny[]> =
	TQueues[number]['name']

export type QueueNames<TQueues extends readonly QueueAny[]> = QueueName<TQueues>

type ExtractQueue<
	T extends string,
	TQueues extends readonly QueueAny[]
> = Extract<TQueues[number], { name: T }>

export type QueueInput<T extends string, TQueues extends readonly QueueAny[]> =
	ExtractQueue<T, TQueues> extends CronQueueConfig<string>
		? undefined
		: ExtractQueue<T, TQueues> extends RegularQueueConfig<infer S, string>
			? z.input<S>
			: ExtractQueue<T, TQueues> extends CronInitQueueConfig<infer S, string>
				? z.input<S>
				: never

export type QueuePayload<
	T extends string,
	TQueues extends readonly QueueAny[]
> =
	ExtractQueue<T, TQueues> extends CronQueueConfig<string>
		? undefined
		: ExtractQueue<T, TQueues> extends RegularQueueConfig<infer S, string>
			? z.output<S>
			: ExtractQueue<T, TQueues> extends CronInitQueueConfig<infer S, string>
				? z.output<S>
				: never

export type InferInput<Q> = Q extends { inputSchema: infer S }
	? S extends z.ZodType
		? z.input<S>
		: undefined
	: undefined

export type InferPayload<Q> = Q extends { inputSchema: infer S }
	? S extends z.ZodType
		? z.output<S>
		: undefined
	: undefined

export type TasksOf<TQueues extends readonly QueueAny[]> = {
	[Q in TQueues[number] as Q['name'] & string]: QueuePayload<Q['name'], TQueues>
}

type CreateJobArgs<T extends string, TQueues extends readonly QueueAny[]> = [
	QueueInput<T, TQueues>
] extends [undefined]
	? [data?: undefined, options?: JobOptions]
	: [data: QueueInput<T, TQueues>, options?: JobOptions]

export type CreateJobFn<TQueues extends readonly QueueAny[]> = <
	T extends QueueName<TQueues>
>(
	queueName: T,
	...args: CreateJobArgs<T, TQueues>
) => Promise<string | null>

export type CreateJobsFn<TQueues extends readonly QueueAny[]> = <
	T extends QueueName<TQueues>
>(
	queueName: T,
	data: QueueInput<T, TQueues> extends undefined
		? never
		: QueueInput<T, TQueues>[],
	options?: JobOptions
) => Promise<string[]>

export type JobsApi<TQueues extends readonly QueueAny[]> = {
	[Q in TQueues[number] as Q['name'] & string]: [
		QueueInput<Q['name'], TQueues>
	] extends [undefined]
		? (data?: undefined, options?: JobOptions) => Promise<string | null>
		: (
				data: QueueInput<Q['name'], TQueues>,
				options?: JobOptions
			) => Promise<string | null>
}

export type CronQueueName<TQueues extends readonly QueueAny[]> = Extract<
	TQueues[number],
	{ cron: unknown }
>['name']
