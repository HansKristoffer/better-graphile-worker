import type z from 'zod'
import type { QueueContract } from './queue.js'
import type { BatchJobOptions, JobOptions } from './job-options.js'
import type { PayloadEnvelope } from './payload.js'

export type { JobOptions, BatchJobOptions }
export type QueueName<TQueues extends readonly QueueContract[]> =
	TQueues[number]['name']
type ExtractQueue<
	T extends string,
	TQueues extends readonly QueueContract[]
> = Extract<TQueues[number], { name: T }>

type SchemaValues<S, Kind extends 'input' | 'output'> = S extends z.ZodType
	? Kind extends 'input'
		? z.input<S>
		: z.output<S>
	: never
export type InferInput<Q> = Q extends { inputSchema: infer S extends z.ZodType }
	? z.input<S>
	: Q extends { inputSchema?: infer S }
		? SchemaValues<S, 'input'> | undefined
		: undefined
export type InferPayload<Q> = Q extends {
	inputSchema: infer S extends z.ZodType
}
	? z.output<S>
	: Q extends { inputSchema?: infer S }
		? SchemaValues<S, 'output'> | undefined
		: undefined

export type QueueInput<
	T extends string,
	TQueues extends readonly QueueContract[]
> = InferInput<ExtractQueue<T, TQueues>>
export type QueuePayload<
	T extends string,
	TQueues extends readonly QueueContract[]
> = InferPayload<ExtractQueue<T, TQueues>>
export type InputsOf<TQueues extends readonly QueueContract[]> = {
	[Q in TQueues[number] as Q['name']]: InferInput<Q>
}
export type PayloadsOf<TQueues extends readonly QueueContract[]> = {
	[Q in TQueues[number] as Q['name']]: InferPayload<Q>
}
/** Raw Graphile task payloads must use the current envelope around producer input. */
export type TasksOf<TQueues extends readonly QueueContract[]> = {
	[Q in TQueues[number] as Q['name']]: PayloadEnvelope<InferInput<Q>>
} & {
	[Q in Extract<
		TQueues[number],
		{ cron: unknown; inputSchema: z.ZodType }
	> as `${Q['name']}_cron-init`]: PayloadEnvelope<undefined>
}
type JobInputArgs<Input> = undefined extends Input
	? [data?: Input, options?: JobOptions]
	: [data: Input, options?: JobOptions]
export type EnqueueArgs<Q> = Q extends QueueContract
	? [queueName: Q['name'], ...args: JobInputArgs<InferInput<Q>>]
	: never
export type CreateJobFn<TQueues extends readonly QueueContract[]> =
	string extends QueueName<TQueues>
		? (
				queueName: string,
				data?: unknown,
				options?: JobOptions
			) => Promise<string | null>
		: (...args: EnqueueArgs<TQueues[number]>) => Promise<string | null>
type BatchArgs<Q> = Q extends { inputSchema: z.ZodType }
	? [
			queueName: Q extends QueueContract ? Q['name'] : never,
			data: readonly InferInput<Q>[],
			options?: BatchJobOptions
		]
	: never
export type CreateJobsFn<TQueues extends readonly QueueContract[]> =
	string extends QueueName<TQueues>
		? (
				queueName: string,
				data: readonly unknown[],
				options?: BatchJobOptions
			) => Promise<string[]>
		: (...args: BatchArgs<TQueues[number]>) => Promise<string[]>

/** A union reference must accept the input regardless of which queue it selects. */
type ReferenceInput<Q> = (
	Q extends QueueContract
		? (input: InferInput<Q>) => void
		: never
) extends (input: infer Input) => void
	? Input
	: never

/** Infer input from the queue argument alone, including schema transforms/defaults. */
export type CreateJobForQueueFn = <const Q extends QueueContract>(
	queue: Q,
	...args: JobInputArgs<NoInfer<ReferenceInput<Q>>>
) => Promise<string | null>

export type CreateJobsForQueueFn = <
	const Q extends QueueContract & { inputSchema: z.ZodType }
>(
	queue: Q,
	data: readonly NoInfer<ReferenceInput<Q>>[],
	options?: BatchJobOptions
) => Promise<string[]>
export type JobsApi<TQueues extends readonly QueueContract[]> =
	string extends QueueName<TQueues>
		? Readonly<
				Record<
					string,
					(data?: unknown, options?: JobOptions) => Promise<string | null>
				>
			>
		: {
				readonly [Q in TQueues[number] as Q['name']]: (
					...args: JobInputArgs<InferInput<Q>>
				) => Promise<string | null>
			}
export type CronQueueName<TQueues extends readonly QueueContract[]> = Extract<
	TQueues[number],
	{ cron: unknown }
>['name']
export type CronInitQueueName<TQueues extends readonly QueueContract[]> =
	Extract<TQueues[number], { cron: unknown; inputSchema: z.ZodType }>['name']
