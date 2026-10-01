import { assertUniqueQueueNames } from './define-queues.js'
import type {
	JobContext,
	InlineJobContext,
	QueueContract,
	QueueHandlers
} from './queue.js'
import { UnknownQueueError } from './errors.js'

/** Internal handler erasure; public APIs retain their registry-specific signatures. */
export type NormalizedQueue = QueueContract & {
	processFn: (payload: unknown, ctx: JobContext) => Promise<void> | void
	initFn?: (ctx: JobContext) => Promise<readonly unknown[]> | readonly unknown[]
}

/** Single erasure boundary for inline definitions and shared contracts. */
export function normalizeWorkerQueues<T extends readonly QueueContract[]>(
	queues: T,
	handlers?: QueueHandlers<T>
): NormalizedQueue[] {
	assertUniqueQueueNames(queues)
	if (handlers !== undefined && (!handlers || typeof handlers !== 'object'))
		throw new TypeError('A handlers registry must be an object')
	if (handlers !== undefined && queues.some((queue) => 'processFn' in queue))
		throw new TypeError(
			'Use inline processFn definitions or a handlers registry, not both'
		)
	const registered = new Set<QueueContract>(queues)
	const available = queues.map((queue) => queue.name)
	function referenceContext(ctx: JobContext): InlineJobContext {
		function name(queue: QueueContract): string {
			if (!registered.has(queue))
				throw new UnknownQueueError(queue.name, available)
			return queue.name
		}
		return {
			...ctx,
			createJob: (queue, ...[data, options]) =>
				ctx.createJob(name(queue), data, options),
			createJobs: (queue, data, options) =>
				ctx.createJobs(name(queue), data, options)
		}
	}
	return queues.map((queue) => {
		const configured =
			handlers === undefined
				? queue
				: Object.hasOwn(handlers, queue.name)
					? (handlers as Record<string, unknown>)[queue.name]
					: undefined
		const value =
			typeof configured === 'function' ? { processFn: configured } : configured
		if (
			!value ||
			typeof value !== 'object' ||
			!('processFn' in value) ||
			typeof value.processFn !== 'function'
		)
			throw new TypeError(`Queue "${queue.name}" requires a processFn`)
		if (queue.cron === undefined && !queue.inputSchema)
			throw new TypeError(
				`Queue "${queue.name}" requires an inputSchema or cron schedule`
			)
		if (
			queue.cron !== undefined &&
			queue.inputSchema &&
			(!('initFn' in value) || typeof value.initFn !== 'function')
		)
			throw new TypeError(`Cron-init queue "${queue.name}" requires an initFn`)
		if (
			(queue.cron === undefined || !queue.inputSchema) &&
			'initFn' in value &&
			value.initFn !== undefined
		)
			throw new TypeError(
				`initFn requires a cron schedule and inputSchema for "${queue.name}"`
			)
		const processFn = value.processFn as (
			payload: unknown,
			ctx: JobContext | InlineJobContext
		) => Promise<void> | void
		const initFn = ('initFn' in value ? value.initFn : undefined) as
			| ((
					ctx: JobContext | InlineJobContext
			  ) => Promise<readonly unknown[]> | readonly unknown[])
			| undefined
		return {
			...queue,
			processFn:
				handlers === undefined
					? (payload, ctx) => processFn(payload, referenceContext(ctx))
					: processFn,
			...(initFn
				? {
						initFn:
							handlers === undefined
								? (ctx: JobContext) => initFn(referenceContext(ctx))
								: initFn
					}
				: {})
		}
	})
}
