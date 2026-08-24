import {
	CRON_INIT_SUFFIX,
	isCronInitQueue,
	type QueueAny
} from './create-queue'
import { DuplicateQueueError, QueueNameCollisionError } from './errors'

type DuplicateName<
	T extends readonly { name: string }[],
	Seen extends string = never
> = T extends readonly [
	infer Head extends { name: string },
	...infer Rest extends { name: string }[]
]
	? Head['name'] extends Seen
		? Head['name']
		: DuplicateName<Rest, Seen | Head['name']>
	: never

export type UniqueQueueNames<T extends readonly { name: string }[]> = [
	DuplicateName<T>
] extends [never]
	? T
	: `Duplicate queue name: ${DuplicateName<T> & string}`

export function assertUniqueQueueNames(queues: readonly QueueAny[]): void {
	const names = new Set<string>()
	const taskNames = new Set<string>()

	for (const queue of queues) {
		if (names.has(queue.name)) {
			throw new DuplicateQueueError(queue.name)
		}
		names.add(queue.name)

		if (taskNames.has(queue.name)) {
			throw new QueueNameCollisionError(queue.name)
		}
		taskNames.add(queue.name)

		if (isCronInitQueue(queue)) {
			const initName = `${queue.name}${CRON_INIT_SUFFIX}`
			if (names.has(initName) || taskNames.has(initName)) {
				throw new QueueNameCollisionError(initName)
			}
			taskNames.add(initName)
		}
	}
}

/**
 * Preserve a tuple of queues (no `as const` needed) and reject duplicate names.
 */
export function defineQueues<const T extends readonly QueueAny[]>(
	queues: UniqueQueueNames<T> & T
): T {
	assertUniqueQueueNames(queues)
	return queues
}
