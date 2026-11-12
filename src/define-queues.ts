import { CRON_INIT_SUFFIX, type QueueContract } from './queue.js'
import { DuplicateQueueError, QueueNameCollisionError } from './errors.js'

type GeneratedTaskName<Q extends { name: string }> = Q extends {
	cron: NonNullable<QueueContract['cron']>
	inputSchema: NonNullable<QueueContract['inputSchema']>
}
	? string extends Q['name']
		? never
		: `${Q['name']}${typeof CRON_INIT_SUFFIX}`
	: never

type TaskNames<Q extends { name: string }> = Q['name'] | GeneratedTaskName<Q>

/** Each task name to the queues that claim it; two different claimants make a union. */
type TaskOwners<T extends readonly { name: string }[]> = {
	[Q in T[number] as string extends Q['name'] ? never : TaskNames<Q>]: Q
}

type CheckQueue<Q, Owners> = Q extends { name: infer N extends string }
	? string extends N
		? Q // Dynamic names are checked at runtime, without rejecting unrelated literals.
		: N extends '' | 'then'
			? `Invalid queue name: "${N}"`
			: [Owners[TaskNames<Q> & keyof Owners]] extends [Q]
				? Q
				: `Duplicate queue name or task collision: ${N}`
	: Q

/**
 * The tuple with an error message in place of each queue whose name is empty, reserved,
 * repeated or collides with a generated cron-init task. One mapped pass keyed by name, so
 * it stays linear in the number of queues. Two definitions with the same name and the
 * same type cannot be told apart by type (keying by position costs five times as much at
 * 400 queues), so `defineQueues` rejects them when it runs, at module load.
 */
export type UniqueQueueNames<T extends readonly { name: string }[]> = {
	[K in keyof T]: CheckQueue<T[K], TaskOwners<T>>
}

export function assertUniqueQueueNames(queues: readonly QueueContract[]): void {
	const names = new Set<string>()
	const taskNames = new Set<string>()

	for (const queue of queues) {
		if (!queue.name || queue.name === 'then')
			throw new Error(
				'Queue names must be non-empty and cannot be "then" (reserved for promise assimilation)'
			)
		if (names.has(queue.name)) {
			throw new DuplicateQueueError(queue.name)
		}
		names.add(queue.name)

		if (taskNames.has(queue.name)) {
			throw new QueueNameCollisionError(queue.name)
		}
		taskNames.add(queue.name)

		if (queue.cron !== undefined && queue.inputSchema !== undefined) {
			const initName = `${queue.name}${CRON_INIT_SUFFIX}`
			if (names.has(initName) || taskNames.has(initName)) {
				throw new QueueNameCollisionError(initName)
			}
			taskNames.add(initName)
		}
	}
}

/**
 * Preserve a tuple (no `as const` needed) and reject invalid names and task collisions.
 */
export function defineQueues<const T extends readonly QueueContract[]>(
	queues: T & UniqueQueueNames<T>
): T {
	assertUniqueQueueNames(queues)
	return queues
}
