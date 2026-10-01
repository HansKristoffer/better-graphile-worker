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

type NameIssue<
	Q extends { name: string },
	SeenNames extends string,
	SeenTasks extends string
> = string extends Q['name']
	? never // Dynamic names are checked at runtime, without rejecting unrelated literals.
	: [Extract<Q['name'], '' | 'then'>] extends [never]
		? [Extract<Q['name'], SeenNames>] extends [never]
			? [Extract<Q['name'] | GeneratedTaskName<Q>, SeenTasks>] extends [never]
				? never
				: `Queue task name collision: ${Extract<Q['name'] | GeneratedTaskName<Q>, SeenTasks>}`
			: `Duplicate queue name: ${Extract<Q['name'], SeenNames>}`
		: `Invalid queue name: "${Extract<Q['name'], '' | 'then'>}"`

type QueueNamesIssue<
	T extends readonly { name: string }[],
	SeenNames extends string = never,
	SeenTasks extends string = never
> = T extends readonly [
	infer Head extends { name: string },
	...infer Rest extends { name: string }[]
]
	? [NameIssue<Head, SeenNames, SeenTasks>] extends [never]
		? QueueNamesIssue<
				Rest,
				SeenNames | (string extends Head['name'] ? never : Head['name']),
				| SeenTasks
				| (string extends Head['name'] ? never : Head['name'])
				| GeneratedTaskName<Head>
			>
		: NameIssue<Head, SeenNames, SeenTasks>
	: never

export type UniqueQueueNames<T extends readonly { name: string }[]> = [
	QueueNamesIssue<T>
] extends [never]
	? T
	: QueueNamesIssue<T>

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
	queues: UniqueQueueNames<T> & T
): T {
	assertUniqueQueueNames(queues)
	return queues
}
