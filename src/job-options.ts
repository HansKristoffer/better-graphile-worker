import type { TaskSpec } from 'graphile-worker'

export type JobOptions = Pick<
	TaskSpec,
	| 'priority'
	| 'runAt'
	| 'queueName'
	| 'maxAttempts'
	| 'jobKey'
	| 'jobKeyMode'
	| 'flags'
> & {
	/** Override worker-level enqueue validation. Defaults to the worker/client setting. */
	validateOnEnqueue?: boolean
}

export type StopOptions = {
	timeout?: number
}
