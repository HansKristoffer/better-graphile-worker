import type { TaskSpec } from 'graphile-worker'

export type JobOptions = Pick<
	TaskSpec,
	'priority' | 'runAt' | 'queueName' | 'maxAttempts' | 'jobKey' | 'jobKeyMode'
> & {
	flags?: readonly string[]
	/** Override worker-level enqueue validation. Defaults to the worker/client setting. */
	validateOnEnqueue?: boolean
}

export type StopOptions = {
	/** Reject after this many milliseconds; shutdown continues and can still be awaited. */
	timeout?: number
}

/** Graphile batch enqueue does not support jobKeyMode or a shared job key. */
export type BatchJobOptions = Omit<JobOptions, 'jobKey' | 'jobKeyMode'> & {
	jobKey?: never
	jobKeyMode?: never
}
