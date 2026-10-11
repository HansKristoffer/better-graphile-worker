import type { TaskSpec } from 'graphile-worker'

/** Milliseconds, or a whole number with a unit: `'90s'`, `'30m'`, `'2h'`, `'7d'`. */
export type Duration = number | `${number}${'s' | 'm' | 'h' | 'd'}`

export type JobOptions = Pick<
	TaskSpec,
	'priority' | 'runAt' | 'queueName' | 'maxAttempts' | 'jobKey' | 'jobKeyMode'
> & {
	flags?: readonly string[]
	/** Override worker-level enqueue validation. Defaults to the worker/client setting. */
	validateOnEnqueue?: boolean
	/**
	 * Enqueue at most one job per key and task within `idempotencyTtl`, even after that
	 * job ran or completed. A duplicate returns the original job's id and enqueues nothing.
	 */
	idempotencyKey?: string
	/** How long an idempotency key holds, from its first enqueue. Defaults to 30 days. */
	idempotencyTtl?: Duration
}

export type StopOptions = {
	/** Reject after this many milliseconds; shutdown continues and can still be awaited. */
	timeout?: number
}

/** Graphile batch enqueue does not support jobKeyMode or a shared job key. */
export type BatchJobOptions = Omit<
	JobOptions,
	'jobKey' | 'jobKeyMode' | 'idempotencyKey'
> & {
	jobKey?: never
	jobKeyMode?: never
	idempotencyKey?: never
}
