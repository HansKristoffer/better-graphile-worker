export class NonRetriableError extends Error {
	constructor(message: string, options?: ErrorOptions) {
		super(message, options)
		this.name = 'NonRetriableError'
	}
}

/**
 * Throw from a handler to spend this attempt and run the next one no earlier than
 * `runAt`. Pass the original error as `cause`: its name and message are what logs,
 * hooks and Graphile's `last_error` record.
 */
export class RetryAtError extends Error {
	readonly runAt: Date
	constructor(runAt: Date | string, options?: ErrorOptions) {
		const date = new Date(runAt)
		if (!Number.isFinite(date.getTime()))
			throw new RangeError('runAt must be a valid timestamp')
		const cause = options?.cause
		super(
			cause instanceof Error
				? cause.message
				: cause === undefined
					? `Retry at ${date.toISOString()}`
					: String(cause),
			options
		)
		this.name = 'RetryAtError'
		this.runAt = date
	}
}

export class UnknownQueueError extends Error {
	constructor(
		readonly queueName: string,
		readonly available: readonly string[]
	) {
		super(
			available.length === 0
				? `Unknown queue "${queueName}". No queues are registered.`
				: `Unknown queue "${queueName}". Known queues: ${available.join(', ')}`
		)
		this.name = 'UnknownQueueError'
	}
}

export class JobValidationError extends NonRetriableError {
	constructor(
		readonly queueName: string,
		readonly issues: readonly { path: PropertyKey[]; message: string }[],
		subject: 'payload' | 'output' = 'payload'
	) {
		const details = issues
			.map((issue) => {
				const path =
					issue.path.length > 0 ? issue.path.map(String).join('.') : '(root)'
				return `${path}: ${issue.message}`
			})
			.join('; ')
		super(`Invalid ${subject} for queue "${queueName}": ${details}`)
		this.name = 'JobValidationError'
	}
}

export class StepPersistenceError extends Error {
	constructor(readonly jobId: string) {
		super(
			`Cannot checkpoint job "${jobId}": its worker no longer owns the lock`
		)
		this.name = 'StepPersistenceError'
	}
}

export class ShutdownTimeoutError extends Error {
	constructor(readonly timeout: number) {
		super(
			`Worker shutdown exceeded ${timeout}ms; shutdown is still in progress`
		)
		this.name = 'ShutdownTimeoutError'
	}
}

/**
 * `stop({ probeDatabase })` gave up waiting: the database stopped answering, so the
 * runners' queries can never finish. Shutdown continues in the background; end the pool
 * without awaiting it, since its checked-out clients may never be released.
 */
export class DatabaseUnreachableError extends Error {
	constructor(options?: ErrorOptions) {
		super(
			'Worker shutdown stopped waiting: the database is unreachable; shutdown is still in progress',
			options
		)
		this.name = 'DatabaseUnreachableError'
	}
}

/**
 * Graphile created fewer jobs than the enqueue asked for, typically because a worker
 * claimed an existing `jobKey` at the same moment. In a transaction, throwing rolls it
 * back; retry the transaction.
 */
export class EnqueueCountError extends Error {
	constructor(
		readonly queue: string,
		readonly expected: number,
		readonly actual: number
	) {
		super(
			`Enqueue of "${queue}" returned ${actual} of ${expected} jobs, likely a jobKey race; retry the transaction`
		)
		this.name = 'EnqueueCountError'
	}
}

/** A `required` enqueue was skipped because `shouldSkipEnqueue()` returned true. */
export class EnqueueSkippedError extends Error {
	constructor(readonly queue: string) {
		super(
			`Required enqueue of "${queue}" was skipped because shouldSkipEnqueue() returned true`
		)
		this.name = 'EnqueueSkippedError'
	}
}

/**
 * The worker's runner stopped although `stop()` was never called, for example after
 * Graphile shut its pool down on a database error; `cause` carries Graphile's reason when
 * it gave one. The worker logs it and starts a new runner; with `restart: false`,
 * `promise` rejects with it and the queues no longer run in this process.
 */
export class WorkerStoppedError extends Error {
	constructor(
		readonly runner: string | undefined,
		options?: ErrorOptions
	) {
		const subject = runner === undefined ? 'Worker' : `Runner "${runner}"`
		const cause = options?.cause
		super(
			`${subject} stopped without stop() being called${
				cause instanceof Error ? `: ${cause.message}` : ''
			}`,
			options
		)
		this.name = 'WorkerStoppedError'
	}
}

/**
 * An attempt ran longer than its queue's `timeout`. It follows the normal retry
 * rules; it is also the `ctx.signal.reason` handed to the timed-out handler.
 */
export class JobTimeoutError extends Error {
	constructor(readonly timeout: number) {
		super(`Job attempt exceeded its ${timeout}ms timeout`)
		this.name = 'JobTimeoutError'
	}
}

export class DuplicateQueueError extends Error {
	constructor(readonly queueName: string) {
		super(`Duplicate queue name "${queueName}"`)
		this.name = 'DuplicateQueueError'
	}
}

export class QueueNameCollisionError extends Error {
	constructor(readonly taskName: string) {
		super(`Queue task name "${taskName}" collides with another registered task`)
		this.name = 'QueueNameCollisionError'
	}
}

export class InvalidSchemaNameError extends Error {
	constructor(readonly schema: string) {
		super(
			`Invalid Graphile Worker schema name "${schema}". Use a simple SQL identifier of at most 63 characters (letters, digits, underscore).`
		)
		this.name = 'InvalidSchemaNameError'
	}
}

export class StepSerializationError extends NonRetriableError {
	constructor(
		readonly stepId: string,
		options?: ErrorOptions
	) {
		super(
			`Step "${stepId}" returned a value that is not JSON-serializable`,
			options
		)
		this.name = 'StepSerializationError'
	}
}

/** Internal: thrown by `ctx.continue()` so nothing after it runs; treated as success. */
export class JobContinuedSignal extends Error {
	constructor() {
		super('Job continued; code after ctx.continue() must not run')
		this.name = 'JobContinuedSignal'
	}
}

/**
 * A child job of `ctx.step.triggerAndWait` failed permanently. The result is
 * checkpointed like a step, so retrying the parent cannot change it: rethrowing it
 * fails the parent without further attempts. `cause` carries the child's error.
 */
export class ChildJobFailedError extends NonRetriableError {
	constructor(
		readonly jobId: string,
		readonly queueName: string,
		error: { name: string; message: string }
	) {
		const cause = new Error(error.message)
		cause.name = error.name
		super(`Child job ${jobId} of "${queueName}" failed: ${error.message}`, {
			cause
		})
		this.name = 'ChildJobFailedError'
	}
}

/**
 * A wait's `timeout` passed before all of its children finished. The children keep
 * running. Checkpointed like a step, so a retry of the parent throws it again.
 */
export class WaitTimeoutError extends NonRetriableError {
	constructor(
		readonly waitId: string,
		/** Child job ids that had not finished. */
		readonly pending: readonly string[]
	) {
		super(
			`Wait "${waitId}" timed out with ${pending.length} unfinished child job(s): ${pending.join(', ')}`
		)
		this.name = 'WaitTimeoutError'
	}
}

/**
 * Internal: thrown by a wait that ended the run until its children finish. The task
 * rethrows it so Graphile unlocks the job and its lane without deleting it.
 */
export class JobSuspendedSignal extends Error {
	constructor(readonly waitId: string) {
		super(`Waiting for the child jobs of "${waitId}"; not a failure`)
		this.name = 'JobSuspendedSignal'
		// Graphile logs every suspension as a failed task; a stack would only add noise.
		this.stack = ''
	}
}
