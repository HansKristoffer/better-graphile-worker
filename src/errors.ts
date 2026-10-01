export class NonRetriableError extends Error {
	constructor(message: string, options?: ErrorOptions) {
		super(message, options)
		this.name = 'NonRetriableError'
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
		readonly issues: readonly { path: PropertyKey[]; message: string }[]
	) {
		const details = issues
			.map((issue) => {
				const path =
					issue.path.length > 0 ? issue.path.map(String).join('.') : '(root)'
				return `${path}: ${issue.message}`
			})
			.join('; ')
		super(`Invalid payload for queue "${queueName}": ${details}`)
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
