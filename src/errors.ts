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
				const path = issue.path.length > 0 ? issue.path.join('.') : '(root)'
				return `${path}: ${issue.message}`
			})
			.join('; ')
		super(`Invalid payload for queue "${queueName}": ${details}`)
		this.name = 'JobValidationError'
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
			`Invalid Graphile Worker schema name "${schema}". Use a simple SQL identifier (letters, digits, underscore).`
		)
		this.name = 'InvalidSchemaNameError'
	}
}
