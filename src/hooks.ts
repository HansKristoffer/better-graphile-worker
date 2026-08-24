export type LogAttributes = Record<
	string,
	string | number | boolean | null | undefined
>

export type JobLogger = {
	debug(message: string, attributes?: LogAttributes): void
	info(message: string, attributes?: LogAttributes): void
	warn(message: string, attributes?: LogAttributes): void
	error(message: string, attributes?: LogAttributes): void
}

export type SpanEventAttributes = Record<
	string,
	string | number | boolean | null | undefined
>

export type JobSpan = {
	setAttribute(key: string, value: string | number | boolean): void
	setAttributes(attributes: Record<string, string | number | boolean>): void
	setStatus(status: { code: number; message?: string }): void
	addEvent(name: string, attributes?: SpanEventAttributes): void
	recordException(error: Error): void
	end(): void
}

export type CreateLoggerOptions = {
	queue: string
	jobId: string
	attempt: number
	span: JobSpan
}

export type JobFinishedEvent = {
	queue: string
	status: 'success' | 'failed'
	durationMs: number
	permanentlyFailed?: boolean
	jobId: string
	operation: 'process' | 'init'
	attempt: number
	maxAttempts: number
	errorType?: string
}

export type PermanentFailureEvent = {
	error: unknown
	queue: string
	jobId: string
	operation: 'process' | 'init'
	attempts: number
	maxAttempts: number
}

export type EnqueueFailEvent = {
	queue: string
	error: unknown
}

export type BetterWorkerHooks = {
	createLogger?: (options: CreateLoggerOptions) => JobLogger
	onJobFinished?: (event: JobFinishedEvent) => void
	onPermanentFailure?: (event: PermanentFailureEvent) => void
	onEnqueueFail?: (event: EnqueueFailEvent) => void
	shouldSkipEnqueue?: () => boolean
}
