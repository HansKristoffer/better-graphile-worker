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
	errorType?: string | undefined
	/** The failed attempt's error message, untrimmed. */
	errorMessage?: string | undefined
	/** The attempt threw `RetryAtError`; the next attempt runs at this time. */
	retryAt?: Date
	/** The run ended with `ctx.continue()`; a continuation job was enqueued. */
	continued?: boolean
	/**
	 * The run ended at `ctx.step.triggerAndWait` until its children finish; the job keeps
	 * its id and attempt, and runs again from the top.
	 */
	waiting?: boolean
}

export type PermanentFailureEvent = {
	error: unknown
	queue: string
	jobId: string
	operation: 'process' | 'init'
	attempts: number
	maxAttempts: number
	/**
	 * The job's payload as the producer enqueued it (wire input, without the
	 * envelope). It may contain personal data: the hook decides what to forward.
	 */
	payload: unknown
}

export type EnqueueFailEvent = {
	queue: string
	error: unknown
}

/**
 * Graphile stopped a runner although `stop()` was never called, and the worker is about
 * to start a new one. Not reported with `restart: false`, where `promise` rejects instead.
 */
export type RunnerStoppedEvent = {
	/** The pool runner's name; undefined for a single worker. */
	runner: string | undefined
	/** Usually a `WorkerStoppedError`, whose `cause` is Graphile's reason. */
	error: unknown
	/** Counts from 1; the restart this stop leads to. */
	restartAttempt: number
	/** The wait before that restart, in milliseconds. */
	restartInMs: number
}

export type BetterWorkerHooks = {
	createLogger?: (options: CreateLoggerOptions) => JobLogger
	onJobFinished?: (event: JobFinishedEvent) => void | Promise<void>
	onPermanentFailure?: (event: PermanentFailureEvent) => void | Promise<void>
	onEnqueueFail?: (event: EnqueueFailEvent) => void | Promise<void>
	/** Once per stop Graphile made on its own, before the first restart try. */
	onRunnerStopped?: (event: RunnerStoppedEvent) => void | Promise<void>
	shouldSkipEnqueue?: () => boolean
}
