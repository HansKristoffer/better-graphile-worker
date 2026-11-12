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

/** Span and log attributes `hooks.jobAttributes` derives from a job's payload. */
export type JobAttributes = Record<string, string | number | boolean>

export type CreateLoggerOptions = {
	queue: string
	jobId: string
	attempt: number
	span: JobSpan
	/** From `hooks.jobAttributes`, for the logger to add to every line. */
	attributes?: JobAttributes | undefined
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
	/** The next attempt runs at this time, from `RetryAtError` or the queue's `retryDelay`. */
	retryAt?: Date
	/**
	 * How long the job waited past its `run_at` (enqueue time, or the scheduled retry)
	 * before its handler started, in milliseconds. Uses this process's clock against the
	 * database's, so clock skew shows up here.
	 */
	waitMs: number
	/** From `hooks.jobAttributes`. */
	attributes?: JobAttributes | undefined
	/** The run ended with `ctx.continue()`; a continuation job was enqueued. */
	continued?: boolean
	/**
	 * The run ended at `ctx.step.triggerAndWait` until its children finish; the job keeps
	 * its id and attempt, and runs again from the top.
	 */
	waiting?: boolean
}

/**
 * What ended a job's last attempt: its handler threw (`error`), ran past its `timeout`,
 * threw `NonRetriableError` (including an invalid payload), its worker stopped
 * heartbeating (`worker_lost`), shutdown cut it off after `shutdownGrace`, or `failJobs`
 * failed it.
 */
export type PermanentFailureReason =
	| 'error'
	| 'timeout'
	| 'non_retriable'
	| 'worker_lost'
	| 'shutdown'
	| 'failJobs'

export type PermanentFailureEvent = {
	reason: PermanentFailureReason
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
	/** Return true to enqueue nothing; producers then get null (or `[]`). */
	shouldSkipEnqueue?: (event: { queue: string }) => boolean
	/**
	 * Attributes to set on each job's span, pass to `createLogger` and add to
	 * `onJobFinished`, such as tenant ids from the wire payload. Synchronous; the hook
	 * decides what leaves the payload.
	 */
	jobAttributes?: (event: {
		queue: string
		payload: unknown
	}) => JobAttributes | undefined
}
