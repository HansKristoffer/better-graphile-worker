import { compact } from './options.js'
import {
	run,
	runOnce,
	parseCronItems,
	type Runner,
	type RunnerOptions,
	type TaskList,
	type CronItem,
	type CronMatcher,
	type JobHelpers,
	type WorkerEventMap,
	type WorkerEvents
} from 'graphile-worker'
import { EventEmitter } from 'node:events'
import type { Pool } from 'pg'
import type z from 'zod'
import {
	type JobContext,
	CRON_INIT_SUFFIX,
	getQueueType,
	formatCronSchedule,
	parseContinueRunAt,
	resolveSerialQueueName,
	type QueueContract
} from './queue.js'
import type { NormalizedQueue } from './registry.js'
import type { CompletedJobsStore } from './completed-jobs-store.js'
import type {
	BetterWorkerHooks,
	JobLogger,
	JobSpan,
	LogAttributes
} from './hooks.js'
import type { CreateJobFn, CreateJobsFn, PreparedJob } from './types.js'
import {
	DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS,
	parseQueuePayload,
	resolveEnqueueSpec
} from './create-job.js'
import {
	JobContinuedSignal,
	JobSuspendedSignal,
	JobTimeoutError,
	NonRetriableError,
	RetryAtError,
	StepPersistenceError,
	UnknownQueueError,
	WorkerStoppedError
} from './errors.js'
import {
	extractCronMeta,
	extractProducerLink,
	isPayloadEnvelope,
	assertPayloadEnvelope,
	injectTraceContext
} from './payload.js'
import { otelStatusCodes, withActiveSpan, type OtelApi } from './otel.js'
import { jobLogger, observe } from './observers.js'
import {
	continueJob,
	enterWait,
	scheduleRetry,
	settleLockedJob,
	writeProgress
} from './private-jobs.js'
import {
	createPgStepStore,
	createStepRunner,
	type WaitBackend
} from './steps.js'
import { encodeQueueOutput, type ChildReport } from './waits.js'
import type { JsonValue } from './validation.js'
import { createProgressRunner, type ProgressRunner } from './progress.js'

export type TaskListRuntime<
	TQueues extends readonly QueueContract[] = readonly QueueContract[]
> = {
	hooks: BetterWorkerHooks
	completedJobs: CompletedJobsStore
	createJob: CreateJobFn<TQueues>
	createJobs: CreateJobsFn<TQueues>
	logger: JobLogger
	schema: string
	otel?: OtelApi | null
	permanentFailure?: 'discard' | 'retain' | undefined
	/** Applies to queues without their own `timeout`, in milliseconds. */
	defaultTimeout?: number | undefined
	/** Record each job's final state in `bgw_job_outcomes`. */
	jobOutcomes?: boolean | undefined
	/** Settles once crash recovery has registered this worker; jobs wait for it. */
	heartbeatReady?: () => Promise<void> | undefined
	/** Every registered queue and prepared enqueue, used by `ctx.step.triggerAndWait`. */
	queues?: readonly QueueContract[] | undefined
	prepareJob?:
		| ((name: string, data?: unknown) => Promise<PreparedJob>)
		| undefined
	prepareJobs?:
		| ((name: string, data: readonly unknown[]) => Promise<PreparedJob>)
		| undefined
}

export { extractProducerLink } from './payload.js'

/**
 * One run's signal and deadline. JavaScript cannot stop a handler that ignores its
 * signal, so on timeout the run is failed without it: the handler keeps running
 * detached, and everything it starts afterwards through the context is refused.
 */
function createAttempt(parent: AbortSignal, timeout: number | undefined) {
	const controller = new AbortController()
	const forward = () => controller.abort(parent.reason)
	if (parent.aborted) forward()
	else parent.addEventListener('abort', forward, { once: true })
	let expired: JobTimeoutError | undefined
	let timer: ReturnType<typeof setTimeout> | undefined
	return {
		signal: controller.signal,
		assertActive() {
			if (expired) throw expired
		},
		/** Settles with `work`, or with a `JobTimeoutError` once the timeout passes. */
		run<T>(work: Promise<T>, onAbandoned: (error?: unknown) => void) {
			if (timeout === undefined) return work
			return new Promise<T>((resolve, reject) => {
				work.then(resolve, reject)
				timer = setTimeout(() => {
					const error = new JobTimeoutError(timeout)
					expired = error
					controller.abort(error)
					reject(error)
					work.then(() => onAbandoned(), onAbandoned)
				}, timeout)
			})
		},
		dispose() {
			clearTimeout(timer)
			parent.removeEventListener('abort', forward)
		}
	}
}

type Attempt = ReturnType<typeof createAttempt>

type Ending =
	| { kind: 'continued'; jobId: string }
	| { kind: 'waiting'; waitId: string }

/**
 * Writes that can end the run, `ctx.continue()` and waits, one transaction at a time. A
 * committed ending decides the run, even when its timeout fires meanwhile, and only the
 * first wait to suspend gives the attempt back; waits after it still enqueue children.
 */
function createEndings() {
	let ended: Ending | undefined
	let chain: Promise<unknown> = Promise.resolve()
	return {
		run<T>(work: () => Promise<T>): Promise<T> {
			const result = chain.then(work, work)
			chain = result.catch(() => {})
			return result
		},
		current: () => ended,
		end(ending: Ending) {
			ended ??= ending
		},
		/** How the run ended, once in-flight endings settle. */
		async settled() {
			await chain
			return ended
		}
	}
}

type Endings = ReturnType<typeof createEndings>

function createWaitBackend(
	helpers: JobHelpers,
	runtime: TaskListRuntime,
	attempt: Attempt,
	endings: Endings
): WaitBackend {
	return {
		async wait(id, queue, inputs, batch, options) {
			const { queues, prepareJob, prepareJobs } = runtime
			if (!queues || !prepareJob || !prepareJobs)
				throw new Error('This worker runtime cannot enqueue waited children')
			if (!queues.includes(queue))
				throw new UnknownQueueError(
					queue.name,
					queues.map((item) => item.name)
				)
			return endings.run(async () => {
				attempt.assertActive()
				const ended = endings.current()
				if (ended?.kind === 'continued')
					throw new Error(
						'A wait cannot start after ctx.continue(); do not catch its rejection'
					)
				const entered = await enterWait(helpers, runtime.schema, {
					waitId: id,
					count: inputs.length,
					timeout: options?.timeout,
					suspended: ended !== undefined,
					prepare: () =>
						batch
							? prepareJobs(queue.name, inputs)
							: prepareJob(queue.name, inputs[0])
				})
				if (entered.state === 'suspended') {
					endings.end({ kind: 'waiting', waitId: id })
					throw new JobSuspendedSignal(id)
				}
				return { checkpoint: entered.checkpoint, persisted: true }
			})
		}
	}
}

function createJobContext(
	jobId: string,
	queueName: string,
	span: JobSpan,
	helpers: JobHelpers,
	hooks: BetterWorkerHooks,
	runtime: TaskListRuntime,
	cron: JobContext['cron'],
	rawPayload: unknown,
	attempt: Attempt,
	progress: ProgressRunner,
	endings: Endings
): JobContext {
	return {
		jobId,
		queue: queueName,
		attempt: helpers.job.attempts,
		maxAttempts: helpers.job.max_attempts,
		span,
		helpers,
		signal: attempt.signal,
		createJob: async (...args) => {
			attempt.assertActive()
			return runtime.createJob(...args)
		},
		createJobs: async (...args) => {
			attempt.assertActive()
			return runtime.createJobs(...args)
		},
		cron,
		continue: createContinue(
			helpers,
			runtime,
			span,
			attempt,
			progress,
			endings
		),
		progress: {
			get: progress.get,
			set: async (value) => {
				attempt.assertActive()
				return progress.set(value)
			},
			flush: async () => {
				attempt.assertActive()
				return progress.flush()
			}
		} as JobContext['progress'],
		logger: jobLogger(hooks, {
			queue: queueName,
			jobId,
			attempt: helpers.job.attempts,
			span
		}),
		step: createStepRunner({
			store: createPgStepStore({
				helpers,
				schema: runtime.schema,
				jobId,
				rawPayload: isPayloadEnvelope(rawPayload)
					? rawPayload
					: injectTraceContext(undefined, null)
			}),
			span,
			assertActive: attempt.assertActive,
			waits: createWaitBackend(helpers, runtime, attempt, endings)
		})
	}
}

function createContinue(
	helpers: JobHelpers,
	runtime: TaskListRuntime,
	span: JobSpan,
	attempt: Attempt,
	progress: ProgressRunner,
	endings: Endings
): JobContext['continue'] {
	let called = false
	return async (options) => {
		attempt.assertActive()
		if (called) throw new Error('ctx.continue() was already called')
		called = true
		const runAt = parseContinueRunAt(options)
		const { traceparent } = injectTraceContext(undefined, runtime.otel)
		const id = await endings.run(async () => {
			if (endings.current())
				throw new Error(
					'ctx.continue() cannot run after a wait suspended this run; do not catch its rejection'
				)
			// The continuation receives the latest value, so a lagging write does not matter.
			await progress.close(false).catch(() => {})
			const jobId = await continueJob(
				helpers,
				runtime.schema,
				runAt,
				traceparent,
				runtime.jobOutcomes === true,
				progress.get()
			)
			endings.end({ kind: 'continued', jobId })
			return jobId
		})
		span.setAttributes({
			'graphile.continued': true,
			'graphile.continuation_job_id': id
		})
		throw new JobContinuedSignal()
	}
}

// Keeps one huge error from flooding the log; hooks get the full message.
const LOGGED_ERROR_MESSAGE_LIMIT = 500

/** Resolves to the run's wire output for waiting parents, if the queue has one. */
type TaskExecutorFn = (
	ctx: JobContext,
	cleanPayload: unknown
) => Promise<JsonValue | undefined>

type ExecuteTaskOptions = {
	queueName: string
	helpers: JobHelpers
	payload: unknown
	operation: 'process' | 'init'
	executor: TaskExecutorFn
	extraAttributes?: Record<string, string | number>
	runtime: TaskListRuntime
	timeout: number | undefined
	progressSchema?: z.ZodType | undefined
}

function isNonRetriable(error: unknown): boolean {
	return error instanceof NonRetriableError
}

async function executeTask(options: ExecuteTaskOptions) {
	const {
		queueName,
		helpers,
		payload,
		operation,
		executor,
		extraAttributes,
		runtime,
		timeout,
		progressSchema
	} = options
	const jobId = String(helpers.job.id)
	const spanName = `job: ${queueName}`
	const { link, cleanPayload } = isPayloadEnvelope(payload)
		? extractProducerLink(payload)
		: { link: null, cleanPayload: undefined }
	const cron = extractCronMeta(payload)
	const statusCodes = otelStatusCodes(runtime.otel)
	await runtime.heartbeatReady?.()

	const suspended = await withActiveSpan(
		'graphile-worker',
		spanName,
		compact({ kind: 'consumer' as const, links: link ? [link] : undefined }),
		async (span) => {
			const startTime = Date.now()

			span.setAttributes({
				'messaging.system': 'graphile-worker',
				'messaging.destination.name': queueName,
				'messaging.message.id': jobId,
				'messaging.operation': operation,
				'graphile.queue': queueName,
				'graphile.job_id': jobId,
				'graphile.task_identifier': helpers.job.task_identifier,
				'graphile.attempts': helpers.job.attempts,
				'graphile.max_attempts': helpers.job.max_attempts,
				...extraAttributes
			})

			if (link) {
				span.setAttribute('messaging.producer.trace_id', link.context.traceId)
				span.setAttribute('messaging.producer.span_id', link.context.spanId)
			}

			const attempt = createAttempt(helpers.abortSignal, timeout)
			const progress = createProgressRunner({
				initial: isPayloadEnvelope(payload) ? payload.progress : undefined,
				schema: progressSchema,
				write: (value) => writeProgress(helpers, runtime.schema, jobId, value)
			})
			const endings = createEndings()
			const ctx = createJobContext(
				jobId,
				queueName,
				span,
				helpers,
				runtime.hooks,
				runtime,
				cron,
				payload,
				attempt,
				progress,
				endings
			)
			// Set by the wait that enqueued this job: report its final state to the parent.
			const waited = isPayloadEnvelope(payload) && payload.waited === true
			let status: 'success' | 'failed' = 'success'
			let errorType: string | undefined
			let errorMessage: string | undefined
			let permanent = false
			let continued = false
			let suspended: JobSuspendedSignal | undefined
			let retryAt: Date | undefined
			const settle = async (
				settled: ChildReport,
				row: Parameters<typeof settleLockedJob>[2]['row']
			) => {
				// Otherwise Graphile completes or fails the job itself.
				if (!runtime.jobOutcomes && !waited && row !== 'exhaust') return true
				const outcome = settled.ok
					? { state: 'completed' as const }
					: { state: 'failed' as const, error: settled.error.message }
				return settleLockedJob(helpers, runtime.schema, {
					outcome: runtime.jobOutcomes ? outcome : undefined,
					report: waited ? settled : undefined,
					row
				})
			}

			try {
				assertPayloadEnvelope(payload)
				let output: JsonValue | undefined
				try {
					output = await attempt.run(executor(ctx, cleanPayload), (error) => {
						const attributes: LogAttributes = {
							event: 'job.timed_out_handler_settled',
							queue: queueName,
							job_id: jobId,
							attempt: helpers.job.attempts,
							duration_ms: Date.now() - startTime
						}
						if (error !== undefined) {
							const message =
								error instanceof Error ? error.message : String(error)
							attributes.error_message = message.slice(
								0,
								LOGGED_ERROR_MESSAGE_LIMIT
							)
						}
						ctx.logger.warn('job.timed_out_handler_settled', attributes)
					})
				} catch (error) {
					// A committed ending decides the run, even against a timeout or an error
					// the handler threw after catching its signal.
					if (!(await endings.settled())) throw error
					// The signal itself, or a timeout that raced the ending, is not news.
					const isEndingOrRace =
						error instanceof JobContinuedSignal ||
						error instanceof JobSuspendedSignal ||
						error instanceof JobTimeoutError
					if (!isEndingOrRace)
						ctx.logger.warn('job.error_after_ending', {
							event: 'job.error_after_ending',
							job_id: jobId,
							error_message: String(error).slice(0, LOGGED_ERROR_MESSAGE_LIMIT)
						})
				}
				// Also when the handler caught its signal: the ending is already committed.
				const ending = await endings.settled()
				continued = ending?.kind === 'continued'
				if (ending?.kind === 'waiting') {
					suspended = new JobSuspendedSignal(ending.waitId)
					// The job keeps its row, so the resumed run reads the latest value.
					await progress.close(true).catch(() => {})
					span.setAttributes({ 'graphile.waiting': true })
					span.setStatus({ code: statusCodes.OK })
					return suspended
				}
				// Graphile deletes completed jobs, progress included.
				await progress.close(false).catch(() => {})
				// Like a checkpoint, a completion that cannot be recorded fails the attempt.
				if (
					!continued &&
					!(await settle(
						{ ok: true, ...(output === undefined ? {} : { output }) },
						'delete'
					))
				)
					throw new StepPersistenceError(jobId)

				const durationMs = Date.now() - startTime
				span.setAttributes({ 'graphile.duration_ms': durationMs })
				span.setStatus({ code: statusCodes.OK })

				runtime.completedJobs.add({
					id: jobId,
					queueName,
					payload: cleanPayload,
					status: 'completed',
					attempts: helpers.job.attempts,
					maxAttempts: helpers.job.max_attempts,
					createdAt: helpers.job.created_at.toISOString(),
					completedAt: new Date().toISOString(),
					durationMs,
					error: null
				})
			} catch (error) {
				status = 'failed'
				// Retries read the latest progress, such as a retry budget.
				await progress.close(true).catch((progressError: unknown) => {
					ctx.logger.warn('job.progress.write_failed', {
						event: 'job.progress.write_failed',
						job_id: jobId,
						error_message: String(progressError).slice(
							0,
							LOGGED_ERROR_MESSAGE_LIMIT
						)
					})
				})
				const durationMs = Date.now() - startTime
				// RetryAtError only carries scheduling; report what actually went wrong.
				const reported =
					error instanceof RetryAtError && error.cause !== undefined
						? error.cause
						: error
				errorMessage =
					reported instanceof Error ? reported.message : String(reported)
				errorType = reported instanceof Error ? reported.name : 'Unknown'

				span.setStatus({ code: statusCodes.ERROR, message: errorMessage })
				span.recordException(
					reported instanceof Error ? reported : new Error(String(reported))
				)
				span.setAttributes({
					'graphile.duration_ms': durationMs,
					'error.type': errorType
				})

				permanent =
					isNonRetriable(error) ||
					helpers.job.attempts >= helpers.job.max_attempts

				if (permanent) {
					await observe(runtime.hooks.onPermanentFailure, {
						error: reported,
						queue: queueName,
						jobId,
						operation,
						attempts: helpers.job.attempts,
						maxAttempts: helpers.job.max_attempts,
						payload: cleanPayload
					})
					runtime.completedJobs.add({
						id: jobId,
						queueName,
						payload: cleanPayload,
						status: 'failed',
						attempts: helpers.job.attempts,
						maxAttempts: helpers.job.max_attempts,
						createdAt: helpers.job.created_at.toISOString(),
						completedAt: new Date().toISOString(),
						durationMs,
						error: errorMessage
					})
				}

				const failure: ChildReport = {
					ok: false,
					error: { name: errorType, message: errorMessage }
				}

				if (!permanent && error instanceof RetryAtError)
					retryAt = await scheduleRetry(helpers, runtime.schema, error.runAt)

				// Retention must hold, like a checkpoint; throwing the original error then lets
				// Graphile record it and release the lock.
				const retain =
					isNonRetriable(error) && runtime.permanentFailure !== 'discard'
				if (retain) {
					if (!(await settle(failure, 'exhaust')))
						throw new StepPersistenceError(jobId)
					throw error
				}

				// The job row stays (or Graphile deletes it on discard), so these writes are
				// best effort; the wait sweep reports a failed child whose report was lost.
				if (permanent)
					await settle(
						failure,
						isNonRetriable(error) ? 'delete' : 'keep'
					).catch((recordError) =>
						ctx.logger.error('job.outcome.failed', {
							queue: queueName,
							job_id: jobId,
							error: String(recordError)
						})
					)
				// Discard: acknowledge, so Graphile deletes the job.
				if (isNonRetriable(error)) return
				throw error
			} finally {
				attempt.dispose()
				const durationMs = Date.now() - startTime
				const permanentlyFailed = status === 'failed' && permanent
				await observe(runtime.hooks.onJobFinished, {
					queue: queueName,
					status,
					durationMs,
					permanentlyFailed,
					jobId,
					operation,
					attempt: helpers.job.attempts,
					maxAttempts: helpers.job.max_attempts,
					errorType,
					errorMessage,
					...(retryAt ? { retryAt } : {}),
					...(continued ? { continued } : {}),
					...(suspended ? { waiting: true } : {})
				})
				const level = status === 'success' ? 'info' : 'error'
				ctx.logger[level]('job.completed', {
					event: 'job.completed',
					queue: queueName,
					job_id: jobId,
					operation,
					attempt: helpers.job.attempts,
					max_attempts: helpers.job.max_attempts,
					duration_ms: durationMs,
					status,
					...(continued ? { continued } : {}),
					...(suspended ? { waiting: true } : {}),
					...(retryAt ? { retry_at: retryAt.toISOString() } : {}),
					...(errorType ? { error_type: errorType } : {}),
					...(errorMessage === undefined
						? {}
						: {
								error_message: errorMessage.slice(0, LOGGED_ERROR_MESSAGE_LIMIT)
							})
				})
			}
		},
		runtime.otel
	)
	// Graphile's failure update unlocks the job and its lane and keeps the row, where a
	// successful return would delete it.
	if (suspended) throw suspended
}

export function buildTaskList<TQueues extends readonly QueueContract[]>(
	queues: readonly NormalizedQueue[],
	runtime: TaskListRuntime<TQueues>
): TaskList {
	const taskList: TaskList = Object.create(null)
	const erasedRuntime = runtime as unknown as TaskListRuntime

	for (const queue of queues) {
		const q = queue
		const timeout = q.timeout ?? runtime.defaultTimeout
		if (q.initFn) {
			const initFn = q.initFn
			const processingTaskName = q.name
			const cronInitTaskName = `${q.name}${CRON_INIT_SUFFIX}`

			taskList[cronInitTaskName] = (payload, helpers) =>
				executeTask({
					queueName: cronInitTaskName,
					helpers,
					payload,
					operation: 'init',
					extraAttributes: { 'graphile.target_queue': processingTaskName },
					runtime: erasedRuntime,
					timeout,
					executor: async (ctx, _cleanPayload) => {
						const items = await initFn(ctx)

						ctx.logger.info('Init function returned items', {
							count: items.length
						})
						ctx.span.setAttribute('graphile.init_items_count', items.length)

						if (items.length > 0) {
							const jobIds = await ctx.createJobs(processingTaskName, items, {
								validateOnEnqueue: true
							})
							ctx.logger.info('Enqueued items for processing', {
								jobCount: jobIds.length
							})
						}
						// Initializers are never waited on, so they have no output.
						return undefined
					}
				})

			taskList[processingTaskName] = (payload, helpers) =>
				executeTask({
					queueName: processingTaskName,
					helpers,
					payload,
					operation: 'process',
					runtime: erasedRuntime,
					timeout,
					progressSchema: q.progressSchema,
					executor: async (ctx, cleanPayload) => {
						const validatedPayload = await parseQueuePayload(q, cleanPayload)
						return encodeQueueOutput(
							q,
							await q.processFn(validatedPayload, ctx)
						)
					}
				})
		} else {
			taskList[q.name] = (payload, helpers) =>
				executeTask({
					queueName: q.name,
					helpers,
					payload,
					operation: 'process',
					runtime: erasedRuntime,
					timeout,
					progressSchema: q.progressSchema,
					executor: async (ctx, cleanPayload) => {
						const validatedPayload = q.inputSchema
							? await parseQueuePayload(q, cleanPayload)
							: undefined
						return encodeQueueOutput(
							q,
							await q.processFn(validatedPayload, ctx)
						)
					}
				})
		}
	}

	return taskList
}

function cronMatches(cron: QueueContract['cron']): Array<string | CronMatcher> {
	if (cron === undefined) return []
	if (typeof cron === 'string' || typeof cron === 'function') return [cron]
	return [...cron]
}

export function buildCronItems(
	queues: readonly QueueContract[],
	defaultMaxAttempts = DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS
): CronItem[] {
	const cronItems: CronItem[] = []

	for (const queue of queues) {
		const q = queue
		if (!q.cron) continue

		const taskName =
			getQueueType(q) === 'cron-init' ? `${q.name}${CRON_INIT_SUFFIX}` : q.name
		const matches = cronMatches(q.cron)
		const options = q.cronOptions
		resolveEnqueueSpec(q, undefined, defaultMaxAttempts, true)

		for (const [index, match] of matches.entries()) {
			const identifier =
				matches.length > 1
					? `${options?.identifier ?? taskName}:${index}`
					: (options?.identifier ?? taskName)

			cronItems.push({
				task: taskName,
				match,
				payload: injectTraceContext(undefined, null),
				identifier,
				options: compact({
					maxAttempts:
						options?.maxAttempts ?? q.maxAttempts ?? defaultMaxAttempts,
					backfillPeriod: options?.backfillPeriod,
					queueName:
						options?.queueName ?? resolveSerialQueueName(q.serial, q.name),
					priority: options?.priority ?? q.priority,
					jobKey: options?.jobKey,
					jobKeyMode: options?.jobKeyMode
				})
			})
		}
	}

	const identifiers = new Set<string>()
	for (const item of cronItems) {
		if (identifiers.has(item.identifier!))
			throw new Error(`Duplicate cron identifier: ${item.identifier}`)
		identifiers.add(item.identifier!)
	}
	return cronItems
}

export function logRegisteredQueues(
	queues: readonly QueueContract[],
	logger: JobLogger
): void {
	if (queues.length === 0) {
		logger.info('No queues registered')
		return
	}

	logger.info('Registered queues:')
	for (const queue of queues) {
		const q = queue
		const type = getQueueType(q)
		if (type === 'cron-init') {
			logger.info(
				`  - ${q.name}${CRON_INIT_SUFFIX} (cron-init: ${formatCronSchedule(q.cron)})`
			)
			logger.info(`  - ${q.name} (processor)`)
		} else if (type === 'cron') {
			logger.info(`  - ${q.name} (cron: ${formatCronSchedule(q.cron)})`)
		} else {
			logger.info(`  - ${q.name} (regular)`)
		}
	}
}

export const DEFAULT_CONCURRENCY = 10
export const DEFAULT_POLL_INTERVAL = 250

export type GraphileRunnerOverrides = Omit<
	Partial<RunnerOptions>,
	| 'pgPool'
	| 'schema'
	| 'taskList'
	| 'parsedCronItems'
	| 'crontab'
	| 'crontabFile'
	| 'connectionString'
	| 'taskDirectory'
	| 'noHandleSignals'
	| 'concurrency'
	| 'pollInterval'
>

/**
 * graphile-worker's default (-1) completes/fails jobs fire-and-forget, so stop()
 * and runOnce() can resolve before the job row is updated. Batching (even with
 * no delay) is flushed during shutdown. Caller presets still take precedence.
 */
function withDurableCompletion(
	graphile: GraphileRunnerOverrides | undefined,
	localQueueSize?: number
) {
	return {
		...graphile,
		preset: {
			extends: [
				{
					worker: {
						completeJobBatchDelay: 0,
						failJobBatchDelay: 0,
						...(localQueueSize === undefined
							? {}
							: { localQueue: { size: localQueueSize } })
					}
				},
				...(graphile?.preset ? [graphile.preset] : [])
			]
		}
	}
}

/**
 * Graphile's fetch locks every available lane with `FOR UPDATE SKIP LOCKED`, and every
 * worker on the database wakes on each insert. One that locked a lane for its own fetch
 * hides the lane's job from the others until its next poll; fetching once more just after
 * the notification finds it.
 */
function secondLookListener(delayMs: number | undefined) {
	if (delayMs === undefined) return undefined
	return ({
		workerPool,
		message
	}: WorkerEventMap['pool:listen:notification']) => {
		if (message.channel !== 'jobs:insert') return
		setTimeout(() => {
			if (workerPool._active && !workerPool._shuttingDown) workerPool.nudge(1)
		}, delayMs).unref()
	}
}

/** One Graphile worker pool and its maintenance; `stop()` can be called more than once. */
export type BetterRunner = { promise: Promise<void>; stop(): Promise<void> }

export async function startRunner(options: {
	pgPool: Pool
	schema: string
	taskList: TaskList
	cronItems: CronItem[]
	concurrency: number
	pollInterval: number
	noHandleSignals: boolean
	graphile?: GraphileRunnerOverrides | undefined
	/** Fetch and lock up to `concurrency + prefetch` jobs at once (Graphile's local queue). */
	prefetch?: number | undefined
	/**
	 * Starts maintenance for the pool id Graphile stores in locked_by, before any job is
	 * claimed. It is stopped once the pool has finished, including a graceful drain.
	 */
	maintain?: ((poolId: string) => { stop(): Promise<void> }) | undefined
	secondLookOnNotify?: number | undefined
	/** Names the pool runner in a `WorkerStoppedError`. */
	runnerName?: string | undefined
}): Promise<BetterRunner> {
	const parsedCronItems =
		options.cronItems.length > 0 ? parseCronItems(options.cronItems) : undefined
	const { maintain, graphile } = options
	// An emitter nested in a preset's `extends` is not found here and gets replaced.
	const events =
		graphile?.events ??
		graphile?.preset?.worker?.events ??
		(new EventEmitter() as WorkerEvents)
	let maintenance: { stop(): Promise<void> } | undefined
	const stopMaintenance = async () => {
		await maintenance?.stop()
	}
	const onCreate = ({ workerPool }: WorkerEventMap['pool:create']) => {
		maintenance = maintain?.(workerPool.id)
	}
	events.on('pool:create', onCreate)
	const secondLook = secondLookListener(options.secondLookOnNotify)
	if (secondLook) events.on('pool:listen:notification', secondLook)
	// Graphile shuts its pool down on errors it cannot recover from (a failed job
	// completion, a local queue that cannot return its jobs) and then resolves the
	// runner's promise as if it had been asked to stop.
	let cause: Error | undefined
	const onFatalError = ({ error }: WorkerEventMap['pool:fatalError']) => {
		cause ??= error instanceof Error ? error : new Error(String(error))
	}
	const onShutdown = ({
		message
	}: WorkerEventMap['pool:gracefulShutdown' | 'pool:forcefulShutdown']) => {
		cause ??= new Error(message)
	}
	const detach = () => {
		if (secondLook) events.off('pool:listen:notification', secondLook)
		events.off('pool:fatalError', onFatalError)
		events.off('pool:gracefulShutdown', onShutdown)
		events.off('pool:forcefulShutdown', onShutdown)
	}
	events.on('pool:fatalError', onFatalError)
	events.on('pool:gracefulShutdown', onShutdown)
	events.on('pool:forcefulShutdown', onShutdown)

	let runner: Runner
	try {
		runner = await run(
			compact({
				...withDurableCompletion(
					graphile,
					options.prefetch === undefined
						? undefined
						: options.concurrency + options.prefetch
				),
				events,
				pgPool: options.pgPool,
				schema: options.schema,
				taskList: options.taskList,
				parsedCronItems,
				noHandleSignals: options.noHandleSignals,
				concurrency: options.concurrency,
				pollInterval: options.pollInterval
			})
		)
	} catch (error) {
		detach()
		await stopMaintenance()
		throw error
	} finally {
		// run() emits pool:create synchronously before it resolves.
		events.off('pool:create', onCreate)
	}
	let requested = false
	const promise = runner.promise
		.finally(detach)
		.finally(stopMaintenance)
		.then(() => {
			if (!requested)
				throw new WorkerStoppedError(options.runnerName, { cause })
		})
	const ended = promise.then(
		() => {},
		() => {}
	)
	return {
		promise,
		stop() {
			requested = true
			// Graphile rejects a second stop, including one that races its own shutdown;
			// either way the runner is ending, so wait for that.
			return runner.stop().then(
				() => ended,
				() => ended
			)
		}
	}
}

export async function runOnceTasks(options: {
	pgPool: Pool
	schema: string
	taskList: TaskList
	noHandleSignals: boolean
	graphile?: GraphileRunnerOverrides | undefined
}): Promise<void> {
	await runOnce({
		...withDurableCompletion(options.graphile),
		pgPool: options.pgPool,
		schema: options.schema,
		taskList: options.taskList,
		noHandleSignals: options.noHandleSignals
	})
}
