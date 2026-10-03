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
	type JobHelpers
} from 'graphile-worker'
import type { Pool } from 'pg'
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
import type { BetterWorkerHooks, JobLogger, JobSpan } from './hooks.js'
import type { CreateJobFn, CreateJobsFn } from './types.js'
import {
	DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS,
	parseQueuePayload,
	resolveEnqueueSpec
} from './create-job.js'
import { JobContinuedSignal, NonRetriableError } from './errors.js'
import {
	extractCronMeta,
	extractProducerLink,
	isPayloadEnvelope,
	assertPayloadEnvelope,
	injectTraceContext
} from './payload.js'
import { otelStatusCodes, withActiveSpan, type OtelApi } from './otel.js'
import { jobLogger, observe } from './observers.js'
import { continueJob, retainPermanentFailure } from './private-jobs.js'
import { createPgStepStore, createStepRunner } from './steps.js'

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
}

export { extractProducerLink } from './payload.js'

function createJobContext(
	jobId: string,
	queueName: string,
	span: JobSpan,
	helpers: JobHelpers,
	hooks: BetterWorkerHooks,
	runtime: TaskListRuntime,
	cron: JobContext['cron'],
	rawPayload: unknown
): JobContext {
	return {
		jobId,
		queue: queueName,
		attempt: helpers.job.attempts,
		maxAttempts: helpers.job.max_attempts,
		span,
		helpers,
		signal: helpers.abortSignal,
		createJob: runtime.createJob,
		createJobs: runtime.createJobs,
		cron,
		continue: createContinue(helpers, runtime, span),
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
			span
		})
	}
}

function createContinue(
	helpers: JobHelpers,
	runtime: TaskListRuntime,
	span: JobSpan
): JobContext['continue'] {
	let called = false
	return async (options) => {
		if (called) throw new Error('ctx.continue() was already called')
		called = true
		const runAt = parseContinueRunAt(options)
		const { traceparent } = injectTraceContext(undefined, runtime.otel)
		const id = await continueJob(helpers, runtime.schema, runAt, traceparent)
		span.setAttributes({
			'graphile.continued': true,
			'graphile.continuation_job_id': id
		})
		throw new JobContinuedSignal()
	}
}

type TaskExecutorFn = (ctx: JobContext, cleanPayload: unknown) => Promise<void>

type ExecuteTaskOptions = {
	queueName: string
	helpers: JobHelpers
	payload: unknown
	operation: 'process' | 'init'
	executor: TaskExecutorFn
	extraAttributes?: Record<string, string | number>
	runtime: TaskListRuntime
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
		runtime
	} = options
	const jobId = String(helpers.job.id)
	const spanName = `job: ${queueName}`
	const { link, cleanPayload } = isPayloadEnvelope(payload)
		? extractProducerLink(payload)
		: { link: null, cleanPayload: undefined }
	const cron = extractCronMeta(payload)
	const statusCodes = otelStatusCodes(runtime.otel)

	await withActiveSpan(
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

			const ctx = createJobContext(
				jobId,
				queueName,
				span,
				helpers,
				runtime.hooks,
				runtime,
				cron,
				payload
			)
			let status: 'success' | 'failed' = 'success'
			let errorType: string | undefined
			let permanent = false
			let continued = false

			try {
				assertPayloadEnvelope(payload)
				try {
					await executor(ctx, cleanPayload)
				} catch (error) {
					if (!(error instanceof JobContinuedSignal)) throw error
					continued = true
				}

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
				const durationMs = Date.now() - startTime
				const errorMessage =
					error instanceof Error ? error.message : String(error)
				errorType = error instanceof Error ? error.name : 'Unknown'

				span.setStatus({ code: statusCodes.ERROR, message: errorMessage })
				span.recordException(
					error instanceof Error ? error : new Error(String(error))
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
						error,
						queue: queueName,
						jobId,
						operation,
						attempts: helpers.job.attempts,
						maxAttempts: helpers.job.max_attempts
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

				if (isNonRetriable(error)) {
					if (runtime.permanentFailure !== 'discard') {
						await retainPermanentFailure(helpers, runtime.schema)
						throw error
					}
					return
				}

				throw error
			} finally {
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
					...(continued ? { continued } : {})
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
					...(errorType ? { error_type: errorType } : {})
				})
			}
		},
		runtime.otel
	)
}

export function buildTaskList<TQueues extends readonly QueueContract[]>(
	queues: readonly NormalizedQueue[],
	runtime: TaskListRuntime<TQueues>
): TaskList {
	const taskList: TaskList = Object.create(null)
	const erasedRuntime = runtime as unknown as TaskListRuntime

	for (const queue of queues) {
		const q = queue
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
					executor: async (ctx, _cleanPayload) => {
						const items = await initFn(ctx)

						ctx.logger.info('Init function returned items', {
							count: items.length
						})
						ctx.span.setAttribute('graphile.init_items_count', items.length)

						if (items.length > 0) {
							const jobIds = await erasedRuntime.createJobs(
								processingTaskName,
								items,
								{ validateOnEnqueue: true }
							)
							ctx.logger.info('Enqueued items for processing', {
								jobCount: jobIds.length
							})
						}
					}
				})

			taskList[processingTaskName] = (payload, helpers) =>
				executeTask({
					queueName: processingTaskName,
					helpers,
					payload,
					operation: 'process',
					runtime: erasedRuntime,
					executor: async (ctx, cleanPayload) => {
						const validatedPayload = await parseQueuePayload(q, cleanPayload)
						await q.processFn(validatedPayload, ctx)
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
					executor: async (ctx, cleanPayload) => {
						const validatedPayload = q.inputSchema
							? await parseQueuePayload(q, cleanPayload)
							: undefined
						await q.processFn(validatedPayload, ctx)
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
function withDurableCompletion(graphile: GraphileRunnerOverrides | undefined) {
	return {
		...graphile,
		preset: {
			extends: [
				{ worker: { completeJobBatchDelay: 0, failJobBatchDelay: 0 } },
				...(graphile?.preset ? [graphile.preset] : [])
			]
		}
	}
}

export async function startRunner(options: {
	pgPool: Pool
	schema: string
	taskList: TaskList
	cronItems: CronItem[]
	concurrency: number
	pollInterval: number
	noHandleSignals: boolean
	graphile?: GraphileRunnerOverrides | undefined
}): Promise<Runner> {
	const parsedCronItems =
		options.cronItems.length > 0 ? parseCronItems(options.cronItems) : undefined

	return run(
		compact({
			...withDurableCompletion(options.graphile),
			pgPool: options.pgPool,
			schema: options.schema,
			taskList: options.taskList,
			parsedCronItems,
			noHandleSignals: options.noHandleSignals,
			concurrency: options.concurrency,
			pollInterval: options.pollInterval
		})
	)
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
