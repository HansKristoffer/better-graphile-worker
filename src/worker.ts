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
import { ZodError } from 'zod'
import {
	type JobContext,
	type QueueAny,
	isCronInitQueue,
	CRON_INIT_SUFFIX,
	getQueueType,
	formatCronSchedule
} from './create-queue'
import type { CompletedJobsStore } from './completed-jobs-store'
import type { BetterWorkerHooks, JobLogger, JobSpan } from './hooks'
import type { CreateJobFn, CreateJobsFn } from './types'
import { DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS } from './create-job'
import { NonRetriableError } from './errors'
import { extractCronMeta, extractProducerLink } from './payload'
import { otelStatusCodes, withActiveSpan } from './otel'
import { createConsoleLogger } from './default-logger'
import { createPgStepStore, createStepRunner } from './steps'

export type TaskListRuntime<
	TQueues extends readonly QueueAny[] = readonly QueueAny[]
> = {
	hooks: BetterWorkerHooks
	completedJobs: CompletedJobsStore
	createJob: CreateJobFn<TQueues>
	createJobs: CreateJobsFn<TQueues>
	logger: JobLogger
	schema: string
}

export { extractProducerLink } from './payload'

function createJobContext<TQueues extends readonly QueueAny[]>(
	jobId: string,
	queueName: string,
	span: JobSpan,
	helpers: JobHelpers,
	hooks: BetterWorkerHooks,
	runtime: TaskListRuntime<TQueues>,
	cron: JobContext['cron'],
	rawPayload: unknown
): JobContext {
	const createLogger = hooks.createLogger ?? createConsoleLogger
	return {
		jobId,
		queue: queueName,
		attempt: helpers.job.attempts,
		maxAttempts: helpers.job.max_attempts,
		span,
		helpers,
		signal: helpers.abortSignal,
		createJob: runtime.createJob as JobContext['createJob'],
		createJobs: runtime.createJobs as JobContext['createJobs'],
		cron,
		logger: createLogger({
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
				rawPayload
			}),
			span
		})
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
	return error instanceof NonRetriableError || error instanceof ZodError
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
	const { link, cleanPayload } = extractProducerLink(payload)
	const cron = extractCronMeta(cleanPayload)
	const statusCodes = otelStatusCodes()

	await withActiveSpan(
		'graphile-worker',
		spanName,
		{ kind: 'consumer', links: link ? [link] : undefined },
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
			let swallowed = false

			try {
				await executor(ctx, cleanPayload)

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

				const permanentlyFailed =
					isNonRetriable(error) ||
					helpers.job.attempts >= helpers.job.max_attempts

				if (permanentlyFailed) {
					runtime.hooks.onPermanentFailure?.({
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
					swallowed = true
					return
				}

				throw error
			} finally {
				const durationMs = Date.now() - startTime
				const permanentlyFailed =
					status === 'failed' &&
					(swallowed || helpers.job.attempts >= helpers.job.max_attempts)
				runtime.hooks.onJobFinished?.({
					queue: queueName,
					status,
					durationMs,
					permanentlyFailed,
					jobId,
					operation,
					attempt: helpers.job.attempts,
					maxAttempts: helpers.job.max_attempts,
					errorType
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
					...(errorType ? { error_type: errorType } : {})
				})
			}
		}
	)
}

export function buildTaskList<TQueues extends readonly QueueAny[]>(
	queues: TQueues,
	runtime: TaskListRuntime<TQueues>
): TaskList {
	const taskList: TaskList = {}

	for (const queue of queues) {
		const q = queue as QueueAny
		if (isCronInitQueue(q)) {
			const processingTaskName = q.name
			const cronInitTaskName = `${q.name}${CRON_INIT_SUFFIX}`

			taskList[cronInitTaskName] = (payload, helpers) =>
				executeTask({
					queueName: cronInitTaskName,
					helpers,
					payload,
					operation: 'init',
					extraAttributes: { 'graphile.target_queue': processingTaskName },
					runtime,
					executor: async (ctx, _cleanPayload) => {
						const items = await q.initFn(ctx)
						const parsed = q.inputSchema.array().safeParse(items)
						if (!parsed.success) {
							throw new NonRetriableError(
								`initFn for "${q.name}" returned invalid items`,
								{ cause: parsed.error }
							)
						}

						ctx.logger.info('Init function returned items', {
							count: items.length
						})
						ctx.span.setAttribute('graphile.init_items_count', items.length)

						if (items.length > 0) {
							const jobIds = await runtime.createJobs(
								processingTaskName as never,
								items as never,
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
					runtime,
					executor: async (ctx, cleanPayload) => {
						const validatedPayload = q.inputSchema.parse(cleanPayload)
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
					runtime,
					executor: async (ctx, cleanPayload) => {
						const validatedPayload = q.inputSchema
							? q.inputSchema.parse(cleanPayload)
							: undefined
						await q.processFn(validatedPayload, ctx)
					}
				})
		}
	}

	return taskList
}

function cronMatches(cron: QueueAny['cron']): Array<string | CronMatcher> {
	if (cron === undefined) return []
	if (typeof cron === 'string' || typeof cron === 'function') return [cron]
	return [...cron]
}

export function buildCronItems(queues: readonly QueueAny[]): CronItem[] {
	const cronItems: CronItem[] = []

	for (const queue of queues) {
		const q = queue as QueueAny
		if (!q.cron) continue

		const taskName = isCronInitQueue(q)
			? `${q.name}${CRON_INIT_SUFFIX}`
			: q.name
		const matches = cronMatches(q.cron)
		const options = q.cronOptions

		for (const [index, match] of matches.entries()) {
			const identifier =
				options?.identifier ??
				(matches.length > 1 ? `${taskName}:${index}` : taskName)

			cronItems.push({
				task: taskName,
				match,
				payload: {},
				identifier,
				options: {
					maxAttempts:
						options?.maxAttempts ??
						q.maxAttempts ??
						DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS,
					backfillPeriod: options?.backfillPeriod,
					queueName: options?.queueName,
					priority: options?.priority,
					jobKey: options?.jobKey,
					jobKeyMode: options?.jobKeyMode
				}
			})
		}
	}

	return cronItems
}

export function logRegisteredQueues(
	queues: readonly QueueAny[],
	logger: JobLogger
): void {
	if (queues.length === 0) {
		logger.info('No queues registered')
		return
	}

	logger.info('Registered queues:')
	for (const queue of queues) {
		const q = queue as QueueAny
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
>

export async function startRunner(options: {
	pgPool: Pool
	schema: string
	taskList: TaskList
	cronItems: CronItem[]
	concurrency: number
	pollInterval: number
	noHandleSignals: boolean
	graphile?: GraphileRunnerOverrides
}): Promise<Runner> {
	const parsedCronItems =
		options.cronItems.length > 0 ? parseCronItems(options.cronItems) : undefined

	return run({
		...options.graphile,
		pgPool: options.pgPool,
		schema: options.schema,
		taskList: options.taskList,
		parsedCronItems,
		noHandleSignals: options.noHandleSignals,
		concurrency: options.concurrency,
		pollInterval: options.pollInterval
	})
}

export async function runOnceTasks(options: {
	pgPool: Pool
	schema: string
	taskList: TaskList
	noHandleSignals: boolean
	graphile?: GraphileRunnerOverrides
}): Promise<void> {
	await runOnce({
		...options.graphile,
		pgPool: options.pgPool,
		schema: options.schema,
		taskList: options.taskList,
		noHandleSignals: options.noHandleSignals
	})
}
