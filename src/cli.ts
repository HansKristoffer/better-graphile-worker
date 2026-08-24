import { parseArgs } from 'node:util'
import { z } from 'zod'
import type { QueueAny } from './create-queue'
import {
	CRON_INIT_SUFFIX,
	formatCronSchedule,
	getQueueType,
	hasInputSchema,
	isCronQueue
} from './create-queue'
import { DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS } from './create-job'
import type { BetterWorker } from './create-better-worker'
import type { JobOptions } from './job-options'

const HELP = `
better-graphile-worker CLI

Usage:
  <cli> <command> [options]

Commands:
  list-queues                 List all registered queues
  schema <queueName>          Show the input schema for a queue
  create-job <queue> [json]   Create a job in a queue
  stats                       Show pending/running/failed counts
  list-jobs                   List recent jobs
  retry <id>                  Reset attempts and run a job again
  fail <id> [reason]          Permanently fail a job
  run-once                    Process available jobs once and exit

Options:
  -h, --help                  Show this help message
  --json                      Print machine-readable JSON
  --priority <n>              Job priority (lower = higher priority)
  --run-at <iso>              Schedule job for later (ISO date string)
  --max-attempts <n>          Override max retry attempts
  --job-key <key>             Dedupe/replace jobs with same key
  --queue <name>              Filter list-jobs by queue
  --state <state>             Filter list-jobs: pending | running | failed
  --limit <n>                 list-jobs limit (default 100, max 1000)
  --offset <n>                list-jobs offset
`

function formatSchema(schema: z.ZodType): string {
	try {
		return JSON.stringify(z.toJSONSchema(schema), null, 2)
	} catch {
		return '(schema present; JSON Schema conversion failed)'
	}
}

function writeJson(value: unknown): void {
	console.log(JSON.stringify(value, null, 2))
}

function parseNumber(
	value: string | undefined,
	label: string
): number | undefined {
	if (value === undefined) return undefined
	const parsed = Number.parseInt(value, 10)
	if (Number.isNaN(parsed)) {
		throw new Error(`Invalid ${label}: ${value}`)
	}
	return parsed
}

export function createCli<TQueues extends readonly QueueAny[]>(
	worker: BetterWorker<TQueues>
): (argv?: string[]) => Promise<void> {
	return async (argv = process.argv.slice(2)) => {
		const { values, positionals } = parseArgs({
			args: argv,
			allowPositionals: true,
			options: {
				help: { type: 'boolean', short: 'h' },
				json: { type: 'boolean' },
				priority: { type: 'string' },
				'run-at': { type: 'string' },
				'max-attempts': { type: 'string' },
				'job-key': { type: 'string' },
				queue: { type: 'string' },
				state: { type: 'string' },
				limit: { type: 'string' },
				offset: { type: 'string' }
			}
		})

		const [command, ...args] = positionals
		const queues = worker.queues
		const json = values.json === true

		if (values.help || !command) {
			console.log(HELP)
			return
		}

		const getQueueByName = (name: string) => queues.find((q) => q.name === name)

		try {
			switch (command) {
				case 'list-queues': {
					const defs = worker.getQueueDefinitions()
					if (json) {
						writeJson(defs)
						return
					}
					console.log('\nRegistered queues:\n')
					for (const queue of queues) {
						const type = getQueueType(queue)
						const maxAttempts =
							queue.maxAttempts ?? DEFAULT_GRAPHILE_JOB_MAX_ATTEMPTS

						if (type === 'cron-init') {
							console.log(`  ${queue.name}${CRON_INIT_SUFFIX}`)
							console.log(
								`    Type: cron-init (${formatCronSchedule(queue.cron)})`
							)
							console.log('    Purpose: Gathers items via initFn')
							console.log()
							console.log(`  ${queue.name}`)
							console.log('    Type: processor')
							console.log('    Purpose: Processes items from cron-init')
							console.log(`    Max Attempts: ${maxAttempts}`)
							console.log('    Has Input Schema: yes')
						} else if (type === 'cron') {
							console.log(`  ${queue.name}`)
							console.log(`    Type: cron (${formatCronSchedule(queue.cron)})`)
							console.log(`    Max Attempts: ${maxAttempts}`)
						} else {
							console.log(`  ${queue.name}`)
							console.log('    Type: regular')
							console.log(`    Max Attempts: ${maxAttempts}`)
							if (hasInputSchema(queue)) {
								console.log('    Has Input Schema: yes')
							}
						}
						console.log()
					}
					break
				}

				case 'schema': {
					const [queueName] = args
					if (!queueName) {
						console.error('Error: Queue name required')
						console.error('Usage: schema <queueName>')
						process.exitCode = 1
						return
					}

					const queue = getQueueByName(queueName)
					if (!queue) {
						console.error(`Error: Queue "${queueName}" not found`)
						process.exitCode = 1
						return
					}

					const type = getQueueType(queue)
					if (json) {
						writeJson({
							name: queueName,
							type,
							cron: formatCronSchedule(queue.cron),
							schema: hasInputSchema(queue)
								? z.toJSONSchema(queue.inputSchema)
								: null
						})
						return
					}

					console.log(`\nSchema for queue: ${queueName}\n`)

					if (type === 'cron') {
						console.log('  This is a simple cron queue (no input schema)')
						console.log(`  Schedule: ${formatCronSchedule(queue.cron)}`)
					} else if (type === 'cron-init' && hasInputSchema(queue)) {
						console.log('  Type: cron-init queue')
						console.log(`  Schedule: ${formatCronSchedule(queue.cron)}`)
						console.log()
						console.log('Input schema (for processFn / createJob):')
						console.log(formatSchema(queue.inputSchema))
					} else if (hasInputSchema(queue)) {
						console.log('JSON Schema:')
						console.log(formatSchema(queue.inputSchema))
					}
					console.log()
					break
				}

				case 'create-job': {
					const [queueName, payloadJson] = args
					if (!queueName) {
						console.error('Error: Queue name required')
						console.error('Usage: create-job <queueName> [payloadJson]')
						process.exitCode = 1
						return
					}

					const queue = getQueueByName(queueName)
					if (!queue) {
						console.error(`Error: Queue "${queueName}" not found`)
						process.exitCode = 1
						return
					}

					const jobOptions: JobOptions = {}
					const priority = parseNumber(values.priority, 'priority')
					if (priority !== undefined) jobOptions.priority = priority
					if (values['run-at']) {
						jobOptions.runAt = new Date(values['run-at'])
					}
					const maxAttempts = parseNumber(
						values['max-attempts'],
						'max-attempts'
					)
					if (maxAttempts !== undefined) jobOptions.maxAttempts = maxAttempts
					if (values['job-key']) {
						jobOptions.jobKey = values['job-key']
					}

					if (isCronQueue(queue)) {
						const jobId = await worker.createJob(
							queueName as never,
							undefined as never,
							jobOptions
						)
						if (json) {
							writeJson({ jobId })
							return
						}
						console.log(`Job created: ${jobId}`)
						return
					}

					if (!hasInputSchema(queue)) {
						console.error(`Error: Queue "${queueName}" has no input schema`)
						process.exitCode = 1
						return
					}

					if (!payloadJson) {
						console.error('Error: Payload JSON required')
						console.error(`Usage: create-job ${queueName} '<json>'`)
						console.error('\nExpected schema:')
						console.log(formatSchema(queue.inputSchema))
						process.exitCode = 1
						return
					}

					let payload: unknown
					try {
						payload = JSON.parse(payloadJson)
					} catch {
						console.error('Error: Invalid JSON payload')
						process.exitCode = 1
						return
					}

					const jobId = await worker.createJob(
						queueName as never,
						payload as never,
						jobOptions
					)
					if (json) {
						writeJson({ jobId })
						return
					}
					console.log(`Creating job in queue "${queueName}"...`)
					console.log(`Job created: ${jobId}`)
					break
				}

				case 'stats': {
					const stats = await worker.getJobStats()
					if (json) {
						writeJson(stats)
						return
					}
					console.log('\nJob stats:\n')
					if (stats.length === 0) {
						console.log('  (no jobs)')
						return
					}
					for (const row of stats) {
						console.log(`  ${row.taskIdentifier}`)
						console.log(
							`    pending: ${row.pending}  running: ${row.running}  failed: ${row.failed}`
						)
					}
					console.log()
					break
				}

				case 'list-jobs': {
					const state = values.state
					if (
						state !== undefined &&
						state !== 'pending' &&
						state !== 'running' &&
						state !== 'failed'
					) {
						console.error('Error: --state must be pending, running, or failed')
						process.exitCode = 1
						return
					}
					const jobs = await worker.listJobs({
						queue: values.queue,
						state,
						limit: parseNumber(values.limit, 'limit'),
						offset: parseNumber(values.offset, 'offset')
					})
					if (json) {
						writeJson(jobs)
						return
					}
					console.log(`\n${jobs.length} job(s):\n`)
					for (const job of jobs) {
						console.log(`  ${job.id}  ${job.queueName}`)
						console.log(
							`    attempts: ${job.attempts}/${job.maxAttempts}  runAt: ${job.runAt}`
						)
						if (job.lastError) {
							console.log(`    lastError: ${job.lastError}`)
						}
					}
					console.log()
					break
				}

				case 'retry': {
					const [id] = args
					if (!id) {
						console.error('Error: Job id required')
						process.exitCode = 1
						return
					}
					const ids = await worker.retryJobs([id])
					if (json) {
						writeJson({ retried: ids })
						return
					}
					console.log(`Retried: ${ids.join(', ') || '(none)'}`)
					break
				}

				case 'fail': {
					const [id, ...reasonParts] = args
					if (!id) {
						console.error('Error: Job id required')
						process.exitCode = 1
						return
					}
					const reason = reasonParts.join(' ') || undefined
					const ids = await worker.failJobs([id], reason)
					if (json) {
						writeJson({ failed: ids })
						return
					}
					console.log(`Failed: ${ids.join(', ') || '(none)'}`)
					break
				}

				case 'run-once': {
					await worker.runOnce()
					if (json) {
						writeJson({ ok: true })
						return
					}
					console.log('run-once complete')
					break
				}

				default: {
					console.error(`Unknown command: ${command}`)
					console.log(HELP)
					process.exitCode = 1
				}
			}
		} catch (error) {
			const message = error instanceof Error ? error.message : String(error)
			if (json) {
				writeJson({ error: message })
			} else {
				console.error(`Error: ${message}`)
			}
			process.exitCode = 1
		}
	}
}
