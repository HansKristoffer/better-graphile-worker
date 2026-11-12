import { compact } from './options.js'
import { parseArgs } from 'node:util'
import { z } from 'zod'
import type { QueueContract } from './queue.js'
import type { BetterWorker } from './create-better-worker.js'
import type { BetterWorkerPool } from './worker-pool.js'
import type { Duration, JobOptions } from './job-options.js'
import { assertInteger } from './validation.js'

const HELP = `better-graphile-worker CLI
Commands: list-queues | schema <queue> | create-job <queue> [json] | trigger-cron <queue> | stats | list-jobs | get-job <id> | get-job --key <key> | retry <id> | fail <id> [reason] | release-locks [--all] | delete-failed <duration> | run-once
Options: --json --priority <n> --run-at <iso> --max-attempts <n> --job-key <key> --key <key> --queue <name> --state <pending|running|failed> --limit <n> --offset <n> --all -h, --help`
function number(
	value: string | undefined,
	label: string,
	min = 0,
	max = Number.MAX_SAFE_INTEGER
) {
	if (value === undefined) return undefined
	if (!/^-?\d+$/.test(value)) throw new RangeError(`Invalid ${label}: ${value}`)
	const parsed = Number(value)
	assertInteger(parsed, label, min, max)
	return parsed
}
/** `30d`, or plain milliseconds. */
function duration(value: string): Duration {
	return /^\d+$/.test(value) ? Number(value) : (value as Duration)
}
function required(value: string | undefined, label: string): string {
	if (!value) throw new Error(`${label} required`)
	return value
}
/** Commands that read or write the database; the rest only describe the registry. */
const DATABASE_COMMANDS = new Set([
	'create-job',
	'trigger-cron',
	'release-locks',
	'delete-failed',
	'stats',
	'list-jobs',
	'get-job',
	'retry',
	'fail',
	'run-once'
])

export type CliOptions = {
	/**
	 * Run `worker.migrate()` before a command that uses the database. Help, `list-queues`
	 * and `schema` never connect.
	 */
	migrate?: boolean | undefined
}

export function createCli<T extends readonly QueueContract[]>(
	worker: BetterWorker<T> | BetterWorkerPool<T>,
	options: CliOptions = {}
): (argv?: string[]) => Promise<void> {
	return async (argv = process.argv.slice(2)) => {
		const json = argv.includes('--json')
		const print = (value: unknown) =>
			console.log(JSON.stringify(value, null, json ? 2 : undefined))
		try {
			const {
				values,
				positionals: [command, ...args]
			} = parseArgs({
				args: argv,
				allowPositionals: true,
				options: {
					help: { type: 'boolean', short: 'h' },
					json: { type: 'boolean' },
					priority: { type: 'string' },
					'run-at': { type: 'string' },
					'max-attempts': { type: 'string' },
					'job-key': { type: 'string' },
					key: { type: 'string' },
					all: { type: 'boolean' },
					queue: { type: 'string' },
					state: { type: 'string' },
					limit: { type: 'string' },
					offset: { type: 'string' }
				}
			})
			if (values.help || !command) {
				console.log(HELP)
				return
			}
			if (options.migrate && DATABASE_COMMANDS.has(command))
				await worker.migrate()
			switch (command) {
				case 'list-queues':
					print(
						worker
							.getQueueDefinitions()
							.map(({ inputSchema, ...definition }) => ({
								...definition,
								inputSchema: inputSchema
									? z.toJSONSchema(inputSchema, { io: 'input' })
									: null
							}))
					)
					break
				case 'schema': {
					const name = required(args[0], 'Queue name')
					const queue = worker
						.getQueueDefinitions()
						.find((q) => q.name === name)
					if (!queue) throw new Error(`Queue "${name}" not found`)
					print({
						name,
						type: queue.type,
						cron: queue.cron,
						schema: queue.inputSchema
							? z.toJSONSchema(queue.inputSchema, { io: 'input' })
							: null
					})
					break
				}
				case 'trigger-cron':
				case 'create-job': {
					const name = required(args[0], 'Queue name')
					const queue = worker.queues.find((q) => q.name === name)
					if (!queue) throw new Error(`Queue "${name}" not found`)
					if (command === 'trigger-cron' && queue.cron === undefined)
						throw new Error(`Queue "${name}" has no cron schedule`)
					const options: JobOptions = {}
					const priority = number(values.priority, 'priority', -32768, 32767)
					const maxAttempts = number(
						values['max-attempts'],
						'max-attempts',
						1,
						32767
					)
					if (priority !== undefined) options.priority = priority
					if (maxAttempts !== undefined) options.maxAttempts = maxAttempts
					if (values['job-key'] !== undefined)
						options.jobKey = values['job-key']
					if (values['run-at'] !== undefined) {
						options.runAt = new Date(values['run-at'])
						if (!Number.isFinite(options.runAt.getTime()))
							throw new RangeError('Invalid run-at timestamp')
					}
					const payload: unknown =
						command === 'trigger-cron' || args[1] === undefined
							? undefined
							: JSON.parse(args[1])
					// A cron name without a payload triggers its run, as trigger-cron does.
					print({
						jobId: await worker.enqueueByName(name, payload, options)
					})
					break
				}
				case 'stats':
					print(await worker.getJobStats())
					break
				case 'list-jobs': {
					const state = values.state
					if (
						state !== undefined &&
						state !== 'pending' &&
						state !== 'running' &&
						state !== 'failed'
					)
						throw new Error('--state must be pending, running, or failed')
					print(
						await worker.listJobs(
							compact({
								queue: values.queue,
								state,
								limit: number(values.limit, 'limit', 0, 1000),
								offset: number(values.offset, 'offset')
							})
						)
					)
					break
				}
				case 'get-job':
					print(
						values.key === undefined
							? await worker.getJob(required(args[0], 'Job id'))
							: await worker.getJobByKey(values.key)
					)
					break
				case 'release-locks':
					print({
						released: await worker.releaseLocks({ all: values.all === true })
					})
					break
				case 'delete-failed':
					print({
						deleted: await worker.deleteFailedJobs(
							duration(required(args[0], 'Duration'))
						)
					})
					break
				case 'retry':
					print({
						retried: await worker.retryJobs([required(args[0], 'Job id')])
					})
					break
				case 'fail':
					print({
						failed: await worker.failJobs(
							[required(args[0], 'Job id')],
							args.slice(1).join(' ') || undefined
						)
					})
					break
				case 'run-once':
					await worker.runOnce()
					print({ ok: true })
					break
				default:
					throw new Error(`Unknown command: ${command}`)
			}
		} catch (error) {
			const message = error instanceof Error ? error.message : String(error)
			if (json) print({ error: message })
			else console.error(`Error: ${message}`)
			process.exitCode = 1
		}
	}
}
