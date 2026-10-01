import { compact } from './options.js'
import { parseArgs } from 'node:util'
import { z } from 'zod'
import type { QueueContract } from './queue.js'
import type { BetterWorker } from './create-better-worker.js'
import type { JobOptions } from './job-options.js'
import { assertInteger } from './validation.js'

const HELP = `better-graphile-worker CLI
Commands: list-queues | schema <queue> | create-job <queue> [json] | stats | list-jobs | retry <id> | fail <id> [reason] | run-once
Options: --json --priority <n> --run-at <iso> --max-attempts <n> --job-key <key> --queue <name> --state <pending|running|failed> --limit <n> --offset <n> -h, --help`
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
function required(value: string | undefined, label: string): string {
	if (!value) throw new Error(`${label} required`)
	return value
}
export function createCli<T extends readonly QueueContract[]>(
	worker: BetterWorker<T>
): (argv?: string[]) => Promise<void> {
	// Dynamic input always goes through the same registry and runtime validation as typed calls.
	const enqueue = worker.createJob as (
		name: string,
		payload?: unknown,
		options?: JobOptions
	) => Promise<string | null>
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
				case 'create-job': {
					const name = required(args[0], 'Queue name')
					const queue = worker.queues.find((q) => q.name === name)
					if (!queue) throw new Error(`Queue "${name}" not found`)
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
						args[1] === undefined ? undefined : JSON.parse(args[1])
					print({ jobId: await enqueue(name, payload, options) })
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
