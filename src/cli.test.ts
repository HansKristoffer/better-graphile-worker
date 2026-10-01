import { expect, test, spyOn } from 'bun:test'
import { z } from 'zod'
import type { Pool } from 'pg'
import { createCli } from './cli.js'
import { createBetterWorker } from './create-better-worker.js'
import { defineQueue } from './queue.js'

const worker = createBetterWorker({
	pgPool: {} as Pool,
	queues: [
		defineQueue({
			name: 'count',
			inputSchema: z.string().transform((value) => value.length)
		})
	],
	handlers: { count: async () => {} },
	hooks: { shouldSkipEnqueue: () => true }
})
async function output(args: string[]) {
	const lines: unknown[] = []
	const previous = process.exitCode
	const log = spyOn(console, 'log').mockImplementation((value) => {
		lines.push(JSON.parse(String(value)))
	})
	try {
		process.exitCode = 0
		await createCli(worker)([...args, '--json'])
		return { lines, code: process.exitCode }
	} finally {
		log.mockRestore()
		process.exitCode = previous
	}
}
test('CLI describes schema input rather than transformed output', async () => {
	const { lines, code } = await output(['schema', 'count'])
	expect(code).toBe(0)
	expect(lines[0]).toMatchObject({ name: 'count', schema: { type: 'string' } })
	const queues = await output(['list-queues'])
	expect(queues.lines[0]).toMatchObject([
		{ name: 'count', inputSchema: { type: 'string' } }
	])
})
test('CLI reports malformed arguments through the JSON error path', async () => {
	for (const args of [
		['create-job', 'count', '"text"', '--priority', '10junk'],
		['create-job', 'count', '"text"', '--max-attempts', '0'],
		['create-job', 'count', '"text"', '--run-at', 'invalid'],
		['list-jobs', '--limit', '1.5'],
		['list-jobs', '--limit', '1001'],
		['--unknown-option'],
		['schema', 'missing']
	]) {
		const result = await output(args)
		expect(result.code).toBe(1)
		expect(result.lines).toHaveLength(1)
		expect(result.lines[0]).toHaveProperty('error')
	}
})
