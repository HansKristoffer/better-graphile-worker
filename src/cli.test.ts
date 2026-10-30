import { afterAll, describe, expect, test, spyOn } from 'bun:test'
import { z } from 'zod'
import { Pool } from 'pg'
import { createCli } from './cli.js'
import { createBetterWorker } from './create-better-worker.js'
import { defineQueue } from './queue.js'

const worker = createBetterWorker({
	// Any use of the pool throws, so these tests open no connection.
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
async function output(args: string[], cli = createCli(worker)) {
	const lines: unknown[] = []
	const previous = process.exitCode
	const log = spyOn(console, 'log').mockImplementation((value) => {
		const text = String(value)
		// Help is plain text even with --json.
		lines.push(
			text.startsWith('better-graphile-worker CLI') ? text : JSON.parse(text)
		)
	})
	try {
		process.exitCode = 0
		await cli([...args, '--json'])
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
test('with migrate, only database commands migrate first', async () => {
	const migrate = spyOn(worker, 'migrate').mockResolvedValue()
	try {
		const cli = createCli(worker, { migrate: true })
		const codes = []
		for (const args of [
			['list-queues'],
			['schema', 'count'],
			['--help'],
			[],
			['unknown']
		])
			codes.push((await output(args, cli)).code)
		expect(codes).toEqual([0, 0, 0, 0, 1])
		expect(migrate).not.toHaveBeenCalled()
		const created = await output(['create-job', 'count', '"text"'], cli)
		expect(created).toEqual({ lines: [{ jobId: null }], code: 0 })
		expect(migrate).toHaveBeenCalledTimes(1)
	} finally {
		migrate.mockRestore()
	}
})
test('a failed migration takes the JSON error path', async () => {
	const migrate = spyOn(worker, 'migrate').mockRejectedValue(
		new Error('database unreachable')
	)
	try {
		const result = await output(['stats'], createCli(worker, { migrate: true }))
		expect(result).toEqual({
			lines: [{ error: 'database unreachable' }],
			code: 1
		})
	} finally {
		migrate.mockRestore()
	}
})

const suite = describe.skipIf(!process.env.DATABASE_URL)
const schema = `bgw_cli_${process.pid}`

suite('CLI on PostgreSQL', () => {
	const pool = new Pool({ connectionString: process.env.DATABASE_URL })
	pool.on('error', () => {})
	afterAll(async () => {
		await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
		await pool.end()
	})

	test('metadata commands open no connection; database commands migrate first', async () => {
		const migrating = createBetterWorker({
			pgPool: pool,
			schema,
			queues: [defineQueue({ name: 'echo', inputSchema: z.string() })],
			handlers: { echo: async () => {} }
		})
		const cli = createCli(migrating, { migrate: true })
		await output(['list-queues'], cli)
		await output(['schema', 'echo'], cli)
		expect(pool.totalCount).toBe(0)
		const created = await output(['create-job', 'echo', '"hi"'], cli)
		expect(created.code).toBe(0)
		expect(await output(['stats'], cli)).toMatchObject({
			code: 0,
			lines: [expect.arrayContaining([expect.objectContaining({ pending: 1 })])]
		})
	})
})
