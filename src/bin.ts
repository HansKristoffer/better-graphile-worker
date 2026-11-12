#!/usr/bin/env node
/**
 * Commands that need only a connection string, for scripts and start hooks that cannot
 * load the app's queue registry. `createCli` covers everything else.
 */
import { parseArgs } from 'node:util'
import pg from 'pg'
import { DEFAULT_GRAPHILE_WORKER_SCHEMA } from './client.js'
import { releaseLocks, resolveCrashRecovery } from './maintenance.js'
import { assertValidSchemaName } from './schema-name.js'

const HELP = `Usage: better-graphile-worker release-locks [--all] [--schema <name>] [--stale-after <ms>] [connection string]
Release jobs and lanes whose lock owner looks dead; --all releases every lock, for when nothing runs.
The connection string defaults to DATABASE_URL.`

async function main(argv = process.argv.slice(2)): Promise<void> {
	const { values, positionals } = parseArgs({
		args: argv,
		allowPositionals: true,
		options: {
			help: { type: 'boolean', short: 'h' },
			all: { type: 'boolean' },
			schema: { type: 'string' },
			'stale-after': { type: 'string' }
		}
	})
	const [command, connectionString = process.env.DATABASE_URL] = positionals
	if (values.help || command !== 'release-locks') {
		console.log(HELP)
		if (!values.help) process.exitCode = 1
		return
	}
	if (!connectionString)
		throw new Error('Pass a connection string or set DATABASE_URL')
	const schema = assertValidSchemaName(
		values.schema ?? DEFAULT_GRAPHILE_WORKER_SCHEMA
	)
	const staleAfter =
		values['stale-after'] === undefined
			? resolveCrashRecovery(true)!.staleAfter
			: Number(values['stale-after'])
	if (!Number.isSafeInteger(staleAfter) || staleAfter < 1)
		throw new RangeError('--stale-after must be a positive number of ms')
	const pool = new pg.Pool({ connectionString, max: 1 })
	pool.on('error', () => {})
	try {
		const client = await pool.connect()
		try {
			const released = await releaseLocks(client, schema, staleAfter, {
				all: values.all === true
			})
			console.log(JSON.stringify({ released: released.map((job) => job.id) }))
		} finally {
			client.release()
		}
	} finally {
		await pool.end()
	}
}

await main().catch((error: unknown) => {
	console.error(
		`Error: ${error instanceof Error ? error.message : String(error)}`
	)
	process.exitCode = 1
})
