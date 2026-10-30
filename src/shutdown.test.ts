import { afterAll, describe, expect, test } from 'bun:test'
import { createServer, connect, type Server, type Socket } from 'node:net'
import { Pool } from 'pg'
import { z } from 'zod'
import { createBetterWorker } from './create-better-worker.js'
import { createBetterWorkerPool } from './worker-pool.js'
import { DatabaseUnreachableError } from './errors.js'
import { defineQueue } from './queue.js'
import { databaseAnswers } from './shutdown.js'

const quiet = { debug() {}, info() {}, warn() {}, error() {} }

/** A local server that handles each connection with `onConnection`. */
async function listen(onConnection: (socket: Socket) => void) {
	const sockets = new Set<Socket>()
	const server: Server = createServer((socket) => {
		sockets.add(socket)
		socket.on('error', () => {})
		onConnection(socket)
	})
	await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
	const address = server.address()
	if (!address || typeof address === 'string') throw new Error('no port')
	return {
		port: address.port,
		sockets,
		close: async () => {
			for (const socket of sockets) socket.destroy()
			await new Promise((resolve) => server.close(resolve))
		}
	}
}

/** A PostgreSQL ErrorResponse, as a server sends one instead of accepting a login. */
function errorResponse(code: string, message: string) {
	const fields = Buffer.from(`SFATAL\0C${code}\0M${message}\0\0`)
	const header = Buffer.alloc(5)
	header.write('E')
	header.writeInt32BE(fields.length + 4, 1)
	return Buffer.concat([header, fields])
}

describe('the shutdown database probe', () => {
	const probe = (port: number) =>
		databaseAnswers(
			new Pool({ connectionString: `postgres://u:p@127.0.0.1:${port}/db` }),
			100
		)

	test('silence and a refused connection are unreachable', async () => {
		const silent = await listen(() => {})
		try {
			expect(await probe(silent.port)).toBe(false)
		} finally {
			await silent.close()
		}
		const closed = await listen(() => {})
		await closed.close()
		expect(await probe(closed.port)).toBe(false)
	})

	test('a server that answers with an error is reachable', async () => {
		const full = await listen((socket) =>
			socket.once('data', () =>
				socket.end(errorResponse('53300', 'too many clients'))
			)
		)
		try {
			expect(await probe(full.port)).toBe(true)
		} finally {
			await full.close()
		}
	})

	test('rejects invalid probe options before stopping', () => {
		const worker = createBetterWorker({
			pgPool: {} as Pool,
			queues: [defineQueue({ name: 'q', inputSchema: z.string() })],
			handlers: { q: () => {} }
		})
		expect(() => worker.stop({ probeDatabase: { timeout: 0 } })).toThrow(
			'probeDatabase.timeout'
		)
	})
})

const suite = describe.skipIf(!process.env.DATABASE_URL)
const schema = `bgw_shutdown_${process.pid}`

/**
 * A TCP proxy to PostgreSQL. `partition()` stops forwarding without closing anything, as a
 * server that died or a network that dropped looks to the client: queries never return.
 */
async function databaseProxy(url: URL) {
	let partitioned = false
	let connections = 0
	const waiting: { count: number; resolve(): void }[] = []
	const proxy = await listen((client) => {
		connections++
		for (const wait of waiting.filter((w) => connections >= w.count))
			wait.resolve()
		if (partitioned) return
		const upstream = connect(Number(url.port || 5432), url.hostname)
		proxy.sockets.add(upstream)
		upstream.on('error', () => client.destroy())
		client.on('close', () => upstream.destroy())
		upstream.on('close', () => client.destroy())
		client.on('data', (data) => partitioned || upstream.write(data))
		upstream.on('data', (data) => partitioned || client.write(data))
	})
	const proxied = new URL(url)
	proxied.hostname = '127.0.0.1'
	proxied.port = String(proxy.port)
	return {
		url: proxied.toString(),
		/** Resolves once `more` connections beyond the current count were opened. */
		connections: (more: number) =>
			new Promise<void>((resolve) =>
				waiting.push({ count: connections + more, resolve })
			),
		partition: () => {
			partitioned = true
		},
		close: proxy.close
	}
}

suite('stop({ probeDatabase }) on PostgreSQL', () => {
	const direct = new Pool({ connectionString: process.env.DATABASE_URL })
	direct.on('error', () => {})
	afterAll(async () => {
		await direct.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
		await direct.end()
	})

	/** A started worker, behind a proxy, running a job until `finish()` is called. */
	async function runningJob(pool: 'worker' | 'pool') {
		const proxy = await databaseProxy(new URL(String(process.env.DATABASE_URL)))
		const pgPool = new Pool({ connectionString: proxy.url })
		pgPool.on('error', () => {})
		pgPool.on('connect', (client) => client.on('error', () => {}))
		let started!: () => void
		const running = new Promise<void>((resolve) => {
			started = resolve
		})
		let finish!: () => void
		const finished = new Promise<void>((resolve) => {
			finish = resolve
		})
		const hold = defineQueue({
			name: 'hold',
			runner: 'only',
			inputSchema: z.string(),
			processFn: async () => {
				started()
				await finished
			}
		})
		const options = {
			pgPool,
			schema,
			queues: [hold],
			hooks: { createLogger: () => quiet },
			graphile: { gracefulShutdownAbortTimeout: 60_000 }
		}
		const worker =
			pool === 'worker'
				? createBetterWorker(options)
				: createBetterWorkerPool({ ...options, runners: { only: {} } })
		await worker.migrate()
		await worker.start()
		await worker.createJob('hold', 'x')
		await running
		return {
			worker,
			proxy,
			finish,
			async close() {
				await proxy.close()
				await worker.waitUntilStopped().catch(() => {})
				await pgPool.end()
			}
		}
	}
	const probeDatabase = { interval: 10, timeout: 300 }

	test('waits for running jobs while the database answers', async () => {
		const { worker, proxy, finish, close } = await runningJob('worker')
		try {
			const probed = proxy.connections(2)
			const stopping = worker.stop({ probeDatabase })
			// Two probes answered while the job was still running.
			await probed
			finish()
			await stopping
		} finally {
			await close()
		}
	})

	for (const kind of ['worker', 'pool'] as const)
		test(`a ${kind} stops waiting once the database stops answering`, async () => {
			const { worker, proxy, finish, close } = await runningJob(kind)
			try {
				const stopping = worker.stop({ probeDatabase })
				proxy.partition()
				// Graphile now waits forever to record the job's completion.
				finish()
				await expect(stopping).rejects.toBeInstanceOf(DatabaseUnreachableError)
			} finally {
				await close()
			}
		})
})
