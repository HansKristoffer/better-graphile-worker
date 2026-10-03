import {
	makeWorkerUtils,
	type AddJobsJobSpec,
	type DbJob,
	type Job,
	type TaskSpec,
	type WorkerUtils
} from 'graphile-worker'
import type { Pool } from 'pg'
import { addJobSql, addJobsSql } from './enqueue-sql.js'
import { assertValidSchemaName, quoteSchemaName } from './schema-name.js'

export const DEFAULT_GRAPHILE_WORKER_SCHEMA = 'graphile_worker'
/** The producer's public-SQL adapter deliberately performs no migrations. */
export type EnqueueAdapter = {
	addJob(identifier: string, payload: unknown, spec?: TaskSpec): Promise<Job>
	/** Return jobs in spec order and honor Graphile's batch preserve_run_at flag. */
	addJobs(
		specs: readonly AddJobsJobSpec[],
		jobKeyPreserveRunAt?: boolean
	): Promise<readonly Job[]>
}
export type WorkerClient = {
	readonly enqueue: EnqueueAdapter
	withPgClient: WorkerUtils['withPgClient']
	getUtils(): Promise<WorkerUtils>
	migrate(): Promise<void>
	release(): Promise<void>
	retryJobs(ids: readonly string[]): Promise<string[]>
}

export function createWorkerClient(options: {
	pgPool: Pool
	schema?: string | undefined
}): WorkerClient {
	const schema = assertValidSchemaName(
		options.schema ?? DEFAULT_GRAPHILE_WORKER_SCHEMA
	)
	const sqlSchema = quoteSchemaName(schema)
	let pending: Promise<WorkerUtils> | undefined
	let releasing: Promise<void> | undefined
	async function getUtils(): Promise<WorkerUtils> {
		if (releasing) await releasing
		if (!pending) {
			pending = makeWorkerUtils({ pgPool: options.pgPool, schema })
			const current = pending
			void current.catch(() => {
				if (pending === current) pending = undefined
			})
		}
		return pending
	}
	function release(): Promise<void> {
		if (releasing) return releasing
		const current = pending
		pending = undefined
		releasing = (async () => {
			if (current) {
				let utils: WorkerUtils
				try {
					utils = await current
				} catch {
					return
				}
				await utils.release()
			}
		})().finally(() => {
			releasing = undefined
		})
		return releasing
	}
	const enqueue: EnqueueAdapter = {
		async addJob(identifier, payload, spec = {}) {
			const { text, values } = addJobSql(schema, identifier, payload, spec, '*')
			const job = (await options.pgPool.query<DbJob>(text, values)).rows[0]
			if (!job?.id) throw new Error('Graphile add_job returned no job')
			return { ...job, task_identifier: identifier }
		},
		async addJobs(specs, jobKeyPreserveRunAt = false) {
			if (!specs.length) return []
			const { text, values } = addJobsSql(
				schema,
				specs,
				jobKeyPreserveRunAt,
				'jobs.*'
			)
			const result = await options.pgPool.query<DbJob>(text, values)
			if (result.rows.length !== specs.length)
				throw new Error('Graphile add_jobs returned fewer jobs than requested')
			return result.rows.map((job, index) => ({
				...job,
				task_identifier: specs[index]!.identifier
			}))
		}
	}
	return {
		withPgClient: async (fn) => {
			const connection = await options.pgPool.connect()
			try {
				return await fn(connection)
			} finally {
				connection.release()
			}
		},
		enqueue,
		getUtils,
		release,
		async migrate() {
			await getUtils()
		}, // makeWorkerUtils initializes/migrates once.
		async retryJobs(ids) {
			if (!ids.length) return []
			const result = await options.pgPool.query<{ id: string }>(
				`SELECT id FROM ${sqlSchema}.reschedule_jobs(
				$1::bigint[], attempts := 0::smallint, run_at := now()
			)`,
				[ids]
			)
			return result.rows.map((job) => String(job.id))
		}
	}
}
