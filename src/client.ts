import {
	makeWorkerUtils,
	type AddJobsJobSpec,
	type DbJob,
	type Job,
	type TaskSpec,
	type WorkerUtils
} from 'graphile-worker'
import type { Pool } from 'pg'
import { assertValidSchemaName, quoteSchemaName } from './schema-name.js'

export const DEFAULT_GRAPHILE_WORKER_SCHEMA = 'graphile_worker'
/** The producer's public-SQL adapter deliberately performs no migrations. */
export type EnqueueAdapter = {
	addJob(identifier: string, payload: unknown, spec?: TaskSpec): Promise<Job>
	addJobs(specs: readonly AddJobsJobSpec[]): Promise<readonly Job[]>
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
			const result = await options.pgPool.query<DbJob>(
				`SELECT * FROM ${sqlSchema}.add_job(
				identifier := $1::text, payload := $2::json, queue_name := $3::text,
				run_at := $4::timestamptz, max_attempts := $5::int, job_key := $6::text,
				priority := $7::int, flags := $8::text[], job_key_mode := $9::text
			)`,
				[
					identifier,
					JSON.stringify(payload),
					spec.queueName ?? null,
					spec.runAt ?? null,
					spec.maxAttempts ?? null,
					spec.jobKey ?? null,
					spec.priority ?? null,
					spec.flags ?? null,
					spec.jobKeyMode ?? 'replace'
				]
			)
			const job = result.rows[0]
			if (!job) throw new Error('Graphile add_job returned no job')
			return { ...job, task_identifier: identifier }
		},
		async addJobs(specs) {
			if (!specs.length) return []
			const dbSpecs = specs.map((spec) => ({
				identifier: spec.identifier,
				payload: spec.payload,
				queue_name: spec.queueName,
				run_at: spec.runAt,
				max_attempts: spec.maxAttempts,
				job_key: spec.jobKey,
				priority: spec.priority,
				flags: spec.flags
			}))
			const result = await options.pgPool.query<DbJob>(
				`SELECT * FROM ${sqlSchema}.add_jobs(
				ARRAY(SELECT json_populate_recordset(NULL::${sqlSchema}.job_spec, $1::json)), false
			)`,
				[JSON.stringify(dbSpecs)]
			)
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
