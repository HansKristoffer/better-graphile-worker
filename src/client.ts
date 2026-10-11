import {
	makeWorkerUtils,
	type AddJobsJobSpec,
	type DbJob,
	type Job,
	type TaskSpec,
	type WorkerUtils
} from 'graphile-worker'
import type { Pool } from 'pg'
import {
	addIdempotentJobsSql,
	addJobSql,
	addJobsSql,
	IDEMPOTENCY_KEYS_TABLE,
	idempotencyTablesSql,
	type IdempotentJobSpec
} from './enqueue-sql.js'
import { HEARTBEATS_TABLE, heartbeatTableSql } from './maintenance.js'
import { JOB_OUTCOMES_TABLE, outcomeTablesSql } from './job-outcomes.js'
import { WAIT_GROUPS_TABLE, WAITS_TABLE, waitTablesSql } from './waits.js'
import { assertValidSchemaName, quoteSchemaName } from './schema-name.js'

/** Tables this library owns, beside Graphile's in its schema. */
const LIBRARY_TABLES = [
	IDEMPOTENCY_KEYS_TABLE,
	HEARTBEATS_TABLE,
	JOB_OUTCOMES_TABLE,
	WAIT_GROUPS_TABLE,
	WAITS_TABLE
]

export const DEFAULT_GRAPHILE_WORKER_SCHEMA = 'graphile_worker'
/** The producer's public-SQL adapter deliberately performs no migrations. */
export type EnqueueAdapter = {
	addJob(identifier: string, payload: unknown, spec?: TaskSpec): Promise<Job>
	/** Return jobs in spec order and honor Graphile's batch preserve_run_at flag. */
	addJobs(
		specs: readonly AddJobsJobSpec[],
		jobKeyPreserveRunAt?: boolean
	): Promise<readonly Job[]>
	/**
	 * Enqueue in spec order, honoring each spec's idempotency key. Required to enqueue with
	 * `idempotencyKey`; adapters without it reject such jobs.
	 */
	addIdempotentJobs?(
		specs: readonly IdempotentJobSpec[]
	): Promise<readonly EnqueuedJob[]>
}
/** `deduplicated` is true when an idempotency key returned an earlier job instead. */
export type EnqueuedJob = { id: string; deduplicated: boolean }
export type WorkerClient = {
	readonly enqueue: EnqueueAdapter
	withPgClient: WorkerUtils['withPgClient']
	getUtils(): Promise<WorkerUtils>
	migrate(): Promise<void>
	/** Whether `migrate()` has created this library's tables in the schema. */
	hasTables(): Promise<boolean>
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
		},
		async addIdempotentJobs(specs) {
			if (!specs.length) return []
			const { text, values } = addIdempotentJobsSql(schema, specs)
			const result = await options.pgPool.query<EnqueuedJob>(text, values)
			if (result.rows.length !== specs.length)
				throw new Error('Idempotent enqueue returned fewer jobs than requested')
			return result.rows
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
			await getUtils() // makeWorkerUtils initializes/migrates once.
			// Text without values runs as one implicit transaction; the lock serializes
			// concurrent migrations, which would otherwise collide on the catalog.
			await options.pgPool.query(
				[
					`SELECT pg_advisory_xact_lock(hashtext('better-graphile-worker:migrate'));`,
					idempotencyTablesSql(schema),
					heartbeatTableSql(schema),
					outcomeTablesSql(schema),
					waitTablesSql(schema)
				].join('\n')
			)
		},
		async hasTables() {
			const result = await options.pgPool.query<{ ready: boolean }>(
				'SELECT bool_and(to_regclass($1 || name) IS NOT NULL) AS ready FROM unnest($2::text[]) AS name',
				[`${sqlSchema}.`, LIBRARY_TABLES]
			)
			return result.rows[0]?.ready === true
		},
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
