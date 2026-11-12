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
	JOB_KEY_RACE_RERUNS,
	type IdempotentJobSpec,
	type SqlStatement
} from './enqueue-sql.js'
import { EnqueueCountError } from './errors.js'
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
	/** Resolves to the new job's id; adapters may return more of the row. */
	addJob(
		identifier: string,
		payload: unknown,
		spec?: TaskSpec
	): Promise<Pick<Job, 'id'>>
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
	/** `migrate()` once per client; a failed migration is tried again on the next call. */
	ensureMigrated(): Promise<void>
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
	let migrated: Promise<void> | undefined
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
	/**
	 * A batch that lost a key race has already inserted its other jobs, so it runs in a
	 * transaction that rolls back before each rerun.
	 */
	async function addKeyedJobs(
		text: string,
		values: SqlStatement['values'],
		specs: readonly AddJobsJobSpec[]
	): Promise<DbJob[]> {
		const client = await options.pgPool.connect()
		try {
			for (let run = 0; ; run++) {
				await client.query('BEGIN')
				let rows: DbJob[]
				try {
					rows = (await client.query<DbJob>(text, values)).rows
				} catch (error) {
					await client.query('ROLLBACK').catch(() => {})
					throw error
				}
				if (rows.length === specs.length || run === JOB_KEY_RACE_RERUNS) {
					await client.query(
						rows.length === specs.length ? 'COMMIT' : 'ROLLBACK'
					)
					return rows
				}
				await client.query('ROLLBACK')
			}
		} finally {
			client.release()
		}
	}
	async function migrate() {
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
	}
	const enqueue: EnqueueAdapter = {
		async addJob(identifier, payload, spec = {}) {
			// Only the id: returning the row would send the payload back over the wire.
			const { text, values } = addJobSql(schema, identifier, payload, spec)
			for (let run = 0; ; run++) {
				const job = (await options.pgPool.query<{ id: string }>(text, values))
					.rows[0]
				if (job?.id) return { id: job.id }
				if (spec.jobKey === undefined || run === JOB_KEY_RACE_RERUNS)
					throw new EnqueueCountError(identifier, 1, 0)
			}
		},
		async addJobs(specs, jobKeyPreserveRunAt = false) {
			if (!specs.length) return []
			const { text, values } = addJobsSql(
				schema,
				specs,
				jobKeyPreserveRunAt,
				'jobs.*'
			)
			const rows = specs.some((spec) => spec.jobKey !== undefined)
				? await addKeyedJobs(text, values, specs)
				: (await options.pgPool.query<DbJob>(text, values)).rows
			if (rows.length !== specs.length)
				throw new EnqueueCountError(
					specs[0]!.identifier,
					specs.length,
					rows.length
				)
			return rows.map((job, index) => ({
				...job,
				task_identifier: specs[index]!.identifier
			}))
		},
		async addIdempotentJobs(specs) {
			if (!specs.length) return []
			const { text, values } = addIdempotentJobsSql(schema, specs)
			const result = await options.pgPool.query<EnqueuedJob>(text, values)
			if (result.rows.length !== specs.length)
				throw new EnqueueCountError(
					specs[0]!.identifier,
					specs.length,
					result.rows.length
				)
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
		migrate,
		ensureMigrated() {
			migrated ??= migrate().catch((error: unknown) => {
				migrated = undefined
				throw error
			})
			return migrated
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
