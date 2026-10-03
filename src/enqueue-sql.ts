import type { AddJobsJobSpec, TaskSpec } from 'graphile-worker'
import { quoteSchemaName } from './schema-name.js'

/** Driver-neutral SQL: scalar parameters only, so pg, Prisma and others run it alike. */
export type SqlStatement = {
	text: string
	values: (string | number | boolean | null)[]
}

const ID_COLUMN = 'jobs.id::text AS id'

function timestamp(value: TaskSpec['runAt']): string | null {
	if (value === undefined || value === null) return null
	return value instanceof Date ? value.toISOString() : String(value)
}

/** The single SQL path for `add_job`, shared by immediate and prepared enqueue. */
export function addJobSql(
	schema: string,
	identifier: string,
	payload: unknown,
	spec: TaskSpec = {},
	select = ID_COLUMN
): SqlStatement {
	return {
		text: `SELECT ${select} FROM ${quoteSchemaName(schema)}.add_job(
	identifier := $1::text, payload := $2::json, queue_name := $3::text,
	run_at := $4::timestamptz, max_attempts := $5::int, job_key := $6::text,
	priority := $7::int,
	flags := (SELECT array_agg(flag) FROM json_array_elements_text($8::json) AS flag),
	job_key_mode := $9::text
) AS jobs`,
		values: [
			identifier,
			JSON.stringify(payload),
			spec.queueName ?? null,
			timestamp(spec.runAt),
			spec.maxAttempts ?? null,
			spec.jobKey ?? null,
			spec.priority ?? null,
			spec.flags ? JSON.stringify(spec.flags) : null,
			spec.jobKeyMode ?? 'replace'
		]
	}
}

/**
 * One `add_jobs` call, returned in spec order. Graphile's join may reorder inserts, so
 * keyed rows are matched by key and unkeyed rows by content (payload text, run_at,
 * max_attempts, priority, flags); specs with equal content are interchangeable.
 * Keys must be unique within one call.
 */
export function addJobsSql(
	schema: string,
	specs: readonly AddJobsJobSpec[],
	jobKeyPreserveRunAt = false,
	select = ID_COLUMN
): SqlStatement {
	assertDistinguishable(specs)
	const quoted = quoteSchemaName(schema)
	return {
		text: `WITH input AS (
	SELECT json_populate_record(NULL::${quoted}.job_spec, item) AS spec, ord
	FROM json_array_elements($1::json) WITH ORDINALITY AS t(item, ord)
), jobs AS (
	SELECT * FROM ${quoted}.add_jobs(ARRAY(SELECT spec FROM input ORDER BY ord), $2::boolean)
), added AS (
	-- One equality column lets PostgreSQL hash-join; keyed rows match by key.
	SELECT id, coalesce('k' || key, 'c' || row_number() OVER (PARTITION BY key IS NULL, fp ORDER BY id) || fp) AS match
	FROM (
		SELECT id, key, jsonb_build_array(run_at, max_attempts, priority, flags)::text || payload::text AS fp
		FROM jobs
	) AS rows
), wanted AS (
	SELECT ord, coalesce('k' || key, 'c' || row_number() OVER (PARTITION BY key IS NULL, fp ORDER BY ord) || fp) AS match
	FROM (
		SELECT ord, (spec).job_key AS key, jsonb_build_array(
			coalesce((spec).run_at, now()), coalesce((spec).max_attempts, 25), coalesce((spec).priority, 0),
			(SELECT jsonb_object_agg(flag, true) FROM unnest((spec).flags) AS flag)
		)::text || coalesce((spec).payload, '{}'::json)::text AS fp
		FROM input
	) AS rows
)
SELECT ${select} FROM wanted
JOIN added ON added.match = wanted.match
JOIN jobs ON jobs.id = added.id
ORDER BY wanted.ord`,
		values: [
			JSON.stringify(
				specs.map((spec) => ({
					identifier: spec.identifier,
					payload: spec.payload,
					queue_name: spec.queueName,
					run_at: timestamp(spec.runAt) ?? undefined,
					max_attempts: spec.maxAttempts,
					job_key: spec.jobKey,
					priority: spec.priority,
					flags: spec.flags
				}))
			),
			jobKeyPreserveRunAt
		]
	}
}

/** Unkeyed specs that match by content must also agree on task and lane. */
function assertDistinguishable(specs: readonly AddJobsJobSpec[]): void {
	const seen = new Map<string, string>()
	for (const spec of specs) {
		if (spec.jobKey) continue
		const content = JSON.stringify([
			spec.payload,
			timestamp(spec.runAt),
			spec.maxAttempts,
			spec.priority,
			spec.flags
		])
		const target = JSON.stringify([spec.identifier, spec.queueName ?? null])
		if ((seen.get(content) ?? target) !== target)
			throw new Error(
				'addJobs cannot match identical unkeyed specs that differ only by identifier or queueName; enqueue them separately'
			)
		seen.set(content, target)
	}
}
