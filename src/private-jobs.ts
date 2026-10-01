/** Version-specific SQL for Graphile Worker 0.17.x; keep private-table access here. */
import type { JobHelpers } from 'graphile-worker'
import { StepPersistenceError } from './errors.js'
import { quoteSchemaName } from './schema-name.js'
export async function retainPermanentFailure(
	helpers: JobHelpers,
	schema: string
): Promise<void> {
	const result = await helpers.withPgClient((client) =>
		client.query(
			`UPDATE ${quoteSchemaName(schema)}._private_jobs SET max_attempts = attempts WHERE id = $1::bigint AND locked_by = $2::text AND locked_at IS NOT NULL`,
			[helpers.job.id, helpers.job.locked_by]
		)
	)
	if (result.rowCount !== 1)
		throw new StepPersistenceError(String(helpers.job.id))
	// Throwing the original error lets Graphile persist last_error and release its lock.
}

export async function writeCheckpoint(
	helpers: JobHelpers,
	schema: string,
	jobId: string,
	id: string,
	checkpoint: unknown
): Promise<void> {
	const quoted = quoteSchemaName(schema)
	const result = await helpers.withPgClient((client) =>
		client.query(
			`WITH owned AS (
		SELECT id, payload::jsonb AS data FROM ${quoted}._private_jobs
		WHERE id = $1::bigint AND locked_by = $4::text AND locked_at IS NOT NULL FOR UPDATE
	) UPDATE ${quoted}._private_jobs jobs SET
		payload = jsonb_set(
			owned.data,
			'{steps}', CASE WHEN jsonb_typeof(owned.data->'steps') = 'object' THEN owned.data->'steps' ELSE '{}'::jsonb END || jsonb_build_object($2::text, $3::jsonb), true
		), updated_at = now()
		FROM owned WHERE jobs.id = owned.id AND owned.data->>'__bgw' = '2' AND owned.data ? 'payload'`,
			[jobId, id, JSON.stringify(checkpoint), helpers.job.locked_by]
		)
	)

	if (result.rowCount !== 1) throw new StepPersistenceError(jobId)
}

export function payloadDebugJoin(quotedSchema: string): {
	column: string
	join: string
} {
	return {
		column: 'private_job.payload',
		join: `JOIN ${quotedSchema}._private_jobs private_job ON private_job.id = jobs.id`
	}
}
