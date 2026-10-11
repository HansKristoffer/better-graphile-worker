import type { QueueContract } from './queue.js'
import {
	ChildJobFailedError,
	JobValidationError,
	WaitTimeoutError
} from './errors.js'
import { isPlainObject } from './payload.js'
import { quoteSchemaName } from './schema-name.js'
import { assertInteger, assertJsonValue, type JsonValue } from './validation.js'

export type WaitOptions = {
	/**
	 * Milliseconds, counted from the first time this wait enqueued its children. Past it,
	 * the wait throws `WaitTimeoutError` if children are still unfinished.
	 */
	timeout?: number
}

/** One child's final state, in input order. `id` is the child's job id. */
export type WaitResult<T> =
	| { ok: true; id: string; output: T }
	| { ok: false; id: string; error: ChildJobFailedError }

/** A child's final state as reported by its worker; `output` is absent for undefined. */
export type ChildReport =
	| { ok: true; output?: JsonValue }
	| { ok: false; error: { name: string; message: string } }

/** Checkpointed under the wait id: every child's report, or the ones still pending. */
export type WaitCheckpoint =
	| (ChildReport & { id: string })[]
	| { timedOut: string[] }

export const WAIT_GROUPS_TABLE = 'bgw_wait_groups'
export const WAITS_TABLE = 'bgw_waits'

/**
 * A group is one wait of one parent run; `remaining` counts its unreported children.
 * Both the parent's suspend and every child's report lock the group row, so a wakeup
 * cannot fall between them. Rows outlive the wait until the parent job is gone.
 */
export function waitTablesSql(schema: string): string {
	const quoted = quoteSchemaName(schema)
	return `SELECT pg_advisory_xact_lock(hashtext('better-graphile-worker:migrate'));
CREATE TABLE IF NOT EXISTS ${quoted}.${WAIT_GROUPS_TABLE} (
	parent_job_id bigint NOT NULL,
	wait_id text NOT NULL,
	remaining integer NOT NULL CHECK (remaining >= 0),
	deadline timestamptz,
	created_at timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (parent_job_id, wait_id)
);
CREATE TABLE IF NOT EXISTS ${quoted}.${WAITS_TABLE} (
	parent_job_id bigint NOT NULL,
	wait_id text NOT NULL,
	idx integer NOT NULL,
	child_job_id bigint NOT NULL,
	result jsonb,
	finished_at timestamptz,
	PRIMARY KEY (parent_job_id, wait_id, idx),
	FOREIGN KEY (parent_job_id, wait_id) REFERENCES ${quoted}.${WAIT_GROUPS_TABLE} ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS ${WAITS_TABLE}_pending ON ${quoted}.${WAITS_TABLE} (child_job_id) WHERE result IS NULL;`
}

export function assertWaitOptions(options: WaitOptions | undefined): void {
	if (options?.timeout !== undefined)
		assertInteger(options.timeout, 'Wait timeout', 1)
}

/** Validate a handler's return value against `outputSchema` and keep its JSON input. */
export async function encodeQueueOutput(
	queue: QueueContract,
	value: unknown
): Promise<JsonValue | undefined> {
	if (!queue.outputSchema) return undefined
	const parsed = await queue.outputSchema.safeParseAsync(value)
	if (!parsed.success)
		throw new JobValidationError(
			queue.name,
			parsed.error.issues.map(({ path, message }) => ({ path, message })),
			'output'
		)
	assertJsonValue(value, true)
	return value === undefined ? undefined : JSON.parse(JSON.stringify(value))
}

/** Results in input order; a stored timeout throws again so replays stay deterministic. */
export async function decodeWaitCheckpoint(
	waitId: string,
	queue: QueueContract,
	stored: unknown
): Promise<WaitResult<unknown>[]> {
	if (isPlainObject(stored) && Array.isArray(stored.timedOut))
		throw new WaitTimeoutError(waitId, stored.timedOut.map(String))
	if (!Array.isArray(stored))
		throw new TypeError(`Checkpoint "${waitId}" does not hold wait results`)
	return Promise.all(
		stored.map(async (report: ChildReport & { id: string }) => {
			if (!report.ok)
				return {
					ok: false as const,
					id: report.id,
					error: new ChildJobFailedError(report.id, queue.name, report.error)
				}
			const parsed = await queue.outputSchema?.safeParseAsync(report.output)
			if (parsed && !parsed.success)
				throw new JobValidationError(
					queue.name,
					parsed.error.issues.map(({ path, message }) => ({ path, message })),
					'output'
				)
			return { ok: true as const, id: report.id, output: parsed?.data }
		})
	)
}
