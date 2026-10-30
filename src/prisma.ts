import { EnqueueCountError, EnqueueSkippedError } from './errors.js'
import type { PreparedJob } from './types.js'

/**
 * Anything that runs a raw statement and resolves to its rows: a Prisma client or
 * interactive transaction, or your own seam over one. The rows are checked at runtime.
 */
export type RawQueryClient = {
	$queryRawUnsafe(
		query: string,
		...values: PreparedJob['values']
	): PromiseLike<unknown>
}

/** @deprecated Use `RawQueryClient`, which also accepts clients without a type parameter. */
export type PrismaRawClient = {
	$queryRawUnsafe<T = unknown>(
		query: string,
		...values: unknown[]
	): PromiseLike<T>
}

export type EnqueueInTransactionOptions = {
	/**
	 * Throw `EnqueueSkippedError` instead of enqueueing nothing when `shouldSkipEnqueue()`
	 * applied, so a transaction that depends on its job rolls back without it.
	 */
	required?: boolean | undefined
}

function jobIds(queue: string, rows: unknown): (string | null)[] {
	if (Array.isArray(rows))
		return rows.map((row: unknown) => {
			const id =
				typeof row === 'object' && row
					? (row as { id?: unknown }).id
					: undefined
			if (typeof id === 'string' || id === null) return id
			throw new TypeError(
				`Enqueue of "${queue}" expected every row to have a text or null "id"`
			)
		})
	throw new TypeError(
		`Enqueue of "${queue}" expected $queryRawUnsafe to resolve to an array of rows`
	)
}

/**
 * Run a `prepareJob` / `prepareJobs` statement in a Prisma transaction, so the jobs commit
 * or roll back with its other writes. Returns the job ids in input order, and throws
 * `EnqueueCountError`, which rolls the transaction back, when Graphile created fewer jobs
 * than prepared.
 */
export async function enqueueInTransaction(
	tx: RawQueryClient,
	prepared: PreparedJob,
	options?: EnqueueInTransactionOptions
): Promise<string[]> {
	if (prepared.skipped && options?.required)
		throw new EnqueueSkippedError(prepared.queue)
	if (prepared.count === 0) return []
	const ids = jobIds(
		prepared.queue,
		await tx.$queryRawUnsafe(prepared.text, ...prepared.values)
	).filter((id) => id !== null)
	if (ids.length !== prepared.count)
		throw new EnqueueCountError(prepared.queue, prepared.count, ids.length)
	return ids
}
