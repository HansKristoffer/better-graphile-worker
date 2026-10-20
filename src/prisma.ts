import type { PreparedJob } from './types.js'

/** The part of a Prisma client or interactive transaction this module uses. */
export type PrismaRawClient = {
	$queryRawUnsafe<T = unknown>(
		query: string,
		...values: unknown[]
	): PromiseLike<T>
}

/**
 * Run a `prepareJob` / `prepareJobs` statement in a Prisma transaction, so the jobs commit
 * or roll back with its other writes. Returns the job ids in input order, and throws, which
 * rolls the transaction back, when Graphile created fewer jobs than prepared.
 */
export async function enqueueInTransaction(
	tx: PrismaRawClient,
	prepared: PreparedJob
): Promise<string[]> {
	if (prepared.count === 0) return []
	const rows = await tx.$queryRawUnsafe<{ id: string | null }[]>(
		prepared.text,
		...prepared.values
	)
	const ids = rows.flatMap((row) => (row.id ? [row.id] : []))
	if (ids.length !== prepared.count)
		throw new Error(
			`Enqueue of "${prepared.queue}" returned ${ids.length} of ${prepared.count} jobs, likely a jobKey race; retry the transaction`
		)
	return ids
}
