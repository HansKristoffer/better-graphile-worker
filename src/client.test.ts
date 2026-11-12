import { describe, expect, test } from 'bun:test'
import type { Pool } from 'pg'
import { createWorkerClient } from './client.js'
import { EnqueueCountError } from './errors.js'

type Rows = { id: string }[]

/** A pool whose statements answer in turn; transaction control statements return nothing. */
function racingPool(answers: Rows[]) {
	const statements: string[] = []
	const query = async (text: string) => {
		statements.push(/^(BEGIN|COMMIT|ROLLBACK)$/.test(text) ? text : 'enqueue')
		if (/^(BEGIN|COMMIT|ROLLBACK)$/.test(text)) return { rows: [] }
		return { rows: answers.shift() ?? [] }
	}
	const pool = {
		query,
		connect: async () => ({ query, release() {} })
	} as unknown as Pool
	return { pool, statements }
}

describe('enqueue reruns a lost jobKey race', () => {
	test('a single keyed job runs again until Graphile returns it', async () => {
		const { pool, statements } = racingPool([[], [{ id: '5' }]])
		const { enqueue } = createWorkerClient({ pgPool: pool })
		const job = await enqueue.addJob('q', {}, { jobKey: 'k' })
		expect(String(job.id)).toBe('5')
		expect(statements).toEqual(['enqueue', 'enqueue'])
	})

	test('an unkeyed job that returns nothing is not rerun', async () => {
		const { pool, statements } = racingPool([[]])
		const { enqueue } = createWorkerClient({ pgPool: pool })
		await expect(enqueue.addJob('q', {})).rejects.toThrow(EnqueueCountError)
		expect(statements).toEqual(['enqueue'])
	})

	test('a keyed batch rolls back a short count before each rerun', async () => {
		const { pool, statements } = racingPool([
			[{ id: '1' }],
			[{ id: '1' }, { id: '2' }]
		])
		const { enqueue } = createWorkerClient({ pgPool: pool })
		const jobs = await enqueue.addJobs([
			{ identifier: 'q', payload: {}, jobKey: 'a' },
			{ identifier: 'q', payload: {} }
		])
		expect(jobs.map((job) => String(job.id))).toEqual(['1', '2'])
		expect(statements).toEqual([
			'BEGIN',
			'enqueue',
			'ROLLBACK',
			'BEGIN',
			'enqueue',
			'COMMIT'
		])
	})

	test('a keyed batch that keeps losing commits nothing and throws', async () => {
		const { pool, statements } = racingPool([[], [], []])
		const { enqueue } = createWorkerClient({ pgPool: pool })
		await expect(
			enqueue.addJobs([{ identifier: 'q', payload: {}, jobKey: 'a' }])
		).rejects.toThrow(EnqueueCountError)
		expect(statements.filter((statement) => statement === 'COMMIT')).toEqual([])
		expect(
			statements.filter((statement) => statement === 'enqueue')
		).toHaveLength(3)
	})
})
