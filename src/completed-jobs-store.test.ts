import { describe, test, expect } from 'bun:test'
import {
	createCompletedJobsStore,
	type CompletedJob
} from './completed-jobs-store'

function job(overrides: Partial<CompletedJob> = {}): CompletedJob {
	return {
		id: '1',
		queueName: 'q',
		payload: { ok: true },
		status: 'completed',
		attempts: 1,
		maxAttempts: 4,
		createdAt: '2026-01-01T00:00:00.000Z',
		completedAt: '2026-01-01T00:00:01.000Z',
		durationMs: 10,
		error: null,
		...overrides
	}
}

describe('createCompletedJobsStore', () => {
	test('adds jobs and returns them newest first', () => {
		const store = createCompletedJobsStore()
		store.add(job({ id: '1', completedAt: '2026-01-01T00:00:01.000Z' }))
		store.add(job({ id: '2', completedAt: '2026-01-01T00:00:02.000Z' }))

		const all = store.getAll()
		expect(all.map((j) => j.id)).toEqual(['2', '1'])
	})

	test('trims to max jobs per queue', () => {
		const store = createCompletedJobsStore(2)
		store.add(job({ id: '1' }))
		store.add(job({ id: '2' }))
		store.add(job({ id: '3' }))

		expect(store.getAll()).toHaveLength(2)
		expect(store.getAll().map((j) => j.id)).toEqual(['3', '2'])
	})

	test('getStats counts completed and failed per queue', () => {
		const store = createCompletedJobsStore()
		store.add(job({ id: '1', queueName: 'a', status: 'completed' }))
		store.add(job({ id: '2', queueName: 'a', status: 'failed' }))
		store.add(job({ id: '3', queueName: 'b', status: 'completed' }))

		expect(store.getStats()).toEqual({
			a: { completed: 1, failed: 1 },
			b: { completed: 1, failed: 0 }
		})
	})
})
