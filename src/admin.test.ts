import { describe, test, expect } from 'bun:test'
import { z } from 'zod'
import { createQueue } from './create-queue'
import { getQueueDefinitions, mergeJobStats } from './admin'

const sendEmail = createQueue({
	name: 'sendEmail',
	inputSchema: z.object({ to: z.string() }),
	processFn: async () => {}
})

const dailySweep = createQueue({
	name: 'dailySweep',
	cron: '0 3 * * *',
	processFn: async () => {}
})

describe('getQueueDefinitions', () => {
	test('includes the Zod input schema for regular queues', () => {
		const defs = getQueueDefinitions([sendEmail, dailySweep])
		expect(defs[0]?.inputSchema).toBe(sendEmail.inputSchema)
		expect(defs[0]?.hasInputSchema).toBe(true)
		expect(defs[1]?.inputSchema).toBeNull()
		expect(defs[1]?.hasInputSchema).toBe(false)
	})
})

describe('mergeJobStats', () => {
	test('merges postgres counts with the completed-jobs ring', () => {
		const stats = mergeJobStats(
			[{ taskIdentifier: 'sendEmail', pending: 2, running: 1, failed: 9 }],
			{ sendEmail: { completed: 4, failed: 1 } },
			['sendEmail', 'dailySweep']
		)

		expect(stats).toEqual([
			{
				taskIdentifier: 'sendEmail',
				pending: 2,
				running: 1,
				completed: 4,
				failed: 1
			},
			{
				taskIdentifier: 'dailySweep',
				pending: 0,
				running: 0,
				completed: 0,
				failed: 0
			}
		])
	})

	test('falls back to postgres failed when the ring has no row', () => {
		const stats = mergeJobStats(
			[{ taskIdentifier: 'sendEmail', pending: 0, running: 0, failed: 3 }],
			{},
			['sendEmail']
		)
		expect(stats[0]?.failed).toBe(3)
		expect(stats[0]?.completed).toBe(0)
	})

	test('includes ring-only task identifiers', () => {
		const stats = mergeJobStats(
			[],
			{ leftover: { completed: 2, failed: 1 } },
			[]
		)
		expect(stats).toEqual([
			{
				taskIdentifier: 'leftover',
				pending: 0,
				running: 0,
				completed: 2,
				failed: 1
			}
		])
	})
})
