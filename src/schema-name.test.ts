import { describe, test, expect } from 'bun:test'
import { assertValidSchemaName } from './schema-name'
import { InvalidSchemaNameError } from './errors'

describe('assertValidSchemaName', () => {
	test('accepts a simple identifier', () => {
		expect(assertValidSchemaName('graphile_worker')).toBe('graphile_worker')
	})

	test('rejects interpolation-prone names', () => {
		expect(() => assertValidSchemaName('worker; drop table')).toThrow(
			InvalidSchemaNameError
		)
		expect(() => assertValidSchemaName('public.jobs')).toThrow(
			InvalidSchemaNameError
		)
	})
})
