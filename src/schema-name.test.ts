import { describe, test, expect } from 'bun:test'
import { assertValidSchemaName } from './schema-name.js'
import { InvalidSchemaNameError } from './errors.js'

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

test('rejects schema names PostgreSQL would truncate', () => {
	expect(() => assertValidSchemaName('a'.repeat(64))).toThrow(
		InvalidSchemaNameError
	)
})
