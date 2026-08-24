import { InvalidSchemaNameError } from './errors'

const SCHEMA_NAME = /^[a-zA-Z_][a-zA-Z0-9_]*$/

export function assertValidSchemaName(schema: string): string {
	if (!SCHEMA_NAME.test(schema)) {
		throw new InvalidSchemaNameError(schema)
	}
	return schema
}
