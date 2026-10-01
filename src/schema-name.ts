import { InvalidSchemaNameError } from './errors.js'

const SCHEMA_NAME = /^[a-zA-Z_][a-zA-Z0-9_]*$/

export function assertValidSchemaName(schema: string): string {
	if (!SCHEMA_NAME.test(schema) || schema.length > 63) {
		throw new InvalidSchemaNameError(schema)
	}
	return schema
}

export function quoteSchemaName(schema: string): string {
	return `"${assertValidSchemaName(schema)}"`
}
