import { NonRetriableError } from './errors.js'

export type JsonPrimitive = string | number | boolean | null
export type JsonValue =
	| JsonPrimitive
	| readonly JsonValue[]
	| { readonly [key: string]: JsonValue }
export type JsonCompatible<T> = T extends JsonPrimitive
	? T
	: T extends readonly unknown[]
		? { [K in keyof T]: JsonCompatible<T[K]> }
		: T extends (...args: never[]) => unknown
			? never
			: T extends object
				? { [K in keyof T]: JsonCompatible<T[K]> }
				: never

export function assertInteger(
	value: number,
	label: string,
	min = 0,
	max = Number.MAX_SAFE_INTEGER
): void {
	if (!Number.isSafeInteger(value) || value < min || value > max)
		throw new RangeError(
			`${label} must be an integer between ${min} and ${max}`
		)
}

/** Reject values JSON would silently change or omit. Root undefined is encoded separately. */
export function assertJsonValue(value: unknown, allowUndefined = false): void {
	const ancestors = new Set<object>()
	function visit(input: unknown, root: boolean): void {
		if (input === undefined && root && allowUndefined) return
		if (
			input === null ||
			typeof input === 'string' ||
			typeof input === 'boolean'
		)
			return
		if (typeof input === 'number' && Number.isFinite(input)) return
		if (typeof input !== 'object' || input === null)
			throw new NonRetriableError(
				'Value must contain only JSON-compatible values'
			)
		if (ancestors.has(input))
			throw new NonRetriableError('JSON values cannot contain cycles')
		if (
			!Array.isArray(input) &&
			Object.getPrototypeOf(input) !== Object.prototype &&
			Object.getPrototypeOf(input) !== null
		)
			throw new NonRetriableError(
				'JSON values must use plain objects; encode dates and class instances explicitly'
			)
		if (Object.getOwnPropertySymbols(input).length > 0)
			throw new NonRetriableError(
				'JSON values cannot contain symbol properties'
			)
		ancestors.add(input)
		if (Array.isArray(input)) {
			for (const item of input) visit(item, false)
		} else {
			for (const item of Object.values(input)) visit(item, false)
		}
		ancestors.delete(input)
	}
	visit(value, true)
}
