type Defined<T extends object> = {
	[K in keyof T as undefined extends T[K] ? never : K]: T[K]
} & {
	[K in keyof T as undefined extends T[K] ? K : never]?: Exclude<
		T[K],
		undefined
	>
}
/** Omit absent options before crossing APIs that distinguish absence from undefined. */
export function compact<const T extends object>(value: T): Defined<T> {
	return Object.fromEntries(
		Object.entries(value).filter(([, item]) => item !== undefined)
	) as Defined<T>
}
