/**
 * A worker, pool or job client built on first use. Queue files import providers, which
 * import the app's worker for `createJob`, which imports the registry of queue files. An
 * ES module cycle is fine as long as nothing reads the registry while the modules load:
 * `lazyWorker` defers the read to the first enqueue, so no `require()` or dynamic import
 * is needed. Every property and method reads through to one instance. Read members when
 * you call them (`workers.createJob(...)`): destructuring at module load creates the
 * instance then.
 *
 * ```ts
 * import { queues } from '../registries/queues'
 * export const workers = lazyWorker(() => createBetterWorkerPool({ pgPool: getPool(), queues, runners }))
 * ```
 */
export function lazyWorker<T extends object>(create: () => T): T {
	let instance: T | undefined
	const target = () => {
		instance ??= create()
		return instance
	}
	return new Proxy({} as T, {
		get: (_, key) => Reflect.get(target(), key),
		has: (_, key) => Reflect.has(target(), key),
		ownKeys: () => Reflect.ownKeys(target()),
		getOwnPropertyDescriptor: (_, key) => {
			const descriptor = Reflect.getOwnPropertyDescriptor(target(), key)
			// A proxy over an empty target must report every property as configurable.
			return descriptor && { ...descriptor, configurable: true }
		}
	})
}
