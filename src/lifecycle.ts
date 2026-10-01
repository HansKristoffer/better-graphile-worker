import { ShutdownTimeoutError } from './errors.js'
import { assertInteger } from './validation.js'

type ManagedRunner = { promise: Promise<void>; stop(): Promise<void> }
/** Serializes transitions while retaining shutdown ownership after caller timeouts. */
export function createLifecycle<R extends ManagedRunner>(options: {
	start(): Promise<R>
	release(): Promise<void>
	onError(error: unknown): void
	handleSignals?: boolean | undefined
}) {
	let runner: R | undefined
	let starting: Promise<void> | undefined
	let stopping: Promise<void> | undefined
	let once: Promise<void> | undefined
	let removeSignals: (() => void) | undefined
	let completion: Promise<void> = Promise.resolve()
	function cleanupSignals() {
		removeSignals?.()
		removeSignals = undefined
	}
	function start(): Promise<void> {
		if (stopping) return stopping.then(start)
		if (once) return Promise.reject(new Error('Worker runOnce is in progress'))
		if (starting) return starting
		if (runner) return Promise.resolve()
		starting = Promise.resolve().then(async () => {
			try {
				if (options.handleSignals) {
					const onSignal = () => {
						void stop().catch(options.onError)
					}
					process.on('SIGINT', onSignal)
					process.on('SIGTERM', onSignal)
					removeSignals = () => {
						process.off('SIGINT', onSignal)
						process.off('SIGTERM', onSignal)
					}
				}
				runner = await options.start()
				completion = runner.promise
				// Track failures even when callers only use start()/stop().
				const owner = runner
				const finished = () => {
					if (runner === owner) cleanupSignals()
				}
				void completion.then(finished, (error) => {
					finished()
					options.onError(error)
				})
			} catch (error) {
				cleanupSignals()
				try {
					await options.release()
				} catch (releaseError) {
					options.onError(releaseError)
				}
				throw error
			} finally {
				starting = undefined
			}
		})
		return starting
	}
	function stop(timeout?: number): Promise<void> {
		if (timeout !== undefined)
			assertInteger(timeout, 'shutdown timeout', 0, 2147483647)
		if (!stopping) {
			stopping = (async () => {
				try {
					if (starting) await starting.catch(() => {})
					if (once) await once.catch(() => {})
					cleanupSignals()
					if (runner) await runner.stop()
					runner = undefined
					await options.release()
				} finally {
					cleanupSignals()
					stopping = undefined
				}
			})()
			void stopping.catch(options.onError)
		}
		const work = stopping
		if (timeout === undefined) return work
		return new Promise<void>((resolve, reject) => {
			const timer = setTimeout(
				() => reject(new ShutdownTimeoutError(timeout)),
				timeout
			)
			work.then(resolve, reject).finally(() => clearTimeout(timer))
		})
	}
	function runOnce(operation: () => Promise<void>): Promise<void> {
		if (starting || stopping || runner || once)
			return Promise.reject(new Error('Worker is busy'))
		once = Promise.resolve()
			.then(operation)
			.finally(() => {
				once = undefined
			})
		return once
	}
	return {
		start,
		stop,
		runOnce,
		get promise() {
			return starting ? starting.then(() => completion) : (once ?? completion)
		},
		waitUntilStopped: () =>
			stopping ??
			(starting ? starting.then(() => completion) : (once ?? completion))
	}
}
