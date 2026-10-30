import { ShutdownTimeoutError } from './errors.js'
import { assertInteger } from './validation.js'

export type RestartOptions = {
	/** Wait before the first restart, in ms. Default 1 second. */
	delay?: number
	/** Each failed restart doubles the wait, up to this many ms. Default 30 seconds, or `delay` if longer. */
	maxDelay?: number
}

/** On unless explicitly disabled. */
export function resolveRestart(option: boolean | RestartOptions | undefined) {
	if (option === false) return undefined
	const { delay = 1000, maxDelay = Math.max(delay, 30_000) } =
		option === true || option === undefined ? {} : option
	assertInteger(delay, 'restart.delay', 0, 2147483647)
	assertInteger(maxDelay, 'restart.maxDelay', delay, 2147483647)
	return { delay, maxDelay }
}

/** `attempt` counts restarts since the runner stopped; `delay` is the wait before it. */
export type RestartAttempt = { attempt: number; delay: number }

type ManagedRunner = { promise: Promise<void>; stop(): Promise<void> }
/** From start() until stop(): the runners it restarts and the backoff stop() cuts short. */
type Run = {
	stopped: boolean
	wake(): void
	restarting?: Promise<void> | undefined
}

/** Serializes transitions while retaining shutdown ownership after caller timeouts. */
export function createLifecycle<R extends ManagedRunner>(options: {
	start(restart: boolean): Promise<R>
	release(): Promise<void>
	onError(error: unknown): void
	handleSignals?: boolean | undefined
	/** Start a new runner when one fails without stop(); without it, `promise` rejects. */
	restart?:
		| {
				delay: number
				maxDelay: number
				onStopped(error: unknown, next: RestartAttempt): void
				onRestartFailed(error: unknown, next: RestartAttempt): void
				onRestarted(attempt: number): void
		  }
		| undefined
}) {
	let runner: R | undefined
	let run: Run | undefined
	let starting: Promise<void> | undefined
	let stopping: Promise<void> | undefined
	let once: Promise<void> | undefined
	let removeSignals: (() => void) | undefined
	let completion: Promise<void> = Promise.resolve()
	function cleanupSignals() {
		removeSignals?.()
		removeSignals = undefined
	}
	function wait(current: Run, ms: number) {
		return new Promise<void>((resolve) => {
			// Not unref'd: while it waits, this timer is all that keeps a worker process alive.
			const timer = setTimeout(resolve, ms)
			current.wake = () => {
				clearTimeout(timer)
				resolve()
			}
		})
	}
	/** Settles when the run ends: after stop(), or when a runner ends and is not restarted. */
	async function supervise(current: Run, first: R): Promise<void> {
		let active = first
		let attempt = 0
		for (;;) {
			const startedAt = Date.now()
			const failure = await active.promise.then(
				() => undefined,
				(error: unknown) => ({ error })
			)
			const restart = options.restart
			if (!failure || current.stopped || !restart) {
				if (!current.stopped) {
					// A runner that ended on its own is gone: start() runs a new one, stop() only releases.
					cleanupSignals()
					runner = undefined
					run = undefined
				}
				return active.promise
			}
			runner = undefined
			// A runner that outlived the longest wait was healthy, so backoff starts over.
			if (Date.now() - startedAt >= restart.maxDelay) attempt = 0
			let error = failure.error
			let report = restart.onStopped
			for (;;) {
				attempt++
				const delay = Math.min(
					restart.maxDelay,
					restart.delay * 2 ** Math.min(attempt - 1, 30)
				)
				report(error, { attempt, delay })
				await wait(current, delay)
				if (current.stopped) return
				const next = options.start(true).then(
					(started) => {
						runner = started
						return { started }
					},
					(startError: unknown) => ({ startError })
				)
				current.restarting = next.then(() => {})
				const result = await next
				current.restarting = undefined
				if ('started' in result) {
					active = result.started
					restart.onRestarted(attempt)
					break
				}
				if (current.stopped) return
				error = result.startError
				report = restart.onRestartFailed
			}
		}
	}
	function start(): Promise<void> {
		if (stopping) return stopping.then(start)
		if (once) return Promise.reject(new Error('Worker runOnce is in progress'))
		if (starting) return starting
		if (run) return Promise.resolve()
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
				runner = await options.start(false)
				const current: Run = { stopped: false, wake() {} }
				run = current
				completion = supervise(current, runner)
				// Track failures even when callers only use start()/stop().
				void completion.catch(options.onError)
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
					const current = run
					if (current) {
						current.stopped = true
						current.wake()
						// A restart in flight finishes first, so its runner is stopped too.
						await current.restarting
					}
					if (runner) await runner.stop()
					runner = undefined
					run = undefined
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
		if (starting || stopping || run || once)
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
