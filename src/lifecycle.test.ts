import { expect, test } from 'bun:test'
import { createLifecycle } from './lifecycle.js'
import { ShutdownTimeoutError } from './errors.js'
function deferred<T>() {
	let resolve!: (value: T | PromiseLike<T>) => void
	let reject!: (reason: unknown) => void
	const promise = new Promise<T>((yes, no) => {
		resolve = yes
		reject = no
	})
	return { promise, resolve, reject }
}
test('concurrent starts share one runner and stop waits for startup', async () => {
	const startup = deferred<{ promise: Promise<void>; stop(): Promise<void> }>()
	let starts = 0,
		stops = 0,
		releases = 0
	const completed = deferred<void>()
	const lifecycle = createLifecycle({
		start: () => {
			starts++
			return startup.promise
		},
		release: async () => {
			releases++
		},
		onError() {}
	})
	const a = lifecycle.start(),
		b = lifecycle.start(),
		shutdown = lifecycle.stop()
	expect(a).toBe(b)
	startup.resolve({
		promise: completed.promise,
		async stop() {
			stops++
			completed.resolve()
		}
	})
	await Promise.all([a, b, shutdown])
	expect([starts, stops, releases]).toEqual([1, 1, 1])
})
test('shutdown timeout retains pending work and a new start waits for it', async () => {
	const shutdown = deferred<void>(),
		completed = deferred<void>()
	let starts = 0
	const lifecycle = createLifecycle({
		start: async () => {
			starts++
			return { promise: completed.promise, stop: () => shutdown.promise }
		},
		release: async () => {},
		onError() {}
	})
	await lifecycle.start()
	await expect(lifecycle.stop(1)).rejects.toBeInstanceOf(ShutdownTimeoutError)
	const restart = lifecycle.start()
	expect(starts).toBe(1)
	shutdown.resolve()
	completed.resolve()
	await restart
	expect(starts).toBe(2)
	await lifecycle.stop()
})
test('startup failure and repeated stop leave no signal listeners', async () => {
	const before = [
		process.listenerCount('SIGINT'),
		process.listenerCount('SIGTERM')
	]
	let fail = true
	const completion = deferred<void>()
	const lifecycle = createLifecycle({
		start: async () => {
			if (fail) throw new Error('startup')
			return {
				promise: completion.promise,
				async stop() {
					completion.resolve()
				}
			}
		},
		release: async () => {},
		handleSignals: true,
		onError() {}
	})
	await expect(lifecycle.start()).rejects.toThrow('startup')
	expect([
		process.listenerCount('SIGINT'),
		process.listenerCount('SIGTERM')
	]).toEqual(before)
	fail = false
	await lifecycle.start()
	expect(process.listenerCount('SIGINT')).toBe(before[0]! + 1)
	await Promise.all([lifecycle.stop(), lifecycle.stop()])
	expect([
		process.listenerCount('SIGINT'),
		process.listenerCount('SIGTERM')
	]).toEqual(before)
})

test('natural runner failure cleans up signals and preserves the startup error', async () => {
	const completion = deferred<void>()
	const before = process.listenerCount('SIGINT')
	const errors: unknown[] = []
	const lifecycle = createLifecycle({
		start: async () => ({ promise: completion.promise, async stop() {} }),
		release: async () => {},
		handleSignals: true,
		onError: (error) => {
			errors.push(error)
		}
	})
	await lifecycle.start()
	const error = new Error('runner failed')
	completion.reject(error)
	await expect(lifecycle.promise).rejects.toBe(error)
	expect(process.listenerCount('SIGINT')).toBe(before)
	expect(errors).toEqual([error])
	await lifecycle.stop()
	const failed = createLifecycle({
		start: async () => {
			throw error
		},
		release: async () => {
			throw new Error('release failed')
		},
		onError: () => {}
	})
	await expect(failed.start()).rejects.toBe(error)
})

test('signals during startup wait for the runner and shut it down', async () => {
	const completed = deferred<void>()
	let stopped = false
	const lifecycle = createLifecycle({
		start: async () => {
			process.emit('SIGTERM')
			return {
				promise: completed.promise,
				async stop() {
					stopped = true
					completed.resolve()
				}
			}
		},
		release: async () => {},
		handleSignals: true,
		onError: () => {}
	})
	await lifecycle.start()
	await lifecycle.waitUntilStopped()
	expect(stopped).toBe(true)
})
test('runOnce is tracked and cannot overlap another transition', async () => {
	const work = deferred<void>()
	let released = false
	const lifecycle = createLifecycle({
		start: async () => ({ promise: Promise.resolve(), async stop() {} }),
		release: async () => {
			released = true
		},
		onError: () => {}
	})
	const once = lifecycle.runOnce(() => work.promise)
	expect(lifecycle.promise).toBe(once)
	await expect(lifecycle.start()).rejects.toThrow('runOnce')
	await expect(lifecycle.runOnce(async () => {})).rejects.toThrow('busy')
	const stopped = lifecycle.stop()
	expect(released).toBe(false)
	work.resolve()
	await stopped
	expect(released).toBe(true)
})
