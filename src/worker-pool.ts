import {
	createWorker,
	resolveCompletedJobsStore,
	type BetterWorker,
	type QueueRegistration,
	type SharedWorkerOptions,
	type WorkerRunnerOptions
} from './create-better-worker.js'
import type { QueueContract } from './queue.js'
import { resolveDatabaseProbe, whileReachable } from './shutdown.js'
import { DEFAULT_CONCURRENCY } from './worker.js'

type RunnerOf<Q, TDefault extends string> = Q extends {
	readonly runner: infer R extends string
}
	? R
	: [TDefault] extends [never]
		? Exclude<Q extends { readonly runner?: infer R } ? R : never, undefined>
		: TDefault

/**
 * The runner names a queue registry uses, from each queue's `runner`; with a default
 * runner, queues without one use it.
 */
export type RunnerName<
	TQueues extends readonly QueueContract[],
	TDefault extends string = never
> = TQueues[number] extends infer Q ? RunnerOf<Q, TDefault> : never

export type BetterWorkerPoolOptions<
	TQueues extends readonly QueueContract[],
	TDefault extends string = never
> = SharedWorkerOptions<TQueues> &
	QueueRegistration<TQueues> & {
		/** Without a `defaultRunner`, every queue names its runner, so none is left without one. */
		queues: TQueues &
			([NoInfer<TDefault>] extends [never]
				? readonly { readonly runner: string }[]
				: unknown)
		/**
		 * Options for every runner the queues use, and optionally for others: a runner
		 * no queue uses stays idle and is never started.
		 */
		runners: {
			readonly [K in NoInfer<
				RunnerName<TQueues, TDefault> | TDefault
			>]: WorkerRunnerOptions
		} & { readonly [runner: string]: WorkerRunnerOptions }
		/** The runner for queues that name none. */
		defaultRunner?: TDefault
	}

/**
 * One worker per runner that has queues, each executing only its runner's queues.
 * Enqueueing, admin and history cover every queue; lifecycle methods apply to all runners.
 */
export type BetterWorkerPool<
	TQueues extends readonly QueueContract[],
	TDefault extends string = never
> = Omit<BetterWorker<TQueues>, 'buildTaskList' | 'buildCronItems'> & {
	readonly runners: {
		readonly [K in RunnerName<TQueues, TDefault>]: BetterWorker<TQueues>
	}
}

type RunnerAssignment = {
	queues: readonly { readonly name: string; readonly runner?: string }[]
	defaultRunner?: string | undefined
}

/** Each runner's queues, for runners that have any. */
function assignQueues(
	runners: Readonly<Record<string, WorkerRunnerOptions>>,
	{ queues, defaultRunner }: RunnerAssignment
): Map<string, string[]> {
	if (defaultRunner !== undefined && !Object.hasOwn(runners, defaultRunner))
		throw new TypeError(
			`defaultRunner "${defaultRunner}" is not one of the runners`
		)
	const assigned = new Map<string, string[]>()
	for (const queue of queues) {
		const runner = queue.runner ?? defaultRunner
		if (runner === undefined)
			throw new TypeError(
				`Queue "${queue.name}" needs a runner, or the pool a defaultRunner`
			)
		if (!Object.hasOwn(runners, runner))
			throw new TypeError(
				`Queue "${queue.name}" uses unknown runner "${runner}"`
			)
		assigned.set(runner, [...(assigned.get(runner) ?? []), queue.name])
	}
	return assigned
}

/**
 * The `pgPool` size a set of runners needs: each runner's concurrency plus its LISTEN and
 * maintenance connections. Pass the queues (and `defaultRunner`) to count only runners
 * that have queues, as the pool starts them. Add headroom for producers that share the pool.
 */
export function pgPoolSize(
	runners: Readonly<Record<string, WorkerRunnerOptions>>,
	registry?: RunnerAssignment
): number {
	const started = registry
		? [...assignQueues(runners, registry).keys()].map((name) => runners[name])
		: Object.values(runners)
	return started.reduce(
		(total, runner) => total + (runner?.concurrency ?? DEFAULT_CONCURRENCY) + 2,
		0
	)
}

export function createBetterWorkerPool<
	const TQueues extends readonly QueueContract[],
	const TDefault extends string = never
>(
	options: BetterWorkerPoolOptions<TQueues, TDefault>
): BetterWorkerPool<TQueues, TDefault> {
	const { runners, defaultRunner, ...shared } = options
	const runnerOptions = runners as Readonly<Record<string, WorkerRunnerOptions>>
	const assigned = assignQueues(runnerOptions, {
		queues: options.queues,
		defaultRunner
	})

	const completedJobs = resolveCompletedJobsStore(options.completedJobs)
	// The registration union stays generic here; each runner gets the caller's options.
	const create = (runner?: string, queues?: string[]) =>
		createWorker<TQueues>(
			{
				...shared,
				...(runner === undefined ? {} : runnerOptions[runner]),
				process: queues
			} as never,
			completedJobs,
			runner
		)
	const workers = Object.fromEntries(
		[...assigned].map(([name, queues]) => [name, create(name, queues)])
	)
	const all = Object.values(workers)
	// Every runner enqueues to and reads every queue, so the first answers for the pool.
	// Without queues no runner starts, and an unstarted worker answers instead.
	const primary = all[0] ?? create()
	const {
		promise: _promise,
		start: _start,
		stop: _stop,
		waitUntilStopped: _waitUntilStopped,
		runOnce: _runOnce,
		buildTaskList: _buildTaskList,
		buildCronItems: _buildCronItems,
		...delegated
	} = primary
	/** Waits for every runner, then rejects with the first failure, so none is left mid-transition. */
	const each = async (
		run: (worker: BetterWorker<TQueues>) => Promise<void>
	) => {
		const results = await Promise.allSettled(all.map(run))
		const failed = results.find((result) => result.status === 'rejected')
		if (failed) throw failed.reason
	}
	const stopAll = () => Promise.allSettled(all.map((worker) => worker.stop()))
	/**
	 * A pool without runners idles from start() until stop(), as a running pool would,
	 * including stopping on SIGINT/SIGTERM with `handleSignals`.
	 */
	let idle: { promise: Promise<void>; stop(): void } | undefined
	const idleOr = (operation: () => Promise<void>) =>
		idle ? idle.promise : operation()
	function startIdle() {
		let resolve = () => {}
		const promise = new Promise<void>((done) => {
			resolve = done
		})
		const onSignal = () => idle?.stop()
		if (options.handleSignals) {
			process.on('SIGINT', onSignal)
			process.on('SIGTERM', onSignal)
		}
		idle = {
			promise,
			stop() {
				process.off('SIGINT', onSignal)
				process.off('SIGTERM', onSignal)
				idle = undefined
				void primary.stop().finally(resolve)
			}
		}
	}

	return {
		...delegated,
		runners: workers as BetterWorkerPool<TQueues, TDefault>['runners'],
		get promise() {
			return idleOr(() => each((worker) => worker.promise))
		},
		async start() {
			if (!all.length) {
				if (!idle) startIdle()
				return
			}
			try {
				await each((worker) => worker.start())
			} catch (error) {
				// A partly started pool would leave some queues without a runner.
				await stopAll()
				throw error
			}
			// Likewise a runner that stopped and is not restarted: the pool stops the rest and rejects.
			for (const worker of all) void worker.promise.catch(stopAll)
		},
		stop(stopOptions) {
			const { probeDatabase, ...runnerStop } = stopOptions ?? {}
			const probe = resolveDatabaseProbe(probeDatabase)
			const waiting = idle?.promise
			idle?.stop()
			const stopping = all.length
				? each((worker) => worker.stop(runnerStop))
				: (waiting ?? primary.stop())
			return probe ? whileReachable(stopping, options.pgPool, probe) : stopping
		},
		waitUntilStopped: () =>
			idleOr(() => each((worker) => worker.waitUntilStopped())),
		runOnce: () => each((worker) => worker.runOnce())
	}
}
