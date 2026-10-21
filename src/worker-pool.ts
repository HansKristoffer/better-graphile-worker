import {
	createWorker,
	resolveCompletedJobsStore,
	type BetterWorker,
	type QueueRegistration,
	type SharedWorkerOptions,
	type WorkerRunnerOptions
} from './create-better-worker.js'
import type { QueueContract } from './queue.js'
import { DEFAULT_CONCURRENCY } from './worker.js'

/** The runner names a queue registry uses, from each queue's `runner`. */
export type RunnerName<TQueues extends readonly QueueContract[]> = Exclude<
	TQueues[number]['runner'],
	undefined
>

export type BetterWorkerPoolOptions<TQueues extends readonly QueueContract[]> =
	SharedWorkerOptions<TQueues> &
		QueueRegistration<TQueues> & {
			/** Every queue names its runner, so none is left without one. */
			queues: TQueues & readonly { readonly runner: string }[]
			runners: { readonly [K in RunnerName<TQueues>]: WorkerRunnerOptions }
		}

/**
 * One worker per runner, each executing only its runner's queues. Enqueueing, admin and
 * history cover every queue; lifecycle methods apply to all runners.
 */
export type BetterWorkerPool<TQueues extends readonly QueueContract[]> = Omit<
	BetterWorker<TQueues>,
	'buildTaskList' | 'buildCronItems'
> & {
	readonly runners: {
		readonly [K in RunnerName<TQueues>]: BetterWorker<TQueues>
	}
}

/**
 * The `pgPool` size a set of runners needs: each runner's concurrency plus its LISTEN and
 * maintenance connections. Add headroom for producers that share the pool.
 */
export function pgPoolSize(
	runners: Readonly<Record<string, WorkerRunnerOptions>>
): number {
	return Object.values(runners).reduce(
		(total, runner) => total + (runner.concurrency ?? DEFAULT_CONCURRENCY) + 2,
		0
	)
}

export function createBetterWorkerPool<
	const TQueues extends readonly QueueContract[]
>(options: BetterWorkerPoolOptions<TQueues>): BetterWorkerPool<TQueues> {
	const { runners, ...shared } = options
	const runnerOptions = runners as Readonly<Record<string, WorkerRunnerOptions>>
	const assigned = new Map<string, string[]>(
		Object.keys(runnerOptions).map((name) => [name, []])
	)
	if (!assigned.size)
		throw new TypeError('runners must list at least one runner')
	for (const queue of options.queues) {
		const list =
			queue.runner === undefined ? undefined : assigned.get(queue.runner)
		if (!list)
			throw new TypeError(
				queue.runner === undefined
					? `Queue "${queue.name}" needs a runner`
					: `Queue "${queue.name}" uses unknown runner "${queue.runner}"`
			)
		list.push(queue.name)
	}
	for (const [name, queues] of assigned)
		if (!queues.length) throw new TypeError(`Runner "${name}" has no queues`)

	const completedJobs = resolveCompletedJobsStore(options.completedJobs)
	const workers = Object.fromEntries(
		[...assigned].map(([name, queues]) => [
			name,
			// The registration union stays generic here; each runner gets the caller's options.
			createWorker<TQueues>(
				{
					...shared,
					...runnerOptions[name],
					process: queues
				} as never,
				completedJobs,
				name
			)
		])
	)
	const all = Object.values(workers)
	// Every runner enqueues to and reads every queue, so the first answers for the pool.
	const [primary] = all as [BetterWorker<TQueues>]
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
	const each = (run: (worker: BetterWorker<TQueues>) => Promise<void>) =>
		Promise.all(all.map(run)).then(() => {})
	/** Waits for every runner, then rejects with the first failure. */
	const settled = async (
		run: (worker: BetterWorker<TQueues>) => Promise<void>
	) => {
		const results = await Promise.allSettled(all.map(run))
		const failed = results.find((result) => result.status === 'rejected')
		if (failed) throw failed.reason
	}
	const stopAll = () => Promise.allSettled(all.map((worker) => worker.stop()))

	return {
		...delegated,
		runners: workers as BetterWorkerPool<TQueues>['runners'],
		get promise() {
			return settled((worker) => worker.promise)
		},
		async start() {
			try {
				await each((worker) => worker.start())
			} catch (error) {
				// A partly started pool would leave some queues without a runner.
				await stopAll()
				throw error
			}
			// Likewise a runner that stops on its own: the pool stops the rest and rejects.
			for (const worker of all) void worker.promise.catch(stopAll)
		},
		stop: (stopOptions) => each((worker) => worker.stop(stopOptions)),
		waitUntilStopped: () => settled((worker) => worker.waitUntilStopped()),
		runOnce: () => each((worker) => worker.runOnce())
	}
}
