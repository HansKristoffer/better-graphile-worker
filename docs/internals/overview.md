# Design overview

These are the decisions that span several modules and the reasons behind them. For how to use the library, see the [README](../../README.md).

## Graphile owns execution

Graphile Worker does the scheduling, locking, retries and backoff. This library does not run its own loop, scheduler or retry policy. Each queue is a Graphile task identifier; cron-init generates a second identifier, `{name}_cron-init`. That is why `defineQueues` rejects names that collide with generated identifiers. Lanes are Graphile job queues, so concurrency within a lane is Graphile's serial guarantee and nothing more.

Ordinary enqueue goes through Graphile's public SQL functions. That keeps producers usable by an enqueue-only database role and lets `prepareJob` hand the same statement to the user's own transaction.

## One wire format, no legacy readers

Every job carries a version-2 envelope ([`src/payload.ts`](../../src/payload.ts)): the producer's wire input, W3C trace context, and step checkpoints in separate fields. Workers reject anything else as a permanent failure. Readers for older formats were removed in 2.0. The cost is that an envelope change is a major release that requires draining old jobs first. Do not add a compatibility reader without that discussion. The exception is a new optional field such as `progress` or `waited`: workers ignore fields they do not know and continuations copy them, so adding one is not a format change.

Wire inputs are restricted to plain JSON values ([`src/validation.ts`](../../src/validation.ts)). JSON serialization would otherwise turn a Date into a string, drop a nested `undefined` or throw on a bigint at enqueue. The handler would receive a different value than the producer sent, or the enqueue would fail late.

## Validate on both sides

The producer stores `z.input`, never the transformed output, and the consumer parses again. Other producers, raw SQL and older deploys can write jobs, so the consumer cannot trust the producer's validation. Storing the input also keeps transforms in one place. A schema failure on the consumer is permanent, because retrying cannot fix the data. A `ZodError` thrown by handler code still retries, because it usually validates an external response.

## Private table access is isolated and pinned

Continuations, permanent-failure retention, retry scheduling, step checkpoints, waits, job outcomes, progress, crash recovery and payload debugging need Graphile 0.17's `_private_jobs` table, which is not a public API. All of that SQL lives in [`src/private-jobs.ts`](../../src/private-jobs.ts), and the `graphile-worker` peer range is bounded to 0.17.x because of it. Raising the range means rerunning the PostgreSQL regression suite against the new version.

Every write there is conditioned on `locked_by` matching the current worker. A worker that lost its lock, for example after Graphile's stale-lock reclaim, must not overwrite the new owner's state. Graphile 0.17 sets `locked_by` to the worker pool's id, which every concurrent worker in the process shares, so a retry picked up by the same process has the same owner. Envelope patches (checkpoints, progress) and continuations therefore also match `locked_at`, which identifies the attempt.

## Timeouts fail the attempt, not the handler

Nothing can stop a handler from outside, so a timeout races it. The task function rejects with `JobTimeoutError`, and Graphile records the failure and releases the lock and lane while the handler may keep running. The attempt's context then refuses steps, progress, continuations and child enqueues, and the `locked_at` fence above rejects envelope writes already queued. A continuation that is committing when the timer fires decides the outcome instead, because reporting a failure next to a committed continuation would leave an exhausted row that looks failed.

## Crash recovery trusts heartbeats, not locks

Graphile 0.17 only reclaims a locked job after 4 hours, and its lock carries no liveness. With `crashRecovery`, each worker pool upserts a row in `_bgw_worker_heartbeats` keyed by its pool id, the value Graphile writes to `locked_by` for jobs and lanes. The id is only exposed through the `pool:create` event, so [`src/worker.ts`](../../src/worker.ts) listens on the runner's emitter. Handlers wait for the first heartbeat. Otherwise a job could kill the process before its worker is registered and stay locked for 4 hours.

Releasing a live worker's job is the dangerous failure. Graphile's `completeJobs` deletes by job id without checking `locked_by`, so the falsely-dead worker finishing later deletes the job while its second run is still going. Our own private-table writes, including completion with `jobOutcomes`, check `locked_by` and refuse. The heartbeat runs on a timer in the worker's own process, so a blocked event loop looks like death. That is why the defaults are conservative and `staleAfter` must cover several missed beats. It holds one pool connection for its lifetime, like Graphile's LISTEN client, so jobs holding every other connection cannot starve it. A heartbeat thread would survive a blocked loop, but it would also keep a hung process looking alive and would need connection details we do not own.

It is on by default because a stuck lane is the failure users meet without knowing the 4-hour scan exists, and a 5-minute synchronous block already breaks Graphile's LISTEN connection and anything else on the loop. Only an explicit opt-in may fail `start()`: the default degrades to a warning when the role cannot create the table, so an upgrade that skips `migrate()` keeps working.

The sweep claims stale heartbeat rows with `FOR UPDATE SKIP LOCKED` and releases their jobs in the same transaction, so concurrent sweepers never release the same worker twice. It touches the jobs table only when it found a dead worker, because `locked_by` is not indexed. Stopped workers keep their row instead of deleting it, so anything still locked by them is swept too.

## Atomic bookkeeping

- A continuation enqueues the next run and marks the current run as never-retry in one transaction. Otherwise a crash between the two could produce a retry next to the continuation.
- Step checkpoints are patched atomically into the envelope and serialized per job. A failed checkpoint write is never treated as completed.
- Progress lives in the envelope beside the checkpoints, so it uses the same lock check and survives retries. Its writes are throttled, so the latest value can be in memory only: a failing attempt writes it before Graphile records the failure, and a continuation receives it inside its transaction.
- Retention exhausts the job's attempt budget and then rethrows the original error, so Graphile records the error and releases the lock itself.
- `RetryAtError` moves the locked job's `run_at` back by Graphile's backoff for that attempt and then rethrows, so Graphile's own failure update lands on the requested time. The alternative, re-enqueueing a copy like a continuation, would hit the time exactly but change the job id on every retry. The cost of keeping Graphile's update is that a time sooner than its backoff waits for the backoff. Recording the failure and unlocking ourselves is not safe either: after the task returns, Graphile's success path deletes the row by id without checking the lock, and its failure path would add its backoff again if a worker in the same pool had already relocked the job.
- With `jobOutcomes`, the worker completes successful jobs itself, replicating Graphile 0.17's `complete_jobs` (delete the row, release its lane) in the same statement as the outcome insert. Graphile's completion then deletes nothing. Recording after Graphile's completion could not be atomic, and recording before it would report a job as completed while a crash left it to run again. Failure outcomes are best effort, because a failed job keeps its row unless the queue discards it, and `getJob` prefers the live row over the outcome.

Idempotency keys go through `bgw_add_jobs`, a PL/pgSQL wrapper around `add_job` that `migrate()` creates ([`src/enqueue-sql.ts`](../../src/enqueue-sql.ts)). It cannot be one prepared statement. A losing producer must read the winner's job id, but a statement's snapshot predates the winner's commit; only `INSERT ... ON CONFLICT DO UPDATE ... RETURNING` sees it. The winner must write the job id after `add_job` returns, and one statement cannot insert and then update the same row. The function runs each step as its own statement and keeps the prepared-SQL contract: one statement, atomic in the caller's transaction. Keys are scoped per task identifier, unlike Graphile's global `jobKey`, so two queues cannot swallow each other's jobs.

None of this makes external effects exactly-once: a process can crash after an effect but before its checkpoint. That limit belongs in user documentation, not in more machinery.

## Waits replay the handler and suspend through Graphile's failure path

A JavaScript stack cannot be suspended, so `ctx.step.triggerAndWait` ends the run and the handler replays once the children finish, with the wait's results stored as a step checkpoint. The job must keep its row and id across the gap, and Graphile 0.17 leaves only one safe way to end a run without deleting the row:

- Returning success is out: `complete_jobs` deletes by id without checking the lock.
- Unlocking the row ourselves and then throwing is out too. Graphile's `fail_job` matches `locked_by`, which is the pool id. Once a child has woken the job, another worker in the same pool can relock it before that batched update runs, and the update would then unlock the new run.
- So the suspend transaction keeps the lock. It gives the attempt back (`attempts - 1`) and moves `run_at` back by the backoff Graphile is about to add, as `RetryAtError` does. The task then throws `JobSuspendedSignal`, and Graphile's own failure update unlocks the job and its lane.

The cost is that Graphile logs each suspension as a failed task and stores its message as `last_error`. Its failure update is also batched. A child that reports before that update lands sets `run_at = now()`, and the update then adds its backoff to it. The sweep wakes unlocked parents that still have a finished group, which bounds that delay.

A wait's children, its group row and one wait row per child commit in one transaction. A replay after a crash therefore finds the group and enqueues nothing, without idempotency keys. The parent's suspend and each child's report both lock the group row, so a report either sees the suspend or the suspend sees the report. The wake in the report statement matches the parent by id only. A filter on `run_at` would read the parent's row from the statement's snapshot, taken before a suspend that committed while the statement waited for the group lock, and the wakeup would be lost. A test in [`src/waits.test.ts`](../../src/waits.test.ts) reproduces that interleaving. A finished wait checkpoints its results and deletes its group in the same transaction, so a group with no children left means exactly "this parent must wake".

Children report because the wait marks their envelope `waited`. A job that nobody waits on costs nothing extra, and there is no worker option to forget. Workers without the mark, such as an older version, complete children silently, and the sweep fails those waits. A child completes itself in the same statement that reports, the way `jobOutcomes` completes jobs, so a crash cannot record a result and then run the child again, or the reverse.

## Observers cannot change outcomes

Hooks, loggers and tracing run around the job, never in its decision path ([`src/observers.ts`](../../src/observers.ts)). Their failures are reported separately and never change acknowledgement or replace the job's error. `shouldSkipEnqueue` is the exception: it is a behavioral decision, so its errors propagate.

Tracing is captured per instance (`otel: { api }`), not through a global setter, so two instances in one process cannot change each other's adapter.

## The test harness shares production code

`createTestHarness` runs handlers without PostgreSQL or Graphile, but parsing, child-enqueue validation and step replay use the same functions as the worker. A harness that reimplements behavior would pass tests that production fails. New job-context behavior should go into the shared code, with the harness supplying in-memory stores.
