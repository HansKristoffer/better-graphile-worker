# Design overview

These are the decisions that span several modules and the reasons behind them. For how to use the library, see the [README](../../README.md).

## Graphile owns execution

Graphile Worker does the scheduling, locking, retries and backoff. This library does not run its own loop, scheduler or retry policy. Each queue is a Graphile task identifier; cron-init generates a second identifier, `{name}_cron-init`. That is why `defineQueues` rejects names that collide with generated identifiers. Lanes are Graphile job queues, so concurrency within a lane is Graphile's serial guarantee and nothing more.

Ordinary enqueue goes through Graphile's public SQL functions. That keeps producers usable by an enqueue-only database role and lets `prepareJob` hand the same statement to the user's own transaction.

## One wire format, no legacy readers

Every job carries a version-2 envelope ([`src/payload.ts`](../../src/payload.ts)): the producer's wire input, W3C trace context, and step checkpoints in separate fields. Workers reject anything else as a permanent failure. Readers for older formats were removed in 2.0. The cost is that an envelope change is a major release that requires draining old jobs first. Do not add a compatibility reader without that discussion.

Wire inputs are restricted to plain JSON values ([`src/validation.ts`](../../src/validation.ts)). JSON serialization would otherwise turn a Date into a string, drop a nested `undefined` or throw on a bigint at enqueue. The handler would receive a different value than the producer sent, or the enqueue would fail late.

## Validate on both sides

The producer stores `z.input`, never the transformed output, and the consumer parses again. Other producers, raw SQL and older deploys can write jobs, so the consumer cannot trust the producer's validation. Storing the input also keeps transforms in one place. A schema failure on the consumer is permanent, because retrying cannot fix the data. A `ZodError` thrown by handler code still retries, because it usually validates an external response.

## Private table access is isolated and pinned

Continuations, permanent-failure retention, step checkpoints and payload debugging need Graphile 0.17's `_private_jobs` table, which is not a public API. All of that SQL lives in [`src/private-jobs.ts`](../../src/private-jobs.ts), and the `graphile-worker` peer range is bounded to 0.17.x because of it. Raising the range means rerunning the PostgreSQL regression suite against the new version.

Every write there is conditioned on `locked_by` matching the current worker. A worker that lost its lock, for example after Graphile's stale-lock reclaim, must not overwrite the new owner's state.

## Atomic bookkeeping

- A continuation enqueues the next run and marks the current run as never-retry in one transaction. Otherwise a crash between the two could produce a retry next to the continuation.
- Step checkpoints are patched atomically into the envelope and serialized per job. A failed checkpoint write is never treated as completed.
- Retention exhausts the job's attempt budget and then rethrows the original error, so Graphile records the error and releases the lock itself.

None of this makes external effects exactly-once: a process can crash after an effect but before its checkpoint. That limit belongs in user documentation, not in more machinery.

## Observers cannot change outcomes

Hooks, loggers and tracing run around the job, never in its decision path ([`src/observers.ts`](../../src/observers.ts)). Their failures are reported separately and never change acknowledgement or replace the job's error. `shouldSkipEnqueue` is the exception: it is a behavioral decision, so its errors propagate.

Tracing is captured per instance (`otel: { api }`), not through a global setter, so two instances in one process cannot change each other's adapter.

## The test harness shares production code

`createTestHarness` runs handlers without PostgreSQL or Graphile, but parsing, child-enqueue validation and step replay use the same functions as the worker. A harness that reimplements behavior would pass tests that production fails. New job-context behavior should go into the shared code, with the harness supplying in-memory stores.
