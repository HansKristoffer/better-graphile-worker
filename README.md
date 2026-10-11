# better-graphile-worker

Typed Graphile Worker queues with Zod schemas, cron and cron-init, durable step checkpoints, job progress, and optional OpenTelemetry. You own the PostgreSQL pool.

## Install

```sh
bun add better-graphile-worker graphile-worker pg zod
```

ESM, Node 20+, TypeScript 5.7+, Graphile Worker 0.17.3–0.17.x, pg 8.16+ (8.x) and Zod 4.x. CI exercises Node 20/22/24/26, minimum/latest peers, NodeNext and bundler resolution. `@opentelemetry/api@^1.9` is optional; tracing is a no-op when absent. The minimum pg version is exercised against native Node and PostgreSQL.

## Define queues and handlers together

Keep each schema, its options and its handler in one `defineQueue` call. Payloads infer from schema output; enqueue inputs infer from schema input. No handler registry or type annotations are required.

```ts
import pg from 'pg'
import { defineQueue, defineQueues, createBetterWorker } from 'better-graphile-worker'
import { z } from 'zod'

const countCharacters = defineQueue({
  name: 'countCharacters',
  inputSchema: z.string().transform(value => value.length),
  processFn: (length, ctx) => {
    ctx.logger.info('counted', { length }) // length: number
  }
})

const sendEmail = defineQueue({
  name: 'sendEmail',
  inputSchema: z.object({ to: z.string().email() }),
  maxAttempts: 5,
  processFn: async (payload, ctx) => {
    ctx.logger.info('sending', { to: payload.to })
    await ctx.createJob(countCharacters, payload.to)
    // The target queue reference checks producer input: string, not number.
  }
})

const dailySweep = defineQueue({
  name: 'dailySweep',
  cron: '0 3 * * *',
  serial: true,
  processFn: (_payload, ctx) => {
    ctx.logger.info('sweeping') // payload: undefined
  }
})

const syncOrders = defineQueue({
  name: 'syncOrders',
  cron: '0 * * * *',
  inputSchema: z.object({ orderId: z.string() }),
  initFn: () => [{ orderId: '1' }] as const,
  processFn: (payload, ctx) => {
    ctx.logger.info('syncing', { orderId: payload.orderId })
  }
})

const queues = defineQueues([sendEmail, countCharacters, dailySweep, syncOrders])
const pgPool = new pg.Pool({ connectionString: process.env.DATABASE_URL })
const worker = createBetterWorker({ pgPool, queues })

await worker.migrate()
await worker.jobs.sendEmail({ to: 'person@example.com' })
await worker.createJobs('countCharacters', ['hello', 'world'] as const)
await worker.start()
```

Inside inline handlers, `ctx.createJob(queue, input)` and `ctx.createJobs(queue, inputs)` take queue definitions. This checks payloads without circular registry inference, including mutually referring queues. Pass the same definition object that was registered; an unregistered reference is rejected before enqueue. Worker/client methods continue to use typed names, such as `worker.createJob('sendEmail', input)`. If a reference can select several queues, its input type must fit every possible target; narrow the reference by `name` when payload types differ.

`defineQueues` preserves the tuple and checks literal names for duplicates, empty names, reserved `then`, and collisions with generated cron-init tasks. Dynamic names and arrays are validated at runtime too. Process handlers and initializers can be synchronous or asynchronous; initializer results can be readonly arrays. Definitions expose readonly names, schemas, options and handlers. Create a new definition when changing configuration.

### Shared producer contracts

When an API server needs definitions without importing worker implementations, omit handlers in a shared module and supply them when constructing the worker. This is optional; use one registration style per worker.

```ts
// contracts.ts
export const queues = defineQueues([
  defineQueue({ name: 'sendEmail', inputSchema: z.object({ to: z.string() }) }),
  defineQueue({ name: 'countCharacters', inputSchema: z.string().transform(value => value.length) })
])

// worker.ts
const worker = createBetterWorker({
  pgPool,
  queues,
  handlers: {
    sendEmail: async (payload, ctx) => {
      await ctx.createJob('countCharacters', payload.to)
    },
    countCharacters: (length, ctx) => {
      ctx.logger.info('counted', { length })
    }
  }
})

// api.ts
import { createJobClient } from 'better-graphile-worker/client'
import { queues } from './contracts.js'

const producer = createJobClient({ pgPool, queues })
await producer.jobs.countCharacters('hello')
await producer.release()
```

Separate handlers infer context enqueue methods from the complete registry and use queue names. Use `satisfies QueueHandlers<typeof queues>` for a reusable handler registry. Handler-free contracts require a handler registry on workers and harnesses. Inline definitions cannot also supply a registry.

Migrate separately using a database owner before starting producers. Ordinary enqueue uses Graphile's public SQL functions and performs no migration checks. An enqueue-only role still needs Graphile's required table, sequence and row-level-security permissions, but does not need schema creation permission. `migrate()`, worker startup and the advanced `getWorkerUtils()` initialize Graphile and can run migrations. Only `migrate()` also creates the idempotency-key table. Concurrent utility requests share one initialization. Release/stop never close your pool.

`isRegularQueue`, `isCronQueue`, `isCronInitQueue` and `hasInputSchema` preserve literal names, concrete schemas and any inline handlers while narrowing variants. Contract guards do not invent handler functions; worker construction checks the required functions.

## Queues, defaults and batching

| Kind | Contract | Handler | Manual trigger |
| --- | --- | --- | --- |
| Regular | `inputSchema` | Function or `{ processFn }` | `createJob(name, input)` |
| Cron | `cron`, no schema | Function or `{ processFn }` | `createJob(name)` or `triggerCron(name)` |
| Cron-init | `cron` and `inputSchema` | `{ initFn, processFn }` | `triggerCron(name)` runs `{name}_cron-init`; `createJob(name, input)` runs the processor |

A library queue is a Graphile task identifier. `serial: true` sets its Graphile serialization queue name to the task name; a string selects a shared serialization queue. `cron` accepts a string, readonly string array or Graphile matcher. Multiple schedules have distinct identifiers, including with a custom `cronOptions.identifier`.

Defaults follow explicit job options, then `deriveJobOptions` (or cron options for cron operations), then queue defaults, then `defaultMaxAttempts`, then the library default of 4. Native schedules and manual cron triggers inherit serial queues, priority and retry defaults. Native Graphile cron does not support queue flags; manual triggers do. `backfillPeriod` belongs to native scheduling. Graphile uses the database clock unless `graphile.useNodeTime` is enabled.

`createJob` creates one job even for array schemas. `createJobs` accepts a readonly array and performs one atomic SQL statement after validating every input; failures include the item index. Shared `jobKey` and `jobKeyMode` are unavailable in batch options; per-item keys come from `deriveJobOptions` (below). Within one batch, keys must be distinct and every keyed item must use the same mode, `replace` or `preserve_run_at`. Ids are returned in input order. Batches are never automatically chunked, since that would change atomicity. Keyed array jobs replace a single payload consistently, regardless of tracing.

`shouldSkipEnqueue()` returning true produces `null` for one job and `[]` for a batch. Named functions in `jobs` are stable, enumerable and frozen.

## Lanes, transactions and long-running jobs

`deriveJobOptions` computes `queueName`, `jobKey`, `jobKeyMode`, `priority`, `flags`, `idempotencyKey` and `idempotencyTtl` from each job's wire input (`z.input`), so callers cannot forget them; a derived `queueName` overrides `serial`. It must be synchronous and pure; errors reject the enqueue and reach `onEnqueueFail`. Precedence is explicit job options, then derived options, then queue defaults (`serial`, `priority`, `flags`), then `defaultMaxAttempts`. It applies to `createJob`, `createJobs`, `jobs.*`, `ctx.createJob` and cron-init fan-out. A `jobKey` only holds while its job is pending: a repeated tick creates another job once the previous one is running or has completed. Use an [idempotency key](#idempotency-keys) to hold the key for a period of time instead. Plain cron queues have no input and cannot derive options.

```ts
const advanceRun = defineQueue({
  name: 'advanceRun',
  inputSchema: z.object({ shopId: z.string(), runId: z.string() }),
  // One job at a time per shop; one pending job per run.
  deriveJobOptions: ({ shopId, runId }) => ({ queueName: `shop:${shopId}`, jobKey: `run:${runId}` }),
  processFn: async ({ runId }, ctx) => {
    const deadline = Date.now() + 45_000
    for (;;) {
      const step = await engine.advance(runId, { signal: ctx.signal })
      if (step.kind === 'done') return
      if (step.kind === 'wait') return ctx.continue({ runAt: step.until })
      if (ctx.signal.aborted || Date.now() > deadline) return ctx.continue()
    }
  }
})
```

### Idempotency keys

`idempotencyKey` enqueues at most one job per key and queue within `idempotencyTtl`, even after that job ran, completed or failed. A duplicate enqueues nothing and returns the original job's id; that job may no longer exist. The TTL counts from the first enqueue and defaults to 30 days. Pass milliseconds or a whole number with a unit: `'90s'`, `'30m'`, `'2h'`, `'7d'`.

```ts
const syncBank = defineQueue({
  name: 'syncBank',
  inputSchema: z.object({ userId: z.string() }),
  // At most one bank sync per user every 2 hours.
  deriveJobOptions: ({ userId }) => ({ idempotencyKey: `bank:${userId}`, idempotencyTtl: '2h' }),
  processFn: async ({ userId }) => { /* ... */ }
})
await worker.jobs.syncBank({ userId }) // enqueues
await worker.jobs.syncBank({ userId }) // returns the same id, enqueues nothing
await worker.createJob('sendEmail', input, { idempotencyKey: `welcome:${userId}` })
```

- Keys are scoped to the queue: the same key on two queues creates two jobs. A cron-init `triggerCron` keys the `{name}_cron-init` task.
- Concurrent producers with the same key create one job; the others wait for it to commit and get its id.
- The key is checked before `jobKey`. A duplicate does not replace or reschedule a pending job with the same `jobKey`. A new key enqueues through Graphile as usual, and records whichever job `jobKey` produced.
- Batches take per-item keys from `deriveJobOptions`; `createJobs` options accept `idempotencyTtl` but no shared `idempotencyKey`. A key repeated within a batch returns the first item's id. Cron-init fan-out with a derived key processes each item once per TTL.
- Enqueue without the key to force a run. A continuation keeps running the same job and does not touch its key.
- Return types are unchanged; a duplicate looks like a successful enqueue. Prepared statements return a `deduplicated` column when a key applies, and producer spans record `job.deduplicated`.
- Keys live in the `bgw_idempotency_keys` table in the Graphile schema, created by `migrate()`. Each keyed enqueue deletes up to 100 expired keys. An enqueue-only role needs `SELECT`, `INSERT`, `UPDATE` and `DELETE` on that table.
- The test harness records keys in `enqueued[].options` but does not deduplicate.

`prepareJob` and `prepareJobs` validate, derive options and build the envelope, then return SQL (`text`, scalar `values`) instead of enqueueing. Run it in your own transaction so a state change and its job commit together. Prepare before opening the transaction; the statement has no effect until executed. Check for one non-null `id` per input inside the transaction; Graphile 0.17.3 can return fewer jobs if a worker claims an existing key during enqueue. `prepareJobs` returns its rows in input order. No rows are expected when `shouldSkipEnqueue()` applied (`skipped: true`) or the batch is empty. `onEnqueueFail` covers prepare errors only; execution errors surface in your transaction. Run `migrate()` first.

```ts
const job = await worker.prepareJob('advanceRun', { shopId, runId })
await prisma.$transaction(async (tx) => {
  await tx.run.update({ where: { id: runId, status: 'sealed' }, data: { status: 'approved' } })
  if (!job.skipped) {
    const rows = await tx.$queryRawUnsafe<{ id: string | null }[]>(job.text, ...job.values)
    if (rows.length !== 1 || !rows[0]?.id) throw new Error('Job enqueue lost a key race')
  }
})
// node-postgres: await client.query(job.text, job.values) between BEGIN and COMMIT
```

Immediate enqueue rejects missing Graphile results, but successful peers in a batch can already have committed. Retrying unkeyed items can duplicate those peers; use prepared SQL with the row-count check when this needs to be atomic with application state.

`ctx.continue({ runAt? })` ends the run successfully and enqueues the same job again: same task, wire input, lane, key, priority, flags and `max_attempts`, with fresh attempts. Completed steps and the latest progress carry over; cron metadata does not. The continuation is a new job with a new id. The enqueue and "never retry this run" commit in one transaction, so a crash or error after `continue()` cannot produce a retry next to the continuation. Code after it never runs; do not swallow its rejection in a `try/catch`, and call it once per run. It also works in `initFn`, which then runs again later. `onJobFinished` reports `status: 'success'` with `continued: true`. Use it for time slices (shorter than your platform's shutdown drain time) and for waits such as rate limits, instead of throwing, which spends an attempt (see `RetryAtError` for choosing its time). Keep cursors and other business state in your own tables; `ctx.progress` is for status that others read.

Re-adding a key held by a running job clears that job's key and exhausts its attempts, so it will not be retried if it later fails. Only the job's own continuation should reuse its key. If another producer has already replaced a running job's key, its continuation remains unkeyed and does not overwrite the replacement.

`process` limits an instance to some queues (and their cron schedules) while enqueueing still covers all of them, so separate instances give workloads their own concurrency. Two instances with disjoint lists never schedule the same cron. `runOnce()` follows the same list. Contract-mode workers still need handlers for every queue. Size a shared pool for `Σ concurrency + 2 per started instance (LISTEN and crash-recovery heartbeat) + producer headroom`.

```ts
const exportsWorker = createBetterWorker({ pgPool, queues, process: ['exportRun'], concurrency: 2 })
const apiWorker = createBetterWorker({ pgPool, queues, process: ['advanceRun'], concurrency: 8 })
await Promise.all([exportsWorker.start(), apiWorker.start()])
```

Keep individual executions below 4 hours: Graphile can reclaim a live job beyond that threshold; a `timeout` below 4 hours enforces this.

### Crash recovery

On its own, Graphile keeps a job and its serial lane locked for at least 4 hours after a hard crash (OOM, `SIGKILL`). Started workers shorten that to about `staleAfter`, 5 minutes by default.

```ts
const worker = createBetterWorker({
  pgPool,
  queues,
  crashRecovery: { heartbeatInterval: 30_000, staleAfter: 300_000 } // the defaults; `false` turns it off
})
```

Each started worker records a heartbeat every `heartbeatInterval` milliseconds and, after each one, looks for workers whose last heartbeat is older than `staleAfter`. The sweep unlocks their jobs and lanes with the error `Worker <pool id> stopped sending heartbeats (crashed or blocked); released by crash recovery`, and Graphile retries them with its normal backoff. The crashed run keeps its attempt, so a job that keeps running out of memory eventually fails permanently and shows up as failed in `listJobs`. `onPermanentFailure` does not fire for it, because no handler ran; watch for `worker.crash_recovery.released` warnings in your logs. Any number of instances can sweep at once.

The risk is a worker that is alive but cannot heartbeat for `staleAfter`. It looks dead, so its job runs a second time while the first run is still going, even in a serial lane. The heartbeat holds its own pool connection, so busy jobs cannot starve it, but it shares the event loop with your handlers. A handler that blocks the event loop synchronously (CPU-bound work, synchronous I/O) for `staleAfter` stops it. Keep `staleAfter` well above your longest synchronous stretch, or turn crash recovery off for that worker. A worker that comes back logs `worker.crash_recovery.presumed_dead`. When it finishes the job, Graphile deletes it, even if another worker is still running it. With `jobOutcomes` on, the worker completes jobs itself only while it holds the lock, so the job survives. `staleAfter` must be at least three heartbeat intervals.

- Only started workers heartbeat. Jobs held by `runOnce()`, by workers with `crashRecovery: false` or by workers on an older version of this library are left to Graphile's 4-hour scan.
- Handlers wait for the worker's first heartbeat before they run.
- The heartbeat table, `_bgw_worker_heartbeats` in the Graphile schema, is created by `migrate()`. A worker also creates it on start if it is missing. If the worker's role cannot, the worker logs `worker.crash_recovery.disabled` and runs without crash recovery; with `crashRecovery` set explicitly, `start()` fails instead. Stopped workers leave their row; a later sweep removes it.

### Timeouts

Graphile has no execution time limit: a hung handler holds its lock and its lane until it returns. Set `timeout` (milliseconds) on a queue, or `defaultTimeout` on a worker for every queue without its own.

```ts
const exportRun = defineQueue({
  name: 'exportRun',
  inputSchema: z.object({ runId: z.string() }),
  timeout: 60 * 60_000,
  processFn: async ({ runId }, ctx) => {
    await fetch(`https://exports.example.com/${runId}`, { method: 'POST', signal: ctx.signal })
  }
})
const worker = createBetterWorker({ pgPool, queues, defaultTimeout: 20 * 60_000 })
```

The clock covers one attempt: each retry and each continuation starts a fresh one, and a cron-init queue's timeout also applies to its `initFn`. There is no per-job override; give jobs with different limits their own queues. When the time is up, `ctx.signal` aborts with a `JobTimeoutError` as its `reason`, and the attempt fails with `JobTimeoutError` whether or not the handler stops. Graphile releases the lock and lane at once. Normal retry rules apply, so the timeout spends an attempt and backs off, and on the last attempt it is a permanent failure. `onJobFinished` and the `job.completed` log report `errorType: 'JobTimeoutError'`, and Graphile stores the message as `last_error`. `getQueueDefinitions()` shows each queue's effective timeout.

JavaScript cannot kill a function. A handler that ignores its signal keeps running, detached, in your process; a retry can run next to it. Everything it starts through the context after the timeout is refused with `JobTimeoutError`: `ctx.step.run`, `ctx.progress.set` and `flush`, `ctx.continue()`, `ctx.createJob` and `ctx.createJobs`. A checkpoint or progress write already in flight lands only if it commits before the failure is recorded; it can never be written into a later attempt. Raw `ctx.helpers` and your own side effects are not guarded, so pass `ctx.signal` to HTTP and database clients and check it in loops. When the detached handler finally settles, a `job.timed_out_handler_settled` warning is logged. Synchronous code that never yields blocks the event loop, so no timeout can fire.

`ctx.continue()` after the timeout is refused, so keep time slices well inside it. A continuation that is already committing when the timeout fires wins, and the run counts as continued. During graceful shutdown, Graphile aborts `ctx.signal` with its own reason after `gracefulShutdownAbortTimeout`; `ctx.continue()` still works then. A hung handler cannot hold up `stop()` past its timeout: the attempt fails and shutdown proceeds without waiting for the detached handler.

## Payloads and failure policy

Producer inputs use `z.input`; consumers receive `z.output`. Both use async parsing, including async refinements and transforms. Validation runs before enqueue by default and again on the consumer because other producers can write jobs. Cron-init validates each returned input once during enqueue; it never enqueues transformed outputs.

Wire inputs must contain plain JSON values: strings, finite numbers, booleans, null, arrays and plain objects. Dates, bigints, functions, class instances, cycles and nested undefined values are rejected. Encode dates as strings at the producer; a schema can transform those strings into Dates in the handler. Root `undefined` is supported for optional/defaulted inputs and restored before parsing. `validateOnEnqueue: false` skips schema checks, but still enforces the JSON wire format.

All new jobs, including scheduled cron, use a version-2 envelope with payload, trace metadata, optional checkpoints and progress in separate fields. Business fields named `__trace`, `traceparent`, `__bgw`, `steps` or `progress` survive inside the payload. Only this format is accepted. Raw Graphile producers must supply the current envelope; prefer instance enqueue methods.

Input-schema failures and `NonRetriableError` are permanent. A `ZodError` thrown by your handler, such as validating a downstream response, follows normal retry behavior.

To choose when the next attempt runs, throw `RetryAtError` with the time and the original error:

```ts
import { RetryAtError } from 'better-graphile-worker'

processFn: async ({ connectionId }, ctx) => {
  try {
    await syncBank(connectionId)
  } catch (error) {
    if (!(error instanceof RateLimitError)) throw error
    const delays = [60_000, 3_600_000, 86_400_000, 7 * 86_400_000]
    const delay = delays[Math.min(ctx.attempt, delays.length) - 1]!
    throw new RetryAtError(new Date(Date.now() + delay), { cause: error })
  }
}
```

It spends the attempt like any other failure: on the last attempt the job fails permanently, `onPermanentFailure` runs and the time is ignored. Logs, `onJobFinished` and Graphile's `last_error` record the cause's name and message, and the scheduled time appears as `retryAt` in `onJobFinished` and `retry_at` in the `job.completed` log. Completed steps and progress carry over. The job keeps its id. The time is a lower bound: if it is sooner than Graphile's own backoff for that attempt (`exp(min(attempt, 10))` seconds, about 3 s after the first attempt, 55 s after the fourth and at most about 6 h), the job waits for the backoff instead. A waiting job does not hold its serial lane, so later jobs in the lane run before it. Re-adding its `jobKey` while it waits replaces it as usual: new input, no steps, attempts reset and the wait cancelled. To wait without spending an attempt, use `ctx.continue({ runAt })`; to stop retrying, throw `NonRetriableError`.

- `permanentFailure: 'discard'` is an explicit choice: acknowledge non-retriable jobs and let Graphile delete them. Failure history then exists only in hooks and the optional local history.
- `permanentFailure: 'retain'` is the default. It marks the locked job's budget exhausted, then lets Graphile record the original error and unlock it. It remains visible in PostgreSQL after restart. `retryJobs` resets attempts to zero; a retained non-retriable job has a reduced attempt limit, so an explicit retry grants another attempt. Ordinary exhausted retries remain in PostgreSQL under either policy.

Continuation, retention, retry scheduling, checkpointing, job outcomes, progress, crash recovery and payload debugging use an isolated adapter for Graphile 0.17's private jobs table. The peer range is bounded accordingly; upgrades require its PostgreSQL regression suite.

## Context and durable steps

Contexts include `jobId`, the task `queue`, `attempt`, `maxAttempts`, `logger`, `span`, `signal` (aborted on shutdown or [timeout](#timeouts)), raw Graphile `helpers`, typed `createJob`/`createJobs`, `continue`, `step`, `progress`, and `cron` metadata (`ts: Date`, optional `backfilled`). Cron-init contexts use the suffixed task name.

```ts
const user = await ctx.step.run('fetch-user-v1', async () => {
  return { id: payload.userId, email: 'person@example.com' }
})
await ctx.step.run('send-email-v1', async () => {
  await sendEmail(user.email) // void is restored as undefined on replay
})
```

Completed steps return their cached result on retry. Concurrent calls with the same ID share pending work. Different checkpoints are serialized and patched atomically in PostgreSQL, and writes require the worker's current lock. A failed checkpoint is never treated as completed. Step IDs such as `toString` and `__proto__` work normally.

Default results must preserve their type through JSON. TypeScript rejects Date and unsupported result types, and runtime validation catches unsafe JavaScript values. To preserve richer values, pass a codec whose decode validates stored data:

```ts
import { z } from 'zod'
const when = await ctx.step.run('timestamp-v1', () => new Date(), {
  encode: date => date.toISOString(),
  decode: wire => new Date(z.string().parse(wire))
}) // Date on both fresh execution and replay
```

Use the same result type and codec for every use of an ID. Version IDs when the stored format changes. Checkpoints cannot guarantee exactly-once external effects: a process can crash after an effect but before its checkpoint. External effects must be idempotent. Steps memoize results; they do not implement suspended workflows or durable sleeps.

## Progress

`ctx.progress` stores one JSON value on the running job, such as a phase, counts or a retry budget. Other processes read it by job id, for example an admin endpoint that polls. An optional `progressSchema` types and validates it; without one, any JSON value is accepted.

```ts
const importOrders = defineQueue({
  name: 'importOrders',
  inputSchema: z.object({ shopId: z.string() }),
  progressSchema: z.object({ phase: z.enum(['fetch', 'write']), done: z.number(), retriesLeft: z.number() }),
  processFn: async ({ shopId }, ctx) => {
    const retriesLeft = ctx.progress.get()?.retriesLeft ?? 3 // survives retries
    await ctx.progress.set({ phase: 'fetch', done: 0, retriesLeft })
    const orders = await fetchOrders(shopId)
    for (const [index, order] of orders.entries()) {
      await saveOrder(order)
      await ctx.progress.set({ phase: 'write', done: index + 1, retriesLeft })
    }
  }
})

// API or admin process, with the worker or a job client:
const job = await producer.getJobProgress('importOrders', jobId)
// null: no such job. job.progress: undefined until the job sets it.
```

- `set` validates the value, makes it visible to `get()` at once and schedules a write. The first value is written immediately; later values within one second coalesce into a single trailing write, so a tight loop costs at most about one `UPDATE` per second. `await ctx.progress.flush()` writes now. A failed write rejects the next `set` or `flush`, then is retried.
- Progress is stored in the job's row next to step checkpoints, and writes require the worker's current lock. A stale worker cannot overwrite it.
- When an attempt fails, its latest value is written before Graphile schedules the retry, so the next attempt's `get()` returns it. `ctx.continue()` hands the latest value to the continuation.
- Graphile deletes a job when it succeeds, and its progress with it: `getJobProgress` then returns `null`. Record final results in your own tables. A continuation has a new job id, so a reader holding the old id also gets `null`. With [`jobOutcomes`](#looking-up-a-job-after-it-finishes) enabled, `getJob(id)` tells the two apart and gives the continuation's id in `continuedTo`.
- The `progressSchema` output must be JSON; encode dates as strings. Readers get the stored value without parsing it again, so a value written under an older schema is returned as it was stored. Change the shape compatibly, or version it.
- `getJobProgress` takes the queue name so the result is typed, and returns `null` for a job of a different queue. A cron-init `initFn` has untyped progress that it can only use itself.

## Lifecycle and observability

Concurrent `start()` calls share startup; stop during startup waits and shuts down the resulting runner. Starts during shutdown wait for completion. `runOnce()` requires an idle instance and cannot overlap another run. Call `stop()` and then `pgPool.end()` when finished.

`stop({ timeout: milliseconds })` rejects with `ShutdownTimeoutError` if the deadline expires. Shutdown continues, the runner stays tracked, and a subsequent `stop()` or `waitUntilStopped()` can await it. Timers and optional signal listeners are cleaned up. Graphile's graceful-shutdown/abort options remain available under `graphile`.

Hooks include `createLogger`, `onJobFinished`, `onPermanentFailure`, `onEnqueueFail` and `shouldSkipEnqueue`. Observer hooks may return promises and are awaited; their errors are reported separately without changing acknowledgement or replacing a job error. Logger failures get a fallback. Keep observers quick: a slow observer still delays completion. A failed attempt's `job.completed` log carries `error_type` and `error_message` (trimmed to 500 characters); `onJobFinished` gets the full `errorMessage`. `onPermanentFailure` receives the job's `payload` so you can tag reports with tenant ids; it may contain personal data, so forward only what you need. `shouldSkipEnqueue` is a behavioral decision; its errors propagate.

Each instance captures `otel: { api }` (or detects the optional peer). Creating another instance cannot change its adapter. Pass `{ api: null }` to disable tracing. Producer/consumer spans link through valid W3C trace contexts, including unsampled flags. Tracing method failures do not change job outcomes. Configure tracing through the instance options.

## Administration, CLI and testing

```ts
await worker.getJobStats()
const first = await worker.listJobs({ limit: 50, state: 'failed', includePayload: false })
const last = first.at(-1)
if (last) await worker.listJobs({ limit: 50, state: 'failed', includePayload: false, before: last.cursor })
await worker.retryJobs(['123'])
await worker.failJobs(['123'], 'gave up')
await worker.getJobProgress('importOrders', '123')
worker.getQueueDefinitions()
```

`pending`, `running` and `failed` are current PostgreSQL states. `recentCompleted` and `recentFailed` count entries in this instance's optional bounded history (`completedJobs: { maxPerQueue: 50 }`); they are neither durable nor lifetime totals. For a durable record per job, use [job outcomes](#looking-up-a-job-after-it-finishes). Database failures cannot be hidden by local history.

Metadata uses Graphile's public `jobs` view. `includePayload` defaults to false and returns null payloads. Set it to true to add the private-table debugging join and inspect business payloads. Listing validates integer limits (0–1000), offsets and states. Use the returned `cursor` to retain PostgreSQL timestamp precision; the display `createdAt` loses sub-millisecond precision. Cursor and offset cannot be combined. Counts scan current jobs; avoid unnecessary high-frequency polling on large queues. Schema identifiers, including uppercase names, are quoted consistently.

### Looking up a job after it finishes

Graphile deletes a job once it succeeds. Set `jobOutcomes` on your workers to keep each job's final state in PostgreSQL, then look a job up by id from a worker or a job client:

```ts
const worker = createBetterWorker({
  pgPool,
  queues,
  jobOutcomes: { retention: 14 * 24 * 60 * 60 * 1000 } // milliseconds; defaults to 7 days
})
await worker.migrate() // also creates graphile_worker.bgw_job_outcomes

const job = await producer.getJob(jobId)
switch (job.state) {
  case 'pending': // waiting for job.runAt
  case 'running':
  case 'retrying': // an attempt failed (job.lastError); the next one runs at job.runAt
    break
  case 'completed':
    break
  case 'continued': // ctx.continue() ended this run; job.continuedTo is the next run's id
    break
  case 'failed': // permanently: attempts exhausted, NonRetriableError or failJobs
    break
  case 'not_found': // never existed, deleted outside the worker, or its outcome expired
}
```

A job still in Graphile's table is reported from there, so a failed job that you retry reads `pending` again. Every state except `not_found` carries `queue` (the Graphile task identifier), `attempts`, `maxAttempts`, `createdAt` and `lastError`; finished states add `finishedAt`. Payloads are not stored.

With the option on, a worker completes a successful job itself: it deletes the job and writes its outcome in one transaction while holding the job's lock. A continuation's enqueue, its outcome and the old run's removal also commit together. So within the retention period, a completed or continued job always has an outcome. If that write fails, the attempt fails and is retried, as with a failed step checkpoint. A failure outcome that cannot be written is logged, not raised. A permanently failed job also stays in Graphile's table unless you chose `permanentFailure: 'discard'`.

Enable the option on every worker instance that processes jobs in the schema; an instance without it lets Graphile complete jobs and records nothing. Producers need no option: `getJob` reads live jobs either way, and outcomes when the table exists. Workers create the table on `migrate()`, `start()` and `runOnce()`, and delete expired outcomes at most once a minute while finishing jobs. Worker roles need `SELECT`, `INSERT`, `UPDATE` and `DELETE` on the table; roles that call `getJob` need `SELECT` on it and on Graphile's `jobs` view.

### CLI

```ts
import { createCli } from 'better-graphile-worker/cli'
await createCli(worker)(process.argv.slice(2))
```

Commands: `list-queues`, `schema <name>`, `create-job <name> [json]`, `stats`, `list-jobs`, `get-job <id>`, `retry <id>`, `fail <id>`, `run-once`. `--json` uses the same structured error path for argument and execution errors. Schema help describes producer input, including transforms.

### Testing

```ts
import { createTestHarness } from 'better-graphile-worker/testing'
const harness = createTestHarness(queues)
await harness.process('sendEmail', { to: 'person@example.com' })
console.log(harness.logs, harness.enqueued, harness.continued)
```

The harness invokes handlers directly without Graphile acknowledgement/retry hooks or PostgreSQL. Parsing, child enqueue validation and step replay share production code. Invocations get fresh IDs. Supply the same `{ jobId: 'retry-1' }` explicitly to simulate retry; it replays checkpoints and returns the last progress, which each invocation keeps when it ends. Different queues never share checkpoints or progress. Read a run's progress with `ctx.progress.get()` on the context `process` returns. `init` accepts only cron-init names and infers its returned inputs. Captured child jobs include queue, wire input and options. Narrowing `job.queue` also narrows its producer input payload; `context(name)` and the context returned by `process` preserve the selected literal name. For shared handler-free contracts, pass the registry as the second argument: `createTestHarness(contracts, handlers)`.

## Types and major-version migration

`QueueName`, `QueueInput`, `QueuePayload`, `InputsOf` and `PayloadsOf` preserve names and input/output inference. An unknown queue resolves to `never`. Enqueue calls correlate names with payloads; uncorrelated unions fail compilation. `TasksOf` requires current payload envelopes around producer input, before transforms. Worker and harness constructors check schema/handler agreement even for manually constructed inline definitions. `DerivedJobOptions`, `ContinueOptions`, `PreparedJob`, `PrepareJobFn` and `PrepareJobsFn` type the long-running-job APIs. Instance methods need no global augmentation. Augment `GraphileWorker.Tasks` only when using Graphile's raw typed API.

Low-level builders, SQL adapters and payload helpers are available exclusively from `better-graphile-worker/advanced`. Application code should use instance methods. The package root exports constructors, contract helpers, public types and errors.

Custom adapters must implement `EnqueueAdapter.addIdempotentJobs(specs)` to accept `idempotencyKey`; without it, keyed enqueues are rejected. Custom `EnqueueAdapter.addJobs(specs, jobKeyPreserveRunAt?)` implementations must return jobs in input order and honor the optional preserve-run-at flag to support derived keyed batches. Existing one-argument adapters remain type-compatible, but need to handle that flag before using `preserve_run_at`. The built-in adapter matches unkeyed rows by content, so it rejects unkeyed specs that are identical except for `identifier` or `queueName`; enqueue those separately.

This implementation requires a **major release**. There are no deprecated aliases or readers for the previous wire format:

1. Pause old producers and cron scheduling. Drain old queued jobs before switching versions. Delayed or failed jobs that need to survive the upgrade must be explicitly re-enqueued through the new client with their business input and scheduling options. Resubmission can repeat work; preserve application idempotency keys.
2. Stop the old workers and upgrade producers and workers together. New workers accept only version-2 envelopes; raw, flat and version-1 jobs are rejected as permanent failures.
3. Rename `createQueue` to `defineQueue`; keep `processFn` and cron-init `initFn` in their definitions:

   ```ts
   const sendEmail = defineQueue({
     name: 'sendEmail',
     inputSchema: z.object({ to: z.string() }),
     processFn: (payload, ctx) => {
       ctx.logger.info('sending', { to: payload.to })
     }
   })
   const queues = defineQueues([sendEmail])
   const worker = createBetterWorker({ pgPool, queues })
   const harness = createTestHarness(queues)
   ```

   Inline context enqueues now take queue references: replace `ctx.createJob('sendEmail', input)` with `ctx.createJob(sendEmail, input)`, and do the same for `ctx.createJobs`. Instance methods still use typed names. For separately shared producer contracts, supply a typed `handlers` registry instead.
4. Move low-level imports to `/advanced`. Remove `setOtelApi` and pass `otel: { api }` to each instance. Use `QueueName` in place of `QueueNames`. Advanced `bindCreateJob` requires an enqueue adapter, and `createJobsApi` requires an explicit registry.
5. Use `recentCompleted` and `recentFailed` for bounded local history; `failed` always means current database failures. Job listing returns metadata by default; request `includePayload: true` for payload debugging. Permanent failures are retained by default; explicitly choose `'discard'` if deletion is desired.

JSON-changing step outputs such as Dates require codecs; void replays as undefined. The pg peer requires 8.16+. Batch keys/modes, lossy wire values, malformed numeric options and reserved queue names are rejected. Handler-thrown Zod errors retry; use `NonRetriableError` for intentional permanent failures. Shutdown timeouts reject while retaining the pending shutdown.

## Development

```sh
bun install --frozen-lockfile
bun run lint
DATABASE_URL=postgres://postgres:postgres@localhost:5432/bgw_test bun test
bun run build
bun run test:consumers
bun run attw
bun run publint
bun run verify:package
```

The PostgreSQL tests create/drop their own schema; the existing baseline integration tests also use the default Graphile schema. Use a disposable test database. Packed consumers exercise NodeNext/bundler compilation, optional-peer absence and native Node runtime behavior; set `PEER_PROFILE=minimum|latest`, `TYPESCRIPT_VERSION=5.7.3` and optionally `NODE_VERSION=20` to select a profile.

## Releasing

Squash PRs with conventional titles (`fix:`, `feat:`, or `feat!:`). Release Please keeps the version and changelog in a release PR; merge that PR to publish with release notes and npm provenance. See [release and recovery instructions](docs/operations/releasing.md).
