---
name: backend-graphile-queues
description: Create and enqueue typed Graphile Worker jobs with better-graphile-worker — pick regular vs cron vs cron-init, write the queue with defineQueue, register it with defineQueues, and enqueue it. Use when adding background jobs, scheduled tasks, or enqueueing work. Do not wrap every handler in step.run; steps are only for multi-stage jobs whose early side effects must not rerun on retry.
---

# Creating jobs

Default path: a **plain `processFn`** in a `defineQueue` call. No `step.run`. Register it in `defineQueues`, then enqueue it.

Use `ctx.step.run` only when a later stage can fail after an earlier stage already did expensive or non-repeatable work.

## Workflow

1. Pick the queue kind (below).
2. Write one `defineQueue` call that holds the schema, options and handler together, in the feature that owns the work.
3. Add it to the app's `defineQueues([...])` list.
4. Enqueue it: `ctx.createJob(queue, input)` inside handlers, `worker.createJob(name, input)` or `worker.jobs.name(input)` elsewhere.
5. Keep `processFn` small, typed and retry-safe.

## Pick the kind

| Need | Kind | Config |
| --- | --- | --- |
| Someone/something triggers work with data | Regular | `inputSchema` + `processFn` |
| One scheduled run, no payload | Cron | `cron` + `processFn` (no schema) |
| Schedule gathers many items, each retries alone | Cron-init | `cron` + `inputSchema` + `initFn` + `processFn` |

Fan-out of N items is `createJobs(name, items)` or cron-init. Do **not** loop `createJob` unless options must differ per item. A schema of `z.array(...)` is still **one** payload.

`serial: true` (or a shared string) when jobs for that task must run one at a time. For one lane per entity (per shop, per user), use `deriveJobOptions` instead. Runner parallelism is set per worker instance with `concurrency`.

## Regular queue (default)

```ts
import { defineQueue } from 'better-graphile-worker'
import { z } from 'zod'

export const sendWelcomeEmail = defineQueue({
  name: 'sendWelcomeEmail',
  inputSchema: z.object({
    userId: z.string(),
    template: z.string()
  }),
  maxAttempts: 5,
  processFn: async (payload, ctx) => {
    ctx.logger.info('sending welcome email', { userId: payload.userId })
    await sendEmail(payload.userId, payload.template)
  }
})
```

## Cron queue

No `inputSchema`. `payload` is `undefined`. Trigger manually with `worker.createJob('myDailyTask')` or `worker.triggerCron('myDailyTask')`.

```ts
export const myDailyTask = defineQueue({
  name: 'myDailyTask',
  cron: '0 9 * * *',
  maxAttempts: 3,
  processFn: async (_payload, ctx) => {
    ctx.logger.info('running daily task')
  }
})
```

`cron` may be a string, a readonly string array, or a Graphile cron matcher. Extra cron fields go on `cronOptions`.

## Cron-init queue

`initFn` gathers producer inputs; each becomes its own job on `{name}`. Cron fires `{name}_cron-init`.

```ts
export const syncOrders = defineQueue({
  name: 'syncOrders',
  cron: '0 * * * *',
  inputSchema: z.object({
    orderId: z.string(),
    status: z.string()
  }),
  maxAttempts: 3,
  initFn: async (ctx) => {
    const orders = await db.order.findMany({
      where: { syncStatus: 'pending' },
      select: { id: true, status: true }
    })
    return orders.map((o) => ({ orderId: o.id, status: o.status }))
  },
  processFn: async (payload, ctx) => {
    ctx.logger.info('syncing order', { orderId: payload.orderId })
    await syncOrderToExternalSystem(payload.orderId)
  }
})
```

You can still `worker.createJob('syncOrders', { orderId, status })` for one item.

## Register

```ts
import { createBetterWorker, defineQueues } from 'better-graphile-worker'

export const queues = defineQueues([sendWelcomeEmail, myDailyTask, syncOrders])
export const worker = createBetterWorker({ pgPool, queues })
```

Names must be unique. `defineQueues` preserves literals and rejects collisions, including with `{name}_cron-init`. Run `worker.migrate()` before starting producers or workers.

When an API process must not import handler code, define handler-free contracts in a shared module and pass `handlers` to the worker. See the README section "Shared producer contracts".

## Enqueue

Inside a handler, pass the queue **definition**, not its name:

```ts
processFn: async (payload, ctx) => {
  await ctx.createJob(sendWelcomeEmail, { userId: payload.userId, template: 'welcome' })
}
```

Elsewhere, use the worker or a job client by name:

```ts
const jobId = await worker.createJob('sendWelcomeEmail', {
  userId: '123',
  template: 'welcome'
})

await worker.createJobs('sendWelcomeEmail', [
  { userId: '123', template: 'welcome' },
  { userId: '456', template: 'welcome' }
])

// API process without handlers:
import { createJobClient } from 'better-graphile-worker/client'
const producer = createJobClient({ pgPool, queues })
await producer.jobs.sendWelcomeEmail({ userId: '123', template: 'welcome' })
```

`jobId` is `string | null` (`null` when `shouldSkipEnqueue` returns true, e.g. while seeding). `createJobs` is one atomic statement and returns ids in input order.

```ts
await worker.createJob('sendWelcomeEmail', payload, {
  priority: 0,           // lower = sooner
  runAt: new Date(),
  maxAttempts: 5,
  jobKey: 'welcome:123',
  jobKeyMode: 'replace'  // 'replace' | 'preserve_run_at' | 'unsafe_dedupe'
})
```

Batches cannot take a shared `jobKey`; derive per-item keys with `deriveJobOptions`. To commit a job together with your own database change, use `worker.prepareJob` and run its SQL in your transaction.

Producer types are `z.input`. Handlers get `z.output` after parsing. Enqueue validation is on unless `validateOnEnqueue: false`.

## Writing `processFn`

- Put **ids** in the payload, not blobs or live objects. Wire inputs must be plain JSON: encode dates as strings and let the schema transform them.
- Use `ctx.logger` (wired to the span). Prefer `ctx` over destructuring so later fields stay available.
- **Throw to retry** (up to `maxAttempts`). Default max attempts is 4 if omitted.
- Throw `NonRetriableError` (from `better-graphile-worker`) for a permanent failure. An input-schema failure is also permanent; a `ZodError` your own code throws still retries.
- Make work **idempotent** when you can (`jobKey`, upserts, provider idempotency keys). That is enough for most jobs.
- For long work, return `ctx.continue()` or `ctx.continue({ runAt })` to end this run and enqueue the same job again, instead of throwing to wait.
- `ctx.signal` is Graphile's abort signal. `ctx.helpers` is the raw Graphile API.

Do not add `step.run` to a job that is one action (send email, write a row, call one API).

## Steps (opt-in)

Skip this section unless the job is a **pipeline**: stage A has a side effect or is expensive, stage B can fail, and retrying A would duplicate work or waste time.

```ts
processFn: async (payload, ctx) => {
  const user = await ctx.step.run('fetch-user-v1', async () => {
    return await db.users.find(payload.userId)
  })

  await ctx.step.run('charge-v1', async () => {
    return await stripe.charges.create({ customer: user.stripeId })
  })

  await ctx.step.run('send-receipt-v1', async () => {
    await sendEmail(user.email)
  })
}
```

Rules when you do use steps:

- Unique ids per job (`send-email-${i}` in loops). Version the id when the stored result's shape changes.
- Results must survive JSON. For a Date or another rich value, pass a codec (`{ encode, decode }`). `void` replays as `undefined`.
- On retry the handler starts over; completed steps return the cache and do not rerun `fn`.
- This is replay-with-cache, not sleep or suspended workflows.
- Do not wrap every line. Cache only stages that must not repeat.

**Do not use steps when:**

- The job is one call or already idempotent.
- Cron-init already isolated items (retry the item job, not steps inside `initFn`, unless init itself is a pipeline).
- You only want logs or spans — use `ctx.logger` / `ctx.span`.

## Testing

```ts
import { createTestHarness } from 'better-graphile-worker/testing'

const harness = createTestHarness(queues)
await harness.process('sendWelcomeEmail', { userId: '123', template: 'welcome' })
// harness.logs, harness.enqueued, harness.continued
```

No Postgres. To simulate a retry of a job that uses steps, call `process` twice with the same `{ jobId: 'retry-1' }`. For handler-free contracts, use `createTestHarness(contracts, handlers)`.

## CLI

```ts
// scripts/queues.ts
import { createCli } from 'better-graphile-worker/cli'
await createCli(worker)(process.argv.slice(2))
```

```bash
bun scripts/queues.ts list-queues
bun scripts/queues.ts schema sendWelcomeEmail
bun scripts/queues.ts create-job sendWelcomeEmail '{"userId":"123","template":"welcome"}'
```

`--priority`, `--run-at`, `--max-attempts`, `--job-key` and `--json` are supported.

## Checklist

- [ ] Right kind (regular / cron / cron-init), not a step-wrapped regular job by default
- [ ] Unique `name`, Zod schema only where there is a payload
- [ ] Registered in `defineQueues`
- [ ] Handlers enqueue with `ctx.createJob(queueDefinition, input)`; other code by name
- [ ] `processFn` is idempotent or uses `NonRetriableError` correctly
- [ ] `step.run` only around stages that must not rerun
