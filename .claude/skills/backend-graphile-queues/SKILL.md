---
name: backend-graphile-queues
description: Create and enqueue typed Graphile Worker jobs with better-graphile-worker — pick regular vs cron vs cron-init, write processFn, register the queue, and call createJob. Use when adding background jobs, scheduled tasks, or enqueueing work. Do not wrap every handler in step.run; steps are only for multi-stage jobs whose early side effects must not rerun on retry.
---

# Creating jobs

Default path: a **plain `processFn`**. No `step.run`. Register the queue, enqueue with `createJob`.

Use `ctx.step.run` only when a later stage can fail after an earlier stage already did expensive or non-repeatable work.

## Workflow

1. Pick the queue kind (below).
2. Add `features/[Feature]/queues/[name].ts` with `createQueue`.
3. Register it in `registries/queues.ts` via `defineQueues`.
4. Enqueue from a handler / provider with `createJob` / `createJobs`.
5. Keep `processFn` small, typed, and retry-safe.

## Pick the kind

| Need | Kind | Config |
| --- | --- | --- |
| Someone/something triggers work with data | Regular | `inputSchema` + `processFn` |
| One scheduled run, no payload | Cron | `cron` + `processFn` (no schema) |
| Schedule gathers many items, each retries alone | Cron-init | `cron` + `inputSchema` + `initFn` + `processFn` |

Fan-out of N items is `createJobs(name, items)` or cron-init. Do **not** loop `createJob` unless options must differ per item. A schema of `z.array(...)` is still **one** payload.

`serial: true` (or a custom string) when jobs for that task must run one at a time. Do not set per-queue concurrency; runner parallelism is global (`GRAPHILE_WORKER_CONCURRENCY`).

## Regular queue (default)

```ts
// features/[Feature]/queues/sendWelcomeEmail.ts
import { createQueue } from '../../../lib/queue/create-queue'
import { z } from 'zod'

export default createQueue({
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

No `inputSchema`. `payload` is `undefined`. Trigger manually with `createJob('myDailyTask')` or `worker.triggerCron('myDailyTask')`.

```ts
export default createQueue({
  name: 'myDailyTask',
  cron: '0 9 * * *',
  maxAttempts: 3,
  processFn: async (_payload, ctx) => {
    ctx.logger.info('running daily task')
  }
})
```

`cron` may be a string, string[], or Graphile `CronMatcher`. Extra cron fields go on `cronOptions`.

## Cron-init queue

`initFn` gathers; each item becomes its own job on `{name}`. Cron fires `{name}_cron-init`.

```ts
export default createQueue({
  name: 'syncOrders',
  cron: '0 * * * *',
  inputSchema: z.object({
    orderId: z.string(),
    status: z.string()
  }),
  maxAttempts: 3,
  initFn: async (ctx) => {
    const orders = await prisma.order.findMany({
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

You can still `createJob('syncOrders', { orderId, status })` for one item.

## Register

```ts
import { defineQueues } from 'better-graphile-worker'
import sendWelcomeEmail from '../features/myFeature/queues/sendWelcomeEmail'
import myDailyTask from '../features/myFeature/queues/myDailyTask'

export const queues = defineQueues([sendWelcomeEmail, myDailyTask])
```

Names must be unique. `defineQueues` preserves literals and rejects collisions (including `{name}_cron-init`).

## Enqueue

```ts
import { createJob, createJobs } from '../../../lib/queue/create-job'

const jobId = await createJob('sendWelcomeEmail', {
  userId: '123',
  template: 'welcome'
})

await createJobs('sendWelcomeEmail', [
  { userId: '123', template: 'welcome' },
  { userId: '456', template: 'welcome' }
])
```

`jobId` is `string | null` (`null` when `shouldSkipEnqueue` is true, e.g. seeding).

```ts
await createJob('sendWelcomeEmail', payload, {
  priority: 0,           // lower = sooner
  runAt: new Date(),
  maxAttempts: 5,
  jobKey: 'welcome:123',
  jobKeyMode: 'replace'  // 'replace' | 'preserve_run_at' | 'unsafe_dedupe'
})
```

Producer types are `z.input`. Handlers get `z.output` after parse. Enqueue validation is on unless `validateOnEnqueue: false`.

API processes should enqueue through `createJob` / `createJobClient`, not load `processFn`.

## Writing `processFn`

- Put **ids** in the payload, not blobs or live objects. Payloads must JSON-round-trip (`z.date()` output will not survive the jobs table).
- Use `ctx.logger` (wired to the span). Prefer `ctx` over destructuring so later fields stay available.
- **Throw to retry** (up to `maxAttempts`). Default max attempts is 4 if omitted.
- Throw `NonRetriableError` (or fail Zod parse) for permanent failure — no retry.
- Make work **idempotent** when you can (`jobKey`, upserts, provider idempotency keys). That is enough for most jobs.
- `ctx.createJob` / `ctx.createJobs` enqueue follow-up work on the same worker.
- `ctx.signal` is Graphile's abort signal. `ctx.helpers` is the raw Graphile API.

Do not add `step.run` to a job that is one action (send email, write a row, call one API).

## Steps (opt-in)

Skip this section unless the job is a **pipeline**: stage A has a side effect or is expensive, stage B can fail, and retrying A would duplicate work or waste time.

```ts
processFn: async (payload, ctx) => {
  const user = await ctx.step.run('fetch-user', async () => {
    return await db.users.find(payload.userId)
  })

  await ctx.step.run('charge', async () => {
    return await stripe.charges.create({ customer: user.stripeId })
  })

  await ctx.step.run('send-receipt', async () => {
    await sendEmail(user.email)
  })
}
```

Rules when you do use steps:

- Unique ids per job (`send-email-${i}` in loops).
- Return values must be JSON-serializable. `undefined` is stored as `null`.
- On retry the handler starts over; completed steps return the cache and do not rerun `fn`.
- This is replay-with-cache, not sleep/wait/checkpointing.
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
```

No Postgres. To simulate a retry of a job that uses steps, call `process` twice with the same `jobId`.

## File structure

```
apps/backend/src/
  registries/queues.ts
  features/[Feature]/queues/*.ts
  lib/queue/          # createQueue, bound createJob, worker instance, CLI
```

Import `QueueName` / `QueueInput<T>` from `../../../registries/queues` when feature code needs them.

## CLI

```bash
bun run apps/backend/src/lib/queue/cli.ts list-queues
bun run apps/backend/src/lib/queue/cli.ts schema sendWelcomeEmail
bun run apps/backend/src/lib/queue/cli.ts create-job sendWelcomeEmail '{"userId":"123","template":"welcome"}'
```

`--priority`, `--run-at`, `--max-attempts`, `--job-key` are supported.

## Checklist

- [ ] Right kind (regular / cron / cron-init), not a step-wrapped regular job by default
- [ ] Unique `name`, Zod schema only where there is a payload
- [ ] Registered in `defineQueues`
- [ ] Enqueue site uses `createJob` / `createJobs` with `z.input`
- [ ] `processFn` is idempotent or uses `NonRetriableError` correctly
- [ ] `step.run` only around stages that must not rerun
