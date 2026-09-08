# better-graphile-worker

Typed Graphile Worker queues with Zod schemas, cron and cron-init patterns, and optional OpenTelemetry.

This package does **not** fork Graphile Worker. It sits on top of `graphile-worker@0.17` and gives you a typed `createQueue` / `createJob` / `createJobs` API plus an instance-based runner.

## Install

```bash
bun add better-graphile-worker graphile-worker pg zod
```

`@opentelemetry/api` is an optional peer. Install it if you want producer/consumer span linking; otherwise tracing is a no-op.

## Usage

```ts
import {
	createQueue,
	defineQueues,
	createBetterWorker
} from 'better-graphile-worker'
import { Pool } from 'pg'
import { z } from 'zod'

const sendEmail = createQueue({
	name: 'sendEmail',
	inputSchema: z.object({ to: z.string().email() }),
	maxAttempts: 5,
	processFn: async (payload, ctx) => {
		ctx.logger.info('sending', { to: payload.to })
	}
})

const dailySweep = createQueue({
	name: 'dailySweep',
	cron: '0 3 * * *',
	processFn: async (_payload, ctx) => {
		ctx.logger.info('running daily sweep')
	}
})

const syncOrders = createQueue({
	name: 'syncOrders',
	cron: '0 * * * *',
	inputSchema: z.object({ orderId: z.string() }),
	initFn: async () => [{ orderId: '1' }],
	processFn: async (payload, ctx) => {
		ctx.logger.info('syncing order', { orderId: payload.orderId })
	}
})

const queues = defineQueues([sendEmail, dailySweep, syncOrders])

const pgPool = new Pool({ connectionString: process.env.DATABASE_URL })

const worker = createBetterWorker({
	pgPool,
	queues,
	schema: 'graphile_worker',
	concurrency: 10,
	pollInterval: 250,
	completedJobs: { maxPerQueue: 50 },
	hooks: {
		createLogger: ({ queue, jobId }) => console,
		onJobFinished: ({ queue, status, durationMs }) => {
			void queue
			void status
			void durationMs
		},
		onPermanentFailure: ({ error, queue, jobId }) => {
			void error
			void queue
			void jobId
		},
		onEnqueueFail: ({ queue, error }) => {
			void queue
			void error
		},
		shouldSkipEnqueue: () => process.env.SEEDING === '1'
	}
})

await worker.migrate()
await worker.createJob('sendEmail', { to: 'a@b.com' })
await worker.createJobs('sendEmail', [{ to: 'b@c.com' }, { to: 'c@d.com' }])
await worker.jobs.sendEmail({ to: 'd@e.com' })
await worker.start()
```

You own the `pg.Pool`. The package does not construct or close it.

`createJob` / `createJobs` are bound to the instance, so queue names and payloads are inferred from the `queues` array you passed in. Job IDs are plain `string`. `createJob` returns `null` (and `createJobs` returns `[]`) when `shouldSkipEnqueue` is true.

### Producer vs worker process

API servers should enqueue without loading handlers. Pass the same queue objects (or a `QueueContract[]` with `name` + `inputSchema`) to `createJobClient`:

```ts
import { createJobClient } from 'better-graphile-worker/client'

const jobs = createJobClient({ pgPool, queues })
await jobs.migrate()
await jobs.createJob('sendEmail', { to: 'a@b.com' })
```

Call `migrate()` explicitly. `createJob` no longer runs Graphile migrations as a side effect.

## Queue types

| Kind | Config | Tasks |
| --- | --- | --- |
| Regular | `inputSchema` + `processFn` | One task named after the queue |
| Cron | `cron` + `processFn` (no payload) | One scheduled task. Trigger manually with `createJob('dailySweep')` or `worker.triggerCron('dailySweep')` |
| Cron-init | `cron` + `inputSchema` + `initFn` + `processFn` | `{name}_cron-init` gathers items; `{name}` processes each. `triggerCron` fires the init task |

A "queue" here is a Graphile **task identifier**, not Graphile's serialization `queueName`. Use `serial: true` (or a custom string) when jobs for a task must run one at a time.

`cron` accepts a crontab string, an array of strings, or a Graphile `CronMatcher`. Extra cron fields (`backfillPeriod`, `identifier`, `priority`, `jobKey`) go on `cronOptions`. Cron uses the **database clock** unless you set `graphile: { useNodeTime: true }`.

## Payloads

- Producer types use `z.input`; handlers receive `z.output` after `parse`.
- Payloads must be JSON-round-trippable. A `z.date()` output will not survive the jobs table.
- `createJob` always enqueues one job. A schema of `z.array(...)` is still a single payload — use `createJobs` for fan-out.
- Enqueue validation is on by default (`validateOnEnqueue: false` to skip).
- Schema parse failures and `NonRetriableError` are treated as permanent failures (no retries).

## JobContext

`processFn` / `initFn` receive:

- `jobId`, `queue`, `attempt`, `maxAttempts`
- `logger`, `span`
- `signal` (Graphile abort signal)
- `createJob` / `createJobs` (bound to the same worker)
- `cron` (`{ ts, backfilled }`) when Graphile injected a `_cron` payload
- `helpers` for the raw Graphile API
- `step.run(id, fn)` — memoizes JSON-serializable results across retries

```ts
processFn: async (payload, ctx) => {
	const user = await ctx.step.run('fetch-user', async () => {
		return await db.users.find(payload.userId)
	})

	await ctx.step.run('send-email', async () => {
		await sendEmail(user.email)
	})
}
```

On retry the handler runs from the top again, but completed steps return the cached output and do not re-run `fn`. Use unique ids in loops (`send-email-${i}`). `undefined` is stored as `null`. This is replay-with-cache, not Trigger.dev-style sleep/wait checkpointing.

## Hooks

All hooks are optional:

- `createLogger({ queue, jobId, attempt, span })` — default: console
- `onJobFinished` — success/fail metrics
- `onPermanentFailure` — last-attempt or non-retriable failures
- `onEnqueueFail` — `createJob` / `createJobs` database failures
- `shouldSkipEnqueue` — return `true` to skip enqueue (e.g. while seeding)

`span` is always present: a real OpenTelemetry span when `@opentelemetry/api` is installed (or injected via `otel: { api }`), otherwise a no-op. `JobSpan` includes `addEvent` so host loggers can attach events without casting. Producer spans use `PRODUCER` kind and W3C `traceparent`; the legacy `__trace` payload field is still read.

## Admin helpers

```ts
worker.getQueueDefinitions()
await worker.getJobStats()
await worker.listJobs({ limit: 50, queue: 'sendEmail', state: 'pending' })
await worker.retryJobs(['123'])
await worker.failJobs(['123'], 'gave up')
worker.getCompletedJobs()
```

Completed jobs are an **opt-in** in-memory ring (`completedJobs: { maxPerQueue: 50 }`) so they remain visible after Graphile deletes them. `getQueueDefinitions()` includes the Zod `inputSchema` when present. `getJobStats()` returns `{ pending, running, completed, failed }` per task: pending/running from Postgres, completed (and ring `failed`) from the in-memory store when enabled.

## CLI

```ts
import { createCli } from 'better-graphile-worker/cli'

await createCli(worker)(process.argv.slice(2))
```

Commands: `list-queues`, `schema <name>`, `create-job <name> [json]`, `stats`, `list-jobs`, `retry <id>`, `fail <id>`, `run-once`. Add `--json` for machine-readable output.

## Testing

```ts
import { createTestHarness } from 'better-graphile-worker/testing'

const harness = createTestHarness(queues)
await harness.process('sendEmail', { to: 'a@b.com' })
expect(harness.logs[0]?.message).toBe('sending')
```

No Postgres required.

## Types

```ts
import type {
	InferInput,
	InferPayload,
	QueueInput,
	QueueNames,
	TasksOf
} from 'better-graphile-worker'

type Names = QueueNames<typeof queues>
type EmailInput = InferInput<typeof sendEmail>

declare global {
	namespace GraphileWorker {
		interface Tasks extends TasksOf<typeof queues> {}
	}
}
```