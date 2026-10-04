# better-graphile-worker

A typed layer over [Graphile Worker](https://worker.graphile.org): queues defined with Zod schemas, cron and cron-init, durable step checkpoints, continuations, prepared enqueue SQL, and optional OpenTelemetry. The user owns the PostgreSQL pool. Graphile does the scheduling, locking and retries; this library adds types, validation and the pieces Graphile lacks.

It is a published npm package (ESM only, Node 20+, TypeScript 5.7+). Its users run it in production job systems, so every change is a change to someone else's backend.

## What we never compromise on

1. **The public API is a promise.** Semver follows the PR title (see [Pull requests](#pull-requests)). Anything reachable from a subpath export is public, including types. A breaking change needs `feat!:` and migration steps in the README.
2. **Types do the work.** Names correlate with payloads, producers see `z.input` and handlers see `z.output`, with no annotations or global augmentation required. A change that makes users write a type annotation is a regression.
3. **Jobs are never silently lost or duplicated by us.** Retries, continuations, retention and checkpoints are the reason the library exists. Users must still make external effects idempotent, but our own bookkeeping must be atomic.
4. **Optional means optional.** `@opentelemetry/api` is an optional peer: the package must load and work without it. Tracing and observer failures never change a job's outcome.

## A note on approach

Prefer the smallest model that makes the correct behavior unsurprising. Do not keep complexity because it already exists, and do not add machinery because it looks impressive. Measure twice, cut once, and YAGNI. These instructions are good defaults; the maintainer's wishes override them. If a rule here fights the task in front of you, say so and ask before breaking it.

## Glossary

- **you** means the agent reading this file. **we / maintainer** means the person you are working with. **user** means a developer using this package in their app.
- **queue** means one `defineQueue` definition. It maps to one Graphile _task identifier_, not to a Graphile _job queue_.
- **lane** means a Graphile job queue (`queueName`): jobs in one lane run one at a time. `serial` and `deriveJobOptions().queueName` select it.
- **kind** is regular (`inputSchema`), cron (`cron`, no schema) or cron-init (`cron` + `inputSchema` + `initFn`, which fans out through a generated `{name}_cron-init` task).
- **contract** means a definition without handlers, shared with producers. Handlers are then supplied as a registry to the worker.
- **wire input** means the producer's `z.input` value as stored in the jobs table. **payload** means the parsed `z.output` handed to `processFn`.
- **envelope** means the version-2 JSON wrapper around the wire input that carries trace context and step checkpoints ([`src/payload.ts`](src/payload.ts)).
- **step** means a memoized `ctx.step.run` result checkpointed into the envelope. **continuation** means `ctx.continue()`: finish this run and enqueue the same job again.
- **private table** means Graphile 0.17's `_private_jobs`. Only [`src/private-jobs.ts`](src/private-jobs.ts) touches it.

## Ways to hurt yourself

1. **Running the test suite against a real database.** The PostgreSQL tests create and drop schemas and use the default `graphile_worker` schema. Point `DATABASE_URL` only at a disposable database.
2. **Trusting a green run without `DATABASE_URL`.** The PostgreSQL suites use `describe.skipIf` and silently skip. If you changed SQL, enqueue, steps, continuation or retention, say whether the database tests actually ran.
3. **Changing the wire format casually.** The envelope is read by workers on other deploys. Changing it means a major release and a documented drain-and-upgrade path. There are deliberately no legacy readers.
4. **Leaking internals into the root export.** Low-level builders, SQL adapters and payload helpers belong in `/advanced`. The root exports constructors, contract helpers, public types and errors.
5. **Editing versions or `CHANGELOG.md` by hand.** Release Please owns both.

## Hit every surface

A change usually works on the path you tested and is missing elsewhere. Before calling a change done, walk this list and say which entries applied:

- **Entry points.** Enqueue behavior is reachable through `createJob`, `createJobs`, `jobs.*`, `ctx.createJob(s)`, `prepareJob(s)`, cron-init fan-out, the job client and the CLI. Fixing one is not fixing the feature.
- **Subpath exports.** `.`, `/client`, `/testing`, `/advanced` and `/cli` are separate entry points in [`package.json`](package.json). New public API needs an export in the right one.
- **Test harness.** `createTestHarness` shares parsing, child-enqueue validation and step replay with production. Route new behavior through the shared code instead of reimplementing it in the harness.
- **Registration styles.** Inline handlers and contract + handler registry are both supported. Check types and runtime for both.
- **Reverse states.** If you added a way in, add the way out and the way to see it: retry for fail, admin visibility for retention.
- **Packed consumers.** Type changes are checked in [`tests/consumers`](tests/consumers) under NodeNext and bundler resolution, with minimum and latest peers.
- **Docs.** Check whether the README is now inaccurate. Apply the [documentation rules](#documentation).

## Verifying

- Use the smallest proof that works: `bun test src/<file>.test.ts` for the behavior you touched, and `bun run typecheck`.
- Behavior changes ship with focused tests for that behavior. Test observable behavior, not implementation wiring.
- Database behavior needs the PostgreSQL tests. Run them with a disposable database: `DATABASE_URL=postgres://postgres:postgres@localhost:5432/bgw_test bun test <files>`.
- Run `bun run build && bun run test:consumers`, `bun run attw` and `bun run publint` only when you changed exports, packaging or public types. CI runs the full matrix.
- Never make a test wait on sleeps or polling for a milestone. Await the promise, event or database state that marks it.

## Pull requests

- Never open a PR unless the maintainer asks.
- The title is the release: `fix:`/`perf:` release a patch, `feat:` a minor, `feat!:` or a `BREAKING CHANGE:` footer a major. `chore:`, `ci:`, `docs:`, `refactor:` and `test:` do not release. Use plain language: `fix(worker): persist job completion before shutdown resolves`.
- Body: the problem in a sentence or two, how you fixed it, and how you verified it. End with the model and harness that did the work.
- One request is one PR.
- Releasing and failed-publish recovery: [docs/operations/releasing.md](docs/operations/releasing.md).

## Documentation

Most code changes do not need a documentation change beyond the README. Agents can read the code.

- `README.md` is the user guide. Update the relevant section when how to use something changes. Keep it in the user's voice: what it does, how to use it, and what is unintuitive. No implementation details.
- `docs/internals/` is for design decisions and their reasons, constraints that span files, and traps that are hard to discover from the source. Before adding a paragraph, ask what a maintainer would get wrong without it. If reading the code answers the question, leave it out.
- `docs/operations/` holds maintainer procedures such as releasing.
- Do not enumerate options or methods, narrate control flow, keep file catalogs, or append PR summaries. Types, tests and the changelog already record those.
- When a documented decision changes, rewrite or remove the affected text. Do not append a second account.
- Keep a local explanation in a nearby code comment. Link to source instead of copying it.
- Do not commit implementation plans, research notes or agent scratch files.

## How it works

`defineQueue` produces a definition; `defineQueues` checks a tuple of them. `createBetterWorker` turns them into a Graphile task list and cron items ([`src/worker.ts`](src/worker.ts)) and binds typed enqueue methods ([`src/create-job.ts`](src/create-job.ts)). Enqueue validates the wire input, derives options, wraps it in an envelope and inserts it through Graphile's public SQL functions. A worker unwraps the envelope, parses the payload, builds the job context and runs `processFn`. Steps, continuations and permanent-failure retention need the private table and commit atomically with the job's lock.

Design decisions and their reasons: [docs/internals/overview.md](docs/internals/overview.md).

## Where code lives

All source is flat in `src/`, with a colocated `*.test.ts` per module.

- **Defining:** `queue.ts` (`defineQueue`), `define-queues.ts`, `registry.ts`, `types.ts`, `job-options.ts`.
- **Enqueuing:** `create-job.ts`, `enqueue-sql.ts`, `job-client.ts`, `client.ts`.
- **Running:** `create-better-worker.ts`, `worker.ts`, `lifecycle.ts`, `steps.ts`, `payload.ts`, `private-jobs.ts`.
- **Observing:** `hooks.ts`, `observers.ts`, `otel.ts`, `default-logger.ts`, `completed-jobs-store.ts`, `admin.ts`.
- **Entry points:** `index.ts`, `advanced.ts`, `testing.ts`, `cli.ts`, `job-client.ts`.
- **Guards:** `validation.ts` (JSON wire values, integers), `schema-name.ts`, `errors.ts`.
- `scripts/` holds build and package verification. `tests/consumers/` holds the packed-consumer fixtures.

## Taste

- Inferred types over annotations. `any` is the enemy; a user-facing type that widens to `any` or `unknown` is a bug.
- Fail loudly at the boundary: reject bad names, options and wire values with a clear error rather than coercing them.
- Comments describe how a thing is used or why it is shaped that way, and move when the code moves. Do not annotate every line.
- Formatting and lint are Biome's (`bun run lint:write`): tabs, single quotes, no semicolons.
