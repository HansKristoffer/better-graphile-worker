# Changelog

## [2.6.2](https://github.com/HansKristoffer/better-graphile-worker/compare/v2.6.1...v2.6.2) (2026-10-21)


### Bug Fixes

* **worker:** reject promise when Graphile stops a runner on its own ([#29](https://github.com/HansKristoffer/better-graphile-worker/issues/29)) ([7488e76](https://github.com/HansKristoffer/better-graphile-worker/commit/7488e761a8d0c3cc47c0fb03a3f14c59e343d005))

## [2.6.1](https://github.com/HansKristoffer/better-graphile-worker/compare/v2.6.0...v2.6.1) (2026-10-21)


### Bug Fixes

* **crash-recovery:** a worker never releases its own jobs in its sweep ([#26](https://github.com/HansKristoffer/better-graphile-worker/issues/26)) ([ab19ed0](https://github.com/HansKristoffer/better-graphile-worker/commit/ab19ed0367e875d438e08e94b16690ac38523085))

## [2.6.0](https://github.com/HansKristoffer/better-graphile-worker/compare/v2.5.0...v2.6.0) (2026-10-21)


### Features

* typed transactional enqueue, runner prefetch and exported queue metric definitions ([#24](https://github.com/HansKristoffer/better-graphile-worker/issues/24)) ([9217c7d](https://github.com/HansKristoffer/better-graphile-worker/commit/9217c7ddf0ea718611adfabde3b12f32048c2ffb))

## [2.5.0](https://github.com/HansKristoffer/better-graphile-worker/compare/v2.4.0...v2.5.0) (2026-10-20)


### Features

* add worker pools, second look on notify, json-compat payloads, Prisma enqueue and queue metrics ([1c0cd78](https://github.com/HansKristoffer/better-graphile-worker/commit/1c0cd78fa0e17200326795d760e5cb1471050bef))

## [2.4.0](https://github.com/HansKristoffer/better-graphile-worker/compare/v2.3.0...v2.4.0) (2026-10-11)


### Features

* let handlers wait for child jobs with ctx.step.triggerAndWait ([#19](https://github.com/HansKristoffer/better-graphile-worker/issues/19)) ([1c5535d](https://github.com/HansKristoffer/better-graphile-worker/commit/1c5535d66e8a0ab7c12e90df763f2d708c431586))
* **worker:** run one maintenance loop on every worker and share job mechanics ([#20](https://github.com/HansKristoffer/better-graphile-worker/issues/20)) ([a736d27](https://github.com/HansKristoffer/better-graphile-worker/commit/a736d277b064203dc39ae7f7814d25f27ba7a08d))

## [2.3.0](https://github.com/HansKristoffer/better-graphile-worker/compare/v2.2.0...v2.3.0) (2026-10-11)


### Features

* add idempotency keys with a TTL to enqueue ([#16](https://github.com/HansKristoffer/better-graphile-worker/issues/16)) ([4bf5a61](https://github.com/HansKristoffer/better-graphile-worker/commit/4bf5a61f0eed37b873a21cdf85b5be7155b4aa38))
* add typed job progress readable by job id ([#17](https://github.com/HansKristoffer/better-graphile-worker/issues/17)) ([f045738](https://github.com/HansKristoffer/better-graphile-worker/commit/f045738a20edce9ceffd53eb2353ee4af21e1074))
* **worker:** add per-queue attempt timeouts ([#14](https://github.com/HansKristoffer/better-graphile-worker/issues/14)) ([45b915f](https://github.com/HansKristoffer/better-graphile-worker/commit/45b915f08dcb338fca6add46933c9ed7a8db789b))
* **worker:** let handlers schedule their next retry with RetryAtError ([#12](https://github.com/HansKristoffer/better-graphile-worker/issues/12)) ([377f27b](https://github.com/HansKristoffer/better-graphile-worker/commit/377f27b4699cb67b30cffecfd783af88f7dd0317))
* **worker:** record durable job outcomes and look jobs up by id ([#13](https://github.com/HansKristoffer/better-graphile-worker/issues/13)) ([d34dede](https://github.com/HansKristoffer/better-graphile-worker/commit/d34dede2572d431d06be273fae497ec00e7e14d8))
* **worker:** release jobs held by crashed workers via heartbeats ([#15](https://github.com/HansKristoffer/better-graphile-worker/issues/15)) ([f6ed0ae](https://github.com/HansKristoffer/better-graphile-worker/commit/f6ed0aef4b25730ce5b1f56aa2f880b3f3bd79ec))

## [2.2.0](https://github.com/HansKristoffer/better-graphile-worker/compare/v2.1.1...v2.2.0) (2026-10-10)


### Features

* **worker:** report failure messages and payloads to logs and hooks ([#10](https://github.com/HansKristoffer/better-graphile-worker/issues/10)) ([566f558](https://github.com/HansKristoffer/better-graphile-worker/commit/566f558314b3aaf8dce4d8f93df9eb3272826a82))

## [2.1.1](https://github.com/HansKristoffer/better-graphile-worker/compare/v2.1.0...v2.1.1) (2026-10-03)


### Bug Fixes

* **worker:** persist job completion before shutdown resolves ([#7](https://github.com/HansKristoffer/better-graphile-worker/issues/7)) ([39b116a](https://github.com/HansKristoffer/better-graphile-worker/commit/39b116a2126c67c5211a253a82a2f43847dbe114))

## [2.1.0](https://github.com/HansKristoffer/better-graphile-worker/compare/v2.0.0...v2.1.0) (2026-10-03)


### Features

* **queue:** add durable continuation and prepared enqueue APIs ([#5](https://github.com/HansKristoffer/better-graphile-worker/issues/5)) ([3a2bb43](https://github.com/HansKristoffer/better-graphile-worker/commit/3a2bb433a7151fbe83a9e88d972a875737bd6be4))

## [2.0.0](https://github.com/HansKristoffer/better-graphile-worker/compare/v1.0.1...v2.0.0) (2026-10-01)


### ⚠ BREAKING CHANGES

* Replace createQueue and legacy queue/config types with defineQueue and the new typed contracts. Import advanced helpers from /advanced, configure tracing per instance, and migrate jobs to version-2 payload envelopes before upgrading.

### Features

* replace createQueue with typed queue contracts ([4589a26](https://github.com/HansKristoffer/better-graphile-worker/commit/4589a26b6e6fbcda2376e4fb944abe5e537a54e5))
