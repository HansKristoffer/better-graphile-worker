# Changelog

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
