# Changelog

## [2.0.0](https://github.com/HansKristoffer/better-graphile-worker/compare/v1.0.1...v2.0.0) (2026-10-01)


### ⚠ BREAKING CHANGES

* Replace createQueue and legacy queue/config types with defineQueue and the new typed contracts. Import advanced helpers from /advanced, configure tracing per instance, and migrate jobs to version-2 payload envelopes before upgrading.

### Features

* replace createQueue with typed queue contracts ([4589a26](https://github.com/HansKristoffer/better-graphile-worker/commit/4589a26b6e6fbcda2376e4fb944abe5e537a54e5))
