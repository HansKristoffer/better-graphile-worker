# Queue contract migration

Replace createQueue with defineQueue while keeping processFn and initFn alongside schemas and options. Inline contexts enqueue through typed queue references; worker/client methods retain correlated queue-name signatures. Shared handler-free contracts and registry-typed handlers remain available when producers need separate imports. Support async schemas, readonly definitions and initializer/batch inputs, and JSON-preserving steps with explicit codecs. Reject unsafe queue-reference/input unions, validate manually constructed inline handlers against their schemas, and preserve typed captured jobs and literal context names in the harness.

Remove the previous queue/config types, QueueNames alias, global setOtelApi setter, legacy root exports for advanced helpers, WorkerUtils producer-binding fallback, and implicit createJobsApi registry lookup. Import internals from /advanced and configure tracing per instance.

Accept only version-2 payload envelopes with W3C traceparent metadata. Drain old jobs before upgrading producers and workers together; flat/version-1 payloads are rejected. Step checkpoints update only the current envelope, with no legacy conversion path. Unsupported wire values and shared batch keys are rejected; void steps replay as undefined.

Retain permanent failures in PostgreSQL by default. Make payload inspection opt-in and rename bounded completed history to recentCompleted alongside recentFailed. Serialize utility initialization and lifecycle transitions, preserve shutdown tracking after timeout, isolate observer failures, and reset retry attempts correctly.

Expose queue input schemas through admin definitions and support JobSpan.addEvent for tracing.

Use ordinary ESM emission with source/declaration maps. Validate PostgreSQL behavior and packed declarations with minimum/latest peers and supported Node versions; include regression coverage. See README for the major-version migration.

See [README migration instructions](../README.md#types-and-major-version-migration) before upgrading.
