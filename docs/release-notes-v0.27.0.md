# Release notes: v0.27.0

`v0.27.0` establishes `github.com/pars-aria-labs/asynq` as the canonical module
path and packages the fork-specific reliability and administration work into a
reviewable release.

## Compatibility notice

The import-path change is source-breaking: consumers must update their Go
imports and `go.mod`. The exported package name remains `asynq`, Redis keys and
serialized task messages remain compatible, and established APIs have not been
removed. See the [migration guide](migrating-to-pars-aria-labs.md).

## Highlights

- Prefix-safe Inspector operations isolate multiple deployments sharing Redis.
- Atomic task batches are bounded to 500 source-state transitions; archive
  retention cleanup has a separate 500-entry eviction budget. Mutations disable
  ambiguous transport retries while preserving safe `NOSCRIPT` and cluster
  redirection handling.
- Multi-batch policy adds a fixed work budget, timeout, and inter-batch
  backpressure.
- `WithContext` makes legacy Inspector calls cancellable without changing their
  signatures.
- Queue snapshots are pipelined in groups of at most 100 queues, with an
  optional memory-sampling cache. Exact aggregation work remains proportional
  to the number of registered groups per queue.
- `QueryTasks` and `ProcessTaskIDs` provide a bounded, explicit workflow for
  type, error, and time-based administrative selection.
- A public observer and optional Prometheus collector report duration, outcome,
  confirmed work, and instrumented Redis client executions without
  high-cardinality queue labels.
- Unique-task locks and aggregation group metadata are cleaned correctly in
  bounded deletion paths.
- Redis INFO parsing accepts values containing colons, and numeric timezone
  offsets such as `+0330` are supported.
- Queue names beginning with `}` and Redis prefixes whose first hash-tag pair
  is empty (`{}`) are rejected, preventing multi-key `CROSSSLOT` failures.
- The companion Asynqmon integration reports batch progress and never retries
  an ambiguous mutation automatically.

## Verification

The release gate includes root, `x`, `tools`, and Asynqmon tests; race and vet
checks; Redis standalone and three-node cluster jobs; UI tests and production
build; an opt-in concurrent soak test with repeated `SCRIPT FLUSH`; and a manual
benchstat comparison workflow.

No automatic data migration is required for this release. Back up Redis and
exercise the upgrade in staging as you would for any queue-runtime deployment.
