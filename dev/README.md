# Testing with the sibling asynqmon checkout

The Inspector optimization and bounded batch-operation phases are complete.
The current preview is `v1.0.0-beta.1`. See `docs/handoff/README.fa.md` for the
release status and full validation record.

Keep `asynq` and `asynqmon` in the same parent directory. With Go 1.25 or
newer on PATH, run this from the asynq repository:

```sh
make test-asynqmon
```

The explicit `dev/asynqmon.work` workspace selects the local asynq module and
its `x` module (including the Prometheus exporter). Asynqmon imports and pins
the canonical `github.com/pars-aria-labs/asynq` modules at `v0.27.1`; the
version-specific workspace replacements select the beta core and local `x`
source for simultaneous development without changing either module file. A
`GOWORK=off` build verifies the released dependency graph
without local overrides.

To run the monitor with these local changes:

```sh
asynq_workspace="$(realpath dev/asynqmon.work)"
cd ../asynqmon
GOWORK="$asynq_workspace" go run ./cmd/asynqmon
```

The Redis address and prefix must match those used by the application workers.
Use the monitor's CLI flags or configuration for your environment.

For an HTTP integration check against a disposable Redis instance:

```sh
ASYNQ_TEST_REDIS_ADDR=127.0.0.1:6379 make smoke-asynqmon
```

This calls the monitor's actual HTTP handlers, verifies statistics, bounded
500-task mutations with continuation, and legacy queue mutations. It uses a
unique prefix in database 13 and cleans up its test tasks.

## Inspector changes

- Queue inspection and mutations consistently use the configured Redis prefix,
  including statistics, groups, active/scheduled/retry/archived/completed tasks,
  pause/resume, and queue removal. Default key names and public APIs are preserved.
- Server, worker, and scheduler metadata reads are pipelined. Missing records,
  wrong Redis types, and malformed payloads retain the existing skip behavior.
- Queue removal avoids duplicate task reads; checking whether a queue is empty
  uses list/sorted-set cardinalities instead of reading all task IDs.
- Scheduler time options accept numeric timezone offsets, including Tehran.
- Redis INFO values containing colons, including IPv6 addresses, are preserved.
- `ProcessTaskBatch` performs bounded archive, run, and delete operations over
  pending, scheduled, retry, archived, completed, and aggregating tasks. Active
  tasks are deliberately excluded. It reports the number processed and whether
  matching work remains.
- `GetQueueInfoBatch` pipelines at most 100 queues per flush, preserves input
  order and duplicates, and supports a short-lived memory cache. Exact
  aggregation work remains proportional to registered groups per queue.
- `WithContext` makes established Inspector methods cancellable;
  `TaskBatchPolicy` adds timeout and backpressure to a bounded series.
- `QueryTasks` filters bounded pages, `ProcessTaskIDs` mutates explicit ID
  lists, and the optional Prometheus collector reports low-cardinality
  operation telemetry. Offset pages are best-effort rather than a point-in-time
  snapshot while producers or workers are active.

Run backend tests only against a disposable Redis instance: the existing tests
flush databases 14 and 15. The regression benchmark can use a separate test DB:

```sh
go test ./...
go vet ./...
go test ./internal/rdb -run '^$' -bench '^BenchmarkInspectorListServers$' -benchmem -redis_db=13
ASYNQ_TEST_REDIS_ADDR=127.0.0.1:6379 make soak-inspector-batch
```

The soak target intentionally has no Redis-address default: set
`ASYNQ_TEST_REDIS_ADDR` to a disposable local instance, or to the disposable
Redis service address provided by CI. It runs `SCRIPT FLUSH`, which affects all
databases on that server. See the [soak-test safety guide](../docs/inspector-soak.md)
before running it.

## Validation (2026-09-06)

- `go test ./...` and `go vet ./...`: passed against the disposable standalone
  Redis endpoint.
- `x` and `tools` tests and vet checks: passed.
- Race-enabled `internal/rdb` tests: passed with CGO enabled.
- Sibling asynqmon Go and UI tests, production UI build, Redis integration test,
  and HTTP smoke test: passed.
- Redis Cluster was not available locally; the CI workflow now includes a
  three-node cluster job.

Three local benchmark runs with 100 server records reduced the median
`ListServers` duration from 10.94 ms to 0.555 ms (about 20x). Allocations fell
from 1,614 to 1,332 per call, while bytes allocated rose from approximately
74.9 KB to 76.4 KB for pipeline command storage. Single-server timings were
similar (approximately 0.23 ms). These measurements are environment-specific;
network latency and deployment topology affect the improvement.

For 100 queues, the batch current-stats benchmark was approximately 7.8x
faster than calling the single-queue path in a loop in this environment
(3.58 ms versus 27.97 ms). Treat these numbers as directional rather than a
deployment guarantee.
