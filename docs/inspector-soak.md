# Inspector batch soak test

`dev/inspector_batch_soak.go` is an opt-in load test for the bounded Inspector
APIs. It is excluded from normal `go test ./...` runs by a build constraint.

The test creates a cryptographically unique Redis prefix and queue, seeds a
scheduled backlog, and then runs these workers concurrently:

- a producer that keeps enqueueing scheduled tasks;
- `ProcessTaskBatch` calls that delete at most the configured limit;
- duplicate-name `GetQueueInfoBatch` reads that exercise ordering, pipelining,
  and the memory sample cache;
- periodic `SCRIPT FLUSH` calls that repeatedly force the safe `NOSCRIPT`
  fallback paths.

After the timed phase, the producer stops, the tool drains the scheduled state,
and it verifies exact enqueue/delete accounting and an empty final queue. Every
concurrent stats sample must also contain only scheduled tasks, with
`Size == Scheduled`. Deleting instead of moving tasks to pending keeps Redis
storage bounded during a long soak. Cleanup never calls `FLUSHDB`: it removes
the now-empty unique queue and scans only the generated `<prefix>:asynq:*`
namespace for leftovers, removing any such keys in pipelined `UNLINK` batches
and verifying that the namespace is empty.

## Safety warning

`SCRIPT FLUSH` affects every client of a Redis server, regardless of database or
key prefix. Run this target only against a disposable Redis instance. The only
Redis endpoint setting read by the tool is `ASYNQ_TEST_REDIS_ADDR`; it defaults
to `deps-redis:6379` and uses database 13.

## Run

The default duration is 30 seconds:

```sh
make soak-inspector-batch
```

A short repeatable validation run is:

```sh
ASYNQ_TEST_REDIS_ADDR=deps-redis:6379 \
ASYNQ_SOAK_DURATION=3s \
ASYNQ_SOAK_INITIAL_TASKS=500 \
make soak-inspector-batch
```

Configuration is supplied only through environment variables:

| Variable | Default | Constraint |
| --- | ---: | --- |
| `ASYNQ_TEST_REDIS_ADDR` | `deps-redis:6379` | Redis `host:port` |
| `ASYNQ_SOAK_DURATION` | `30s` | at least `1s` |
| `ASYNQ_SOAK_BATCH_SIZE` | `500` | `1..500` |
| `ASYNQ_SOAK_INITIAL_TASKS` | `1000` | `1..1000000` |
| `ASYNQ_SOAK_PRODUCER_INTERVAL` | `2ms` | positive duration |
| `ASYNQ_SOAK_STATS_INTERVAL` | `25ms` | positive and shorter than the run for coverage |
| `ASYNQ_SOAK_SCRIPT_FLUSH_INTERVAL` | `200ms` | positive and shorter than the run for coverage |
| `ASYNQ_SOAK_OPERATION_TIMEOUT` | `5s` | positive duration |
| `ASYNQ_SOAK_DRAIN_TIMEOUT` | `30s` | positive duration |

The command exits nonzero on any Redis/API error, a batch larger than its
limit, negative counters, stalled draining, insufficient stats/flush coverage,
or an enqueue/delete accounting mismatch. `SIGINT` and `SIGTERM` stop workers
and still run namespace-scoped cleanup.
