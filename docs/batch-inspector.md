# Bounded Inspector operations

The context-aware Inspector APIs below are intended for dashboards and other
administrative clients that need bounded task mutations, cancellable reads,
and fewer client executions when inspecting many queues.

## Mutating tasks in bounded batches

`ProcessTaskBatch` transitions at most 500 source tasks atomically:

```go
processed, remaining, err := inspector.ProcessTaskBatch(
    ctx,
    "critical", // queue
    "scheduled", // state
    "run",       // action
    "",          // group; required only for aggregating tasks
    500,
)
```

The batch size must be between 1 and 500. Supported state/action pairs are:

| State | Delete | Run | Archive |
| --- | --- | --- | --- |
| `pending` | yes | no | yes |
| `scheduled` | yes | yes | yes |
| `retry` | yes | yes | yes |
| `archived` | yes | yes | no |
| `completed` | yes | no | no |
| `aggregating` | yes | yes | yes |

For `aggregating`, pass the group name. Active tasks are deliberately excluded
because mutating a task that a worker owns would violate its lease.

Each call is atomic, but a sequence of calls is not. `remaining` is the live
source-state cardinality after the batch, so producers and workers can change
it between requests. A client that drains a state should capture a total budget
from its first successful response and stop after that much work. This prevents
concurrent producers from keeping an administrative request alive forever:

```go
const maxBatch = 500

total, budget := 0, -1
for budget < 0 || total < budget {
    limit := maxBatch
    if budget >= 0 && budget-total < limit {
        limit = budget - total
    }
    n, remaining, err := inspector.ProcessTaskBatch(
        ctx, queue, state, action, group, limit,
    )
    total += n // n may be nonzero together with an error.
    if err != nil {
        return fmt.Errorf("bulk operation stopped after %d tasks: %w", total, err)
    }
    if budget < 0 {
        budget = total + remaining
    }
    if remaining == 0 || n == 0 {
        break
    }
}
```

Do not automatically retry a failed request after a transport timeout: Redis
may have committed the mutation even when the reply did not reach the client.
Report the confirmed `processed` count and let an operator decide whether to
continue. The Inspector disables go-redis transport retries for each mutating
Lua command so one API call cannot silently execute a second batch. A safe
`NOSCRIPT` fallback and Redis Cluster `MOVED`/`ASK` redirects are still handled.

Archiving also performs independently bounded housekeeping: one call mutates
at most 500 source tasks and removes at most 500 expired or excess archive
entries. Evicted archive entries have their task hashes and unique locks
deleted. If a large stale backlog exists, later archive calls continue cleanup
without making a single Redis script unbounded.

The older `DeleteAll*`, `RunAll*`, and `ArchiveAll*` methods remain available.
They now execute bounded internal batches and cap their work at the source
cardinality observed when the call starts. New HTTP services should prefer
`ProcessTaskBatch` so cancellation, progress, and partial results remain visible.

## Reading queue information in a pipeline

`GetQueueInfoBatch` returns queue information in input order and preserves
duplicates:

```go
infos, err := inspector.GetQueueInfoBatch(
    ctx,
    []string{"critical", "default", "critical"},
    15*time.Second,
)
```

Only the approximate `MemoryUsage` sample is cached. Queue sizes, pause state,
and processed/failed counters are read from Redis on every call. A zero TTL
forces a fresh memory sample; a negative TTL is invalid. A missing queue or any
Redis error fails the whole call, so callers never receive an ambiguous partial
slice.

The implementation pipelines at most 100 queues per flush. Every Lua invocation
uses keys for one queue, which keeps routing compatible with Redis Cluster; a
cold or flushed script cache is recovered with an `EVAL` retry on the affected
node. Exact `Aggregating` counts and uncached memory estimates enumerate the
registered aggregation groups of each queue, so work inside those scripts is
still proportional to group cardinality. Use a context deadline and remove
obsolete dynamic groups when that cardinality can grow substantially.

## Applying timeout and backpressure to a complete operation

`ProcessTaskBatches` builds a safe, reusable loop on top of the atomic
single-batch API. `MaxTasks` is a hard cap when it is positive. When it is zero,
the method freezes its work budget from the source cardinality observed after
the first batch, so new producers cannot keep the call alive indefinitely.

```go
result, err := inspector.ProcessTaskBatches(
    ctx,
    "critical",
    "retry",
    "run",
    "", // group is required only for aggregating tasks
    asynq.TaskBatchPolicy{
        BatchSize:       250,
        MaxTasks:        2_000,
        Timeout:         20 * time.Second,
        InterBatchDelay: 50 * time.Millisecond,
    },
)
if err != nil {
    // This is the confirmed lower bound; the failed batch may be ambiguous.
    return fmt.Errorf("stopped after %d tasks: %w", result.Processed, err)
}
log.Printf("processed=%d remaining=%d batches=%d",
    result.Processed, result.Remaining, result.Batches)
```

The delay is the backpressure control: it is inserted only between batches and
is interrupted immediately by context cancellation. `Timeout` covers Redis
work and these delays. Individual mutations remain atomic; the complete series
is deliberately not one transaction.

## Making every legacy Inspector call context-aware

`WithContext` returns a lightweight view on the same Redis connection. All
legacy methods invoked through that view use the supplied context:

```go
ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
defer cancel()

view := inspector.WithContext(ctx)
tasks, err := view.ListRetryTasks("critical", asynq.PageSize(100))
if err != nil {
    return err
}
```

Do not close the derived view; its `Close` method intentionally returns a
shared-connection error. Continue to close the original Inspector. Methods
that already take a context, including `ProcessTaskBatch` and
`GetQueueInfoBatch`, use their direct argument.

## Filtering a bounded task page

`QueryTasks` supports exact task types, a substring in the last error, and an
explicit time field. Filtering happens after reading one bounded page. This is
important: the method never hides an unbounded queue scan behind a convenient
API.

```go
query := asynq.TaskQuery{
    Queue:    "critical",
    State:    asynq.TaskStateRetry,
    Page:     1,
    PageSize: 500,
    Filter: asynq.TaskFilter{
        Types:         []string{"email:deliver", "email:receipt"},
        ErrorContains: "timeout",
        Time: &asynq.TaskTimeRange{
            Field:  asynq.TaskTimeLastFailure,
            From:   time.Now().Add(-24 * time.Hour),
            Before: time.Now(),
        },
    },
}
page, err := inspector.QueryTasks(ctx, query)
if err != nil {
    return err
}
log.Printf("matched %d of %d scanned tasks", len(page.Tasks), page.Scanned)
```

Offset pagination over a live queue is not a point-in-time snapshot. Producers,
workers, schedulers, and your own mutations can shift page boundaries, so a
multi-page walk can repeat or miss IDs. When exhaustive selection matters,
quiesce the selected state while collecting pages. Collect every ID before the
first mutation and de-duplicate it, then mutate chunks of at most 500.

```go
const pageSize = 500
var ids []string
seen := make(map[string]struct{})
for pageNumber := 1; ; pageNumber++ {
    query.Page = pageNumber
    query.PageSize = pageSize
    result, err := inspector.QueryTasks(ctx, query)
    if err != nil {
        return err
    }
    for _, task := range result.Tasks {
        if _, duplicate := seen[task.ID]; !duplicate {
            seen[task.ID] = struct{}{}
            ids = append(ids, task.ID)
        }
    }
    if result.Scanned < pageSize {
        break
    }
}

for start := 0; start < len(ids); start += asynq.MaxInspectorBatchSize {
    end := min(start+asynq.MaxInspectorBatchSize, len(ids))
    processed, err := inspector.ProcessTaskIDs(
        ctx, "critical", "archive", ids[start:end],
    )
    if err != nil {
        return fmt.Errorf("archived %d IDs from this chunk: %w", processed, err)
    }
}
```

Each ID transition is atomic, while the complete ID slice is not. Processing
stops at the first error. The returned count is a confirmed lower bound: it
counts successful replies, but a transition whose reply was lost may still
have committed. Inspect current state before deciding whether to continue;
never blindly retry the failed suffix.

## Telemetry and Prometheus

Attach an `InspectorOperationObserver` to receive duration, processed count,
partial failure, and instrumented Redis execution data. Observations omit
queue, group, task ID, and raw-error labels by design. The optional collector in
`x/metrics` turns them into low-cardinality Prometheus metrics:

```go
inspectorMetrics := metrics.NewInspectorMetricsCollector()
inspector.SetOperationObserver(inspectorMetrics)

registry := prometheus.NewRegistry()
registry.MustRegister(
    metrics.NewQueueMetricsCollector(inspector),
    inspectorMetrics,
)

http.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
log.Fatal(http.ListenAndServe(":9876", nil))
```

The collector exports:

- `asynq_inspector_operations_total`, with `success`, `error`, or `partial`;
- `asynq_inspector_items_processed_total`;
- `asynq_inspector_redis_round_trips_total`;
- `asynq_inspector_operation_duration_seconds`.

The `redis_round_trips_total` name is retained as a compact operational signal,
but it counts instrumented client executions and pipeline flush attempts—not
physical network RTTs. Retries and Redis Cluster fan-out performed inside
go-redis may use additional network exchanges.

The observer runs synchronously and may be invoked concurrently. A custom
observer must be concurrency-safe, should return quickly, and must not call
back into the same Inspector. Its panic is recovered so monitoring cannot
change the Redis result. If you use the bundled exporter, start it with the
same Redis address, database, credentials, and prefix as the application. For
example:

```sh
(cd tools && go run ./metrics_exporter \
  -redis-addr=127.0.0.1:6379 \
  -redis-db=2 \
  -redis-prefix=billing-prod \
  -port=9876)
```

Configure Prometheus to scrape port `9876`; a Prometheus query endpoint is not
a push destination. The bundled exporter currently supports a standalone
Redis connection. For Redis Cluster, embed the collectors in an application
that constructs the Inspector with `RedisClusterClientOpt`.
