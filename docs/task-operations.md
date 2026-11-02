# Task execution and operator APIs

These additions are developed locally on top of v1.0.1. Existing task payloads,
protobuf messages, task IDs and queue operations remain readable. The sibling
Asynqmon checkout uses these APIs through the parent `go.work`; no release has
been published by this change.

## Labels and indexed search

```go
info, err := client.Enqueue(
    asynq.NewTask("order:sync", payload),
    asynq.Label("order_id", "123"),
    asynq.Label("tenant", "north"),
    asynq.Retention(24*time.Hour),
)

page, err := inspector.SearchTasks(ctx, asynq.TaskSearchQuery{
    Labels: map[string]string{"order_id": "123", "tenant": "north"},
    Limit: 50,
})
```

All labels match exactly. A task supports at most 16 labels; keys are 1–64 ASCII
letters, digits, dots, underscores or hyphens, and values are at most 256 UTF-8
bytes. `Label` uses the reserved `asynq.label.` header namespace and works with
regular, delayed, unique, grouped and batch enqueue operations. Labels are
metadata supplied at enqueue time, rather than fields extracted from payloads.

The first label in sorted key order selects a Redis sorted-set index. Other
labels, optional type and state filters are checked against at most `Limit`
candidates per page. An empty page can still have a continuation cursor. Keep
the same query and pass the returned cursor until it is empty. Searches cover
at most 100 selected queues per request and inspect at most 500 candidate IDs.
Results are live; concurrent deletion/enqueue can change membership. Old tasks
without labels are not retroactively indexed.

Indexes are written in the same Lua script as enqueue, share the queue's Redis
Cluster hash tag, and are removed by task deletion, bulk deletion, successful
completion without retention, archive eviction and completed-task cleanup.
No unbounded scan of task payloads is used.

## Attempt history and progress

```go
func handle(ctx context.Context, task *asynq.Task) error {
    for done := int64(0); done <= 100; done++ {
        if err := asynq.ReportProgress(ctx, done, 100, "Importing orders"); err != nil {
            return err
        }
        // Perform cancellable work here.
    }
    return nil
}

execution, err := inspector.GetTaskExecution(ctx, info.Queue, info.ID)
// execution.Attempts, execution.Progress, execution.Labels, execution.Parent
```

Workers record each invocation's start, finish, worker identity, duration,
outcome and error code. Invocation numbers are separate from retry counters;
`IsFailure` can prevent the retry counter from advancing. The newest 50 attempts
are retained, with errors capped at 4096 bytes. A previous unfinished attempt
is marked interrupted when a new invocation begins. Abrupt process death cannot
provide an exact finish timestamp, so interrupted records can have no finish.

Progress requires the handler's context, `0 <= completed <= total <= 2^53`, a
positive total and a UTF-8 stage of at most 128 bytes. Writes are limited to once
per second in Redis, with final `completed == total` updates always allowed.
Within a stage, completed work cannot decrease. Progress resets on a new attempt.
A dequeue token fences both history and progress against stale workers; updates
after cancellation, completion or supersession cannot resurrect a task hash.

History/progress live inside the task hash and follow its lifecycle. Use
`Retention` to inspect successful tasks afterwards. They are removed with the
task; old executions are not reconstructed. Metadata recording errors are logged
without changing the existing job retry/ack behavior. ReportProgress returns its
error to the handler, which decides whether that affects the business job.

## Failure groups and replay

```go
return asynq.WithErrorCode("provider_unavailable", err)
// The original cause and errors.Is semantics are preserved.

groups, err := inspector.ListFailureGroups(ctx, "default", "", 50)
tasks, cursor, total, err := inspector.ListFailureGroupTasks(
    ctx, "default", "webhook:deliver", "provider_unavailable", "", 50,
)

results, err := inspector.ReplayTasks(ctx, "default", selectedIDs,
    asynq.ReplayOptions{
        RequestID: "operator-request-123",
        Payload: []byte(`{"corrected":true}`),
    }, 2*time.Second,
)
```

Errors implementing `ErrorCode() string` are recognized through wrapping.
Timeout, cancellation and lease expiration have built-in codes; other errors
use `unclassified`. Groups contain tasks whose latest recorded invocation failed
with the same type/code. Starting a new invocation removes the old membership.
Group reads are bounded and paginated. Old tasks are not classified retroactively.

Replay creates a new task in the same queue and keeps the original task and its
history intact. Copies preserve type, headers/labels, retry policy, timeout and
retention; deadline, uniqueness and aggregation are deliberately not inherited.
The copy's reserved `asynq.replay.queue` and `asynq.replay.id` headers identify its
source. `nil` replacement payload preserves the original bytes, while a non-nil
empty byte slice clears them. Only archived, retry or completed sources qualify.

A batch freezes 1–100 explicit IDs, returns per-source outcomes and spaces
eligibility times by an interval of 0–1 hour. This controls scheduling, rather
than guaranteeing a hard processing rate across workers. Delay cannot exceed
30 days. Retry the same selection with the same RequestID after an ambiguous
network failure: deterministic copy IDs prevent duplicates while the copies
still exist. Reusing the key with a different replacement payload or retry
setting is rejected. There is no all-or-nothing transaction across source IDs.

## Operator audit records

`Inspector.RecordAuditEvent` and `ListAuditEvents` provide the storage used by
Asynqmon. The default Redis list retains the newest 1000 records and expires
after 90 days without a write. Records contain operator identity, selected IDs,
action, status and confirmed counts, and never need payloads or credentials.
For durable external storage, implement Asynqmon's `AuditStore` interface.
