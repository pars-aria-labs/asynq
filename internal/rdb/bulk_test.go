package rdb

import (
	"context"
	stderrors "errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pars-aria-labs/asynq/internal/base"
	rdberrors "github.com/pars-aria-labs/asynq/internal/errors"
	h "github.com/pars-aria-labs/asynq/internal/testutil"
	"github.com/redis/go-redis/v9"
)

type testRedisHook struct {
	process         func(context.Context, redis.Cmder, redis.ProcessHook) error
	processPipeline func(context.Context, []redis.Cmder, redis.ProcessPipelineHook) error
}

func (h *testRedisHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *testRedisHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	if h.process == nil {
		return next
	}
	return func(ctx context.Context, cmd redis.Cmder) error {
		return h.process(ctx, cmd, next)
	}
}

func (h *testRedisHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	if h.processPipeline == nil {
		return next
	}
	return func(ctx context.Context, cmds []redis.Cmder) error {
		return h.processPipeline(ctx, cmds, next)
	}
}

func isEvalShaFor(cmd redis.Cmder, script *redis.Script) bool {
	args := cmd.Args()
	return cmd.Name() == "evalsha" && len(args) > 1 && fmt.Sprint(args[1]) == script.Hash()
}

func seedBulkZSet(t testing.TB, r *RDB, queue, key string, count int) []string {
	t.Helper()
	ctx := context.Background()
	if err := r.client.SAdd(ctx, base.AllQueuesKey(r.prefix), queue).Err(); err != nil {
		t.Fatal(err)
	}
	ids := make([]string, count)
	members := make([]redis.Z, count)
	pipe := r.client.Pipeline()
	for i := range count {
		ids[i] = fmt.Sprintf("task-%04d", i)
		members[i] = redis.Z{Member: ids[i], Score: float64(i)}
		pipe.HSet(ctx, base.TaskKeyWithPrefix(r.prefix, queue, ids[i]), "state", "scheduled")
	}
	pipe.ZAdd(ctx, key, members...)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	return ids
}

func TestTaskBatchLimits(t *testing.T) {
	for _, state := range []string{"pending", "scheduled", "retry", "archived", "completed", "aggregating"} {
		for _, action := range []string{"delete", "run", "archive"} {
			if action == "run" && (state == "pending" || state == "completed") || action == "archive" && (state == "archived" || state == "completed") {
				continue
			}
			t.Run(state+"/"+action, func(t *testing.T) {
				r := setup(t)
				defer r.Close()
				r.prefix = "batch-test"
				ctx := context.Background()
				const count = BulkBatchSize + 3
				var sampleID string
				for j := 0; j < count; j++ {
					msg := h.NewTaskMessage("batch", nil)
					if j == 0 {
						sampleID = msg.ID
					}
					var err error
					switch state {
					case "pending":
						err = r.Enqueue(ctx, msg)
					case "scheduled":
						err = r.Schedule(ctx, msg, time.Now().Add(time.Hour))
					case "aggregating":
						err = r.AddToGroup(ctx, msg, "group")
					default:
						if err = r.Enqueue(ctx, msg); err != nil {
							t.Fatal(err)
						}
						if _, _, err = r.Dequeue("default"); err != nil {
							t.Fatal(err)
						}
						switch state {
						case "retry":
							err = r.Retry(ctx, msg, time.Now().Add(time.Hour), "failed", true)
						case "archived":
							err = r.Archive(ctx, msg, "failed")
						case "completed":
							msg.Retention = 3600
							err = r.MarkAsComplete(ctx, msg)
						}
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				n, left, err := r.TaskBatch(ctx, "default", state, action, "group", BulkBatchSize)
				if err != nil || n != BulkBatchSize || left != 3 {
					t.Fatalf("first batch = %d, %d, %v", n, left, err)
				}
				if state == "aggregating" {
					groups, err := r.GroupStats("default")
					if err != nil || len(groups) != 1 || groups[0].Size != 3 {
						t.Fatalf("group disappeared before last batch: %v %v", groups, err)
					}
				}
				n, left, err = r.TaskBatch(ctx, "default", state, action, "group", BulkBatchSize)
				if err != nil || n != 3 || left != 0 {
					t.Fatalf("last batch = %d, %d, %v", n, left, err)
				}
				if state == "aggregating" {
					groups, err := r.GroupStats("default")
					if err != nil || len(groups) != 0 {
						t.Fatalf("group not cleaned: %v %v", groups, err)
					}
				}
				switch action {
				case "delete":
					if got := r.client.Exists(ctx, base.TaskKeyWithPrefix(r.prefix, "default", sampleID)).Val(); got != 0 {
						t.Fatalf("deleted task hash still exists: %s", sampleID)
					}
				case "run":
					if got := r.client.LLen(ctx, base.PendingKeyWithPrefix(r.prefix, "default")).Val(); got != count {
						t.Fatalf("pending count = %d, want %d", got, count)
					}
					if got := r.client.HGet(ctx, base.TaskKeyWithPrefix(r.prefix, "default", sampleID), "state").Val(); got != "pending" {
						t.Fatalf("task state = %q, want pending", got)
					}
				case "archive":
					if got := r.client.ZCard(ctx, base.ArchivedKeyWithPrefix(r.prefix, "default")).Val(); got != count {
						t.Fatalf("archived count = %d, want %d", got, count)
					}
					if got := r.client.HGet(ctx, base.TaskKeyWithPrefix(r.prefix, "default", sampleID), "state").Val(); got != "archived" {
						t.Fatalf("task state = %q, want archived", got)
					}
				}
			})
		}
	}
}

func TestArchiveTaskBatchBoundsHousekeepingAndDeletesEvictedData(t *testing.T) {
	r := setup(t)
	defer r.Close()
	r.prefix = "bounded-cleanup-prefix"
	ctx := context.Background()
	const queue = "bounded-archive-cleanup"
	source := h.NewTaskMessageWithQueue("source", nil, queue)
	if err := r.Enqueue(ctx, source); err != nil {
		t.Fatal(err)
	}

	archiveKey := base.ArchivedKeyWithPrefix(r.prefix, queue)
	taskPrefix := base.TaskKeyPrefixWithPrefix(r.prefix, queue)
	oldest := time.Now().AddDate(0, 0, -archivedExpirationInDays-1).Unix()
	pipe := r.client.Pipeline()
	for j := 0; j < BulkBatchSize+1; j++ {
		id := fmt.Sprintf("old-%04d", j)
		uniqueKey := ""
		if j == 0 {
			uniqueKey = base.UniqueKeyWithPrefix(r.prefix, queue, "old", []byte("payload"))
			pipe.Set(ctx, uniqueKey, id, time.Hour)
		}
		pipe.ZAdd(ctx, archiveKey, redis.Z{Member: id, Score: float64(oldest + int64(j))})
		pipe.HSet(ctx, taskPrefix+id, "state", "archived", "unique_key", uniqueKey)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}

	processed, remaining, err := r.TaskBatch(ctx, queue, "pending", "archive", "", 1)
	if err != nil || processed != 1 || remaining != 0 {
		t.Fatalf("TaskBatch() = (%d, %d, %v), want (1, 0, nil)", processed, remaining, err)
	}
	if got := r.client.ZCard(ctx, archiveKey).Val(); got != 2 {
		t.Fatalf("archive size = %d, want 2 after a cleanup capped at %d", got, BulkBatchSize)
	}
	if got := r.client.Exists(ctx, taskPrefix+"old-0000", taskPrefix+"old-0499").Val(); got != 0 {
		t.Fatalf("%d evicted task hashes still exist", got)
	}
	if got := r.client.Exists(ctx, taskPrefix+"old-0500", taskPrefix+source.ID).Val(); got != 2 {
		t.Fatalf("retained task hashes = %d, want 2", got)
	}
	uniqueKey := base.UniqueKeyWithPrefix(r.prefix, queue, "old", []byte("payload"))
	if got := r.client.Exists(ctx, uniqueKey).Val(); got != 0 {
		t.Fatal("evicted task's uniqueness lock still exists")
	}

	// An archive request with an empty source is a true no-op; housekeeping is
	// never a hidden side effect of a zero-processed response.
	processed, remaining, err = r.TaskBatch(ctx, queue, "pending", "archive", "", 1)
	if err != nil || processed != 0 || remaining != 0 {
		t.Fatalf("empty TaskBatch() = (%d, %d, %v), want (0, 0, nil)", processed, remaining, err)
	}
	if got := r.client.Exists(ctx, taskPrefix+"old-0500").Val(); got != 1 {
		t.Fatal("empty archive batch unexpectedly ran housekeeping")
	}
}

func TestArchiveCleanupToleratesMalformedMetadata(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	const queue = "archive-malformed-metadata"
	source := h.NewTaskMessageWithQueue("source", nil, queue)
	if err := r.Enqueue(ctx, source); err != nil {
		t.Fatal(err)
	}

	archiveKey := base.ArchivedKeyWithPrefix(r.prefix, queue)
	taskPrefix := base.TaskKeyPrefixWithPrefix(r.prefix, queue)
	old := time.Now().AddDate(0, 0, -archivedExpirationInDays-1).Unix()
	reassignedLock := base.UniqueKeyWithPrefix(r.prefix, queue, "reassigned", nil)
	wrongTypeLock := base.UniqueKeyWithPrefix(r.prefix, queue, "wrong-type", nil)
	pipe := r.client.Pipeline()
	pipe.ZAdd(ctx, archiveKey,
		redis.Z{Member: "wrong-task-type", Score: float64(old)},
		redis.Z{Member: "wrong-lock-type", Score: float64(old + 1)},
		redis.Z{Member: "reassigned-lock", Score: float64(old + 2)},
	)
	pipe.Set(ctx, taskPrefix+"wrong-task-type", "not-a-hash", 0)
	pipe.HSet(ctx, taskPrefix+"wrong-lock-type", "state", "archived", "unique_key", wrongTypeLock)
	pipe.LPush(ctx, wrongTypeLock, "not-a-string")
	pipe.HSet(ctx, taskPrefix+"reassigned-lock", "state", "archived", "unique_key", reassignedLock)
	pipe.Set(ctx, reassignedLock, "new-owner", time.Hour)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}

	processed, remaining, err := r.TaskBatch(ctx, queue, "pending", "archive", "", 1)
	if err != nil || processed != 1 || remaining != 0 {
		t.Fatalf("TaskBatch() = (%d, %d, %v), want (1, 0, nil)", processed, remaining, err)
	}
	if got := r.client.ZCard(ctx, archiveKey).Val(); got != 1 {
		t.Fatalf("archive size = %d, want only the newly archived task", got)
	}
	if got := r.client.Exists(ctx,
		taskPrefix+"wrong-task-type",
		taskPrefix+"wrong-lock-type",
		taskPrefix+"reassigned-lock",
	).Val(); got != 0 {
		t.Fatalf("%d malformed archived task keys still exist", got)
	}
	if got := r.client.Get(ctx, reassignedLock).Val(); got != "new-owner" {
		t.Fatalf("reassigned uniqueness lock owner = %q, want new-owner", got)
	}
	if got := r.client.Type(ctx, wrongTypeLock).Val(); got != "list" {
		t.Fatalf("wrong-type uniqueness key type = %q, want list", got)
	}
}

func TestDeletePathsTolerateMalformedUniqueMetadata(t *testing.T) {
	t.Run("explicit task", func(t *testing.T) {
		r := setup(t)
		defer r.Close()
		ctx := context.Background()
		msg := h.NewTaskMessage("explicit-delete", nil)
		if err := r.Enqueue(ctx, msg); err != nil {
			t.Fatal(err)
		}
		taskKey := base.TaskKey(msg.Queue, msg.ID)
		wrongTypeLock := base.UniqueKey(msg.Queue, "explicit-wrong-type", nil)
		if err := r.client.HSet(ctx, taskKey, "unique_key", wrongTypeLock).Err(); err != nil {
			t.Fatal(err)
		}
		if err := r.client.LPush(ctx, wrongTypeLock, "not-a-string").Err(); err != nil {
			t.Fatal(err)
		}

		if err := r.DeleteTask(msg.Queue, msg.ID); err != nil {
			t.Fatalf("DeleteTask() error = %v", err)
		}
		if got := r.client.Exists(ctx, taskKey).Val(); got != 0 {
			t.Fatal("deleted task hash still exists")
		}
		if got := r.client.LLen(ctx, base.PendingKey(msg.Queue)).Val(); got != 0 {
			t.Fatalf("pending length = %d, want 0", got)
		}
		if got := r.client.Type(ctx, wrongTypeLock).Val(); got != "list" {
			t.Fatalf("wrong-type uniqueness key type = %q, want list", got)
		}
	})

	t.Run("bounded batch", func(t *testing.T) {
		r := setup(t)
		defer r.Close()
		ctx := context.Background()
		msgs := []*base.TaskMessage{
			h.NewTaskMessage("wrong-task-type", nil),
			h.NewTaskMessage("wrong-lock-type", nil),
			h.NewTaskMessage("reassigned-lock", nil),
		}
		for _, msg := range msgs {
			if err := r.Enqueue(ctx, msg); err != nil {
				t.Fatal(err)
			}
		}

		wrongTaskKey := base.TaskKey(msgs[0].Queue, msgs[0].ID)
		wrongLockKey := base.UniqueKey(msgs[1].Queue, "batch-wrong-type", nil)
		reassignedLock := base.UniqueKey(msgs[2].Queue, "batch-reassigned", nil)
		pipe := r.client.Pipeline()
		pipe.Set(ctx, wrongTaskKey, "not-a-hash", 0)
		pipe.HSet(ctx, base.TaskKey(msgs[1].Queue, msgs[1].ID), "unique_key", wrongLockKey)
		pipe.LPush(ctx, wrongLockKey, "not-a-string")
		pipe.HSet(ctx, base.TaskKey(msgs[2].Queue, msgs[2].ID), "unique_key", reassignedLock)
		pipe.Set(ctx, reassignedLock, "new-owner", time.Hour)
		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatal(err)
		}

		processed, remaining, err := r.TaskBatch(ctx, msgs[0].Queue, "pending", "delete", "", len(msgs))
		if err != nil || processed != int64(len(msgs)) || remaining != 0 {
			t.Fatalf("TaskBatch() = (%d, %d, %v), want (%d, 0, nil)", processed, remaining, err, len(msgs))
		}
		for _, msg := range msgs {
			if got := r.client.Exists(ctx, base.TaskKey(msg.Queue, msg.ID)).Val(); got != 0 {
				t.Errorf("deleted task hash %q still exists", msg.ID)
			}
		}
		if got := r.client.Type(ctx, wrongLockKey).Val(); got != "list" {
			t.Errorf("wrong-type uniqueness key type = %q, want list", got)
		}
		if got := r.client.Get(ctx, reassignedLock).Val(); got != "new-owner" {
			t.Errorf("reassigned uniqueness lock owner = %q, want new-owner", got)
		}
	})
}

func TestTaskBatchValidation(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	if err := r.Enqueue(ctx, h.NewTaskMessage("seed", nil)); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name              string
		queue, state      string
		action, group     string
		limit             int
		wantQueueNotFound bool
	}{
		{name: "zero limit", queue: "default", state: "pending", action: "delete", limit: 0},
		{name: "oversized limit", queue: "default", state: "pending", action: "delete", limit: BulkBatchSize + 1},
		{name: "empty queue", state: "pending", action: "delete", limit: 1},
		{name: "unsupported state", queue: "default", state: "active", action: "delete", limit: 1},
		{name: "missing group", queue: "default", state: "aggregating", action: "delete", limit: 1},
		{name: "unsupported action", queue: "default", state: "pending", action: "retry", limit: 1},
		{name: "run pending", queue: "default", state: "pending", action: "run", limit: 1},
		{name: "archive completed", queue: "default", state: "completed", action: "archive", limit: 1},
		{name: "missing queue", queue: "missing", state: "pending", action: "delete", limit: 1, wantQueueNotFound: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := r.TaskBatch(ctx, tc.queue, tc.state, tc.action, tc.group, tc.limit)
			if err == nil {
				t.Fatal("TaskBatch succeeded")
			}
			if tc.wantQueueNotFound && !rdberrors.IsQueueNotFound(err) {
				t.Fatalf("TaskBatch error = %v, want QueueNotFoundError", err)
			}
		})
	}
}

func TestBulkLegacySpansBatches(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	const count = 2*BulkBatchSize + 3
	items := make([]base.BatchEnqueueItem, count)
	for j := range items {
		items[j] = base.BatchEnqueueItem{Msg: h.NewTaskMessage("bulk", nil), ProcessAt: time.Now().Add(time.Hour)}
	}
	if n, err := r.BatchEnqueue(ctx, items); err != nil || n != count {
		t.Fatalf("seed: %d %v", n, err)
	}
	if n, err := r.RunAllScheduledTasks("default"); err != nil || n != count {
		t.Fatalf("run: %d %v", n, err)
	}
	if n, err := r.ArchiveAllPendingTasks("default"); err != nil || n != count {
		t.Fatalf("archive: %d %v", n, err)
	}
	if n, err := r.DeleteAllArchivedTasks("default"); err != nil || n != count {
		t.Fatalf("delete: %d %v", n, err)
	}
}

func TestBulkLegacyReturnsPartialCount(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	key := base.ScheduledKeyWithPrefix(r.prefix, "default")
	seedBulkZSet(t, r, "default", key, BulkBatchSize+3)
	if err := deleteAllCmd.Load(ctx, r.client).Err(); err != nil {
		t.Fatal(err)
	}
	injected := stderrors.New("injected second-batch failure")
	matched := 0
	r.client.AddHook(&testRedisHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		if isEvalShaFor(cmd, deleteAllCmd) {
			matched++
			if matched == 2 {
				return injected
			}
		}
		return next(ctx, cmd)
	}})

	processed, err := r.DeleteAllScheduledTasks("default")
	if !stderrors.Is(err, injected) {
		t.Fatalf("DeleteAllScheduledTasks error = %v, want injected error", err)
	}
	if processed != BulkBatchSize {
		t.Fatalf("DeleteAllScheduledTasks processed = %d, want %d", processed, BulkBatchSize)
	}
	if remaining := r.client.ZCard(ctx, key).Val(); remaining != 3 {
		t.Fatalf("remaining = %d, want 3", remaining)
	}
}

func TestBulkLegacyUsesInitialCardinalityBudget(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	key := base.ScheduledKeyWithPrefix(r.prefix, "default")
	const initial = BulkBatchSize + 1
	seedBulkZSet(t, r, "default", key, initial)
	if err := deleteAllCmd.Load(ctx, r.client).Err(); err != nil {
		t.Fatal(err)
	}
	matched := 0
	r.client.AddHook(&testRedisHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		err := next(ctx, cmd)
		if err == nil && isEvalShaFor(cmd, deleteAllCmd) {
			matched++
			if matched == 1 {
				members := make([]redis.Z, 5)
				for i := range members {
					members[i] = redis.Z{Member: fmt.Sprintf("concurrent-%d", i), Score: -1}
				}
				return r.client.ZAdd(context.Background(), key, members...).Err()
			}
		}
		return err
	}})

	processed, err := r.DeleteAllScheduledTasks("default")
	if err != nil {
		t.Fatal(err)
	}
	if processed != initial {
		t.Fatalf("processed = %d, want initial cardinality %d", processed, initial)
	}
	if remaining := r.client.ZCard(ctx, key).Val(); remaining != 5 {
		t.Fatalf("remaining = %d, want concurrent additions 5", remaining)
	}
}

func TestMutationScriptsDisableTransportRetry(t *testing.T) {
	for _, mode := range []string{"legacy", "task-batch"} {
		for _, cache := range []string{"warm", "cold"} {
			t.Run(mode+"/"+cache, func(t *testing.T) {
				r := setup(t)
				defer r.Close()
				ctx := context.Background()
				key := base.ScheduledKeyWithPrefix(r.prefix, "default")
				seedBulkZSet(t, r, "default", key, 1)
				if cache == "warm" {
					if err := deleteAllCmd.Load(ctx, r.client).Err(); err != nil {
						t.Fatal(err)
					}
				} else if err := r.client.ScriptFlush(ctx).Err(); err != nil {
					t.Fatal(err)
				}

				var retryPolicies, cloneRetryPolicies []bool
				r.client.AddHook(&testRedisHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
					if cmd.Name() == "evalsha" || cmd.Name() == "eval" {
						retryPolicies = append(retryPolicies, cmd.NoRetry())
						cloneRetryPolicies = append(cloneRetryPolicies, cmd.Clone().NoRetry())
					}
					return next(ctx, cmd)
				}})

				var err error
				if mode == "legacy" {
					_, err = r.DeleteAllScheduledTasks("default")
				} else {
					_, _, err = r.TaskBatch(ctx, "default", "scheduled", "delete", "", 1)
				}
				if err != nil {
					t.Fatal(err)
				}
				wantCommands := 1
				if cache == "cold" {
					wantCommands = 2 // EVALSHA followed by the safe NOSCRIPT EVAL fallback.
				}
				if len(retryPolicies) != wantCommands {
					t.Fatalf("saw %d mutation commands, want %d", len(retryPolicies), wantCommands)
				}
				for j, noRetry := range retryPolicies {
					if !noRetry {
						t.Fatalf("mutation command %d permits transport retry", j)
					}
					if !cloneRetryPolicies[j] {
						t.Fatalf("clone of mutation command %d permits transport retry", j)
					}
				}
			})
		}
	}
}

func TestSingleTaskMutationScriptsDisableTransportRetry(t *testing.T) {
	tests := []struct {
		name   string
		script *redis.Script
		seed   func(context.Context, *RDB, *base.TaskMessage) error
		mutate func(*RDB, string) error
	}{
		{
			name:   "delete",
			script: deleteTaskCmd,
			seed:   func(ctx context.Context, r *RDB, message *base.TaskMessage) error { return r.Enqueue(ctx, message) },
			mutate: func(r *RDB, id string) error { return r.DeleteTask("default", id) },
		},
		{
			name:   "run",
			script: runTaskCmd,
			seed: func(ctx context.Context, r *RDB, message *base.TaskMessage) error {
				return r.Schedule(ctx, message, time.Now().Add(time.Hour))
			},
			mutate: func(r *RDB, id string) error { return r.RunTask("default", id) },
		},
		{
			name:   "archive",
			script: archiveTaskCmd,
			seed:   func(ctx context.Context, r *RDB, message *base.TaskMessage) error { return r.Enqueue(ctx, message) },
			mutate: func(r *RDB, id string) error { return r.ArchiveTask("default", id) },
		},
	}
	for _, tc := range tests {
		for _, cache := range []string{"warm", "cold"} {
			t.Run(tc.name+"/"+cache, func(t *testing.T) {
				r := setup(t)
				defer r.Close()
				ctx := context.Background()
				message := h.NewTaskMessage("single-mutation", nil)
				if err := tc.seed(ctx, r, message); err != nil {
					t.Fatal(err)
				}
				if cache == "warm" {
					if err := tc.script.Load(ctx, r.client).Err(); err != nil {
						t.Fatal(err)
					}
				} else if err := r.client.ScriptFlush(ctx).Err(); err != nil {
					t.Fatal(err)
				}

				var retryPolicies, cloneRetryPolicies []bool
				r.client.AddHook(&testRedisHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
					if cmd.Name() == "evalsha" || cmd.Name() == "eval" {
						retryPolicies = append(retryPolicies, cmd.NoRetry())
						cloneRetryPolicies = append(cloneRetryPolicies, cmd.Clone().NoRetry())
					}
					return next(ctx, cmd)
				}})
				if err := tc.mutate(r, message.ID); err != nil {
					t.Fatal(err)
				}
				wantCommands := 1
				if cache == "cold" {
					wantCommands = 2
				}
				if len(retryPolicies) != wantCommands {
					t.Fatalf("saw %d mutation commands, want %d", len(retryPolicies), wantCommands)
				}
				for j, noRetry := range retryPolicies {
					if !noRetry || !cloneRetryPolicies[j] {
						t.Fatalf("mutation command %d or its clone permits transport retry", j)
					}
				}
			})
		}
	}
}

func TestBulkEmptyAggregatingGroupCleanup(t *testing.T) {
	for _, mode := range []string{"legacy", "task-batch"} {
		for _, action := range []string{"delete", "run", "archive"} {
			t.Run(mode+"/"+action, func(t *testing.T) {
				r := setup(t)
				defer r.Close()
				ctx := context.Background()
				const queue, group = "default", "empty-group"
				if err := r.client.SAdd(ctx, base.AllQueuesKey(r.prefix), queue).Err(); err != nil {
					t.Fatal(err)
				}
				if err := r.client.SAdd(ctx, base.AllGroupsWithPrefix(r.prefix, queue), group).Err(); err != nil {
					t.Fatal(err)
				}
				var (
					processed int64
					remaining int64
					err       error
				)
				if mode == "task-batch" {
					processed, remaining, err = r.TaskBatch(ctx, queue, "aggregating", action, group, 1)
				} else {
					switch action {
					case "delete":
						processed, err = r.DeleteAllAggregatingTasks(queue, group)
					case "run":
						processed, err = r.RunAllAggregatingTasks(queue, group)
					case "archive":
						processed, err = r.ArchiveAllAggregatingTasks(queue, group)
					}
				}
				if err != nil || processed != 0 || remaining != 0 {
					t.Fatalf("processed=%d remaining=%d err=%v", processed, remaining, err)
				}
				if r.client.SIsMember(ctx, base.AllGroupsWithPrefix(r.prefix, queue), group).Val() {
					t.Fatal("empty group was not removed from groups set")
				}
			})
		}
	}
}

func TestBulkDeleteClearsUniqueLocks(t *testing.T) {
	for _, mode := range []string{"legacy", "task-batch"} {
		for _, state := range []string{"pending", "aggregating"} {
			t.Run(mode+"/"+state, func(t *testing.T) {
				r := setup(t)
				defer r.Close()
				r.prefix = "batch-unique"
				ctx := context.Background()
				msg := h.NewTaskMessage("unique-batch", nil)
				uniqueKey := base.UniqueKeyWithPrefix(r.prefix, msg.Queue, msg.Type, msg.Payload)
				msg.UniqueKey = uniqueKey
				var err error
				if state == "pending" {
					err = r.EnqueueUnique(ctx, msg, time.Hour)
				} else {
					err = r.AddToGroupUnique(ctx, msg, "group", time.Hour)
				}
				if err != nil {
					t.Fatal(err)
				}
				taskKey := base.TaskKeyWithPrefix(r.prefix, msg.Queue, msg.ID)
				if got := r.client.HGet(ctx, taskKey, "unique_key").Val(); got != uniqueKey {
					t.Fatalf("task unique_key = %q, want %q", got, uniqueKey)
				}

				var processed, remaining int64
				if mode == "task-batch" {
					processed, remaining, err = r.TaskBatch(ctx, msg.Queue, state, "delete", "group", 1)
				} else if state == "pending" {
					processed, err = r.DeleteAllPendingTasks(msg.Queue)
				} else {
					processed, err = r.DeleteAllAggregatingTasks(msg.Queue, "group")
				}
				if err != nil || processed != 1 || remaining != 0 {
					t.Fatalf("delete result = (%d, %d, %v), want (1, 0, nil)", processed, remaining, err)
				}
				if got := r.client.Exists(ctx, uniqueKey).Val(); got != 0 {
					t.Fatalf("uniqueness lock still exists: %s", uniqueKey)
				}
			})
		}
	}
}

func TestTaskBatchReturnsProcessedCountWhenRemainingFails(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	key := base.ScheduledKeyWithPrefix(r.prefix, "default")
	seedBulkZSet(t, r, "default", key, 1)
	injected := stderrors.New("injected remaining-count failure")
	failed := false
	r.client.AddHook(&testRedisHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		if !failed && cmd.Name() == "zcard" && len(cmd.Args()) > 1 && fmt.Sprint(cmd.Args()[1]) == key {
			failed = true
			return injected
		}
		return next(ctx, cmd)
	}})

	processed, remaining, err := r.TaskBatch(ctx, "default", "scheduled", "delete", "", 1)
	if !stderrors.Is(err, injected) {
		t.Fatalf("TaskBatch error = %v, want injected error", err)
	}
	if processed != 1 || remaining != 0 {
		t.Fatalf("processed=%d remaining=%d, want 1, 0", processed, remaining)
	}
	if r.client.Exists(ctx, base.TaskKeyWithPrefix(r.prefix, "default", "task-0000")).Val() != 0 {
		t.Fatal("task mutation did not complete before remaining-count failure")
	}
}

func TestCurrentStatsBatch(t *testing.T) {
	r := setup(t)
	defer r.Close()
	r.prefix = "batch-stats"
	ctx := context.Background()
	queues := make([]string, 103)
	for j := range queues {
		queues[j] = fmt.Sprintf("queue-%d", j)
		if err := r.Enqueue(ctx, h.NewTaskMessageWithQueue("test", nil, queues[j])); err != nil {
			t.Fatal(err)
		}
	}
	// A cold cache exercises the pipelined EVALSHA fallback.
	if err := r.client.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	stats, err := r.CurrentStatsBatch(ctx, queues, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != len(queues) {
		t.Fatal("wrong result count")
	}
	for j, s := range stats {
		if s.Queue != queues[j] || s.Pending != 1 || s.Size != 1 || s.MemoryUsage <= 0 {
			t.Fatalf("unexpected stats: %+v", s)
		}
	}
	stats, err = r.CurrentStatsBatch(ctx, []string{queues[0], queues[0]}, nil)
	if err != nil || len(stats) != 2 || stats[0].Queue != queues[0] || stats[1].Queue != queues[0] || stats[0].MemoryUsage <= 0 || stats[1].MemoryUsage <= 0 {
		t.Fatalf("duplicate uncached stats: %v %v", stats, err)
	}
	cached := map[string]int64{queues[0]: 123}
	stats, err = r.CurrentStatsBatch(ctx, []string{queues[0], queues[0]}, cached)
	if err != nil || len(stats) != 2 || stats[0].Queue != queues[0] || stats[1].Queue != queues[0] || stats[0].MemoryUsage != 123 || stats[1].MemoryUsage != 123 {
		t.Fatalf("cached stats: %v %v", stats, err)
	}
	if _, err = r.CurrentStatsBatch(ctx, []string{"missing"}, nil); !rdberrors.IsQueueNotFound(err) {
		t.Fatalf("missing queue error = %v, want QueueNotFoundError", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = r.CurrentStatsBatch(canceled, queues, nil); !stderrors.Is(err, context.Canceled) {
		t.Fatalf("CurrentStatsBatch canceled error = %v, want context.Canceled", err)
	}
	if _, _, err = r.TaskBatch(canceled, queues[0], "pending", "delete", "", 1); !stderrors.Is(err, context.Canceled) {
		t.Fatalf("TaskBatch canceled error = %v, want context.Canceled", err)
	}
}

func TestCurrentStatsBatchQueueDeletedDuringPipeline(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	const queue = "deleted-during-pipeline"
	if err := r.Enqueue(ctx, h.NewTaskMessageWithQueue("test", nil, queue)); err != nil {
		t.Fatal(err)
	}
	var (
		once      sync.Once
		deleteErr error
	)
	r.client.AddHook(&testRedisHook{processPipeline: func(ctx context.Context, cmds []redis.Cmder, next redis.ProcessPipelineHook) error {
		once.Do(func() {
			deleteErr = r.client.SRem(context.Background(), base.AllQueuesKey(r.prefix), queue).Err()
		})
		if deleteErr != nil {
			return deleteErr
		}
		return next(ctx, cmds)
	}})

	_, err := r.CurrentStatsBatch(ctx, []string{queue}, nil)
	if !rdberrors.IsQueueNotFound(err) {
		t.Fatalf("CurrentStatsBatch error = %v, want QueueNotFoundError", err)
	}
}

func TestCurrentStatsBatchReturnsPipelineError(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	const queue = "pipeline-error"
	if err := r.Enqueue(ctx, h.NewTaskMessageWithQueue("test", nil, queue)); err != nil {
		t.Fatal(err)
	}
	injected := stderrors.New("injected pipeline failure")
	r.client.AddHook(&testRedisHook{processPipeline: func(context.Context, []redis.Cmder, redis.ProcessPipelineHook) error {
		return injected
	}})

	_, err := r.CurrentStatsBatch(ctx, []string{queue}, nil)
	if !stderrors.Is(err, injected) {
		t.Fatalf("CurrentStatsBatch error = %v, want injected error", err)
	}
}

func BenchmarkCurrentStatsBatch(b *testing.B) {
	for _, queueCount := range []int{1, 100} {
		b.Run(fmt.Sprintf("queues=%d", queueCount), func(b *testing.B) {
			r := setup(b)
			defer r.Close()
			ctx := context.Background()
			queues := make([]string, queueCount)
			for i := range queues {
				queues[i] = fmt.Sprintf("benchmark-%d", i)
				if err := r.Enqueue(ctx, h.NewTaskMessageWithQueue("benchmark", nil, queues[i])); err != nil {
					b.Fatal(err)
				}
			}
			if _, err := r.CurrentStatsBatch(ctx, queues, nil); err != nil {
				b.Fatal(err)
			}

			b.Run("loop", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					for _, queue := range queues {
						if _, err := r.CurrentStats(queue); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
			b.Run("batch", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					stats, err := r.CurrentStatsBatch(ctx, queues, nil)
					if err != nil || len(stats) != len(queues) {
						b.Fatalf("CurrentStatsBatch returned %d results, err=%v", len(stats), err)
					}
				}
			})
		})
	}
}
