package asynq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pars-aria-labs/asynq/internal/base"
)

func TestTaskLabelsSearchPaginationAndReplay(t *testing.T) {
	rc := setup(t)
	client := newClientFromRedisClient(rc, "features")
	inspector := newInspectorFromRedisClient(rc, "features")
	ctx := context.Background()
	for _, queue := range []string{"orders", "webhooks"} {
		for n := 0; n < 4; n++ {
			_, err := client.Enqueue(NewTask("order:sync", []byte(`{"version":1}`)), Queue(queue), TaskID(fmt.Sprintf("job-%d", n)), Label("order_id", "123"), Label("tenant", "north"), Retention(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	other := newClientFromRedisClient(rc, "other")
	if _, err := other.Enqueue(NewTask("order:sync", nil), Queue("orders"), Label("order_id", "123")); err != nil {
		t.Fatal(err)
	}
	query := TaskSearchQuery{Labels: map[string]string{"order_id": "123", "tenant": "north"}, Limit: 3}
	seen := make(map[string]bool)
	for page := 0; page < 10; page++ {
		result, err := inspector.SearchTasks(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		if result.Scanned > 3 {
			t.Fatal("unbounded candidate scan")
		}
		for _, task := range result.Tasks {
			key := task.Queue + "/" + task.ID
			if seen[key] {
				t.Fatalf("duplicate task across pages: %s", key)
			}
			seen[key] = true
		}
		if result.Cursor == "" {
			break
		}
		query.Cursor = result.Cursor
	}
	if len(seen) != 8 {
		t.Fatalf("matched %d tasks, want 8", len(seen))
	}
	first, err := inspector.SearchTasks(ctx, TaskSearchQuery{Labels: map[string]string{"order_id": "123"}, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := inspector.SearchTasks(ctx, TaskSearchQuery{Labels: map[string]string{"order_id": "456"}, Cursor: first.Cursor}); err == nil {
		t.Fatal("cursor from a different query accepted")
	}
	noMatch, err := inspector.SearchTasks(ctx, TaskSearchQuery{Labels: map[string]string{"order_id": "123", "tenant": "south"}, Limit: 2})
	if err != nil || len(noMatch.Tasks) != 0 || noMatch.Scanned != 2 || noMatch.Cursor == "" {
		t.Fatalf("empty filtered page must retain its cursor: %+v %v", noMatch, err)
	}
	if err := inspector.ArchiveTask("orders", "job-0"); err != nil {
		t.Fatal(err)
	}
	before, err := inspector.GetTaskExecution(ctx, "orders", "job-0")
	if err != nil {
		t.Fatal(err)
	}
	options := ReplayOptions{Payload: []byte(`{"version":2}`), Delay: time.Minute, RequestID: "request-123"}
	copy, err := inspector.ReplayTask(ctx, "orders", "job-0", options)
	if err != nil {
		t.Fatal(err)
	}
	if copy.ID == "job-0" || copy.State != TaskStateScheduled || string(copy.Payload) != `{"version":2}` {
		t.Fatalf("bad replay: %+v", copy)
	}
	again, err := inspector.ReplayTask(ctx, "orders", "job-0", options)
	if err != nil || again.ID != copy.ID {
		t.Fatalf("replay retry made a duplicate: %+v %v", again, err)
	}
	meta, err := inspector.GetTaskExecution(ctx, "orders", copy.ID)
	if err != nil || meta.Parent == nil || meta.Parent.ID != "job-0" || meta.Labels["order_id"] != "123" {
		t.Fatalf("lost replay provenance: %+v %v", meta, err)
	}
	after, _ := inspector.GetTaskExecution(ctx, "orders", "job-0")
	beforeJSON, _ := json.Marshal(before)
	afterJSON, _ := json.Marshal(after)
	if string(beforeJSON) != string(afterJSON) {
		t.Fatal("replay modified source execution")
	}
	original, _ := inspector.GetTaskInfo("orders", "job-0")
	if string(original.Payload) != `{"version":1}` || original.State != TaskStateArchived {
		t.Fatal("replay modified original task")
	}
	if _, err := inspector.ReplayTask(ctx, "orders", "job-1", options); err == nil {
		t.Fatal("pending source accepted")
	}
	if _, err := inspector.ReplayTasks(ctx, "orders", []string{"job-0", "job-0"}, options, time.Second); err == nil {
		t.Fatal("duplicate sources accepted")
	}
	if err := inspector.DeleteTask("orders", "job-2"); err != nil {
		t.Fatal(err)
	}
	result, err := inspector.SearchTasks(ctx, TaskSearchQuery{Labels: map[string]string{"order_id": "123"}, Queues: []string{"orders"}, Limit: 100})
	if err != nil || len(result.Tasks) != 4 {
		t.Fatalf("deleted task remained indexed: %d %v", len(result.Tasks), err)
	}
	changed := options
	changed.Payload = []byte(`{"version":99}`)
	if _, err := inspector.ReplayTask(ctx, "orders", "job-0", changed); err == nil {
		t.Fatal("idempotency key reused with a different payload")
	}
	if err := inspector.DeleteTask("orders", "job-0"); err != nil {
		t.Fatal(err)
	}
	confirmed, err := inspector.ReplayTask(ctx, "orders", "job-0", options)
	if err != nil || confirmed.ID != copy.ID {
		t.Fatalf("could not confirm replay after source deletion: %+v %v", confirmed, err)
	}
}

func TestBatchLabelsAndValidation(t *testing.T) {
	rc := setup(t)
	client := NewClientFromRedisClient(rc)
	inspector := NewInspectorFromRedisClient(rc)
	results := client.BatchEnqueueContext(context.Background(), []*Task{NewTask("batch", nil, Label("order_id", "old")), NewTask("batch", nil)}, Label("order_id", "123"), Header("application", "demo"))
	for _, result := range results {
		if result.Err != nil || result.TaskInfo.Headers[base.LabelHeaderPrefix+"order_id"] != "123" || result.TaskInfo.Headers["application"] != "demo" {
			t.Fatalf("batch options not merged: %+v", result)
		}
	}
	found, err := inspector.SearchTasks(context.Background(), TaskSearchQuery{Labels: map[string]string{"order_id": "123"}})
	if err != nil || len(found.Tasks) != 2 {
		t.Fatalf("batch labels not indexed: %+v %v", found, err)
	}
	for _, option := range []Option{Label("", "value"), Label("bad key", "value"), Label("id", string(make([]byte, 257)))} {
		if _, err := client.Enqueue(NewTask("invalid", nil), option); err == nil {
			t.Fatal("invalid label accepted")
		}
	}
	if err := ReportProgress(context.Background(), 1, 2, "work"); err == nil {
		t.Fatal("progress accepted outside a handler")
	}
	if !errors.Is(WithErrorCode("bad_input", SkipRetry), SkipRetry) {
		t.Fatal("classified error lost its cause")
	}
}

func TestWorkerRecordsAttemptHistoryAndProgress(t *testing.T) {
	setup(t)
	conn := getRedisConnOpt(t)
	client := NewClient(conn)
	defer client.Close()
	inspector := NewInspector(conn)
	defer inspector.Close()
	worker := NewServer(conn, Config{Concurrency: 2, TaskCheckInterval: 10 * time.Millisecond, DelayedTaskCheckInterval: 20 * time.Millisecond, ShutdownTimeout: time.Second, LogLevel: FatalLevel, RetryDelayFunc: func(int, error, *Task) time.Duration { return 20 * time.Millisecond }})
	reports := make(chan error, 8)
	if err := worker.Start(HandlerFunc(func(ctx context.Context, task *Task) error {
		// Headers exposed to application code cannot rewrite the stored label.
		task.Headers()[base.LabelHeaderPrefix+"order_id"] = "mutated-by-handler"
		reports <- ReportProgress(ctx, 1, 10, "import")
		retry, _ := GetRetryCount(ctx)
		if retry < 2 {
			return WithErrorCode("upstream_unavailable", fmt.Errorf("simulated 503 on attempt %d", retry+1))
		}
		reports <- ReportProgress(ctx, 10, 10, "import")
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	defer worker.Shutdown()
	info, err := client.Enqueue(NewTask("import", nil), Retention(time.Hour), MaxRetry(3), Label("order_id", "worker-123"))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		current, err := inspector.GetTaskInfo(info.Queue, info.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.State == TaskStateCompleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("task did not complete: %+v", current)
		}
		time.Sleep(20 * time.Millisecond)
	}
	execution, err := inspector.GetTaskExecution(context.Background(), info.Queue, info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(execution.Attempts) != 3 || execution.Attempts[0].ErrorCode != "upstream_unavailable" || execution.Attempts[2].Status != "succeeded" || execution.Attempts[2].Number != 3 {
		t.Fatalf("bad history: %+v", execution)
	}
	if execution.Progress == nil || execution.Progress.Completed != 10 || execution.Progress.AttemptID != execution.Attempts[2].AttemptID {
		t.Fatalf("bad final progress: %+v", execution.Progress)
	}
	if execution.Labels["order_id"] != "worker-123" {
		t.Fatal("handler mutated indexed enqueue metadata")
	}
	groups, err := inspector.ListFailureGroups(context.Background(), info.Queue, "", 50)
	if err != nil || len(groups.Groups) != 0 {
		t.Fatalf("successful task remained in failure index: %+v %v", groups, err)
	}
	for len(reports) > 0 {
		if err := <-reports; err != nil {
			t.Fatal(err)
		}
	}
}
