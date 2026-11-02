package rdb

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pars-aria-labs/asynq/internal/base"
	"github.com/pars-aria-labs/asynq/internal/timeutil"
)

func TestAttemptFencingHistoryBoundsAndCleanup(t *testing.T) {
	r := setup(t)
	defer r.Close()
	r.prefix = "feature"
	ctx := context.Background()
	msg := &base.TaskMessage{ID: "job", Queue: "orders", Type: "import", Headers: map[string]string{base.LabelHeaderPrefix + "order_id": "123"}}
	if err := r.Enqueue(ctx, msg); err != nil {
		t.Fatal(err)
	}
	var previous string
	for n := 1; n <= 55; n++ {
		active, _, err := r.Dequeue("orders")
		if err != nil {
			t.Fatal(err)
		}
		token := active.AttemptToken
		if token == "" || token == previous {
			t.Fatal("dequeue token not unique")
		}
		entry := []byte(fmt.Sprintf(`{"attempt_id":%q,"status":"running"}`, token))
		if err := r.BeginTaskAttempt(ctx, "orders", "job", token, entry); err != nil {
			t.Fatal(err)
		}
		if previous != "" {
			if err := r.ReportTaskProgress(ctx, "orders", "job", previous, []byte(`{"completed":1,"total":2,"stage":"old"}`)); err == nil {
				t.Fatal("superseded worker progress accepted")
			}
			if err := r.FinishTaskAttempt(ctx, "orders", "job", previous, "import", "stale", []byte(`{"status":"failed"}`)); err != nil {
				t.Fatal(err)
			}
		}
		first := []byte(fmt.Sprintf(`{"attempt_id":%q,"completed":1,"total":10,"stage":"work"}`, token))
		if err := r.ReportTaskProgress(ctx, "orders", "job", token, first); err != nil {
			t.Fatal(err)
		}
		if err := r.ReportTaskProgress(ctx, "orders", "job", token, []byte(`{"completed":0,"total":10,"stage":"work"}`)); err == nil {
			t.Fatal("decreasing progress accepted")
		}
		if err := r.ReportTaskProgress(ctx, "orders", "job", token, []byte(`{"completed":5,"total":10,"stage":"work"}`)); err != nil {
			t.Fatal(err)
		}
		fields, _ := r.TaskExecution(ctx, "orders", "job")
		if fields["_progress"] != string(first) {
			t.Fatal("progress throttle failed")
		}
		final := []byte(fmt.Sprintf(`{"attempt_id":%q,"completed":10,"total":10,"stage":"work"}`, token))
		if err := r.ReportTaskProgress(ctx, "orders", "job", token, final); err != nil {
			t.Fatal(err)
		}
		finished := []byte(fmt.Sprintf(`{"attempt_id":%q,"status":"failed","error_code":"upstream"}`, token))
		if err := r.FinishTaskAttempt(ctx, "orders", "job", token, "import", "upstream", finished); err != nil {
			t.Fatal(err)
		}
		if err := r.ReportTaskProgress(ctx, "orders", "job", token, final); err == nil {
			t.Fatal("progress accepted after execution finished")
		}
		previous = token
		if n < 55 {
			if err := r.Requeue(ctx, active); err != nil {
				t.Fatal(err)
			}
		}
	}
	fields, err := r.TaskExecution(ctx, "orders", "job")
	if err != nil {
		t.Fatal(err)
	}
	var history []struct {
		Number int    `json:"number"`
		Code   string `json:"error_code"`
	}
	if err := json.Unmarshal([]byte(fields["_history"]), &history); err != nil {
		t.Fatal(err)
	}
	if len(history) != 50 || history[0].Number != 6 || history[49].Number != 55 || history[49].Code != "upstream" {
		t.Fatalf("history bound or fencing failed: %+v", history)
	}
	ids, count, err := r.FailureGroupIDs(ctx, "orders", "import", "upstream", "", 50)
	if err != nil || count != 1 || len(ids) != 1 {
		t.Fatalf("bad failure group: %v %d %v", ids, count, err)
	}
	if err := r.Archive(ctx, msg, "failed"); err != nil {
		t.Fatal(err)
	}
	if err := r.DeleteTask("orders", "job"); err != nil {
		t.Fatal(err)
	}
	ids, err = r.SearchLabelIDs(ctx, "orders", "order_id", "123", "", 50)
	if err != nil || len(ids) != 0 {
		t.Fatalf("label index leaked: %v %v", ids, err)
	}
	groups, err := r.FailureGroupMembers(ctx, "orders", "", 50)
	if err != nil || len(groups) != 0 {
		t.Fatalf("failure group registry leaked: %v %v", groups, err)
	}
	fields, _ = r.TaskExecution(ctx, "orders", "job")
	if len(fields) != 0 {
		t.Fatal("task metadata survived deletion")
	}
}

func TestLabelsRemovedBySuccessAndCompletionRetention(t *testing.T) {
	r := setup(t)
	defer r.Close()
	r.prefix = "retention"
	ctx := context.Background()
	for n := 0; n < 2; n++ {
		msg := &base.TaskMessage{ID: fmt.Sprintf("job-%d", n), Queue: "orders", Type: "sync", Headers: map[string]string{base.LabelHeaderPrefix + "order_id": "123"}, Retention: int64(n)}
		if err := r.Enqueue(ctx, msg); err != nil {
			t.Fatal(err)
		}
		active, _, err := r.Dequeue("orders")
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			err = r.Done(ctx, active)
		} else {
			err = r.MarkAsComplete(ctx, active)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	ids, err := r.SearchLabelIDs(ctx, "orders", "order_id", "123", "", 10)
	if err != nil || len(ids) != 1 {
		t.Fatalf("success cleanup: %v %v", ids, err)
	}
	r.SetClock(timeutil.NewSimulatedClock(time.Now().Add(time.Hour)))
	if err := r.DeleteExpiredCompletedTasks("orders", 100); err != nil {
		t.Fatal(err)
	}
	ids, err = r.SearchLabelIDs(ctx, "orders", "order_id", "123", "", 10)
	if err != nil || len(ids) != 0 {
		t.Fatalf("completion retention leaked labels: %v %v", ids, err)
	}
}

func TestLabelsIndexedForEveryEnqueuePath(t *testing.T) {
	r := setup(t)
	defer r.Close()
	r.prefix = "feature"
	ctx := context.Background()
	for n := 0; n < 8; n++ {
		msg := &base.TaskMessage{ID: fmt.Sprintf("job-%d", n), Queue: "orders", Type: fmt.Sprintf("type-%d", n), Headers: map[string]string{base.LabelHeaderPrefix + "order_id": "123"}}
		var err error
		switch n {
		case 0:
			err = r.Enqueue(ctx, msg)
		case 1:
			err = r.EnqueueUnique(ctx, msg, time.Minute)
		case 2:
			err = r.Schedule(ctx, msg, time.Now().Add(time.Hour))
		case 3:
			err = r.ScheduleUnique(ctx, msg, time.Now().Add(time.Hour), time.Hour)
		case 4:
			err = r.AddToGroup(ctx, msg, "digest")
		case 5:
			err = r.AddToGroupUnique(ctx, msg, "digest", time.Minute)
		case 6:
			_, err = r.BatchEnqueue(ctx, []base.BatchEnqueueItem{{Msg: msg}})
		case 7:
			_, err = r.BatchEnqueue(ctx, []base.BatchEnqueueItem{{Msg: msg, ProcessAt: time.Now().Add(time.Hour)}})
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	ids, err := r.SearchLabelIDs(ctx, "orders", "order_id", "123", "", 20)
	if err != nil || len(ids) != 8 {
		t.Fatalf("missing labels: %v %v", ids, err)
	}
	if _, err := r.DeleteAllPendingTasks("orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.DeleteAllScheduledTasks("orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.DeleteAllAggregatingTasks("orders", "digest"); err != nil {
		t.Fatal(err)
	}
	ids, err = r.SearchLabelIDs(ctx, "orders", "order_id", "123", "", 20)
	if err != nil || len(ids) != 0 {
		t.Fatalf("index leaked after bulk deletion: %v %v", ids, err)
	}
}
