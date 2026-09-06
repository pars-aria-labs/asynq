package asynq

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/pars-aria-labs/asynq/internal/base"
	"github.com/pars-aria-labs/asynq/internal/rdb"
	h "github.com/pars-aria-labs/asynq/internal/testutil"
	"github.com/redis/go-redis/v9"
)

func TestInspectorQueryTasksDispatchesSupportedStates(t *testing.T) {
	redisClient := setup(t)
	defer redisClient.Close()
	store := rdb.NewRDB(redisClient)
	inspector := newInspectorFromRedisClient(redisClient, "")
	ctx := context.Background()

	type queryFixture struct {
		query TaskQuery
		id    string
	}
	var fixtures []queryFixture

	addFixture := func(state TaskState, queue, group string, prepare func(*base.TaskMessage) error) {
		t.Helper()
		message := h.NewTaskMessageWithQueue("query:"+state.String(), nil, queue)
		message.GroupKey = group
		if err := prepare(message); err != nil {
			t.Fatalf("prepare %s fixture: %v", state, err)
		}
		fixtures = append(fixtures, queryFixture{
			query: TaskQuery{Queue: queue, State: state, Group: group, PageSize: 10},
			id:    message.ID,
		})
	}

	addFixture(TaskStatePending, "query-pending", "", func(message *base.TaskMessage) error {
		return store.Enqueue(ctx, message)
	})
	addFixture(TaskStateScheduled, "query-scheduled", "", func(message *base.TaskMessage) error {
		return store.Schedule(ctx, message, time.Now().Add(time.Hour))
	})
	addFixture(TaskStateArchived, "query-archived", "", func(message *base.TaskMessage) error {
		if err := store.Enqueue(ctx, message); err != nil {
			return err
		}
		return store.ArchiveTask(message.Queue, message.ID)
	})
	addFixture(TaskStateCompleted, "query-completed", "", func(message *base.TaskMessage) error {
		message.Retention = int64(time.Hour.Seconds())
		if err := store.Enqueue(ctx, message); err != nil {
			return err
		}
		dequeued, _, err := store.Dequeue(message.Queue)
		if err != nil {
			return err
		}
		return store.MarkAsComplete(ctx, dequeued)
	})
	addFixture(TaskStateAggregating, "query-aggregating", "query-group", func(message *base.TaskMessage) error {
		return store.AddToGroup(ctx, message, message.GroupKey)
	})
	addFixture(TaskStateActive, "query-active", "", func(message *base.TaskMessage) error {
		if err := store.Enqueue(ctx, message); err != nil {
			return err
		}
		dequeued, _, err := store.Dequeue(message.Queue)
		if err != nil {
			return err
		}
		return redisClient.ZAdd(ctx, base.LeaseKey(message.Queue), redis.Z{
			Score:  float64(time.Now().Add(-time.Minute).Unix()),
			Member: dequeued.ID,
		}).Err()
	})

	for _, fixture := range fixtures {
		fixture := fixture
		t.Run(fixture.query.State.String(), func(t *testing.T) {
			result, err := inspector.QueryTasks(ctx, fixture.query)
			if err != nil {
				t.Fatalf("QueryTasks(%s): %v", fixture.query.State, err)
			}
			if result.Scanned != 1 || len(result.Tasks) != 1 {
				t.Fatalf("QueryTasks(%s) = %+v, want exactly one task", fixture.query.State, result)
			}
			got := result.Tasks[0]
			if got.ID != fixture.id || got.State != fixture.query.State {
				t.Fatalf("QueryTasks(%s) task = %+v, want ID %q in state %s", fixture.query.State, got, fixture.id, fixture.query.State)
			}
			if fixture.query.State == TaskStateActive && !got.IsOrphaned {
				t.Fatal("active task with an expired lease was not marked orphaned")
			}
		})
	}
}

func TestInspectorQueryTasksUsesHalfOpenTimeRange(t *testing.T) {
	redisClient := setup(t)
	defer redisClient.Close()
	store := rdb.NewRDB(redisClient)
	inspector := newInspectorFromRedisClient(redisClient, "")
	ctx := context.Background()
	queue := "query-time-boundaries"
	from := time.Now().UTC().Truncate(time.Second).Add(time.Hour)
	before := from.Add(10 * time.Second)

	timestamps := []struct {
		name string
		at   time.Time
		want bool
	}{
		{name: "before-from", at: from.Add(-time.Second)},
		{name: "at-from", at: from, want: true},
		{name: "inside", at: from.Add(5 * time.Second), want: true},
		{name: "at-before", at: before},
		{name: "after-before", at: before.Add(time.Second)},
	}
	wantIDs := make(map[string]bool)
	for _, timestamp := range timestamps {
		message := h.NewTaskMessageWithQueue("boundary:"+timestamp.name, nil, queue)
		if err := store.Schedule(ctx, message, timestamp.at); err != nil {
			t.Fatalf("schedule %s: %v", timestamp.name, err)
		}
		if timestamp.want {
			wantIDs[message.ID] = true
		}
	}

	result, err := inspector.QueryTasks(ctx, TaskQuery{
		Queue:    queue,
		State:    TaskStateScheduled,
		PageSize: 10,
		Filter: TaskFilter{Time: &TaskTimeRange{
			Field:  TaskTimeNextProcess,
			From:   from,
			Before: before,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Scanned != len(timestamps) {
		t.Fatalf("Scanned = %d, want %d", result.Scanned, len(timestamps))
	}
	if len(result.Tasks) != len(wantIDs) {
		t.Fatalf("matched tasks = %+v, want IDs %v", result.Tasks, wantIDs)
	}
	for _, task := range result.Tasks {
		if !wantIDs[task.ID] {
			t.Errorf("unexpected boundary match: ID %q at %v", task.ID, task.NextProcessAt)
		}
		delete(wantIDs, task.ID)
	}
	if len(wantIDs) != 0 {
		t.Fatalf("missing boundary matches: %v", wantIDs)
	}
}

func TestInspectorQueryTasksPreservesCanceledContext(t *testing.T) {
	redisClient := setup(t)
	defer redisClient.Close()
	store := rdb.NewRDB(redisClient)
	inspector := newInspectorFromRedisClient(redisClient, "")
	message := h.NewTaskMessageWithQueue("query:canceled", nil, "query-canceled")
	if err := store.Enqueue(context.Background(), message); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := inspector.QueryTasks(ctx, TaskQuery{Queue: message.Queue, State: TaskStatePending})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("QueryTasks error = %v, want context.Canceled", err)
	}
}

func TestInspectorQueryTasksHonorsRedisPrefix(t *testing.T) {
	redisClient := setup(t)
	defer redisClient.Close()
	ctx := context.Background()
	const (
		prefix = "query-tenant"
		queue  = "shared-query-queue"
	)

	prefixedStore := rdb.NewRDB(redisClient, prefix)
	prefixedInspector := newInspectorFromRedisClient(redisClient, prefix)
	prefixedMessage := h.NewTaskMessageWithQueue("query:prefixed", nil, queue)
	if err := prefixedStore.Enqueue(ctx, prefixedMessage); err != nil {
		t.Fatal(err)
	}

	defaultStore := rdb.NewRDB(redisClient)
	defaultMessage := h.NewTaskMessageWithQueue("query:default", nil, queue)
	if err := defaultStore.Enqueue(ctx, defaultMessage); err != nil {
		t.Fatal(err)
	}

	result, err := prefixedInspector.QueryTasks(ctx, TaskQuery{
		Queue: queue, State: TaskStatePending, PageSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Scanned != 1 || len(result.Tasks) != 1 || result.Tasks[0].ID != prefixedMessage.ID {
		t.Fatalf("prefixed QueryTasks result = %+v, want only %q", result, prefixedMessage.ID)
	}
}

func TestInspectorQueryTasksFiltersAndProcessesSnapshot(t *testing.T) {
	redisClient := setup(t)
	defer redisClient.Close()
	store := rdb.NewRDB(redisClient)
	inspector := newInspectorFromRedisClient(redisClient, "")
	ctx := context.Background()
	from := time.Now().Add(-time.Minute)

	email := h.NewTaskMessage("email:send", nil)
	report := h.NewTaskMessage("report:build", nil)
	if err := store.Enqueue(ctx, email); err != nil {
		t.Fatal(err)
	}
	if err := store.Enqueue(ctx, report); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		message, _, err := store.Dequeue("default")
		if err != nil {
			t.Fatal(err)
		}
		errorMessage := "invalid template"
		if message.ID == email.ID {
			errorMessage = "upstream timeout"
		}
		if err := store.Retry(ctx, message, time.Now().Add(time.Hour), errorMessage, true); err != nil {
			t.Fatal(err)
		}
	}

	result, err := inspector.QueryTasks(ctx, TaskQuery{
		Queue:    "default",
		State:    TaskStateRetry,
		PageSize: 10,
		Filter: TaskFilter{
			Types:         []string{"email:send"},
			ErrorContains: "timeout",
			Time: &TaskTimeRange{
				Field:  TaskTimeLastFailure,
				From:   from,
				Before: time.Now().Add(time.Minute),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Scanned != 2 || len(result.Tasks) != 1 || result.Tasks[0].ID != email.ID {
		t.Fatalf("query result = %+v", result)
	}

	processed, err := inspector.ProcessTaskIDs(ctx, "default", "run", []string{result.Tasks[0].ID})
	if err != nil || processed != 1 {
		t.Fatalf("ProcessTaskIDs() = (%d, %v), want (1, nil)", processed, err)
	}
	info, err := inspector.GetTaskInfo("default", email.ID)
	if err != nil {
		t.Fatal(err)
	}
	if info.State != TaskStatePending {
		t.Fatalf("task state = %v, want pending", info.State)
	}
}

func TestInspectorProcessTaskIDsReturnsPartialCount(t *testing.T) {
	redisClient := setup(t)
	defer redisClient.Close()
	store := rdb.NewRDB(redisClient)
	inspector := newInspectorFromRedisClient(redisClient, "")
	message := h.NewTaskMessage("snapshot-task", nil)
	if err := store.Enqueue(context.Background(), message); err != nil {
		t.Fatal(err)
	}

	processed, err := inspector.ProcessTaskIDs(context.Background(), "default", "delete", []string{message.ID, "missing"})
	if processed != 1 || !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("ProcessTaskIDs() = (%d, %v), want partial count and ErrTaskNotFound", processed, err)
	}
}

func TestInspectorQueryAndProcessValidation(t *testing.T) {
	valid := TaskQuery{Queue: "default", State: TaskStatePending}
	tests := []TaskQuery{
		{Queue: " ", State: TaskStatePending},
		{Queue: "default", State: TaskStatePending, Page: -1},
		{Queue: "default", State: TaskStatePending, PageSize: MaxInspectorBatchSize + 1},
		{Queue: "default", State: TaskStatePending, Page: math.MaxInt, PageSize: MaxInspectorBatchSize},
		{Queue: "default", State: TaskStateAggregating},
		{Queue: "default", State: TaskStatePending, Filter: TaskFilter{Time: &TaskTimeRange{Field: "unknown"}}},
		{Queue: "default", State: TaskStatePending, Filter: TaskFilter{Time: &TaskTimeRange{Field: TaskTimeCompletion, From: time.Now(), Before: time.Now().Add(-time.Hour)}}},
	}
	for _, query := range tests {
		if _, _, err := validateTaskQuery(query); err == nil {
			t.Fatalf("query %+v was accepted", query)
		}
	}
	if _, _, err := validateTaskQuery(valid); err != nil {
		t.Fatalf("valid query rejected: %v", err)
	}

	inspector := &Inspector{}
	tooMany := make([]string, MaxInspectorBatchSize+1)
	if _, err := inspector.ProcessTaskIDs(context.Background(), "default", "delete", tooMany); err == nil {
		t.Fatal("too many task IDs were accepted")
	}
	if _, err := inspector.ProcessTaskIDs(context.Background(), "default", "unknown", nil); err == nil {
		t.Fatal("unknown action was accepted")
	}
	if _, err := inspector.ProcessTaskIDs(context.Background(), "default", "delete", []string{"same", "same"}); err == nil {
		t.Fatal("duplicate task IDs were accepted")
	}
}
