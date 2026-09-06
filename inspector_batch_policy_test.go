package asynq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pars-aria-labs/asynq/internal/rdb"
	h "github.com/pars-aria-labs/asynq/internal/testutil"
)

func TestInspectorProcessTaskBatchesHonorsPolicy(t *testing.T) {
	redisClient := setup(t)
	defer redisClient.Close()
	store := rdb.NewRDB(redisClient)
	inspector := newInspectorFromRedisClient(redisClient, "")
	for range 7 {
		if err := store.Schedule(context.Background(), h.NewTaskMessage("policy-task", nil), time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}

	got, err := inspector.ProcessTaskBatches(
		context.Background(), "default", "scheduled", "archive", "",
		TaskBatchPolicy{BatchSize: 3, MaxTasks: 5},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := TaskBatchResult{Processed: 5, Remaining: 2, Batches: 2}
	if got != want {
		t.Fatalf("ProcessTaskBatches() = %+v, want %+v", got, want)
	}
}

func TestInspectorProcessTaskBatchesTimeoutIncludesDelay(t *testing.T) {
	redisClient := setup(t)
	defer redisClient.Close()
	store := rdb.NewRDB(redisClient)
	inspector := newInspectorFromRedisClient(redisClient, "")
	for range 2 {
		if err := store.Schedule(context.Background(), h.NewTaskMessage("timeout-task", nil), time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}

	got, err := inspector.ProcessTaskBatches(
		context.Background(), "default", "scheduled", "delete", "",
		TaskBatchPolicy{BatchSize: 1, Timeout: 20 * time.Millisecond, InterBatchDelay: time.Second},
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context.DeadlineExceeded", err)
	}
	if got.Processed != 1 || got.Remaining != 1 || got.Batches != 1 {
		t.Fatalf("result = %+v, want processed=1 remaining=1 batches=1", got)
	}
}

func TestInspectorProcessTaskBatchesValidatesPolicy(t *testing.T) {
	inspector := &Inspector{}
	tests := []TaskBatchPolicy{
		{BatchSize: -1},
		{BatchSize: MaxInspectorBatchSize + 1},
		{MaxTasks: -1},
		{Timeout: -1},
		{InterBatchDelay: -1},
	}
	for _, policy := range tests {
		if _, err := inspector.ProcessTaskBatches(context.Background(), "default", "scheduled", "delete", "", policy); err == nil {
			t.Fatalf("policy %+v was accepted", policy)
		}
	}
}

type recordingOperationObserver struct {
	mu           sync.Mutex
	observations []InspectorOperation
	panic        bool
}

func (o *recordingOperationObserver) ObserveInspectorOperation(observation InspectorOperation) {
	if o.panic {
		panic("observer panic")
	}
	o.mu.Lock()
	o.observations = append(o.observations, observation)
	o.mu.Unlock()
}

func TestInspectorOperationObserver(t *testing.T) {
	redisClient := setup(t)
	defer redisClient.Close()
	store := rdb.NewRDB(redisClient)
	inspector := newInspectorFromRedisClient(redisClient, "")
	if err := store.Schedule(context.Background(), h.NewTaskMessage("observed-task", nil), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	observer := new(recordingOperationObserver)
	inspector.SetOperationObserver(observer)

	processed, remaining, err := inspector.ProcessTaskBatch(context.Background(), "default", "scheduled", "delete", "", 1)
	if err != nil || processed != 1 || remaining != 0 {
		t.Fatalf("batch = (%d, %d, %v)", processed, remaining, err)
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if len(observer.observations) != 1 {
		t.Fatalf("observations = %d, want 1", len(observer.observations))
	}
	got := observer.observations[0]
	if got.Name != InspectorOperationTaskBatch || got.Processed != 1 || got.Err != nil || got.Duration <= 0 || got.RedisRoundTrips < 3 {
		t.Fatalf("observation = %+v", got)
	}
}

func TestInspectorOperationObserverPanicIsIsolated(t *testing.T) {
	redisClient := setup(t)
	defer redisClient.Close()
	store := rdb.NewRDB(redisClient)
	inspector := newInspectorFromRedisClient(redisClient, "")
	if err := store.Schedule(context.Background(), h.NewTaskMessage("panic-observer-task", nil), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	inspector.SetOperationObserver(&recordingOperationObserver{panic: true})
	if _, _, err := inspector.ProcessTaskBatch(context.Background(), "default", "scheduled", "delete", "", 1); err != nil {
		t.Fatalf("observer panic changed operation result: %v", err)
	}
}

func TestInspectorOperationObserverRecordsQueueBatch(t *testing.T) {
	redisClient := setup(t)
	defer redisClient.Close()
	store := rdb.NewRDB(redisClient)
	inspector := newInspectorFromRedisClient(redisClient, "")
	if err := store.Enqueue(context.Background(), h.NewTaskMessage("observed-queue", nil)); err != nil {
		t.Fatal(err)
	}
	observer := new(recordingOperationObserver)
	inspector.SetOperationObserver(observer)

	infos, err := inspector.GetQueueInfoBatch(context.Background(), []string{"default"}, 0)
	if err != nil || len(infos) != 1 {
		t.Fatalf("GetQueueInfoBatch() = (%d entries, %v)", len(infos), err)
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if len(observer.observations) != 1 {
		t.Fatalf("observations = %d, want 1", len(observer.observations))
	}
	got := observer.observations[0]
	if got.Name != InspectorOperationQueueInfoBatch || got.Processed != 1 || got.RedisRoundTrips < 1 {
		t.Fatalf("observation = %+v", got)
	}
}
