// Copyright 2026 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

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

func TestInspectorProcessTaskBatch(t *testing.T) {
	tests := []struct {
		state  string
		action string
	}{
		{state: "pending", action: "delete"},
		{state: "pending", action: "archive"},
		{state: "scheduled", action: "delete"},
		{state: "scheduled", action: "run"},
		{state: "scheduled", action: "archive"},
		{state: "retry", action: "delete"},
		{state: "retry", action: "run"},
		{state: "retry", action: "archive"},
		{state: "archived", action: "delete"},
		{state: "archived", action: "run"},
		{state: "completed", action: "delete"},
		{state: "aggregating", action: "delete"},
		{state: "aggregating", action: "run"},
		{state: "aggregating", action: "archive"},
	}
	for _, tc := range tests {
		t.Run(tc.state+"/"+tc.action, func(t *testing.T) {
			redisClient := setup(t)
			store := rdb.NewRDB(redisClient)
			inspector := newInspectorFromRedisClient(redisClient, "")
			ctx := context.Background()
			for range 2 {
				if err := seedTaskBatchState(ctx, store, tc.state); err != nil {
					t.Fatal(err)
				}
			}

			processed, remaining, err := inspector.ProcessTaskBatch(ctx, "default", tc.state, tc.action, "batch-group", 1)
			if err != nil || processed != 1 || remaining != 1 {
				t.Fatalf("first batch = (%d, %d, %v), want (1, 1, nil)", processed, remaining, err)
			}
			processed, remaining, err = inspector.ProcessTaskBatch(ctx, "default", tc.state, tc.action, "batch-group", 1)
			if err != nil || processed != 1 || remaining != 0 {
				t.Fatalf("second batch = (%d, %d, %v), want (1, 0, nil)", processed, remaining, err)
			}
		})
	}
}

func seedTaskBatchState(ctx context.Context, store *rdb.RDB, state string) error {
	msg := h.NewTaskMessage("batch-task", nil)
	switch state {
	case "pending":
		return store.Enqueue(ctx, msg)
	case "scheduled":
		return store.Schedule(ctx, msg, time.Now().Add(time.Hour))
	case "aggregating":
		return store.AddToGroup(ctx, msg, "batch-group")
	}
	if err := store.Enqueue(ctx, msg); err != nil {
		return err
	}
	msg, _, err := store.Dequeue("default")
	if err != nil {
		return err
	}
	switch state {
	case "retry":
		return store.Retry(ctx, msg, time.Now().Add(time.Hour), "failed", true)
	case "archived":
		return store.Archive(ctx, msg, "failed")
	case "completed":
		msg.Retention = int64(time.Hour.Seconds())
		return store.MarkAsComplete(ctx, msg)
	default:
		return nil
	}
}

func TestInspectorProcessTaskBatchRejectsInvalidInput(t *testing.T) {
	redisClient := setup(t)
	store := rdb.NewRDB(redisClient)
	inspector := newInspectorFromRedisClient(redisClient, "")
	ctx := context.Background()
	if err := store.Enqueue(ctx, h.NewTaskMessage("batch-task", nil)); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name                   string
		queue, state, action   string
		group                  string
		limit                  int
		wantQueueNotFoundError bool
	}{
		{name: "zero limit", queue: "default", state: "pending", action: "delete", limit: 0},
		{name: "large limit", queue: "default", state: "pending", action: "delete", limit: rdb.BulkBatchSize + 1},
		{name: "invalid queue", queue: "bad queue", state: "pending", action: "delete", limit: 1},
		{name: "missing queue", queue: "missing", state: "pending", action: "delete", limit: 1, wantQueueNotFoundError: true},
		{name: "unknown state", queue: "default", state: "active", action: "delete", limit: 1},
		{name: "unknown action", queue: "default", state: "pending", action: "retry", limit: 1},
		{name: "missing group", queue: "default", state: "aggregating", action: "delete", limit: 1},
		{name: "run pending", queue: "default", state: "pending", action: "run", limit: 1},
		{name: "run completed", queue: "default", state: "completed", action: "run", limit: 1},
		{name: "archive archived", queue: "default", state: "archived", action: "archive", limit: 1},
		{name: "archive completed", queue: "default", state: "completed", action: "archive", limit: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			processed, remaining, err := inspector.ProcessTaskBatch(ctx, tc.queue, tc.state, tc.action, tc.group, tc.limit)
			if err == nil {
				t.Fatalf("ProcessTaskBatch() = (%d, %d, nil), want an error", processed, remaining)
			}
			if tc.wantQueueNotFoundError && !errors.Is(err, ErrQueueNotFound) {
				t.Fatalf("ProcessTaskBatch() error = %v, want ErrQueueNotFound", err)
			}
		})
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := inspector.ProcessTaskBatch(canceled, "default", "pending", "delete", "", 1); err == nil {
		t.Fatal("ProcessTaskBatch with a canceled context succeeded")
	}
}

func TestInspectorGetQueueInfoBatch(t *testing.T) {
	redisClient := setup(t)
	store := rdb.NewRDB(redisClient)
	inspector := newInspectorFromRedisClient(redisClient, "")
	ctx := context.Background()
	for _, queue := range []string{"first", "second"} {
		if err := store.Enqueue(ctx, h.NewTaskMessageWithQueue("batch-stats", nil, queue)); err != nil {
			t.Fatal(err)
		}
	}

	// A known sample verifies both duplicate ordering and that only the memory
	// estimate is cached; live counters still come from Redis.
	inspector.memorySamples = map[string]queueMemorySample{
		"first": {bytes: -1, at: time.Now().Add(time.Hour)},
	}
	infos, err := inspector.GetQueueInfoBatch(ctx, []string{"first", "second", "first"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 3 || infos[0].Queue != "first" || infos[1].Queue != "second" || infos[2].Queue != "first" {
		t.Fatalf("GetQueueInfoBatch returned queues out of order: %+v", infos)
	}
	if infos[0].MemoryUsage != -1 || infos[2].MemoryUsage != -1 {
		t.Fatalf("cached memory sample was not used: %+v", infos)
	}
	if infos[0].Pending != 1 || infos[1].Pending != 1 || infos[2].Pending != 1 {
		t.Fatalf("unexpected live counters: %+v", infos)
	}

	// A zero TTL forces a fresh sample and evicts an otherwise fresh value.
	infos, err = inspector.GetQueueInfoBatch(ctx, []string{"first"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if infos[0].MemoryUsage < 0 {
		t.Fatalf("zero TTL reused cached sample: %+v", infos[0])
	}
	if _, ok := inspector.memorySamples["first"]; ok {
		t.Fatal("zero TTL retained a memory sample")
	}
}

func TestInspectorGetQueueInfoBatchErrors(t *testing.T) {
	redisClient := setup(t)
	inspector := newInspectorFromRedisClient(redisClient, "")
	ctx := context.Background()

	if _, err := inspector.GetQueueInfo("missing"); !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("GetQueueInfo error = %v, want ErrQueueNotFound", err)
	}
	if _, err := inspector.GetQueueInfoBatch(ctx, nil, -time.Second); err == nil {
		t.Fatal("GetQueueInfoBatch accepted a negative cache TTL")
	}
	if _, err := inspector.GetQueueInfoBatch(ctx, []string{"bad queue"}, 0); err == nil {
		t.Fatal("GetQueueInfoBatch accepted an invalid queue name")
	}
	if _, err := inspector.GetQueueInfoBatch(ctx, []string{"missing"}, 0); !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("GetQueueInfoBatch error = %v, want ErrQueueNotFound", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := inspector.GetQueueInfoBatch(canceled, []string{"missing"}, 0); err == nil {
		t.Fatal("GetQueueInfoBatch with a canceled context succeeded")
	}
}

func TestInspectorGetQueueInfoBatchConcurrent(t *testing.T) {
	redisClient := setup(t)
	store := rdb.NewRDB(redisClient)
	inspector := newInspectorFromRedisClient(redisClient, "")
	ctx := context.Background()
	if err := store.Enqueue(ctx, h.NewTaskMessageWithQueue("batch-stats", nil, "concurrent")); err != nil {
		t.Fatal(err)
	}

	const goroutines = 8
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 5 {
				infos, err := inspector.GetQueueInfoBatch(ctx, []string{"concurrent"}, time.Minute)
				if err != nil {
					errs <- err
					return
				}
				if len(infos) != 1 || infos[0].Pending != 1 {
					errs <- errors.New("unexpected concurrent queue snapshot")
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
