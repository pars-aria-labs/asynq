package asynq

import (
	"context"
	"errors"
	"testing"
)

func TestInspectorWithContextCancelsLegacyMethods(t *testing.T) {
	redisClient := setup(t)
	defer redisClient.Close()
	inspector := newInspectorFromRedisClient(redisClient, "")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	contextInspector := inspector.WithContext(ctx)

	tests := []struct {
		name string
		call func() error
	}{
		{name: "queues", call: func() error { _, err := contextInspector.Queues(); return err }},
		{name: "queue info", call: func() error { _, err := contextInspector.GetQueueInfo("default"); return err }},
		{name: "list", call: func() error { _, err := contextInspector.ListPendingTasks("default"); return err }},
		{name: "mutation", call: func() error { return contextInspector.PauseQueue("default") }},
		{name: "payload mutation", call: func() error { return contextInspector.UpdateTaskPayload("default", "task", nil) }},
		{name: "servers", call: func() error { _, err := contextInspector.Servers(); return err }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v, want context.Canceled", err)
			}
		})
	}

	if _, err := inspector.Queues(); err != nil {
		t.Fatalf("original Inspector was affected by derived context: %v", err)
	}
	if err := contextInspector.Close(); err == nil {
		t.Fatal("derived Inspector.Close() succeeded; want shared-connection error")
	}
}

func TestInspectorWithContextRejectsNil(t *testing.T) {
	redisClient := setup(t)
	defer redisClient.Close()
	inspector := newInspectorFromRedisClient(redisClient, "")
	defer func() {
		if recover() == nil {
			t.Fatal("WithContext(nil) did not panic")
		}
	}()
	inspector.WithContext(nil)
}
