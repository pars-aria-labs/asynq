package metrics

import (
	"errors"
	"testing"
	"time"

	"github.com/pars-aria-labs/asynq"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestInspectorMetricsCollector(t *testing.T) {
	collector := NewInspectorMetricsCollector()
	collector.ObserveInspectorOperation(asynq.InspectorOperation{
		Name:            asynq.InspectorOperationTaskBatch,
		State:           "scheduled",
		Action:          "archive",
		Processed:       4,
		RedisRoundTrips: 3,
		Duration:        20 * time.Millisecond,
	})
	collector.ObserveInspectorOperation(asynq.InspectorOperation{
		Name:            asynq.InspectorOperationTaskBatch,
		State:           "scheduled",
		Action:          "archive",
		Processed:       2,
		RedisRoundTrips: 2,
		Duration:        10 * time.Millisecond,
		Err:             errors.New("reply lost"),
	})

	if got := testutil.ToFloat64(collector.operations.WithLabelValues("task_batch", "scheduled", "archive", "success")); got != 1 {
		t.Fatalf("success operations = %v, want 1", got)
	}
	if got := testutil.ToFloat64(collector.operations.WithLabelValues("task_batch", "scheduled", "archive", "partial")); got != 1 {
		t.Fatalf("partial operations = %v, want 1", got)
	}
	if got := testutil.ToFloat64(collector.items.WithLabelValues("task_batch", "scheduled", "archive")); got != 6 {
		t.Fatalf("processed items = %v, want 6", got)
	}
	if got := testutil.ToFloat64(collector.roundTrips.WithLabelValues("task_batch")); got != 5 {
		t.Fatalf("Redis round trips = %v, want 5", got)
	}
}

func TestInspectorMetricsCollectorNormalizesUntrustedLabels(t *testing.T) {
	collector := NewInspectorMetricsCollector()
	for _, operation := range []asynq.InspectorOperation{
		{Name: "custom-one", State: "tenant-one", Action: "action-one"},
		{Name: "custom-two", State: "tenant-two", Action: "action-two"},
	} {
		collector.ObserveInspectorOperation(operation)
	}

	if got := testutil.ToFloat64(collector.operations.WithLabelValues("unknown", "unknown", "unknown", "success")); got != 2 {
		t.Fatalf("normalized operations = %v, want 2", got)
	}
	if got := testutil.ToFloat64(collector.roundTrips.WithLabelValues("unknown")); got != 0 {
		t.Fatalf("normalized Redis round trips = %v, want 0", got)
	}
}
