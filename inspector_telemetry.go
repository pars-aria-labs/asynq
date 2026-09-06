package asynq

import "time"

// Inspector operation names emitted through InspectorOperationObserver.
const (
	InspectorOperationTaskBatch       = "task_batch"
	InspectorOperationTaskBatchSeries = "task_batch_series"
	InspectorOperationTaskIDs         = "task_ids"
	InspectorOperationQueueInfoBatch  = "queue_info_batch"
)

// InspectorOperation describes one observable Inspector batch operation.
// Queue and group names are deliberately omitted to prevent accidental
// high-cardinality metric labels. Err is provided for logging and tracing;
// metric adapters should reduce it to a small outcome set.
type InspectorOperation struct {
	Name      string
	State     string
	Action    string
	Requested int
	Processed int
	Remaining int
	Batches   int
	// RedisRoundTrips counts instrumented client execution attempts, including
	// one count per pipeline flush. It approximates application-visible Redis
	// round trips; retries and cluster fan-out inside the Redis client can use
	// more physical network exchanges.
	RedisRoundTrips int
	StartedAt       time.Time
	Duration        time.Duration
	Err             error
}

// Partial reports whether an operation changed at least one task and then
// returned an error.
func (o InspectorOperation) Partial() bool {
	return o.Err != nil && o.Processed > 0
}

// InspectorOperationObserver receives low-cardinality observations from batch
// Inspector APIs. ObserveInspectorOperation is called synchronously after an
// operation finishes and may be called concurrently by different goroutines.
// Implementations must be concurrency-safe, should return quickly, and must
// not call back into the same Inspector. A panic in an observer is recovered
// and never changes the result of the Redis operation.
type InspectorOperationObserver interface {
	ObserveInspectorOperation(InspectorOperation)
}

// SetOperationObserver replaces the optional observer used by this Inspector.
// Passing nil disables observations. It is safe to call concurrently with
// batch operations.
func (i *Inspector) SetOperationObserver(observer InspectorOperationObserver) {
	i.observerMu.Lock()
	i.observer = observer
	i.observerMu.Unlock()
}

func (i *Inspector) operationObserver() InspectorOperationObserver {
	i.observerMu.RLock()
	observer := i.observer
	i.observerMu.RUnlock()
	return observer
}

func (i *Inspector) observeOperation(observation InspectorOperation) {
	observer := i.operationObserver()
	if observer == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	observer.ObserveInspectorOperation(observation)
}
