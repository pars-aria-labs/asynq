package asynq

import "context"

// WithContext returns a lightweight Inspector view whose legacy methods use
// ctx for Redis commands. It makes methods such as Queues, GetTaskInfo,
// ListPendingTasks, DeleteTask, PauseQueue, and Servers cancellable without
// changing their established signatures.
//
// The returned Inspector shares the underlying Redis connection with i. Its
// Close method therefore returns an error instead of closing that connection.
// Memory-usage samples are intentionally not copied, so a derived Inspector
// starts with an empty local cache. Methods that already accept a context keep
// using the context supplied directly to those methods.
//
// Passing a nil context panics, consistently with the standard context-aware
// APIs in the Go ecosystem.
func (i *Inspector) WithContext(ctx context.Context) *Inspector {
	if ctx == nil {
		panic("nil context")
	}
	derived := &Inspector{
		rdb:              i.rdb.WithContext(ctx),
		sharedConnection: true,
	}
	derived.observer = i.operationObserver()
	return derived
}
