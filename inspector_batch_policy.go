package asynq

import (
	"context"
	"fmt"
	"time"

	"github.com/pars-aria-labs/asynq/internal/rdb"
)

// MaxInspectorBatchSize is the largest atomic source-state transition batch
// accepted by the bounded Inspector APIs. Archive retention cleanup has a
// separate bounded eviction budget of the same size.
const MaxInspectorBatchSize = rdb.BulkBatchSize

// TaskBatchPolicy controls a bounded series of task batches.
type TaskBatchPolicy struct {
	// BatchSize is the maximum number of source tasks transitioned atomically.
	// Zero uses MaxInspectorBatchSize. Values above MaxInspectorBatchSize are
	// rejected.
	BatchSize int

	// MaxTasks caps the total number of tasks changed by the series. Zero
	// freezes a budget from the source cardinality observed after the first
	// batch, preventing concurrent producers from extending the operation.
	MaxTasks int

	// Timeout bounds the whole series, including InterBatchDelay. Zero leaves
	// timeout control to ctx.
	Timeout time.Duration

	// InterBatchDelay throttles successive batches to reduce pressure on Redis.
	// The wait is context-aware.
	InterBatchDelay time.Duration
}

// TaskBatchResult summarizes a ProcessTaskBatches call. Processed is the number
// of mutations confirmed by successful Redis replies; after a transport error
// it is a lower bound because the failing mutation may have committed.
// Remaining is the live source-state cardinality observed after the last
// completed batch.
type TaskBatchResult struct {
	Processed int
	Remaining int
	Batches   int
}

// ProcessTaskBatches repeatedly invokes ProcessTaskBatch under a fixed work
// budget. It provides a reusable timeout and backpressure policy while keeping
// every individual Redis mutation atomic and limited to 500 source-state
// transitions. An archive mutation may also perform up to 500 bounded
// retention evictions.
//
// A non-nil error can accompany a nonzero Processed count. In that case the
// caller must not automatically retry the failed batch, because a lost Redis
// reply can make the mutation outcome ambiguous.
func (i *Inspector) ProcessTaskBatches(ctx context.Context, queue, state, action, group string, policy TaskBatchPolicy) (result TaskBatchResult, err error) {
	started := time.Now()
	ctx, roundTrips := rdb.WithRoundTripCounter(ctx)
	defer func() {
		i.observeOperation(InspectorOperation{
			Name:            InspectorOperationTaskBatchSeries,
			State:           state,
			Action:          action,
			Requested:       policy.MaxTasks,
			Processed:       result.Processed,
			Remaining:       result.Remaining,
			Batches:         result.Batches,
			RedisRoundTrips: roundTrips(),
			StartedAt:       started,
			Duration:        time.Since(started),
			Err:             err,
		})
	}()

	batchSize, err := normalizeTaskBatchPolicy(policy)
	if err != nil {
		return result, err
	}
	if policy.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, policy.Timeout)
		defer cancel()
	}

	budget := policy.MaxTasks
	budgetKnown := budget > 0
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		limit := batchSize
		if budgetKnown {
			remainingBudget := budget - result.Processed
			if remainingBudget <= 0 {
				return result, nil
			}
			limit = min(limit, remainingBudget)
		}

		processed, remaining, batchErr := i.ProcessTaskBatch(ctx, queue, state, action, group, limit)
		result.Processed += processed
		result.Remaining = remaining
		result.Batches++
		if !budgetKnown {
			budget = result.Processed + remaining
			budgetKnown = true
		}
		if batchErr != nil {
			return result, batchErr
		}
		if remaining == 0 || processed == 0 || result.Processed >= budget {
			return result, nil
		}

		if policy.InterBatchDelay > 0 {
			timer := time.NewTimer(policy.InterBatchDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return result, ctx.Err()
			case <-timer.C:
			}
		}
	}
}

func normalizeTaskBatchPolicy(policy TaskBatchPolicy) (int, error) {
	if policy.BatchSize < 0 || policy.BatchSize > MaxInspectorBatchSize {
		return 0, fmt.Errorf("batch size must be between 1 and %d, or zero for the default", MaxInspectorBatchSize)
	}
	if policy.MaxTasks < 0 {
		return 0, fmt.Errorf("max tasks must not be negative")
	}
	if policy.Timeout < 0 {
		return 0, fmt.Errorf("timeout must not be negative")
	}
	if policy.InterBatchDelay < 0 {
		return 0, fmt.Errorf("inter-batch delay must not be negative")
	}
	if policy.BatchSize == 0 {
		return MaxInspectorBatchSize, nil
	}
	return policy.BatchSize, nil
}
