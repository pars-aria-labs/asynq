package asynq

import (
	"context"
	"fmt"
	"time"

	"github.com/pars-aria-labs/asynq/internal/base"
	"github.com/pars-aria-labs/asynq/internal/errors"
	"github.com/pars-aria-labs/asynq/internal/rdb"
)

// ProcessTaskBatch transitions at most limit source tasks (1..500) in one
// atomic Redis batch. Archive actions may additionally evict up to 500 expired
// or excess archive entries for retention maintenance. Supported actions are
// delete, archive and run. Active tasks are never mutated. Remaining is a live
// source count; callers must impose a total budget when repeating batches in
// queues with concurrent producers. An error after a successful mutation may
// return a nonzero processed count. If the queue does not exist, the returned
// error wraps ErrQueueNotFound.
func (i *Inspector) ProcessTaskBatch(ctx context.Context, queue, state, action, group string, limit int) (processed, remaining int, err error) {
	started := time.Now()
	ctx, roundTrips := rdb.WithRoundTripCounter(ctx)
	defer func() {
		i.observeOperation(InspectorOperation{
			Name:            InspectorOperationTaskBatch,
			State:           state,
			Action:          action,
			Requested:       limit,
			Processed:       processed,
			Remaining:       remaining,
			Batches:         1,
			RedisRoundTrips: roundTrips(),
			StartedAt:       started,
			Duration:        time.Since(started),
			Err:             err,
		})
	}()
	n, left, err := i.rdb.TaskBatch(ctx, queue, state, action, group, limit)
	if errors.IsQueueNotFound(err) {
		err = fmt.Errorf("asynq: %w", ErrQueueNotFound)
	}
	return int(n), int(left), err
}

// GetQueueInfoBatch reads current counters for queues using Redis pipelines.
// Only memory estimates may be cached, for at most memoryCacheTTL. Zero forces
// fresh samples. Results follow input order and duplicate names are allowed. If
// a queue does not exist, the returned error wraps ErrQueueNotFound.
func (i *Inspector) GetQueueInfoBatch(ctx context.Context, queues []string, memoryCacheTTL time.Duration) (result []*QueueInfo, err error) {
	started := time.Now()
	ctx, roundTrips := rdb.WithRoundTripCounter(ctx)
	defer func() {
		i.observeOperation(InspectorOperation{
			Name:            InspectorOperationQueueInfoBatch,
			Requested:       len(queues),
			Processed:       len(result),
			RedisRoundTrips: roundTrips(),
			StartedAt:       started,
			Duration:        time.Since(started),
			Err:             err,
		})
	}()
	if memoryCacheTTL < 0 {
		return nil, fmt.Errorf("memory cache TTL must not be negative")
	}
	for _, q := range queues {
		if err := base.ValidateQueueName(q); err != nil {
			return nil, err
		}
	}
	now := time.Now()
	memory := make(map[string]int64)
	i.memoryMu.Lock()
	if memoryCacheTTL == 0 {
		// A zero TTL is an unconditional fresh-read request. Do not compare
		// timestamps: another goroutine can publish a newer sample after now was
		// captured, and a negative age must not make that sample eligible here.
		for _, q := range queues {
			delete(i.memorySamples, q)
		}
	} else {
		for q, sample := range i.memorySamples {
			if now.Sub(sample.at) >= memoryCacheTTL {
				delete(i.memorySamples, q)
			}
		}
		for _, q := range queues {
			if sample, ok := i.memorySamples[q]; ok {
				memory[q] = sample.bytes
			}
		}
	}
	i.memoryMu.Unlock()
	stats, err := i.rdb.CurrentStatsBatch(ctx, queues, memory)
	if errors.IsQueueNotFound(err) {
		return nil, fmt.Errorf("asynq: %w", ErrQueueNotFound)
	}
	if err != nil {
		return nil, err
	}
	result = make([]*QueueInfo, len(stats))
	i.memoryMu.Lock()
	defer i.memoryMu.Unlock()
	if i.memorySamples == nil {
		i.memorySamples = make(map[string]queueMemorySample)
	}
	for j, s := range stats {
		result[j] = queueInfoFromStats(s)
		if _, cached := memory[s.Queue]; !cached && memoryCacheTTL > 0 {
			i.memorySamples[s.Queue] = queueMemorySample{bytes: s.MemoryUsage, at: now}
		}
	}
	return result, nil
}

type queueMemorySample struct {
	bytes int64
	at    time.Time
}
