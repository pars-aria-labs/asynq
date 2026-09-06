package rdb

import (
	"context"
	"fmt"

	"github.com/pars-aria-labs/asynq/internal/base"
	"github.com/pars-aria-labs/asynq/internal/errors"
	"github.com/redis/go-redis/v9"
)

type statsScriptCall struct {
	script *redis.Script
	keys   []string
	args   []any
	cmd    *redis.Cmd
}

// CurrentStatsBatch reads queue snapshots in pipelines capped at 100 queues per
// flush. Every script uses one queue's hash slot, so pipelines can also route
// through Redis Cluster. Exact aggregation counts and uncached memory estimates
// still inspect the registered aggregation groups of each queue, so their work
// is proportional to that group count. Memory samples supplied by the caller
// avoid expensive repeated MEMORY calls.
func (r *RDB) CurrentStatsBatch(ctx context.Context, queues []string, memory map[string]int64) ([]*Stats, error) {
	const pipelineBatchSize = 100
	var op errors.Op = "rdb.CurrentStatsBatch"
	result := make([]*Stats, 0, len(queues))
	for start := 0; start < len(queues); start += pipelineBatchSize {
		batch := queues[start:min(start+pipelineBatchSize, len(queues))]
		now := r.clock.Now()
		pipe := r.client.Pipeline()
		exists := make([]*redis.BoolCmd, len(batch))
		calls := make([]statsScriptCall, 0, len(batch)*2)
		positions := make([]int, len(batch))
		for j, q := range batch {
			exists[j] = pipe.SIsMember(ctx, base.AllQueuesKey(r.prefix), q)
			keys, args := r.currentStatsArgs(q, now)
			positions[j] = len(calls)
			calls = append(calls, statsScriptCall{script: currentStatsCmd, keys: keys, args: args})
			if _, cached := memory[q]; !cached {
				keys, args = r.memoryUsageArgs(q)
				calls = append(calls, statsScriptCall{script: memoryUsageCmd, keys: keys, args: args})
			}
		}
		for j := range calls {
			c := &calls[j]
			c.cmd = c.script.EvalSha(ctx, pipe, c.keys, c.args...)
		}
		// Individual command errors are inspected below. Retry only NOSCRIPT,
		// including a cold cache on any of the cluster nodes.
		countRoundTrip(ctx)
		_, execErr := pipe.Exec(ctx)
		if execErr != nil && !redis.HasErrorPrefix(execErr, "NOSCRIPT") {
			return nil, errors.E(op, errors.Unknown, execErr)
		}
		for j, q := range batch {
			ok, err := exists[j].Result()
			if err != nil {
				return nil, errors.E(op, errors.Unknown, err)
			}
			if !ok {
				return nil, errors.E(op, errors.NotFound, &errors.QueueNotFoundError{Queue: q})
			}
		}
		retry := r.client.Pipeline()
		retryNeeded := false
		for j := range calls {
			c := &calls[j]
			if redis.HasErrorPrefix(c.cmd.Err(), "NOSCRIPT") {
				c.cmd = c.script.Eval(ctx, retry, c.keys, c.args...)
				retryNeeded = true
			}
		}
		if retryNeeded {
			countRoundTrip(ctx)
			if _, err := retry.Exec(ctx); err != nil {
				return nil, errors.E(op, errors.Unknown, err)
			}
		}
		for j, q := range batch {
			index := positions[j]
			data, err := calls[index].cmd.Result()
			if err != nil {
				return nil, errors.E(op, errors.Unknown, err)
			}
			stats, err := r.decodeCurrentStats(q, now, data)
			if err != nil {
				return nil, err
			}
			if value, cached := memory[q]; cached {
				stats.MemoryUsage = value
			} else {
				value, err := calls[index+1].cmd.Int64()
				if err != nil {
					return nil, fmt.Errorf("queue %q memory usage: %w", q, err)
				}
				stats.MemoryUsage = value
			}
			result = append(result, stats)
		}
	}
	return result, nil
}
