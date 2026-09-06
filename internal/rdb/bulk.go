package rdb

import (
	"context"
	"fmt"

	"github.com/pars-aria-labs/asynq/internal/base"
	"github.com/pars-aria-labs/asynq/internal/errors"
	"github.com/redis/go-redis/v9"
)

// BulkBatchSize caps source-state transitions in one mutation script. Archive
// operations may additionally evict up to BulkBatchSize expired or excess
// archive entries as an independent retention-maintenance budget.
const BulkBatchSize = 500

// noRetryCmd prevents go-redis from replaying a mutating Lua command after an
// ambiguous transport failure. Redis Cluster may still follow MOVED/ASK
// redirects, which are safe because the command was rejected by the old node.
type noRetryCmd struct {
	*redis.Cmd
}

func (*noRetryCmd) NoRetry() bool { return true }

func (cmd *noRetryCmd) Clone() redis.Cmder {
	return &noRetryCmd{Cmd: cmd.Cmd.Clone().(*redis.Cmd)}
}

type noRetryScripter struct {
	redis.UniversalClient
}

func (c noRetryScripter) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	return c.eval(ctx, "eval", script, keys, args...)
}

func (c noRetryScripter) EvalSha(ctx context.Context, hash string, keys []string, args ...any) *redis.Cmd {
	cmd := c.eval(ctx, "evalsha", hash, keys, args...)
	if redis.HasErrorPrefix(cmd.Err(), "NOSCRIPT") {
		cmd.SetErr(redis.ErrNoScript)
	}
	return cmd
}

func (c noRetryScripter) eval(ctx context.Context, name, payload string, keys []string, args ...any) *redis.Cmd {
	commandArgs := make([]any, 3+len(keys), 3+len(keys)+len(args))
	commandArgs[0] = name
	commandArgs[1] = payload
	commandArgs[2] = len(keys)
	for j, key := range keys {
		commandArgs[3+j] = key
	}
	commandArgs = append(commandArgs, args...)
	cmd := redis.NewCmd(ctx, commandArgs...)
	if len(keys) > 0 {
		cmd.SetFirstKeyPos(3)
	}
	countRoundTrip(ctx)
	_ = c.Process(ctx, &noRetryCmd{Cmd: cmd})
	return cmd
}

func (r *RDB) runMutationScript(ctx context.Context, script *redis.Script, keys []string, args ...any) *redis.Cmd {
	return script.Run(ctx, noRetryScripter{UniversalClient: r.client}, keys, args...)
}

func (r *RDB) bulkSize(ctx context.Context, script *redis.Script, key string) (int64, error) {
	countRoundTrip(ctx)
	if script == archiveAllPendingCmd || script == deleteAllPendingCmd {
		return r.client.LLen(ctx, key).Result()
	}
	return r.client.ZCard(ctx, key).Result()
}

// Each batch is atomic, but the whole operation is not. Bound work by the
// initial cardinality so concurrent producers cannot keep this call alive.
// If a later batch fails, the returned count includes all earlier successful
// batches.
func (r *RDB) runBulkScript(ctx context.Context, script *redis.Script, keys []string, args ...any) (int64, error) {
	budget, err := r.bulkSize(ctx, script, keys[0])
	if err != nil {
		return 0, err
	}
	if budget == 0 {
		n, err := r.runMutationScript(ctx, script, keys, append(args, int64(0))...).Int64()
		if err != nil {
			return 0, err
		}
		if n != 0 {
			return 0, fmt.Errorf("invalid bulk result for empty source: %d", n)
		}
		return 0, nil
	}
	var total int64
	for total < budget {
		limit := min(int64(BulkBatchSize), budget-total)
		n, err := r.runMutationScript(ctx, script, keys, append(args, limit)...).Int64()
		if err != nil {
			return total, err
		}
		if n < 0 || n > limit {
			return total, fmt.Errorf("invalid bulk result: %d", n)
		}
		total += n
		if n < limit {
			break
		}
	}
	return total, nil
}

// TaskBatch performs one bounded batch. Remaining is a live count, not a
// snapshot. Callers should cap their total work to avoid chasing producers.
func (r *RDB) TaskBatch(ctx context.Context, queue, state, action, group string, limit int) (int64, int64, error) {
	if limit < 1 || limit > BulkBatchSize {
		return 0, 0, fmt.Errorf("batch size must be between 1 and %d", BulkBatchSize)
	}
	if err := base.ValidateQueueName(queue); err != nil {
		return 0, 0, err
	}
	var source string
	switch state {
	case "pending":
		source = base.PendingKeyWithPrefix(r.prefix, queue)
	case "scheduled":
		source = base.ScheduledKeyWithPrefix(r.prefix, queue)
	case "retry":
		source = base.RetryKeyWithPrefix(r.prefix, queue)
	case "archived":
		source = base.ArchivedKeyWithPrefix(r.prefix, queue)
	case "completed":
		source = base.CompletedKeyWithPrefix(r.prefix, queue)
	case "aggregating":
		if group == "" {
			return 0, 0, fmt.Errorf("group is required")
		}
		source = base.GroupKeyWithPrefix(r.prefix, queue, group)
	default:
		return 0, 0, fmt.Errorf("unsupported task state %q", state)
	}
	taskPrefix := base.TaskKeyPrefixWithPrefix(r.prefix, queue)
	keys := []string{source}
	args := []any{taskPrefix}
	var script *redis.Script
	switch action {
	case "delete":
		script = deleteAllCmd
		if state == "pending" {
			script = deleteAllPendingCmd
		}
		if state == "aggregating" {
			script = deleteAllAggregatingCmd
			keys = append(keys, base.AllGroupsWithPrefix(r.prefix, queue))
			args = append(args, group)
		}
	case "run":
		if state == "pending" || state == "completed" {
			return 0, 0, fmt.Errorf("cannot run %s tasks", state)
		}
		script = runAllCmd
		keys = append(keys, base.PendingKeyWithPrefix(r.prefix, queue))
		if state == "aggregating" {
			script = runAllAggregatingCmd
			keys = append(keys, base.AllGroupsWithPrefix(r.prefix, queue))
			args = append(args, group)
		}
	case "archive":
		if state == "archived" || state == "completed" {
			return 0, 0, fmt.Errorf("cannot archive %s tasks", state)
		}
		script = archiveAllCmd
		keys = append(keys, base.ArchivedKeyWithPrefix(r.prefix, queue))
		now := r.clock.Now()
		args = []any{now.Unix(), now.AddDate(0, 0, -archivedExpirationInDays).Unix(), maxArchiveSize, taskPrefix}
		if state == "pending" {
			script = archiveAllPendingCmd
		}
		if state == "aggregating" {
			script = archiveAllAggregatingCmd
			keys = append(keys, base.AllGroupsWithPrefix(r.prefix, queue))
			args = append(args, group)
		}
		args = append(args, BulkBatchSize)
	default:
		return 0, 0, fmt.Errorf("unsupported task action %q", action)
	}
	countRoundTrip(ctx)
	exists, err := r.client.SIsMember(ctx, base.AllQueuesKey(r.prefix), queue).Result()
	if err != nil {
		return 0, 0, err
	}
	if !exists {
		return 0, 0, errors.E(errors.NotFound, &errors.QueueNotFoundError{Queue: queue})
	}
	n, err := r.runMutationScript(ctx, script, keys, append(args, limit)...).Int64()
	if err != nil {
		return 0, 0, err
	}
	if n < 0 || n > int64(limit) {
		return 0, 0, fmt.Errorf("invalid bulk result: %d", n)
	}
	remaining, err := r.bulkSize(ctx, script, source)
	return n, remaining, err
}
