package asynq_test

import (
	"context"
	"time"

	"github.com/pars-aria-labs/asynq"
)

// These method expressions make accidental signature changes fail at compile
// time. They are deliberately kept in an external package to verify the public
// surface seen by downstream users.
var (
	_ func(*asynq.Inspector, context.Context, string, string, string, string, int) (int, int, error)                                = (*asynq.Inspector).ProcessTaskBatch
	_ func(*asynq.Inspector, context.Context, []string, time.Duration) ([]*asynq.QueueInfo, error)                                  = (*asynq.Inspector).GetQueueInfoBatch
	_ func(*asynq.Inspector, context.Context, string, string, string, string, asynq.TaskBatchPolicy) (asynq.TaskBatchResult, error) = (*asynq.Inspector).ProcessTaskBatches
	_ func(*asynq.Inspector, context.Context) *asynq.Inspector                                                                      = (*asynq.Inspector).WithContext
	_ func(*asynq.Inspector, context.Context, asynq.TaskQuery) (asynq.TaskQueryResult, error)                                       = (*asynq.Inspector).QueryTasks
	_ func(*asynq.Inspector, context.Context, string, string, []string) (int, error)                                                = (*asynq.Inspector).ProcessTaskIDs
	_ func(*asynq.Inspector, asynq.InspectorOperationObserver)                                                                      = (*asynq.Inspector).SetOperationObserver
)
