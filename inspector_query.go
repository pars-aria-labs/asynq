package asynq

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/pars-aria-labs/asynq/internal/base"
	"github.com/pars-aria-labs/asynq/internal/rdb"
)

// TaskTimeField selects the timestamp used by a TaskTimeRange.
type TaskTimeField string

const (
	TaskTimeNextProcess TaskTimeField = "next_process_at"
	TaskTimeLastFailure TaskTimeField = "last_failed_at"
	TaskTimeCompletion  TaskTimeField = "completed_at"
)

// TaskTimeRange is a half-open interval [From, Before). A zero endpoint is
// unbounded. Tasks with a zero value for the selected field do not match a
// bounded interval.
type TaskTimeRange struct {
	Field  TaskTimeField
	From   time.Time
	Before time.Time
}

// TaskFilter selects tasks by exact type, last-error substring, and an
// explicit timestamp range. Empty fields impose no restriction.
type TaskFilter struct {
	Types         []string
	ErrorContains string
	Time          *TaskTimeRange
}

// TaskQuery describes one bounded page read. Page and PageSize default to 1
// and 30. PageSize cannot exceed MaxInspectorBatchSize. Group is required for
// TaskStateAggregating and ignored for other states.
type TaskQuery struct {
	Queue    string
	State    TaskState
	Group    string
	Page     int
	PageSize int
	Filter   TaskFilter
}

// TaskQueryResult contains the matching tasks and the number inspected before
// applying filters. Filters are evaluated against this page, not against an
// unbounded scan of the entire queue.
type TaskQueryResult struct {
	Tasks    []*TaskInfo
	Scanned  int
	Page     int
	PageSize int
}

// QueryTasks reads and filters one bounded page of tasks. Offset pagination is
// not a point-in-time snapshot: concurrent producers and workers can move page
// boundaries, causing IDs to be missed or repeated. Quiesce the selected state
// when exhaustive selection is required, de-duplicate collected IDs, and pass
// explicit batches to ProcessTaskIDs.
func (i *Inspector) QueryTasks(ctx context.Context, query TaskQuery) (TaskQueryResult, error) {
	page, pageSize, err := validateTaskQuery(query)
	if err != nil {
		return TaskQueryResult{}, err
	}
	view := i.WithContext(ctx)
	opts := []ListOption{Page(page), PageSize(pageSize)}
	var tasks []*TaskInfo
	switch query.State {
	case TaskStateActive:
		tasks, err = view.ListActiveTasks(query.Queue, opts...)
	case TaskStatePending:
		tasks, err = view.ListPendingTasks(query.Queue, opts...)
	case TaskStateScheduled:
		tasks, err = view.ListScheduledTasks(query.Queue, opts...)
	case TaskStateRetry:
		tasks, err = view.ListRetryTasks(query.Queue, opts...)
	case TaskStateArchived:
		tasks, err = view.ListArchivedTasks(query.Queue, opts...)
	case TaskStateCompleted:
		tasks, err = view.ListCompletedTasks(query.Queue, opts...)
	case TaskStateAggregating:
		tasks, err = view.ListAggregatingTasks(query.Queue, query.Group, opts...)
	default:
		return TaskQueryResult{}, fmt.Errorf("unsupported task state %d", query.State)
	}
	if err != nil {
		return TaskQueryResult{}, err
	}
	result := TaskQueryResult{Scanned: len(tasks), Page: page, PageSize: pageSize}
	for _, task := range tasks {
		if taskMatchesFilter(task, query.Filter) {
			result.Tasks = append(result.Tasks, task)
		}
	}
	return result, nil
}

func validateTaskQuery(query TaskQuery) (page, pageSize int, err error) {
	if err := base.ValidateQueueName(query.Queue); err != nil {
		return 0, 0, err
	}
	page = query.Page
	if page == 0 {
		page = 1
	}
	if page < 1 {
		return 0, 0, fmt.Errorf("page must be at least 1")
	}
	pageSize = query.PageSize
	if pageSize == 0 {
		pageSize = defaultPageSize
	}
	if pageSize < 1 || pageSize > MaxInspectorBatchSize {
		return 0, 0, fmt.Errorf("page size must be between 1 and %d", MaxInspectorBatchSize)
	}
	if int64(page) > math.MaxInt64/int64(pageSize) {
		return 0, 0, fmt.Errorf("page and page size exceed the supported pagination range")
	}
	if query.State == TaskStateAggregating && query.Group == "" {
		return 0, 0, fmt.Errorf("group is required for aggregating tasks")
	}
	if query.Filter.Time != nil {
		timeRange := query.Filter.Time
		switch timeRange.Field {
		case TaskTimeNextProcess, TaskTimeLastFailure, TaskTimeCompletion:
		default:
			return 0, 0, fmt.Errorf("unsupported task time field %q", timeRange.Field)
		}
		if !timeRange.From.IsZero() && !timeRange.Before.IsZero() && !timeRange.From.Before(timeRange.Before) {
			return 0, 0, fmt.Errorf("time range From must be before Before")
		}
	}
	return page, pageSize, nil
}

func taskMatchesFilter(task *TaskInfo, filter TaskFilter) bool {
	if len(filter.Types) > 0 {
		matched := false
		for _, taskType := range filter.Types {
			if task.Type == taskType {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if filter.ErrorContains != "" && !strings.Contains(task.LastErr, filter.ErrorContains) {
		return false
	}
	if filter.Time == nil {
		return true
	}
	var timestamp time.Time
	switch filter.Time.Field {
	case TaskTimeNextProcess:
		timestamp = task.NextProcessAt
	case TaskTimeLastFailure:
		timestamp = task.LastFailedAt
	case TaskTimeCompletion:
		timestamp = task.CompletedAt
	}
	if timestamp.IsZero() && (!filter.Time.From.IsZero() || !filter.Time.Before.IsZero()) {
		return false
	}
	if !filter.Time.From.IsZero() && timestamp.Before(filter.Time.From) {
		return false
	}
	if !filter.Time.Before.IsZero() && !timestamp.Before(filter.Time.Before) {
		return false
	}
	return true
}

// ProcessTaskIDs applies delete, run, or archive to at most 500 explicit task
// unique IDs. Each task transition is atomic; the whole slice is not a
// transaction. Processing stops at the first error and returns the number of
// transitions whose replies were received successfully. That count is a
// confirmed lower bound: a lost Redis reply can leave the failing transition's
// outcome ambiguous even though transport retries are disabled.
func (i *Inspector) ProcessTaskIDs(ctx context.Context, queue, action string, ids []string) (processed int, err error) {
	started := time.Now()
	ctx, roundTrips := rdb.WithRoundTripCounter(ctx)
	defer func() {
		i.observeOperation(InspectorOperation{
			Name:            InspectorOperationTaskIDs,
			Action:          action,
			Requested:       len(ids),
			Processed:       processed,
			Batches:         1,
			RedisRoundTrips: roundTrips(),
			StartedAt:       started,
			Duration:        time.Since(started),
			Err:             err,
		})
	}()
	if err := base.ValidateQueueName(queue); err != nil {
		return 0, err
	}
	if len(ids) > MaxInspectorBatchSize {
		return 0, fmt.Errorf("task ID count must not exceed %d", MaxInspectorBatchSize)
	}
	if action != "delete" && action != "run" && action != "archive" {
		return 0, fmt.Errorf("unsupported task action %q", action)
	}
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id == "" {
			return processed, fmt.Errorf("task ID must not be empty")
		}
		if _, duplicate := seen[id]; duplicate {
			return processed, fmt.Errorf("duplicate task ID %q", id)
		}
		seen[id] = struct{}{}
	}
	view := i.WithContext(ctx)
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return processed, err
		}
		var taskErr error
		switch action {
		case "delete":
			taskErr = view.DeleteTask(queue, id)
		case "run":
			taskErr = view.RunTask(queue, id)
		case "archive":
			taskErr = view.ArchiveTask(queue, id)
		}
		if taskErr != nil {
			return processed, taskErr
		}
		processed++
	}
	return processed, nil
}
