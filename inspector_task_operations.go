package asynq

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/pars-aria-labs/asynq/internal/base"
)

// TaskSearchQuery uses an exact label index as its entry point. All labels must
// match. A page inspects at most Limit candidate IDs, even if other filters
// reject them. Continue with Cursor until it is empty, including empty pages.
type TaskSearchQuery struct {
	Labels map[string]string
	Queues []string
	Type   string
	State  TaskState
	Limit  int
	Cursor string
}

type TaskSearchResult struct {
	Tasks   []*TaskInfo `json:"tasks"`
	Cursor  string      `json:"cursor"`
	Scanned int         `json:"scanned"`
}

type searchCursor struct{ Queue, ID, Query string }

func (i *Inspector) SearchTasks(ctx context.Context, query TaskSearchQuery) (TaskSearchResult, error) {
	result := TaskSearchResult{Tasks: []*TaskInfo{}}
	if len(query.Labels) == 0 {
		return result, fmt.Errorf("at least one exact label is required")
	}
	if err := base.ValidateLabels(query.Labels); err != nil {
		return result, err
	}
	if query.Limit == 0 {
		query.Limit = 50
	}
	if query.Limit < 1 || query.Limit > MaxInspectorBatchSize {
		return result, fmt.Errorf("limit must be between 1 and %d", MaxInspectorBatchSize)
	}
	if query.State < 0 || query.State > TaskStateAggregating {
		return result, fmt.Errorf("invalid state")
	}
	queues := append([]string(nil), query.Queues...)
	if len(queues) == 0 {
		var err error
		queues, err = i.WithContext(ctx).Queues()
		if err != nil {
			return result, err
		}
	}
	if len(queues) > 100 {
		return result, fmt.Errorf("search supports at most 100 queues; select an explicit subset")
	}
	sort.Strings(queues)
	queues = uniqueStrings(queues)
	for _, q := range queues {
		if err := base.ValidateQueueName(q); err != nil {
			return result, err
		}
	}
	keys := make([]string, 0, len(query.Labels))
	for k := range query.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fingerprintData, _ := json.Marshal(struct {
		Labels map[string]string
		Queues []string
		Type   string
		State  TaskState
	}{query.Labels, queues, query.Type, query.State})
	sum := sha256.Sum256(fingerprintData)
	fingerprint := hex.EncodeToString(sum[:])
	var cursor searchCursor
	if query.Cursor != "" {
		if len(query.Cursor) > 16384 {
			return result, fmt.Errorf("cursor is too long")
		}
		raw, err := base64.RawURLEncoding.DecodeString(query.Cursor)
		if err != nil || json.Unmarshal(raw, &cursor) != nil || cursor.Query != fingerprint {
			return result, fmt.Errorf("invalid cursor for this search")
		}
	}
	for _, queue := range queues {
		if queue < cursor.Queue {
			continue
		}
		after := ""
		if queue == cursor.Queue {
			after = cursor.ID
		}
		ids, err := i.rdb.SearchLabelIDs(ctx, queue, keys[0], query.Labels[keys[0]], after, query.Limit-result.Scanned+1)
		if err != nil {
			return result, err
		}
		for _, id := range ids {
			if result.Scanned == query.Limit {
				// Cursor already points to the final inspected task, not this lookahead.
				return result, nil
			}
			result.Scanned++
			position, _ := json.Marshal(searchCursor{Queue: queue, ID: id, Query: fingerprint})
			result.Cursor = base64.RawURLEncoding.EncodeToString(position)
			info, err := i.WithContext(ctx).GetTaskInfo(queue, id)
			if errors.Is(err, ErrTaskNotFound) {
				continue
			}
			if err != nil {
				return result, err
			}
			if query.Type != "" && info.Type != query.Type || query.State != 0 && info.State != query.State {
				continue
			}
			labels := base.TaskLabels(info.Headers)
			match := true
			for k, v := range query.Labels {
				if actual, ok := labels[k]; !ok || actual != v {
					match = false
					break
				}
			}
			if match {
				result.Tasks = append(result.Tasks, info)
			}
		}
	}
	result.Cursor = ""
	return result, nil
}

func uniqueStrings(values []string) []string {
	out := values[:0]
	for _, value := range values {
		if len(out) == 0 || out[len(out)-1] != value {
			out = append(out, value)
		}
	}
	return out
}

// FailureGroup contains tasks whose latest recorded execution failed with the
// same type and stable error code. Starting another execution removes the task
// from its previous group. Historical failures remain in the task timeline.
type FailureGroup struct {
	Type  string `json:"type"`
	Code  string `json:"code"`
	Count int64  `json:"count"`
}
type FailureGroupPage struct {
	Groups []FailureGroup `json:"groups"`
	Cursor string         `json:"cursor"`
}

func (i *Inspector) ListFailureGroups(ctx context.Context, queue, cursor string, limit int) (FailureGroupPage, error) {
	out := FailureGroupPage{Groups: []FailureGroup{}}
	if err := base.ValidateQueueName(queue); err != nil {
		return out, err
	}
	if limit < 1 || limit > 100 || len(cursor) > 4096 {
		return out, fmt.Errorf("limit must be 1..100 and cursor at most 4096 bytes")
	}
	after := ""
	if cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil {
			return out, fmt.Errorf("invalid cursor")
		}
		after = string(raw)
	}
	members, err := i.rdb.FailureGroupMembers(ctx, queue, after, limit+1)
	if err != nil {
		return out, err
	}
	if len(members) > limit {
		out.Cursor = base64.RawURLEncoding.EncodeToString([]byte(members[limit-1]))
		members = members[:limit]
	}
	for _, member := range members {
		var group FailureGroup
		if err := json.Unmarshal([]byte(member), &group); err != nil {
			return out, err
		}
		_, count, err := i.rdb.FailureGroupIDs(ctx, queue, group.Type, group.Code, "", 1)
		if err != nil {
			return out, err
		}
		group.Count = count
		if count > 0 {
			out.Groups = append(out.Groups, group)
		}
	}
	return out, nil
}

// ListFailureGroupTasks returns a bounded live page. Counts and membership can
// change concurrently; replay uses explicit IDs frozen by the caller.
func (i *Inspector) ListFailureGroupTasks(ctx context.Context, queue, taskType, code, after string, limit int) ([]*TaskInfo, string, int64, error) {
	if err := base.ValidateQueueName(queue); err != nil {
		return nil, "", 0, err
	}
	if limit < 1 || limit > MaxInspectorBatchSize || taskType == "" || len(taskType) > 4096 || code == "" || len(code) > 128 || len(after) > 4096 {
		return nil, "", 0, fmt.Errorf("invalid group or page options")
	}
	ids, count, err := i.rdb.FailureGroupIDs(ctx, queue, taskType, code, after, limit+1)
	if err != nil {
		return nil, "", 0, err
	}
	next := ""
	if len(ids) > limit {
		next = ids[limit-1]
		ids = ids[:limit]
	}
	out := make([]*TaskInfo, 0, len(ids))
	for _, id := range ids {
		info, err := i.WithContext(ctx).GetTaskInfo(queue, id)
		if errors.Is(err, ErrTaskNotFound) {
			continue
		}
		if err != nil {
			return nil, "", 0, err
		}
		out = append(out, info)
	}
	return out, next, count, nil
}

// ReplayOptions creates a new task, leaving the source payload and history
// unchanged. Nil Payload preserves the original bytes; a non-nil empty slice
// explicitly clears them. Deadline, uniqueness and aggregation are not copied.
// RequestID makes repeat requests with the same queue/source ID idempotent
// while the resulting task exists. It is required for ReplayTasks.
type ReplayOptions struct {
	Payload   []byte
	MaxRetry  *int
	Delay     time.Duration
	RequestID string
}

func validateReplayOptions(options ReplayOptions) error {
	if options.Delay < 0 || options.Delay > 30*24*time.Hour || len(options.RequestID) > 128 {
		return fmt.Errorf("invalid replay delay or request ID")
	}
	if options.MaxRetry != nil && (*options.MaxRetry < 0 || *options.MaxRetry > 100) {
		return fmt.Errorf("max retry must be 0..100")
	}
	if len(options.Payload) > 1<<20 {
		return fmt.Errorf("replacement payload cannot exceed 1 MiB")
	}
	return nil
}

func (i *Inspector) ReplayTask(ctx context.Context, queue, id string, options ReplayOptions) (*TaskInfo, error) {
	if err := validateReplayOptions(options); err != nil {
		return nil, err
	}
	if err := base.ValidateQueueName(queue); err != nil {
		return nil, err
	}
	if id == "" {
		return nil, fmt.Errorf("source task ID cannot be empty")
	}
	requestData, _ := json.Marshal(options)
	requestHash := sha256.Sum256(requestData)
	fingerprint := hex.EncodeToString(requestHash[:])
	copyID := ""
	verifyCopy := func(info *TaskInfo) (*TaskInfo, error) {
		if info.Headers["asynq.replay.id"] != id || info.Headers["asynq.replay.queue"] != queue || info.Headers["asynq.replay.request"] != fingerprint {
			return nil, fmt.Errorf("replay request ID already used with different options")
		}
		return info, nil
	}
	if options.RequestID != "" {
		raw, _ := json.Marshal([]string{queue, id, options.RequestID})
		copyID = uuid.NewSHA1(uuid.NameSpaceURL, raw).String()
		existing, err := i.WithContext(ctx).GetTaskInfo(queue, copyID)
		if err == nil {
			return verifyCopy(existing)
		}
		if !errors.Is(err, ErrTaskNotFound) && !errors.Is(err, ErrQueueNotFound) {
			return nil, err
		}
	}
	original, err := i.WithContext(ctx).GetTaskInfo(queue, id)
	if err != nil {
		return nil, err
	}
	if original.State != TaskStateArchived && original.State != TaskStateRetry && original.State != TaskStateCompleted {
		return nil, fmt.Errorf("only archived, retry or completed tasks can be replayed")
	}
	headers := maps.Clone(original.Headers)
	if headers == nil {
		headers = make(map[string]string)
	}
	headers["asynq.replay.queue"], headers["asynq.replay.id"] = queue, id
	headers["asynq.replay.request"] = fingerprint
	data := original.Payload
	if options.Payload != nil {
		data = options.Payload
	}
	retries := original.MaxRetry
	if options.MaxRetry != nil {
		retries = *options.MaxRetry
	}
	opts := []Option{Queue(queue), MaxRetry(retries), Timeout(original.Timeout), Retention(original.Retention), ProcessIn(options.Delay)}
	if copyID != "" {
		opts = append(opts, TaskID(copyID))
	}
	client := &Client{broker: i.rdb, sharedConnection: true}
	info, err := client.EnqueueContext(ctx, NewTaskWithHeaders(original.Type, data, headers), opts...)
	if errors.Is(err, ErrTaskIDConflict) && options.RequestID != "" {
		// Retry of a committed enqueue: verify provenance before returning it.
		info, err = i.WithContext(ctx).GetTaskInfo(queue, copyID)
		if err == nil {
			return verifyCopy(info)
		}
	}
	return info, err
}

type ReplayResult struct {
	SourceID string `json:"source_id"`
	TaskID   string `json:"task_id,omitempty"`
	Error    string `json:"error,omitempty"`
}

// ReplayTasks schedules up to 100 explicit source IDs with increasing delays.
// Interval spaces eligibility times; it is not a hard distributed rate limit.
// Replies report each confirmed enqueue. A transport error may be ambiguous;
// retry with the same RequestID and selection to avoid duplicate clones.
func (i *Inspector) ReplayTasks(ctx context.Context, queue string, ids []string, options ReplayOptions, interval time.Duration) ([]ReplayResult, error) {
	if err := validateReplayOptions(options); err != nil {
		return nil, err
	}
	if len(ids) == 0 || len(ids) > 100 || options.RequestID == "" || interval < 0 || interval > time.Hour || options.Delay < 0 || options.Delay+time.Duration(len(ids)-1)*interval > 30*24*time.Hour {
		return nil, fmt.Errorf("replay requires 1..100 IDs, a request ID, and a valid interval (0..1h)")
	}
	seen := make(map[string]bool)
	for _, id := range ids {
		if id == "" || seen[id] {
			return nil, fmt.Errorf("IDs must be nonempty and unique")
		}
		seen[id] = true
	}
	out := make([]ReplayResult, 0, len(ids))
	for position, id := range ids {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		opt := options
		opt.Delay += time.Duration(position) * interval
		info, err := i.ReplayTask(ctx, queue, id, opt)
		result := ReplayResult{SourceID: id}
		if err != nil {
			result.Error = err.Error()
		} else {
			result.TaskID = info.ID
		}
		out = append(out, result)
	}
	return out, nil
}

// AuditEvent records operator intent and the observed HTTP outcome. TaskIDs are
// the explicit selection, not a claim that every selected task was changed.
// Payloads, credentials and authorization headers must never be included.
type AuditEvent struct {
	ID         string    `json:"id"`
	Actor      string    `json:"actor"`
	At         time.Time `json:"at"`
	Action     string    `json:"action"`
	Queue      string    `json:"queue,omitempty"`
	TaskIDs    []string  `json:"task_ids,omitempty"`
	StatusCode int       `json:"status_code"`
	Outcome    string    `json:"outcome"`
	Processed  *int      `json:"processed,omitempty"`
}

// RecordAuditEvent retains the newest 1000 records, expiring after 90 days of
// inactivity. Use a host application's audit sink for durable compliance logs.
func (i *Inspector) RecordAuditEvent(ctx context.Context, event AuditEvent) error {
	if len(event.ID) > 128 || len(event.Actor) > 256 || len(event.Action) > 2048 || len(event.Queue) > 1024 || len(event.TaskIDs) > 500 {
		return fmt.Errorf("audit event exceeds metadata limits")
	}
	for _, id := range event.TaskIDs {
		if len(id) > 4096 {
			return fmt.Errorf("audit task ID exceeds 4096 bytes")
		}
	}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return i.rdb.RecordAudit(ctx, data)
}

func (i *Inspector) ListAuditEvents(ctx context.Context, offset, limit int) ([]AuditEvent, error) {
	if offset < 0 || offset > 999 || limit < 1 || limit > 100 {
		return nil, fmt.Errorf("audit offset must be 0..999 and limit 1..100")
	}
	rows, err := i.rdb.ListAudit(ctx, offset, limit)
	if err != nil {
		return nil, err
	}
	out := make([]AuditEvent, 0, len(rows))
	for _, row := range rows {
		var event AuditEvent
		if err := json.Unmarshal([]byte(row), &event); err != nil {
			return nil, err
		}
		out = append(out, event)
	}
	return out, nil
}
