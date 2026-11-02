package asynq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
	"unicode/utf8"

	"github.com/pars-aria-labs/asynq/internal/base"
)

// TaskAttempt describes one handler invocation. Number counts invocations,
// independently of the retry counter (which IsFailure may leave unchanged).
type TaskAttempt struct {
	AttemptID  string     `json:"attempt_id"`
	Number     int        `json:"number"`
	Worker     string     `json:"worker"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	DurationMS int64      `json:"duration_ms"`
	Status     string     `json:"status"`
	Error      string     `json:"error,omitempty"`
	ErrorCode  string     `json:"error_code,omitempty"`
}

// TaskProgress is a snapshot belonging to one invocation, reset on each retry.
type TaskProgress struct {
	AttemptID string    `json:"attempt_id"`
	Completed int64     `json:"completed"`
	Total     int64     `json:"total"`
	Stage     string    `json:"stage"`
	UpdatedAt time.Time `json:"updated_at"`
}

type TaskReference struct {
	Queue string `json:"queue"`
	ID    string `json:"id"`
}

// TaskExecution is available while the task hash is retained. The newest 50
// attempts are kept; successfully processed tasks need a Retention option.
type TaskExecution struct {
	Attempts  []TaskAttempt     `json:"attempts"`
	Progress  *TaskProgress     `json:"progress,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
	Parent    *TaskReference    `json:"parent,omitempty"`
	ErrorCode string            `json:"error_code,omitempty"`
}

// Label attaches an indexed, exact-match business label. At most 16 labels are
// allowed, with keys of 1..64 ASCII bytes and values of at most 256 UTF-8 bytes.
func Label(key, value string) Option { return Header(base.LabelHeaderPrefix+key, value) }

func (t *Task) Labels() map[string]string { return base.TaskLabels(t.headers) }

// CodedError optionally supplies a stable error class for failure grouping.
// Wrapped errors implementing this interface are recognized through errors.As.
type CodedError interface{ ErrorCode() string }

type classifiedError struct {
	code  string
	cause error
}

func (e *classifiedError) Error() string     { return e.cause.Error() }
func (e *classifiedError) Unwrap() error     { return e.cause }
func (e *classifiedError) ErrorCode() string { return e.code }

// WithErrorCode preserves the cause (including SkipRetry) and adds a stable
// code. Invalid codes are classified as "unclassified" when recorded.
func WithErrorCode(code string, cause error) error {
	if cause == nil {
		return nil
	}
	return &classifiedError{code: code, cause: cause}
}

func taskErrorCode(err error) string {
	if err == nil {
		return ""
	}
	var coded CodedError
	if errors.As(err, &coded) {
		code := coded.ErrorCode()
		if len(code) > 0 && len(code) <= 128 && utf8.ValidString(code) {
			return code
		}
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, ErrLeaseExpired):
		return "lease_expired"
	default:
		return "unclassified"
	}
}

type executionStore interface {
	BeginTaskAttempt(context.Context, string, string, string, []byte) error
	FinishTaskAttempt(context.Context, string, string, string, string, string, []byte) error
	ReportTaskProgress(context.Context, string, string, string, []byte) error
}
type progressContextKey struct{}
type progressReporter struct {
	store            executionStore
	queue, id, token string
}

// ReportProgress reports completed/total units and a stage name from a handler's
// context. Writes are throttled to once per second per attempt in Redis, with a
// final completed==total update always allowed. Counts must not decrease within
// a stage. Late writes from canceled or superseded attempts are rejected.
func ReportProgress(ctx context.Context, completed, total int64, stage string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if total <= 0 || total > 1<<53 || completed < 0 || completed > total || len(stage) > 128 || !utf8.ValidString(stage) {
		return fmt.Errorf("progress requires 0 <= completed <= total <= 2^53, total > 0, and a UTF-8 stage of at most 128 bytes")
	}
	reporter, ok := ctx.Value(progressContextKey{}).(*progressReporter)
	if !ok {
		return fmt.Errorf("progress can only be reported from an Asynq handler context")
	}
	data, _ := json.Marshal(TaskProgress{AttemptID: reporter.token, Completed: completed, Total: total, Stage: stage, UpdatedAt: time.Now().UTC()})
	return reporter.store.ReportTaskProgress(ctx, reporter.queue, reporter.id, reporter.token, data)
}

func (p *processor) beginAttempt(ctx context.Context, msg *base.TaskMessage) (context.Context, *TaskAttempt) {
	store, ok := p.broker.(executionStore)
	if !ok || msg.AttemptToken == "" {
		return ctx, nil
	}
	host, _ := os.Hostname()
	entry := &TaskAttempt{AttemptID: msg.AttemptToken, Worker: fmt.Sprintf("%s:%d", host, os.Getpid()), StartedAt: time.Now().UTC(), Status: "running"}
	data, _ := json.Marshal(entry)
	writeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	err := store.BeginTaskAttempt(writeCtx, msg.Queue, msg.ID, msg.AttemptToken, data)
	cancel()
	if err != nil {
		p.logger.Warnf("Could not record task attempt id=%s: %v", msg.ID, err)
		return ctx, nil
	}
	return context.WithValue(ctx, progressContextKey{}, &progressReporter{store: store, queue: msg.Queue, id: msg.ID, token: msg.AttemptToken}), entry
}

func (p *processor) finishAttempt(msg *base.TaskMessage, entry *TaskAttempt, err error, interrupted bool) {
	if entry == nil {
		return
	}
	now := time.Now().UTC()
	entry.FinishedAt = &now
	entry.DurationMS = now.Sub(entry.StartedAt).Milliseconds()
	entry.Status = "succeeded"
	entry.ErrorCode = taskErrorCode(err)
	if err != nil {
		entry.Status = "failed"
		entry.Error = err.Error()
		if len(entry.Error) > 4096 {
			entry.Error = string([]rune(entry.Error[:4096]))
		}
		if errors.Is(err, context.DeadlineExceeded) {
			entry.Status = "timeout"
		}
		if errors.Is(err, context.Canceled) {
			entry.Status = "canceled"
		}
	}
	if interrupted {
		entry.Status = "interrupted"
		entry.ErrorCode = "interrupted"
	}
	data, _ := json.Marshal(entry)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if e := p.broker.(executionStore).FinishTaskAttempt(ctx, msg.Queue, msg.ID, msg.AttemptToken, msg.Type, entry.ErrorCode, data); e != nil {
		p.logger.Warnf("Could not record task outcome id=%s: %v", msg.ID, e)
	}
}

func (i *Inspector) GetTaskExecution(ctx context.Context, queue, id string) (*TaskExecution, error) {
	if err := base.ValidateQueueName(queue); err != nil {
		return nil, err
	}
	if id == "" {
		return nil, fmt.Errorf("task ID cannot be empty")
	}
	// One atomic hash read keeps labels, provenance, history and progress from
	// the same task incarnation, even if a custom ID is deleted and reused.
	fields, err := i.rdb.TaskExecution(ctx, queue, id)
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, ErrTaskNotFound
	}
	message, err := base.DecodeMessage([]byte(fields["msg"]))
	if err != nil {
		return nil, fmt.Errorf("decode task metadata: %w", err)
	}
	out := &TaskExecution{Attempts: []TaskAttempt{}, Labels: base.TaskLabels(message.Headers), ErrorCode: fields["_error_code"]}
	if raw := fields["_history"]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &out.Attempts); err != nil {
			return nil, fmt.Errorf("decode task history: %w", err)
		}
	}
	if raw := fields["_progress"]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &out.Progress); err != nil {
			return nil, fmt.Errorf("decode task progress: %w", err)
		}
	}
	if parentID := message.Headers["asynq.replay.id"]; parentID != "" {
		out.Parent = &TaskReference{Queue: message.Headers["asynq.replay.queue"], ID: parentID}
	}
	return out, nil
}
