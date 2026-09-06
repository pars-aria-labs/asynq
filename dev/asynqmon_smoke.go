//go:build ignore

// Run explicitly with the sibling workspace; see dev/README.md.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"time"

	"github.com/pars-aria-labs/asynq"
	"github.com/pars-aria-labs/asynqmon"
)

func main() {
	if err := check(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("asynqmon HTTP compatibility: queues, tasks, bounded batches, pause/resume, archive/run/delete passed")
}

func check() error {
	addr := os.Getenv("ASYNQ_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	opt := asynq.RedisClientOpt{Addr: addr, DB: 13, Prefix: fmt.Sprintf("compat-%d", time.Now().UnixNano())}
	client := asynq.NewClient(opt)
	defer client.Close()
	inspector := asynq.NewInspector(opt)
	defer inspector.Close()
	defer inspector.DeleteQueue("default", true)
	defer inspector.DeleteAllArchivedTasks("default")
	defer inspector.DeleteAllScheduledTasks("default")
	defer inspector.DeleteAllPendingTasks("default")
	handler := asynqmon.New(asynqmon.Options{RedisConnOpt: opt})
	defer handler.Close()
	if _, err := client.Enqueue(asynq.NewTask("compat:pending", nil)); err != nil {
		return err
	}
	const scheduledCount = 501
	tasks := make([]*asynq.Task, scheduledCount)
	for j := range tasks {
		tasks[j] = asynq.NewTask("compat:scheduled", nil)
	}
	for j, result := range client.BatchEnqueueContext(context.Background(), tasks, asynq.ProcessIn(time.Hour)) {
		if result.Err != nil {
			return fmt.Errorf("enqueue scheduled task %d: %w", j, result.Err)
		}
	}
	request := func(method, path string) (map[string]any, error) {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(method, path, nil))
		if w.Code < 200 || w.Code >= 300 {
			return nil, fmt.Errorf("%s %s: status %d: %s", method, path, w.Code, w.Body.String())
		}
		var payload map[string]any
		if w.Body.Len() > 0 {
			if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
				return nil, err
			}
		}
		return payload, nil
	}
	for _, path := range []string{"/api/queues", "/api/queues/default", "/api/queues/default/pending_tasks", "/api/queues/default/scheduled_tasks"} {
		if _, err := request("GET", path); err != nil {
			return err
		}
	}
	data, err := request("GET", "/api/queues/default")
	if err != nil {
		return err
	}
	stats, ok := data["current"].(map[string]any)
	if !ok || stats["pending"] != float64(1) || stats["scheduled"] != float64(scheduledCount) {
		return fmt.Errorf("unexpected queue stats: %v", data)
	}
	for _, action := range []string{"pause", "resume"} {
		if _, err := request("POST", "/api/queues/default:"+action); err != nil {
			return err
		}
		info, err := inspector.GetQueueInfo("default")
		if err != nil {
			return err
		}
		if info.Paused != (action == "pause") {
			return fmt.Errorf("%s did not change queue state", action)
		}
	}
	data, err = request("POST", "/api/queues/default/scheduled_tasks:archive_all?batch_size=500")
	if err != nil {
		return err
	}
	if data["archived"] != float64(500) || data["remaining"] != float64(1) {
		return fmt.Errorf("unexpected first batch response: %v", data)
	}
	data, err = request("POST", "/api/queues/default/scheduled_tasks:archive_all?batch_size=500")
	if err != nil {
		return err
	}
	if data["archived"] != float64(1) || data["remaining"] != float64(0) {
		return fmt.Errorf("unexpected final batch response: %v", data)
	}
	if _, err := request("POST", "/api/queues/default/archived_tasks:run_all"); err != nil {
		return err
	}
	info, err := inspector.GetQueueInfo("default")
	if err != nil {
		return err
	}
	if info.Pending != scheduledCount+1 || info.Scheduled != 0 || info.Archived != 0 {
		return fmt.Errorf("unexpected state after archive/run: %+v", info)
	}
	if _, err := request("DELETE", "/api/queues/default/pending_tasks:delete_all"); err != nil {
		return err
	}
	info, err = inspector.GetQueueInfo("default")
	if err != nil {
		return err
	}
	if info.Size != 0 {
		return fmt.Errorf("delete left %d tasks", info.Size)
	}
	return nil
}
