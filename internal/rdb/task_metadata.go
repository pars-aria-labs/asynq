package rdb

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/pars-aria-labs/asynq/internal/base"
	"github.com/redis/go-redis/v9"
)

// Metadata is stored in the task hash, so every task deletion removes its
// history and progress. Secondary indexes share the task's Cluster hash tag.
const taskMetadataLua = `
local function metadata_string(key, field)
  local result = redis.pcall("HGET", key, field)
  if type(result) == "string" then return result end
  return nil
end
local function clear_error_index(key)
  local idx = metadata_string(key, "_error_index")
  if idx then
    local id = metadata_string(key, "_task_id")
    if id then redis.pcall("ZREM", idx, id) end
    if redis.pcall("ZCARD", idx) == 0 then
      local registry = metadata_string(key, "_error_registry")
      local member = metadata_string(key, "_error_member")
      if registry and member then redis.pcall("ZREM", registry, member) end
      redis.pcall("DEL", idx)
    end
    redis.pcall("HDEL", key, "_error_index", "_error_registry", "_error_member")
  end
end
local function delete_task(key)
  local raw = metadata_string(key, "_label_indexes")
  if raw then
    local id = metadata_string(key, "_task_id")
    local valid, indexes = pcall(cjson.decode, raw)
    if valid and type(indexes) == "table" and id then
      for _, idx in ipairs(indexes) do
        if type(idx) == "string" then
          redis.pcall("ZREM", idx, id)
          if redis.pcall("ZCARD", idx) == 0 then redis.pcall("DEL", idx) end
        end
      end
    end
  end
  clear_error_index(key)
  return redis.call("DEL", key)
end
local function set_labels(key, id, indexes)
  if not indexes or indexes == "" or indexes == "[]" then return end
  redis.call("HSET", key, "_label_indexes", indexes, "_task_id", id)
  for _, idx in ipairs(cjson.decode(indexes)) do redis.call("ZADD", idx, 0, id) end
end
`

func metadataDigest(key, value string) string {
	sum := sha1.Sum([]byte(key + "\x00" + value))
	return hex.EncodeToString(sum[:])
}

func (r *RDB) labelIndex(queue, key, value string) string {
	return base.QueueKeyPrefixWithPrefix(r.prefix, queue) + "labels:" + metadataDigest(key, value)
}

func (r *RDB) labelIndexes(msg *base.TaskMessage) string {
	labels := base.TaskLabels(msg.Headers)
	indexes := make([]string, 0, len(labels))
	for key, value := range labels {
		indexes = append(indexes, r.labelIndex(msg.Queue, key, value))
	}
	sort.Strings(indexes)
	data, _ := json.Marshal(indexes)
	return string(data)
}

var beginAttemptCmd = redis.NewScript(taskMetadataLua + `
if redis.call("HGET", KEYS[1], "state") ~= "active" or
   redis.call("HGET", KEYS[1], "_attempt_token") ~= ARGV[1] then return 0 end
if redis.call("HGET", KEYS[1], "_attempt_open") == ARGV[1] then return 1 end
local raw = redis.call("HGET", KEYS[1], "_history")
local history = raw and cjson.decode(raw) or {}
if #history > 0 and history[#history].status == "running" then
  history[#history].status = "interrupted"
  history[#history].error_code = "interrupted"
  history[#history].error = "The previous execution ended without a recorded outcome."
end
local entry = cjson.decode(ARGV[2])
entry.number = redis.call("HINCRBY", KEYS[1], "_attempt_sequence", 1)
table.insert(history, entry)
while #history > 50 do table.remove(history, 1) end
clear_error_index(KEYS[1])
redis.call("HSET", KEYS[1], "_history", cjson.encode(history), "_attempt_open", ARGV[1], "_task_id", ARGV[3])
redis.call("HDEL", KEYS[1], "_progress", "_progress_at", "_error_code")
return 1
`)

// BeginTaskAttempt is fenced by the token assigned atomically at dequeue.
func (r *RDB) BeginTaskAttempt(ctx context.Context, queue, id, token string, entry []byte) error {
	return beginAttemptCmd.Run(ctx, r.client, []string{base.TaskKeyWithPrefix(r.prefix, queue, id)}, token, entry, id).Err()
}

var finishAttemptCmd = redis.NewScript(taskMetadataLua + `
if redis.call("HGET", KEYS[1], "state") ~= "active" or
   redis.call("HGET", KEYS[1], "_attempt_token") ~= ARGV[1] or
   redis.call("HGET", KEYS[1], "_attempt_open") ~= ARGV[1] then return 0 end
local history = cjson.decode(redis.call("HGET", KEYS[1], "_history") or "[]")
if #history == 0 or history[#history].attempt_id ~= ARGV[1] then return 0 end
local entry = cjson.decode(ARGV[2])
entry.number = history[#history].number
history[#history] = entry
redis.call("HSET", KEYS[1], "_history", cjson.encode(history), "_error_code", ARGV[3])
redis.call("HDEL", KEYS[1], "_attempt_open")
clear_error_index(KEYS[1])
if ARGV[3] ~= "" then
  redis.call("ZADD", KEYS[2], 0, ARGV[5])
  redis.call("ZADD", KEYS[3], 0, ARGV[4])
  redis.call("HSET", KEYS[1], "_error_index", KEYS[2], "_error_registry", KEYS[3], "_error_member", ARGV[4])
end
return 1
`)

func (r *RDB) FinishTaskAttempt(ctx context.Context, queue, id, token, taskType, code string, entry []byte) error {
	member, _ := json.Marshal(struct {
		Type string `json:"type"`
		Code string `json:"code"`
	}{taskType, code})
	prefix := base.QueueKeyPrefixWithPrefix(r.prefix, queue)
	return finishAttemptCmd.Run(ctx, r.client, []string{
		base.TaskKeyWithPrefix(r.prefix, queue, id),
		prefix + "errors:" + metadataDigest(taskType, code), prefix + "errors",
	}, token, entry, code, string(member), id).Err()
}

var reportProgressCmd = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= "active" or
   redis.call("HGET", KEYS[1], "_attempt_token") ~= ARGV[1] or
   redis.call("HGET", KEYS[1], "_attempt_open") ~= ARGV[1] then return -1 end
local previous = redis.call("HGET", KEYS[1], "_progress")
local next = cjson.decode(ARGV[2])
if previous then
  local old = cjson.decode(previous)
  if old.stage == next.stage and next.completed < old.completed then return -2 end
end
local last = tonumber(redis.call("HGET", KEYS[1], "_progress_at") or "0")
if tonumber(ARGV[3]) - last < 1000 and next.completed < next.total then return 0 end
redis.call("HSET", KEYS[1], "_progress", ARGV[2], "_progress_at", ARGV[3])
return 1
`)

func (r *RDB) ReportTaskProgress(ctx context.Context, queue, id, token string, data []byte) error {
	n, err := reportProgressCmd.Run(ctx, r.client, []string{base.TaskKeyWithPrefix(r.prefix, queue, id)}, token, data, time.Now().UnixMilli()).Int()
	if err != nil {
		return err
	}
	if n == -1 {
		return fmt.Errorf("task execution is no longer active")
	}
	if n == -2 {
		return fmt.Errorf("completed work cannot decrease within the same stage")
	}
	return nil
}

func (r *RDB) TaskExecution(ctx context.Context, queue, id string) (map[string]string, error) {
	return r.client.HGetAll(ctx, base.TaskKeyWithPrefix(r.prefix, queue, id)).Result()
}

// SearchLabelIDs scans only one label index, never the task queue itself.
func (r *RDB) SearchLabelIDs(ctx context.Context, queue, key, value, after string, limit int) ([]string, error) {
	min := "-"
	if after != "" {
		min = "(" + after
	}
	return r.client.ZRangeByLex(ctx, r.labelIndex(queue, key, value), &redis.ZRangeBy{Min: min, Max: "+", Count: int64(limit)}).Result()
}

func (r *RDB) FailureGroupMembers(ctx context.Context, queue, after string, limit int) ([]string, error) {
	min := "-"
	if after != "" {
		min = "(" + after
	}
	return r.client.ZRangeByLex(ctx, base.QueueKeyPrefixWithPrefix(r.prefix, queue)+"errors", &redis.ZRangeBy{Min: min, Max: "+", Count: int64(limit)}).Result()
}

func (r *RDB) FailureGroupIDs(ctx context.Context, queue, taskType, code, after string, limit int) ([]string, int64, error) {
	key := base.QueueKeyPrefixWithPrefix(r.prefix, queue) + "errors:" + metadataDigest(taskType, code)
	min := "-"
	if after != "" {
		min = "(" + after
	}
	pipe := r.client.Pipeline()
	ids := pipe.ZRangeByLex(ctx, key, &redis.ZRangeBy{Min: min, Max: "+", Count: int64(limit)})
	count := pipe.ZCard(ctx, key)
	_, err := pipe.Exec(ctx)
	return ids.Val(), count.Val(), err
}

var recordAuditCmd = redis.NewScript(`
redis.call("LPUSH", KEYS[1], ARGV[1])
redis.call("LTRIM", KEYS[1], 0, 999)
redis.call("EXPIRE", KEYS[1], 7776000)
return 1
`)

func (r *RDB) RecordAudit(ctx context.Context, data []byte) error {
	return r.runMutationScript(ctx, recordAuditCmd, []string{base.AllQueuesKey(r.prefix) + ":audit"}, data).Err()
}

func (r *RDB) ListAudit(ctx context.Context, offset, limit int) ([]string, error) {
	return r.client.LRange(ctx, base.AllQueuesKey(r.prefix)+":audit", int64(offset), int64(offset+limit-1)).Result()
}
