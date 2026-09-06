package rdb

import (
	"context"
	stderrors "errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/pars-aria-labs/asynq/internal/base"
	h "github.com/pars-aria-labs/asynq/internal/testutil"
	"github.com/pars-aria-labs/asynq/internal/timeutil"
	"github.com/redis/go-redis/v9"
)

func TestParseInfoPreservesColons(t *testing.T) {
	got, err := parseInfo("# Server\r\nredis_version:8.0\r\nexecutable:/usr/bin/redis:server\r\nmaster_host:::1\r\nempty:\r\ninvalid\r\n# ignored:comment\r\n")
	want := map[string]string{"redis_version": "8.0", "executable": "/usr/bin/redis:server", "master_host": "::1", "empty": ""}
	if err != nil || !cmp.Equal(got, want) {
		t.Fatalf("parseInfo = %v, %v; want %v", got, err, want)
	}
}

// Exercise the same queue name in two namespaces, including mutations used by
// asynqmon. A monitor must never read or mutate another namespace's queues.
func TestInspectorNamespaceIsolation(t *testing.T) {
	plain := setup(t)
	defer plain.Close()
	custom := NewRDB(plain.client, "tenant")
	ctx := context.Background()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, r := range []*RDB{plain, custom} {
		must(r.Enqueue(ctx, h.NewTaskMessage("pending", nil)))
		must(r.Schedule(ctx, h.NewTaskMessage("scheduled", nil), time.Now().Add(time.Hour)))
		must(r.AddToGroup(ctx, h.NewTaskMessage("grouped", nil), "group"))
	}
	for _, r := range []*RDB{plain, custom} {
		stats, err := r.CurrentStats("default")
		must(err)
		if stats.Pending != 1 || stats.Scheduled != 1 || stats.Aggregating != 1 || stats.Size != 3 || stats.MemoryUsage <= 0 {
			t.Fatalf("prefix %q: unexpected stats: %+v", r.prefix, stats)
		}
		tasks, err := r.ListScheduled("default", Pagination{Size: 10})
		must(err)
		if len(tasks) != 1 || tasks[0].Message.Type != "scheduled" {
			t.Fatalf("prefix %q: scheduled tasks = %v", r.prefix, tasks)
		}
		groups, err := r.GroupStats("default")
		must(err)
		if len(groups) != 1 || groups[0].Size != 1 {
			t.Fatalf("prefix %q: groups = %v", r.prefix, groups)
		}
	}
	must(custom.Pause("default"))
	stats, err := plain.CurrentStats("default")
	must(err)
	if stats.Paused {
		t.Fatal("custom pause affected default namespace")
	}
	stats, err = custom.CurrentStats("default")
	must(err)
	if !stats.Paused {
		t.Fatal("custom queue was not paused")
	}
	must(custom.Unpause("default"))
	n, err := custom.ArchiveAllScheduledTasks("default")
	must(err)
	if n != 1 {
		t.Fatalf("archived %d tasks, want 1", n)
	}
	tasks, err := custom.ListArchived("default", Pagination{Size: 10})
	must(err)
	if len(tasks) != 1 || tasks[0].Message.Type != "scheduled" {
		t.Fatalf("archived tasks = %v", tasks)
	}
	n, err = custom.RunAllArchivedTasks("default")
	must(err)
	if n != 1 {
		t.Fatalf("ran %d tasks, want 1", n)
	}
	n, err = custom.RunAllAggregatingTasks("default", "group")
	must(err)
	if n != 1 {
		t.Fatalf("ran %d grouped tasks, want 1", n)
	}
	msg, _, err := custom.Dequeue("default")
	must(err)
	if msg == nil {
		t.Fatal("no task dequeued")
	}
	active, err := custom.ListActive("default", Pagination{Size: 10})
	must(err)
	if len(active) != 1 || active[0].Message.ID != msg.ID {
		t.Fatalf("active tasks = %v", active)
	}
	must(custom.Done(ctx, msg))
	history, err := custom.HistoricalStats("default", 1)
	must(err)
	if history[0].Processed != 1 {
		t.Fatalf("custom processed = %d, want 1", history[0].Processed)
	}
	history, err = plain.HistoricalStats("default", 1)
	must(err)
	if history[0].Processed != 0 {
		t.Fatal("custom processing affected default history")
	}
	must(custom.RemoveQueue("default", true))
	stats, err = plain.CurrentStats("default")
	must(err)
	if stats.Pending != 1 || stats.Scheduled != 1 || stats.Aggregating != 1 {
		t.Fatalf("custom operations affected default namespace: %+v", stats)
	}
}

func TestAggregationLifecycleHonorsNamespacePrefix(t *testing.T) {
	root := setup(t)
	defer root.Close()
	r := NewRDB(root.client, "tenant")
	ctx := context.Background()
	now := time.Now().Truncate(time.Second)
	r.SetClock(timeutil.NewSimulatedClock(now))

	seedGroupTask := func(group string) *base.TaskMessage {
		t.Helper()
		msg := h.NewTaskMessageBuilder().SetQueue("default").SetGroup(group).Build()
		if err := r.client.HSet(ctx, base.TaskKeyWithPrefix(r.prefix, msg.Queue, msg.ID), map[string]any{
			"msg": h.MustMarshal(t, msg), "state": "aggregating", "group": group,
		}).Err(); err != nil {
			t.Fatal(err)
		}
		if err := r.client.ZAdd(ctx, base.GroupKeyWithPrefix(r.prefix, msg.Queue, group), redis.Z{
			Member: msg.ID, Score: float64(now.Unix()),
		}).Err(); err != nil {
			t.Fatal(err)
		}
		if err := r.client.SAdd(ctx, base.AllGroupsWithPrefix(r.prefix, msg.Queue), group).Err(); err != nil {
			t.Fatal(err)
		}
		return msg
	}

	deleteMsg := seedGroupTask("delete-group")
	deleteSetID, err := r.AggregationCheck("default", "delete-group", now, time.Minute, time.Minute, 1)
	if err != nil || deleteSetID == "" {
		t.Fatalf("AggregationCheck(delete) = (%q, %v)", deleteSetID, err)
	}
	msgs, _, err := r.ReadAggregationSet("default", "delete-group", deleteSetID)
	if err != nil || len(msgs) != 1 || msgs[0].ID != deleteMsg.ID {
		t.Fatalf("ReadAggregationSet() = (%v, %v), want task %q", msgs, err, deleteMsg.ID)
	}
	if err := r.DeleteAggregationSet(ctx, "default", "delete-group", deleteSetID); err != nil {
		t.Fatal(err)
	}
	deleteSetKey := base.AggregationSetKeyWithPrefix(r.prefix, "default", "delete-group", deleteSetID)
	if n := r.client.Exists(ctx, deleteSetKey, base.TaskKeyWithPrefix(r.prefix, "default", deleteMsg.ID)).Val(); n != 0 {
		t.Fatalf("DeleteAggregationSet left %d namespaced keys", n)
	}

	reclaimMsg := seedGroupTask("reclaim-group")
	reclaimSetID, err := r.AggregationCheck("default", "reclaim-group", now, time.Minute, time.Minute, 1)
	if err != nil || reclaimSetID == "" {
		t.Fatalf("AggregationCheck(reclaim) = (%q, %v)", reclaimSetID, err)
	}
	r.SetClock(timeutil.NewSimulatedClock(now.Add(aggregationTimeout + time.Second)))
	if err := r.ReclaimStaleAggregationSets("default"); err != nil {
		t.Fatal(err)
	}
	prefixedGroup := base.GroupKeyWithPrefix(r.prefix, "default", "reclaim-group")
	if score, err := r.client.ZScore(ctx, prefixedGroup, reclaimMsg.ID).Result(); err != nil || int64(score) != now.Unix() {
		t.Fatalf("reclaimed task score = (%v, %v), want %d", score, err, now.Unix())
	}
	if n := r.client.Exists(ctx, base.AggregationSetKey("default", "reclaim-group", reclaimSetID), base.GroupKey("default", "reclaim-group")).Val(); n != 0 {
		t.Fatalf("aggregation lifecycle leaked %d unprefixed keys", n)
	}
}

func TestForwardGroupedTaskHonorsNamespacePrefix(t *testing.T) {
	root := setup(t)
	defer root.Close()
	r := NewRDB(root.client, "tenant")
	ctx := context.Background()
	now := time.Now().Truncate(time.Second)
	r.SetClock(timeutil.NewSimulatedClock(now))
	msg := h.NewTaskMessageBuilder().SetQueue("default").SetGroup("group").Build()
	if err := r.Schedule(ctx, msg, now.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := r.ForwardIfReady("default"); err != nil {
		t.Fatal(err)
	}
	if score, err := r.client.ZScore(ctx, base.GroupKeyWithPrefix(r.prefix, "default", "group"), msg.ID).Result(); err != nil || int64(score) != now.Unix() {
		t.Fatalf("prefixed group score = (%v, %v), want %d", score, err, now.Unix())
	}
	if n := r.client.ZCard(ctx, base.GroupKey("default", "group")).Val(); n != 0 {
		t.Fatalf("unprefixed group contains %d tasks", n)
	}
}

func TestUniqueTaskModesHonorNamespacePrefix(t *testing.T) {
	tests := []struct {
		name    string
		enqueue func(*RDB, context.Context, *base.TaskMessage) error
	}{
		{name: "pending", enqueue: func(r *RDB, ctx context.Context, msg *base.TaskMessage) error {
			return r.EnqueueUnique(ctx, msg, time.Hour)
		}},
		{name: "scheduled", enqueue: func(r *RDB, ctx context.Context, msg *base.TaskMessage) error {
			return r.ScheduleUnique(ctx, msg, time.Now().Add(time.Hour), time.Hour)
		}},
		{name: "aggregating", enqueue: func(r *RDB, ctx context.Context, msg *base.TaskMessage) error {
			return r.AddToGroupUnique(ctx, msg, "group", time.Hour)
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := setup(t)
			defer root.Close()
			r := NewRDB(root.client, "tenant")
			ctx := context.Background()
			msg := h.NewTaskMessage("unique", []byte("payload"))
			msg.UniqueKey = base.UniqueKey(msg.Queue, msg.Type, msg.Payload)
			if err := tc.enqueue(r, ctx, msg); err != nil {
				t.Fatal(err)
			}

			prefixedUnique := base.UniqueKeyWithPrefix(r.prefix, msg.Queue, msg.Type, msg.Payload)
			if got := r.client.Get(ctx, prefixedUnique).Val(); got != msg.ID {
				t.Fatalf("prefixed unique lock = %q, want task ID %q", got, msg.ID)
			}
			if got := r.client.Exists(ctx, base.UniqueKey(msg.Queue, msg.Type, msg.Payload)).Val(); got != 0 {
				t.Fatal("unique task created an unprefixed lock")
			}
			taskKey := base.TaskKeyWithPrefix(r.prefix, msg.Queue, msg.ID)
			if got := r.client.HGet(ctx, taskKey, "unique_key").Val(); got != prefixedUnique {
				t.Fatalf("task hash unique_key = %q, want %q", got, prefixedUnique)
			}
			stored := h.MustUnmarshal(t, r.client.HGet(ctx, taskKey, "msg").Val())
			if stored.UniqueKey != prefixedUnique {
				t.Fatalf("encoded message unique key = %q, want %q", stored.UniqueKey, prefixedUnique)
			}
		})
	}
}

func TestInspectorPipelineSkipsBadRecords(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	server := &base.ServerInfo{Host: "healthy", PID: 1, ServerID: "ok", Queues: map[string]int{}}
	worker := &base.WorkerInfo{ID: "task", Host: "healthy", PID: 1, ServerID: "ok"}
	entry := &base.SchedulerEntry{Spec: "@every 1m", Type: "healthy"}
	must(r.WriteServerState(server, []*base.WorkerInfo{worker}, time.Minute))
	must(r.WriteSchedulerEntries("healthy", []*base.SchedulerEntry{entry}, time.Minute))
	for _, registry := range []string{base.AllServersKey(r.prefix), base.AllWorkersKey(r.prefix), base.AllSchedulersKey(r.prefix)} {
		for _, kind := range []string{"missing", "wrongtype", "invalid"} {
			key := registry + ":" + kind
			must(r.client.ZAdd(ctx, registry, redis.Z{Score: float64(time.Now().Add(time.Minute).Unix()), Member: key}).Err())
			if kind == "wrongtype" {
				must(r.client.SAdd(ctx, key, "bad").Err())
			}
			if kind == "invalid" {
				switch registry {
				case base.AllServersKey(r.prefix):
					must(r.client.Set(ctx, key, "\xff", 0).Err())
				case base.AllWorkersKey(r.prefix):
					must(r.client.HSet(ctx, key, "bad", "\xff").Err())
				case base.AllSchedulersKey(r.prefix):
					must(r.client.RPush(ctx, key, "\xff").Err())
				}
			}
		}
	}
	servers, err := r.ListServers()
	must(err)
	if diff := cmp.Diff([]*base.ServerInfo{server}, servers); diff != "" {
		t.Fatal(diff)
	}
	workers, err := r.ListWorkers()
	must(err)
	if diff := cmp.Diff([]*base.WorkerInfo{worker}, workers); diff != "" {
		t.Fatal(diff)
	}
	entries, err := r.ListSchedulerEntries()
	must(err)
	if diff := cmp.Diff([]*base.SchedulerEntry{entry}, entries); diff != "" {
		t.Fatal(diff)
	}
}

func TestInspectorRegistryListsReturnPipelineErrors(t *testing.T) {
	tests := []struct {
		name string
		seed func(*RDB) error
		list func(*RDB) error
	}{
		{
			name: "servers",
			seed: func(r *RDB) error {
				return r.WriteServerState(&base.ServerInfo{Host: "host", PID: 1, ServerID: "server", Queues: map[string]int{}}, nil, time.Minute)
			},
			list: func(r *RDB) error { _, err := r.ListServers(); return err },
		},
		{
			name: "workers",
			seed: func(r *RDB) error {
				return r.WriteServerState(
					&base.ServerInfo{Host: "host", PID: 1, ServerID: "server", Queues: map[string]int{}},
					[]*base.WorkerInfo{{ID: "worker", Host: "host", PID: 1, ServerID: "server"}},
					time.Minute,
				)
			},
			list: func(r *RDB) error { _, err := r.ListWorkers(); return err },
		},
		{
			name: "scheduler entries",
			seed: func(r *RDB) error {
				return r.WriteSchedulerEntries("scheduler", []*base.SchedulerEntry{{Spec: "@every 1m", Type: "task"}}, time.Minute)
			},
			list: func(r *RDB) error { _, err := r.ListSchedulerEntries(); return err },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := setup(t)
			defer r.Close()
			if err := tc.seed(r); err != nil {
				t.Fatal(err)
			}
			injected := stderrors.New("injected registry pipeline failure")
			r.client.AddHook(&testRedisHook{processPipeline: func(context.Context, []redis.Cmder, redis.ProcessPipelineHook) error {
				return injected
			}})

			if err := tc.list(r); !stderrors.Is(err, injected) {
				t.Fatalf("list error = %v, want %v", err, injected)
			}
		})
	}
}

func BenchmarkInspectorListServers(b *testing.B) {
	for _, count := range []int{1, 100} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			r := setup(b)
			defer r.Close()
			for i := 0; i < count; i++ {
				if err := r.WriteServerState(&base.ServerInfo{Host: "bench", PID: i, ServerID: fmt.Sprint(i)}, nil, time.Hour); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				servers, err := r.ListServers()
				if err != nil || len(servers) != count {
					b.Fatalf("ListServers: count=%d, err=%v", len(servers), err)
				}
			}
		})
	}
}
