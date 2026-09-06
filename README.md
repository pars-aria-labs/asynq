<img src="https://user-images.githubusercontent.com/11155743/114697792-ffbfa580-9d26-11eb-8e5b-33bef69476dc.png" alt="Asynq logo" width="360">

# Asynq

A simple, reliable, and efficient Redis-backed distributed task queue for Go.

[![Go Reference](https://pkg.go.dev/badge/github.com/pars-aria-labs/asynq.svg)](https://pkg.go.dev/github.com/pars-aria-labs/asynq)
[![Go Report Card](https://goreportcard.com/badge/github.com/pars-aria-labs/asynq)](https://goreportcard.com/report/github.com/pars-aria-labs/asynq)
[![Build](https://github.com/pars-aria-labs/asynq/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/pars-aria-labs/asynq/actions/workflows/build.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Clients enqueue tasks, servers process them concurrently, and Redis coordinates
delivery, scheduling, retries, and recovery across processes and machines.

## Source and fork provenance

- Original upstream: [`hibiken/asynq`](https://github.com/hibiken/asynq).
- Intermediate fork: [`parsidev/asynq`](https://github.com/parsidev/asynq).
- Fork base: tag [`v0.26.0-parsidev-02`](https://github.com/parsidev/asynq/tree/v0.26.0-parsidev-02),
  commit [`2f4fd0a`](https://github.com/parsidev/asynq/commit/2f4fd0a).
- Canonical module maintained here: `github.com/pars-aria-labs/asynq`.

This is an independently maintained fork and is **not an official upstream
Asynq release**. The project remains MIT-licensed; original copyright notices,
license text, Git history, and attribution are retained. Links marked
"upstream wiki" lead to the original project's documentation and may not cover
fork-specific APIs.

See the [migration guide](docs/migrating-to-pars-aria-labs.md) and
[fork release notes](docs/release-notes-v0.27.0.md) for compatibility details.

## What this fork adds

The core enqueue and worker model remains familiar, with additional operational
safety and observability around Inspector workflows:

- **Redis namespace isolation:** Inspector reads and mutations consistently
  honor `RedisClientOpt.Prefix`, including queue, task, group, worker, server,
  and scheduler metadata.
- **Bounded administration:** atomic Inspector task mutations are limited to
  500 source-state transitions. Archive actions have a separate bounded budget
  of 500 retention evictions. These mutating Lua commands disable ambiguous
  transport retries while retaining safe `NOSCRIPT` recovery and Redis Cluster
  redirection. Deletion also cleans unique-task locks and aggregation metadata.
- **Controlled multi-batch work:** `TaskBatchPolicy` adds a fixed work budget,
  whole-operation timeout, and context-aware delay between batches.
- **Lower read amplification:** queue snapshots are pipelined in groups of at
  most 100 queues, while server, worker, and scheduler metadata reads are also
  pipelined. Only approximate memory samples have an optional short-lived
  cache. Exact aggregation statistics still scale with the number of groups
  registered for each queue.
- **Cancellation and explicit selection:** `Inspector.WithContext` makes legacy
  methods cancellable. `QueryTasks` filters one bounded page by type, last-error
  substring, or time range, and `ProcessTaskIDs` mutates an explicit list of at
  most 500 IDs.
- **Low-cardinality telemetry:** an Inspector operation observer reports
  duration, outcome, confirmed work, and instrumented Redis client executions.
  The optional `x/metrics` collector exports these observations to Prometheus
  without queue, task, group, or raw-error labels.
- **Operational verification:** CI covers race-enabled standalone Redis and a
  three-node Redis Cluster, plus the companion
  [Asynqmon checkout](https://github.com/pars-aria-labs/asynqmon). A separate
  [opt-in soak harness](docs/inspector-soak.md) exercises concurrent producers,
  bounded mutations, pipelined reads, cancellation, and repeated script-cache
  flushes.

The detailed contracts and supported state/action combinations are documented
in [Bounded Inspector operations](docs/batch-inspector.md).

## Core capabilities

- At-least-once task execution
- Immediate, delayed, and periodic scheduling
- Automatic retry and worker-crash recovery
- Weighted and strict-priority queues
- Per-task timeout, deadline, retention, and uniqueness
- Task aggregation
- Middleware-based handlers
- Queue pause/resume, Inspector APIs, CLI, Web UI, and Prometheus integration
- Direct Redis, Sentinel, and Redis Cluster connection options

The original project remains a useful reference for
[retries (upstream wiki)](https://github.com/hibiken/asynq/wiki/Task-Retry),
[queue priority (upstream wiki)](https://github.com/hibiken/asynq/wiki/Queue-Priority),
[unique tasks (upstream wiki)](https://github.com/hibiken/asynq/wiki/Unique-Tasks),
[timeouts (upstream wiki)](https://github.com/hibiken/asynq/wiki/Task-Timeout-and-Cancelation),
[aggregation (upstream wiki)](https://github.com/hibiken/asynq/wiki/Task-aggregation),
and [handler middleware (upstream wiki)](https://github.com/hibiken/asynq/wiki/Handler-Deep-Dive).

## Install or migrate

The module currently targets Go 1.25.

```sh
go get github.com/pars-aria-labs/asynq@v0.27.0
```

The import-path migration is source-breaking, but the exported package name
remains `asynq`:

```diff
-import "github.com/hibiken/asynq"
+import "github.com/pars-aria-labs/asynq"
```

Update optional submodules in the same way, then run `go mod tidy` and your test
suite. No Redis data rewrite is required solely because of the Go module rename;
keep the Redis endpoint, database, and prefix unchanged. See
[Migrating to the Pars Aria Labs module](docs/migrating-to-pars-aria-labs.md)
before upgrading a production deployment.

## Quickstart

Use the same Redis settings for producers, workers, schedulers, and Inspectors.
A non-empty prefix stores keys under `<Prefix>:asynq:*`, allowing independent
deployments to share a Redis database without sharing Asynq state.

### Enqueue a task

```go
package main

import (
	"encoding/json"
	"log"

	"github.com/pars-aria-labs/asynq"
)

func main() {
	redisOpt := asynq.RedisClientOpt{
		Addr:   "127.0.0.1:6379",
		Prefix: "billing-prod",
	}
	client := asynq.NewClient(redisOpt)
	defer client.Close()

	payload, err := json.Marshal(struct {
		UserID int `json:"user_id"`
	}{UserID: 42})
	if err != nil {
		log.Fatal(err)
	}

	info, err := client.Enqueue(
		asynq.NewTask("email:welcome", payload),
		asynq.Queue("critical"),
		asynq.MaxRetry(5),
	)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("enqueued task id=%s queue=%s", info.ID, info.Queue)
}
```

### Run a worker

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/pars-aria-labs/asynq"
)

func main() {
	redisOpt := asynq.RedisClientOpt{
		Addr:   "127.0.0.1:6379",
		Prefix: "billing-prod", // Must match the producer.
	}
	server := asynq.NewServer(redisOpt, asynq.Config{
		Concurrency: 10,
		Queues: map[string]int{
			"critical": 6,
			"default":  3,
			"low":      1,
		},
	})

	mux := asynq.NewServeMux()
	mux.HandleFunc("email:welcome", func(ctx context.Context, task *asynq.Task) error {
		var payload struct {
			UserID int `json:"user_id"`
		}
		if err := json.Unmarshal(task.Payload(), &payload); err != nil {
			return fmt.Errorf("decode payload: %v: %w", err, asynq.SkipRetry)
		}
		log.Printf("send welcome email to user %d", payload.UserID)
		return nil
	})

	if err := server.Run(mux); err != nil {
		log.Fatal(err)
	}
}
```

The original [Getting Started guide (upstream wiki)](https://github.com/hibiken/asynq/wiki/Getting-Started)
covers the core producer/worker model. Use this README and the local `docs/`
directory for fork-specific behavior.

## Bounded Inspector examples

Create the Inspector with the exact Redis configuration used by the workload:

```go
redisOpt := asynq.RedisClientOpt{
	Addr:   "127.0.0.1:6379",
	Prefix: "billing-prod",
}
inspector := asynq.NewInspector(redisOpt)
defer inspector.Close()
```

Queue names must not begin with `}`, and the first `{...}` pair in `Prefix`,
if any, must not be empty. These constraints keep all keys for one queue in the
same Redis Cluster hash slot and turn a potential `CROSSSLOT` failure into an
early configuration error.

Legacy methods can be cancelled without changing their signatures:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

queues, err := inspector.WithContext(ctx).Queues()
```

### Apply a batch policy

`ProcessTaskBatches` repeatedly executes individually atomic, bounded batches.
A positive `MaxTasks` is a hard cap; zero freezes a budget from the source
cardinality observed after the first batch.

```go
result, err := inspector.ProcessTaskBatches(
	ctx,
	"critical",
	"scheduled",
	"run",
	"", // Group is required only for aggregating tasks.
	asynq.TaskBatchPolicy{
		BatchSize:       200,
		MaxTasks:        1_000,
		Timeout:         30 * time.Second,
		InterBatchDelay: 25 * time.Millisecond,
	},
)
if err != nil {
	// Preserve result.Processed; do not blindly retry an ambiguous mutation.
	log.Printf("batch stopped after %d tasks: %v", result.Processed, err)
}
```

### Filter, collect, then mutate explicit IDs

`QueryTasks` filters after reading one bounded page; it does not scan an
unbounded queue. Offset pages are not a point-in-time snapshot: producers and
workers can shift page boundaries, causing duplicates or omissions. For an
exhaustive selection, quiesce the selected state while collecting IDs; always
de-duplicate the collected IDs before mutation.

```go
page, err := inspector.QueryTasks(ctx, asynq.TaskQuery{
	Queue:    "critical",
	State:    asynq.TaskStateRetry,
	Page:     1,
	PageSize: 100,
	Filter: asynq.TaskFilter{
		Types:         []string{"email:welcome"},
		ErrorContains: "timeout",
		Time: &asynq.TaskTimeRange{
			Field:  asynq.TaskTimeLastFailure,
			From:   time.Now().Add(-24 * time.Hour),
			Before: time.Now(),
		},
	},
})
if err != nil {
	return err
}

ids := make([]string, 0, len(page.Tasks))
for _, task := range page.Tasks {
	ids = append(ids, task.ID)
}
processed, err := inspector.ProcessTaskIDs(ctx, "critical", "archive", ids)
```

`ProcessTaskIDs` accepts at most 500 unique IDs and stops at the first error.
Its processed count covers replies received successfully and is therefore a
confirmed lower bound: if a Redis reply is lost, the failing ID may already
have changed. Do not blindly retry the failed suffix. For larger selections,
collect and de-duplicate all IDs before submitting chunks of at most
`asynq.MaxInspectorBatchSize`.

### Export Inspector telemetry to Prometheus

The operation collector is both a Prometheus collector and an
`asynq.InspectorOperationObserver`:

```go
import (
	asynqmetrics "github.com/pars-aria-labs/asynq/x/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

operationMetrics := asynqmetrics.NewInspectorMetricsCollector()
prometheus.MustRegister(operationMetrics)
inspector.SetOperationObserver(operationMetrics)

// Optional queue gauges and counters; this collector uses pipelined reads.
prometheus.MustRegister(asynqmetrics.NewQueueMetricsCollector(inspector))
```

Expose the registry using your normal Prometheus HTTP handler. Observer
callbacks run synchronously and can arrive concurrently, so custom observers
must be concurrency-safe, should return quickly, and must not call back into
the same Inspector. The `redis_round_trips_total` metric counts instrumented
client executions and pipeline flush attempts; go-redis retries or cluster
fan-out can involve additional physical network exchanges.

## Web UI

The companion [Asynqmon fork](https://github.com/pars-aria-labs/asynqmon) provides a
browser UI for queue and task inspection. This repository's CI exercises it
against the local `asynq` and `x` modules. See [the local integration notes](dev/README.md)
for workspace and smoke-test instructions.

## Command-line tool

Install the CLI from the canonical module:

```sh
go install github.com/pars-aria-labs/asynq/tools/asynq@latest
```

Run `asynq dash` for the terminal dashboard. See the
[CLI documentation](tools/asynq/README.md) for commands and connection flags.

## Stability and compatibility

The module is still pre-v1, so public APIs may change before `v1.0.0`.
The current CI target is Go 1.25 with Redis 7.4. Standalone and Redis Cluster
paths are tested separately; because Asynq relies on Lua scripts, validate your
specific topology and upgrade against staging data before production rollout.

Redis keys and serialized task messages remain compatible with the fork base
when endpoint, database, and prefix are unchanged. No automatic data migration
is required for the module-path change.

## Contributing

Issues and pull requests are welcome. Review the
[contribution guide](CONTRIBUTING.md) and search the
[current issue tracker](https://github.com/pars-aria-labs/asynq/issues) first.

## License and attribution

Asynq is available under the [MIT License](LICENSE). The original work is
copyright 2019-present Ken Hibino and
[upstream contributors](https://github.com/hibiken/asynq/graphs/contributors).
Original notices and history are preserved. The logo was created by
[Vic Shóstak](https://github.com/koddr) and released under
[CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/).
