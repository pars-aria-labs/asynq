<img src="https://user-images.githubusercontent.com/11155743/114697792-ffbfa580-9d26-11eb-8e5b-33bef69476dc.png" alt="Asynq logo" width="360">

# Asynq

Asynq is a Redis-backed task queue for Go. An application puts work on a
queue, one or more workers process it in the background, and Redis keeps the
state that lets the system schedule, retry, recover, and distribute that work.

The API is deliberately small enough for a first background job and sturdy
enough for a service with several queues, priorities, workers, and deployment
namespaces.

[![Go Reference](https://pkg.go.dev/badge/github.com/pars-aria-labs/asynq.svg)](https://pkg.go.dev/github.com/pars-aria-labs/asynq)
[![Go Report Card](https://goreportcard.com/badge/github.com/pars-aria-labs/asynq)](https://goreportcard.com/report/github.com/pars-aria-labs/asynq)
[![Build](https://github.com/pars-aria-labs/asynq/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/pars-aria-labs/asynq/actions/workflows/build.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Where this repository came from

This repository is a fork of
[`https://github.com/hibiken/asynq`](https://github.com/hibiken/asynq).
The upstream Git history, MIT license, and original copyright notices are
preserved here.

Development of this fork is maintained at
[`github.com/pars-aria-labs/asynq`](https://github.com/pars-aria-labs/asynq).
This is an independently maintained fork, not an official release from the
upstream maintainers. Applications should use the canonical address above for
every import, module, issue, and pull request.

## What changed in this fork

The familiar client-and-worker model is still here. Most of our work has gone
into the rough edges that tend to appear when Asynq is operated in production:

- The root, `x`, and `tools` Go modules, generated metadata, examples, CI, and
  support links now share one canonical repository identity. The Go package
  name is still `asynq`.
- Redis prefixes are handled consistently by Inspectors, queue and task
  mutations, server and scheduler metadata, the CLI, and the Prometheus
  exporter. Separate deployments can safely share one Redis database without
  seeing each other's Asynq state.
- Inspector mutations are bounded. One atomic call changes at most 500 source
  tasks, archive cleanup has its own 500-entry budget, and deletion also
  removes unique locks and aggregation metadata.
- Administrative work can now be cancelled and paced with `WithContext`,
  `ProcessTaskBatches`, `QueryTasks`, and `ProcessTaskIDs`. Partial progress is
  returned explicitly instead of being hidden behind an all-or-nothing API.
- Queue snapshots and server, worker, and scheduler reads use pipelines.
  Queue batches are flushed in groups of at most 100. Only approximate memory
  samples may be cached for a short time; queue counts remain live.
- `x/metrics` includes low-cardinality Inspector telemetry suitable for
  Prometheus. Queue names, task IDs, group names, and raw error strings are not
  used as labels for operation telemetry.
- The CLI now carries the same Redis prefix through standalone and Cluster
  connections. The exporter gained prefix support and Inspector telemetry;
  the existing TLS and ACL options remain available. CI covers race-enabled
  standalone Redis, a real three-node Redis Cluster, and Asynqmon.
- Stable and beta releases are built by GitHub Actions for Linux, macOS, and
  Windows on both `amd64` and `arm64`. Every release includes SHA-256 checksums
  and GitHub build-provenance attestations.

## Features

- At-least-once task execution
- Immediate, delayed, and periodic scheduling
- Automatic retries and worker-crash recovery
- Weighted or strict-priority queues
- Per-task timeout, deadline, retention, and uniqueness
- Task aggregation and middleware-based handlers
- Direct Redis, Sentinel, and Redis Cluster connections
- Inspector APIs, a terminal CLI, a web UI, and Prometheus integration

## Requirements and installation

The current stable release is `v1.0.1` and requires Go 1.25. Install the root
module, and install the optional `x` module only if your application imports a
package below `x/`:

```sh
go get github.com/pars-aria-labs/asynq@v1.0.1
go get github.com/pars-aria-labs/asynq/x@v1.0.1 # optional
go mod tidy
```

Use this import path:

```go
import "github.com/pars-aria-labs/asynq"
```

Do not append `/v1`. Go keeps the original module path for major versions zero
and one; a suffix becomes necessary only for `v2` and later.

## Quick start

The producer and worker must use the same Redis address, database, and prefix.
The prefix in these examples keeps the application isolated from other Asynq
installations using the same Redis database.

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

Asynq uses at-least-once delivery, so a task can be delivered more than once.
Whenever possible, make handlers idempotent—for example, record a business
operation ID before sending an email or charging a payment.

![A task moving through an Asynq queue](docs/assets/task-queue.png)

## Redis namespaces and Cluster

With `Prefix: "billing-prod"`, keys are stored below
`billing-prod:asynq:*`. Use that exact prefix for every producer, worker,
scheduler, Inspector, CLI command, and metrics collector that belongs to the
same deployment. A mismatched prefix usually looks like an empty installation;
it does not mean the tasks disappeared.

Redis Cluster uses database zero and a seed list instead of one address:

```go
redisOpt := asynq.RedisClusterClientOpt{
	Addrs: []string{
		"redis-0:7000",
		"redis-1:7001",
		"redis-2:7002",
	},
	Prefix: "billing-prod",
}
```

Queue names must not begin with `}`, and the first `{...}` pair in a prefix
must not be empty. Those checks keep every multi-key queue operation in one
Redis Cluster hash slot and turn a later `CROSSSLOT` error into an early
configuration error.

## Safe Inspector operations

Create one Inspector from the same Redis options as the workload and close the
original Inspector when the process is done:

```go
inspector := asynq.NewInspector(redisOpt)
defer inspector.Close()

ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

queues, err := inspector.WithContext(ctx).Queues()
```

`WithContext` returns a lightweight view backed by the original connection.
Do not close that derived view; close the original Inspector.

`ProcessTaskBatch` changes no more than 500 tasks in one atomic call. Active
tasks are intentionally excluded because a worker owns their lease.

| Source state | Delete | Run now | Archive |
| --- | --- | --- | --- |
| `pending` | yes | no | yes |
| `scheduled` | yes | yes | yes |
| `retry` | yes | yes | yes |
| `archived` | yes | yes | no |
| `completed` | yes | no | no |
| `aggregating` | yes | yes | yes |

For longer jobs, let `ProcessTaskBatches` enforce a total budget, deadline, and
small pause between Redis calls:

```go
result, err := inspector.ProcessTaskBatches(
	ctx,
	"critical",
	"retry",
	"run",
	"", // A group is required only for aggregating tasks.
	asynq.TaskBatchPolicy{
		BatchSize:       250,
		MaxTasks:        2_000,
		Timeout:         20 * time.Second,
		InterBatchDelay: 50 * time.Millisecond,
	},
)
if err != nil {
	// Processed is confirmed progress, even when the final call was ambiguous.
	log.Printf("stopped after %d tasks: %v", result.Processed, err)
}
```

Each batch is atomic; the entire series is not. If a transport timeout occurs,
Redis may have committed the last mutation even though its reply was lost.
Keep the confirmed processed count, inspect current state, and do not blindly
retry the failed suffix.

`QueryTasks` filters one bounded page rather than scanning an unlimited queue:

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
if err != nil {
	log.Printf("archived %d confirmed tasks: %v", processed, err)
	return err
}
```

Pagination over a live queue is not a snapshot. If a complete selection
matters, quiesce every producer, worker, and scheduler that can change the
selected state while collecting pages. De-duplicate all IDs before mutating
chunks of at most `asynq.MaxInspectorBatchSize`.

For queue dashboards, `GetQueueInfoBatch` keeps input order and duplicates and
pipelines up to 100 queues per flush:

```go
infos, err := inspector.GetQueueInfoBatch(
	ctx,
	[]string{"critical", "default", "critical"},
	15*time.Second, // TTL for approximate memory samples only.
)
```

## Prometheus metrics

The optional collector is both a Prometheus collector and an Inspector
observer. Register it once, attach it to the Inspector, and expose the registry
through the usual Prometheus HTTP handler:

```go
import (
	"log"
	"net/http"

	asynqmetrics "github.com/pars-aria-labs/asynq/x/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

operationMetrics := asynqmetrics.NewInspectorMetricsCollector()
inspector.SetOperationObserver(operationMetrics)

registry := prometheus.NewRegistry()
registry.MustRegister(
	asynqmetrics.NewQueueMetricsCollector(inspector),
	operationMetrics,
)

http.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
log.Fatal(http.ListenAndServe(":9876", nil))
```

Inspector telemetry exports these low-cardinality series:

- `asynq_inspector_operations_total`
- `asynq_inspector_items_processed_total`
- `asynq_inspector_redis_round_trips_total`
- `asynq_inspector_operation_duration_seconds`

The round-trip counter measures instrumented client executions and pipeline
flush attempts, not every physical network exchange hidden inside retries or
cluster fan-out. Observer callbacks are synchronous and may arrive
concurrently, so a custom observer should be fast, concurrency-safe, and must
not call back into the same Inspector.

For a standalone Redis server, the bundled exporter is the quickest option:

```sh
(cd tools && go run ./metrics_exporter \
  -redis-addr=127.0.0.1:6379 \
  -redis-db=2 \
  -redis-prefix=billing-prod \
  -port=9876)
```

Configure Prometheus to scrape port `9876`. The bundled executable currently
uses a standalone Redis connection; for Redis Cluster, embed the collectors in
an application that creates its Inspector with `RedisClusterClientOpt`.

## Command-line tool

The CLI is a separate module. Pin it to the same release as the services it
administers:

```sh
go install github.com/pars-aria-labs/asynq/tools/asynq@v1.0.1
asynq version
```

Common commands include:

| Command | What it does |
| --- | --- |
| `asynq dash` | Opens the interactive terminal dashboard |
| `asynq queue list` | Lists queues and their current state |
| `asynq task list` | Lists tasks by queue and state |
| `asynq task inspect` | Shows one task |
| `asynq task enqueue` | Enqueues a task from the shell |
| `asynq group list` | Lists aggregation groups |
| `asynq cron list` | Lists scheduler entries |
| `asynq server list` | Lists active servers |

Pass the same connection data used by the application:

```sh
asynq queue list \
  --uri=127.0.0.1:6379 \
  --db=2 \
  --prefix=billing-prod

asynq stats \
  --cluster \
  --cluster_addrs=redis-0:7000,redis-1:7001,redis-2:7002 \
  --prefix=billing-prod
```

Global flags include `--username`, `--password`, `--tls`, `--tls_server`,
`--insecure`, and `--config`. Flags override environment and config-file
values. Keep passwords out of shell history, and never use `--insecure` in
production merely to silence a certificate error.

![Asynq terminal dashboard](docs/assets/dash.gif)

## Web UI

[`pars-aria-labs/asynqmon`](https://github.com/pars-aria-labs/asynqmon) is the
companion browser interface for queue and task inspection. Give it the same
Redis endpoint, database, credentials, and prefix as the workers.

![Asynqmon queue view](docs/assets/asynqmon-queues-view.png)

## Upgrading an existing application

Changing the Go module path does not rewrite Redis keys or task payloads. The
current code keeps the established wire format, so existing queues remain
readable when the Redis endpoint, database, and prefix stay the same.

For a production upgrade:

1. Pin the root and optional `x` modules to the same release.
2. Test against a copy of production data or a staging Redis instance.
3. Exercise enqueue, processing, retry, scheduling, uniqueness, aggregation,
   and administrative flows.
4. Compare queue counts and metrics before and after the deployment.
5. Roll out gradually and keep the previous application build available for
   rollback.

Avoid a permanent `replace` directive that disguises one module identity as
another. Use a temporary Go workspace for simultaneous local development and
use normal versioned modules in released applications.

## Testing changes

The repository contains three Go modules, so test each graph explicitly:

```sh
GOWORK=off go test ./...
GOWORK=off go vet ./...
(cd x && GOWORK=off go test ./... && GOWORK=off go vet ./...)
(cd tools && GOWORK=off go test ./... && GOWORK=off go vet ./...)
```

Redis-backed tests can flush their assigned databases. Run them only against
a disposable Redis instance, never production:

```sh
redis_addr=127.0.0.1:6379
go test -count=1 -p=1 -race . -args -redis_addr="$redis_addr"
go test -count=1 -p=1 -race ./internal/rdb -args -redis_addr="$redis_addr"
(cd x && go test -count=1 -p=1 -race ./rate -args -redis_addr="$redis_addr")
```

The opt-in soak test also runs `SCRIPT FLUSH`, which affects the entire Redis
server rather than one database. Use a dedicated disposable server:

```sh
ASYNQ_TEST_REDIS_ADDR=127.0.0.1:6379 make soak-inspector-batch
```

If the Asynqmon repository is checked out beside this one, run
`make test-asynqmon` to test it against the local root and `x` modules. The
Makefile and CI create temporary Go workspaces; no committed development
workspace is required.

## Publishing a new release

Use Semantic Versioning on the v1 line:

- `v1.0.2` for the next backward-compatible fix
- `v1.1.0` for a backward-compatible feature
- `v1.1.0-beta.1` for a preview of the next minor release

Prepare one release commit. Update `internal/base.Version` without the leading
`v`, the root requirement in `x/go.mod`, the root and `x` requirements in
`tools/go.mod`, the current-version examples in this README, and
`CHANGELOG.md`. Review public API changes before a stable release.

For changes that span all three modules, create a temporary local workspace:

```sh
repo_root=$(pwd)
workspace_dir=$(mktemp -d)
(cd "$workspace_dir" && GOWORK=off go work init \
  "$repo_root" "$repo_root/x" "$repo_root/tools")

root_version=$(awk \
  '$1 == "github.com/pars-aria-labs/asynq" { print $2 }' \
  "$repo_root/x/go.mod")
x_version=$(awk \
  '$1 == "github.com/pars-aria-labs/asynq/x" { print $2 }' \
  "$repo_root/tools/go.mod")
GOWORK="$workspace_dir/go.work" go work edit \
  "-replace=github.com/pars-aria-labs/asynq@${root_version}=$repo_root" \
  "-replace=github.com/pars-aria-labs/asynq/x@${x_version}=$repo_root/x"

GOWORK="$workspace_dir/go.work" go test ./...
GOWORK="$workspace_dir/go.work" go vet ./...
(cd x && \
  GOWORK="$workspace_dir/go.work" go test ./... && \
  GOWORK="$workspace_dir/go.work" go vet ./...)
(cd tools && \
  GOWORK="$workspace_dir/go.work" go test ./... && \
  GOWORK="$workspace_dir/go.work" go vet ./...)
actionlint .github/workflows/*.yml
git diff --check
```

Push the release commit to `main` and wait until CI succeeds on that exact SHA.
Then create three annotated tags on the reviewed commit and send only those
refs in one atomic push:

```sh
set -euo pipefail

version=v1.0.2
git fetch origin main

if [ -n "$(git status --porcelain)" ]; then
  echo "Refusing to tag a working tree with uncommitted changes" >&2
  exit 1
fi

candidate=$(git rev-parse HEAD)

test "$candidate" = "$(git rev-parse origin/main)"

for tag in "$version" "x/$version" "tools/$version"; do
  if git show-ref --verify --quiet "refs/tags/$tag" || \
     git ls-remote --exit-code --tags origin "refs/tags/$tag" >/dev/null 2>&1; then
    echo "Refusing to reuse existing tag: $tag" >&2
    exit 1
  fi
done

git tag -a "$version" "$candidate" -m "Asynq $version"
git tag -a "x/$version" "$candidate" -m "Asynq x $version"
git tag -a "tools/$version" "$candidate" -m "Asynq tools $version"

git push --atomic origin \
  "refs/tags/$version" \
  "refs/tags/x/$version" \
  "refs/tags/tools/$version"
```

The `tools/` tag starts the release workflow. GitHub generates the release
notes, reruns the full standalone, Cluster, and Asynqmon gates, builds six CLI
archives, creates `SHA256SUMS`, records provenance, and publishes the release.
Stable versions become normal latest releases; beta versions remain explicit
pre-releases.

Once a tag reaches GitHub, treat it as immutable. Never delete, move,
force-push, or reuse it. If a published tag is defective, fix `main` and publish
the next unused patch version.

After publication, verify the assets and public modules:

```sh
verify_dir=$(mktemp -d)
gh release download "$version" \
  --repo pars-aria-labs/asynq \
  --dir "$verify_dir"
(cd "$verify_dir" && sha256sum --check SHA256SUMS)

for archive in "$verify_dir"/*.tar.gz "$verify_dir"/*.zip; do
  gh attestation verify "$archive" --repo pars-aria-labs/asynq
done

consumer_dir=$(mktemp -d)
export GOMODCACHE="$consumer_dir/mod"
export GOBIN="$consumer_dir/bin"
export GOWORK=off
export GOPROXY=https://proxy.golang.org,direct
export GOSUMDB=sum.golang.org
mkdir -p "$GOMODCACHE" "$GOBIN"

go list -m "github.com/pars-aria-labs/asynq@$version"
go list -m "github.com/pars-aria-labs/asynq/x@$version"
go list -m "github.com/pars-aria-labs/asynq/tools@$version"
go install "github.com/pars-aria-labs/asynq/tools/asynq@$version"
test "$("$GOBIN/asynq" version)" = "asynq version ${version#v}"
```

Once the public module proxy can see all three tags, run
`GOWORK=off go mod tidy` in the root, `x`, and `tools` modules. Commit any
newly recorded same-repository checksums as an ordinary follow-up change on
`main`; do not move the release tags to that commit.

Do not publish `v2.0.0` with the current module declarations. Go requires a
`/v2` suffix for the root module, `x`, `tools`, every import, and affected
consumer before any v2 tag is created.

## Contributing

Issues and pull requests are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md)
and search the [issue tracker](https://github.com/pars-aria-labs/asynq/issues)
before opening a new report.

## License and attribution

Asynq is available under the [MIT License](LICENSE). Original notices and Git
history are preserved. The project includes work by its original authors and
all later contributors. The logo was created by
[Vic Shóstak](https://github.com/koddr) and released under
[CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/).
