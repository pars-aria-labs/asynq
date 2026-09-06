# Migrating to the Pars Aria Labs module

The canonical module path of this fork is
`github.com/pars-aria-labs/asynq`. The Go package identifier remains `asynq`, so
application code changes only at import and dependency boundaries.

## Source lineage

This repository retains its imported MIT-licensed Git history. The maintained
source and module live at
[`pars-aria-labs/asynq`](https://github.com/pars-aria-labs/asynq), and the
imported history baseline is commit
[`2f4fd0a`](https://github.com/pars-aria-labs/asynq/commit/2f4fd0a).
Original copyright notices, commit history, license text, and historical
change references are retained.

This fork is maintained independently under its canonical repository identity.

## Updating an application

Use the canonical module import in Go files:

```go
import "github.com/pars-aria-labs/asynq"
```

Then update the dependency to the canonical release:

```sh
go get github.com/pars-aria-labs/asynq@v0.27.1
go mod tidy
go test ./...
```

Optional packages use the same canonical module namespace:

```go
import "github.com/pars-aria-labs/asynq/x/metrics"
```

Then pin the matching optional-module release before tidying:

```sh
go get github.com/pars-aria-labs/asynq/x@v0.27.1
```

Do not use a permanent `replace` directive to disguise the old module as the
new one. Go's `internal` package rules and self-imports make that arrangement
fragile. A local `go.work` file is appropriate when actively developing two
sibling checkouts together; released consumers should resolve the versioned
modules directly.

## Redis data compatibility

Changing the Go module path does not rewrite Redis keys or task payloads. The
fork keeps the Asynq wire format. Existing queues therefore remain readable as
long as the same Redis endpoint, database, and namespace prefix are used.

Always stage an upgrade against a copy of production data first. Stop writers
when changing any Redis namespace setting, and verify queue counts with an
Inspector before and after deployment.

Before upgrading, check unusual names and prefixes: queue names may not begin
with `}`, and the first Redis hash-tag pair in a configured `Prefix` may not be
empty (`{}`). Earlier versions accepted these values, but multi-key operations
could fail with `CROSSSLOT` on Redis Cluster.

## Adopting the new administrative APIs

Existing Inspector calls remain source-compatible. New code can add
cancellation without rewriting each call:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

queues, err := inspector.WithContext(ctx).Queues()
```

For large administrative changes, prefer the bounded APIs described in
[Bounded Inspector operations](batch-inspector.md). They expose progress,
enforce a maximum atomic batch of 500 source-state transitions, and preserve a
confirmed lower bound on partial failures. Archive operations may additionally
evict up to 500 expired or excess archive entries per call. A lost Redis reply
can make the failing mutation ambiguous, so inspect current state instead of
blindly retrying it.
