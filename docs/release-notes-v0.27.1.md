# Release notes: v0.27.1

`v0.27.1` is the first patch release of the canonical
`github.com/pars-aria-labs/asynq` module. It keeps the API and Redis data model
from `v0.27.0` and aligns the published source, operational tools, tutorials,
and multi-node release checks.

The source lineage is unchanged: this independently maintained fork descends
from `hibiken/asynq` through `parsidev/asynq`, with direct base tag
`v0.26.0-parsidev-02` at commit `2f4fd0a`. It is not an official upstream
Asynq release; the original MIT license, notices, attribution, and Git history
remain intact.

## What changed

- The terminal CLI accepts `--prefix` through either a flag or its config file
  and applies it consistently to standalone Redis, Redis Cluster, Inspector,
  Client, and direct RDB-backed commands.
- The bundled Prometheus exporter accepts `-redis-prefix`, so its Inspector
  observes the same namespace as prefixed applications.
- CLI cluster output now honors `cluster: true` supplied by a config file, not
  only the command-line flag.
- Redis Cluster tests close every owned Inspector and Redis client. Assertions
  that enumerate task keys query all cluster masters rather than an arbitrary
  node, eliminating test-only resource leaks and single-node assumptions.
- The soak harness now requires an explicit disposable Redis endpoint before
  it can execute the server-wide `SCRIPT FLUSH` command.
- Fork-owned funding, support, and conduct guidance no longer presents upstream
  maintainers as operators of this repository.
- Package, migration, CLI, TLS, prefix, cluster, and Prometheus examples were
  corrected and expanded.

## Upgrade

Pin only the modules your project uses:

```sh
go get github.com/pars-aria-labs/asynq@v0.27.1
go get github.com/pars-aria-labs/asynq/x@v0.27.1
go mod tidy
```

Install the matching administration CLI separately:

```sh
go install github.com/pars-aria-labs/asynq/tools/asynq@v0.27.1
```

When an application sets `RedisClientOpt.Prefix`, pass that exact value to the
CLI with `--prefix` and to the exporter with `-redis-prefix`. Do not append the
generated `:asynq:` portion yourself.

No Redis rewrite or migration command is required when upgrading from
`v0.27.0`. As with any queue-runtime update, back up Redis and validate the
same topology and prefix in staging before production rollout.
