# Asynq v1.0.0-beta.1

`v1.0.0-beta.1` is the first public preview of the Asynq 1.0 release line. It
is published as a GitHub **pre-release** so teams can validate the v1 contract,
operational behavior, and deployment process before the final `v1.0.0` release.

The sole canonical module identity is:

```text
github.com/pars-aria-labs/asynq
```

## What is included

- Context-aware and bounded Inspector operations with explicit work budgets.
- Safe batch mutation behavior for ambiguous Redis transport failures,
  `NOSCRIPT` recovery, and Redis Cluster redirects.
- Consistent Redis namespace-prefix support across the library, Inspector,
  terminal CLI, and Prometheus exporter.
- Pipelined operational reads, optional short-lived memory estimates, and
  low-cardinality Inspector telemetry.
- Companion Asynqmon support for bounded bulk operations and visible progress.
- Race-enabled standalone and three-node Redis Cluster validation, plus an
  opt-in concurrent soak harness.

## Install the beta

Pin only the modules used by your application:

```sh
go get github.com/pars-aria-labs/asynq@v1.0.0-beta.1
go get github.com/pars-aria-labs/asynq/x@v1.0.0-beta.1
go mod tidy
go test ./...
```

The root, `x`, and `tools` module tags are published from the same reviewed
commit. This keeps the optional packages and CLI aligned with the beta source.

Install the matching administration CLI with:

```sh
go install github.com/pars-aria-labs/asynq/tools/asynq@v1.0.0-beta.1
asynq version
```

### Verify release assets

The release page also provides prebuilt CLI archives for Linux, macOS, and
Windows on amd64 and arm64. To verify one downloaded archive without requiring
the other five archives, select its line from `SHA256SUMS`:

```sh
archive=asynq_1.0.0-beta.1_linux_amd64.tar.gz
grep -F "  $archive" SHA256SUMS | sha256sum --check -
```

On macOS, calculate the digest with `shasum -a 256 "$archive"`; on Windows,
use `Get-FileHash -Algorithm SHA256`. Compare the result with the matching line
in `SHA256SUMS`.

GitHub also records build provenance for every archive. With GitHub CLI
installed, verify that attestation against this repository:

```sh
gh attestation verify "$archive" --repo pars-aria-labs/asynq
```

## Upgrade guidance

The Go package identifier remains `asynq`, and no Redis data rewrite is needed
solely for this release. Producers, workers, schedulers, Inspectors, the CLI,
and Asynqmon must use the same Redis endpoint, database, and namespace prefix.

Before production testing:

1. Back up Redis or test against a production-like copy.
2. Deploy the beta to staging with the same Redis topology and prefix.
3. Exercise enqueue, processing, retry, scheduling, uniqueness, aggregation,
   and administrative workflows.
4. Confirm queue counts and error rates before and after the deployment.
5. Keep the previous application build available for rollback.

This is a beta: although the Redis wire format remains compatible with the
`v0.27.x` line, public API details may still change before final `v1.0.0`.

## Release integrity

The beta workflow validates that the Git tag and embedded CLI version match,
runs race-enabled tests and static analysis, executes the bounded Inspector
soak test, cross-compiles all release archives, attests their provenance,
verifies their checksums, and publishes the result as a GitHub pre-release.
