# Asynq v1.0.0

`v1.0.0` is the first stable release of the independently maintained Asynq
module at:

```text
github.com/pars-aria-labs/asynq
```

The root library, optional `x` packages, and administration CLI are released
from one reviewed commit with matching module tags. The Go package identifier
remains `asynq`, and v1 imports do not add a `/v1` suffix.

## Highlights

- Bounded, context-aware Inspector mutations with explicit work budgets,
  confirmed partial-progress reporting, and safe handling of ambiguous Redis
  transport failures.
- Consistent Redis namespace prefixes throughout Inspector operations, the
  CLI, and the Prometheus exporter.
- Pipelined operational reads, optional short-lived memory estimates, and
  low-cardinality Inspector telemetry in `x/metrics`.
- Validation against standalone Redis and a three-node Redis Cluster, plus
  Asynqmon compatibility and an opt-in concurrent Inspector soak harness.
- Prebuilt CLI archives for Linux, macOS, and Windows on amd64 and arm64, with
  SHA-256 checksums and GitHub build-provenance attestations.

Compared with `v1.0.0-beta.1`, the public v1 contract is now stable and the
dependency graph has been refreshed. This includes Redis client, Protobuf,
Prometheus, Viper, terminal/Unicode, and release-action updates. No exported
Go API was removed between the beta and this final release.

## Install or upgrade

Pin only the modules your application imports:

```sh
go get github.com/pars-aria-labs/asynq@v1.0.0
go get github.com/pars-aria-labs/asynq/x@v1.0.0  # only when an x package is used
go mod tidy
go test ./...
```

The canonical import remains:

```go
import "github.com/pars-aria-labs/asynq"
```

Install the matching CLI with:

```sh
go install github.com/pars-aria-labs/asynq/tools/asynq@v1.0.0
asynq version
```

The final command must print `asynq version 1.0.0`.

### Upgrading from v0.27.1 or the beta

No Redis rewrite is required solely for this upgrade. Keep every producer,
worker, scheduler, Inspector, CLI, and Asynqmon instance on the same Redis
endpoint, database, and namespace prefix. For a controlled production rollout:

1. Back up Redis or use a production-like copy.
2. Upgrade the application in staging and run enqueue, processing, retry,
   scheduling, uniqueness, aggregation, and administrative flows.
3. Compare queue counts, failures, latency, and telemetry before and after the
   change.
4. Roll out gradually while retaining the previous application build for
   rollback.

See the [migration guide](migrating-to-pars-aria-labs.md) and the
[bounded Inspector guide](batch-inspector.md) for code examples and operational
limits.

## Verify downloaded assets

The release page contains six archives and `SHA256SUMS`. Verify the selected
archive before extracting it:

```sh
archive=asynq_1.0.0_linux_amd64.tar.gz
grep -F "  $archive" SHA256SUMS | sha256sum --check -
```

On macOS, calculate the digest with `shasum -a 256 "$archive"`; on Windows,
use `Get-FileHash -Algorithm SHA256`. Compare the result with the matching line
in `SHA256SUMS`.

GitHub also records build provenance for each archive. With GitHub CLI
installed, verify an attestation against this repository:

```sh
gh attestation verify "$archive" --repo pars-aria-labs/asynq
```

## Compatibility promise

The Redis key layout and serialized task format remain compatible with the
`v0.27.x` line and `v1.0.0-beta.1`. Starting with this release, backward-
incompatible changes to the public Go API require a new major-version module
path in accordance with Semantic Versioning.
