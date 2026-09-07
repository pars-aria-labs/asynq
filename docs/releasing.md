# Releasing Asynq

This guide covers stable patch and minor releases on the v1 line, as well as
beta releases for the next v1 minor. Releases are built and published by
GitHub Actions. Do not upload locally built binaries to a release.

Asynq is a repository with three Go modules. Every release therefore uses
three version tags that point to one reviewed commit:

```text
v1.0.1
x/v1.0.1
tools/v1.0.1
```

The `tools/` tag is the sole release-workflow trigger. The root and `x/` tags
must already be visible when that workflow starts, so all three tags are sent
in one atomic push.

## Choose the version

Use Semantic Versioning within the v1 line:

- Patch release: `v1.0.1` for backward-compatible fixes.
- Minor release: `v1.1.0` for backward-compatible features.
- Beta release: `v1.1.0-beta.1` for a preview of the next minor release.

Increment the beta number for each new candidate: `beta.1`, `beta.2`, and so
on. A beta must always be requested explicitly by consumers; a stable release
remains the appropriate default for production installations.

The commands below use a stable patch as their main example:

```sh
version=v1.0.1
plain_version=${version#v}
```

For a beta, only the value changes:

```sh
version=v1.1.0-beta.1
plain_version=${version#v}
```

## Prepare the source

Start from an up-to-date `main` branch and make sure the working tree contains
only intended release changes:

```sh
git switch main
git pull --ff-only origin main
git status --short
```

Update every location that represents the release version:

1. Set `internal/base.Version` to the version without the leading `v`, for
   example `1.0.1` or `1.1.0-beta.1`.
2. In `x/go.mod`, require the matching root module version.
3. In `tools/go.mod`, require the matching root and `x` module versions.
4. Update the version-specific replacements in `dev/modules.work`. Update a
   replacement in `dev/asynqmon.work` only when that entry is intentionally
   testing the new candidate; do not silently change Asynqmon's stable module
   requirements.
5. Add a dated entry to `CHANGELOG.md` and create
   `docs/release-notes-<version>.md`, including upgrade commands, compatibility
   notes, and any operational precautions.
6. Update the current-version examples in `README.md`,
   `tools/asynq/README.md`, and other documentation that describes the active
   release line.

Go source stores `1.0.1`, while module requirements, documentation commands,
and Git tags use `v1.0.1`.

Before continuing, check for stale version text. Set `previous_version` to the
release being superseded, and review every match rather than replacing older
release notes or historical changelog entries:

```sh
previous_version=v1.0.0
rg -n -F "$previous_version" \
  README.md CHANGELOG.md internal x tools dev docs
rg -n -F "${previous_version#v}" internal/base/base.go
```

## Tidy the three module graphs

The root, `x`, and `tools` modules have separate `go.mod` and `go.sum` files.
Treat each graph independently.

First, tidy ordinary dependency changes before changing an intra-repository
requirement to a version that has not been tagged yet:

```sh
GOWORK=off go mod tidy
(cd x && GOWORK=off go mod tidy)
(cd tools && GOWORK=off go mod tidy)
```

Then set the new root requirement in `x/go.mod` and the new root and `x`
requirements in `tools/go.mod`. Use `dev/modules.work` for local validation and
tidying while those versions are still unpublished:

```sh
workspace=$(realpath dev/modules.work)
(cd x && GOWORK="$workspace" go mod tidy)
(cd tools && GOWORK="$workspace" go mod tidy)
GOWORK="$workspace" go work sync
```

Inspect all module-file changes:

```sh
git diff -- go.mod go.sum x/go.mod x/go.sum tools/go.mod tools/go.sum \
  dev/modules.work dev/modules.work.sum
```

Do not add `replace` directives to a released `go.mod`, and never invent or
copy checksum lines. Checksums for a new same-repository module version may be
unavailable before its tag exists. The release workflow verifies the real
published graph with `GOWORK=off` after the atomic tag push.

After a successful publication, run the three `GOWORK=off go mod tidy`
commands again. Commit any newly generated same-repository checksum entries to
`main` as a normal post-release maintenance commit. Do not move the release
tags to that commit.

## Run the release gates

Run the inexpensive source and workflow checks first:

```sh
git diff --check
actionlint .github/workflows/*.yml

go test ./...
go vet ./...
(cd x && go test ./... && go vet ./...)
(cd tools && go test ./... && go vet ./...)
```

Redis-backed tests must use a disposable Redis deployment. The test suites
flush their assigned databases, so do not point them at production or run two
suites concurrently against the same test databases.

For example, run the Redis-dependent race suites serially against an explicitly
selected disposable endpoint:

```sh
redis_addr=127.0.0.1:6379
go test -count=1 -p=1 -race . -args -redis_addr="$redis_addr"
go test -count=1 -p=1 -race ./internal/rdb -args -redis_addr="$redis_addr"
(cd x && go test -count=1 -p=1 -race ./rate -args -redis_addr="$redis_addr")
```

The GitHub release gate performs the authoritative validation with Go 1.25:

- race-enabled root, `x`, and `tools` tests;
- standalone Redis and a real three-node Redis Cluster;
- the bounded Inspector soak test;
- Asynqmon backend compatibility, UI tests, and the production UI build;
- independent `GOWORK=off` tests and vet for all three modules;
- six CLI cross-builds for Linux, macOS, and Windows on `amd64` and `arm64`.

Review the release notes, module diffs, and public API changes before creating
the candidate commit. A stable release must not accidentally remove or change
an API that was available in its preceding v1 release.

## Push the candidate and wait for CI

Commit the release preparation, push `main`, and wait for every required check
on that exact commit to pass:

```sh
git push origin main
git fetch origin main

candidate=$(git rev-parse HEAD)
test "$candidate" = "$(git rev-parse origin/main)"
```

Do not create release tags while CI is pending, failing, or attached to an
older commit. If another commit reaches `main`, repeat the checks and select a
new candidate SHA.

## Create and publish all three tags

Confirm that the version has never been used locally or remotely:

```sh
set -euo pipefail

version=v1.0.1
candidate=$(git rev-parse HEAD)

for tag in "$version" "x/$version" "tools/$version"; do
  if git show-ref --verify --quiet "refs/tags/$tag" || \
     git ls-remote --exit-code --tags origin "refs/tags/$tag" >/dev/null 2>&1; then
    echo "Refusing to reuse existing tag: $tag" >&2
    exit 1
  fi
done
```

Create annotated tags from the reviewed candidate and verify their peeled
commit targets:

```sh
git tag -a "$version" "$candidate" -m "Asynq $version"
git tag -a "x/$version" "$candidate" -m "Asynq x $version"
git tag -a "tools/$version" "$candidate" -m "Asynq tools $version"

test "$(git rev-list -n 1 "$version")" = "$candidate"
test "$(git rev-list -n 1 "x/$version")" = "$candidate"
test "$(git rev-list -n 1 "tools/$version")" = "$candidate"
```

Push only those exact refs. Do not use `git push --tags`, because that can
publish unrelated local tags:

```sh
git push --atomic origin \
  "refs/tags/$version" \
  "refs/tags/x/$version" \
  "refs/tags/tools/$version"
```

The release workflow validates the tags and module versions, reruns the full
gate, builds and attests the archives, generates `SHA256SUMS`, and publishes a
GitHub release attached to the root tag. Stable versions are published as
normal releases; beta versions are marked as pre-releases and are not marked
as latest.

## Recover safely from a failed release

An atomic-push rejection creates none of the remote tags. Fix the cause,
remove only the unpushed local tags if necessary, and retry from the reviewed
candidate.

Once a tag has reached GitHub, treat it as immutable. Never delete, move,
force-push, or reuse it: Go proxies and developer caches may already have
recorded its content.

- If the workflow was interrupted by a transient service failure, rerun it.
  It may recover its unpublished draft, but it must not replace an already
  published release.
- If the tagged source or tagged workflow is defective, fix `main` and choose
  a new version. For example, replace `v1.1.0-beta.1` with
  `v1.1.0-beta.2`; after a defective stable `v1.0.1` tag, release the fix as
  `v1.0.2`.

Record the failure in the changelog or release notes when consumers could
have fetched the defective tag.

## Verify the published release

On GitHub, confirm that the release:

- is not a draft;
- is a normal and latest release for a stable version, or a non-latest
  pre-release for a beta;
- points to the expected root tag and candidate commit;
- contains six platform archives plus `SHA256SUMS`.

Download the assets, verify their checksums, and verify GitHub's build
provenance attestations:

```sh
verify_dir=$(mktemp -d)

gh release download "$version" \
  --repo pars-aria-labs/asynq \
  --dir "$verify_dir"

(cd "$verify_dir" && sha256sum --check SHA256SUMS)

for archive in "$verify_dir"/*.tar.gz "$verify_dir"/*.zip; do
  gh attestation verify "$archive" --repo pars-aria-labs/asynq
done
```

Finally, verify all three modules through the public Go infrastructure with a
fresh module cache:

```sh
consumer_dir=$(mktemp -d)
export GOMODCACHE="$consumer_dir/mod"
export GOBIN="$consumer_dir/bin"
export GOWORK=off
export GOPROXY=https://proxy.golang.org,direct
export GOSUMDB=sum.golang.org

go list -m "github.com/pars-aria-labs/asynq@$version"
go list -m "github.com/pars-aria-labs/asynq/x@$version"
go list -m "github.com/pars-aria-labs/asynq/tools@$version"
go install "github.com/pars-aria-labs/asynq/tools/asynq@$version"

test "$("$GOBIN/asynq" version)" = "asynq version ${version#v}"
```

Always pass an explicit beta version in these commands. An unqualified update
is not a reliable way to opt into a pre-release.

## Do not publish v2 with the v1 module paths

Go requires semantic import versioning for major versions v2 and later. A v2
release is a module-path migration, not a routine tag change. Before any
`v2.0.0` tag is created, the module declarations and imports would need paths
ending in `/v2`, including the separate submodules:

```text
github.com/pars-aria-labs/asynq/v2
github.com/pars-aria-labs/asynq/x/v2
github.com/pars-aria-labs/asynq/tools/v2
```

The corresponding repository tags would still use the subdirectory prefixes,
for example `v2.0.0`, `x/v2.0.0`, and `tools/v2.0.0`. Do not create those tags
until source imports, generated metadata, documentation, consumers, and the
release workflow have all been migrated and tested together.
