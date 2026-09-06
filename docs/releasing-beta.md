# Releasing a beta

Beta releases are built and published by GitHub Actions. The repository has
three Go modules, so one release candidate must have three annotated tags that
all point to the same commit:

```text
v1.0.0-beta.1
x/v1.0.0-beta.1
tools/v1.0.0-beta.1
```

The `tools/` tag is the only workflow trigger. It must be included in the same
atomic push as the root and `x/` tags; the workflow refuses to publish when a
tag is missing or the three tags do not resolve to one commit.

## Prepare the candidate

Update `internal/base.Version`, the root requirement in `x/go.mod`, both local
module requirements in `tools/go.mod`, the exact workspace replacements, the
changelog, and the release notes. Keep the version text without `v` in Go code
and with `v` in module requirements and Git tags.

Run the normal test workflow on `main` before creating tags. Confirm that the
candidate commit is the remote branch tip:

```sh
git push origin main
git fetch origin main
test "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)"
```

## Publish the module tags

Create all tags from the reviewed commit, verify their targets, and push only
those exact refs. Do not use `git push --tags`, because it may publish unrelated
local tags.

```sh
set -euo pipefail

version=v1.0.0-beta.1
candidate=$(git rev-parse HEAD)

git tag -a "$version" -m "Asynq $version"
git tag -a "x/$version" -m "Asynq x $version"
git tag -a "tools/$version" -m "Asynq tools $version"

test "$(git rev-list -n 1 "$version")" = "$candidate"
test "$(git rev-list -n 1 "x/$version")" = "$candidate"
test "$(git rev-list -n 1 "tools/$version")" = "$candidate"

git push --atomic origin \
  "refs/tags/$version" \
  "refs/tags/x/$version" \
  "refs/tags/tools/$version"
```

The action then reruns standalone, three-node Redis Cluster, Inspector soak,
Asynqmon, UI, independent module-graph, and static-analysis checks. After those
gates pass, it creates six CLI archives, attests their build provenance,
generates `SHA256SUMS`, and publishes a GitHub pre-release attached to the root
tag.

Never move or reuse a published beta tag. If a candidate or workflow fails,
fix the cause on `main` and publish the next beta number. An interrupted run
can repair its unpublished draft, but it never replaces an already published
release.

## Verify as a consumer

Use a fresh module cache so the check does not rely on the release workspace:

```sh
verify_dir=$(mktemp -d)
export GOMODCACHE="$verify_dir/mod"
export GOBIN="$verify_dir/bin"
export GOPROXY=https://proxy.golang.org,direct
export GOSUMDB=sum.golang.org
export GOWORK=off

go list -m github.com/pars-aria-labs/asynq@v1.0.0-beta.1
go list -m github.com/pars-aria-labs/asynq/x@v1.0.0-beta.1
go install github.com/pars-aria-labs/asynq/tools/asynq@v1.0.0-beta.1
"$GOBIN/asynq" version
```

Finally, download `SHA256SUMS` with the matching archive from the GitHub
release and verify it before installation, as shown in the
[release notes](release-notes-v1.0.0-beta.1.md#verify-release-assets).
