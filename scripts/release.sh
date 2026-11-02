#!/usr/bin/env bash
# Usage: scripts/release.sh v1.2.0 [--push]
# Tags root (vX), x (x/vX) and tools (tools/vX) on HEAD so all modules share
# one version. Without --push the tags are only created locally.
set -euo pipefail

version="${1:?usage: $0 vMAJOR.MINOR.PATCH[-beta.N] [--push]}"
push="${2:-}"
cd "$(git rev-parse --show-toplevel)"

re='^v1\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-beta\.[1-9][0-9]*)?$'
[[ "$version" =~ $re ]] || { echo "bad version: $version" >&2; exit 1; }
[ -z "$(git status --porcelain)" ] || { echo "working tree not clean" >&2; exit 1; }

embedded="$(sed -n 's/^const Version = "\([^"]*\)"/\1/p' internal/base/base.go)"
[ "$embedded" = "${version#v}" ] || { echo "internal/base.Version=$embedded, expected ${version#v}" >&2; exit 1; }

check() { # file module
  awk -v m="$2" -v v="$version" '$1==m && $2==v {f=1} END{exit !f}' "$1" \
    || { echo "$1 must require $2 $version" >&2; exit 1; }
}
check x/go.mod github.com/pars-aria-labs/asynq
check tools/go.mod github.com/pars-aria-labs/asynq
check tools/go.mod github.com/pars-aria-labs/asynq/x

tags=("$version" "x/$version" "tools/$version")
for t in "${tags[@]}"; do git tag -a "$t" -m "$t"; done
echo "created: ${tags[*]}"

if [ "$push" = "--push" ]; then
  git push --atomic origin "${tags[@]}"
else
  echo "push with: git push --atomic origin ${tags[*]}"
fi
