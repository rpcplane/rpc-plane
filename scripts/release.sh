#!/usr/bin/env bash
# Bump the workspace version, commit, tag, and push to trigger the release workflow.
# Usage: ./scripts/release.sh 0.2.0
set -euo pipefail

VERSION=${1:-}
if [[ -z "$VERSION" ]]; then
  echo "usage: $0 <version>   (e.g. $0 0.2.0)" >&2
  exit 1
fi
VERSION=${VERSION#v}          # strip leading v if given
TAG="v${VERSION}"

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$ ]]; then
  echo "error: '$VERSION' is not a valid semver string" >&2
  exit 1
fi

if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "error: working tree has uncommitted changes — clean up first" >&2
  exit 1
fi

if git rev-parse "$TAG" &>/dev/null; then
  echo "error: tag $TAG already exists" >&2
  exit 1
fi

echo "Bumping workspace version to $VERSION ..."
# Matches the `version = "x.y.z"` line under [workspace.package] — the only
# line that starts with `version =` in this Cargo.toml.
sed -i "s/^version = \"[0-9][^\"]*\"/version = \"${VERSION}\"/" Cargo.toml

echo "Refreshing Cargo.lock ..."
cargo build -q --workspace

git add Cargo.toml Cargo.lock
git commit -m "chore: release ${TAG}"
git tag "${TAG}"

BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo ""
echo "Created commit and tag ${TAG} on ${BRANCH}."
echo "Pushing ..."
git push origin "${BRANCH}" "${TAG}"
echo ""
echo "Release workflow triggered: https://github.com/rpcplane/rpc-plane/actions"
