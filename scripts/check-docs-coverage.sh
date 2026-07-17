#!/usr/bin/env bash
#
# Asserts that every serde-deserialized field in the user-facing config structs
# (ServerConfig, HealthConfig, RoutingConfig, ProviderConfig) is mentioned in the
# documentation. This automates the docs-sync rule: a flag that lands in
# config.rs without a matching entry in configuration.md fails the build.
#
# Usage:
#   scripts/check-docs-coverage.sh [path/to/configuration.md]
#
# The docs live in a separate repository, so CI checks it out and passes its
# path. When run with no argument, the script guesses a sibling docs/ checkout.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_RS="$ROOT/src/config.rs"
DOCS="${1:-$ROOT/../docs/docs/configuration.md}"

if [[ ! -f "$CONFIG_RS" ]]; then
  echo "error: cannot find config source at $CONFIG_RS" >&2
  exit 2
fi
if [[ ! -f "$DOCS" ]]; then
  echo "error: cannot find configuration.md at $DOCS" >&2
  echo "       pass its path as the first argument" >&2
  exit 2
fi

# Extract the serde field name of every `pub` field inside the four user-facing
# config structs. A field-level #[serde(rename = "...")] wins over the Rust name.
# Uses only POSIX awk features so it runs under both mawk and gawk.
fields="$(awk '
  /^pub struct (ServerConfig|HealthConfig|RoutingConfig|ProviderConfig) \{/ { in_s=1; rename=""; next }
  in_s && /^\}/                                                            { in_s=0; rename=""; next }
  !in_s { next }
  /#\[serde\(rename = "/ {
    s=$0
    if (match(s, /rename = "[^"]+"/)) {
      t=substr(s, RSTART, RLENGTH); sub(/rename = "/, "", t); sub(/"$/, "", t); rename=t
    }
    next
  }
  /^[[:space:]]*pub[[:space:]]/ {
    s=$0
    if (match(s, /pub[[:space:]]+[a-z_][a-z0-9_]*/)) {
      t=substr(s, RSTART, RLENGTH); sub(/pub[[:space:]]+/, "", t)
      print (rename != "" ? rename : t)
    }
    rename=""
  }
' "$CONFIG_RS")"

if [[ -z "$fields" ]]; then
  echo "error: extracted no fields from $CONFIG_RS — has the struct layout changed?" >&2
  exit 2
fi

missing=()
while IFS= read -r field; do
  [[ -z "$field" ]] && continue
  # -w: whole-word match, so `url` does not match inside other identifiers.
  if ! grep -qw -- "$field" "$DOCS"; then
    missing+=("$field")
  fi
done <<< "$fields"

if (( ${#missing[@]} > 0 )); then
  echo "✗ config fields missing from $(basename "$DOCS"):" >&2
  printf '    - %s\n' "${missing[@]}" >&2
  echo "" >&2
  echo "Document each flag in configuration.md (see the docs-sync rule), then re-run." >&2
  exit 1
fi

count="$(printf '%s\n' "$fields" | grep -c .)"
echo "✓ all $count config fields are documented in $(basename "$DOCS")"
