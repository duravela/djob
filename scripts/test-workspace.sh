#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
export GOWORK="$PWD/go.work"

# Discover the engine, SDK and consumer modules. The root ./... pattern covers
# the engine, but intentionally excludes nested Go modules.
module_dirs=$(go list -m -f '{{.Dir}}')
modules=()
while IFS= read -r dir; do
  if [[ -n "$dir" ]]; then
    modules+=("$dir/...")
  fi
done <<< "$module_dirs"

if [[ ${#modules[@]} -eq 0 ]]; then
  echo "No Go workspace modules found" >&2
  exit 1
fi

exec go test -race -count=1 -timeout=5m "$@" "${modules[@]}"
