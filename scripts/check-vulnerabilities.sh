#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
export GOWORK="$PWD/go.work"
if [[ $# -ne 1 ]]; then
  echo "Usage: $0 REPORT_DIRECTORY" >&2
  exit 2
fi
mkdir -p "$1"
reports=$(cd "$1" && pwd)
scanner=${GOVULNCHECK_BIN:-govulncheck}

# The root ./... pattern excludes the nested SDK and consumer modules. Scan all
# modules and all release-style binaries; any finding fails this check.
module_dirs=$(go list -m -f '{{.Dir}}')
packages=()
while IFS= read -r dir; do
  if [[ -n "$dir" ]]; then
    packages+=("$dir/...")
  fi
done <<< "$module_dirs"
if [[ ${#packages[@]} -eq 0 ]]; then
  echo "No Go workspace modules found" >&2
  exit 1
fi

status=0
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 "$scanner" -C cmd/djob -show=version,verbose "${packages[@]}" > "$reports/source.txt" 2>&1 || status=1
cat "$reports/source.txt"
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o "$reports/djob" ./cmd/djob
"$scanner" -mode=binary -show=version,verbose "$reports/djob" > "$reports/binary.txt" 2>&1 || status=1
cat "$reports/binary.txt"
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o "$reports/djob-worker" ./cmd/djob-worker
"$scanner" -mode=binary -show=version,verbose "$reports/djob-worker" > "$reports/worker-binary.txt" 2>&1 || status=1
cat "$reports/worker-binary.txt"
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o "$reports/djob-console" ./cmd/djob-console
"$scanner" -mode=binary -show=version,verbose "$reports/djob-console" > "$reports/console-binary.txt" 2>&1 || status=1
cat "$reports/console-binary.txt"
exit "$status"
