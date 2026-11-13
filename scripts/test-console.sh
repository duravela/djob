#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
build_dir=$(mktemp -d "${TMPDIR:-/tmp}/djob-console-browser.XXXXXXXX")
trap 'rm -rf "$build_dir"' EXIT
go build -o "$build_dir/djob" ./cmd/djob
go build -o "$build_dir/djob-console" ./cmd/djob-console
node --test internal/console/ui/*.test.js
npm --prefix test/console ci --ignore-scripts
DJOB_CONSOLE_TEST_BIN_DIR="$build_dir" npm --prefix test/console test -- "$@"
