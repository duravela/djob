#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
export GOWORK="$PWD/go.work"

# Fail when Docker is unavailable. An explicitly invoked E2E run must not skip.
arch=$(docker info --format '{{.Architecture}}')
case "$arch" in
  aarch64|arm64) arch=arm64 ;;
  x86_64|amd64) arch=amd64 ;;
  *) echo "Unsupported Docker architecture: $arch" >&2; exit 1 ;;
esac
build_dir=$(mktemp -d "${TMPDIR:-/tmp}/djob-e2e.XXXXXXXX")
trap 'rm -rf "$build_dir"' EXIT
if [[ -n ${DJOB_E2E_RELEASE_DIR:-} ]]; then
  python3 scripts/stage_release.py "$DJOB_E2E_RELEASE_DIR" "$arch" "$build_dir"
else
  GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go build -o "$build_dir/djob" ./cmd/djob
  GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go build -o "$build_dir/djob-worker" ./cmd/djob-worker
  GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go build -o "$build_dir/djob-console" ./cmd/djob-console
fi
GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go build -o "$build_dir/company-node" ./examples/company-node/cmd/company-node
GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go build -o "$build_dir/company-worker" ./examples/company-worker/cmd/company-worker
GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go build -o "$build_dir/restore-fixture" ./test/fixtures/restore
GOWORK=off GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go -C test/e2e build -o "$build_dir/gateway" ./gateway
diorama_revision=$(cat test/e2e/diorama-revision)
diorama_source=${DJOB_DIORAMA_SOURCE:-"$build_dir/diorama-source"}
if [[ -z ${DJOB_DIORAMA_SOURCE:-} ]]; then
  git init --quiet "$diorama_source"
  git -C "$diorama_source" fetch --quiet --depth 1 https://github.com/pruefwerk/diorama.git "$diorama_revision"
  git -C "$diorama_source" checkout --quiet --detach FETCH_HEAD
fi
if [[ $(git -C "$diorama_source" rev-parse HEAD) != "$diorama_revision" ]] || [[ -n $(git -C "$diorama_source" status --porcelain) ]]; then
  echo "Diorama source must be a clean checkout of the pinned fixture revision" >&2
  exit 1
fi
GOWORK=off GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go -C "$diorama_source" build -trimpath -o "$build_dir/diorama" ./cmd/diorama
cat > "$build_dir/Dockerfile" <<'EOF'
FROM scratch
COPY djob djob-worker djob-console company-node company-worker gateway diorama restore-fixture /
ENTRYPOINT ["/djob"]
EOF
# The complete sequential suite has measured runs longer than 16 minutes.
# Leave headroom for ordinary image pulls and cluster provisioning.
DJOB_E2E_BUILD_DIR="$build_dir" GOWORK=off go -C test/e2e test -race -count=1 -timeout=25m "$@" ./...
