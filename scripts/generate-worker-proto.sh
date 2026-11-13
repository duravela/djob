#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# Match the generators used by the controller's checked-in protobuf bindings.
[[ $(protoc-gen-go --version) == 'protoc-gen-go v1.36.12' ]]
[[ $(protoc-gen-go-grpc --version) == 'protoc-gen-go-grpc 1.6.2' ]]
protoc -I sdk/go/worker/protocol/v2 \
  --go_out=sdk/go/worker/protocol/v2 --go_opt=paths=source_relative \
  --go-grpc_out=sdk/go/worker/protocol/v2 --go-grpc_opt=paths=source_relative \
  sdk/go/worker/protocol/v2/worker.proto
