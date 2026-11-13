# djob

djob is a Go engine for durable distributed job execution. Controllers use etcd
Raft for consensus and bbolt for replicated state. Independent workers execute
company handlers and protocol integrations over authenticated gRPC with TLS.
A separate management console provides the cluster and worker interface.

This is a **public preview**, without stable production qualification. Detailed
project documentation is private for now. Deployment-specific gateway fencing,
Kubernetes storage behavior and capacity sizing require validation in the target
environment. Recorded tests do not establish unlimited storage or workload capacity.

## Licensing

The engine is source-available under the [Duravela Internal Business Source
Available License 1.1](LICENSE). It permits internal business use under its terms;
external engine offerings, redistribution and embedding require a separate
commercial agreement. The engine is not open source. See [COMMERCIAL.md](COMMERCIAL.md).

The entire [Go SDK](sdk/go/README.md), including its worker client and protocol
bindings, is separately [Apache 2.0](sdk/go/LICENSE). Engine packages retain their
own terms. Retained upstream portions carry their original grants and notices;
see [THIRD_PARTY_NOTICES.txt](THIRD_PARTY_NOTICES.txt) and [licenses/](licenses/).
Independent legal approval of the custom engine license has not been recorded.

## Components

- [Controller](cmd/djob): scheduling, durable state, APIs and Raft membership.
- [Worker](cmd/djob-worker): independent execution and protocol integrations.
- [Management console](cmd/djob-console): cluster and worker operations.
- [Go SDK](sdk/go): pure handler contracts and the remote worker runtime.
- [Company worker example](examples/company-worker): a custom independent worker.
- [Company node example](examples/company-node): embedding with custom handlers.

Jobs support once, interval and cron triggers, durable cancellation and retries,
and explicit worker release selection. Worker releases can coexist while older
instances drain. Shared channel reservations serialize access to half-duplex
resources, including Modbus buses. MQTT and HTTP integrations are also supplied.

## Build and test

Use Go 1.26 or later. The root `go.work` connects the SDK and examples for local
development; third-party modules must be downloadable.

```sh
go build ./cmd/djob ./cmd/djob-worker ./cmd/djob-console
./scripts/test-workspace.sh
python3 -m unittest discover -s scripts -p '*_test.py'
python3 scripts/check_provenance.py
./scripts/check-packaging.sh
```

Run an executable with `--help` for its configuration flags. Integration tests
need local TCP listeners. Docker-based scenarios use `./scripts/test-e2e.sh`;
browser workflows use `./scripts/test-console.sh` and
`./scripts/test-console-shared.sh`. See [CI](.github/workflows/test.yml) for their
setup and [CONTRIBUTING.md](CONTRIBUTING.md) for contribution requirements.

The SDK and engine use separate tags (`sdk/go/v...` and `v...`). Availability is
established by published tags and successful module-proxy checks, not by version
requirements in source. Release archives are built with
`./scripts/build-release.sh --version VERSION --output /absolute/new/directory`.
