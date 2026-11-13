# Company extension example

This module shows how a company embeds a djob server/Raft member with its own jobs and integrations. Use [company-worker](../company-worker/README.md) for independent workers that keep company code outside the controller.

| Path | Responsibility |
| --- | --- |
| [`jobs/measurement`](jobs/measurement) | Pure business handler and tests, importing only the SDK and standard library |
| [`integrations/meter`](integrations/meter) | Modbus selection and normalization into a domain reading |
| [`registry.go`](registry.go) | Explicit `acme.check-measurement@1` and `acme.meter@1` registration |
| [`cmd/company-node`](cmd/company-node) | Authentication, TLS, member configuration and startup |

The handler requests the semantic operation `measure` and checks a result such as `{ "value": 42 }` against its input `{ "maximum": 100 }`. The integration maps that operation through an operator-owned Modbus binding. A different adapter could supply the same domain reading through MQTT or HTTP.

To add a business job, create a package under `jobs/` and register its handler in `registry.go`. To support another device, add an adapter under `integrations/` that returns the domain results the handler expects. The handler and adapter packages export their `Name` and `Version` constants so registration uses the same identities as their implementations.

From the djob repository root:

```sh
go test -race ./examples/company-node/...
go build -o /tmp/company-node ./examples/company-node/cmd/company-node
```

The executable requires a persistent member directory and an API credential file, and accepts member and TLS flags listed by `company-node --help`. The [Testcontainers scenario](../../test/e2e/scheduler_test.go) configures real members, bindings and definitions and verifies accepted/rejected readings.

Copy this layout into your own module and update its module and import paths. The example declares candidate engine and SDK versions and resolves them locally through `go.work`. [The packaging check](../../scripts/check-packaging.sh) verifies the same source outside the workspace. Actual remote installation requires publishing the release tags first.
