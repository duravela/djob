# Company worker example

This independent executable runs company handlers and integrations using `sdk/go/worker`. It connects to an ordinary djob controller over gRPC with verified TLS and never starts Raft or opens a member database. The controller needs no company packages.

| Path | Responsibility |
| --- | --- |
| [`jobs/measurement`](jobs/measurement) | Protocol-agnostic measurement rules and domain results; SDK-only dependencies |
| [`integrations/meter`](integrations/meter) | Select Modbus TCP/RTU, map an approved operation and normalize one register |
| [`registry.go`](registry.go) | Register built-ins, company handler versions and `acme.meter@1` |
| [`cmd/company-worker`](cmd/company-worker) | Private connection configuration, signals and worker lifecycle |

`acme.check-measurement@1` requests a semantic `measure` operation and checks `{ "value": 42 }` against `{ "maximum": 100 }`. `acme.measure-twice@1` performs two measurements with a durable `continue` event wait between them. The runtime retains the same driver and canonical resource reservation throughout that conversation. Invalid input and failed readings never authorize another measurement.

Build and run from the repository root:

```sh
go test -race ./examples/company-worker/...
go build -o /tmp/company-worker ./examples/company-worker/cmd/company-worker
/tmp/company-worker --worker-config /private/worker.json
```

Enable the controller remote-worker endpoint and provision a tenant worker credential and private TLS configuration. Use the executable’s `--help` for its configuration flags.

The executable uses SDK `worker.Serve` to reconnect with bounded backoff after transient failures and successful local cleanup. Set optional `fallback_urls` in the private configuration to try other explicitly trusted members. Authentication, TLS, protocol or lost-session cleanup errors stop it. `--reconnect=false` selects one session. Reconnection never resumes a driver or replays an old action; gateway blocks remain durable.

The platform operator configures a canonical channel/resource and an `acme.meter@1` binding, for example:

```json
{
  "channel_id": "factory-bus",
  "protocol": "modbus-rtu-tcp",
  "operations": [
    {
      "name": "measure",
      "operation": {
        "version": 1,
        "unit_id": 3,
        "function": "read_holding_registers",
        "address": 20,
        "quantity": 1
      }
    }
  ]
}
```

`protocol` belongs to this company adapter. It removes that selection field before passing the static mapping to the supplied Modbus encoder. The binding must reserve `factory-bus`, and the tenant must have its grant. Gateway endpoints and duplex facts come from the controller's verified allocation. Job input contains the business limit; it cannot override a unit, register, endpoint or wire operation.

Copy this layout into `extensions/<organization>` or a private company project, rename its module/import paths, and add it to the local workspace with `go work use`. Implement business rules under `jobs/` and device/protocol mappings under `integrations/`, then register explicit versions. For application embedding, use [company-node](../company-node/README.md).

The manifests reference candidate releases. [Packaging checks](../../scripts/check-packaging.sh) build this module outside the workspace without local replacements; normal remote installation requires publishing those tags. The [container test](../../test/e2e/worker_test.go) exercises this exact executable against independent controllers and gateway fixtures, including worker loss and retained blocks through restart.

To let djob maintain instances of this worker, package the Linux executable in an
image and provision a digest-pinned deployment with its existing configuration Secret.
The same registry and SDK startup work for managed and externally operated workers.
