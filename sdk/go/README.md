# djob job-authoring SDK

This entire module is licensed under [Apache 2.0](LICENSE); see [NOTICE](NOTICE).
It includes the base contracts, `handlers`, `worker`, protocol schemas and
generated bindings. You may use it in proprietary products and distribute or
sell SDK-based workers subject to Apache's terms and applicable dependency
licenses. The engine's custom license does not apply to this subtree or merely
because a worker connects to an engine. Importing code from engine packages such
as `pkg/integrations` or `pkg/server` introduces their separate license terms.
See the [licensing boundary](../../COMMERCIAL.md#sdk-and-engine-dependencies).

Import `github.com/duravela/djob/sdk/go` as package `djob`. This module owns the job value types and the `Handler`, `View`, `Decision`, `Action`, `Effect`, `Driver`, `DriverFactory` and `Registry` contracts. The base package and `handlers` compile using only the standard library, with no Raft, database or protocol driver dependency. The optional `worker` subpackage adds gRPC/protobuf dependencies for remote execution and lifecycle; `handlers` supplies small built-in jobs.

```go
package companyjobs

import (
    "crypto/sha256"

    "github.com/duravela/djob/sdk/go"
)

func Hash(v djob.View) (djob.Decision, error) {
    sum := sha256.Sum256(v.Run.Definition.Input)
    return djob.Decision{
        Kind: djob.JobComplete, Status: djob.JobSucceeded, Result: sum[:],
    }, nil
}
```

Business packages can depend on this SDK alone. A company worker assembles handlers and drivers in a `djob.Registry` and supplies it through `worker.Config.Registry`. Embedded applications can also use `server.Config.JobRegistry`. The engine's `pkg/jobs` retains aliases for its existing extension contracts; storage aliases the SDK's plain value types. Durable commands, validation, persistence and authorization stay in the engine.

Use [the company-worker example](../../examples/company-worker/README.md) for independent execution, [company-node](../../examples/company-node/README.md) for embedding for company code placement. Supplied MQTT, HTTP and Modbus integrations stay optional. A company driver may keep a temporary session within one attempt and must establish quiescence before the runtime releases a reserved resource.

`worker.Serve(ctx, cfg, worker.ReconnectConfig{})` supervises fresh authenticated sessions with bounded backoff and optional trusted fallback controllers. It waits for local I/O and cleanup before reconnecting; authentication, TLS, protocol and lost-session cleanup failures are terminal. `worker.Run(ctx, cfg)` remains the single-session API. `worker.LoadConfig` loads private credentials and verified TLS settings, including optional `fallback_urls`. Both APIs retain a driver across a conversation, enforce command sequence and lease budgets, drain on cancellation, and cancel/join I/O after connection loss. Reconnection never resumes a prior session or replays effects/results.

The manifests target the first `v0.1.0` release. They do not imply that a tag has been published. Use separate SDK tags (`sdk/go/v...`) and engine tags (`v...`); preserve existing tags and published module checksums. Local development uses the root `go.work`; `scripts/check-packaging.sh` validates a consumer against candidate module archives with `GOWORK=off`.

The same SDK worker can run as a managed Kubernetes deployment.
Package it in a company image and enroll its identity; djob maintains instances
from a digest-pinned image and existing Secret. `worker.LoadConfig` picks up the
injected deployment generation and projected credentials. Ordinary externally
operated workers require no Kubernetes dependency.
