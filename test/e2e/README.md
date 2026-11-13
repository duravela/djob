# Testcontainers end-to-end tests

Run from the repository root with Go 1.26+, Docker, Git and a C compiler for the race-enabled test process:

```sh
./scripts/test-e2e.sh -v
```

The script builds Linux binaries for the Docker server's architecture, then Testcontainers for Go creates a binary-only scratch image, a private Docker network, three independent djob members, separate worker processes and gateway fixtures. It also builds the Diorama revision pinned in `diorama-revision`, downloaded into the temporary build directory; set `DJOB_DIORAMA_SOURCE` to an existing clean checkout of that revision to reuse it. Only compiled binaries enter the scratch image. Public APIs use scoped credentials and TLS with hostname verification. Independent workers use protobuf gRPC streams over the TLS API origin, including configurations downloaded from the console. Raft uses a separate CA and mutually authenticated member certificates. The test process runs with race detection; the containers run cgo-free release-style executables. They have no repository or host-directory mounts. Docker access is required; an explicitly invoked script fails if the daemon is unavailable.

The scenarios verify:

- A separate management console over TLS, scoped browser sessions, reviewed job configuration and tenant quota policies, one-shot launch, schedule pause/resume, reader run/history/usage isolation and console restart while controller-owned work survives. Worker observations remain tenant scoped across independent processes and show explicit partial results during a controller outage. Dynamic enrollment starts separate company workers using private downloaded credentials on other members; drain/resume, rotation and revocation retain two-command half-duplex conversations and survive controller restart.
- Modbus TCP and raw RTU-over-TCP framing through separate gateway containers.
- Half-duplex exclusion across units, two tenants, a callback-waiting conversation and legacy dispatch.
- Corrupt replies preserving unknown outcomes and blocking the shared resource.
- Actual leader termination, stable exact-retry acknowledgements, and whole-cluster restart with retained member files and blocked work.
- Independent computation progressing after restart without unblocking uncertain gateway work.
- A separate company executable registering its own handler and driver, using the same engine and applying domain-specific limits to normalized results.
- Independent company workers retaining the same driver and half-duplex reservation across related commands and callback waits.
- The same company worker process reconnecting to a trusted fallback after its attached controller is killed, completing an independent conversation while the old bus remains blocked and its confirmed action stays in history.
- Active and idle workers relocating after a Raft-only network partition while all controllers, APIs, workers and gateways remain reachable. The isolated controller joins its sessions and releases their capacity slots. State metrics distinguish polling sessions, clear all old session states after disconnect, expose configured member/tenant limits, and show a new connected session after recovery. Healing the separate Raft network reopens worker admission and preserves confirmed action history and resource blocks without replaying gateway I/O.
- Worker termination during I/O and while waiting; fresh workers cannot resume the physical session or bypass retained blocks, including after all controllers restart.
- A corrupt second RTU exchange making the real worker driver fail cleanup, stopping the worker without claiming unrelated queued work. A fresh worker completes that independent job while the gateway stays blocked and both confirmed and uncertain action history remain intact.
- Managed Kubernetes worker Pods using a pinned K3s image, projected credentials, queued one-shot jobs, process replacement, scale-to-zero/resume, digest-pinned image rollout and Raft leader replacement. The managed worker is killed both while holding a half-duplex conversation and during gateway I/O; a replacement runs unrelated work while uncertain resources stay blocked. A managed MQTT/Diorama scenario also tests projected TLS profiles, namespace-quota failure/recovery and process restart. This isolated fixture requires privileged Docker containers; it does not touch a host Kubernetes cluster.
- A 40-deployment Kubernetes catalog spanning multiple reconciliation pages, including queued work, start/stop of a worker on the final page and retention of all zero-replica revision fences.
- Multiple Kubernetes worker pools with separate namespaces and service-account permissions, identically named tenant configuration Secrets, fail-closed routing for unknown pools, and continued work in one pool while another loses permissions across Raft leader replacement.
- Shared console sessions using encrypted controller records, actual console process replacement during command review and private worker configuration download, controller leader replacement, and session preservation after console restart. The existing memory-session scenario still verifies invalidation on restart.
- Controller-local gRPC session observations across Kubernetes start, stop, resume and image rollout, including the managed MQTT worker and the final catalog page. Tests wait for actual SDK connections separately from Pod readiness.
- Company workload policy surviving managed scale and image changes: init containers, placement, annotations, environment, extra resource limits and volumes, including multiple mounts of the credential volume. A scale change retains the existing Pod; a subsequent image rollout establishes fresh SDK sessions and completes a job.
- A platform-paused Deployment retaining its old worker Pods while an image update is committed, an explicit paused observation, then new-generation connections after Kubernetes resume. Enrollment drain/resume appears independently in every controller's deployment-access observations and new SDK sessions complete work afterward. A Deployment held in deletion by a test-owned finalizer, a newer durable stop while it terminates, and recreation of the latest zero-replica revision fence after releasing only that hold. A later start reconnects an SDK worker and completes work.
- MQTT 5 publishing and request/reply over verified TLS to Mosquitto, interoperating with Diorama's MQTT 3.1.1 client; tenant-profile denial, TLS trust failure and missing-reply uncertainty, using both embedded and independent worker execution.
- A configurable two-tenant simulator soak with recurring compute, MQTT request/reply, shared RTU conversations, gateway outages, worker restarts and Raft-only partitions. Metrics use a dedicated monitoring identity. The default 30-second admission window is a CI smoke check. Longer runs need explicit duration and resource budgets; inspect the generated measurement reports.

The suite has its own `go.mod` and runs with `GOWORK=off`, so Docker client dependencies do not enter the runtime's module graph. Normal workspace tests need no Docker daemon. Running this module directly without the script skips container scenarios because no prepared build context is supplied. CI invokes the script and scans the test module with `govulncheck -test`.

The Raft-only partition scenario requires Docker endpoint gateway-priority support (`GwPriority`). It keeps the application network as the default gateway so removing and restoring the separate Raft endpoint preserves published API ports.

Testcontainers cleanup and Ryuk remove the containers and network, including on failures. Gateway faults are controlled inside the test network; no real device is contacted. Set `DJOB_TEST_ARTIFACT_DIR` to retain simulator/member logs, which CI uploads on failure. Credentials are random and short-lived and are not included in the image build context. The suite uses synthetic read-only register operations and does not establish serial timing, echo suppression or physical quiescence; those still require qualification on an isolated hardware bench.

For a local Colima setup, select its Docker context or set `DOCKER_HOST` to its socket. When using a non-default socket, also set `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock` for Ryuk. See the [official Colima setup](https://golang.testcontainers.org/system_requirements/using_colima/). The script respects the selected daemon and does not change Docker contexts.

## Diorama

[Diorama](https://github.com/pruefwerk/diorama) now runs in the MQTT scenario at the revision pinned by `diorama-revision`. The fixture uses its long-running `gateway-sim.v0.7` runtime with one synthetic meter. djob requests a device list through a fresh reply topic, then publishes a reboot command and verifies Diorama's independent gateway state. The Mosquitto broker exposes a test-only anonymous TCP listener to Diorama and a TLS listener to djob, using an ephemeral private CA.

The simulator has no Modbus TCP/RTU wire server at this pinned revision, so the Modbus tests continue to use the controlled wire fixture. Neither a transcript event nor an MQTT acknowledgement is evidence of serial timing or downstream quiescence.

The nested K3s fixture needs bridge netfilter on the Docker daemon's Linux host:

```sh
sudo modprobe br_netfilter
sudo sysctl -w net.bridge.bridge-nf-call-iptables=1
```

For Colima, run these in the selected VM with `colima ssh --profile <profile>`
(from a directory available in that VM, such as `/tmp`). CI prepares its ephemeral
Linux runner automatically. The fixture checks this prerequisite and fails early
with setup instructions. Without it, direct Pod connections can work while DNS
through the Kubernetes Service times out.


`TestConsoleDeploymentManifest` checks `deploy/console/kubernetes.yaml` with
`kubectl apply --dry-run=server --validate=strict` against an isolated K3s fixture.
No resources are applied to the operator's current Kubernetes context. The
separate `scripts/test-console-shared.sh` Chromium suite exercises two real
console processes behind a local non-sticky load balancer, including rolling
replacement, acknowledged draft restoration and exact prepared-envelope recovery.


`TestManagedWorkerReleasesOnKubernetes` imports two distinct digest-pinned company
worker images into isolated K3s, publishes separate releases and keeps both
running. It checks that beginning v1 retirement leaves Pod identities and the
execution generation intact, rejects new v1 runs and premature scale-to-zero,
finishes a resource-backed v1 conversation while v2 executes independently, and
removes v1 only after its outstanding count reaches zero. Attempt owner identities
and run snapshots verify that each run used its selected release.
