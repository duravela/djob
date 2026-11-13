# Two console replicas

`kubernetes.yaml` deploys two consoles behind a TCP load-balancing Service.
Replace the image with an immutable release image containing `/djob-console`,
set the public origin and controller origins, and provision these Secrets:

- `djob-console-tls`: `tls.crt` and `tls.key`, valid for the public hostname.
- `djob-controller-ca`: `ca.crt`, trusted for all configured controller origins.

Start controllers with `--console-sessions` and activate their shared session
storage through the authenticated `/api/jobs/v1/console-sessions/activate` API.
Configure each console with `--shared-sessions` before starting the replicas.
Configure DNS for the Service's external address. Browser TLS terminates at the
console Pods; no HTTP rewrite or forwarded identity header is needed. A cloud
provider or another LoadBalancer implementation must supply that address.

Apply the example only after replacing its image and hostnames:

```sh
kubectl apply -f deploy/console/kubernetes.yaml
kubectl rollout status deployment/djob-console
kubectl rollout restart deployment/djob-console
kubectl rollout status deployment/djob-console
```

The Deployment keeps at least two ready replicas during same-version rolling restarts
with `maxUnavailable: 0` and `maxSurge: 1`; capacity for a third Pod is required.
The disruption budget permits only one voluntary eviction at a time. Preferred
anti-affinity places replicas on different nodes when possible; two replicas on
one node do not protect against losing that node.

On SIGTERM the console first removes `/readyz`, keeps serving routed traffic
for `--drain-delay` (five seconds), then shuts down its listener and gives active
requests `--shutdown-timeout` (30 seconds). The 45-second Pod termination grace
covers both intervals. Liveness stays healthy during draining. Readiness reports
whether this console accepts traffic; it does not certify controller quorum.
Shared-session requests independently require fresh quorum confirmation.

See Kubernetes documentation for [Pod termination](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination)
and [rolling deployments](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#rolling-update-deployment).
Endpoint propagation time depends on the actual load balancer; measure it and
adjust the drain delay and termination grace together.

`scripts/test-console-shared.sh` runs Chromium against two real console
processes behind a readiness-aware, non-sticky load balancer and a TLS controller.
It replaces both processes, checks authenticated reads during replacement,
and recovers editor drafts and the same prepared command without signing in
again. Its local fixture is separate from production hosting.


When upgrading from an older console without editor-draft support, start a
separate pool of upgraded replicas and switch traffic once ready. Draft-enabled
sessions use encryption format 2, which older consoles cannot read. Do not mix
those versions behind one origin. Before runtime session-limit changes, upgrade
all controllers to command version 21 / database format 16. See the console guide
for the policy replacement API and its retained-usage checks.
