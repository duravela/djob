# Company extensions

Use `extensions/<organization>` for company-owned modules developed in a djob checkout. Each module owns its business jobs, integrations and application assembly:

```text
<organization>/
  go.mod
  registry.go
  jobs/<job>/
  integrations/<device-or-protocol>/
  cmd/<application>/
```

Start with [`examples/company-worker`](../examples/company-worker/README.md) and copy it, change its module path and add it to the local Go workspace with `go work use`. Registration is explicit; placing a directory here does not load or execute it.

The djob engine, [job-authoring SDK](../sdk/go/README.md) and first-party protocol packages stay outside this directory. Add reusable protocols intended for djob itself under `pkg/integrations/`. Company mappings and business rules belong in the company module. Business packages can import the SDK alone.

Committed extensions share this repository's visibility. Private implementations can use the same layout in a private company project. This directory contains guidance only; each company chooses the visibility of its own code.

The SDK supports independent gRPC workers with TLS, keeping company code outside the Raft controller. [`examples/company-node`](../examples/company-node/README.md) remains an embedding alternative. Both use the same handler and driver contracts.
