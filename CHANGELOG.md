# Changelog

## 0.1.0.dev0 (unreleased)

First implementation of the cloud execution plan (milestones M1 to M3).

- Versioned contracts for job specs, adapter capabilities, the agent protocol, events, revision and attempt manifests, provenance and errors, published as JSON Schemas.
- PostgreSQL schema with lifecycle check constraints, and the initial migration.
- Shared services: authorization from the platform's own membership records, idempotent submission, AI agent sessions with their own credentials, budgets and human approval, fenced leases, heartbeats, verified publication, cancellation, results and audit.
- Transactional outbox dispatcher, and a reconciler for missed heartbeats, abandoned attempts, queue deadlines and expired approvals or grants.
- HTTP API with typed error bodies, and `PlatformClient`.
- Execution agent with egress filtering, storage grants, output verification, buffering through control-plane outages, and the local-pool adapter.
- Packaging of SWMM models as one-component HydroCouple compositions, including external files.
- `swmm.simulation@1` runner on the OpenSWMM engine, writing a `run-1.0` run manifest and a platform attempt manifest.
