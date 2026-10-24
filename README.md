# HydroCouple execution platform

Durable, bring-your-own-compute execution of SWMM simulations and HydroCouple compositions, for people, the GUI, and AI agents (including through `openswmm.mcp`).

- **The control plane** (this repository's `server` extra) records requests, enforces permissions, budgets and lifecycle rules, leases work and publishes results. It is PostgreSQL plus a stateless HTTP API, a dispatcher and a reconciler.
- **The execution agent** (`agent` extra) runs in the customer's account or site. It opens every connection itself, claims fenced leases, runs work on its own scheduler, mints storage grants with the site's own identity, and filters what leaves the site.
- **Runners** (`runner` extra) execute approved, versioned workflows next to the engine. They verify every input, then write a HydroCouple run manifest and a platform attempt manifest with provenance.

The design, and the decisions behind it, are in [`docs/decisions.md`](docs/decisions.md). The full plan lives in `openswmm.gui/workplans/SWMM_CLOUD_EXECUTION_PLATFORM_PLAN_2026-10-15.md`.

## Status

| Milestone | State |
| --- | --- |
| M1 Contracts | Job spec, capabilities, agent protocol, events, revision and attempt manifests, provenance, errors; JSON Schemas in `src/hydrocouple_platform/schemas/platform/` |
| M2 Durable core | PostgreSQL schema and migration, shared services, HTTP API, client SDK, outbox, dispatcher, reconciler, agent with the local-pool adapter, agent sessions with budgets and approvals |
| M3 Packaging and runners | `.inp` dependency packaging as a one-component composition, the `swmm.simulation@1` runner on the real engine, run and attempt manifests |
| Next | MCP platform toolset (after the current `openswmm.mcp` work lands), studies and evaluator pools (M3b), Kubernetes adapter (M4) |

## Layout

```text
src/hydrocouple_platform/
  contracts/   versioned data contracts shared by every process
  db/          SQLAlchemy models and Alembic migrations
  services/    authorization, submission, budgets, leases, publication, reconciliation
  api/         FastAPI application (client and agent routes)
  agent/       execution agent, egress filter, scheduler adapters
  runner/      approved workflow handlers (swmm.simulation)
  packaging/   builds immutable revisions from local model files
  client.py    PlatformClient for the GUI, CLI and MCP adapters
  storage.py   object storage interface and the file:// store
  workflows.py the registry of approved workflows
  schemas/     published JSON Schemas (platform) and HydroCoupleSDK copies
```

## Running it locally

```bash
pip install -e ".[dev]"
export HCP_DATABASE_URL="postgresql+psycopg://user@localhost/hcp"
export HCP_JWT_ISSUER="https://your-idp.example" HCP_JWT_AUDIENCE="hydrocouple-platform"
export HCP_JWKS_URL="https://your-idp.example/.well-known/jwks.json"   # or HCP_JWKS_FILE
export HCP_ATTEMPT_TOKEN_SECRET="$(python -c 'import secrets; print(secrets.token_urlsafe(48))')"

hcp migrate
hcp bootstrap-org "Utility A" --issuer "$HCP_JWT_ISSUER" --subject "<your sign-in subject>"
hcp serve --port 8080 &          # API
hcp worker &                     # dispatcher and reconciler
```

Register a target with `POST /v1/organizations/{org}/targets`. The response contains the agent credential, shown once. Then start the agent where the work should run:

```bash
export HCP_AGENT_CREDENTIAL="hcpa_..."
hcp agent --control-plane http://localhost:8080 --store file:///data/hcp-store \
          --work-root /data/hcp-work --slots 2 --egress summaries
```

The control plane refuses to start without a token verifier; there is no unauthenticated mode.

An AI agent (for example through `openswmm.mcp`) works under an agent session: a person starts one with `POST /v1/agent-sessions`, choosing its budget, and gives the returned `hcps_...` credential to the agent. Everything the agent submits is checked against that budget; anything beyond it waits for a person's approval.

## Tests

```bash
pytest                                   # everything that can run here
pytest -m "not engine"                   # skip tests that run the real Solver
```

- **PostgreSQL tests** need a server. Set `HCP_TEST_DATABASE_URL`, or set `HCP_TEST_PG_BIN` to a folder containing `initdb` and `pg_ctl`; with neither set, the `pgserver` package's binaries are used where it has wheels (macOS arm64, Linux x86_64). Each test gets its own database cloned from a migrated template.
- **Engine tests** run the real `openswmm` Solver and skip when the installed engine cannot run on this CPU.
- Files a test produces are kept under `test_artifacts/<module>/<test>/` for review.

## License

Apache License 2.0; see [LICENSE](LICENSE) and [NOTICE](NOTICE). Contributions require the [CLA](CLA.md).
