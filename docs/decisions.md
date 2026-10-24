# Architecture decisions

These records implement the cloud execution plan revised on October 15, 2026 (scheduler families revised October 24). Each states the decision, why, and what it rules out. A change to any of them updates the plan first.

## 1. Bring your own compute through a pull-based agent

**Decision.** Every compute target is served by an execution agent that runs inside the target's boundary and opens every connection itself. It polls for leases and grant requests, runs work on its own scheduler, and reports heartbeats, events and results. The control plane never holds credentials into a customer account and never connects into it.

**Why.** Customers keep their credentials, private and on-premises sites need only outbound HTTPS, and one code path serves both service-managed and organization-owned targets.

**Revocation.** A revoked target cannot claim new work, but it can still report on attempts it already holds, so in-flight work drains. The agent stops once nothing is left.

**Rules out.** Cross-account AssumeRole or workload federation for organization-owned targets in release 1. Those remain for service-managed targets and a later opt-in managed mode.

## 2. One adapter contract for five scheduler families

**Decision.** Scheduler adapters live inside the agent and implement `capabilities / validate / submit / inspect / cancel / cleanup`. Every adapter reports the same capability record (`contracts/capabilities.py`), and a job is checked against it before a lease is offered. An unknown capability rejects any request that depends on it. Provider retries must report zero, so the platform's retry budget is the only one.

**Status.** The local-pool adapter is implemented. Kubernetes is next (M4). The managed Batch, serverless container job and SLURM adapters follow by demand (M5).

## 3. Job classes

**Decision.** `single`, `evaluator-pool`, `multi-rank` and `analysis`. Release 1 runs `single`. The spec and capability checks already carry pool size and ranks, so evaluator pools (M3b) and multi-rank jobs need no contract change.

## 4. Compositions are the unit of submission; the attempt manifest sits beside the run manifest

**Decision.** A revision is a HydroCouple composition document plus every file it references. A single SWMM model is the one-component case (`org.hydrocouple.openswmm.engine`, configuration `openswmm.component/1`). The runner writes a run manifest that validates against `run-1.0.json`. Because that schema is closed (`additionalProperties: false`), the platform never extends it. Instead it writes `attempt.json` beside it, referencing the run manifest by checksum and adding the execution half of provenance.

**Correction to the plan.** The plan said the run manifest would be "extended with attempt identity, image digest and object versions." The schema forbids that, so the attempt manifest carries those fields.

**SDK gap.** `run-1.0.json` limits `results[].format` to csv, hdf5_ugrid, netcdf_ugrid and geopackage. SWMM's native `.out` and `.rpt` cannot be catalogued there, so the run manifest lists no results and the attempt manifest lists them. A `run-1.1` with SWMM formats would close this.

## 5. PostgreSQL is authoritative; leases are rows

**Decision.** All lifecycle state lives in PostgreSQL. Accepted work enters a transactional outbox in the same transaction as the run and job. The dispatcher moves it to `LEASABLE`, and agents claim it with `SELECT ... FOR UPDATE SKIP LOCKED`. Locks are always taken job first, then attempt. The reconciler handles each stale item in its own short transaction, in age order, bounded per pass.

**Rules out.** A separate message broker as a dependency for the first release, and any per-timestep or per-evaluation rows.

## 6. Fencing and attempt tokens

**Decision.** Each lease creates a new attempt whose number is its fencing token (unique per job). The lease carries an attempt token: an HS256 JWT bound to target, job, attempt and fencing token, with minutes-scale lifetime, renewed on every heartbeat. A report is accepted only if:

- it carries the target's agent credential;
- the attempt token's signature and binding are valid;
- the attempt is the job's current attempt;
- the attempt is not terminal.

An expired attempt token is still accepted while its attempt holds the job. Because the agent credential is required too, a stolen token alone is useless, and a long outage never discards finished work.

The agent reports which attempts it holds on every poll. The control plane answers with the ones to stop (cancelled, superseded, ended or unknown), and re-offers any lease whose response the agent never received. The server also enforces each attempt's execution timeout itself.

Publication is one transaction: it sets the run's canonical attempt only if none is set. Outputs live under per-attempt prefixes and are written create-if-absent.

**Guarantee.** One logical accepted request per idempotency key, and one canonical published result per run. Exactly-once execution is not promised: a superseded attempt may still run, but it can never report or publish, and the agent is told to stop it.

## 7. AI agents act under delegated budgets

**Decision.** An agent session records the client, its version, the model, the conversation reference and a budget (runs, concurrent jobs, spend, targets, job classes). It has its own credential (`hcps_...`), returned once. An MCP server or other orchestrator uses that credential and never holds the person's sign-in token, so the budget applies to every request it makes and cannot be skipped. Every job's actor chain names both the human and the agent session.

Authorization is evaluated for the human; limits are evaluated for the session. A submission outside the budget is stored as `PENDING_APPROVAL`. Only a person can resolve it: an agent session cannot resolve approvals, change membership, register or revoke targets, or start other sessions. With a spend cap and no price source, the cost is unknown, so approval is required.

## 8. Egress policy governs events and diagnostics; the publication record always travels

**Decision.** The agent filters every event and failure diagnostic by the target's policy (`metrics-only`, `summaries`, `full`) before it leaves the site. The attempt manifest (checksums, package paths, parameters, versions, quality numbers) is the publication record and is sent unchanged under every policy. Model files, reports and binary outputs never leave the site's storage unless a user downloads them through a grant.

## 9. Storage grants are minted by the agent

**Decision.** Uploads, downloads and checksum verification are requested through the control plane and answered by the target's agent, using the site's own storage identity. The agent grants only keys under `revisions/` and `runs/`. A revision becomes `READY` only after the agent has verified every object's size and SHA-256. The runner verifies every input again before running.

## Out of release 1

- Checkpoint recovery for anything other than qualified single simulations. The engine's checkpoint session excludes callbacks, external drivers, live forcing, heat, quality and plugins, and composition `execution.mode: resume` is unimplemented.
- Multi-component composition execution: HydroCoupleSDK has no general host executable yet.
- Arbitrary uploaded code, and automatic movement of data between targets.
