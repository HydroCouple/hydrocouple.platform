# Architecture decisions

These records implement the cloud execution plan revised on October 15, 2026 (scheduler families revised October 24). Each states the decision, why, and what it rules out. A change to any of them updates the plan first.

## 1. Bring your own compute through a pull-based agent

**Decision.** Every compute target is served by an execution agent that runs inside the target's boundary and opens every connection itself. It polls for leases and grant requests, runs work on its own scheduler, and reports heartbeats, events and results. The control plane never holds credentials into a customer account and never connects into it.

**Why.** Customers keep their credentials, private and on-premises sites need only outbound HTTPS, and one code path serves both service-managed and organization-owned targets.

**Revocation.** A revoked target cannot claim new work, but it can still report on attempts it already holds, so in-flight work drains. The agent stops once nothing is left.

**Rules out.** Cross-account AssumeRole or workload federation for organization-owned targets in release 1. Those remain for service-managed targets and a later opt-in managed mode.

## 2. One adapter contract for five scheduler families

**Decision.** Scheduler adapters live inside the agent and implement `capabilities / validate / submit / inspect / cancel / cleanup`. Every adapter reports the same capability record (`contracts/capabilities.py`), and a job is checked against it before a lease is offered. An unknown capability rejects any request that depends on it. Provider retries must report zero, so the platform's retry budget is the only one.

**Status.** The local-pool and Kubernetes adapters are implemented (M4). The managed Batch, serverless container job and SLURM adapters follow by demand (M5).

**Kubernetes.** Each attempt is one `batch/v1` Job:
- `backoffLimit: 0`, and a pod failure policy that fails the Job on an eviction or preemption, which is reported as an interruption.
- The attempt's execution timeout is the Job's `activeDeadlineSeconds`.
- A Kueue LocalQueue label when the target names one, so Kueue decides when the Job starts.

Each evaluator pool task is its own single-pod Job, because each task has its own attempt identity; Indexed Jobs would put several tasks under one.

The spec, and an evaluator's gateway token, reach the runner in a per-attempt Secret owned by the Job. The Secret also keeps the lease, so an agent that restarts adopts the Jobs still running, and the next poll says which of them to stop.

Events come from the pod log. The outcome comes from the Job and pod status:
- an OOM kill is the out-of-memory class;
- an eviction is an interruption;
- an image that cannot be pulled within the start grace is a missing dependency;
- a Job deleted behind the agent's back is lost.

Runner pods run under the `restricted` Pod Security Standard, with no service-account token. The adapter polls with one list of Jobs and pods per interval, rather than a watch.

**Install.** `deploy/kustomize` installs the agent in its own namespace. The agent's Role is namespaced, the only permissions it has, and a test keeps it equal to what the adapter checks. Agent and runners have separate ServiceAccounts, and so separate workload identities: the runners' can only read revisions and write runs.

Network policies limit runner egress to DNS, the agent's gateway and the store's endpoint, and runners accept no connections. The store's endpoint includes where the runners' workload identity gets its token:
- STS on EKS (IRSA), through an interface endpoint;
- Microsoft Entra ID on AKS, by its service tag's prefixes;
- the metadata server on GKE.

Without it a runner can't authenticate to the store.

How soon a pod's policy takes effect depends on the CNI. k3s's kube-router applies it a moment after the pod starts, and the install test measured it enforced within a second. Sites that need enforcement before a runner's first packet should use a CNI that programs policy when the pod is created (Calico, Cilium).

## 3. Job classes

**Decision.** `single`, `evaluator-pool`, `multi-rank` and `analysis`. `single` and `evaluator-pool` run (the pool since M3b, §10). The spec and capability checks already carry ranks, so multi-rank jobs need no contract change.

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

**Backends.** `file://` (local, or an NFS claim shared with runner pods), Amazon S3 and S3-compatible stores (`s3://`), Azure Blob Storage (`azure://`) and Google Cloud Storage (`gs://`).
- Credentials come from the environment's workload identity, so nothing is stored.
- Every backend writes create-if-absent when asked (`If-None-Match: *`, or `ifGenerationMatch=0` on Cloud Storage), and upload grants never replace an object.
- S3 upload grants carry the expected SHA-256, which Amazon S3 enforces.

**Checksums.** The SHA-256 a backend reports comes from the store only where the store verified it: whole-object uploads on Amazon S3. Everywhere else the agent reads the object:
- multipart S3 uploads;
- S3-compatible endpoints, which may keep a client's checksum unchecked;
- Blob Storage and Cloud Storage, which keep no SHA-256.

So the agent's verification never trusts a checksum the client supplied.

## 10. Studies are ask/tell; a pool is task jobs; a chunk is the evaluation lease

**Decision.** A study is a registered evaluation function. It records:

- a revision;
- an evaluator workflow and its parameters;
- the decision dimensions and the objectives;
- a seed;
- a failed-candidate policy (`fail` or `penalty`, as in `openswmm_gymnasium`);
- an evaluation budget and a pool size.

Callers submit batches of candidates and read back their costs. The optimizer always runs with the caller: the managed algorithms run through `hydrocouple_platform.gym.run_platform_study`, and an AI agent can run its own search. The control plane never imports gymnasium.

**The pool.** An evaluator pool of size N is N `evaluator-pool` task jobs under the study's run. Each task keeps the engine loaded. Each task is a normal job with attempts, fencing and retries, and its attempt manifest is recorded, but no task is the run's canonical result: the study holds the results.

**Chunks.** A batch is split into chunks of up to 32 candidates, about two per task. A chunk is leased with a lease number that fences its completion, which is accepted only:

- from the holding attempt;
- at the current lease number;
- while that attempt holds its job.

A late result from an earlier lease is discarded. When an attempt ends, its chunks are released, and the reconciler re-offers chunks whose leases expired. After three deliveries without a result, the chunk's candidates fail under the study's policy.

A task asking for a lease while it still holds a chunk gets that chunk again: the earlier answer was lost on the way.

**Evaluations.** Each evaluation must finish within the study's `evaluation_timeout_seconds`. One that runs over fails as a candidate, under the study's policy, and the task carries on. So does a candidate that returns a long error or a non-finite cost: its error is cut to 2,000 characters, and invalid costs become an error.

**Rows and records.** Rows are per study, batch and chunk, never per evaluation. The evaluator writes each chunk's records as an append-only JSON-lines object in the target's storage. The chunk row keeps the object's key and SHA-256, and the study manifest indexes them.

**Retiring tasks.** A task retires when the study has ended, or when it has been idle for the idle timeout. The control plane decides this under the study lock, so a batch submitted at the same moment starts a new task. Every batch submission, every task end and the reconciler top the pool back up to N while work remains.

**A pool that cannot work.** The pool is not replaced forever:

- An evaluator that cannot start fails the study at once, as invalid input.
- After three tasks in a row fail without completing a chunk, the study fails. Any completed chunk resets the count.
- A queued task that passes its queue deadline fails the study only when it is the study's last task.

**Runner gateway.** An evaluator talks to its agent through a runner gateway: loopback HTTP for the local pool, with a token per attempt. The token reaches the runner on stdin, never in its environment, and no runner inherits the agent's `HCP_*` settings, so runners never hold the agent credential. The agent relays each call with its own credential and the attempt token. It verifies each records object in storage, and applies the egress policy to candidate error text. Objective values are metrics and always travel.

**Budgets.** An agent session's budget gains `max_evaluations`. A study reserves its evaluation budget when it is created, and its pool size against the session's concurrent-attempt limit. A study beyond the session's budget is `PENDING_APPROVAL`. Only a person's approval moves it on, and until then it has no tasks and cannot be closed. An agent session submits batches only to studies it created.

**Rules out.**

- Evaluation rows in PostgreSQL.
- Optimizers running inside the control plane.
- Runners with platform credentials.

**Known limits.**

- Every lease and completion for one study takes the study's row lock. That is measured fine for a few tasks: 1,000 twin-tank evaluations on three tasks took about a minute. Pools of hundreds of tasks will need the lock narrowed.
- A pool's size is not checked against the target's capacity. Tasks beyond it wait in the queue.
- Under the `fail` policy, chunks run in parallel, so a study can fail on a later chunk while earlier ones are still unevaluated. `run_platform_study` writes the records evaluated before the failure, as `run_study` does, and reports the rest as not evaluated, where `run_study` would have evaluated them.
- Platypus draws from Python's `random`, which `run_study` does not seed. `run_platform_study` seeds it with the study's seed, so a same-seeded local comparison must seed it too.

## 11. Images are pinned by digest, signed, and verified by the agent

**Decision.** The control plane's workflow registry names each workflow's runner image by digest (`HCP_WORKFLOW_IMAGES`). A container target runs only workflows listed there.

Before a container adapter starts an attempt, the agent:
- rewrites the image to the site's registry mirror, when it has one, keeping the digest;
- verifies the image's cosign signature against the public keys the site pins.

An image no pinned key signed fails the attempt as `untrusted_image`, which is never retried. Nothing is started. An agent with no pinned keys runs no images, unless it was started explicitly with unsigned images allowed, for development clusters.

The runner reads the image from its spec, so the attempt manifest's provenance names the digest that ran.

**Images.** `images/build.sh` builds the runner and agent images from a wheelhouse, on a base pinned by digest, then:
- pushes it;
- writes an SPDX SBOM with syft;
- signs the image with cosign and attaches the SBOM as a signed attestation.

**Transparency log.** Signatures are checked against the pinned keys alone by default, because a private site may not reach a transparency log. `require_transparency_log` adds the check where the site can.

**Registry credentials.** cosign reads signatures from the registry itself. In an agent pod it has no Docker configuration, so a private registry would refuse it and every image would be untrusted. With `k8s_keychain`, set in the EKS, AKS and GKE overlays, cosign uses the agent's workload identity for ECR, ACR and Artifact Registry. The agent's identity therefore needs read access to the registry, as the nodes do.

**Rules out.**
- Images by tag.
- Images chosen by parameters.
- Running an unverified image because verification was unavailable.

## 12. Deletion is asynchronous, confirmed by the agent, and blocked rather than assumed

**Decision.** `DELETE /v1/runs/{id}` (and `/v1/studies/{id}`, through its run) records a deletion and marks the run deleting. A deleting run gets no new work, no new grants and no results. Its active attempts are asked to stop, and an attempt that finishes anyway is not published.

**Waiting for writers.** Erasure starts only once no attempt of the run may still write. An attempt counts as a possible writer until its agent confirms it stopped:
- by reporting its result;
- or by polling without holding it once it ended (`attempts.released_at`).

An attempt the reconciler marked lost or timed out on the server's clock is not confirmed. If confirmation does not come within `deletion_writer_wait_seconds`, the deletion is `BLOCKED`. It says which attempt and when the agent last polled, and it continues when the agent confirms.

**Erasure.** The target's agent deletes every object under the run's prefix, including old versions on S3, versioned Cloud Storage and versioned Blob Storage, and reports how many there were. A second pass after `deletion_recheck_seconds` removes anything written late. The deletion is `ERASED` only when a pass finds nothing left, and nothing is described as deleted before that.

**Delivery.** Each poll that hands out an erasure marks it delivered, so a long poll returns as soon as one is due, as it does for leases and grant requests. Otherwise a poll that keeps waiting would mark erasures delivered and never return them.

**What is kept.** Revisions are shared inputs and are kept, and the agent erases only `runs/<org>/<project>/<run>/` prefixes. The run's record stays as a tombstone, and its artifact records are removed.

**Limits.** Blob Storage soft delete keeps deleted blobs recoverable for the account's retention period, and the module sets 7 days.

## Out of release 1

- Checkpoint recovery for anything other than qualified single simulations. The engine's checkpoint session excludes callbacks, external drivers, live forcing, heat, quality and plugins, and composition `execution.mode: resume` is unimplemented.
- Multi-component composition execution: HydroCoupleSDK has no general host executable yet.
- Arbitrary uploaded code, and automatic movement of data between targets.
