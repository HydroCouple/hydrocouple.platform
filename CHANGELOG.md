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
- Studies (M3b): ask/tell batches evaluated on evaluator pools, with fenced chunk leases, re-leasing after task loss, failed-candidate policies, per-session evaluation budgets and approvals, lineage, a study manifest and the Pareto set.
- `gym.evaluate@1` evaluator tasks on `openswmm_gymnasium`, reached through the agent's runner gateway; `run_platform_study` drives gymnasium's optimizers on the platform.
- Migration `0002` for studies, batches and evaluation chunks.
- Kubernetes adapter (M4): one Job per attempt, with provider retries off, the execution timeout as the Job deadline, Kueue labels, per-attempt Secrets, outcomes from Job and pod status, events from the pod log, and adoption of running Jobs after an agent restart.
- Signed runner images (M4): the workflow registry pins images by digest. The agent rewrites them to the site's mirror and verifies their cosign signatures against pinned keys, refusing the rest as `untrusted_image`. With `k8s_keychain` it reads a private cloud registry with its workload identity. `images/build.sh` builds, signs and attests the runner and agent images with their SBOMs.
- Object storage backends (M4): Amazon S3 and S3-compatible stores, Azure Blob Storage and Google Cloud Storage, with workload-identity credentials, create-if-absent writes, presigned grants that never replace an object, and SHA-256 checks the client cannot forge. Extras `s3`, `azure` and `gcs`.
- Kubernetes install and onboarding (M4):
  - the agent image;
  - `deploy/kustomize`: a base with a namespaced Role, separate agent and runner identities, network policies limiting runner egress to DNS, the gateway and the store, and the restricted Pod Security Standard; overlays for EKS, AKS, GKE and on-premises clusters; and a Kueue component;
  - `deploy/terraform` modules for the store and workload identities on AWS, Azure and Google Cloud;
  - `hcp agent --config` (agent.json), `--verify` (including a runner pod) and `--dry-run`, and `hcp target register`.
- Run and study deletion (M4): asynchronous erasure by the target's agent once every attempt is confirmed stopped, with a second pass for late writes, `BLOCKED` with a reason when it cannot be confirmed, shared revisions kept, and the run kept as a tombstone. Migration `0003`.
- The M4 gate (`tests/k8s/test_install_on_kubernetes.py`), through the HTTP API against the agent as installed: upload, run, disconnect with the agent pod restarted mid-run, reconnect, inspect, approve and delete; a study on an evaluator pool; numerical parity with a local pool; and revocation draining. It passes on k3s. `deploy/gate` has the runbooks and `run-gate.sh` for running it on EKS, AKS and GKE.
- The shared adapter contract (M5): `tests/contract` runs one set of cases against every scheduler adapter through a harness per adapter, starting with the local pool and Kubernetes. It adds a runner stopped from outside the agent (an eviction on Kubernetes), which must be reported as an interruption. That case found that the Kubernetes adapter reported an evicted pod deleted before it looked as an unknown failure; the Job's pod failure policy reason now says it was an interruption.
- Admission for serverless services (M5):
  - where scratch is held in memory, the request's memory includes it;
  - a service that offers only fixed CPU and memory pairs (`task_sizes`) runs a request on the smallest that fits, and refuses it when none does;
  - an adapter that hasn't declared where scratch lives, or caps runtime without saying how long, rejects the requests that depend on it.
- Runners inherit none of the agent's `HCP_*` settings, so they never hold the agent credential; the evaluator's gateway token reaches it on stdin.
- A run summary: `swmm.simulation@1` writes `summary.json` beside the report and binary output, the engine's end-of-run report as data (continuity, flooded nodes, storage, links, pumps, subcatchments). Each column the engine catalog describes carries its catalog path and the unit the engine declares for the model. The contract is `RunSummary`, published as `run-summary-1.json`. The summary is an artifact in the target's storage, not a required one, and is never sent in events.
- Provenance in PROV-JSON names the AI agent session as an agent acting on behalf of the person, and `AttemptManifest.to_prov()` adds every artifact as an entity the attempt generated.
- A revision whose manifest names `derived_from` is accepted only when its base revision is in the same project with that manifest hash and, when it names a study, the study evaluated that base.
- `swmm.simulation@2`: the same run with `overrides`, element field values (`{path, id, value}` by engine catalog path) written after the model opens and before it initializes. The runner refuses anything but a writable `float`, `int` or `bool` element field, an unknown element or a value of the wrong type as invalid input; the attempt's quality lists each override with the value it replaced. An override run gives the same binary output as a run of the edited, repackaged model.
- Lineage contracts (ecosystem Phase 2): `RevisionManifest.derived_from` names the revision a revision was made from and the study evaluation that chose it, and is absent when unset, so existing manifests keep their digests; `StudyManifest` (`study-manifest-1.json`) holds a study's definition, evaluations and result, with everything that may differ between two executions in `runtime`; `AgentSessionView` and `ApprovalView` are published.
- For agent clients and the knowledge graph: `GET /v1/me` names the agent session of an agent session credential (`agent_session_id`, null for a person); `GET /v1/runs/{run_id}/manifest` returns a published attempt's manifest with its checksum (`PlatformClient.attempt_manifest`); `PlatformClient.list_sessions` lists a project's platform sessions.
