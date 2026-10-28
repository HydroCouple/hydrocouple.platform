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
- Runners inherit none of the agent's `HCP_*` settings, so they never hold the agent credential; the evaluator's gateway token reaches it on stdin.
