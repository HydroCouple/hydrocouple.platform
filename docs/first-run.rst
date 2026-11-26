Run a model on a local pool
===========================

Try the drainage fixture
------------------------

From a source checkout, run the end-to-end example. It registers a target, packages an input
file with external rainfall, uploads the revision, starts a local agent, submits a simulation,
downloads its outputs and compares them to a direct solver run.

.. code:: sh

   python -m pip install -e '.[dev]'
   python -m pytest -q -rs tests/integration/test_end_to_end.py::test_package_run_and_download_with_numerical_parity

The fixture needs an engine wheel that runs on your CPU, PostgreSQL binaries and local socket
access. The test looks for embedded PostgreSQL or the binaries in ``HCP_TEST_PG_BIN``. A passing
run verifies the result manifest, the rainfall checksum and the numerical comparison. Pytest
reports a skip when prerequisites are unavailable.

Start a persistent target
-------------------------

Use a PostgreSQL database and the JWT configuration in the `repository
setup <https://github.com/HydroCouple/hydrocouple.platform#running-it-locally>`__. Set
``HCP_TOKEN`` to your sign-in token and ``HCP_SUBJECT`` to its subject. In the same environment,
migrate the database and create the organization.

.. code:: sh

   hcp migrate
   HCP_ORG=$(hcp bootstrap-org "Example utility" --issuer "$HCP_JWT_ISSUER" --subject "$HCP_SUBJECT")
   export HCP_ORG
   printenv HCP_ORG
   hcp serve --port 8080

Run ``hcp worker`` in a second terminal with the same control-plane settings. In the agent
terminal, set ``HCP_ORG`` to the displayed organization ID, export your sign-in token as
``HCP_TOKEN``, and register a target. The following commands use a local directory for storage
and a separate directory for runner work. The client and agent must both be able to access the
file store for this example.

.. code:: sh

   export HCP_AGENT_CONTROL_PLANE=http://127.0.0.1:8080
   mkdir -p "$PWD/store" "$PWD/work"
   export HCP_AGENT_STORE_URI="file://$PWD/store"
   export HCP_AGENT_WORK_ROOT="$PWD/work"
   target=$(hcp target register --control-plane "$HCP_AGENT_CONTROL_PLANE" \
     --org "$HCP_ORG" --name local --adapter local-pool --storage-uri "$HCP_AGENT_STORE_URI")
   export HCP_TARGET_ID=$(echo "$target" | python -c 'import json,sys; print(json.load(sys.stdin)["target_id"])')
   export HCP_AGENT_CREDENTIAL=$(echo "$target" | python -c 'import json,sys; print(json.load(sys.stdin)["agent_credential"])')
   unset target
   hcp agent --verify
   hcp agent

Submit the first revision
-------------------------

In a client terminal at the repository root, set ``HCP_TARGET_ID``, ``HCP_ORG``, ``HCP_TOKEN``
and ``HCP_AGENT_CONTROL_PLANE`` to the values from setup. Run this Python example once to create
a project, upload the drainage fixture and download its report. Keep the returned project and
revision IDs when submitting subsequent runs.

.. code:: python

   import os
   from pathlib import Path

   from hydrocouple_platform.client import PlatformClient
   from hydrocouple_platform.packaging import package_swmm_model

   env = os.environ
   target_id = env["HCP_TARGET_ID"]
   with PlatformClient(env["HCP_AGENT_CONTROL_PLANE"], env["HCP_TOKEN"]) as client:
       project = client.create_project(env["HCP_ORG"], "Drainage example")
       package = package_swmm_model(Path("tests/data/external_rain/model.inp"), Path("package"))
       revision = client.upload_revision(package, project_id=project["project_id"], target_id=target_id)
       run = client.submit_run({
           "project_id": project["project_id"], "revision_id": revision["revision_id"],
           "target_id": target_id, "workflow": "swmm.simulation", "workflow_version": "1",
       }, idempotency_key="drainage-example-1")
       job = client.wait_for_job(run["job_id"])
       if job["state"] != "SUCCEEDED":
           raise RuntimeError(job)
       client.download(run["run_id"], ["model.rpt", "run.json"], Path("downloaded"))
       print(project["project_id"], revision["revision_id"], run["run_id"])

For a Kubernetes target, use the `onboarding
overlays <https://github.com/HydroCouple/hydrocouple.platform/tree/dev/deploy/kustomize/overlays>`__.
The repository records a passing k3s gate. Managed EKS, AKS and GKE runs have separate
deployment gates.
