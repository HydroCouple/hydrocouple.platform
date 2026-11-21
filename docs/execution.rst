Run lifecycle and recovery
==========================

A model revision identifies immutable input files. The client submits the revision and a
workflow to a target. The dispatcher makes accepted work available for leasing. The agent
initiates the connection from the target and starts the runner on its scheduler.

.. mermaid::

   flowchart TD
       accTitle: Approval and execution of a simulation
       request["Submit revision and workflow"] --> gate{"Approval required?"}
       gate -->|Yes| review["Human review"]
       review -->|Reject| stop["Rejected"]
       review -->|Approve| accepted["Accepted job"]
       gate -->|No| accepted
       accepted --> lease["Agent claims lease"]
       lease --> runner["Runner verifies inputs and executes"]
       runner --> publish["Publish artifacts and provenance"]

An agent session can require human approval when a request exceeds its delegated budget. Use the
**job ID** to follow state and progress. Use the **run ID** to retrieve the published result.
Reusing a submission key with the same request returns the original job.

Recover from an interrupted attempt
-----------------------------------

A missed heartbeat first makes the target unreachable. Once the abandonment deadline passes, the
reconciler can mark the attempt lost. A retryable failure with remaining budget makes the job
leasable again. The next lease carries a higher fencing token.

.. mermaid::

   flowchart TD
       accTitle: Lease expiry, retry and publication
       lost["Attempt lost"] --> retry{"Retry allowed?"}
       retry -->|No| failed["Terminal job"]
       retry -->|Yes| lease["New lease and fencing token"]
       lease --> current["Current attempt completes"]
       current --> canonical["Canonical result"]
       lost --> late["Old attempt reports late"]
       late --> refused["Stale report refused"]

The attempt manifest records the published execution. Read it with
``client.attempt_manifest(run_id)`` and match its checksum to
``client.results(run_id)["attempt_manifest_sha256"]``. A repeated completion acknowledgement
leaves the canonical result intact.

Choose an adapter
-----------------

+------------+------------------------------------+---------------------------------------+
| Target     | Execution                          | What to check                         |
+============+====================================+=======================================+
| Local pool | Runner processes on the agent host | Work directory, available slots,      |
|            |                                    | engine installation and file-store    |
|            |                                    | access.                               |
+------------+------------------------------------+---------------------------------------+
| Kubernetes | One Job per attempt                | Runner image digest, service account, |
|            |                                    | storage identity, resource limits and |
|            |                                    | target verification.                  |
+------------+------------------------------------+---------------------------------------+

The agent advertises capabilities. Submission checks them against the selected workflow. The
:doc:`architecture decisions <decisions>` describe the adapter contract, cancellation, egress
policies and storage grants.
