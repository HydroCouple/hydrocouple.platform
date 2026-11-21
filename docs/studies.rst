Run an optimization study
=========================

Use a study when several model evaluations share the same project, revision and target. The
optimizer proposes candidates through an ask/tell interface. Evaluator tasks claim chunks, run
the candidates and return their objectives. Study lineage records which evaluations contributed
to the result.

.. mermaid::

   flowchart TD
       accTitle: Ask and tell through a leased evaluator pool
       optimizer["Optimizer proposes candidates"] --> batch["Submit batch"]
       batch --> pool["Evaluator pool leases chunks"]
       pool --> results["Return objectives and failures"]
       results --> done{"Budget exhausted?"}
       done -->|No| optimizer
       done -->|Yes| finish["Close study and record results"]

Install the gym extra alongside the client and evaluator, and use a target supporting
``gym.evaluate@1``. The `study
example <https://github.com/HydroCouple/hydrocouple.platform#studies>`__ supplies the
environment configuration and uses ``run_platform_study``. Its output includes
``evaluations.jsonl`` and ``result.json``. For an existing ask/tell optimizer, use
``PlatformClient.create_study``, ``submit_batch``, ``wait_for_batch`` and ``close_study``. Use
the batch's failed-candidate policy when interpreting missing objectives.
