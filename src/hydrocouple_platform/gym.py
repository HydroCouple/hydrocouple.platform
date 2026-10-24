"""Drive a platform study with ``openswmm_gymnasium.optimize``.

``run_platform_study`` is ``openswmm_gymnasium.optimize.run_study`` with the
platform as the evaluator pool: the same ask/tell loop and optimizers, and the
same ``evaluations.jsonl`` and ``result.json`` in a folder the user can read,
while every batch is evaluated by the study's pool on its compute target.

The loop mirrors ``run_study`` line for line (``tests/integration/
test_studies_engine.py`` checks that both produce the same records). It needs
``openswmm_gymnasium`` with its ``spec`` extra, and ``platypus`` for the MOEAs.
"""

from __future__ import annotations

import json
import random
from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np

from .client import PlatformClient
from .contracts.studies import Dimension, Objective, StudyCreate

DEFAULT_BATCH_PER_TASK = 4


def study_request(env_config, opt, *, project_id: str, revision_id: str, target_id: str,
                  inp_path: str, session_id: str | None = None, pool_size: int = 1,
                  workers: int = 1, threads: int = 1, tool_call_id: str | None = None,
                  idle_timeout_seconds: float | None = None,
                  evaluation_timeout_seconds: float | None = None) -> StudyCreate:
    """The study for an env config and ``OptimizationConfig``.

    ``inp_path`` is the model's path inside the revision; the env config's own path
    is a local one and never leaves this machine.
    """
    from openswmm_gymnasium.optimize import (
        PLATYPUS_ALGORITHMS,
        design_dimensions,
        flat_bounds,
        objective_directions,
    )

    dims = design_dimensions(env_config)
    low, high = flat_bounds(dims)
    labels = [label for d in dims for label in d.labels]
    payload = env_config.model_dump(mode="json")
    payload.update(inp_path=inp_path, rpt_path=None, out_path=None)
    parameters: dict[str, Any] = {"env_config": payload,
                                  "max_steps_per_episode": opt.max_steps_per_episode,
                                  "threads": threads, "workers": workers}
    if idle_timeout_seconds is not None:
        parameters["idle_timeout_seconds"] = idle_timeout_seconds
    if evaluation_timeout_seconds is not None:
        parameters["evaluation_timeout_seconds"] = evaluation_timeout_seconds
    # Platypus stops at the first generation that reaches the budget, so a MOEA may
    # evaluate up to one population beyond it.
    budget = opt.budget + (opt.population_size if opt.algorithm in PLATYPUS_ALGORITHMS else 0)
    return StudyCreate(
        project_id=project_id, session_id=session_id, revision_id=revision_id,
        target_id=target_id, parameters=parameters,
        dimensions=[Dimension(label=label, low=float(lo), high=float(hi))
                    for label, lo, hi in zip(labels, low, high, strict=True)],
        objectives=[Objective(name=n, direction=d)
                    for n, d in objective_directions(env_config).items()],
        seed=opt.seed, on_failure=opt.on_failure, failure_penalty=opt.failure_penalty,
        max_evaluations=budget, pool_size=pool_size, tool_call_id=tool_call_id)


class PlatformPool:
    """The evaluator-pool interface of ``openswmm_gymnasium.optimize`` over a study.

    Each ``evaluate`` call is one batch; its idempotency key is derived from the
    study and the batch number, so a retried call never queues a batch twice.
    """

    def __init__(self, client: PlatformClient, study_id: str, *, timeout: float = 24 * 3600.0):
        self._client, self.study_id, self._timeout = client, study_id, timeout
        self._batches = 0

    def evaluate(self, vectors, should_stop: Callable[[], bool] = lambda: False) -> list:
        from openswmm_gymnasium.optimize import Evaluation, StudyCancelled

        self._batches += 1
        candidates = [np.asarray(v, dtype=float).tolist() for v in vectors]
        batch = self._client.submit_batch(self.study_id, candidates,
                                          idempotency_key=f"{self.study_id}-b{self._batches}")
        view = self._client.wait_for_batch(self.study_id, batch["batch_id"], self._timeout,
                                           should_stop=should_stop)
        if view["state"] == "QUEUED" and should_stop():
            raise StudyCancelled()
        if view["state"] != "DONE":
            study = self._client.study(self.study_id)
            raise RuntimeError(f"batch {view['batch_id']} ended {view['state']}: "
                               f"{study.get('failure')}")
        return [Evaluation(costs=None if r["failed"] else r["costs"], steps=r["steps"],
                           error=r["error"]) for r in view["results"]]


def run_platform_study(env_config, opt, output_dir: str | Path, *, client: PlatformClient,
                       project_id: str, revision_id: str, target_id: str, inp_path: str,
                       session_id: str | None = None, pool_size: int = 1, workers: int = 1,
                       batch_size: int | None = None, idempotency_key: str | None = None,
                       should_stop: Callable[[], bool] = lambda: False,
                       on_evaluation: Callable[[dict[str, Any]], None] | None = None,
                       ) -> dict[str, Any]:
    """Search an env config's space on the platform; return ``run_study``'s result payload.

    ``batch_size`` is the random and grid search batch (a MOEA's batch is its
    generation); it changes how candidates are grouped, never which are drawn.
    With a seed, Python's ``random`` is seeded too, because Platypus draws from it.
    """
    from openswmm_gymnasium.optimize import (
        PLATYPUS_ALGORITHMS,
        StudyCancelled,
        assemble_result,
        design_dimensions,
        flat_bounds,
        make_optimizer,
        objective_directions,
        split_vector,
    )
    from openswmm_gymnasium.spec.envs import json_safe

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    dims = design_dimensions(env_config)
    names = list(objective_directions(env_config))
    low, high = flat_bounds(dims)
    request = study_request(env_config, opt, project_id=project_id, revision_id=revision_id,
                            target_id=target_id, inp_path=inp_path, session_id=session_id,
                            pool_size=pool_size, workers=workers)
    study = client.create_study(request, idempotency_key)
    study_id = study["study_id"]
    pool = PlatformPool(client, study_id)
    if opt.seed is not None and opt.algorithm in PLATYPUS_ALGORITHMS:
        random.seed(opt.seed)  # just before the MOEA starts drawing from it
    optimizer = make_optimizer(
        opt.algorithm, low, high, len(names), budget=opt.budget, seed=opt.seed,
        population_size=opt.population_size, grid_levels=opt.grid_levels,
        batch=batch_size or DEFAULT_BATCH_PER_TASK * pool_size * workers)
    evaluations: list[dict[str, Any]] = []
    try:
        with (output_dir / "evaluations.jsonl").open("w", encoding="utf-8") as log:
            while True:
                if should_stop():
                    raise StudyCancelled()
                batch = optimizer.ask()
                if not batch:
                    break
                outcomes = pool.evaluate(batch, should_stop)
                told = []
                for vector, outcome in zip(batch, outcomes, strict=True):
                    record: dict[str, Any] = {
                        "evaluation": len(evaluations) + 1,
                        "decisions": json_safe(split_vector(dims, vector)),
                    }
                    if outcome.error is None:
                        costs = outcome.costs
                    elif opt.on_failure == "fail":
                        raise RuntimeError(
                            f"evaluation {record['evaluation']} failed: {outcome.error}")
                    else:
                        costs = [opt.failure_penalty] * len(names)
                        record.update(failed=True, error=outcome.error)
                    record["objectives"] = dict(zip(names, costs, strict=True))
                    record["steps"] = outcome.steps
                    evaluations.append(record)
                    log.write(json.dumps(record) + "\n")
                    log.flush()
                    if on_evaluation is not None:
                        on_evaluation(record)
                    told.append(costs)
                optimizer.tell(batch, told)
    except StudyCancelled:
        client.cancel_study(study_id)
        raise
    finally:
        optimizer.close()
    client.close_study(study_id)
    result = assemble_result(evaluations, names)
    result["optimization"] = opt.resolved()
    result["execution"] = {"platform": {"study_id": study_id, "target_id": target_id,
                                        "pool_size": pool_size, "workers": workers},
                           "on_failure": opt.on_failure}
    result = json_safe(result)
    (output_dir / "result.json").write_text(json.dumps(result, indent=2) + "\n",
                                            encoding="utf-8")
    return result
