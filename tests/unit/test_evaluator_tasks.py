"""Evaluator tasks without a database: the runner, the agent's relay and the client driver.

These cover the October 24 review of M3b: runners get no platform credentials,
one bad candidate cannot fail a task, the evaluation timeout is enforced, a task
whose evaluator cannot start fails as invalid input, a failed batch keeps its
evaluated records, and the Pareto rule is gymnasium's.
"""

from __future__ import annotations

import io
import json
import math
import random
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from hydrocouple_platform.agent.adapters.local import runner_environment
from hydrocouple_platform.agent.agent import _trimmed
from hydrocouple_platform.contracts.studies import EvaluationResult
from hydrocouple_platform.runner import EXIT_CANCELLED, EXIT_INVALID_INPUT, RunnerFailure
from hydrocouple_platform.runner import __main__ as runner_main
from hydrocouple_platform.runner import gym as runner_gym
from hydrocouple_platform.services.studies import pareto_indices

# -- runners get no platform credentials (finding 5) ---------------------------------


def test_a_runner_inherits_none_of_the_agent_settings(monkeypatch):
    monkeypatch.setenv("HCP_AGENT_CREDENTIAL", "hcpt_secret")
    monkeypatch.setenv("HCP_ATTEMPT_TOKEN_SECRET", "s" * 48)
    monkeypatch.setenv("PATH_FOR_THE_TEST", "kept")
    env = runner_environment(2, {"HCP_GATEWAY_URL": "http://127.0.0.1:1"},
                             {"HCP_RUNNER_TOKEN": "hcpr_token"})
    assert "HCP_AGENT_CREDENTIAL" not in env and "HCP_ATTEMPT_TOKEN_SECRET" not in env
    assert "hcpr_token" not in json.dumps(env)  # the token goes on stdin
    assert env["HCP_GATEWAY_URL"] == "http://127.0.0.1:1"
    assert env["HCP_RUNNER_SECRETS_STDIN"] == "1"
    assert env["PATH_FOR_THE_TEST"] == "kept" and env["OMP_NUM_THREADS"] == "2"
    assert "HCP_RUNNER_SECRETS_STDIN" not in runner_environment(1, None, None)


def test_the_runner_reads_its_secrets_from_stdin(monkeypatch):
    monkeypatch.delenv("HCP_RUNNER_SECRETS_STDIN", raising=False)
    monkeypatch.setattr("sys.stdin", io.StringIO('{"HCP_RUNNER_TOKEN": "hcpr_x"}'))
    assert runner_main._secrets() == {}  # nothing is read unless the agent sent secrets
    monkeypatch.setenv("HCP_RUNNER_SECRETS_STDIN", "1")
    assert runner_main._secrets() == {"HCP_RUNNER_TOKEN": "hcpr_x"}
    monkeypatch.setattr("sys.stdin", io.StringIO('{"HCP_RUNNER_TOKEN": 5}'))
    with pytest.raises(ValueError):
        runner_main._secrets()


# -- one bad candidate cannot fail its task (finding 7) -------------------------------


def test_long_errors_and_non_finite_costs_become_reportable_results():
    long = _trimmed({"costs": None, "steps": 3, "error": "x" * 5000})
    assert len(long["error"]) == 2000 and long["steps"] == 3
    for bad in (float("nan"), float("inf")):
        nan = _trimmed({"costs": [1.0, bad], "steps": 1, "error": None})
        assert nan["costs"] is None and "invalid costs" in nan["error"]
        EvaluationResult.model_validate(nan)
    ok = {"costs": [1.0, 2.0], "steps": 1, "error": None}
    assert _trimmed(ok) == ok
    for raw in (long, ok):
        EvaluationResult.model_validate(raw)


def test_the_runner_checks_each_candidate_before_reporting_it():
    Outcome = SimpleNamespace
    assert runner_gym._checked(Outcome(costs=[1, 2.5], error=None), 2) == ([1.0, 2.5], None)
    assert runner_gym._checked(Outcome(costs=None, error="e" * 3000), 2) == (None, "e" * 2000)
    for costs in ([1.0, math.nan], [1.0], None):
        reported, error = runner_gym._checked(Outcome(costs=costs, error=None), 2)
        assert reported is None and "invalid costs" in error


# -- the evaluation timeout (finding 8) -----------------------------------------------


@pytest.fixture
def gymnasium():
    return pytest.importorskip("openswmm_gymnasium.optimize")


class FakePool:
    """Gymnasium's pool interface; a negative first decision is an episode that hangs."""

    def __init__(self, gymnasium):
        self.gymnasium, self.calls = gymnasium, []

    def evaluate(self, vectors, should_stop):
        self.calls.append([v[0] for v in vectors])
        if any(v[0] < 0 for v in vectors):
            while not should_stop():
                time.sleep(0.01)
            raise self.gymnasium.StudyCancelled()
        return [self.gymnasium.Evaluation(costs=[float(v[0])], steps=1) for v in vectors]


def test_an_evaluation_that_runs_over_fails_and_the_rest_keep_their_results(gymnasium):
    pool, ctx = FakePool(gymnasium), SimpleNamespace(cancel=threading.Event())
    out = runner_gym._evaluate(pool, [[1], [-1], [2], [3]], workers=2, timeout=0.1, ctx=ctx)
    assert [o.costs for o in out] == [[1.0], None, [2.0], [3.0]]
    assert out[1].error == "did not finish within 0.1 s (evaluation_timeout_seconds)"
    # The group that ran over is evaluated again one candidate at a time.
    assert pool.calls == [[1, -1], [1], [-1], [2, 3]]


def test_cancelling_a_task_still_stops_it(gymnasium):
    ctx = SimpleNamespace(cancel=threading.Event())
    ctx.cancel.set()
    with pytest.raises(RunnerFailure) as stopped:
        runner_gym._evaluate(FakePool(gymnasium), [[-1]], workers=1, timeout=60, ctx=ctx)
    assert stopped.value.exit_code == EXIT_CANCELLED


# -- an evaluator that cannot start (finding 3) ----------------------------------------


ENV_CONFIG = {
    "env_type": "cip", "inp_path": "model/model.inp",
    "design_factories": [
        {"kind": "node_max_depth", "params": {"node_ids": ["T1", "T2"], "low": 2.0, "high": 4.0}},
        {"kind": "link_diameter", "params": {"link_ids": ["OUT"], "low": 0.5, "high": 3.0}}],
    "observations": {"node_depths": ["T1", "T2"]},
    "reward_terms": [{"kind": "flooding_volume", "params": {}},
                     {"kind": "peak_outflow", "params": {"link_ids": ["OUT"]}}],
}
COMPOSITION = {"components": [{
    "info": {"component_info_id": "org.hydrocouple.openswmm.engine"},
    "arguments": {"configuration": {"schema": "openswmm.component/1",
                                    "model": "model/model.inp"}}}]}


def test_an_evaluator_that_cannot_start_fails_as_invalid_input(gymnasium, monkeypatch):
    def cannot_start(*args, **kwargs):
        raise RuntimeError("evaluation worker failed to start: no such node T9")

    monkeypatch.setattr(gymnasium, "WorkerPool", cannot_start)
    monkeypatch.setenv("HCP_GATEWAY_URL", "http://127.0.0.1:9")
    study = {"study_id": "std_test", "seed": 1,
             "dimensions": [{"label": "node_max_depth:T1", "low": 2.0, "high": 4.0},
                            {"label": "node_max_depth:T2", "low": 2.0, "high": 4.0},
                            {"label": "link_diameter:OUT", "low": 0.5, "high": 3.0}],
             "objectives": [{"name": "flooding_volume", "direction": "minimize"},
                            {"name": "peak_outflow", "direction": "minimize"}]}
    ctx = SimpleNamespace(
        spec=SimpleNamespace(workload=SimpleNamespace(parameters={
            "env_config": ENV_CONFIG, "study": study, "max_steps_per_episode": 10})),
        secrets={"HCP_RUNNER_TOKEN": "hcpr_test"}, inputs=Path("/nonexistent/inputs"),
        events=SimpleNamespace(emit=lambda *a, **k: None), cancel=threading.Event())
    with pytest.raises(RunnerFailure) as failed:
        runner_gym.run(ctx, COMPOSITION)
    assert failed.value.exit_code == EXIT_INVALID_INPUT
    assert "could not start" in str(failed.value) and "T9" in str(failed.value)


# -- the client driver on a failed batch (finding 12) ----------------------------------


class FailedBatchClient:
    """A study whose second batch (evaluations 11 to 14) failed at evaluation 12."""

    def __init__(self, state="FAILED"):
        self.state = state
        self.records = [
            {"evaluation": 10, "costs": [9.0, 9.0], "failed": False, "error": None, "steps": 1},
            {"evaluation": 11, "costs": [1.0, 2.0], "failed": False, "error": None, "steps": 1},
            {"evaluation": 12, "costs": None, "failed": True, "error": "boom", "steps": 0},
        ]

    def submit_batch(self, study_id, candidates, idempotency_key):
        return {"batch_id": "bat_2"}

    def wait_for_batch(self, study_id, batch_id, timeout, should_stop):
        return {"batch_id": batch_id, "state": self.state, "first_evaluation": 11, "size": 4}

    def study_evaluations(self, study_id, after=0, limit=500):
        page = [r for r in self.records if r["evaluation"] > after][:1]  # one per page
        return {"evaluations": page, "next_after": page[-1]["evaluation"] if page else after,
                "complete": not page}

    def study(self, study_id):
        return {"failure": {"reason": "evaluation 12 failed and the study's policy is 'fail'"}}


def test_a_failed_batch_keeps_the_candidates_evaluated_before_the_failure(gymnasium):
    from hydrocouple_platform.gym import PlatformPool

    out = PlatformPool(FailedBatchClient(), "std_test").evaluate([[3.0, 3.0, 1.0]] * 4)
    assert [o.costs for o in out] == [[1.0, 2.0], None, None, None]
    assert [o.error for o in out] == [
        None, "boom", *["not evaluated: evaluation 12 failed and the study's policy is 'fail'"] * 2]


def test_a_cancelled_batch_stops_the_driver_as_a_cancelled_study(gymnasium):
    from hydrocouple_platform.gym import PlatformPool

    with pytest.raises(gymnasium.StudyCancelled):
        PlatformPool(FailedBatchClient("CANCELLED"), "std_test").evaluate([[3.0, 3.0, 1.0]])


# -- the Pareto rule is gymnasium's (finding 12) ---------------------------------------


def test_the_pareto_set_matches_assemble_result_with_near_duplicates(gymnasium):
    names = ["a", "b"]
    rng = random.Random(5)
    points = [[rng.random(), rng.random()] for _ in range(200)]
    front_point = min(points, key=lambda p: p[0])
    # Near-duplicates of a front point: dominated, but allclose, so gymnasium keeps them.
    points += [[front_point[0] + 1e-10, front_point[1]], list(front_point)]
    records = [{"evaluation": i + 1, "objectives": dict(zip(names, p, strict=True))}
               for i, p in enumerate(points)]
    expected = [r["evaluation"] for r in gymnasium.assemble_result(records, names)["pareto"]]
    got = [i + 1 for i in pareto_indices(list(enumerate(points)))]
    assert got == expected
    assert len(points) - 1 in got and len(points) in got
