"""``gym.evaluate@1``: one evaluator pool task of a study.

The task keeps an ``openswmm_gymnasium`` env loaded across evaluations, in
gymnasium's ``WorkerPool`` so that an engine crash fails one candidate and not
the task. It leases chunks of the study's batches through its agent's runner
gateway (runners never talk to the control plane), evaluates them, writes each
chunk's records as an append-only JSON-lines object, and reports the costs. It
finishes when the control plane says the study needs it no more.
"""

from __future__ import annotations

import json
import math
import os
import time
from typing import Any

import httpx

from ..contracts.manifests import ArtifactKind, check_safe_path
from ..storage import sha256_file
from . import (
    EXIT_CANCELLED,
    EXIT_INVALID_INPUT,
    EXIT_MISSING_DEPENDENCY,
    EXIT_TRANSFER,
    RunnerFailure,
)
from .core import Context, HandlerResult, _upload
from .swmm import _component

LEASE_WAIT_SECONDS = 20.0
MAX_ERROR = 2000  # the length the agent protocol carries


class Gateway:
    """The agent's runner gateway, retrying while the agent cannot reach the control plane."""

    def __init__(self, url: str, token: str, retry_seconds: float = 300.0):
        self._http = httpx.Client(base_url=url, timeout=LEASE_WAIT_SECONDS + 30,
                                  headers={"Authorization": f"Bearer {token}"})
        self._retry_seconds = retry_seconds

    def call(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        deadline, delay = time.monotonic() + self._retry_seconds, 0.5
        while True:
            try:
                response = self._http.post(path, json=body)
            except httpx.HTTPError as exc:
                problem = f"the agent's gateway is unreachable: {exc}"
            else:
                if response.status_code < 400:
                    return response.json()
                error = _body(response)
                if response.status_code in (401, 404, 409) or \
                        error.get("code") == "STALE_ATTEMPT":
                    raise RunnerFailure(EXIT_CANCELLED,
                                        f"the agent no longer holds this attempt: {error}")
                if error.get("code") == "TARGET_REVOKED":
                    return {"done": True}
                if not error.get("retryable"):
                    raise RunnerFailure(EXIT_TRANSFER, f"the gateway refused {path}: {error}")
                problem = str(error.get("message"))
            if time.monotonic() > deadline:
                raise RunnerFailure(EXIT_TRANSFER, problem)
            time.sleep(delay)
            delay = min(delay * 2, 10.0)

    def close(self) -> None:
        self._http.close()


def _body(response: httpx.Response) -> dict[str, Any]:
    try:
        return response.json()
    except ValueError:
        return {"message": response.text[:500]}


def _env(ctx: Context, parameters: dict[str, Any]):
    """The study's env config with its model path resolved to the staged revision."""
    try:
        from openswmm_gymnasium.optimize import design_dimensions, flat_bounds, objective_directions
        from openswmm_gymnasium.spec.config import EnvConfig
    except ImportError as exc:
        raise RunnerFailure(EXIT_MISSING_DEPENDENCY,
                            f"this runner has no openswmm_gymnasium: {exc}") from None
    raw = dict(parameters["env_config"])
    try:
        check_safe_path(raw.get("inp_path", ""))
        raw["inp_path"] = str(ctx.inputs / raw["inp_path"])
        # Reports and outputs belong to the task, never to fixed user paths.
        raw["rpt_path"], raw["out_path"] = None, None
        config = EnvConfig.model_validate(raw)
        dims = design_dimensions(config)
    except Exception as exc:
        raise RunnerFailure(EXIT_INVALID_INPUT, f"the env config is invalid: {exc}") from None

    study = parameters["study"]
    low, high = flat_bounds(dims)
    labels = [label for d in dims for label in d.labels]
    expected = study["dimensions"]
    if len(expected) != len(labels) or any(
            e["label"] != label or not math.isclose(e["low"], lo, abs_tol=1e-12)
            or not math.isclose(e["high"], hi, abs_tol=1e-12)
            for e, label, lo, hi in zip(expected, labels, low, high, strict=True)):
        raise RunnerFailure(EXIT_INVALID_INPUT,
                            "the study's dimensions do not match the env config's search space")
    directions = objective_directions(config)
    if [(o["name"], o["direction"]) for o in study["objectives"]] != list(directions.items()):
        raise RunnerFailure(EXIT_INVALID_INPUT,
                            "the study's objectives do not match the env config's reward terms")
    return config, dims


def _evaluate(pool, vectors: list, workers: int, timeout: float, ctx: Context) -> list:
    """Evaluate in groups of ``workers``, each within ``timeout`` seconds.

    When a group runs over, gymnasium's pool replaces its workers and the group's
    results are lost, so its candidates are evaluated again one at a time: those
    that finish keep their result, and only the ones that run over again fail.
    """
    from openswmm_gymnasium.optimize import Evaluation

    def over() -> Evaluation:
        return Evaluation(costs=None, error=f"did not finish within {timeout:g} s "
                          "(evaluation_timeout_seconds)")

    out: list = []
    for start in range(0, len(vectors), workers):
        group = vectors[start:start + workers]
        done = _within(pool, group, timeout, ctx)
        if done is None and len(group) == 1:
            done = [over()]
        elif done is None:
            done = [(_within(pool, [v], timeout, ctx) or [over()])[0] for v in group]
        out.extend(done)
    return out


def _within(pool, vectors: list, timeout: float, ctx: Context) -> list | None:
    """The pool's results, or None when they took longer than ``timeout`` seconds."""
    from openswmm_gymnasium.optimize import StudyCancelled

    deadline = time.monotonic() + timeout
    try:
        return pool.evaluate(vectors, lambda: ctx.cancel.is_set() or time.monotonic() > deadline)
    except StudyCancelled:
        if ctx.cancel.is_set():
            raise RunnerFailure(EXIT_CANCELLED, "cancelled by the agent") from None
        return None


def _checked(outcome, n_objectives: int) -> tuple[list[float] | None, str | None]:
    """The costs to report, or an error: one bad candidate must not fail the whole chunk."""
    if outcome.error is not None:
        return None, outcome.error[:MAX_ERROR]
    costs = outcome.costs
    if costs is None or len(costs) != n_objectives or \
            not all(math.isfinite(float(c)) for c in costs):
        return None, f"the evaluation returned invalid costs: {costs!r}"[:MAX_ERROR]
    return [float(c) for c in costs], None


def run(ctx: Context, composition: dict) -> HandlerResult:
    comp = _component(composition)
    parameters = ctx.spec.workload.parameters
    study = parameters.get("study")
    url = os.environ.get("HCP_GATEWAY_URL")
    token = ctx.secrets.get("HCP_RUNNER_TOKEN")  # on stdin, never in the environment
    if not study or not url or not token:
        raise RunnerFailure(EXIT_INVALID_INPUT,
                            "gym.evaluate runs only as a study's pool task, through its agent")
    config, dims = _env(ctx, parameters)

    from openswmm_gymnasium.optimize import WorkerPool, split_vector
    from openswmm_gymnasium.spec.envs import json_safe

    names = [o["name"] for o in study["objectives"]]
    gateway = Gateway(url, token)
    artifacts: list[tuple[str, ArtifactKind, str]] = []
    counts = {"evaluations": 0, "failed": 0, "chunks": 0, "discarded_chunks": 0}
    ctx.events.emit("lifecycle", "running", f"evaluator for study {study['study_id']}")
    workers = int(parameters.get("workers", 1))
    timeout = float(parameters.get("evaluation_timeout_seconds", 600))
    try:
        pool = WorkerPool(config, dims, workers=workers, seed=study.get("seed"),
                          max_steps=int(parameters["max_steps_per_episode"]))
    except RuntimeError as exc:  # the env could not be built: the same on every retry
        raise RunnerFailure(EXIT_INVALID_INPUT, f"the evaluator could not start: {exc}") from None
    try:
        while True:
            ctx.check_cancel()
            lease = gateway.call("/v1/evaluations/lease", {"wait_seconds": LEASE_WAIT_SECONDS})
            if lease.get("done"):
                break
            chunk = lease.get("chunk")
            if chunk is None:
                time.sleep(float(lease.get("poll_after_seconds") or 1.0))
                continue
            outcomes = _evaluate(pool, chunk["candidates"], workers, timeout, ctx)
            rel = f"evaluations/{chunk['chunk_id']}-{chunk['lease_number']}.jsonl"
            path = ctx.outputs / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            results = []
            with path.open("w", encoding="utf-8") as fh:
                pairs = zip(chunk["candidates"], outcomes, strict=True)
                for i, (vector, outcome) in enumerate(pairs):
                    costs, error = _checked(outcome, len(names))
                    record = {
                        "evaluation": chunk["first_evaluation"] + i,
                        "decisions": json_safe(split_vector(dims, vector)), "vector": vector,
                        "objectives": (dict(zip(names, costs, strict=True))
                                       if error is None else None),
                        "steps": outcome.steps,
                        "error": error if outcome.error is None else outcome.error,
                        "study_id": study["study_id"], "chunk_id": chunk["chunk_id"],
                        "lease_number": chunk["lease_number"], "attempt_id": ctx.attempt_id,
                        "seed": study.get("seed"),
                    }
                    fh.write(json.dumps(record) + "\n")
                    results.append({"costs": costs, "steps": outcome.steps, "error": error})
            _upload(ctx, rel, path)
            ack = gateway.call("/v1/evaluations/complete", {
                "chunk_id": chunk["chunk_id"], "lease_number": chunk["lease_number"],
                "results": results, "object": {"key": ctx.output_prefix + rel,
                                               "sha256": sha256_file(path),
                                               "size": path.stat().st_size}})
            artifacts.append((rel, ArtifactKind.EVALUATIONS, "jsonl"))
            counts["chunks"] += 1
            if not ack.get("accepted"):
                counts["discarded_chunks"] += 1  # leased again elsewhere; kept for review
                continue
            counts["evaluations"] += len(results)
            counts["failed"] += sum(r["error"] is not None for r in results)
            ctx.events.emit("progress", "evaluating", completed=counts["evaluations"],
                            unit="evaluations")
    finally:
        pool.close()
        gateway.close()

    import openswmm

    return HandlerResult(
        status="completed",
        components=[{"id": comp["id"], "final_status": "Finished", "errors": []}],
        quality=counts, engine_version=getattr(openswmm, "__version__", None),
        artifacts=artifacts)
