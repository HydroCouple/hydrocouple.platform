"""The execution agent loop.

It opens every connection itself (outbound HTTPS only), so it can run in a
private network. Running attempts keep running while the control plane is
unreachable; events are buffered and replayed when it returns. A report the
control plane refuses as stale stops the local attempt.
"""

from __future__ import annotations

import json
import logging
import math
import os
import socket
import tempfile
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

import httpx

from .. import __version__
from ..client import _error
from ..contracts.agent import (
    AttemptOutcome,
    AttemptResult,
    EventKind,
    GrantFulfillment,
    GrantMode,
    GrantRequest,
    Heartbeat,
    HeartbeatAck,
    LeaseGrant,
    PollRequest,
    PollResponse,
    ResultAck,
)
from ..contracts.errors import ErrorCode, PlatformError
from ..contracts.jobs import EgressPolicy, FailureClass, JobClass
from ..contracts.manifests import AttemptManifest, sha256_of
from ..contracts.states import AttemptState
from ..contracts.studies import (
    CompletionAck,
    EvaluationCompletion,
    EvaluationLeaseRequest,
    EvaluationLeaseResponse,
    EvaluationResult,
    ResultObject,
)
from ..runner import EXIT_CANCELLED, EXIT_CLASS
from ..storage import StorageBackend
from .adapters import Observation, SchedulerAdapter
from .egress import SUMMARY_LIMIT, filter_diagnostics, filter_event
from .gateway import RunnerGateway

log = logging.getLogger(__name__)
MAX_BUFFERED_EVENTS = 10_000
MAX_EVALUATION_ERROR = 2000


def _trimmed(raw: object) -> object:
    """One candidate's result from a runner, made reportable: a long error is cut and
    non-finite costs become an error, so one bad candidate cannot fail its chunk."""
    if not isinstance(raw, dict):
        return raw
    out = dict(raw)
    costs = out.get("costs")
    if out.get("error") is None and isinstance(costs, list) and not all(
            isinstance(c, (int, float)) and math.isfinite(c) for c in costs):
        out.update(costs=None, error=f"the evaluation returned invalid costs: {costs!r}")
    if isinstance(out.get("error"), str):
        out["error"] = out["error"][:MAX_EVALUATION_ERROR]
    return out


class AgentClient:
    """Calls the control plane's agent routes with the target credential."""

    def __init__(self, base_url: str, credential: str, transport: httpx.BaseTransport | None = None,
                 timeout: float = 40.0):
        self._http = httpx.Client(base_url=base_url, transport=transport, timeout=timeout,
                                  headers={"Authorization": f"Bearer {credential}"})

    def _post(self, path: str, body, token: str | None = None):
        headers = {"X-HC-Attempt-Token": token} if token else None
        response = self._http.post(path, content=body.model_dump_json(), headers={
            "Content-Type": "application/json", **(headers or {})})
        if response.status_code >= 400:
            raise _error(response)
        return response.json() if response.content else None

    def poll(self, req: PollRequest) -> PollResponse:
        return PollResponse.model_validate(self._post("/v1/agent/poll", req))

    def fulfill(self, body: GrantFulfillment) -> None:
        self._post("/v1/agent/grants", body)

    def heartbeat(self, token: str, hb: Heartbeat) -> HeartbeatAck:
        return HeartbeatAck.model_validate(self._post("/v1/agent/heartbeat", hb, token))

    def result(self, token: str, res: AttemptResult) -> ResultAck:
        return ResultAck.model_validate(self._post("/v1/agent/result", res, token))

    def lease_evaluations(self, token: str, req: EvaluationLeaseRequest
                          ) -> EvaluationLeaseResponse:
        return EvaluationLeaseResponse.model_validate(
            self._post("/v1/agent/evaluations/lease", req, token))

    def complete_evaluations(self, token: str, body: EvaluationCompletion) -> CompletionAck:
        return CompletionAck.model_validate(
            self._post("/v1/agent/evaluations/complete", body, token))

    def close(self) -> None:
        self._http.close()


@dataclass
class _Held:
    lease: LeaseGrant
    handle: str | None  # None when the local start failed
    token: str
    state: AttemptState = AttemptState.QUEUED
    buffer: deque = field(default_factory=lambda: deque(maxlen=MAX_BUFFERED_EVENTS))
    last_beat: float = 0.0
    final: AttemptResult | None = None
    clock0: float = field(default_factory=time.monotonic)
    failure_message: str | None = None


class Agent:
    def __init__(self, client: AgentClient, adapter: SchedulerAdapter, store: StorageBackend,
                 store_uri: str, egress: EgressPolicy, instance: str | None = None,
                 gateway_host: str = "127.0.0.1"):
        self.client, self.adapter, self.store = client, adapter, store
        self.store_uri, self.egress = store_uri, egress
        self.instance = instance or f"{socket.gethostname()}-{os.getpid()}"
        self.held: dict[str, _Held] = {}
        # Started with the first evaluator task; runners reach the control plane only here.
        self.gateway: RunnerGateway | None = None
        self.gateway_host = gateway_host
        self.heartbeat_interval = 15.0
        # Set when the target is revoked: report on held attempts, claim nothing new.
        self.draining = False

    # -- one cycle -----------------------------------------------------------------

    def run_once(self, wait_seconds: float = 0.0) -> PollResponse | None:
        """Poll, start new leases, answer grants, report on held attempts.

        Each step is isolated, so one failing lease, grant or report never stops
        the others.
        """
        response = None
        if not self.draining:
            try:
                response = self.client.poll(PollRequest(
                    agent_instance=self.instance, agent_version=__version__,
                    capabilities=self.adapter.capabilities(),
                    free_slots=self.adapter.free_slots(), wait_seconds=wait_seconds,
                    held_attempts=sorted(self.held)))
            except PlatformError as exc:
                if exc.code == ErrorCode.TARGET_REVOKED:
                    log.error("the target was revoked; finishing %d held attempts, claiming "
                              "nothing new", len(self.held))
                    self.draining = True
                else:
                    log.warning("poll refused; attempts keep running: %s", exc)
            except httpx.HTTPError as exc:
                log.warning("poll failed; attempts keep running: %s", exc)
        if response is not None:
            self.heartbeat_interval = response.poll_after_seconds
            for lease in response.leases:
                self._start(lease)
            for request in response.grant_requests:
                try:
                    self._answer(request)
                except Exception:
                    log.exception("answering grant request %s failed", request.grant_request_id)
            for attempt_id in response.cancel_attempts:
                held = self.held.get(attempt_id)
                if held is not None and held.handle is not None:
                    self.adapter.cancel(held.handle)
        self._report()
        return response

    def run_forever(self, stop, wait_seconds: float = 10.0) -> None:
        """Run until ``stop`` is set, or until a revoked target has drained its work."""
        while not stop.is_set():
            try:
                self.run_once(wait_seconds=0.0 if self.held else wait_seconds)
            except Exception:
                log.exception("agent cycle failed; continuing")
            if self.draining and not self.held:
                log.error("the target was revoked and its work has drained; stopping")
                return
            stop.wait(min(1.0, self.heartbeat_interval) if self.held else 0.1)

    # -- leases ----------------------------------------------------------------------

    def _start(self, lease: LeaseGrant) -> None:
        if lease.attempt_id in self.held:
            return
        try:
            extra = self._runner_env(lease)
            # Only evaluator tasks get the gateway; other submits keep the plain call.
            handle = (self.adapter.submit(lease, self.store_uri, env=extra[0], secrets=extra[1])
                      if extra else self.adapter.submit(lease, self.store_uri))
        except Exception as exc:
            if self.gateway is not None:
                self.gateway.unregister(lease.attempt_id)
            # Report it at once so the job is retried now, not after the lease expires.
            log.exception("could not start attempt %s", lease.attempt_id)
            self.held[lease.attempt_id] = _Held(
                lease=lease, handle=None, token=lease.attempt_token,
                state=AttemptState.FINALIZING,
                final=AttemptResult(
                    attempt_id=lease.attempt_id, fencing_token=lease.fencing_token,
                    outcome=AttemptOutcome.FAILED, failure_class=FailureClass.CAPACITY,
                    diagnostics=filter_diagnostics(
                        {"message": f"the scheduler could not start it: {exc}"}, self.egress)))
            return
        self.held[lease.attempt_id] = _Held(lease=lease, handle=handle, token=lease.attempt_token)

    def _report(self) -> None:
        for attempt_id in list(self.held):
            held = self.held[attempt_id]
            obs: Observation | None = None
            if held.final is None:
                obs = self.adapter.inspect(held.handle)
                for event in obs.events:
                    if event.kind == EventKind.LIFECYCLE and event.phase == "failed":
                        held.failure_message = event.message
                    filtered = filter_event(event.model_copy(update={
                        "agent_clock_seconds": time.monotonic() - held.clock0}), self.egress)
                    if filtered is not None:
                        held.buffer.append(filtered)
                if obs.finished:
                    held.state = AttemptState.FINALIZING
                    held.final = self._result(held, obs)
                else:
                    held.state = obs.state
            try:
                due = time.monotonic() - held.last_beat >= self.heartbeat_interval
                if held.buffer or due or held.final is not None:
                    self._beat(held, obs)
                if held.final is not None:
                    self.client.result(held.token, held.final)
                    self._forget(attempt_id)
            except PlatformError as exc:
                if exc.code in (ErrorCode.STALE_ATTEMPT, ErrorCode.NOT_FOUND,
                                ErrorCode.UNAUTHENTICATED):
                    # Superseded, unknown, or a token that can never be accepted: stop it.
                    log.warning("attempt %s is no longer ours: %s", attempt_id, exc)
                    if held.handle is not None:
                        self.adapter.cancel(held.handle)
                    self._forget(attempt_id)
                else:
                    log.warning("report for %s failed; will retry: %s", attempt_id, exc)
            except httpx.HTTPError as exc:
                log.warning("control plane unreachable; buffering %s: %s", attempt_id, exc)

    def _beat(self, held: _Held, obs: Observation | None) -> None:
        events = [held.buffer[i] for i in range(min(len(held.buffer), 500))]
        ack = self.client.heartbeat(held.token, Heartbeat(
            attempt_id=held.lease.attempt_id, fencing_token=held.lease.fencing_token,
            state=held.state, provider_reference=obs.provider_reference if obs else None,
            events=events))
        for _ in events:
            held.buffer.popleft()
        held.token, held.last_beat = ack.attempt_token, time.monotonic()
        if ack.cancel_requested and held.final is None and held.handle is not None:
            self.adapter.cancel(held.handle)

    # -- grants -------------------------------------------------------------------------

    ALLOWED_PREFIXES = ("revisions/", "runs/")

    def _answer(self, request: GrantRequest) -> None:
        """Mint grants or verify objects with this site's own storage identity."""
        grants, problems = [], []
        for obj in request.objects:
            if not obj.key.startswith(self.ALLOWED_PREFIXES):
                problems.append(f"{obj.key} is outside the platform's storage prefixes")
                continue
            try:
                if request.mode == GrantMode.UPLOAD:
                    grants.append(self.store.grant(obj.key, "PUT", request.expires_in_seconds))
                    continue
                stat = self.store.stat(obj.key)
                if stat is None:
                    problems.append(f"{obj.key} is missing")
                elif (obj.size is not None and stat[0] != obj.size) or (
                        obj.sha256 is not None and stat[1] != obj.sha256):
                    problems.append(f"{obj.key} does not match its size or sha256")
                elif request.mode == GrantMode.DOWNLOAD:
                    grants.append(self.store.grant(obj.key, "GET", request.expires_in_seconds))
            except ValueError as exc:
                problems.append(str(exc))
        try:
            self.client.fulfill(GrantFulfillment(
                grant_request_id=request.grant_request_id, ok=not problems, grants=grants,
                problems=problems))
        except (httpx.HTTPError, PlatformError) as exc:
            log.warning("grant answer failed; the request will be redelivered: %s", exc)

    def _forget(self, attempt_id: str) -> None:
        held = self.held.pop(attempt_id, None)
        if self.gateway is not None:
            self.gateway.unregister(attempt_id)
        if held is not None and held.handle is not None:
            self.adapter.cleanup(held.handle)

    def close(self) -> None:
        if self.gateway is not None:
            self.gateway.close()
            self.gateway = None

    # -- evaluator tasks: the runner gateway calls these from its own threads -------------

    def _runner_env(self, lease: LeaseGrant) -> tuple[dict[str, str], dict[str, str]] | None:
        """The gateway address (environment) and token (secret) for an evaluator task."""
        if lease.spec.resources.job_class != JobClass.EVALUATOR_POOL:
            return None
        if self.gateway is None:
            self.gateway = RunnerGateway(self, host=self.gateway_host)
        return ({"HCP_GATEWAY_URL": self.gateway.url},
                {"HCP_RUNNER_TOKEN": self.gateway.register(lease.attempt_id)})

    def _held_task(self, attempt_id: str) -> _Held:
        held = self.held.get(attempt_id)
        if held is None or held.final is not None:
            raise PlatformError(ErrorCode.STALE_ATTEMPT,
                                f"attempt {attempt_id} is not running here")
        return held

    def lease_evaluations(self, attempt_id: str, body: dict) -> dict:
        held = self._held_task(attempt_id)
        req = EvaluationLeaseRequest(attempt_id=attempt_id,
                                     fencing_token=held.lease.fencing_token,
                                     wait_seconds=min(float(body.get("wait_seconds", 0)), 20.0))
        return self.client.lease_evaluations(held.token, req).model_dump(mode="json")

    def complete_evaluations(self, attempt_id: str, body: dict) -> dict:
        """Check the chunk's records object in this site's storage, filter candidate error
        text by the egress policy, and report the results."""
        held = self._held_task(attempt_id)
        obj = ResultObject.model_validate(body.get("object"))
        prefix = f"{held.lease.spec.outputs.storage_prefix}{attempt_id}/evaluations/"
        if not obj.key.startswith(prefix):
            raise PlatformError(ErrorCode.VALIDATION_ERROR,
                                f"evaluation records must be stored under {prefix}")
        if self.store.stat(obj.key) != (obj.size, obj.sha256):
            raise PlatformError(ErrorCode.VALIDATION_ERROR,
                                f"{obj.key} is missing or does not match its size or sha256")
        results = [self._filter_result(EvaluationResult.model_validate(_trimmed(r)))
                   for r in body.get("results") or []]
        completion = EvaluationCompletion(
            attempt_id=attempt_id, fencing_token=held.lease.fencing_token,
            chunk_id=body.get("chunk_id"), lease_number=body.get("lease_number"),
            results=results, object=obj)
        return self.client.complete_evaluations(held.token, completion).model_dump(mode="json")

    def _filter_result(self, result: EvaluationResult) -> EvaluationResult:
        if result.error is None or self.egress == EgressPolicy.FULL:
            return result
        if self.egress == EgressPolicy.METRICS_ONLY:
            return result.model_copy(update={
                "error": "evaluation failed (details withheld by this site's egress policy)"})
        return result.model_copy(update={"error": result.error[:SUMMARY_LIMIT]})

    # -- results ----------------------------------------------------------------------

    def _result(self, held: _Held, obs: Observation) -> AttemptResult:
        base = {"attempt_id": held.lease.attempt_id, "fencing_token": held.lease.fencing_token}
        if obs.timed_out:
            return AttemptResult(**base, outcome=AttemptOutcome.TIMED_OUT,
                                 failure_class=FailureClass.TIMEOUT, exit_code=obs.exit_code)
        if obs.cancelled_by_agent:
            return AttemptResult(**base, outcome=AttemptOutcome.CANCELLED,
                                 exit_code=obs.exit_code)
        if obs.exit_code == EXIT_CANCELLED:
            # Stopped by a signal this agent did not send: an interruption, not a cancel.
            return AttemptResult(**base, outcome=AttemptOutcome.FAILED, exit_code=obs.exit_code,
                                 failure_class=FailureClass.INTERRUPTED)
        if obs.exit_code == 0:
            manifest, digest, problems = self._verify_outputs(held)
            if not problems:
                return AttemptResult(**base, outcome=AttemptOutcome.SUCCEEDED, exit_code=0,
                                     attempt_manifest=manifest, attempt_manifest_sha256=digest)
            return AttemptResult(**base, outcome=AttemptOutcome.FAILED, exit_code=0,
                                 failure_class=FailureClass.TRANSFER,
                                 diagnostics={"problems": problems[:20]})
        if obs.signal is not None:
            failure = (FailureClass.ENGINE_CRASH if obs.signal in (6, 11) else
                       FailureClass.UNKNOWN)
        else:
            failure = EXIT_CLASS.get(obs.exit_code or 1, FailureClass.UNKNOWN)
        message = held.failure_message or (obs.stderr_tail or "").strip()[-500:] or None
        diagnostics = filter_diagnostics(
            {"exit_code": obs.exit_code, "signal": obs.signal, "message": message}, self.egress)
        return AttemptResult(**base, outcome=AttemptOutcome.FAILED, failure_class=failure,
                             exit_code=obs.exit_code, diagnostics=diagnostics)

    def _verify_outputs(self, held: _Held) -> tuple[dict | None, str | None, list[str]]:
        """Check the attempt manifest and every artifact it lists against storage.

        The attempt manifest (checksums, package paths, parameters, versions) is
        the publication record and is sent unchanged under every egress policy;
        the policy governs events and diagnostics.
        """
        prefix = f"{held.lease.spec.outputs.storage_prefix}{held.lease.attempt_id}/"
        key = prefix + "attempt.json"
        if not self.store.exists(key):
            return None, None, ["attempt.json was not published"]
        with tempfile.TemporaryDirectory() as tmp:
            local = Path(tmp) / "attempt.json"
            self.store.get_file(key, local)
            try:
                data = json.loads(local.read_text(encoding="utf-8"))
                manifest = AttemptManifest.model_validate(data)
            except ValueError as exc:
                return None, None, [f"attempt.json is invalid: {exc}"]
        problems = []
        for artifact in manifest.artifacts:
            stat = self.store.stat(prefix + artifact.path)
            if stat is None:
                problems.append(f"{artifact.path} is missing from storage")
            elif stat != (artifact.size, artifact.sha256):
                problems.append(f"{artifact.path} does not match its size or sha256")
        return data, sha256_of(data), problems
