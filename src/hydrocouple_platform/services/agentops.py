"""Agent-facing operations, the dispatcher and the reconciler.

Locks are always taken job first, then attempt, so the agent endpoints and the
reconciler cannot deadlock each other.
"""

from __future__ import annotations

import time
from datetime import timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from ..contracts.agent import (
    AttemptOutcome,
    AttemptResult,
    EventKind,
    GrantFulfillment,
    GrantRequest,
    Heartbeat,
    HeartbeatAck,
    LeaseGrant,
    ObjectRef,
    PollRequest,
    PollResponse,
    ResultAck,
)
from ..contracts.capabilities import check_spec
from ..contracts.errors import ErrorCode, PlatformError
from ..contracts.jobs import FAILURE_RETRIES, FailureClass, JobSpec
from ..contracts.manifests import AttemptManifest, sha256_of
from ..contracts.states import (
    ATTEMPT_TERMINAL,
    AttemptState,
    JobState,
    attempt_can,
    is_forward_progress,
    job_can,
)
from ..db.models import (
    Approval,
    Artifact,
    Attempt,
    ComputeTarget,
    EventRow,
    GrantRequestRow,
    Job,
    Outbox,
    Revision,
    Run,
    new_id,
)
from .base import ServiceBase

OUTBOX_MAX_TRIES = 10


class AgentServices(ServiceBase):
    # -- poll ------------------------------------------------------------------------

    def agent_poll(self, target_id: str, req: PollRequest) -> PollResponse:
        """Claim leases and grant requests; waits up to ``req.wait_seconds`` for either.

        The HTTP API runs the wait asynchronously (``api/app.py``); this blocking
        form serves in-process callers.
        """
        deadline = time.monotonic() + req.wait_seconds
        while True:
            response = self.poll_once(target_id, req)
            if response.leases or response.grant_requests or time.monotonic() >= deadline:
                return response
            time.sleep(min(0.5, max(deadline - time.monotonic(), 0)))

    def poll_once(self, target_id: str, req: PollRequest) -> PollResponse:
        now = self.now()
        with self.tx() as s:
            # FOR NO KEY UPDATE: inserts that reference the target are not blocked.
            target = s.get(ComputeTarget, target_id, with_for_update={"key_share": True})
            if target is None or target.revoked_at is not None:
                raise PlatformError(ErrorCode.TARGET_REVOKED, "the target is not active")
            capabilities = req.capabilities.model_dump(mode="json")
            if (target.capabilities != capabilities or target.agent_instance != req.agent_instance
                    or target.agent_version != req.agent_version or target.last_poll_at is None
                    or target.last_poll_at < now - timedelta(seconds=5)):
                target.last_poll_at = now
                target.agent_instance = req.agent_instance
                target.agent_version = req.agent_version
                target.capabilities = capabilities

            held = set(req.held_attempts)
            leases = self._redeliver(s, target, req, held)
            slots = req.free_slots - len(leases)
            if slots > 0:
                # Look past jobs that do not fit this agent right now; they stay queued.
                candidates = s.scalars(
                    select(Job)
                    .where(Job.target_id == target.id, Job.state == JobState.LEASABLE.value,
                           Job.cancel_requested.is_(False))
                    .order_by(Job.leasable_at, Job.id)
                    .limit(max(slots * 4, 20))
                    .with_for_update(skip_locked=True)
                )
                for job in candidates:
                    if slots <= 0:
                        break
                    lease = self._lease(s, job, target, req)
                    if lease is not None:
                        leases.append(lease)
                        slots -= 1

            grants = [
                GrantRequest(grant_request_id=g.id, mode=g.mode,
                             objects=[ObjectRef(**o) for o in g.objects])
                for g in self.pending_grant_requests(s, target.id)  # type: ignore[attr-defined]
            ]
            return PollResponse(leases=leases, grant_requests=grants,
                                cancel_attempts=self._to_stop(s, target.id, held),
                                poll_after_seconds=self.settings.heartbeat_interval_seconds)

    def _to_stop(self, s: Session, target_id: str, held: set[str]) -> list[str]:
        """Held attempts the agent must stop: cancelled, superseded, ended or unknown."""
        if not held:
            return []
        rows = {a.id: (a, j) for a, j in s.execute(
            select(Attempt, Job).join(Job, Job.id == Attempt.job_id)
            .where(Attempt.id.in_(held), Attempt.target_id == target_id))}
        stop = []
        for attempt_id in sorted(held):
            pair = rows.get(attempt_id)
            if pair is None:
                stop.append(attempt_id)
                continue
            attempt, job = pair
            if (AttemptState(attempt.state) in ATTEMPT_TERMINAL
                    or job.current_attempt_id != attempt.id or job.cancel_requested):
                stop.append(attempt_id)
        return stop

    def _redeliver(self, s: Session, target: ComputeTarget, req: PollRequest,
                   held: set[str]) -> list[LeaseGrant]:
        """Re-offer leases this agent instance was given but never started (lost response)."""
        if req.free_slots <= 0:
            return []
        rows = s.execute(
            select(Attempt, Job).join(Job, Job.id == Attempt.job_id)
            .where(Attempt.target_id == target.id, Attempt.state == AttemptState.LEASED.value,
                   Attempt.agent_instance == req.agent_instance,
                   Attempt.last_heartbeat_at.is_(None), Job.current_attempt_id == Attempt.id)
            .order_by(Attempt.started_at).limit(req.free_slots))
        out = []
        for attempt, job in rows:
            if attempt.id in held:
                continue
            attempt.lease_expires_at = self.now() + timedelta(seconds=self.settings.lease_seconds)
            out.append(self._grant(job, attempt, JobSpec.model_validate(job.spec)))
        return out

    def _lease(self, s: Session, job: Job, target: ComputeTarget,
               req: PollRequest) -> LeaseGrant | None:
        spec = JobSpec.model_validate(job.spec)
        findings = check_spec(req.capabilities, spec)
        if findings:
            # It does not fit this agent now (e.g. free disk); it stays queued, visibly,
            # until something fits or its queue deadline passes.
            waiting = {"phase": "waiting", "message": "; ".join(findings)[:1000]}
            if job.progress != waiting:
                job.progress, job.updated_at = waiting, self.now()
            return None
        now = self.now()
        number = job.attempts_used + 1
        attempt = Attempt(
            id=new_id("att"), job_id=job.id, organization_id=job.organization_id,
            target_id=target.id, number=number, state=AttemptState.LEASED.value,
            agent_instance=req.agent_instance,
            lease_expires_at=now + timedelta(seconds=self.settings.lease_seconds), started_at=now,
            execution_deadline_at=now + timedelta(
                seconds=spec.limits.execution_timeout_seconds + self.settings.lease_seconds),
        )
        s.add(attempt)
        job.current_attempt_id, job.attempts_used, job.agent_unreachable = attempt.id, number, False
        job.progress = None
        self._set_state(job, JobState.LEASED)
        return self._grant(job, attempt, spec)

    def _grant(self, job: Job, attempt: Attempt, spec: JobSpec) -> LeaseGrant:
        return LeaseGrant(
            attempt_id=attempt.id, job_id=job.id, fencing_token=attempt.number,
            attempt_token=self.tokens.issue(target_id=attempt.target_id, job_id=job.id,
                                            attempt_id=attempt.id, fence=attempt.number),
            lease_expires_at=attempt.lease_expires_at,
            heartbeat_interval_seconds=self.settings.heartbeat_interval_seconds,
            spec=spec,
        )

    def agent_fulfill_grant(self, target: ComputeTarget, body: GrantFulfillment) -> None:
        self.fulfill_grant(target, body)  # type: ignore[attr-defined]

    # -- attempt authority -----------------------------------------------------------

    def _locked(self, s: Session, target_id: str, attempt_id: str) -> tuple[Job, Attempt]:
        attempt = s.get(Attempt, attempt_id)
        if attempt is None or attempt.target_id != target_id:
            raise PlatformError(ErrorCode.NOT_FOUND, f"attempt {attempt_id} was not found")
        job = s.get(Job, attempt.job_id, with_for_update=True)
        s.refresh(attempt, with_for_update=True)
        return job, attempt

    @staticmethod
    def _authority(job: Job, attempt: Attempt, fence: int) -> None:
        if (attempt.number != fence or job.current_attempt_id != attempt.id
                or AttemptState(attempt.state) in ATTEMPT_TERMINAL):
            raise PlatformError(
                ErrorCode.STALE_ATTEMPT,
                f"attempt {attempt.id} no longer holds job {job.id}",
                next_action="Stop this attempt and discard its outputs.",
            )

    # -- heartbeat ---------------------------------------------------------------------

    def agent_heartbeat(self, target_id: str, attempt_token: str, hb: Heartbeat) -> HeartbeatAck:
        self.tokens.check(attempt_token, target_id=target_id, attempt_id=hb.attempt_id,
                          fence=hb.fencing_token)
        now = self.now()
        with self.tx() as s:
            job, attempt = self._locked(s, target_id, hb.attempt_id)
            self._authority(job, attempt, hb.fencing_token)
            if is_forward_progress(AttemptState(attempt.state), hb.state):
                attempt.state = hb.state.value
                job_state = JobState(hb.state.value)
                if job_can(JobState(job.state), job_state):
                    self._set_state(job, job_state)
            if hb.provider_reference:
                attempt.provider_reference = hb.provider_reference
            attempt.lease_expires_at = now + timedelta(seconds=self.settings.lease_seconds)
            attempt.last_heartbeat_at = now
            job.agent_unreachable = False
            accepted = self._ingest(s, job, attempt, hb)
            job.updated_at = now
            return HeartbeatAck(
                lease_expires_at=attempt.lease_expires_at, cancel_requested=job.cancel_requested,
                accepted_events=accepted,
                attempt_token=self.tokens.issue(target_id=target_id, job_id=job.id,
                                                attempt_id=attempt.id, fence=attempt.number),
            )

    def _ingest(self, s: Session, job: Job, attempt: Attempt, hb: Heartbeat) -> int:
        if not hb.events:
            return 0
        rows = [
            {"event_id": e.event_id, "organization_id": job.organization_id, "job_id": job.id,
             "attempt_id": attempt.id, "sequence": e.sequence, "kind": e.kind.value,
             "phase": e.phase, "completed": e.completed, "total": e.total, "unit": e.unit,
             "message": e.message, "agent_clock_seconds": e.agent_clock_seconds,
             "occurred_at": e.occurred_at, "received_at": self.now()}
            for e in hb.events
        ]
        stmt = (pg_insert(EventRow).values(rows)
                .on_conflict_do_nothing(constraint="events_dedupe").returning(EventRow.id))
        accepted = len(s.execute(stmt).all())
        # Progress only moves forward: late or replayed events never regress it.
        newest = max((e for e in hb.events if e.kind == EventKind.PROGRESS),
                     key=lambda e: e.sequence, default=None)
        if newest is not None and newest.sequence > attempt.last_sequence:
            job.progress = {"phase": newest.phase, "completed": newest.completed,
                            "total": newest.total, "unit": newest.unit,
                            "message": newest.message, "sequence": newest.sequence,
                            "occurred_at": newest.occurred_at.isoformat()}
        attempt.last_sequence = max(attempt.last_sequence, max(e.sequence for e in hb.events))
        return accepted

    # -- results ------------------------------------------------------------------------

    def agent_complete(self, target_id: str, attempt_token: str, res: AttemptResult) -> ResultAck:
        self.tokens.check(attempt_token, target_id=target_id, attempt_id=res.attempt_id,
                          fence=res.fencing_token)
        with self.tx() as s:
            job, attempt = self._locked(s, target_id, res.attempt_id)
            run = s.get(Run, job.run_id, with_for_update=True)
            if (attempt.state == AttemptState.SUCCEEDED.value
                    and run.canonical_attempt_id == attempt.id
                    and res.outcome == AttemptOutcome.SUCCEEDED):
                return ResultAck(job_state=job.state, canonical=True, retry_scheduled=False)
            self._authority(job, attempt, res.fencing_token)
            spec = JobSpec.model_validate(job.spec)

            if res.outcome == AttemptOutcome.SUCCEEDED:
                manifest, problems, cls = self._check_manifest(spec, attempt, res)
                if not problems:
                    return self._publish(s, job, attempt, run, spec, manifest, res)
                res = res.model_copy(update={
                    "outcome": AttemptOutcome.FAILED, "failure_class": cls,
                    "diagnostics": {**res.diagnostics, "manifest_problems": problems},
                })
            elif res.outcome == AttemptOutcome.CANCELLED and not job.cancel_requested:
                # Nobody asked: an eviction, preemption or shutdown stopped it. Retry.
                res = res.model_copy(update={
                    "outcome": AttemptOutcome.FAILED, "failure_class": FailureClass.INTERRUPTED})
            return self._fail(s, job, attempt, res)

    def _check_manifest(self, spec: JobSpec, attempt: Attempt, res: AttemptResult
                        ) -> tuple[AttemptManifest | None, list[str], FailureClass]:
        """Problems that block publication, and how to classify them.

        A checksum mismatch is a transfer problem and may be retried; anything else
        is the runner contradicting the workflow and is retried at most once.
        """
        unknown = FailureClass.UNKNOWN
        if res.attempt_manifest is None or res.attempt_manifest_sha256 is None:
            return None, ["a successful result must include the attempt manifest and its sha256"], \
                unknown
        if sha256_of(res.attempt_manifest) != res.attempt_manifest_sha256:
            return None, ["the attempt manifest does not match its sha256"], FailureClass.TRANSFER
        try:
            manifest = AttemptManifest.model_validate(res.attempt_manifest)
        except ValueError as exc:
            return None, [f"the attempt manifest is invalid: {exc}"], unknown
        problems: list[str] = []
        prov = manifest.provenance
        expected = {"run_id": spec.run_id, "job_id": spec.job_id, "attempt_id": attempt.id,
                    "attempt_number": attempt.number, "revision_id": spec.inputs.revision_id,
                    "revision_manifest_sha256": spec.inputs.manifest_sha256}
        for key, value in expected.items():
            if getattr(prov, key) != value:
                problems.append(f"provenance {key} is {getattr(prov, key)!r}, expected {value!r}")
        if manifest.status != "completed":
            problems.append(f"the run manifest status is {manifest.status}")
        paths = {a.path: a for a in manifest.artifacts}
        for required in spec.outputs.required_artifacts:
            if required != "attempt.json" and required not in paths:
                problems.append(f"required artifact {required} is missing")
        run_entry = paths.get(manifest.run_manifest)
        if run_entry is None or run_entry.sha256 != manifest.run_manifest_sha256:
            problems.append("the run manifest is missing from the artifacts or its sha256 differs")
        return manifest, problems, unknown

    def _publish(self, s, job, attempt, run, spec, manifest: AttemptManifest,
                 res: AttemptResult) -> ResultAck:
        now = self.now()
        for state in (AttemptState.FINALIZING, AttemptState.SUCCEEDED):
            if attempt_can(AttemptState(attempt.state), state):
                attempt.state = state.value
        if JobState(job.state) != JobState.FINALIZING:
            self._set_state(job, JobState.FINALIZING)
        self._set_state(job, JobState.SUCCEEDED)
        job.failure = None
        attempt.finished_at, attempt.exit_code = now, res.exit_code
        attempt.manifest = res.attempt_manifest
        attempt.manifest_sha256 = res.attempt_manifest_sha256
        canonical = run.canonical_attempt_id is None
        if canonical:
            run.canonical_attempt_id, run.published_at = attempt.id, now
            run.summary = {"status": manifest.status, "quality": manifest.quality}
            prefix = f"{spec.outputs.storage_prefix}{attempt.id}/"
            for a in manifest.artifacts:
                s.add(Artifact(id=new_id("art"), organization_id=job.organization_id,
                               run_id=run.id, attempt_id=attempt.id, path=a.path,
                               storage_key=prefix + a.path, kind=a.kind.value, format=a.format,
                               sha256=a.sha256, size=a.size))
        self.audit(s, job.organization_id, {"target_id": job.target_id}, "run.publish", "run",
                   run.id, {"attempt_id": attempt.id, "canonical": canonical})
        return ResultAck(job_state=job.state, canonical=canonical, retry_scheduled=False)

    def _fail(self, s: Session, job: Job, attempt: Attempt, res: AttemptResult) -> ResultAck:
        terminal = {
            AttemptOutcome.FAILED: AttemptState.FAILED,
            AttemptOutcome.CANCELLED: AttemptState.CANCELLED,
            AttemptOutcome.TIMED_OUT: AttemptState.TIMED_OUT,
        }[res.outcome]
        failure_class = res.failure_class or (
            FailureClass.TIMEOUT if res.outcome == AttemptOutcome.TIMED_OUT else
            FailureClass.UNKNOWN)
        return self._end_attempt(s, job, attempt, terminal, failure_class, res.exit_code,
                                 res.diagnostics, cancelled=res.outcome == AttemptOutcome.CANCELLED)

    def _end_attempt(self, s, job, attempt, terminal: AttemptState, failure_class: FailureClass,
                     exit_code: int | None, diagnostics: dict[str, Any],
                     cancelled: bool = False) -> ResultAck:
        attempt.state = terminal.value
        attempt.failure_class = failure_class.value
        attempt.exit_code = exit_code
        attempt.diagnostics = diagnostics
        attempt.finished_at = self.now()
        s.flush()
        failure = {"class": failure_class.value, "attempt_id": attempt.id,
                   "exit_code": exit_code, "diagnostics": diagnostics}
        # The observed outcome is kept; a cancel request only rules out a retry.
        if cancelled or (job.cancel_requested and terminal == AttemptState.LOST):
            self._set_state(job, JobState.CANCELLED, failure={**failure, "reason": "cancelled"})
            return ResultAck(job_state=job.state, canonical=False, retry_scheduled=False)
        if terminal == AttemptState.TIMED_OUT:
            self._set_state(job, JobState.TIMED_OUT, failure=failure)
            return ResultAck(job_state=job.state, canonical=False, retry_scheduled=False)
        # Failures of this class so far, this one included, against its retry allowance.
        same_class = s.scalar(select(func.count()).select_from(Attempt).where(
            Attempt.job_id == job.id, Attempt.failure_class == failure_class.value)) or 0
        retry = (not job.cancel_requested
                 and job.attempts_used < job.retry_budget + 1
                 and same_class <= FAILURE_RETRIES[failure_class])
        if retry:
            spec = JobSpec.model_validate(job.spec)
            self._set_state(job, JobState.LEASABLE, failure=failure)
            job.current_attempt_id, job.leasable_at = None, self.now()
            # A retry gets a fresh queue deadline; the first wait was already spent.
            job.queue_deadline_at = self.now() + timedelta(
                seconds=spec.limits.queue_deadline_seconds)
        else:
            self._set_state(job, JobState.FAILED, failure=failure)
        self.audit(s, job.organization_id, {"target_id": job.target_id}, "attempt.end", "attempt",
                   attempt.id, {"state": terminal.value, "class": failure_class.value,
                                "retry": retry})
        return ResultAck(job_state=job.state, canonical=False, retry_scheduled=retry)

    # -- dispatcher ---------------------------------------------------------------------

    def dispatch(self, limit: int = 100) -> int:
        """Relay outbox commands; returns the number processed."""
        now = self.now()
        done = 0
        with self.tx() as s:
            rows = list(s.scalars(
                select(Outbox).where(Outbox.processed_at.is_(None), Outbox.available_at <= now)
                .order_by(Outbox.id).limit(limit).with_for_update(skip_locked=True)
            ))
            for row in rows:
                try:
                    with s.begin_nested():
                        self._handle(s, row)
                    row.processed_at = now
                    done += 1
                except Exception as exc:  # recorded and retried with backoff; never lost
                    row.tries += 1
                    row.last_error = f"{type(exc).__name__}: {exc}"[:2000]
                    if row.tries >= OUTBOX_MAX_TRIES:
                        row.processed_at = now
                        row.last_error = "dead-lettered after retries: " + row.last_error
                    else:
                        row.available_at = now + timedelta(seconds=2 ** row.tries)
        return done

    def _handle(self, s: Session, row: Outbox) -> None:
        if row.topic != "job.accepted":
            raise ValueError(f"unknown outbox topic {row.topic!r}")
        job = s.get(Job, row.payload["job_id"], with_for_update=True)
        if job is None or job.state != JobState.ACCEPTED.value:
            return  # cancelled or already dispatched: nothing to do
        spec = JobSpec.model_validate(job.spec)
        self._set_state(job, JobState.LEASABLE)
        job.leasable_at = self.now()
        job.queue_deadline_at = self.now() + timedelta(seconds=spec.limits.queue_deadline_seconds)

    # -- reconciler ---------------------------------------------------------------------

    def reconcile(self, limit: int = 200) -> dict[str, int]:
        """Act on missed heartbeats, queue deadlines and expired approvals or grants.

        Each item is handled in its own short transaction, in order of age and
        bounded by ``limit``, so recovery after an outage cannot overload the
        database.
        """
        now = self.now()
        counts = {"unreachable": 0, "lost": 0, "execution_timeouts": 0, "queue_timeouts": 0,
                  "approvals_expired": 0, "grants_expired": 0}
        with self.tx() as s:
            open_states = [x.value for x in ATTEMPT_TERMINAL]
            overdue = list(s.scalars(
                select(Attempt.id).where(Attempt.state.notin_(open_states),
                                         Attempt.execution_deadline_at < now)
                .order_by(Attempt.execution_deadline_at).limit(limit)))
            stale = list(s.scalars(
                select(Attempt.id).where(Attempt.state.notin_(open_states),
                                         Attempt.lease_expires_at < now)
                .order_by(Attempt.lease_expires_at).limit(limit)))
        for attempt_id in overdue:
            # The agent should have stopped it; the server enforces the limit regardless.
            with self.tx() as s:
                attempt = s.get(Attempt, attempt_id)
                job = s.get(Job, attempt.job_id, with_for_update=True)
                s.refresh(attempt, with_for_update=True)
                if (AttemptState(attempt.state) in ATTEMPT_TERMINAL
                        or job.current_attempt_id != attempt.id):
                    continue
                self._end_attempt(s, job, attempt, AttemptState.TIMED_OUT, FailureClass.TIMEOUT,
                                  None, {"reason": "the execution timeout passed"})
                counts["execution_timeouts"] += 1
        abandon = self.settings.abandon_after_seconds
        for attempt_id in stale:
            with self.tx() as s:
                attempt = s.get(Attempt, attempt_id)
                job = s.get(Job, attempt.job_id, with_for_update=True)
                s.refresh(attempt, with_for_update=True)
                if (AttemptState(attempt.state) in ATTEMPT_TERMINAL
                        or attempt.lease_expires_at >= now or job.current_attempt_id != attempt.id):
                    continue
                if abandon and attempt.lease_expires_at < now - timedelta(seconds=abandon):
                    self._end_attempt(s, job, attempt, AttemptState.LOST, FailureClass.LOST, None,
                                      {"reason": "no heartbeat before the abandon deadline"})
                    counts["lost"] += 1
                elif not job.agent_unreachable:
                    job.agent_unreachable = True
                    job.updated_at = now
                    counts["unreachable"] += 1

        with self.tx() as s:
            for job in s.scalars(
                select(Job).where(Job.state == JobState.LEASABLE.value,
                                  Job.queue_deadline_at < now)
                .limit(limit).with_for_update(skip_locked=True)):
                self._set_state(job, JobState.TIMED_OUT, failure={
                    "class": FailureClass.CAPACITY.value,
                    "reason": "no capacity claimed the job before its queue deadline"})
                counts["queue_timeouts"] += 1
            for approval in s.scalars(
                select(Approval).where(Approval.state == "PENDING", Approval.expires_at < now)
                .limit(limit).with_for_update(skip_locked=True)):
                approval.state = "EXPIRED"
                job = s.get(Job, approval.job_id, with_for_update=True)
                if job.state == JobState.PENDING_APPROVAL.value:
                    self._set_state(job, JobState.REJECTED,
                                    failure={"reason": "approval expired unanswered"})
                counts["approvals_expired"] += 1
            for grant in s.scalars(
                select(GrantRequestRow).where(
                    GrantRequestRow.state.in_(["PENDING", "DELIVERED"]),
                    GrantRequestRow.expires_at < now)
                .limit(limit).with_for_update(skip_locked=True)):
                grant.state = "EXPIRED"
                if grant.revision_id and grant.mode == "verify":
                    rev = s.get(Revision, grant.revision_id, with_for_update=True)
                    if rev.state == "VERIFYING":
                        rev.state = "FAILED"
                counts["grants_expired"] += 1
        return counts
