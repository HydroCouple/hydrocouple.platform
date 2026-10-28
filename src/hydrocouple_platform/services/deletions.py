"""Run and study deletion: asynchronous, confirmed erasure (``contracts.deletion``, plan §12).

**Locks.** The run, the study, each job and the deletion are each locked in their own
transaction, so deletion never holds two of them at once against the job, attempt,
then run order of result reports.
"""

from __future__ import annotations

from datetime import timedelta

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from ..contracts.deletion import DeletionState, DeletionView, Erasure, ErasureReport
from ..contracts.errors import ErrorCode, PlatformError
from ..contracts.states import JOB_HELD, JOB_TERMINAL, JobState
from ..contracts.studies import STUDY_TERMINAL, StudyState
from ..db.models import Artifact, Attempt, ComputeTarget, Deletion, Job, Run, Study, new_id
from .auth import Principal
from .base import ServiceBase, human_actor

_D = DeletionState
WAITING = (_D.REQUESTED.value, _D.WAITING_FOR_WRITERS.value, _D.BLOCKED.value)
ERASING = (_D.ERASING.value, _D.VERIFYING.value)
TERMINAL_JOB_STATES = [s.value for s in JOB_TERMINAL]


class DeletionServices(ServiceBase):
    # -- requests --------------------------------------------------------------------

    def delete_run(self, principal: Principal, run_id: str) -> DeletionView:
        """Ask for a run's results to be erased. Asking again returns the same deletion."""
        with self.tx() as s:
            run = self.owned(s, Run, run_id, principal, "editor")
            if principal.agent_session_id and run.agent_session_id != principal.agent_session_id:
                raise PlatformError(ErrorCode.FORBIDDEN,
                                    "an AI agent session deletes only runs it submitted")
            existing = s.scalar(select(Deletion).where(Deletion.run_id == run.id))
            if existing is not None:
                return self._deletion_view(existing)
            study_id = s.scalar(select(Study.id).where(Study.run_id == run.id))
        if study_id is not None:
            self._cancel_for_deletion(study_id)
        with self.tx() as s:
            # FOR NO KEY UPDATE, as result reports lock the run.
            run = s.get(Run, run_id, with_for_update={"key_share": True})
            existing = s.scalar(select(Deletion).where(Deletion.run_id == run.id))
            if existing is not None:  # a concurrent request won
                return self._deletion_view(existing)
            now = self.now()
            run.deleting_at = now
            deletion = Deletion(
                id=new_id("del"), organization_id=run.organization_id, run_id=run.id,
                target_id=run.target_id, study_id=study_id, state=_D.REQUESTED.value,
                prefixes=[f"runs/{run.organization_id}/{run.project_id}/{run.id}/"],
                requested_by=principal.user_id,
                actor={**human_actor(principal), "agent_session_id": principal.agent_session_id},
                requested_at=now, updated_at=now)
            s.add(deletion)
            self.audit(s, run.organization_id, deletion.actor, "run.delete", "run", run.id,
                       {"deletion_id": deletion.id, "study_id": study_id})
            job_ids = list(s.scalars(select(Job.id).where(
                Job.run_id == run.id, Job.state.notin_(TERMINAL_JOB_STATES))))
            deletion_id = deletion.id
        for job_id in job_ids:  # each job in its own transaction
            with self.tx() as s:
                job = s.get(Job, job_id, with_for_update=True)
                state = JobState(job.state)
                if state in JOB_TERMINAL:
                    continue
                if state in JOB_HELD:
                    job.cancel_requested, job.updated_at = True, self.now()
                else:
                    self._set_state(job, JobState.CANCELLED, failure={"reason": "run deleted"})
        self.reconcile_deletions()  # erase at once when nothing was running
        return self.get_deletion_by_id(deletion_id)

    def delete_study(self, principal: Principal, study_id: str) -> DeletionView:
        with self.tx() as s:
            study = self.owned(s, Study, study_id, principal, "editor")
            run_id = study.run_id
        return self.delete_run(principal, run_id)

    def _cancel_for_deletion(self, study_id: str) -> None:
        with self.tx() as s:
            study = s.get(Study, study_id, with_for_update=True)
            if StudyState(study.state) not in STUDY_TERMINAL:
                self._end_study(s, study, StudyState.CANCELLED,  # type: ignore[attr-defined]
                                {"reason": "the study was deleted"})
        self._stop_pool(study_id)  # type: ignore[attr-defined]

    # -- views ---------------------------------------------------------------------------

    def get_deletion(self, principal: Principal, deletion_id: str) -> DeletionView:
        with self.tx() as s:
            return self._deletion_view(self.owned(s, Deletion, deletion_id, principal, "viewer"))

    def get_deletion_by_id(self, deletion_id: str) -> DeletionView:
        with self.tx() as s:
            return self._deletion_view(s.get(Deletion, deletion_id))

    @staticmethod
    def _deletion_view(d: Deletion) -> DeletionView:
        state = DeletionState(d.state)
        nxt = {
            _D.BLOCKED: "Erasure is blocked until the run's attempts are confirmed stopped; "
                        "check the target's agent, which must be running to erase anything",
            _D.ERASED: None,
        }.get(state, f"Query the deletion again; it is {state.value}")
        return DeletionView(
            deletion_id=d.id, run_id=d.run_id, study_id=d.study_id, state=state,
            prefixes=d.prefixes, passes=d.passes, objects_deleted=d.objects_deleted,
            reason=d.reason, requested_at=d.requested_at, updated_at=d.updated_at,
            completed_at=d.completed_at, next_action=nxt)

    # -- the reconciler ------------------------------------------------------------------

    def reconcile_deletions(self, limit: int = 200) -> dict[str, int]:
        """Move deletions on once no writer remains, or mark them blocked when that cannot
        be confirmed in time."""
        counts = {"deletions_erasing": 0, "deletions_blocked": 0}
        with self.tx() as s:
            ids = list(s.scalars(select(Deletion.id).where(Deletion.state.in_(WAITING))
                                 .order_by(Deletion.updated_at).limit(limit)))
        wait = timedelta(seconds=self.settings.deletion_writer_wait_seconds)
        for deletion_id in ids:
            with self.tx() as s:
                d = s.get(Deletion, deletion_id, with_for_update=True)
                if d.state not in WAITING:
                    continue
                # A writer is any attempt its agent has not confirmed stopped, even one the
                # control plane already ended (lost, or timed out on the server's clock).
                writers = list(s.execute(
                    select(Job.id, Attempt.state).join(Attempt, Attempt.job_id == Job.id)
                    .where(Job.run_id == d.run_id, Attempt.released_at.is_(None))))
                writers += list(s.execute(select(Job.id, Job.state).where(
                    Job.run_id == d.run_id, Job.state.notin_(TERMINAL_JOB_STATES),
                    Job.current_attempt_id.is_(None))))
                now = self.now()
                if not writers:
                    d.state, d.not_before, d.updated_at = _D.ERASING.value, now, now
                    d.reason = None
                    counts["deletions_erasing"] += 1
                elif now - d.requested_at > wait:
                    target = s.get(ComputeTarget, d.target_id)
                    seen = target.last_poll_at.isoformat() if target.last_poll_at else "never"
                    reason = (f"erasure blocked: the agent has not confirmed that {len(writers)} "
                              f"attempt(s) of this run stopped (job {writers[0][0]}: "
                              f"{writers[0][1]}); the target's agent last polled {seen}")
                    if d.state != _D.BLOCKED.value or d.reason != reason:
                        d.state, d.reason, d.updated_at = _D.BLOCKED.value, reason, now
                        counts["deletions_blocked"] += 1
                        self.audit(s, d.organization_id, {"system": "reconciler"},
                                   "run.delete.blocked", "run", d.run_id, {"reason": reason})
                elif d.state == _D.REQUESTED.value:
                    d.state, d.updated_at = _D.WAITING_FOR_WRITERS.value, now
        # An erasure only the target's agent can do, for a target that no longer polls.
        with self.tx() as s:
            for d in s.scalars(select(Deletion).join(
                    ComputeTarget, ComputeTarget.id == Deletion.target_id).where(
                    Deletion.state.in_(ERASING), ComputeTarget.revoked_at.is_not(None),
                    Deletion.reason.is_(None)).limit(limit).with_for_update(skip_locked=True)):
                d.reason = ("erasure blocked: the target is revoked, so its agent no longer "
                            "polls; restore the target or erase its prefix there by hand")
                d.updated_at = self.now()
                counts["deletions_blocked"] += 1
        return counts

    # -- the agent -------------------------------------------------------------------------

    def pending_erasures(self, s: Session, target_id: str) -> list[Erasure]:
        """Erasures this target's agent should do now: due, and not delivered recently."""
        now = self.now()
        again = now - timedelta(seconds=self.settings.grant_redeliver_seconds)
        out = []
        for d in s.scalars(select(Deletion).where(
                Deletion.target_id == target_id, Deletion.state.in_(ERASING),
                Deletion.not_before <= now)
                .with_for_update(skip_locked=True)):
            if d.delivered_at is not None and d.delivered_at > again:
                continue
            d.delivered_at = now
            out.append(Erasure(deletion_id=d.id, pass_number=d.passes + 1, prefixes=d.prefixes))
        return out

    def agent_erasure_report(self, target_id: str, report: ErasureReport) -> DeletionView:
        with self.tx() as s:
            d = s.get(Deletion, report.deletion_id, with_for_update=True)
            if d is None or d.target_id != target_id:
                raise PlatformError(ErrorCode.NOT_FOUND,
                                    f"deletion {report.deletion_id} was not found")
            if d.state not in ERASING or report.pass_number != d.passes + 1:
                return self._deletion_view(d)  # a repeated or late report
            now = self.now()
            d.updated_at = now
            if not report.ok:
                d.reason = "; ".join(report.problems)[:2000] or "the agent could not erase"
                d.delivered_at = None  # ask again at the next poll after the redelivery delay
                d.not_before = now + timedelta(seconds=self.settings.grant_redeliver_seconds)
                return self._deletion_view(d)
            d.passes += 1
            d.objects_deleted += report.objects_deleted
            d.reason, d.delivered_at = None, None
            if d.state == _D.VERIFYING.value and report.objects_deleted == 0:
                d.state, d.completed_at = _D.ERASED.value, now
                run = s.get(Run, d.run_id, with_for_update={"key_share": True})
                run.deleted_at, run.summary = now, None
                s.execute(delete(Artifact).where(Artifact.run_id == run.id))
                self.audit(s, d.organization_id, {"target_id": target_id}, "run.erased", "run",
                           run.id, {"deletion_id": d.id, "objects_deleted": d.objects_deleted,
                                    "passes": d.passes})
            else:
                # Erased once (or a late write was found): check again after the delay.
                d.state = _D.VERIFYING.value
                d.not_before = now + timedelta(seconds=self.settings.deletion_recheck_seconds)
            return self._deletion_view(d)
