"""Immutable composition revisions and storage grants.

The control plane never holds a key to an organization's storage. Uploads,
downloads and checksum verification are requested here and carried out by the
target's agent, which mints short-lived grants with the organization's own
identity.
"""

from __future__ import annotations

from datetime import timedelta

from sqlalchemy import select

from ..contracts.agent import GrantFulfillment, GrantMode
from ..contracts.errors import ErrorCode, PlatformError
from ..contracts.requests import RevisionCreate
from ..db.models import (
    ComputeTarget,
    GrantRequestRow,
    PlatformSession,
    Project,
    Revision,
    new_id,
)
from .auth import Principal
from .base import ServiceBase, human_actor


def revision_prefix(organization_id: str, project_id: str, revision_id: str) -> str:
    return f"revisions/{organization_id}/{project_id}/{revision_id}/"


class RevisionServices(ServiceBase):
    def create_revision(self, principal: Principal, body: RevisionCreate) -> dict:
        manifest = body.manifest
        if body.manifest.composition not in {f.path for f in manifest.files}:
            raise PlatformError(ErrorCode.VALIDATION_ERROR,
                                f"the manifest does not include {manifest.composition}")
        paths = [f.path for f in manifest.files]
        if len(set(paths)) != len(paths):
            raise PlatformError(ErrorCode.VALIDATION_ERROR, "the manifest lists a path twice")
        if len(paths) > self.settings.max_revision_files:
            raise PlatformError(ErrorCode.VALIDATION_ERROR,
                                f"the package has {len(paths)} files; the limit is "
                                f"{self.settings.max_revision_files}")
        total = sum(f.size for f in manifest.files)
        if total != manifest.total_bytes or total > self.settings.max_revision_bytes:
            raise PlatformError(ErrorCode.VALIDATION_ERROR,
                                "total_bytes does not match the files or exceeds the size limit")

        with self.tx() as s:
            project = self.owned(s, Project, body.project_id, principal, "editor")
            target = self.owned(s, ComputeTarget, body.target_id, principal, "viewer")
            if target.organization_id != project.organization_id:
                raise PlatformError(ErrorCode.NOT_FOUND, f"target {body.target_id} was not found")
            if target.revoked_at is not None:
                raise PlatformError(ErrorCode.TARGET_REVOKED, "the target was revoked")
            if body.session_id is not None:
                session = self.owned(s, PlatformSession, body.session_id, principal, "viewer")
                if session.project_id != project.id:
                    raise PlatformError(ErrorCode.NOT_FOUND,
                                        f"session {body.session_id} was not found")
            rev_id = new_id("rev")
            prefix = revision_prefix(project.organization_id, project.id, rev_id)
            s.add(Revision(
                id=rev_id, organization_id=project.organization_id, project_id=project.id,
                session_id=body.session_id, target_id=target.id, state="UPLOADING",
                manifest=manifest.model_dump(mode="json", by_alias=True),
                manifest_sha256=manifest.digest(), storage_prefix=prefix,
                created_by=principal.user_id,
            ))
            s.flush()  # the grant request references the revision
            grant = self._grant_request(
                s, project.organization_id, target.id, principal, GrantMode.UPLOAD,
                [{"key": prefix + f.path, "sha256": f.sha256, "size": f.size}
                 for f in manifest.files],
                revision_id=rev_id,
            )
            self.audit(s, project.organization_id, human_actor(principal), "revision.create",
                       "revision", rev_id, {"manifest_sha256": manifest.digest()})
            return {"revision_id": rev_id, "state": "UPLOADING", "grant_request_id": grant,
                    "storage_prefix": prefix, "manifest_sha256": manifest.digest()}

    def finalize_revision(self, principal: Principal, revision_id: str) -> dict:
        """Ask the agent to verify every object's checksum before the revision is usable."""
        with self.tx() as s:
            rev = self.owned(s, Revision, revision_id, principal, "editor", lock=True)
            if rev.state == "READY":
                return {"revision_id": rev.id, "state": "READY", "grant_request_id": None}
            if rev.state not in ("UPLOADING", "FAILED"):
                raise PlatformError(ErrorCode.NOT_READY,
                                    f"revision {rev.id} is {rev.state}; wait for verification")
            rev.state = "VERIFYING"
            files = rev.manifest["files"]
            grant = self._grant_request(
                s, rev.organization_id, rev.target_id, principal, GrantMode.VERIFY,
                [{"key": rev.storage_prefix + f["path"], "sha256": f["sha256"], "size": f["size"]}
                 for f in files],
                revision_id=rev.id,
            )
            return {"revision_id": rev.id, "state": "VERIFYING", "grant_request_id": grant}

    def get_revision(self, principal: Principal, revision_id: str) -> dict:
        with self.tx() as s:
            rev = self.owned(s, Revision, revision_id, principal, "viewer")
            return {"revision_id": rev.id, "state": rev.state, "project_id": rev.project_id,
                    "target_id": rev.target_id, "manifest_sha256": rev.manifest_sha256,
                    "manifest": rev.manifest}

    def get_grant_request(self, principal: Principal, grant_request_id: str) -> dict:
        with self.tx() as s:
            row = self.owned(s, GrantRequestRow, grant_request_id, principal, "viewer")
            if row.requested_by != principal.user_id:
                raise PlatformError(ErrorCode.NOT_FOUND,
                                    f"grant request {grant_request_id} was not found")
            return {"grant_request_id": row.id, "mode": row.mode, "state": row.state,
                    "grants": row.grants or [], "problems": row.problems or []}

    # -- agent side --------------------------------------------------------------

    def pending_grant_requests(self, s, target_id: str, limit: int = 20) -> list[GrantRequestRow]:
        now = self.now()
        redeliver = now - timedelta(seconds=self.settings.grant_redeliver_seconds)
        rows = list(s.scalars(
            select(GrantRequestRow)
            .where(
                GrantRequestRow.target_id == target_id,
                GrantRequestRow.expires_at > now,
                (GrantRequestRow.state == "PENDING")
                | ((GrantRequestRow.state == "DELIVERED")
                   & (GrantRequestRow.delivered_at < redeliver)),
            )
            .order_by(GrantRequestRow.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        ))
        for row in rows:
            row.state, row.delivered_at = "DELIVERED", now
        return rows

    def fulfill_grant(self, target: ComputeTarget, body: GrantFulfillment) -> None:
        with self.tx() as s:
            row = s.get(GrantRequestRow, body.grant_request_id, with_for_update=True)
            if row is None or row.target_id != target.id:
                raise PlatformError(ErrorCode.NOT_FOUND, "unknown grant request")
            if row.state in ("FULFILLED", "FAILED", "EXPIRED"):
                return  # duplicate delivery; the first answer stands
            requested = {o["key"] for o in row.objects}
            if any(g.key not in requested for g in body.grants):
                raise PlatformError(ErrorCode.VALIDATION_ERROR,
                                    "a grant names an object that was not requested")
            row.state = "FULFILLED" if body.ok else "FAILED"
            row.grants = [g.model_dump(mode="json") for g in body.grants]
            row.problems = body.problems
            row.fulfilled_at = self.now()
            if row.mode == GrantMode.VERIFY.value and row.revision_id:
                rev = s.get(Revision, row.revision_id, with_for_update=True)
                if rev is not None and rev.state == "VERIFYING":
                    rev.state = "READY" if body.ok and not body.problems else "FAILED"
                    rev.finalized_at = self.now()
                    self.audit(s, rev.organization_id, {"target_id": target.id},
                               "revision.verified", "revision", rev.id,
                               {"ok": rev.state == "READY", "problems": body.problems[:20]})
