"""``PlatformClient``: the HTTP client used by the GUI, CLI and a local MCP process.

Every call returns plain data or raises ``PlatformError`` with the server's
typed error body, so callers (including AI agents) see the same ``retryable``
flag and ``next_action`` whether they call in process or over HTTPS.
"""

from __future__ import annotations

import shutil
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import httpx

from .contracts.errors import ErrorBody, ErrorCode, PlatformError
from .contracts.requests import RunRequest


class PlatformClient:
    """``token`` is a person's sign-in token, or an agent session credential (``hcps_...``)
    when an AI agent calls on a person's behalf under the session's budget."""

    def __init__(self, base_url: str, token: str, *,
                 transport: httpx.BaseTransport | None = None, timeout: float = 30.0):
        headers = {"Authorization": f"Bearer {token}"}
        self._http = httpx.Client(base_url=base_url, headers=headers, transport=transport,
                                  timeout=timeout)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> PlatformClient:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _call(self, method: str, path: str, **kw) -> Any:
        response = self._http.request(method, path, **kw)
        if response.status_code >= 400:
            raise _error(response)
        return response.json() if response.content else None

    # -- catalog --------------------------------------------------------------

    def me(self) -> dict:
        return self._call("GET", "/v1/me")

    def workflows(self) -> list[dict]:
        return self._call("GET", "/v1/workflows")

    def create_project(self, organization_id: str, name: str) -> dict:
        return self._call("POST", f"/v1/organizations/{organization_id}/projects",
                          json={"name": name})

    def register_target(self, organization_id: str, **body) -> dict:
        return self._call("POST", f"/v1/organizations/{organization_id}/targets", json=body)

    def list_targets(self, organization_id: str) -> list[dict]:
        return self._call("GET", f"/v1/organizations/{organization_id}/targets")

    def revoke_target(self, target_id: str) -> None:
        self._call("POST", f"/v1/targets/{target_id}/revoke")

    def add_member(self, organization_id: str, issuer: str, subject: str, role: str) -> None:
        self._call("POST", f"/v1/organizations/{organization_id}/members",
                   json={"issuer": issuer, "subject": subject, "role": role})

    def create_session(self, project_id: str, name: str) -> dict:
        return self._call("POST", f"/v1/projects/{project_id}/sessions", json={"name": name})

    def create_agent_session(self, **body) -> dict:
        return self._call("POST", "/v1/agent-sessions", json=body)

    # -- revisions --------------------------------------------------------------

    def create_revision(self, body: dict) -> dict:
        return self._call("POST", "/v1/revisions", json=body)

    def get_revision(self, revision_id: str) -> dict:
        return self._call("GET", f"/v1/revisions/{revision_id}")

    def finalize_revision(self, revision_id: str) -> dict:
        return self._call("POST", f"/v1/revisions/{revision_id}/finalize")

    def grant_request(self, grant_request_id: str) -> dict:
        return self._call("GET", f"/v1/grant-requests/{grant_request_id}")

    def wait_for_grants(self, grant_request_id: str, timeout: float = 120.0) -> dict:
        """Wait until the target's agent has answered a grant request."""
        deadline = time.monotonic() + timeout
        while True:
            row = self.grant_request(grant_request_id)
            if row["state"] in ("FULFILLED", "FAILED", "EXPIRED"):
                return row
            if time.monotonic() > deadline:
                raise PlatformError(ErrorCode.NOT_READY,
                                    "the target's agent did not answer the grant request",
                                    retryable=True,
                                    next_action="Check that the target's agent is running.")
            time.sleep(0.25)

    def upload_revision(self, package, *, project_id: str, target_id: str,
                        session_id: str | None = None, timeout: float = 300.0) -> dict:
        """Register a packaged revision, upload its files through grants and verify it."""
        created = self.create_revision({
            "project_id": project_id, "session_id": session_id, "target_id": target_id,
            "manifest": package.manifest.model_dump(mode="json", by_alias=True),
        })
        grants = self.wait_for_grants(created["grant_request_id"], timeout)
        if grants["state"] != "FULFILLED":
            raise PlatformError(ErrorCode.NOT_READY, "upload grants were refused",
                                details={"problems": grants.get("problems")})
        by_key = {g["key"]: g for g in grants["grants"]}
        for path, source in package.sources().items():
            _put(by_key[created["storage_prefix"] + path], source, self._http)
        verify = self.finalize_revision(created["revision_id"])
        if verify["grant_request_id"]:
            self.wait_for_grants(verify["grant_request_id"], timeout)
        revision = self.get_revision(created["revision_id"])
        if revision["state"] != "READY":
            raise PlatformError(ErrorCode.NOT_READY,
                                f"revision {created['revision_id']} failed verification")
        return revision

    # -- runs and jobs -------------------------------------------------------------

    def estimate(self, request: RunRequest | dict) -> dict:
        return self._call("POST", "/v1/runs/estimate", json=_dump(request))

    def submit_run(self, request: RunRequest | dict, idempotency_key: str | None = None) -> dict:
        """Submit a run. Retrying with the same key returns the same run, never a second one."""
        key = idempotency_key or f"hcp-{uuid.uuid4().hex}"
        return self._call("POST", "/v1/runs", json=_dump(request),
                          headers={"Idempotency-Key": key})

    def job(self, job_id: str) -> dict:
        return self._call("GET", f"/v1/jobs/{job_id}")

    def cancel(self, job_id: str) -> dict:
        return self._call("POST", f"/v1/jobs/{job_id}/cancel")

    def jobs(self, project_id: str, **params) -> dict:
        return self._call("GET", f"/v1/projects/{project_id}/jobs", params=params)

    def approvals(self, project_id: str) -> list[dict]:
        return self._call("GET", f"/v1/projects/{project_id}/approvals")

    def resolve_approval(self, approval_id: str, approve: bool, note: str | None = None) -> dict:
        return self._call("POST", f"/v1/approvals/{approval_id}",
                          json={"approve": approve, "note": note})

    def results(self, run_id: str) -> dict:
        return self._call("GET", f"/v1/runs/{run_id}/results")

    def wait_for_job(self, job_id: str, timeout: float = 600.0, interval: float = 0.5) -> dict:
        terminal = {"SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT", "REJECTED"}
        deadline = time.monotonic() + timeout
        while True:
            view = self.job(job_id)
            if view["state"] in terminal or time.monotonic() > deadline:
                return view
            time.sleep(interval)

    def download(self, run_id: str, paths: list[str], destination: Path,
                 timeout: float = 300.0) -> dict[str, Path]:
        """Download published artifacts directly from storage into ``destination``."""
        request = self._call("POST", f"/v1/runs/{run_id}/downloads", json={"paths": paths})
        grants = self.wait_for_grants(request["grant_request_id"], timeout)
        if grants["state"] != "FULFILLED":
            raise PlatformError(ErrorCode.NOT_READY, "download grants were refused",
                                details={"problems": grants.get("problems")})
        destination.mkdir(parents=True, exist_ok=True)
        by_key = {g["key"]: g for g in grants["grants"]}
        out: dict[str, Path] = {}
        for path in paths:
            grant = by_key[request["keys"][path]]
            target = destination / path
            target.parent.mkdir(parents=True, exist_ok=True)
            _get(grant, target, self._http)
            out[path] = target
        return out


def _dump(request: RunRequest | dict) -> dict:
    return request.model_dump(mode="json") if isinstance(request, RunRequest) else request


def _error(response: httpx.Response) -> PlatformError:
    try:
        body = ErrorBody.model_validate(response.json())
    except Exception:
        body = ErrorBody(code=ErrorCode.NOT_READY if response.status_code >= 500
                         else ErrorCode.VALIDATION_ERROR,
                         message=f"HTTP {response.status_code}: {response.text[:500]}",
                         retryable=response.status_code >= 500)
    return PlatformError(body.code, body.message, retryable=body.retryable,
                         next_action=body.next_action, details=body.details)


def _local_path(url: str) -> Path:
    return Path(unquote(urlparse(url).path))


def _put(grant: dict, source: Path, http: httpx.Client) -> None:
    if grant["method"] == "file":
        dest = _local_path(grant["url"])
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".part")
        shutil.copyfile(source, tmp)
        tmp.replace(dest)
    else:
        with open(source, "rb") as fh:
            httpx.put(grant["url"], content=fh, headers=grant.get("headers") or {},
                      timeout=None).raise_for_status()


def _get(grant: dict, target: Path, http: httpx.Client) -> None:
    if grant["method"] == "file":
        shutil.copyfile(_local_path(grant["url"]), target)
    else:
        with httpx.stream("GET", grant["url"], headers=grant.get("headers") or {},
                          timeout=None) as r:
            r.raise_for_status()
            with open(target, "wb") as fh:
                for chunk in r.iter_bytes():
                    fh.write(chunk)
