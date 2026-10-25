"""The application API.

Client routes authenticate a user token and call the shared services with the
resulting principal. Agent routes authenticate a target credential and, for
attempt reports, an attempt token. No route accepts an organization from the
caller as proof of membership.

This module deliberately does not use ``from __future__ import annotations``:
FastAPI must evaluate the dependency aliases defined inside ``create_app``.
"""


import asyncio
import time
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Header, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict

from .. import __version__
from ..config import ControlPlaneSettings
from ..contracts.agent import (
    AttemptResult,
    GrantFulfillment,
    Heartbeat,
    HeartbeatAck,
    PollRequest,
    PollResponse,
    ResultAck,
)
from ..contracts.errors import ErrorBody, ErrorCode, PlatformError
from ..contracts.requests import (
    AgentSessionCreate,
    Estimate,
    JobView,
    RevisionCreate,
    RunAccepted,
    RunRequest,
    TargetCreate,
)
from ..contracts.studies import (
    BatchSubmit,
    BatchView,
    CompletionAck,
    EvaluationCompletion,
    EvaluationLeaseRequest,
    EvaluationLeaseResponse,
    StudyCreate,
    StudyView,
)
from ..db import make_engine, make_sessionmaker
from ..db.models import ComputeTarget
from ..services import PlatformServices, Principal, UserTokenVerifier
from ..services.auth import AGENT_CREDENTIAL_PREFIX, SESSION_CREDENTIAL_PREFIX
from ..workflows import REGISTRY


class _Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MemberBody(_Body):
    issuer: str
    subject: str
    role: str


class NameBody(_Body):
    name: str


class ApprovalBody(_Body):
    approve: bool
    note: str | None = None


class DownloadBody(_Body):
    paths: list[str]


def _bearer(authorization: str | None) -> str:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise PlatformError(ErrorCode.UNAUTHENTICATED, "a bearer token is required",
                            next_action="Sign in and send Authorization: Bearer <token>.")
    return authorization.split(" ", 1)[1].strip()


def create_app(
    settings: ControlPlaneSettings | None = None,
    services: PlatformServices | None = None,
    verifier: UserTokenVerifier | None = None,
) -> FastAPI:
    settings = settings or ControlPlaneSettings()  # type: ignore[call-arg]
    if services is None:
        services = PlatformServices(make_sessionmaker(make_engine(settings.database_url)),
                                    settings)
    if verifier is None:
        common = {"issuer": settings.jwt_issuer, "audience": settings.jwt_audience,
                  "algorithms": settings.jwt_algorithms}
        verifier = (UserTokenVerifier(jwks_url=settings.jwks_url, **common) if settings.jwks_url
                    else UserTokenVerifier.from_file(settings.jwks_file, **common))  # type: ignore[arg-type]

    app = FastAPI(title="HydroCouple execution platform", version=__version__)
    svc = services

    @app.exception_handler(PlatformError)
    async def _platform_error(_: Request, exc: PlatformError):
        return JSONResponse(exc.body.model_dump(mode="json"), status_code=exc.status_code)

    @app.exception_handler(RequestValidationError)
    async def _validation_error(_: Request, exc: RequestValidationError):
        body = ErrorBody(code=ErrorCode.VALIDATION_ERROR, message="the request body is invalid",
                         details={"errors": exc.errors()})
        return JSONResponse(body.model_dump(mode="json"), status_code=422)

    def principal(authorization: Annotated[str | None, Header()] = None) -> Principal:
        """A person, or an AI agent session acting for a person under its budget."""
        token = _bearer(authorization)
        if token.startswith(AGENT_CREDENTIAL_PREFIX):
            raise PlatformError(ErrorCode.FORBIDDEN, "agent credentials cannot call client routes")
        if token.startswith(SESSION_CREDENTIAL_PREFIX):
            return svc.session_principal(token)
        return svc.principal_for(verifier.verify(token))

    def _agent_token(authorization: str | None) -> str:
        token = _bearer(authorization)
        if not token.startswith(AGENT_CREDENTIAL_PREFIX):
            raise PlatformError(ErrorCode.FORBIDDEN, "agent routes need an agent credential")
        return token

    def agent(authorization: Annotated[str | None, Header()] = None) -> ComputeTarget:
        return svc.agent_target(_agent_token(authorization))

    def draining_agent(authorization: Annotated[str | None, Header()] = None) -> ComputeTarget:
        # Reports on held attempts are accepted from a revoked target so work drains.
        return svc.agent_target(_agent_token(authorization), allow_revoked=True)

    User = Annotated[Principal, Depends(principal)]
    Agent = Annotated[ComputeTarget, Depends(agent)]
    DrainingAgent = Annotated[ComputeTarget, Depends(draining_agent)]

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok", "version": __version__}

    # -- identity and catalog ---------------------------------------------------

    @app.get("/v1/me")
    def me(p: User) -> dict[str, Any]:
        return {"user_id": p.user_id, "subject": p.subject, "roles": p.roles}

    @app.get("/v1/workflows")
    def workflows(_: User) -> list[dict[str, Any]]:
        return [{"workflow": w.name, "version": w.version, "job_class": w.job_class.value,
                 "parameters_schema": w.parameters_schema,
                 "allowed_components": sorted(w.allowed_components),
                 "description": w.description} for w in REGISTRY.values()]

    @app.post("/v1/organizations/{org_id}/members", status_code=204)
    def add_member(org_id: str, body: MemberBody, p: User) -> None:
        svc.add_member(p, org_id, body.issuer, body.subject, body.role)

    @app.post("/v1/organizations/{org_id}/projects", status_code=201)
    def create_project(org_id: str, body: NameBody, p: User) -> dict:
        return svc.create_project(p, org_id, body.name)

    @app.get("/v1/organizations/{org_id}/targets")
    def list_targets(org_id: str, p: User) -> list[dict]:
        return svc.list_targets(p, org_id)

    @app.post("/v1/organizations/{org_id}/targets", status_code=201)
    def register_target(org_id: str, body: TargetCreate, p: User) -> dict:
        return svc.register_target(p, org_id, body)

    @app.post("/v1/targets/{target_id}/revoke", status_code=204)
    def revoke_target(target_id: str, p: User) -> None:
        svc.revoke_target(p, target_id)

    @app.post("/v1/projects/{project_id}/sessions", status_code=201)
    def create_session(project_id: str, body: NameBody, p: User) -> dict:
        return svc.create_session(p, project_id, body.name)

    @app.get("/v1/projects/{project_id}/sessions")
    def list_sessions(project_id: str, p: User) -> list[dict]:
        return svc.list_sessions(p, project_id)

    @app.post("/v1/agent-sessions", status_code=201)
    def create_agent_session(body: AgentSessionCreate, p: User) -> dict:
        return svc.create_agent_session(p, body)

    @app.post("/v1/agent-sessions/{agent_session_id}/revoke", status_code=204)
    def revoke_agent_session(agent_session_id: str, p: User) -> None:
        svc.revoke_agent_session(p, agent_session_id)

    # -- revisions and grants ---------------------------------------------------

    @app.post("/v1/revisions", status_code=201)
    def create_revision(body: RevisionCreate, p: User) -> dict:
        return svc.create_revision(p, body)

    @app.get("/v1/revisions/{revision_id}")
    def get_revision(revision_id: str, p: User) -> dict:
        return svc.get_revision(p, revision_id)

    @app.post("/v1/revisions/{revision_id}/finalize")
    def finalize_revision(revision_id: str, p: User) -> dict:
        return svc.finalize_revision(p, revision_id)

    @app.get("/v1/grant-requests/{grant_request_id}")
    def get_grant_request(grant_request_id: str, p: User) -> dict:
        return svc.get_grant_request(p, grant_request_id)

    # -- runs, jobs and approvals ---------------------------------------------------

    @app.post("/v1/runs/estimate")
    def estimate(body: RunRequest, p: User) -> Estimate:
        return svc.estimate(p, body)

    @app.post("/v1/runs", status_code=202)
    def submit_run(
        body: RunRequest,
        p: User,
        idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
    ) -> RunAccepted:
        if not idempotency_key:
            raise PlatformError(ErrorCode.VALIDATION_ERROR, "an Idempotency-Key header is required",
                                next_action="Send a unique key and reuse it when retrying.")
        return svc.submit_run(p, body, idempotency_key)

    @app.get("/v1/runs/{run_id}/results")
    def results(run_id: str, p: User) -> dict:
        return svc.get_results(p, run_id)

    @app.post("/v1/runs/{run_id}/downloads", status_code=202)
    def downloads(run_id: str, body: DownloadBody, p: User) -> dict:
        return svc.request_downloads(p, run_id, body.paths)

    @app.get("/v1/jobs/{job_id}")
    def get_job(job_id: str, p: User) -> JobView:
        return svc.get_job(p, job_id)

    @app.post("/v1/jobs/{job_id}/cancel", status_code=202)
    def cancel_job(job_id: str, p: User) -> dict:
        return svc.cancel_job(p, job_id)

    @app.get("/v1/projects/{project_id}/jobs")
    def list_jobs(project_id: str, p: User, state: str | None = None, limit: int = 50,
                  before: str | None = None) -> dict:
        return svc.list_jobs(p, project_id, state, limit, before)

    @app.get("/v1/projects/{project_id}/approvals")
    def list_approvals(project_id: str, p: User) -> list[dict]:
        return svc.list_approvals(p, project_id)

    @app.post("/v1/approvals/{approval_id}")
    def resolve_approval(approval_id: str, body: ApprovalBody, p: User) -> dict:
        return svc.resolve_approval(p, approval_id, body.approve, body.note)

    # -- studies ------------------------------------------------------------------------

    def _key(value: str | None) -> str:
        if not value:
            raise PlatformError(ErrorCode.VALIDATION_ERROR, "an Idempotency-Key header is required",
                                next_action="Send a unique key and reuse it when retrying.")
        return value

    IdempotencyKey = Annotated[str | None, Header(alias="Idempotency-Key")]

    @app.post("/v1/studies", status_code=201)
    def create_study(body: StudyCreate, p: User, key: IdempotencyKey = None) -> StudyView:
        return svc.create_study(p, body, _key(key))

    @app.get("/v1/studies/{study_id}")
    def get_study(study_id: str, p: User) -> StudyView:
        return svc.get_study(p, study_id)

    @app.post("/v1/studies/{study_id}/batches", status_code=202)
    def submit_batch(study_id: str, body: BatchSubmit, p: User,
                     key: IdempotencyKey = None) -> BatchView:
        return svc.submit_batch(p, study_id, body, _key(key))

    @app.get("/v1/studies/{study_id}/batches/{batch_id}")
    def get_batch(study_id: str, batch_id: str, p: User) -> BatchView:
        return svc.get_batch(p, study_id, batch_id)

    @app.post("/v1/studies/{study_id}/close")
    def close_study(study_id: str, p: User) -> StudyView:
        return svc.close_study(p, study_id)

    @app.post("/v1/studies/{study_id}/cancel", status_code=202)
    def cancel_study(study_id: str, p: User) -> StudyView:
        return svc.cancel_study(p, study_id)

    @app.get("/v1/studies/{study_id}/results")
    def study_results(study_id: str, p: User) -> dict:
        return svc.study_results(p, study_id)

    @app.get("/v1/studies/{study_id}/evaluations")
    def study_evaluations(study_id: str, p: User, after: int = 0, limit: int = 500) -> dict:
        return svc.list_evaluations(p, study_id, after, limit)

    @app.get("/v1/studies/{study_id}/manifest")
    def study_manifest(study_id: str, p: User) -> dict:
        return svc.study_manifest(p, study_id)

    # -- agent routes -------------------------------------------------------------------

    AttemptToken = Annotated[str | None, Header(alias="X-HC-Attempt-Token")]

    def _attempt_token(value: str | None) -> str:
        if not value:
            raise PlatformError(ErrorCode.UNAUTHENTICATED, "an X-HC-Attempt-Token is required")
        return value

    @app.post("/v1/agent/poll")
    async def agent_poll(body: PollRequest, t: Agent) -> PollResponse:
        # The wait happens on the event loop, so idle agents hold no worker threads.
        deadline = time.monotonic() + body.wait_seconds
        while True:
            response = await run_in_threadpool(svc.poll_once, t.id, body)
            if response.leases or response.grant_requests or time.monotonic() >= deadline:
                return response
            await asyncio.sleep(min(0.5, max(deadline - time.monotonic(), 0)))

    @app.post("/v1/agent/grants", status_code=204)
    def agent_grants(body: GrantFulfillment, t: DrainingAgent) -> None:
        svc.agent_fulfill_grant(t, body)

    @app.post("/v1/agent/heartbeat")
    def agent_heartbeat(body: Heartbeat, t: DrainingAgent,
                        token: AttemptToken = None) -> HeartbeatAck:
        return svc.agent_heartbeat(t.id, _attempt_token(token), body)

    @app.post("/v1/agent/result")
    def agent_result(body: AttemptResult, t: DrainingAgent,
                     token: AttemptToken = None) -> ResultAck:
        return svc.agent_complete(t.id, _attempt_token(token), body)

    @app.post("/v1/agent/evaluations/lease")
    async def evaluation_lease(body: EvaluationLeaseRequest, t: Agent,
                               token: AttemptToken = None) -> EvaluationLeaseResponse:
        # More evaluations are new work, so a revoked target gets none.
        attempt_token = _attempt_token(token)
        deadline = time.monotonic() + body.wait_seconds
        while True:
            response = await run_in_threadpool(svc.evaluation_lease, t.id, attempt_token, body)
            if response.chunk or response.done or time.monotonic() >= deadline:
                return response
            await asyncio.sleep(min(0.5, max(deadline - time.monotonic(), 0)))

    @app.post("/v1/agent/evaluations/complete")
    def evaluation_complete(body: EvaluationCompletion, t: DrainingAgent,
                            token: AttemptToken = None) -> CompletionAck:
        return svc.evaluation_complete(t.id, _attempt_token(token), body)

    return app
