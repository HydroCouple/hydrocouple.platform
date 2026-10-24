"""Identity: user principals, agent credentials and attempt tokens.

- Users sign in with an external identity provider; the control plane verifies
  issuer, audience, expiry and signature, then derives organization roles from
  its own records. A caller-supplied organization is never trusted.
- An agent holds one revocable credential per compute target. Only its SHA-256
  digest is stored.
- Each lease carries a short-lived attempt token bound to (target, job,
  attempt, fencing token). Reports for an attempt need both. An expired token
  is still accepted for its own attempt while that attempt holds the job, so a
  long outage never discards finished work; it is renewed on the next report.
- An AI agent session has its own credential, so an MCP server never holds the
  human's sign-in token and every request it makes carries the session's budget.
"""

from __future__ import annotations

import hashlib
import json
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import jwt

from ..contracts.errors import ErrorCode, PlatformError

ROLE_RANK = {"viewer": 1, "editor": 2, "admin": 3}
AGENT_CREDENTIAL_PREFIX = "hcpa_"
SESSION_CREDENTIAL_PREFIX = "hcps_"


@dataclass(frozen=True)
class Principal:
    """A verified human caller. Roles are loaded per request from the database."""

    user_id: str
    issuer: str
    subject: str
    email: str | None = None
    roles: dict[str, str] = field(default_factory=dict)
    # Set when an AI agent acts for this person through an agent session.
    agent_session_id: str | None = None

    def role_in(self, organization_id: str) -> str | None:
        return self.roles.get(organization_id)


@dataclass(frozen=True)
class VerifiedClaims:
    issuer: str
    subject: str
    email: str | None


class UserTokenVerifier:
    """Verifies user bearer tokens against one configured key source."""

    def __init__(
        self,
        *,
        issuer: str,
        audience: str,
        algorithms: list[str],
        jwks_url: str | None = None,
        jwks: dict | None = None,
    ):
        if (jwks_url is None) == (jwks is None):
            raise ValueError("provide exactly one of jwks_url or jwks")
        self.issuer, self.audience, self.algorithms = issuer, audience, algorithms
        self._client = jwt.PyJWKClient(jwks_url) if jwks_url else None
        self._keys = jwt.PyJWKSet.from_dict(jwks) if jwks else None

    @classmethod
    def from_file(cls, path: str, **kw) -> UserTokenVerifier:
        with open(path, encoding="utf-8") as fh:
            return cls(jwks=json.load(fh), **kw)

    def _key(self, token: str):
        if self._client is not None:
            return self._client.get_signing_key_from_jwt(token).key
        kid = jwt.get_unverified_header(token).get("kid")
        for key in self._keys.keys:  # type: ignore[union-attr]
            if kid is None or key.key_id == kid:
                return key.key
        raise jwt.InvalidTokenError("no signing key matches the token")

    def verify(self, token: str) -> VerifiedClaims:
        try:
            claims = jwt.decode(
                token,
                self._key(token),
                algorithms=self.algorithms,
                audience=self.audience,
                issuer=self.issuer,
                options={"require": ["exp", "iss", "aud", "sub"]},
            )
        except jwt.PyJWTError as exc:
            raise PlatformError(
                ErrorCode.UNAUTHENTICATED,
                f"the sign-in token was rejected: {exc}",
                next_action="Sign in again to obtain a fresh token.",
            ) from None
        return VerifiedClaims(issuer=claims["iss"], subject=claims["sub"],
                              email=claims.get("email"))


def new_agent_credential(prefix: str = AGENT_CREDENTIAL_PREFIX) -> tuple[str, str]:
    """A new credential and the digest to store; the credential is shown once."""
    token = prefix + secrets.token_urlsafe(32)
    return token, credential_digest(token)


def credential_digest(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


class AttemptTokens:
    """Issues and checks attempt tokens (HS256; only the control plane verifies)."""

    def __init__(self, secret: str, lifetime_seconds: int):
        self._secret, self._lifetime = secret, lifetime_seconds

    def issue(self, *, target_id: str, job_id: str, attempt_id: str, fence: int) -> str:
        now = datetime.now(timezone.utc)
        claims = {
            "typ": "hcp-attempt",
            "tgt": target_id,
            "job": job_id,
            "att": attempt_id,
            "fence": fence,
            "iat": int(now.timestamp()),
            "exp": int((now + timedelta(seconds=self._lifetime)).timestamp()),
        }
        return jwt.encode(claims, self._secret, algorithm="HS256")

    def check(self, token: str, *, target_id: str, attempt_id: str, fence: int) -> dict:
        """Verify signature and binding. Expiry is reported in ``claims['expired']``;
        the caller accepts an expired token only while its attempt still holds the job."""
        try:
            claims = jwt.decode(
                token, self._secret, algorithms=["HS256"],
                options={"require": ["exp", "att"], "verify_exp": False},
            )
        except jwt.PyJWTError as exc:
            raise PlatformError(
                ErrorCode.UNAUTHENTICATED,
                f"the attempt token was rejected: {exc}",
                next_action="Stop this attempt; it no longer holds a valid lease.",
            ) from None
        if (
            claims.get("typ") != "hcp-attempt"
            or claims.get("tgt") != target_id
            or claims.get("att") != attempt_id
            or claims.get("fence") != fence
        ):
            raise PlatformError(
                ErrorCode.STALE_ATTEMPT,
                "the attempt token does not match this target, attempt and fencing token",
                next_action="Stop this attempt; it no longer holds the job's lease.",
            )
        claims["expired"] = claims["exp"] < datetime.now(timezone.utc).timestamp()
        return claims
