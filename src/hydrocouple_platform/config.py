"""Control-plane settings (environment variables prefixed ``HCP_``).

The control plane refuses to start without a token verifier: there is no
unauthenticated mode.
"""

from __future__ import annotations

import re

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ControlPlaneSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="HCP_", extra="ignore")

    database_url: str

    # User sign-in: exactly one key source plus issuer and audience.
    jwt_issuer: str
    jwt_audience: str
    jwks_url: str | None = None
    jwks_file: str | None = None
    jwt_algorithms: list[str] = Field(default_factory=lambda: ["RS256", "ES256", "EdDSA"])

    # Signs short-lived attempt tokens; only the control plane verifies them.
    attempt_token_secret: SecretStr

    lease_seconds: int = Field(default=120, ge=10)
    heartbeat_interval_seconds: float = Field(default=15.0, gt=0)
    abandon_after_seconds: int = Field(default=900, ge=0)
    attempt_token_seconds: int = Field(default=600, ge=60)
    approval_ttl_seconds: int = Field(default=24 * 3600, gt=0)
    grant_redeliver_seconds: int = Field(default=60, gt=0)
    max_revision_files: int = Field(default=5000, gt=0)
    max_revision_bytes: int = Field(default=20 * 1024**3, gt=0)
    # The workflow registry's images: "name@version" -> a signed image by digest
    # (HCP_WORKFLOW_IMAGES, as JSON). Container targets run only workflows listed here.
    workflow_images: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _one_key_source(self) -> ControlPlaneSettings:
        if bool(self.jwks_url) == bool(self.jwks_file):
            raise ValueError("configure exactly one of HCP_JWKS_URL or HCP_JWKS_FILE")
        if len(self.attempt_token_secret.get_secret_value()) < 32:
            raise ValueError("HCP_ATTEMPT_TOKEN_SECRET must be at least 32 characters")
        if self.abandon_after_seconds and self.abandon_after_seconds < self.lease_seconds:
            raise ValueError("HCP_ABANDON_AFTER_SECONDS must be at least HCP_LEASE_SECONDS")
        for workflow, image in self.workflow_images.items():
            if not re.fullmatch(r"[^@\s]+@sha256:[0-9a-f]{64}", image):
                raise ValueError(f"HCP_WORKFLOW_IMAGES: the image for {workflow} must be "
                                 "pinned by sha256 digest")
        return self
