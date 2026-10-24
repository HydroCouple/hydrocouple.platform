"""Typed errors shared by the API, the client and the MCP adapter."""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel


class ErrorCode(str, Enum):
    UNAUTHENTICATED = "UNAUTHENTICATED"
    FORBIDDEN = "FORBIDDEN"
    NOT_FOUND = "NOT_FOUND"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    CONFLICT = "CONFLICT"
    INVALID_TRANSITION = "INVALID_TRANSITION"
    STALE_ATTEMPT = "STALE_ATTEMPT"
    CAPABILITY_UNSUPPORTED = "CAPABILITY_UNSUPPORTED"
    NOT_READY = "NOT_READY"
    TARGET_REVOKED = "TARGET_REVOKED"


_STATUS = {
    ErrorCode.UNAUTHENTICATED: 401,
    ErrorCode.FORBIDDEN: 403,
    ErrorCode.NOT_FOUND: 404,
    ErrorCode.VALIDATION_ERROR: 422,
    ErrorCode.CONFLICT: 409,
    ErrorCode.INVALID_TRANSITION: 409,
    ErrorCode.STALE_ATTEMPT: 409,
    ErrorCode.CAPABILITY_UNSUPPORTED: 422,
    ErrorCode.NOT_READY: 409,
    ErrorCode.TARGET_REVOKED: 403,
}


class ErrorBody(BaseModel):
    code: ErrorCode
    message: str
    retryable: bool = False
    next_action: str | None = None
    details: dict | None = None


class PlatformError(Exception):
    """An error a caller can act on. ``retryable`` means the same call may succeed later."""

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        *,
        retryable: bool = False,
        next_action: str | None = None,
        details: dict | None = None,
    ):
        super().__init__(f"[{code.value}] {message}")
        self.body = ErrorBody(
            code=code, message=message, retryable=retryable, next_action=next_action,
            details=details,
        )

    @property
    def code(self) -> ErrorCode:
        return self.body.code

    @property
    def status_code(self) -> int:
        return _STATUS[self.body.code]
