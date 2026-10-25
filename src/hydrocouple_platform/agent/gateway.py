"""The runner gateway: how evaluator runners lease evaluations through their agent.

Runners never hold platform credentials and talk only to their agent. An
evaluator task needs a conversation with the control plane (lease a chunk,
report its results), so the agent serves a small HTTP endpoint, on loopback for
the local pool, and relays each call with its own credential and the attempt's
current attempt token.

Each attempt gets its own random runner token; a token names exactly one held
attempt, so one runner cannot act for another.
"""

from __future__ import annotations

import json
import logging
import secrets
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any

from ..contracts.errors import ErrorBody, ErrorCode, PlatformError

if TYPE_CHECKING:
    from .agent import Agent

log = logging.getLogger(__name__)
MAX_BODY = 32 * 1024 * 1024


class RunnerGateway:
    def __init__(self, agent: Agent, host: str = "127.0.0.1", port: int = 0):
        self._agent = agent
        self._tokens: dict[str, str] = {}
        self._lock = threading.Lock()
        gateway = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args: Any) -> None:  # requests are not logged
                pass

            def do_POST(self) -> None:  # noqa: N802 (http.server naming)
                status, body = gateway._handle(self.path, self.headers.get("Authorization"),
                                               self.rfile, int(self.headers.get(
                                                   "Content-Length") or 0))
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self._server = ThreadingHTTPServer((host, port), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, name="runner-gateway",
                                        daemon=True)
        self._thread.start()

    @property
    def url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    def register(self, attempt_id: str) -> str:
        token = "hcpr_" + secrets.token_urlsafe(32)
        with self._lock:
            self._tokens[token] = attempt_id
        return token

    def unregister(self, attempt_id: str) -> None:
        with self._lock:
            for token in [t for t, a in self._tokens.items() if a == attempt_id]:
                del self._tokens[token]

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()

    def _handle(self, path: str, authorization: str | None, stream, length: int
                ) -> tuple[int, dict]:
        token = (authorization or "").removeprefix("Bearer ").strip()
        with self._lock:
            attempt_id = self._tokens.get(token)
        try:
            if attempt_id is None:
                raise PlatformError(ErrorCode.UNAUTHENTICATED, "unknown runner token")
            if length > MAX_BODY:
                raise PlatformError(ErrorCode.VALIDATION_ERROR, "the request is too large")
            body = json.loads(stream.read(length) or b"{}")
            if path == "/v1/evaluations/lease":
                return 200, self._agent.lease_evaluations(attempt_id, body)
            if path == "/v1/evaluations/complete":
                return 200, self._agent.complete_evaluations(attempt_id, body)
            raise PlatformError(ErrorCode.NOT_FOUND, f"no route {path}")
        except PlatformError as exc:
            return exc.status_code, exc.body.model_dump(mode="json")
        except ValueError as exc:
            body = ErrorBody(code=ErrorCode.VALIDATION_ERROR, message=str(exc)[:2000])
            return 422, body.model_dump(mode="json")
        except Exception as exc:  # the control plane is unreachable: the runner retries
            log.warning("runner gateway call failed: %s", exc)
            body = ErrorBody(code=ErrorCode.NOT_READY, message=f"the agent could not reach the "
                             f"control plane: {type(exc).__name__}", retryable=True)
            return 503, body.model_dump(mode="json")
