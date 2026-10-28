"""Image trust: an agent runs only images signed by keys its site pins.

Images are referenced by digest. Before a container adapter starts an attempt, the
agent:
- rewrites the image to the site's registry mirror, when it has one, keeping the
  digest;
- verifies its cosign signature against the pinned public keys.

An image no pinned key has signed fails the attempt as ``untrusted_image``, which is
never retried. A site that mirrors images copies their signatures with them
(``cosign copy``). Signatures are checked against the keys alone by default, because
a private site may not reach a transparency log. ``require_transparency_log`` adds
the log check where it can.
"""

from __future__ import annotations

import re
import subprocess
import time
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

DIGEST_REF = re.compile(r"^(?P<repository>[^@\s]+)@sha256:(?P<digest>[0-9a-f]{64})$")


class UntrustedImage(Exception):
    pass


class ImageTrust(BaseModel):
    model_config = ConfigDict(extra="forbid")

    keys: list[str] = Field(min_length=1, description="pinned cosign public keys (PEM files)")
    mirror: str | None = Field(default=None, description="registry and path prefix to pull from")
    require_transparency_log: bool = False
    allow_http_registry: bool = False  # a plain-HTTP registry, for test clusters only
    cosign: str = "cosign"
    timeout_seconds: float = Field(default=120, gt=0)
    cache_seconds: float = Field(default=3600, ge=0)


def mirrored(image: str, mirror: str | None) -> str:
    """``image`` pulled from ``mirror`` instead, with the same repository path and digest."""
    match = DIGEST_REF.match(image or "")
    if match is None:
        raise UntrustedImage(f"{image!r} is not an image reference by sha256 digest")
    if not mirror:
        return image
    first, _, rest = match["repository"].partition("/")
    # The first component is a registry host when it has a dot or port, or is localhost.
    path = rest if rest and ("." in first or ":" in first or first == "localhost") else \
        match["repository"]
    return f"{mirror.rstrip('/')}/{path}@sha256:{match['digest']}"


class ImageVerifier:
    def __init__(self, trust: ImageTrust):
        self.trust = trust
        self._verified: dict[str, float] = {}
        missing = [k for k in trust.keys if not Path(k).is_file()]
        if missing:
            raise ValueError(f"pinned image keys not found: {', '.join(missing)}")

    def resolve(self, image: str | None) -> str:
        """The image to run, signed by a pinned key; raises UntrustedImage otherwise."""
        ref = mirrored(image or "", self.trust.mirror)
        checked = self._verified.get(ref)
        if checked is None or time.monotonic() - checked > self.trust.cache_seconds:
            self._verify(ref)
            self._verified[ref] = time.monotonic()
        return ref

    def _verify(self, ref: str) -> None:
        flags = [] if self.trust.require_transparency_log else ["--insecure-ignore-tlog=true"]
        if self.trust.allow_http_registry:
            flags += ["--allow-http-registry", "--allow-insecure-registry"]
        problems = []
        for key in self.trust.keys:
            try:
                done = subprocess.run(
                    [self.trust.cosign, "verify", "--key", key, *flags, ref],
                    capture_output=True, text=True, timeout=self.trust.timeout_seconds)
            except (OSError, subprocess.TimeoutExpired) as exc:
                raise UntrustedImage(f"could not verify {ref}: {exc}") from None
            if done.returncode == 0:
                return
            lines = [line for line in done.stderr.splitlines() if line.strip()]
            problems.append(f"{Path(key).name}: {lines[-1] if lines else 'not verified'}")
        raise UntrustedImage(f"{ref} is not signed by a pinned key ({'; '.join(problems)})")
