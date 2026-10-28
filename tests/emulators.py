"""Starting storage emulators for tests: moto (S3), Azurite (Blob Storage), fake-gcs-server.

Each is found through an environment variable or on PATH (and beside the running Python),
and its log goes under ``test_artifacts/emulators/``. A test whose emulator is missing
skips.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

from conftest import ARTIFACTS

LOGS = ARTIFACTS / "emulators"


def free_port(host: str = "127.0.0.1") -> int:
    with socket.socket() as s:
        s.bind((host, 0))
        return s.getsockname()[1]


def binary(env: str, name: str) -> str | None:
    found = os.environ.get(env) or shutil.which(name) or shutil.which(
        name, path=str(Path(sys.executable).parent))
    return found if found and Path(found).exists() else None


def start(name: str, command: list[str], port: int) -> subprocess.Popen:
    """Start an emulator and wait until it answers HTTP on ``port``."""
    LOGS.mkdir(parents=True, exist_ok=True)
    log = open(LOGS / f"{name}.log", "wb")
    process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, cwd=LOGS)
    deadline = time.monotonic() + 30
    while True:
        try:
            httpx.get(f"http://127.0.0.1:{port}/", timeout=1)
            return process
        except httpx.HTTPError:
            if process.poll() is not None or time.monotonic() > deadline:
                process.kill()
                pytest.fail(f"the {name} emulator did not start; see {LOGS / name}.log")
            time.sleep(0.2)


def moto(host: str = "127.0.0.1") -> tuple[subprocess.Popen, int]:
    path = binary("HCP_TEST_MOTO_SERVER", "moto_server")
    if path is None:
        pytest.skip("moto_server is not installed (pip install 'moto[server]')")
    port = free_port(host)
    return start("moto", [path, "-H", host, "-p", str(port)], port), port
