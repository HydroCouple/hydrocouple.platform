"""Local-pool adapter: runs each attempt as a runner process on this machine."""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

from ...contracts.agent import Event, LeaseGrant
from ...contracts.capabilities import AdapterCapabilities
from ...contracts.jobs import JobClass
from ...contracts.states import AttemptState
from ...runner import EVENT_PREFIX
from . import Observation


def _memory_gib() -> float:
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 1024**3
    except (ValueError, OSError, AttributeError):
        return 0.0


@dataclass
class _Proc:
    lease: LeaseGrant
    process: subprocess.Popen
    workdir: Path
    started: float
    events: deque = field(default_factory=deque)
    cancelled_by_agent: bool = False
    timed_out: bool = False
    kill_at: float | None = None
    reader: threading.Thread | None = None


class LocalPoolAdapter:
    name = "local-pool"

    def __init__(self, work_root: str | Path, slots: int = 1, python: str = sys.executable,
                 grace_seconds: float = 10.0, keep_workdirs: bool = False):
        self.work_root = Path(work_root).resolve()
        self.work_root.mkdir(parents=True, exist_ok=True)
        self.slots, self.python, self.grace = slots, python, grace_seconds
        self.keep_workdirs = keep_workdirs
        self._procs: dict[str, _Proc] = {}

    def capabilities(self) -> AdapterCapabilities:
        return AdapterCapabilities(
            adapter=self.name, family="local",
            job_classes=[JobClass.SINGLE, JobClass.ANALYSIS],
            has_runtime_cap=False, max_task_seconds=None, max_parallel_tasks=1,
            max_cpu=float(os.cpu_count() or 1), max_memory_gib=round(_memory_gib(), 2),
            gpu=[], scratch_gib=round(shutil.disk_usage(self.work_root).free / 1024**3, 2),
            scratch_in_memory=False, interruption_tolerant=False, provider_retries=0,
            idempotent_submit=True, status_integration="direct", runs_images=False,
        )

    def free_slots(self) -> int:
        running = sum(1 for p in self._procs.values() if p.process.poll() is None)
        return max(self.slots - running, 0)

    def submit(self, lease: LeaseGrant, store_uri: str) -> str:
        handle = lease.attempt_id
        if handle in self._procs:  # idempotent: the same attempt is never started twice
            return handle
        workdir = self.work_root / lease.attempt_id
        workdir.mkdir(parents=True, exist_ok=True)
        spec_path = workdir / "spec.json"
        spec_path.write_text(lease.spec.model_dump_json(indent=2), encoding="utf-8")
        env = dict(os.environ, OMP_NUM_THREADS=str(lease.spec.runtime.threads),
                   PYTHONUNBUFFERED="1")
        stderr = open(workdir / "runner.stderr.log", "wb")
        process = subprocess.Popen(
            [self.python, "-m", "hydrocouple_platform.runner", "--spec", str(spec_path),
             "--attempt-id", lease.attempt_id, "--attempt-number", str(lease.fencing_token),
             "--store", store_uri, "--workdir", str(workdir)],
            stdout=subprocess.PIPE, stderr=stderr, env=env, start_new_session=True,
        )
        stderr.close()
        proc = _Proc(lease=lease, process=process, workdir=workdir, started=time.monotonic())
        proc.reader = threading.Thread(target=self._read, args=(proc,), daemon=True)
        proc.reader.start()
        self._procs[handle] = proc
        return handle

    @staticmethod
    def _read(proc: _Proc) -> None:
        assert proc.process.stdout is not None
        for raw in proc.process.stdout:
            line = raw.decode("utf-8", "replace").rstrip("\n")
            if line.startswith(EVENT_PREFIX):
                try:
                    proc.events.append(Event.model_validate(json.loads(line[len(EVENT_PREFIX):])))
                except ValueError:
                    pass

    def inspect(self, handle: str) -> Observation:
        proc = self._procs[handle]
        code = proc.process.poll()
        limit = proc.lease.spec.limits.execution_timeout_seconds
        if code is None and not proc.timed_out and time.monotonic() - proc.started > limit:
            proc.timed_out = True
            self._terminate(proc)
        if code is None and proc.kill_at is not None and time.monotonic() >= proc.kill_at:
            os.killpg(proc.process.pid, signal.SIGKILL)
        if code is not None and proc.reader is not None:
            proc.reader.join(timeout=5)
        events = []
        while proc.events:
            events.append(proc.events.popleft())
        observation = Observation(
            state=AttemptState.RUNNING if code is None else AttemptState.FINALIZING,
            finished=code is not None,
            exit_code=code if code is not None and code >= 0 else None,
            signal=-code if code is not None and code < 0 else None,
            cancelled_by_agent=proc.cancelled_by_agent, timed_out=proc.timed_out,
            provider_reference=f"pid:{proc.process.pid}", events=events,
        )
        if code is not None and code != 0:
            log = proc.workdir / "runner.stderr.log"
            if log.exists():
                observation.stderr_tail = log.read_text("utf-8", "replace")[-2000:]
        return observation

    def _terminate(self, proc: _Proc) -> None:
        if proc.process.poll() is None:
            try:
                os.killpg(proc.process.pid, signal.SIGTERM)
            except ProcessLookupError:
                return
            proc.kill_at = time.monotonic() + self.grace

    def cancel(self, handle: str) -> None:
        proc = self._procs.get(handle)
        if proc is not None:
            proc.cancelled_by_agent = True
            self._terminate(proc)

    def cleanup(self, handle: str) -> None:
        proc = self._procs.pop(handle, None)
        if proc is not None and not self.keep_workdirs:
            shutil.rmtree(proc.workdir, ignore_errors=True)

    def handles(self) -> list[str]:
        return list(self._procs)
