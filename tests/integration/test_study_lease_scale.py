"""Study lease and completion latency by pool size (Phase 5, work package 5.2a).

A measurement, opt in with ``HCP_TEST_LEASE_SCALE=1`` (minutes): for each pool size (default
8, 32, 128 and 256 tasks, or ``HCP_TEST_LEASE_POOLS``), one study evaluates one batch of 1,024
candidates on that many emulated tasks. Each task is a thread scripted against the agent routes
in process (the services, PostgreSQL, no engine and no HTTP), leasing a chunk, sleeping
``HCP_TEST_LEASE_EVAL_SECONDS`` (default 0.02 s) per candidate in place of an episode, and
completing it. Every call's latency is recorded. Results, with the raw latencies, stay in
``test_artifacts/test_study_lease_scale/``; the test only asserts that every candidate was
evaluated once. ``HCP_TEST_LEASE_DB_POOL`` gives the measurement its own connection pool of that
size, to tell the database pool's limit from the study lock's.
"""

from __future__ import annotations

import csv
import json
import multiprocessing
import os
import statistics
import threading
import time
from pathlib import Path

import pytest
from test_studies import Task, candidates, create

from hydrocouple_platform.contracts.jobs import JobClass
from hydrocouple_platform.contracts.studies import BatchSubmit
from world import local_capabilities, make_world, ready_revision

pytestmark = [pytest.mark.postgres,
              pytest.mark.skipif(not os.environ.get("HCP_TEST_LEASE_SCALE"),
                                 reason="a measurement; set HCP_TEST_LEASE_SCALE=1 to run it")]

POOLS = [int(p) for p in os.environ.get("HCP_TEST_LEASE_POOLS", "8,32,128,256").split(",")]
EVAL_SECONDS = float(os.environ.get("HCP_TEST_LEASE_EVAL_SECONDS", "0.02"))
PROCESSES = int(os.environ.get("HCP_TEST_LEASE_PROCESSES", "8"))
CANDIDATES = 1024


def percentiles(values: list[float]) -> dict[str, float]:
    if not values:
        return {}
    q = statistics.quantiles(values, n=100, method="inclusive")
    return {"n": len(values), "p50_ms": round(1000 * q[49], 2), "p95_ms": round(1000 * q[94], 2),
            "p99_ms": round(1000 * q[98], 2), "max_ms": round(1000 * max(values), 2)}


def _drive(world, tasks: list[Task], eval_seconds: float) -> tuple[list, int]:
    """Each task leases and completes chunks until none is left; one thread per task."""
    calls: list[tuple[str, int, float]] = []  # (call, chunk size, seconds)
    lock, evaluated = threading.Lock(), [0]

    def work(task: Task) -> None:
        while True:
            t0 = time.perf_counter()
            response = task.lease_chunk()
            lease = time.perf_counter() - t0
            if response.chunk is None:
                with lock:
                    calls.append(("empty_lease", 0, lease))
                return
            size = len(response.chunk.candidates)
            time.sleep(eval_seconds * size)
            t1 = time.perf_counter()
            ack = task.complete(response.chunk)
            done = time.perf_counter() - t1
            assert ack.accepted
            with lock:
                calls.append(("lease", size, lease))
                calls.append(("complete", size, done))
                evaluated[0] += size

    threads = [threading.Thread(target=work, args=(t,)) for t in tasks]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return calls, evaluated[0]


def _worker(job) -> dict:
    """One process of emulated tasks with its own services and connection pool."""
    from types import SimpleNamespace

    from hydrocouple_platform.config import ControlPlaneSettings
    from hydrocouple_platform.contracts.agent import LeaseGrant
    from hydrocouple_platform.db import make_engine, make_sessionmaker
    from hydrocouple_platform.services import PlatformServices
    from hydrocouple_platform.storage import LocalStore

    settings, store_root, root, target_id, leases, eval_seconds, out = job
    settings = ControlPlaneSettings(**settings)
    engine = make_engine(settings.database_url)
    world = SimpleNamespace(services=PlatformServices(make_sessionmaker(engine), settings),
                            target_id=target_id, store=LocalStore(Path(store_root)),
                            root=Path(root))
    tasks = [Task(world, LeaseGrant.model_validate(x)) for x in leases]
    cpu, start = time.process_time(), time.time()
    calls, evaluated = _drive(world, tasks, eval_seconds)
    report = {"calls": calls, "evaluated": evaluated, "cpu_seconds": time.process_time() - cpu,
              "start": start, "end": time.time()}
    Path(out).write_text(json.dumps(report) + "\n")
    engine.dispose()
    return report


def measure(services, artifact_dir, pool: int) -> dict:
    from conftest import DATA

    world = make_world(services, artifact_dir / f"pool_{pool}", org_name=f"Utility {pool}")
    caps = local_capabilities(job_classes=[JobClass.SINGLE, JobClass.EVALUATOR_POOL],
                              max_parallel_tasks=pool)
    revision = ready_revision(world, DATA / "twin_tank_heavy" / "model.inp")
    world.poll(free_slots=0, caps=caps)  # after the upload's grant polls, which report defaults
    study = create(world, revision, key=f"lease-scale-{pool:04d}", pool_size=pool,
                   max_evaluations=CANDIDATES)
    world.services.submit_batch(world.admin, study.study_id,
                                BatchSubmit(candidates=candidates(CANDIDATES)),
                                f"lease-scale-batch-{pool:04d}")
    tasks: list[Task] = []
    for _ in range(10):  # dispatch and a poll each handle a bounded number; report those held
        world.services.dispatch()
        tasks += [Task(world, lease) for lease in world.poll(
            free_slots=pool - len(tasks), caps=caps,
            held=[t.attempt_id for t in tasks]).leases]
        if len(tasks) >= pool:
            break
    assert len(tasks) == pool == len({t.attempt_id for t in tasks}), (
        f"{len(tasks)} leases for {len({t.attempt_id for t in tasks})} attempts of {pool}")
    # The tasks, split over processes that each run their own control-plane services (as the
    # API's workers would) against the same database, so one Python process's lock on its own
    # interpreter is not what is measured.
    groups = [tasks[k::PROCESSES] for k in range(min(PROCESSES, pool))]
    store_root = world.root / "store"
    jobs = [(world.services.settings.model_dump(), str(store_root), str(world.root),
             world.target_id, [t.lease.model_dump(mode="json") for t in group], EVAL_SECONDS,
             str(artifact_dir / f"pool_{pool}_worker_{k}.json"))
            for k, group in enumerate(groups)]
    context = multiprocessing.get_context("spawn")
    with context.Pool(len(jobs)) as workers:
        reports = workers.map(_worker, jobs)
    # From the first worker's first call to the last one's end, not counting process start-up.
    wall = max(r["end"] for r in reports) - min(r["start"] for r in reports)
    calls = [tuple(c) for r in reports for c in r["calls"]]
    evaluated = [sum(r["evaluated"] for r in reports)]
    cpu = sum(r["cpu_seconds"] for r in reports)
    with (artifact_dir / f"latencies_pool_{pool}.csv").open("w", newline="") as stream:
        w = csv.writer(stream)
        w.writerow(["call", "chunk_size", "seconds"])
        w.writerows(calls)
    sizes = sorted({size for call, size, _ in calls if call == "lease"})
    return {
        "pool": pool, "candidates": CANDIDATES, "evaluated": evaluated[0],
        "chunk_sizes": sizes, "eval_seconds_per_candidate": EVAL_SECONDS,
        "wall_seconds": round(wall, 3),
        "processes": len(jobs),
        # The workers' CPU time over wall time, per process: near 1, a worker's Python is the
        # limit rather than the database.
        "cpu_per_wall_per_process": round(cpu / wall / len(jobs), 2),
        "evaluations_per_second": round(evaluated[0] / wall, 1),
        "ideal_seconds": round(CANDIDATES * EVAL_SECONDS / pool, 3),
        "lease": percentiles([s for call, _, s in calls if call == "lease"]),
        "complete": percentiles([s for call, _, s in calls if call == "complete"]),
        "empty_lease": percentiles([s for call, _, s in calls if call == "empty_lease"]),
    }


def test_lease_and_completion_latency_by_pool_size(services, settings, clock, artifact_dir):
    db_pool = os.environ.get("HCP_TEST_LEASE_DB_POOL")
    if db_pool:  # a larger connection pool than the control plane's default, to separate the two
        from sqlalchemy import create_engine

        from hydrocouple_platform.db import make_sessionmaker
        from hydrocouple_platform.services import PlatformServices

        services = PlatformServices(make_sessionmaker(create_engine(
            settings.database_url, pool_size=int(db_pool), max_overflow=0, pool_pre_ping=True)),
            settings, clock)
    engine = services._factory.kw["bind"]  # the control plane's SQLAlchemy engine
    results = {"database_pool": {"size": engine.pool.size(),
                                 "max_overflow": engine.pool._max_overflow},
               "pools": []}
    for pool in POOLS:
        r = measure(services, artifact_dir, pool)
        results["pools"].append(r)
        (artifact_dir / "lease_scale.json").write_text(json.dumps(results, indent=2) + "\n")
        assert r["evaluated"] == CANDIDATES, r
