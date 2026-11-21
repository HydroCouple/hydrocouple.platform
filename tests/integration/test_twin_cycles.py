"""Twin cycles that resume from the previous cycle's saved state (Phase 4, 4.2a, 4.2b, 4.4).

Two ``twin.cycle@1`` runs chained by the first one's ``state.h5`` reach the state one run takes
over the same period with records at the same times, bit for bit; a tampered record fails its
checksum before the engine reads it; a revision whose past rain was rewritten is refused by the
engine, naming the series. A replay schedule of six cycles runs them in order, each resuming the
last, to the same state; a cycle that fails pauses its schedule, and resuming submits that cycle
again. Each cycle's forecast proposes setpoints; only a person decides them, a run applies only
approved ones, and the next cycle applies and records them. Runs, downloads and the reference
keep their files under ``test_artifacts/`` for review.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from conftest import DATA
from hydrocouple_platform.agent import Agent, AgentClient
from hydrocouple_platform.agent.adapters.local import LocalPoolAdapter
from hydrocouple_platform.client import PlatformClient
from hydrocouple_platform.contracts.errors import PlatformError
from hydrocouple_platform.contracts.jobs import EgressPolicy
from hydrocouple_platform.contracts.manifests import canonical_json
from hydrocouple_platform.packaging import package_swmm_model
from hydrocouple_platform.storage import sha256_file
from world import make_world, ready_revision

pytestmark = pytest.mark.postgres

MODEL = DATA / "twin_tank_heavy" / "model.inp"
START = datetime(2024, 1, 1)
CYCLE_ENDS = [START + timedelta(hours=2), START + timedelta(hours=4)]
# Rain (in/h) every 5 minutes: a storm in the first hour, another in the third.
RAIN = ([0.0, 2.0, 6.0, 9.0, 12.0, 8.0, 5.0, 3.0, 1.0, 0.5, 0.0, 0.0] + [0.0] * 18
        + [4.0, 7.0, 11.0, 6.0, 3.0, 1.0] + [0.0] * 12)


def rain_csv(path: Path, rain: list[float]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [f"{(START + i * timedelta(minutes=5)).isoformat()},{v}" for i, v in enumerate(rain)]
    path.write_text("time,RG\n" + "\n".join(rows) + "\n")
    return path


def cycle(cycle_end: datetime) -> dict:
    return {"cycle_end": cycle_end.isoformat(timespec="minutes"),
            "telemetry": [{"gage": "RG", "series": "data/rain.csv"}]}


@pytest.fixture
def engine_continues():
    """Skip unless the installed engine continues into a model with longer series."""
    engine = pytest.importorskip("openswmm.engine")
    capabilities = getattr(engine, "capabilities", None)  # the published 6.0.0a4.dev1 has none
    if capabilities is None or "continue_extended_forcing" not in capabilities()["checkpoints"]:
        pytest.skip("the installed openswmm engine cannot continue into extended forcing")


class Platform:
    """The dispatcher, reconciler and agent loops, in the background."""

    def __init__(self, services, world, server, artifact_dir):
        self.services = services
        self.adapter = LocalPoolAdapter(artifact_dir / "agent-work", slots=2, keep_workdirs=True,
                                        grace_seconds=3)
        self.agent = Agent(AgentClient(server, world.credential), self.adapter, world.store,
                           world.store_uri, EgressPolicy.SUMMARIES)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self.errors: list[Exception] = []

    def _loop(self):
        while not self._stop.is_set():
            try:
                self.services.dispatch()
                self.services.reconcile()
                self.agent.run_once()
            except Exception as exc:
                self.errors.append(exc)
                return
            time.sleep(0.1)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join(timeout=30)
        for handle in self.adapter.handles():
            self.adapter.cancel(handle)
        self.agent.close()
        if self.errors:
            raise self.errors[0]


def reference(folder: Path, csv: Path, cycle_ends: list[datetime] = CYCLE_ENDS) -> list[dict]:
    """One run over every cycle, records at each cycle's end: the state at each cycle's end."""
    from openswmm.engine import EngineState, Solver, catalog

    from hydrocouple_platform.runner.twin import _feed

    folder.mkdir(parents=True, exist_ok=True)
    solver = Solver(str(MODEL), str(folder / "reference.rpt"), "")
    states = []
    try:
        solver.open()
        catalog.resolve(solver, "options", None).end_datetime = cycle_ends[-1]
        (folder / "data").mkdir(exist_ok=True)
        (folder / "data" / "rain.csv").write_bytes(csv.read_bytes())
        _feed(solver, folder, [{"gage": "RG", "series": "data/rain.csv"}], cycle_ends[-1])
        r = solver.restart
        r.record_file, r.record_format = str(folder / "reference.h5"), "HDF5"
        r.record_at, r.record_continuation = [*cycle_ends[:-1], "END"], True
        solver.initialize()
        solver.start()
        def state() -> dict:
            return {"nodes": {n.id: float(n.depth) for n in solver.nodes},
                    "links": {lk.id: {"flow": float(lk.flow),
                                      "setting": float(lk.control_setting)}
                              for lk in solver.links}}

        due = list(cycle_ends[:-1])
        while int(solver.state) in (EngineState.STARTED, EngineState.RUNNING):
            solver.step()
            if due and solver.current_datetime >= due[0] - timedelta(milliseconds=1):
                due.pop(0)
                states.append(state())
        states.append(state())  # at the end, where a cycle takes its end state too
        solver.end()
    finally:
        solver.close()
        solver.destroy()
    (folder / "states.json").write_text(json.dumps(states, indent=2) + "\n")
    return states


def store_file(world, run_id: str, path: str) -> Path:
    (found,) = (world.root / "store").glob(f"runs/**/{run_id}/*/{path}")
    return found


@pytest.mark.engine
def test_chained_cycles_equal_one_run_and_refuse_bad_records(services, artifact_dir, server,
                                                              make_token, engine_continues):
    world = make_world(services, artifact_dir)
    csv = rain_csv(artifact_dir / "inputs" / "rain.csv", RAIN)
    rewritten = list(RAIN)
    rewritten[4] = 13.0  # the first storm's peak, before the first cycle's end
    past = rain_csv(artifact_dir / "inputs" / "rain_rewritten.csv", rewritten)
    with Platform(services, world, server, artifact_dir), \
            PlatformClient(server, make_token("alice")) as client:
        revision = client.upload_revision(
            package_swmm_model(MODEL, artifact_dir / "package",
                               extra_files={"data/rain.csv": csv}),
            project_id=world.project_id, target_id=world.target_id)
        history = client.upload_revision(
            package_swmm_model(MODEL, artifact_dir / "package_rewritten",
                               extra_files={"data/rain.csv": past}),
            project_id=world.project_id, target_id=world.target_id)

        def run(name, rev, cycle_end, resume_from=None):
            body = {"project_id": world.project_id, "revision_id": rev["revision_id"],
                    "workflow": "twin.cycle", "workflow_version": "1",
                    "target_id": world.target_id, "parameters": cycle(cycle_end),
                    "retry_budget": 0}
            if resume_from is not None:
                body["resume_from"] = resume_from
            accepted = client.submit_run(body, idempotency_key=f"twin-{name}")
            return accepted, client.wait_for_job(accepted["job_id"], timeout=180)

        first = run("first", revision, CYCLE_ENDS[0])
        assert first[1]["state"] == "SUCCEEDED", first[1]
        second = run("second", revision, CYCLE_ENDS[1], first[0]["run_id"])
        assert second[1]["state"] == "SUCCEEDED", second[1]
        rewritten_run = run("rewritten", history, CYCLE_ENDS[1], first[0]["run_id"])

        cycles = [json.loads(client.download(r[0]["run_id"], ["cycle.json"],
                                             artifact_dir / "downloaded" / name)
                             ["cycle.json"].read_text())
                  for name, r in (("first", first), ("second", second))]
        manifest = client.attempt_manifest(second[0]["run_id"])["attempt_manifest"]
        first_manifest = client.attempt_manifest(first[0]["run_id"])["attempt_manifest"]

        # The first cycle's record, tampered with in the target's store.
        record = store_file(world, first[0]["run_id"], "state.h5")
        record.write_bytes(record.read_bytes()[:-1] + bytes([record.read_bytes()[-1] ^ 1]))
        tampered = run("tampered", revision, CYCLE_ENDS[1], first[0]["run_id"])

    expected = reference(artifact_dir / "reference", csv)
    assert [c["end_state"] for c in cycles] == expected
    assert cycles[0]["resumed_from"] is None
    (state,) = [a for a in first_manifest["artifacts"] if a["path"] == "state.h5"]
    assert manifest["provenance"]["resumed_from"] == {
        "run_id": first[0]["run_id"], "attempt_id": first_manifest["provenance"]["attempt_id"],
        "path": "state.h5", "sha256": state["sha256"]}
    assert "resumed_from" not in first_manifest["provenance"]
    assert manifest["provenance"]["inputs"]["resume/state.h5"] == state["sha256"]

    view = rewritten_run[1]
    assert view["state"] == "FAILED" and view["failure"]["exit_code"] == 10, view
    assert "time series telemetry:RG" in view["failure"]["diagnostics"]["message"], view
    view = tampered[1]
    assert view["state"] == "FAILED" and view["failure"]["exit_code"] == 13, view
    assert "does not match its checksum" in view["failure"]["diagnostics"]["message"], view


def test_only_a_published_run_of_the_same_resumable_workflow_can_be_resumed(services,
                                                                             artifact_dir):
    world = make_world(services, artifact_dir)
    revision = ready_revision(world, MODEL)

    def request(workflow, version, parameters, **kw):
        return world.run_request(revision["revision_id"], **kw).model_copy(
            update={"workflow": workflow, "workflow_version": version,
                    "parameters": parameters})

    simulation = services.submit_run(world.admin, world.run_request(revision["revision_id"]),
                                     "resume-simulation")
    pending = services.submit_run(world.admin, request("twin.cycle", "1", cycle(CYCLE_ENDS[0])),
                                  "resume-pending")
    with pytest.raises(PlatformError, match="does not continue from an earlier run"):
        services.estimate(world.admin, world.run_request(revision["revision_id"],
                                                         resume_from=pending.run_id))
    for earlier, finding in ((simulation.run_id, "ran swmm.simulation, not twin.cycle"),
                             (pending.run_id, "has no published result yet")):
        estimate = services.estimate(world.admin, request(
            "twin.cycle", "1", cycle(CYCLE_ENDS[1]), resume_from=earlier))
        assert not estimate.ok and any(finding in f for f in estimate.findings), estimate
    with pytest.raises(PlatformError, match="was not found"):
        services.estimate(world.admin, request("twin.cycle", "1", cycle(CYCLE_ENDS[1]),
                                               resume_from="run_missing"))


def wait_for_schedule(client, schedule_id: str, states: set[str], timeout: float = 300) -> dict:
    deadline = time.monotonic() + timeout
    while True:
        view = client.schedule(schedule_id)
        if view["state"] in states or time.monotonic() > deadline:
            return view
        time.sleep(0.3)


@pytest.mark.engine
def test_a_replay_schedule_runs_its_cycles_in_order_and_pauses_on_a_failure(
        services, artifact_dir, server, make_token, engine_continues):
    world = make_world(services, artifact_dir)
    csv = rain_csv(artifact_dir / "inputs" / "rain.csv", RAIN)
    bad = artifact_dir / "inputs" / "rain_bad.csv"
    lines = csv.read_text().splitlines()
    lines[31] = lines[31].split(",")[0] + ",n/a"  # 02:30, inside the fourth 40-minute cycle
    bad.write_text("\n".join(lines) + "\n")
    ends = [START + timedelta(minutes=40 * (i + 1)) for i in range(6)]
    with Platform(services, world, server, artifact_dir), \
            PlatformClient(server, make_token("alice")) as client:
        views = {}
        for name, source in (("replay", csv), ("bad", bad)):
            revision = client.upload_revision(
                package_swmm_model(MODEL, artifact_dir / f"package_{name}",
                                   extra_files={"data/rain.csv": source}),
                project_id=world.project_id, target_id=world.target_id)
            created = client.create_schedule({
                "project_id": world.project_id, "revision_id": revision["revision_id"],
                "target_id": world.target_id, "workflow": "twin.cycle", "workflow_version": "1",
                "parameters": {"telemetry": [{"gage": "RG", "series": "data/rain.csv"}]},
                "first_cycle_end": ends[0].isoformat(), "cycle_minutes": 40, "cycles": 6,
                "retry_budget": 0}, idempotency_key=f"schedule-{name}")
            views[name] = created["schedule_id"]
        replay = wait_for_schedule(client, views["replay"], {"COMPLETED", "PAUSED"})
        paused = wait_for_schedule(client, views["bad"], {"COMPLETED", "PAUSED"})
        resumed = client.resume_schedule(views["bad"])
        again = wait_for_schedule(client, views["bad"], {"PAUSED"})
        last = json.loads(client.download(replay["history"][-1]["run_id"], ["cycle.json"],
                                          artifact_dir / "downloaded")["cycle.json"].read_text())
        manifests = [client.attempt_manifest(c["run_id"])["attempt_manifest"]
                     for c in replay["history"]]
    (artifact_dir / "schedules.json").write_text(json.dumps(
        {"replay": replay, "paused": paused, "again": again}, indent=2, default=str) + "\n")

    assert replay["state"] == "COMPLETED", replay
    history = replay["history"]
    assert [c["number"] for c in history] == list(range(6))
    assert [c["cycle_end"] for c in history] == [e.isoformat() for e in ends]
    assert [c["resumed_from"] for c in history] == [None] + [c["run_id"] for c in history[:-1]]
    assert all(c["job_state"] == "SUCCEEDED" and c["attempt"] == 1 for c in history)
    assert [m["provenance"].get("resumed_from", {}).get("run_id") for m in manifests] == \
        [None] + [c["run_id"] for c in history[:-1]]
    assert last["end_state"] == reference(artifact_dir / "reference", csv, ends)[-1]

    assert paused["state"] == "PAUSED" and paused["next_cycle"] == 3, paused
    assert paused["pause_reason"]["cycle"] == 3
    assert paused["pause_reason"]["job_state"] == "FAILED"
    assert "rain.csv line 32" in paused["pause_reason"]["failure"]["diagnostics"]["message"]
    assert paused["last_run_id"] == paused["history"][2]["run_id"]
    assert resumed["state"] == "ACTIVE" and resumed["pause_reason"] is None
    assert again["next_cycle"] == 3 and [(c["number"], c["attempt"]) for c in
                                          again["history"]] == [(0, 1), (1, 1), (2, 1), (3, 1),
                                                                (3, 2)]
    assert again["history"][4]["resumed_from"] == again["history"][2]["run_id"]


def test_a_schedule_needs_a_resumable_workflow_and_whole_cycles(services, artifact_dir):
    from hydrocouple_platform.contracts.schedules import ScheduleCreate

    world = make_world(services, artifact_dir)
    revision = ready_revision(world, MODEL)
    base = dict(project_id=world.project_id, revision_id=revision["revision_id"],
                target_id=world.target_id, workflow="twin.cycle", workflow_version="1",
                first_cycle_end=CYCLE_ENDS[0], cycle_minutes=60, cycles=2)
    for change, message in (
            ({"workflow": "swmm.simulation"}, "cannot run on a schedule"),
            ({"parameters": {"cycle_end": "2024-01-01T01:00"}}, "leave it out"),
            ({"cycle_minutes": 0.25 / 60}, "whole number of seconds"),
            ({"first_cycle_end": "2024-01-01T02:00:00+00:00"}, "without a zone"),
            ({"lag_seconds": 30}, "wall_clock schedules only")):
        with pytest.raises(PlatformError, match=message):
            services.create_schedule(world.admin, ScheduleCreate(**{**base, **change}),
                                     f"refused-{message[:20]}")
    created = services.create_schedule(world.admin, ScheduleCreate(**base), "schedule-twice")
    assert services.create_schedule(world.admin, ScheduleCreate(**base),
                                    "schedule-twice") == created
    with pytest.raises(PlatformError, match="already used for a different schedule"):
        services.create_schedule(world.admin, ScheduleCreate(**{**base, "cycles": 3}),
                                 "schedule-twice")
    assert services.pause_schedule(world.admin, created.schedule_id).state == "PAUSED"
    assert services.resume_schedule(world.admin, created.schedule_id).state == "ACTIVE"


def agent_principal(world):
    from hydrocouple_platform.contracts.requests import AgentSessionCreate, Budget

    session = world.services.create_agent_session(world.admin, AgentSessionCreate(
        project_id=world.project_id, client_name="claude-code", budget=Budget()))
    return world.services.session_principal(session["agent_session_token"])


def test_only_a_person_decides_and_a_run_applies_only_approved_setpoints(services,
                                                                          artifact_dir):
    from hydrocouple_platform.contracts.schedules import (
        ProposalCreate,
        ProposalDecision,
        ScheduleCreate,
    )

    world = make_world(services, artifact_dir)
    revision = ready_revision(world, MODEL)
    schedule = services.create_schedule(world.admin, ScheduleCreate(
        project_id=world.project_id, revision_id=revision["revision_id"],
        target_id=world.target_id, workflow="twin.cycle", workflow_version="1",
        first_cycle_end=CYCLE_ENDS[0], cycle_minutes=120, cycles=2), "proposals-schedule")
    agent = agent_principal(world)
    when = START + timedelta(minutes=150)
    proposed = services.propose_setpoint(agent, schedule.schedule_id, ProposalCreate(
        link="ORIF", value=0.4, effective_from=when, note="lower the orifice before the storm"))
    assert proposed.state == "PROPOSED" and proposed.proposed_by["kind"] == "agent"
    with pytest.raises(PlatformError, match="whole seconds"):
        services.propose_setpoint(agent, schedule.schedule_id, ProposalCreate(
            link="ORIF", value=0.4, effective_from=when + timedelta(microseconds=5)))

    def cycle_run(principal, setpoint):
        parameters = {**cycle(CYCLE_ENDS[1]), "setpoints": [setpoint]}
        return services.estimate(principal, world.run_request(revision["revision_id"])
                                 .model_copy(update={"workflow": "twin.cycle",
                                                     "workflow_version": "1",
                                                     "parameters": parameters}))

    setpoint = {"link": "ORIF", "value": 0.4, "effective_from": when.isoformat(),
                "proposal_id": proposed.proposal_id}
    for principal in (agent, world.admin):  # undecided: nobody's run applies it
        with pytest.raises(PlatformError, match="not a proposal a person approved"):
            cycle_run(principal, setpoint)
    with pytest.raises(PlatformError, match="cannot approve or reject a setpoint"):
        services.decide_proposal(agent, proposed.proposal_id, ProposalDecision(approve=True))
    decided = services.decide_proposal(world.admin, proposed.proposal_id,
                                       ProposalDecision(approve=True, note="agreed"))
    assert (decided.state, decided.decided_by) == ("APPROVED", "alice")
    with pytest.raises(PlatformError, match="is APPROVED, not PROPOSED"):
        services.decide_proposal(world.admin, proposed.proposal_id,
                                 ProposalDecision(approve=False))
    assert cycle_run(agent, setpoint).ok
    with pytest.raises(PlatformError, match="not a proposal a person approved"):
        cycle_run(agent, setpoint | {"value": 0.9})  # not as approved
    assert [p.proposal_id for p in services.list_proposals(
        world.admin, schedule.schedule_id)] == [proposed.proposal_id]


POLICY = {"link": "ORIF", "observe": "node.depth", "element": "T1", "x": [0.0, 2.0, 6.0],
          "setting": [1.0, 0.6, 0.2], "resolution": 0.2}


@pytest.mark.engine
def test_approved_proposals_are_applied_by_the_next_cycle_and_recorded(
        services, artifact_dir, server, make_token, engine_continues):
    world = make_world(services, artifact_dir)
    csv = rain_csv(artifact_dir / "inputs" / "rain.csv", RAIN)
    decisions = []
    with Platform(services, world, server, artifact_dir), \
            PlatformClient(server, make_token("alice")) as client:
        revision = client.upload_revision(
            package_swmm_model(MODEL, artifact_dir / "package",
                               extra_files={"data/rain.csv": csv}),
            project_id=world.project_id, target_id=world.target_id)
        schedule = client.create_schedule({
            "project_id": world.project_id, "revision_id": revision["revision_id"],
            "target_id": world.target_id, "workflow": "twin.cycle", "workflow_version": "1",
            "parameters": {"telemetry": [{"gage": "RG", "series": "data/rain.csv"}],
                           "forecast": {"horizon_minutes": 60, "every_minutes": 10,
                                        "observe": [{"path": "node.depth", "id": "T1"}]},
                           "policy": POLICY},
            "first_cycle_end": START.replace(minute=40).isoformat(), "cycle_minutes": 40,
            "cycles": 4, "await_decisions": True, "retry_budget": 0},
            idempotency_key="proposals-applied")["schedule_id"]
        from hydrocouple_platform.contracts.schedules import ProposalCreate

        agent, agent_proposal = agent_principal(world), None
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            view = client.schedule(schedule)
            if view["state"] != "ACTIVE":
                break
            seen = {d["proposal_id"] for d in decisions}
            fresh = [p for p in client.proposals(schedule)
                     if p["state"] == "PROPOSED" and p["proposal_id"] not in seen]
            if fresh and agent_proposal is None:  # an agent proposes too, after the first forecast
                agent_proposal = services.propose_setpoint(agent, schedule, ProposalCreate(
                    link="ORIF", value=0.5, effective_from=START.replace(hour=1, minute=30),
                    note="open the orifice halfway before the second storm"))
                fresh.append(client.proposals(schedule)[-1])
            # A person decides each new proposal: the agent's and the first of each forecast are
            # approved, the forecast's others rejected.
            forecast = sorted((p for p in fresh if p["run_id"]), key=lambda p: p["effective_from"])
            first = forecast[0]["proposal_id"] if forecast else None
            for p in fresh:
                approve = p["run_id"] is None or p["proposal_id"] == first
                decisions.append(client.decide_proposal(p["proposal_id"], approve=approve))
            time.sleep(0.3)
        proposals = client.proposals(schedule)
        # What the knowledge graph loads: each cycle's attempt manifest and forecast, the
        # schedule view and its proposals (water-ontology's twin fixture is made from these).
        graph = artifact_dir / "graph"
        cycles = {}
        for c in view["history"]:
            folder = graph / "attempts" / str(c["number"])
            files = client.download(c["run_id"], ["forecast.json", "cycle.json"], folder)
            cycles[c["run_id"]] = json.loads(files["cycle.json"].read_text())
            published = client.attempt_manifest(c["run_id"])
            (folder / "attempt.json").write_bytes(canonical_json(published["attempt_manifest"]))
            assert sha256_file(folder / "attempt.json") == published["attempt_manifest_sha256"]
        (graph / "schedule.json").write_text(json.dumps(view, indent=2) + "\n")
        (graph / "proposals.json").write_text(json.dumps(proposals, indent=2) + "\n")

    assert view["state"] == "COMPLETED", view
    for c in view["history"]:  # one sample every 10 minutes, to the hour's horizon
        forecast = json.loads((graph / "attempts" / str(c["number"]) / "forecast.json").read_text())
        issued = datetime.fromisoformat(forecast["issued_at"])
        (series,) = forecast["series"]
        assert series["times"] == [(issued + timedelta(minutes=10 * k)).isoformat()
                                   for k in range(1, 7)]
        assert all(datetime.fromisoformat(p["effective_from"]) < issued + timedelta(hours=1)
                   for p in proposals if p["run_id"] == c["run_id"])
    last_cycle = view["history"][-1]["run_id"]
    approved = [p for p in proposals if p["state"] in ("APPROVED", "APPLIED")]
    assert all(p["decided_by"] == "alice" for p in approved)
    # Each approved proposal is applied by the next cycle; the last cycle's have no next one.
    assert all(p["state"] == "APPLIED" for p in approved if p["run_id"] != last_cycle)
    applied = [p for p in proposals if p["state"] == "APPLIED"]
    assert len(applied) == len(view["history"]) - 1 + 1  # one forecast's per cycle, the agent's
    (by_agent,) = [p for p in proposals if p["proposal_id"] == agent_proposal.proposal_id]
    assert (by_agent["state"], by_agent["proposed_by"]["kind"]) == ("APPLIED", "agent")
    applied_at = datetime.fromisoformat(by_agent["applied_at"])  # the first step from 01:30
    assert timedelta(0) <= applied_at - START.replace(hour=1, minute=30) < timedelta(seconds=30)
    for p in applied:
        record = [a for a in cycles[p["applied_run_id"]]["setpoints_applied"]
                  if a["proposal_id"] == p["proposal_id"]]
        assert len(record) == 1 and record[0]["value"] == p["value"]
        assert p["applied_at"] >= p["effective_from"]
        assert p["applied_run_id"] != p["run_id"]  # a later cycle than the one proposing it
    rejected = {p["proposal_id"] for p in proposals if p["state"] == "REJECTED"}
    assert rejected and not any(a["proposal_id"] in rejected for c in cycles.values()
                                for a in c["setpoints_applied"])
