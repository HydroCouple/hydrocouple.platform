"""Command line: ``hcp migrate | serve | worker | agent | bootstrap-org``."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading


def _settings():
    from .config import ControlPlaneSettings

    return ControlPlaneSettings()  # type: ignore[call-arg]


def _services():
    from .db import make_engine, make_sessionmaker
    from .services import PlatformServices

    settings = _settings()
    return PlatformServices(make_sessionmaker(make_engine(settings.database_url)), settings)


def _stop_event() -> threading.Event:
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    return stop


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="hcp", description="HydroCouple execution platform")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("migrate", help="apply database migrations")
    serve = sub.add_parser("serve", help="run the HTTP API and hosted interfaces")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    worker = sub.add_parser("worker", help="run the dispatcher and reconciler")
    worker.add_argument("--interval", type=float, default=1.0)
    agent = sub.add_parser("agent", help="run an execution agent (local pool)")
    agent.add_argument("--control-plane", default=os.environ.get("HCP_AGENT_CONTROL_PLANE"))
    agent.add_argument("--store", default=os.environ.get("HCP_AGENT_STORE_URI"))
    agent.add_argument("--work-root", default=os.environ.get("HCP_AGENT_WORK_ROOT"))
    agent.add_argument("--slots", type=int, default=int(os.environ.get("HCP_AGENT_SLOTS", "1")))
    agent.add_argument("--egress", default=os.environ.get("HCP_AGENT_EGRESS", "summaries"))
    boot = sub.add_parser("bootstrap-org", help="create an organization and its first admin")
    boot.add_argument("name")
    boot.add_argument("--issuer", required=True)
    boot.add_argument("--subject", required=True)
    boot.add_argument("--email")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    if args.command == "migrate":
        from .db import upgrade

        upgrade(_settings().database_url)
    elif args.command == "serve":
        import uvicorn

        from .api import create_app

        uvicorn.run(create_app(_settings()), host=args.host, port=args.port)
    elif args.command == "worker":
        services, stop = _services(), _stop_event()
        while not stop.is_set():
            services.dispatch()
            services.reconcile()
            stop.wait(args.interval)
    elif args.command == "agent":
        from .agent import Agent, AgentClient
        from .agent.adapters.local import LocalPoolAdapter
        from .contracts.jobs import EgressPolicy
        from .storage import open_store

        credential = os.environ.get("HCP_AGENT_CREDENTIAL")
        missing = [n for n, v in (("--control-plane", args.control_plane),
                                  ("--store", args.store), ("--work-root", args.work_root),
                                  ("HCP_AGENT_CREDENTIAL", credential)) if not v]
        if missing:
            parser.error("missing " + ", ".join(missing))
        execution_agent = Agent(AgentClient(args.control_plane, credential),
                                LocalPoolAdapter(args.work_root, slots=args.slots),
                                open_store(args.store), args.store, EgressPolicy(args.egress))
        try:
            execution_agent.run_forever(_stop_event())
        finally:
            execution_agent.close()
    elif args.command == "bootstrap-org":
        org_id = _services().bootstrap_organization(args.name, args.issuer, args.subject,
                                                    args.email)
        print(org_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
