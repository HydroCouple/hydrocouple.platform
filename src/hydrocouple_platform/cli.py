"""Command line: ``hcp migrate | serve | worker | agent | target register | bootstrap-org``."""

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
    agent = sub.add_parser("agent", help="run, verify or describe an execution agent")
    agent.add_argument("--config", default=os.environ.get("HCP_AGENT_CONFIG"),
                       help="agent.json; without it, the flags below configure a local pool")
    agent.add_argument("--kubeconfig", help="outside a cluster: the kubeconfig to use")
    agent.add_argument("--verify", action="store_true",
                       help="check the control plane, scheduler and store, then exit")
    agent.add_argument("--image", help="with --verify: also run this runner image (by digest)")
    agent.add_argument("--dry-run", action="store_true",
                       help="print every call this agent makes, without making any")
    agent.add_argument("--control-plane", default=os.environ.get("HCP_AGENT_CONTROL_PLANE"))
    agent.add_argument("--store", default=os.environ.get("HCP_AGENT_STORE_URI"))
    agent.add_argument("--work-root", default=os.environ.get("HCP_AGENT_WORK_ROOT"))
    agent.add_argument("--slots", type=int, default=int(os.environ.get("HCP_AGENT_SLOTS", "1")))
    agent.add_argument("--egress", default=os.environ.get("HCP_AGENT_EGRESS", "summaries"))
    target = sub.add_parser("target", help="onboard compute targets")
    target_sub = target.add_subparsers(dest="target_command", required=True)
    register = target_sub.add_parser(
        "register", help="register a target (signed in with HCP_TOKEN) and print its credential")
    register.add_argument("--control-plane", required=True)
    register.add_argument("--org", required=True, help="organization id")
    register.add_argument("--name", required=True)
    register.add_argument("--adapter", required=True, choices=["local-pool", "kubernetes"])
    register.add_argument("--storage-uri", required=True)
    register.add_argument("--egress", default="summaries",
                          choices=["metrics-only", "summaries", "full"])
    register.add_argument("--ownership", default="organization-owned",
                          choices=["organization-owned", "service-managed"])
    register.add_argument("--region")
    register.add_argument("--kubernetes-secret", metavar="NAMESPACE/NAME",
                          help="print the credential as a Kubernetes Secret manifest instead")
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
        return _agent(parser, args)
    elif args.command == "target":
        return _register_target(args)
    elif args.command == "bootstrap-org":
        org_id = _services().bootstrap_organization(args.name, args.issuer, args.subject,
                                                    args.email)
        print(org_id)
    return 0


def _agent(parser: argparse.ArgumentParser, args) -> int:
    import json

    from .agent.config import AgentConfig, build_agent
    from .agent.verify import dry_run, report, verify

    if args.config:
        cfg = AgentConfig.load(args.config)
    else:
        missing = [n for n, v in (("--control-plane", args.control_plane),
                                  ("--store", args.store), ("--work-root", args.work_root))
                   if not v]
        if missing:
            parser.error("missing " + ", ".join(missing) + " (or --config)")
        cfg = AgentConfig(control_plane=args.control_plane, store=args.store,
                          egress=args.egress, adapter="local-pool",
                          local_pool={"work_root": args.work_root, "slots": args.slots})
    if args.dry_run:
        print(json.dumps(dry_run(cfg), indent=2))
        return 0
    credential = os.environ.get("HCP_AGENT_CREDENTIAL")
    if not credential:
        parser.error("missing HCP_AGENT_CREDENTIAL")
    if args.verify:
        checks = verify(cfg, credential, args.image, args.kubeconfig)
        print(report(checks))
        return 0 if all(c.ok for c in checks) else 1
    execution_agent = build_agent(cfg, credential, args.kubeconfig)
    try:
        execution_agent.run_forever(_stop_event())
    finally:
        execution_agent.close()
    return 0


def _register_target(args) -> int:
    """Register a target as the signed-in person (HCP_TOKEN). The agent credential is shown
    once: as JSON, or as a Kubernetes Secret manifest to pipe into ``kubectl apply -f -``."""
    import json

    from .client import PlatformClient

    token = os.environ.get("HCP_TOKEN")
    if not token:
        print("set HCP_TOKEN to your sign-in token", file=sys.stderr)
        return 2
    with PlatformClient(args.control_plane, token) as client:
        target = client.register_target(
            args.org, name=args.name, ownership_mode=args.ownership, adapter=args.adapter,
            storage_uri=args.storage_uri, region=args.region, egress_policy=args.egress)
    print(f"registered target {target['target_id']}", file=sys.stderr)
    if args.kubernetes_secret:
        namespace, _, name = args.kubernetes_secret.partition("/")
        print(json.dumps({
            "apiVersion": "v1", "kind": "Secret", "type": "Opaque",
            "metadata": {"name": name or "hcp-agent-credential", "namespace": namespace,
                         "labels": {"hydrocouple.org/target": target["target_id"]}},
            "stringData": {"credential": target["agent_credential"]}}, indent=2))
    else:
        print(json.dumps({"target_id": target["target_id"],
                          "agent_credential": target["agent_credential"]}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
