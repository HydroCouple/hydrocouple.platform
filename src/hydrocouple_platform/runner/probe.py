"""``python -m hydrocouple_platform.runner.probe --key KEY --store URI``: write one object.

``hcp agent --verify --image ...`` runs this in a runner pod, built exactly as attempts'
pods are. It shows that the image starts under the site's pod rules and network policy,
and that the runner reaches the store with its own identity.
"""

from __future__ import annotations

import argparse
import json
import platform
import sys
import tempfile
from pathlib import Path

from .. import __version__
from ..storage import open_store


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="hydrocouple_platform.runner.probe")
    parser.add_argument("--key", required=True)
    parser.add_argument("--store", required=True)
    args = parser.parse_args(argv)
    try:
        from openswmm import __version__ as engine
    except ImportError:
        engine = None
    body = {"probe": args.key, "platform": __version__, "python": platform.python_version(),
            "engine": engine}
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "probe.json"
        path.write_text(json.dumps(body), encoding="utf-8")
        open_store(args.store).put_file(args.key, path, if_absent=True)
    print(json.dumps(body))
    return 0


if __name__ == "__main__":
    sys.exit(main())
