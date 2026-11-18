"""colbatch command line interface.

Usage:
    python -m colbatch.cli run --config run.json

Exit codes: 0 = completed, 3 = cancelled by signal, 1 = aborted / config error.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

# Allow both "python -m colbatch.cli" and direct execution of this file.
if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from colbatch.host import Config, ConfigError, Host


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="colbatch")
    sub = parser.add_subparsers(dest="cmd", required=True)
    runp = sub.add_parser("run", help="run a cleaning job from a JSON config")
    runp.add_argument("--config", required=True, help="path to the run config JSON")
    args = parser.parse_args(argv)

    if args.cmd == "run":
        with open(args.config, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        base_dir = os.path.dirname(os.path.abspath(args.config))
        try:
            cfg = Config(raw, base_dir=base_dir)
        except ConfigError as exc:
            print(f"config error: {exc}", file=sys.stderr)
            return 1
        return Host(cfg).run()
    return 1


if __name__ == "__main__":
    sys.exit(main())
