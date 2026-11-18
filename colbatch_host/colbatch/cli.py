"""colbatch command line interface.

Usage:
    python -m colbatch.cli run --config run.json
    python -m colbatch.cli preflight --config run.json

Exit codes:
    run        0 = completed, 3 = cancelled by signal, 1 = aborted/config/gate error
    preflight  0 = admitted, 2 = quality rules rejected, 3 = config error,
               4 = input data error, 5 = profiling/publish execution error
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys

# Allow both "python -m colbatch.cli" and direct execution of this file.
if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from colbatch.preflight import (
    EXIT_CONFIG_ERROR,
    EXIT_DATA_ERROR,
    EXIT_OK,
    EXIT_PROFILE_ERROR,
    EXIT_QUALITY_FAILED,
    load_gate_decision,
    run_preflight,
)
from colbatch.quality import QualityConfig, QualityConfigError

# Host/pyarrow are imported lazily inside the run path so that preflight
# processes do not pay (or count in RSS) the plugin framework footprint.


def _load_raw(config_path):
    with open(config_path, "r", encoding="utf-8") as fh:
        return json.load(fh), os.path.dirname(os.path.abspath(config_path))


def _build_quality(raw, base_dir):
    """Validate the quality section against the columns in the run config.

    The quality section may declare its own ``columns`` list (it additionally
    accepts ``float64``); otherwise it inherits the host's ``columns``, whose
    types are restricted to the plugin contract (utf8/int64).
    """
    host_columns = raw.get("columns") or []
    qraw = raw.get("quality") or {}
    q_columns = qraw.get("columns") if qraw.get("columns") else host_columns
    quality = QualityConfig(qraw, q_columns, base_dir)
    host_by_name = {c["name"]: c for c in host_columns}
    for col in quality.columns:
        if col["type"] not in ("utf8", "int64", "float64"):
            raise QualityConfigError(f"unsupported quality column type {col['type']!r}")
        if not col.get("field"):
            raise QualityConfigError("quality columns need a field")
        host_col = host_by_name.get(col["name"])
        if host_col is not None and host_col["field"] != col["field"]:
            raise QualityConfigError(
                f"quality column {col['name']!r} field {col['field']!r} disagrees "
                f"with host column field {host_col['field']!r}"
            )
    return quality


def _cmd_preflight(config_path: str) -> int:
    try:
        raw, base_dir = _load_raw(config_path)
        quality = _build_quality(raw, base_dir)
        input_rel = raw.get("input")
        if not input_rel:
            raise QualityConfigError("config needs input")
        source = input_rel if os.path.isabs(input_rel) else os.path.join(base_dir, input_rel)
        source = os.path.abspath(source)
        if not os.path.exists(source):
            raise QualityConfigError(f"input not found: {source}")
    except (OSError, json.JSONDecodeError) as exc:
        print(f"config error: cannot read config: {exc}", file=sys.stderr)
        return EXIT_CONFIG_ERROR
    except QualityConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return EXIT_CONFIG_ERROR

    # Cooperative cancellation: flip a flag checked at every scan/copy loop
    # so the finally-blocks remove every temp file before exit.  A second
    # signal falls back to the default disposition.
    cancel = {"flag": False}

    def _on_sigint(signum, frame):
        cancel["flag"] = True

    old_int = signal.signal(signal.SIGINT, _on_sigint)
    old_term = signal.signal(signal.SIGTERM, _on_sigint)
    try:
        outcome = run_preflight(quality, source, base_dir,
                                cancel_flag=lambda: cancel["flag"])
    finally:
        signal.signal(signal.SIGINT, old_int)
        signal.signal(signal.SIGTERM, old_term)
    if outcome.error is not None:
        print(
            json.dumps({"preflight": "error", **outcome.error}, ensure_ascii=False),
            file=sys.stderr,
        )
        if outcome.exit_code == EXIT_DATA_ERROR:
            print(
                "data error: input could not be profiled (this is not a quality failure)",
                file=sys.stderr,
            )
        else:
            print("preflight execution error; no report/snapshot pair was published",
                  file=sys.stderr)
        return outcome.exit_code

    with open(outcome.report_path, "r", encoding="utf-8") as fh:
        print(fh.read())
    if outcome.admitted:
        print(f"preflight: ADMITTED (report: {outcome.report_path})", file=sys.stderr)
        return EXIT_OK
    failed = [r["id"] for r in outcome.report["rules"] if not r["passed"]]
    print(
        f"preflight: REJECTED by quality rules: {', '.join(failed)} "
        f"(report: {outcome.report_path})",
        file=sys.stderr,
    )
    return EXIT_QUALITY_FAILED


def _cmd_run(config_path: str) -> int:
    # Lazy import keeps the preflight process free of the pyarrow dependency.
    from colbatch.host import Config, ConfigError, Host

    with open(config_path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    base_dir = os.path.dirname(os.path.abspath(config_path))
    try:
        cfg = Config(raw, base_dir=base_dir)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 1

    gate = None
    if (raw.get("quality") or {}).get("enabled"):
        try:
            quality = _build_quality(raw, base_dir)
            gate = load_gate_decision(quality)
        except QualityConfigError as exc:
            print(f"config error: {exc}", file=sys.stderr)
            return 1
        except Exception as exc:
            # Any gate failure: do not start workers, do not touch outputs.
            print(f"quality gate blocked the run: {exc}", file=sys.stderr)
            return 1
    return Host(cfg, gate=gate).run()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="colbatch")
    sub = parser.add_subparsers(dest="cmd", required=True)

    runp = sub.add_parser("run", help="run a cleaning job from a JSON config")
    runp.add_argument("--config", required=True, help="path to the run config JSON")

    pre = sub.add_parser(
        "preflight",
        help="snapshot and profile the raw input, evaluate quality rules, "
        "publish a verifiable admission report (no plugin workers, no output writes)",
    )
    pre.add_argument("--config", required=True, help="path to the run config JSON")

    args = parser.parse_args(argv)
    if args.cmd == "run":
        return _cmd_run(args.config)
    if args.cmd == "preflight":
        return _cmd_preflight(args.config)
    return 1


if __name__ == "__main__":
    sys.exit(main())
