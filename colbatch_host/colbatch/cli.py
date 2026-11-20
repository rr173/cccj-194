"""colbatch command line interface.

Usage:
    python -m colbatch.cli run --config run.json
    python -m colbatch.cli preflight --config run.json
    python -m colbatch.cli compare --config compare.json
    python -m colbatch.cli compare-status --config compare.json
    python -m colbatch.cli warehouse-apply --config batch.json
    python -m colbatch.cli warehouse-export --db wh.sqlite --revision latest
    python -m colbatch.cli warehouse-status --db wh.sqlite

Exit codes:
    run        0 = completed, 3 = cancelled by signal, 1 = aborted/config/gate error
    preflight  0 = admitted, 2 = quality rules rejected, 3 = config error,
               4 = input data error, 5 = profiling/publish execution error
    compare    0 = reconciled and report published (changes are data, not an
               error), 3 = config error, 4 = runs cannot be reconciled
               (missing/inconsistent/corrupt inputs), 5 = comparator
               execution error (I/O, input mutated mid-compare, cancel)
    compare-status
               0 = a complete verified report exists, 3 = config error,
               4 = no valid report (never compared, corrupt, or detail
               digest mismatch)
    warehouse-apply
               0 = revision published (or the identical batch returned its
               original revision), 3 = config error, 4 = batch rejected as a
               whole, 5 = execution error (nothing published), 6 = conflict
               (batch id reuse with different content, stale parent)
    warehouse-export
               0 = exported and verified, 3 = config error, 5 = execution
               error (missing db / unknown revision / inconsistent state)
    warehouse-status
               0 = printed revision list, 3 = config error, 5 = missing db
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

from colbatch.compare import (
    CompareConfig,
    CompareConfigError,
    query_status,
    run_compare,
)
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


def _load_compare_config(config_path: str):
    """Parse the compare config file; raises CompareConfigError."""
    try:
        with open(config_path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        raise CompareConfigError(f"cannot read config: {exc}") from exc
    base_dir = os.path.dirname(os.path.abspath(config_path))
    return CompareConfig(raw, base_dir)


def _install_cancel_flag():
    """Cooperative cancellation: first signal sets a flag checked inside the
    merge loop so finally-blocks remove every staged artifact before exit."""
    cancel = {"flag": False}

    def _on_signal(signum, frame):
        cancel["flag"] = True

    old_int = signal.signal(signal.SIGINT, _on_signal)
    old_term = signal.signal(signal.SIGTERM, _on_signal)
    return cancel, old_int, old_term


def _cmd_compare(config_path: str) -> int:
    try:
        config = _load_compare_config(config_path)
    except CompareConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return EXIT_CONFIG_ERROR

    cancel, old_int, old_term = _install_cancel_flag()
    try:
        outcome = run_compare(config, cancel_flag=lambda: cancel["flag"])
    finally:
        signal.signal(signal.SIGINT, old_int)
        signal.signal(signal.SIGTERM, old_term)

    if outcome.error is not None:
        print(
            json.dumps({"compare": "error", **outcome.error}, ensure_ascii=False),
            file=sys.stderr,
        )
        if outcome.error.get("kind") == "data_error":
            print(
                "compare: the two runs cannot be reconciled; no report was "
                "published and any previous report is untouched",
                file=sys.stderr,
            )
        else:
            print(
                "compare: execution error; no report was published and any "
                "previous report is untouched",
                file=sys.stderr,
            )
        return outcome.exit_code

    with open(outcome.report_path, "r", encoding="utf-8") as fh:
        print(fh.read())
    summary = outcome.report["summary"]
    print(
        f"compare: OK — {summary['rows_compared']} rows compared, "
        f"{summary['changed_rows']} changed "
        f"(fields={summary['success_fields_changed']}, "
        f"s→f={summary['success_to_failure']}, "
        f"f→s={summary['failure_to_success']}, "
        f"reason={summary['failure_reason_changed']}); "
        f"report: {outcome.report_path}",
        file=sys.stderr,
    )
    return 0


def _cmd_compare_status(config_path: str) -> int:
    try:
        config = _load_compare_config(config_path)
    except CompareConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return EXIT_CONFIG_ERROR
    outcome = query_status(config)
    if outcome.error is not None:
        print(
            json.dumps({"compare_status": "invalid", **outcome.error},
                       ensure_ascii=False),
            file=sys.stderr,
        )
        return outcome.exit_code
    with open(config.report_path, "r", encoding="utf-8") as fh:
        print(fh.read())
    summary = outcome.report["summary"]
    print(
        f"compare-status: report valid — {summary['rows_compared']} rows, "
        f"{summary['changed_rows']} changed",
        file=sys.stderr,
    )
    return 0


def _cmd_warehouse_apply(config_path: str) -> int:
    from colbatch.warehouse.api import apply_batch
    from colbatch.warehouse.config import WarehouseConfig, WarehouseConfigError

    try:
        with open(config_path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        base_dir = os.path.dirname(os.path.abspath(config_path))
        config = WarehouseConfig(raw, base_dir=base_dir)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"config error: cannot read config: {exc}", file=sys.stderr)
        return 3
    except WarehouseConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 3

    result = apply_batch(config)
    if result.error is not None:
        print(json.dumps({"warehouse_apply": "error", **result.error},
                         ensure_ascii=False), file=sys.stderr)
        return result.exit_code
    receipt = result.payload.get("receipt", result.payload)
    summary = {
        "status": result.payload.get("status"),
        "batch_id": receipt.get("batch_id", config.batch_id),
        "revision": result.payload.get("revision"),
        "parent_revision": receipt.get("parent_revision"),
        "head_revision": result.payload.get("head_revision"),
        "metrics": receipt.get("metrics"),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=1, sort_keys=True))
    return 0


def _cmd_warehouse_export(args) -> int:
    from colbatch.warehouse.config import WarehouseConfig, WarehouseConfigError
    from colbatch.warehouse.query import export_revision

    db_path = os.path.abspath(args.db)
    try:
        config = WarehouseConfig(
            {"db": db_path, "_mode": "read"},
            base_dir=os.path.dirname(db_path),
        )
        outcome = export_revision(
            config, args.revision, fmt=args.format, output_path=args.out,
            stream=None if args.out else sys.stdout, verify=not args.no_verify)
    except WarehouseConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 3
    except Exception as exc:  # noqa: BLE001 - mapped to a documented exit code
        reason = getattr(exc, "reason", "exec_error")
        message = getattr(exc, "message", str(exc))
        print(json.dumps({"warehouse_export": "error", "reason": reason,
                          "message": message}, ensure_ascii=False),
              file=sys.stderr)
        return 5
    print(json.dumps({"warehouse_export": "ok", **outcome.payload},
                     ensure_ascii=False), file=sys.stderr)
    return 0


def _cmd_warehouse_status(args) -> int:
    from colbatch.warehouse.api import status_payload
    try:
        payload = status_payload(os.path.abspath(args.db),
                                 revision=args.revision,
                                 batch_id=args.batch_id)
    except Exception as exc:  # noqa: BLE001
        reason = getattr(exc, "reason", "exec_error")
        message = getattr(exc, "message", str(exc))
        print(json.dumps({"warehouse_status": "error", "reason": reason,
                          "message": message}, ensure_ascii=False),
              file=sys.stderr)
        return 5
    print(json.dumps(payload, ensure_ascii=False, indent=1, sort_keys=True))
    return 0


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

    cmp_p = sub.add_parser(
        "compare",
        help="reconcile two completed runs of the same quality snapshot "
        "row by row and publish an auditable change set",
    )
    cmp_p.add_argument("--config", required=True, help="path to the compare config JSON")

    stat_p = sub.add_parser(
        "compare-status",
        help="read-only check of the published comparison report and its "
        "detail artifact",
    )
    stat_p.add_argument("--config", required=True, help="path to the compare config JSON")

    wa = sub.add_parser(
        "warehouse-apply",
        help="ingest one batch (cleaning run and/or cancellations/assignment "
        "changes) into the shipment-attribution warehouse as a new revision",
    )
    wa.add_argument("--config", required=True, help="path to the batch config JSON")

    we = sub.add_parser(
        "warehouse-export",
        help="export one historical revision (or latest) as a verified stream "
        "or file group",
    )
    we.add_argument("--db", required=True, help="warehouse SQLite database")
    we.add_argument("--revision", default="latest",
                    help="revision number or 'latest' (default)")
    we.add_argument("--out", default=None,
                    help="write to this JSONL file; default is stdout")
    we.add_argument("--format", choices=["jsonl", "files"], default="jsonl",
                    help="single typed stream (default) or an equivalent file group")
    we.add_argument("--no-verify", action="store_true",
                    help="skip the revision fingerprint check")

    ws = sub.add_parser(
        "warehouse-status",
        help="print the revision list (and optionally one batch's receipt)",
    )
    ws.add_argument("--db", required=True, help="warehouse SQLite database")
    ws.add_argument("--revision", default=None, help="show one revision's header")
    ws.add_argument("--batch-id", default=None, help="show one batch's stored receipt")

    args = parser.parse_args(argv)
    if args.cmd == "run":
        return _cmd_run(args.config)
    if args.cmd == "preflight":
        return _cmd_preflight(args.config)
    if args.cmd == "compare":
        return _cmd_compare(args.config)
    if args.cmd == "compare-status":
        return _cmd_compare_status(args.config)
    if args.cmd == "warehouse-apply":
        return _cmd_warehouse_apply(args.config)
    if args.cmd == "warehouse-export":
        return _cmd_warehouse_export(args)
    if args.cmd == "warehouse-status":
        return _cmd_warehouse_status(args)
    return 1


if __name__ == "__main__":
    sys.exit(main())
