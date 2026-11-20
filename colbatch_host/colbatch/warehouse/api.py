"""CLI-facing orchestration: validate -> stage -> apply -> receipt.

The receipt file (``config.receipt``) is written *after* the warehouse
transaction commits, via temp-file + ``os.replace``.  A process killed
between commit and receipt can therefore be recovered by re-running the
same command: the batch row committed with the revision is recognised and
the stored receipt is returned.  A process killed before the commit leaves
the previous head and no batch row, so the retry applies normally.

Crash/pause fault injection for the recovery matrix lives in
:mod:`colbatch.warehouse.apply` (``COLBATCH_WAREHOUSE_FAULT``) so both hooks
sit inside the same transaction boundary as the commit they bracket.
"""

from __future__ import annotations

import json
import os
import tempfile

from .apply import run_apply
from .config import (
    WarehouseConflictError,
    WarehouseConfig,
    WarehouseConfigError,
    WarehouseDataError,
    WarehouseExecError,
    canonical_json,
)
from .ingest import build_stage
from .schema import WarehouseDB

EXIT_OK = 0
EXIT_CONFIG = 3
EXIT_DATA = 4
EXIT_EXEC = 5
EXIT_CONFLICT = 6


def write_receipt(config: WarehouseConfig, receipt: dict):
    if not config.receipt_path:
        return
    directory = os.path.dirname(os.path.abspath(config.receipt_path))
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".receipt-", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(canonical_json(receipt))
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, config.receipt_path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


class ApplyResult:
    def __init__(self, exit_code, payload, error=None):
        self.exit_code = exit_code
        self.payload = payload
        self.error = error


def apply_batch(config: WarehouseConfig) -> ApplyResult:
    staged = None
    try:
        config.check_db_identity()
        staged = build_stage(config)
        outcome = run_apply(config, staged)
    except WarehouseDataError as exc:
        return ApplyResult(EXIT_DATA, {}, {
            "kind": "data_error", "reason": exc.reason, "message": exc.message})
    except WarehouseConflictError as exc:
        return ApplyResult(EXIT_CONFLICT, {}, {
            "kind": "conflict", "reason": exc.reason, "message": exc.message})
    except WarehouseExecError as exc:
        return ApplyResult(EXIT_EXEC, {}, {
            "kind": "exec_error", "reason": exc.reason, "message": exc.message})
    except WarehouseConfigError as exc:
        return ApplyResult(EXIT_CONFIG, {}, {
            "kind": "config_error", "reason": "config", "message": str(exc)})
    finally:
        if staged is not None:
            staged.close()

    payload = dict(outcome.payload)
    try:
        write_receipt(config, payload["receipt"])
    except OSError as exc:
        return ApplyResult(EXIT_EXEC, payload, {
            "kind": "exec_error", "reason": "receipt",
            "message": f"revision committed but receipt could not be written: {exc}"})
    return ApplyResult(EXIT_OK, payload, None)


def status_payload(db_path: str, revision=None, batch_id=None) -> dict:
    cfg = WarehouseConfig(minimal_config(db_path), base_dir=os.path.dirname(
        os.path.abspath(db_path)))
    if not cfg.db_exists():
        raise WarehouseExecError("missing_db", f"warehouse not found: {db_path}")
    db = WarehouseDB(cfg, readonly=True)
    try:
        head = db.meta_int("head_revision")
        payload = {"head_revision": head}
        rows = [dict(r) for r in db.conn.execute(
            "SELECT revision, parent_revision, batch_id, created_at, facts_count, "
            "daily_count, exclusions_count FROM revisions ORDER BY revision")]
        payload["revisions"] = rows
        if batch_id is not None:
            row = db.conn.execute(
                "SELECT batch_id, expected_parent_version, revision, applied_at, "
                "input_fingerprint, receipt_json FROM batches WHERE batch_id=?",
                (batch_id,)).fetchone()
            if row is None:
                payload["batch"] = None
            else:
                payload["batch"] = {
                    "batch_id": row["batch_id"],
                    "expected_parent_version": row["expected_parent_version"],
                    "revision": row["revision"],
                    "applied_at": row["applied_at"],
                    "input_fingerprint": row["input_fingerprint"],
                    "receipt": json.loads(row["receipt_json"]),
                }
        elif revision is not None:
            row = db.conn.execute(
                "SELECT * FROM revisions WHERE revision=?", (int(revision),)
            ).fetchone()
            payload["revision"] = dict(row) if row else None
        return payload
    finally:
        db.close()


def minimal_config(db_path: str) -> dict:
    return {"db": db_path, "_mode": "read"}


__all__ = [
    "ApplyResult",
    "EXIT_CONFLICT",
    "EXIT_CONFIG",
    "EXIT_DATA",
    "EXIT_EXEC",
    "EXIT_OK",
    "apply_batch",
    "status_payload",
]
