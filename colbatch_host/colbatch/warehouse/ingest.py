"""Read, verify and stage the inputs of one warehouse batch.

Nothing in this module writes to the published warehouse.  It performs

  1. strict verification of the cleaning run (completion status, manifest
     digests/counts, exact source-row coverage of ``[0, input_rows_read)``
     across the success/failure streams, no cross-stream collisions);
  2. per-row normalisation into a disposable on-disk stage database;
  3. a content fingerprint derived from the exact bytes consumed (never from
     paths), used to recognise a retry of the same batch.

Anything structurally wrong raises :class:`WarehouseDataError` and the whole
batch is rejected; nothing has been written to the warehouse at that point.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone

from .config import (
    Ink,
    WarehouseConfig,
    WarehouseDataError,
    WarehouseExecError,
    canonical_json,
    sha256_text,
)

_HEX64 = re.compile(r"[0-9a-f]{64}")
_AMOUNT = re.compile(r"-?\d+(?:\.\d{1,2})?")
_TS = "%Y-%m-%dT%H:%M:%SZ"


# ---------------------------------------------------------------------------
# scalar parsers (also used by the assignment/cancellation files)


def reject_constant(value: str):
    raise ValueError(f"non-standard JSON constant {value!r}")


def parse_json(raw: bytes, where: str):
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise WarehouseDataError("bad_encoding", f"{where}: invalid UTF-8: {exc}") from exc
    try:
        return json.loads(text, parse_constant=reject_constant)
    except (json.JSONDecodeError, ValueError) as exc:
        raise WarehouseDataError("bad_json", f"{where}: {exc}") from exc


def parse_utc_epoch(value) -> int | None:
    """Strict ``YYYY-MM-DDTHH:MM:SSZ`` -> UTC epoch seconds, else None."""
    if not isinstance(value, str) or len(value) != 20:
        return None
    try:
        dt = datetime.strptime(value, _TS)
    except ValueError:
        return None
    if dt.strftime(_TS) != value:
        return None
    return int(dt.replace(tzinfo=timezone.utc).timestamp())


def time_to_epoch(value, where: str) -> tuple[int, str]:
    if not isinstance(value, str) or not value:
        raise WarehouseDataError("bad_time", f"{where}: time must be a non-empty string")
    epoch = parse_utc_epoch(value)
    if epoch is None:
        raise WarehouseDataError(
            "bad_time", f"{where}: {value!r} is not a UTC 'YYYY-MM-DDTHH:MM:SSZ' time"
        )
    return epoch, value


def parse_cents(value) -> int | None:
    """Exact decimal string -> integer cents (negative allowed)."""
    if not isinstance(value, str):
        return None
    if not _AMOUNT.fullmatch(value):
        return None
    negative = value.startswith("-")
    body = value[1:] if negative else value
    whole, _, frac = body.partition(".")
    frac = (frac + "00")[:2]
    try:
        cents = int(whole) * 100 + int(frac)
    except ValueError:
        return None
    if negative:
        cents = -cents
    if cents < -(2**63) or cents > 2**63 - 1:
        return None
    return cents


def is_nonempty_str(value) -> bool:
    return isinstance(value, str) and value.strip() != ""


def _is_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_sha(value) -> bool:
    return isinstance(value, str) and bool(_HEX64.fullmatch(value))


class Digest:
    __slots__ = ("sha256", "size")

    def __init__(self, sha256: str, size: int):
        self.sha256 = sha256
        self.size = size


def digest_bytes(raw: bytes) -> Digest:
    return Digest(hashlib.sha256(raw).hexdigest(), len(raw))


# ---------------------------------------------------------------------------
# cleaning manifest verification


@dataclass
class CleaningRun:
    manifest_path: str
    manifest_sha256: str
    success_path: str
    failure_path: str
    success_sha256: str
    success_rows: int
    failure_sha256: str
    failure_rows: int
    input_rows_read: int
    plugin_name: str
    plugin_version: str
    run_id: str

    def run_dict(self):
        return {
            "run_id": self.run_id,
            "manifest_sha256": self.manifest_sha256,
            "plugin_name": self.plugin_name,
            "plugin_version": self.plugin_version,
            "input_rows_read": self.input_rows_read,
            "success_sha256": self.success_sha256,
            "success_rows": self.success_rows,
            "failure_sha256": self.failure_sha256,
            "failure_rows": self.failure_rows,
        }


def _manifest_int(manifest, key, label):
    val = manifest.get(key)
    if not _is_int(val) or val < 0:
        raise WarehouseDataError(
            "manifest_invalid", f"manifest {label} must be a non-negative integer"
        )
    return val


def _manifest_stream(manifest, key):
    outputs = manifest.get("outputs")
    if not isinstance(outputs, dict) or not isinstance(outputs.get(key), dict):
        raise WarehouseDataError(
            "manifest_invalid", f"manifest outputs.{key} is missing"
        )
    entry = outputs[key]
    if not _is_sha(entry.get("sha256")):
        raise WarehouseDataError(
            "manifest_invalid", f"manifest outputs.{key}.sha256 is not a digest"
        )
    rows = entry.get("rows")
    if not _is_int(rows) or rows < 0:
        raise WarehouseDataError(
            "manifest_invalid", f"manifest outputs.{key}.rows must be a non-negative integer"
        )
    return entry["sha256"], rows, entry.get("path")


def load_cleaning_run(config: WarehouseConfig) -> CleaningRun:
    path = config.cleaning_manifest
    try:
        size = os.path.getsize(path)
    except OSError as exc:
        raise WarehouseExecError("input_missing", f"cannot stat manifest: {exc}") from exc
    limit = config.limits["max_manifest_bytes"]
    if size > limit:
        raise WarehouseDataError(
            "manifest_too_large",
            f"manifest is {size} bytes > max_manifest_bytes={limit}",
        )
    try:
        with open(path, "rb") as fh:
            raw = fh.read()
    except OSError as exc:
        raise WarehouseExecError("input_unreadable", f"cannot read manifest: {exc}") from exc
    m_digest = digest_bytes(raw)
    manifest = parse_json(raw, "cleaning manifest")
    if not isinstance(manifest, dict):
        raise WarehouseDataError("manifest_invalid", "manifest must be a JSON object")
    if manifest.get("status") != "completed":
        raise WarehouseDataError(
            "run_not_completed",
            f"cleaning run status is {manifest.get('status')!r}, expected 'completed'",
        )
    if manifest.get("abort_reason") is not None:
        raise WarehouseDataError(
            "run_aborted", f"cleaning run recorded abort_reason={manifest.get('abort_reason')!r}"
        )
    plugin = manifest.get("plugin")
    if not isinstance(plugin, dict) or not isinstance(plugin.get("name"), str):
        raise WarehouseDataError("manifest_invalid", "manifest plugin identity is missing")

    input_rows = _manifest_int(manifest, "input_rows_read", "input_rows_read")
    batches_formed = _manifest_int(manifest, "batches_formed", "batches_formed")
    delivered_batches = _manifest_int(manifest, "delivered_batches", "delivered_batches")
    if delivered_batches != batches_formed:
        raise WarehouseDataError(
            "run_partial",
            f"manifest delivered {delivered_batches} of {batches_formed} batches",
        )
    s_sha, s_rows, s_path = _manifest_stream(manifest, "success")
    f_sha, f_rows, f_path = _manifest_stream(manifest, "failure")
    if manifest.get("delivered_rows_success") != s_rows:
        raise WarehouseDataError(
            "manifest_invalid", "delivered_rows_success disagrees with outputs.success.rows"
        )
    if manifest.get("delivered_rows_failure") != f_rows:
        raise WarehouseDataError(
            "manifest_invalid", "delivered_rows_failure disagrees with outputs.failure.rows"
        )
    if s_rows + f_rows != input_rows:
        raise WarehouseDataError(
            "row_count_mismatch",
            f"success+failure rows {s_rows}+{f_rows} != input_rows_read {input_rows}",
        )
    last = manifest.get("last_delivered_row_id")
    expected_last = input_rows - 1 if input_rows else -1
    if last != expected_last:
        raise WarehouseDataError(
            "row_coverage",
            f"last_delivered_row_id={last!r} but input_rows_read={input_rows}",
        )

    manifest_dir = os.path.dirname(os.path.abspath(path))
    success_path = config.cleaning_success
    if success_path is None:
        if not s_path:
            raise WarehouseDataError(
                "manifest_invalid", "cleaning_run.success missing and manifest has no path")
        success_path = s_path if os.path.isabs(s_path) else os.path.join(manifest_dir, s_path)
        success_path = os.path.abspath(success_path)
    failure_path = config.cleaning_failure
    if failure_path is None:
        if not f_path:
            raise WarehouseDataError(
                "manifest_invalid", "cleaning_run.failure missing and manifest has no path")
        failure_path = f_path if os.path.isabs(f_path) else os.path.join(manifest_dir, f_path)
        failure_path = os.path.abspath(failure_path)
    for label, resolved in (("success", success_path), ("failure", failure_path)):
        if not os.path.exists(resolved):
            raise WarehouseExecError("input_missing", f"{label} stream not found: {resolved}")

    return CleaningRun(
        manifest_path=path,
        manifest_sha256=m_digest.sha256,
        success_path=success_path,
        failure_path=failure_path,
        success_sha256=s_sha,
        success_rows=s_rows,
        failure_sha256=f_sha,
        failure_rows=f_rows,
        input_rows_read=input_rows,
        plugin_name=plugin.get("name"),
        plugin_version=str(plugin.get("version", "")),
        run_id=sha256_text(canonical_json({
            "manifest": m_digest.sha256,
            "success": {"sha256": s_sha, "rows": s_rows},
            "failure": {"sha256": f_sha, "rows": f_rows},
        })),
    )


# ---------------------------------------------------------------------------
# on-disk staging


STAGE_DDL = [
    """CREATE TABLE IF NOT EXISTS records (
        shipment_id TEXT PRIMARY KEY,
        station_id TEXT NOT NULL,
        norm_ts TEXT NOT NULL,
        ts_epoch INTEGER NOT NULL,
        utc_date TEXT NOT NULL,
        amount_cents INTEGER NOT NULL,
        run_id TEXT NOT NULL,
        manifest_sha256 TEXT NOT NULL,
        success_sha256 TEXT NOT NULL,
        source_row_id INTEGER NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS cancellations (
        shipment_id TEXT PRIMARY KEY,
        row_no INTEGER NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS interval_changes (
        station_id TEXT NOT NULL,
        assignment_id TEXT NOT NULL,
        op TEXT NOT NULL,
        from_ts TEXT,
        from_epoch INTEGER,
        to_ts TEXT,
        to_epoch INTEGER,
        region TEXT,
        row_no INTEGER NOT NULL,
        PRIMARY KEY (station_id, assignment_id)
    )""",
    """CREATE TABLE IF NOT EXISTS exclusions (
        seq INTEGER PRIMARY KEY,
        run_id TEXT NOT NULL,
        manifest_sha256 TEXT NOT NULL,
        stream TEXT NOT NULL,
        row_id INTEGER NOT NULL,
        shipment_id TEXT,
        station_id TEXT,
        reason TEXT NOT NULL,
        detail TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS daily_delta (
        utc_date TEXT NOT NULL,
        region_key TEXT NOT NULL,
        region TEXT,
        shipments INTEGER NOT NULL,
        amount_cents INTEGER NOT NULL,
        PRIMARY KEY (utc_date, region_key)
    )""",
]


class Stage:
    """Disposable workspace scratch database (bounded RAM, disk-backed)."""

    def __init__(self, work_dir: str):
        self.dir = work_dir
        self.path = os.path.join(work_dir, "stage.sqlite")
        self.conn = sqlite3.connect(self.path, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        c = self.conn
        c.execute("PRAGMA journal_mode=OFF")
        c.execute("PRAGMA synchronous=OFF")
        c.execute("PRAGMA temp_store=FILE")
        c.execute("PRAGMA cache_size=-8192")
        c.execute("BEGIN")
        for stmt in STAGE_DDL:
            c.execute(stmt)
        self._dirty = True

    def finish_writes(self):
        if self._dirty:
            self.conn.execute("COMMIT")
            self._dirty = False

    def close(self):
        try:
            if self._dirty:
                self.conn.execute("ROLLBACK")
                self._dirty = False
        except sqlite3.Error:
            pass
        try:
            self.conn.close()
        except sqlite3.Error:
            pass
        shutil.rmtree(self.dir, ignore_errors=True)

    def add_exclusion(self, seq, run_id, manifest_sha, stream, row_id,
                      shipment_id, station_id, reason, detail):
        self.conn.execute(
            "INSERT INTO exclusions(seq, run_id, manifest_sha256, stream, row_id, "
            "shipment_id, station_id, reason, detail) VALUES(?,?,?,?,?,?,?,?,?)",
            (seq, run_id, manifest_sha, stream, row_id, shipment_id, station_id,
             reason, detail),
        )


# ---------------------------------------------------------------------------
# stream ingestion


def _extract_source_ids(rec, config):
    """Best-effort (shipment, station) from a source object for exclusions."""
    source = rec.get("source") if isinstance(rec, dict) else None
    sid = station = None
    if isinstance(source, dict):
        s = source.get(config.shipment_field)
        t = source.get(config.station_field)
        if isinstance(s, str):
            sid = s
        if isinstance(t, str):
            station = t
    return sid, station


def _stage_success_record(rec, row_id, run, stage, config):
    where = f"success stream row_id={row_id}"
    source = rec.get("source")
    output = rec.get("output")
    if not isinstance(source, dict) or not isinstance(output, dict):
        raise WarehouseDataError(
            "structure", f"{where}: source/output must be objects")
    is_valid = output.get("is_valid")
    if not isinstance(is_valid, bool):
        raise WarehouseDataError(
            "structure", f"{where}: output.is_valid must be a boolean")
    reason = output.get("invalid_reason")
    if reason is not None and not isinstance(reason, str):
        raise WarehouseDataError(
            "structure", f"{where}: output.invalid_reason must be a string or null")

    raw_shipment = source.get(config.shipment_field)
    raw_station = source.get(config.station_field)
    if raw_shipment is not None and not isinstance(raw_shipment, str):
        raise WarehouseDataError(
            "structure",
            f"{where}: {config.shipment_field} must be a string or null",
        )
    if raw_station is not None and not isinstance(raw_station, str):
        raise WarehouseDataError(
            "structure",
            f"{where}: {config.station_field} must be a string or null",
        )

    if not is_valid:
        stage.add_exclusion(
            row_id, run.run_id, run.manifest_sha256, "success", row_id,
            raw_shipment, raw_station, "invalid_row",
            reason or "cleaning marked the row invalid",
        )
        return

    # Qualifying row: missing (null/absent/blank) required values are
    # exclusions that leave the shipment's previous state untouched; a
    # non-null value of the wrong type/shape rejects the whole batch.
    missing = []
    for label, raw in (
        (config.shipment_field, raw_shipment),
        (config.station_field, raw_station),
    ):
        if raw is None or raw.strip() == "":
            missing.append(label)
    norm_ts = output.get("norm_ts")
    ts_epoch = None
    if not isinstance(norm_ts, str) or norm_ts == "":
        if norm_ts is not None and not isinstance(norm_ts, str):
            raise WarehouseDataError(
                "structure", f"{where}: output.norm_ts must be a string or null")
        missing.append("norm_ts")
    else:
        ts_epoch = parse_utc_epoch(norm_ts)
        if ts_epoch is None:
            raise WarehouseDataError(
                "bad_time",
                f"{where}: norm_ts {norm_ts!r} is not a UTC 'YYYY-MM-DDTHH:MM:SSZ' time",
            )
    norm_amount = output.get("norm_amount")
    amount_cents = None
    if not isinstance(norm_amount, str) or norm_amount == "":
        if norm_amount is not None and not isinstance(norm_amount, str):
            raise WarehouseDataError(
                "structure", f"{where}: output.norm_amount must be a string or null")
        missing.append("norm_amount")
    else:
        amount_cents = parse_cents(norm_amount)
        if amount_cents is None:
            raise WarehouseDataError(
                "bad_amount",
                f"{where}: norm_amount {norm_amount!r} is not a legal decimal string",
            )

    if missing:
        stage.add_exclusion(
            row_id, run.run_id, run.manifest_sha256, "success", row_id,
            raw_shipment, raw_station, "missing_required",
            "missing " + ",".join(missing),
        )
        return

    try:
        stage.conn.execute(
            "INSERT INTO records(shipment_id, station_id, norm_ts, ts_epoch, "
            "utc_date, amount_cents, run_id, manifest_sha256, success_sha256, "
            "source_row_id) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                raw_shipment, raw_station, norm_ts, ts_epoch, norm_ts[:10],
                amount_cents, run.run_id, run.manifest_sha256,
                run.success_sha256, row_id,
            ),
        )
    except sqlite3.IntegrityError:
        raise WarehouseDataError(
            "duplicate_shipment",
            f"{where}: shipment {raw_shipment!r} appears more than once among "
            f"qualifying records in this batch",
        ) from None


def _stage_failure_record(rec, row_id, run, stage, config):
    where = f"failure stream row_id={row_id}"
    sid, station = _extract_source_ids(rec, config)
    error = rec.get("error")
    cause = detail = None
    if isinstance(error, dict):
        cause = error.get("cause")
        detail = error.get("detail")
    elif isinstance(error, str):
        detail = error
    reason_text = cause if isinstance(cause, str) and cause else "cleaning_failure"
    detail_text = detail if isinstance(detail, str) else None
    if detail_text is not None and len(detail_text) > 500:
        detail_text = detail_text[:500]
    stage.add_exclusion(
        row_id, run.run_id, run.manifest_sha256, "failure", row_id,
        sid, station, reason_text, detail_text,
    )


def _stream_records(ink: Ink, stream: str):
    """Yield ``(row_id, rec)``; row_id must be a strictly increasing integer."""
    prev = -1
    for line_no, raw in ink:
        if not raw.strip():
            raise WarehouseDataError(
                "structure", f"{stream} line {line_no}: empty record")
        rec = parse_json(raw, f"{stream} line {line_no}")
        if not isinstance(rec, dict):
            raise WarehouseDataError(
                "structure", f"{stream} line {line_no}: record is not an object")
        rid = rec.get("row_id")
        if not _is_int(rid) or rid < 0:
            raise WarehouseDataError(
                "structure",
                f"{stream} line {line_no}: row_id must be a non-negative integer",
            )
        if rid <= prev:
            raise WarehouseDataError(
                "row_order",
                f"{stream} line {line_no}: row_id {rid} is not strictly increasing "
                f"(previous {prev})",
            )
        prev = rid
        yield rid, rec


def _iter_coverage(success_ink, failure_ink, n_rows):
    """Yield ``(row_id, stream, record)`` for ids 0..n_rows-1 exactly once.

    Both streams are consumed as sorted merges; a missing id, a duplicate or
    an out-of-order id, a cross-stream collision or any leftover row beyond
    ``n_rows`` is a batch error.  Memory use is one buffered record per
    stream.
    """
    s_it = iter(_stream_records(success_ink, "success"))
    f_it = iter(_stream_records(failure_ink, "failure"))
    s_cur = next(s_it, None)
    f_cur = next(f_it, None)
    for expected in range(n_rows):
        s_hit = s_cur is not None and s_cur[0] == expected
        f_hit = f_cur is not None and f_cur[0] == expected
        if s_hit and f_hit:
            raise WarehouseDataError(
                "cross_stream_collision",
                f"row_id {expected} appears in both success and failure streams",
            )
        if s_hit:
            yield expected, "success", s_cur[1]
            s_cur = next(s_it, None)
            continue
        if f_hit:
            yield expected, "failure", f_cur[1]
            f_cur = next(f_it, None)
            continue
        s_id = s_cur[0] if s_cur is not None else None
        f_id = f_cur[0] if f_cur is not None else None
        raise WarehouseDataError(
            "row_coverage",
            f"row_id {expected} is not delivered exactly once "
            f"(success next={s_id}, failure next={f_id})",
        )
    if s_cur is not None:
        raise WarehouseDataError(
            "row_coverage",
            f"success stream has an extra row_id {s_cur[0]} beyond {n_rows} rows",
        )
    if f_cur is not None:
        raise WarehouseDataError(
            "row_coverage",
            f"failure stream has an extra row_id {f_cur[0]} beyond {n_rows} rows",
        )


def stage_cleaning_run(run: CleaningRun, stage: Stage, config: WarehouseConfig):
    """Verify and stage the two cleaning streams.  Raises WarehouseDataError."""
    success_ink = Ink(run.success_path, config.limits["max_line_bytes"],
                      config.limits["read_block_bytes"])
    failure_ink = Ink(run.failure_path, config.limits["max_line_bytes"],
                      config.limits["read_block_bytes"])
    s_count = f_count = 0
    seq = 0
    for row_id, stream, rec in _iter_coverage(
        success_ink, failure_ink, run.input_rows_read
    ):
        if stream == "success":
            _stage_success_record(rec, row_id, run, stage, config)
            s_count += 1
        else:
            _stage_failure_record(rec, row_id, run, stage, config)
            f_count += 1
        seq += 1

    # Verification is on the bytes actually consumed, not on file metadata.
    if success_ink.digest() != run.success_sha256:
        raise WarehouseDataError(
            "digest_mismatch",
            f"success stream digest {success_ink.digest()} != manifest "
            f"{run.success_sha256}",
        )
    if failure_ink.digest() != run.failure_sha256:
        raise WarehouseDataError(
            "digest_mismatch",
            f"failure stream digest {failure_ink.digest()} != manifest "
            f"{run.failure_sha256}",
        )
    if s_count != run.success_rows:
        raise WarehouseDataError(
            "row_count_mismatch",
            f"read {s_count} success rows, manifest says {run.success_rows}",
        )
    if f_count != run.failure_rows:
        raise WarehouseDataError(
            "row_count_mismatch",
            f"read {f_count} failure rows, manifest says {run.failure_rows}",
        )
    # Exclusions were staged while run_id was still a placeholder; assign the
    # now-known identity to every row this run produced (they all precede
    # any cancellation/assignment staging, which adds none).
    if stage.conn.execute(
        "SELECT 1 FROM exclusions WHERE run_id = '' LIMIT 1").fetchone():
        raise WarehouseExecError("internal", "exclusion run identity not resolved")


def stage_cancellations(path, stage, config: WarehouseConfig) -> Digest:
    ink = Ink(path, config.limits["max_line_bytes"],
              config.limits["read_block_bytes"])
    for line_no, raw in ink:
        if not raw.strip():
            continue
        rec = parse_json(raw, f"cancellations line {line_no}")
        if not isinstance(rec, dict):
            raise WarehouseDataError(
                "structure", f"cancellations line {line_no}: not an object")
        sid = rec.get("shipment_id")
        if not is_nonempty_str(sid):
            raise WarehouseDataError(
                "structure",
                f"cancellations line {line_no}: shipment_id must be a "
                f"non-empty string",
            )
        try:
            stage.conn.execute(
                "INSERT INTO cancellations(shipment_id, row_no) VALUES(?,?)",
                (sid, line_no),
            )
        except sqlite3.IntegrityError:
            raise WarehouseDataError(
                "duplicate_cancellation",
                f"cancellations line {line_no}: shipment {sid!r} is cancelled "
                f"more than once in this batch",
            ) from None
    return Digest(ink.digest(), ink.nbytes)


def stage_assignments(path, stage, config: WarehouseConfig) -> Digest:
    ink = Ink(path, config.limits["max_line_bytes"],
              config.limits["read_block_bytes"])
    for line_no, raw in ink:
        if not raw.strip():
            continue
        rec = parse_json(raw, f"assignments line {line_no}")
        where = f"assignments line {line_no}"
        if not isinstance(rec, dict):
            raise WarehouseDataError("structure", f"{where}: not an object")
        op = rec.get("op", "write")
        if op in ("write", None):
            op = "upsert"
        if op not in ("upsert", "delete"):
            raise WarehouseDataError(
                "structure", f"{where}: op must be 'write' (upsert) or 'delete'")
        station = rec.get("station_id")
        assignment = rec.get("assignment_id")
        if not is_nonempty_str(station):
            raise WarehouseDataError(
                "structure", f"{where}: station_id must be a non-empty string")
        if not is_nonempty_str(assignment):
            raise WarehouseDataError(
                "structure", f"{where}: assignment_id must be a non-empty string")
        from_ts = from_epoch = to_ts = to_epoch = region = None
        if op == "upsert":
            if "from" not in rec:
                raise WarehouseDataError(
                    "structure", f"{where}: upsert needs from")
            from_epoch, from_ts = time_to_epoch(rec["from"], f"{where}.from")
            to_raw = rec.get("to")
            if to_raw is not None:
                to_epoch, to_ts = time_to_epoch(to_raw, f"{where}.to")
                if not from_epoch < to_epoch:
                    raise WarehouseDataError(
                        "bad_interval",
                        f"{where}: finite endpoints require from < to",
                    )
            region = rec.get("region")
            if not is_nonempty_str(region):
                raise WarehouseDataError(
                    "structure", f"{where}: region must be a non-empty string")
        try:
            stage.conn.execute(
                "INSERT INTO interval_changes(station_id, assignment_id, op, "
                "from_ts, from_epoch, to_ts, to_epoch, region, row_no) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (station, assignment, op, from_ts, from_epoch, to_ts, to_epoch,
                 region, line_no),
            )
        except sqlite3.IntegrityError:
            raise WarehouseDataError(
                "duplicate_interval_key",
                f"{where}: interval ({station!r}, {assignment!r}) is written more "
                f"than once in this batch",
            ) from None
    return Digest(ink.digest(), ink.nbytes)


@dataclass
class StagedBatch:
    stage: Stage
    cleaning_run: CleaningRun | None
    fingerprint: str
    digests: dict
    assignment_sha256: str | None
    cancellation_sha256: str | None

    def close(self):
        self.stage.close()


def build_stage(config: WarehouseConfig) -> StagedBatch:
    """Full structural validation of the batch into a disposable stage DB."""
    cleaning_run = None
    if config.cleaning_manifest is not None:
        cleaning_run = load_cleaning_run(config)

    try:
        os.makedirs(config.workspace_dir, exist_ok=True)
        work = tempfile.mkdtemp(prefix=".whstage-", dir=config.workspace_dir)
    except OSError as exc:
        raise WarehouseExecError("workspace", f"cannot create workspace: {exc}") from exc
    stage = Stage(work)
    try:
        if cleaning_run is not None:
            stage_cleaning_run(cleaning_run, stage, config)
        cancel_digest = (
            stage_cancellations(config.cancellations, stage, config)
            if config.cancellations is not None else None
        )
        assign_digest = (
            stage_assignments(config.assignments, stage, config)
            if config.assignments is not None else None
        )
        stage.finish_writes()
    except BaseException:
        stage.close()
        raise

    # Content identity: digests of the exact bytes consumed plus business
    # settings.  File paths never enter it, so moving inputs does not turn a
    # retry into a new batch.
    files = {}
    if cleaning_run is not None:
        files["manifest"] = cleaning_run.manifest_sha256
        files["success"] = cleaning_run.success_sha256
        files["failure"] = cleaning_run.failure_sha256
    if cancel_digest is not None:
        files["cancellations"] = cancel_digest.sha256
    if assign_digest is not None:
        files["assignments"] = assign_digest.sha256
    fingerprint = sha256_text(canonical_json({
        "business": config.business_fingerprint,
        "files": files,
    }))
    return StagedBatch(
        stage=stage,
        cleaning_run=cleaning_run,
        fingerprint=fingerprint,
        digests=files,
        assignment_sha256=(
            assign_digest.sha256 if assign_digest is not None else None),
        cancellation_sha256=(
            cancel_digest.sha256 if cancel_digest is not None else None),
    )
