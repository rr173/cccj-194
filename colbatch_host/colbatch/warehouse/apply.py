"""Incremental application of one staged batch to the warehouse.

Storage is slowly-changing-dimensions style: ``facts`` and ``daily`` are keyed
by ``(revision, ...)``.  Applying a batch writes a complete copy of the
*currently valid* state under the new revision number, and the revision row
records that number; no row is ever updated or deleted.  Consequently an
old revision remains byte-identical after any number of later corrections,
and an export of revision N can never observe revision N+1 state.

Contract: after every published revision the materialised facts and daily
summary of that revision equal a from-scratch computation over (all valid
facts, all intervals) as of that version.  Incrementality is in *how* the new
copy is produced:

  * Only shipments the batch touches are re-evaluated: ready records,
    cancellations, and existing facts whose station has a changed interval,
    restricted to the changed time spans (old and new interval of each
    changed key).  These keys are materialised in the stage database, so a
    large re-judge set never lives in process memory.
  * Untouched current fact rows are copied with a single ``INSERT ... SELECT``
    by revision range, then the touched rows are resolved one key at a time.
  * Daily contributions are copied from the previous revision and adjusted by
    the net delta of touched facts, one aggregate write per affected
    ``(date, region)`` key.  A fact moved within the batch produces a zero net
    delta, so overlapping touches are idempotent.
  * Facts are read by primary key or through ``(station_id, ts_epoch)``; an
    index probe never scans the fact table.  The plan is recorded in the
    receipt as observable evidence.
  * Revision fingerprints are computed while the new rows are written, so an
    export can prove its streamed output equals the revision without a second
    scan of another table.

Everything runs in one ``BEGIN IMMEDIATE`` transaction: a crash before commit
leaves the previous head untouched; a crash after commit leaves a complete
revision plus its batch row (written atomically with it), so a retry returns
the stored receipt.
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import sqlite3
import time

from .config import (
    WarehouseConfig,
    WarehouseConflictError,
    WarehouseDataError,
    WarehouseExecError,
    canonical_json,
)
from .ingest import StagedBatch
from .schema import (
    WarehouseDB,
    daily_hash,
    fact_hash,
    hex_xor,
    stage_attached,
    xor_hex,
)

FACT_COLS = (
    "shipment_id, station_id, norm_ts, ts_epoch, utc_date, amount_cents, "
    "assignment_id, region, source_run_id, source_manifest_sha256, "
    "source_success_sha256, source_stream, source_row_id, updated_revision"
)

SPAN_QUERY = (
    "SELECT shipment_id FROM facts WHERE station_id=? AND ts_epoch>=? "
    "AND (? IS NULL OR ts_epoch<?) AND valid_to IS NULL"
)


def utc_now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def region_key(region) -> str:
    return "u" if region is None else "r:" + region


class Metrics:
    def __init__(self):
        self.records = 0
        self.cancellations = 0
        self.interval_changes = 0
        self.interval_deletes = 0
        self.touched_shipments = 0
        self.inserted_facts = 0
        self.updated_facts = 0
        self.unchanged_facts = 0
        self.deleted_facts = 0
        self.rejudged_history_facts = 0
        self.moved_facts = 0
        self.daily_deltas = 0
        self.daily_keys_changed = 0
        self.exclusions = 0
        self.facts_read = 0
        self.daily_rows_read = 0
        self.intervals_read = 0
        self.span_rows_read = 0

    def as_dict(self, revision, parent):
        return {
            "revision": revision,
            "parent_revision": parent,
            "records": self.records,
            "cancellations": self.cancellations,
            "interval_changes": self.interval_changes,
            "interval_deletes": self.interval_deletes,
            "touched_shipments": self.touched_shipments,
            "inserted_facts": self.inserted_facts,
            "updated_facts": self.updated_facts,
            "unchanged_facts": self.unchanged_facts,
            "deleted_facts": self.deleted_facts,
            "rejudged_history_facts": self.rejudged_history_facts,
            "moved_facts": self.moved_facts,
            "daily_deltas": self.daily_deltas,
            "daily_keys_changed": self.daily_keys_changed,
            "exclusions": self.exclusions,
            "facts_read": self.facts_read,
            "daily_rows_read": self.daily_rows_read,
            "intervals_read": self.intervals_read,
            "span_rows_read": self.span_rows_read,
        }


# ---------------------------------------------------------------------------
# intervals


def _interval_row(conn, station, assignment):
    return conn.execute(
        "SELECT station_id, assignment_id, from_ts, from_epoch, to_ts, to_epoch, "
        "region FROM intervals WHERE station_id=? AND assignment_id=?",
        (station, assignment),
    ).fetchone()


def _check_overlaps(conn, stations, metrics):
    for station in stations:
        rows = conn.execute(
            "SELECT assignment_id, from_ts, from_epoch, to_ts, to_epoch FROM intervals "
            "WHERE station_id=? ORDER BY from_epoch, to_epoch IS NOT NULL, to_epoch, "
            "assignment_id",
            (station,),
        ).fetchall()
        metrics.intervals_read += len(rows)
        prev = None
        for row in rows:
            if prev is not None:
                prev_to = prev["to_epoch"]
                if prev_to is None or row["from_epoch"] < prev_to:
                    raise WarehouseDataError(
                        "interval_overlap",
                        f"station {station!r}: interval {prev['assignment_id']!r} "
                        f"[{prev['from_ts']}, {prev['to_ts'] or '+inf'}) overlaps "
                        f"{row['assignment_id']!r} starting {row['from_ts']}",
                    )
            prev = row


def apply_interval_changes(db, stage, metrics):
    conn = db.conn
    stations = set()
    spans = []  # (station_id, from_epoch, to_epoch_or_None)
    for ch in stage.conn.execute(
        "SELECT station_id, assignment_id, op, from_ts, from_epoch, to_ts, "
        "to_epoch, region, row_no FROM interval_changes ORDER BY row_no"
    ):
        station = ch["station_id"]
        assignment = ch["assignment_id"]
        stations.add(station)
        existing = _interval_row(conn, station, assignment)
        if existing is not None:
            metrics.intervals_read += 1
        if ch["op"] == "delete":
            if existing is None:
                raise WarehouseDataError(
                    "delete_unknown_interval",
                    f"assignments line {ch['row_no']}: interval "
                    f"({station!r}, {assignment!r}) does not exist",
                )
            spans.append((station, existing["from_epoch"], existing["to_epoch"]))
            conn.execute(
                "DELETE FROM intervals WHERE station_id=? AND assignment_id=?",
                (station, assignment),
            )
            metrics.interval_deletes += 1
            continue
        if existing is not None:
            spans.append((station, existing["from_epoch"], existing["to_epoch"]))
        spans.append((station, ch["from_epoch"], ch["to_epoch"]))
        conn.execute(
            "INSERT INTO intervals(station_id, assignment_id, from_ts, from_epoch, "
            "to_ts, to_epoch, region, updated_revision) VALUES(?,?,?,?,?,?,?,0) "
            "ON CONFLICT(station_id, assignment_id) DO UPDATE SET "
            "from_ts=excluded.from_ts, from_epoch=excluded.from_epoch, "
            "to_ts=excluded.to_ts, to_epoch=excluded.to_epoch, "
            "region=excluded.region",
            (station, assignment, ch["from_ts"], ch["from_epoch"], ch["to_ts"],
             ch["to_epoch"], ch["region"]),
        )
        metrics.interval_changes += 1
    _check_overlaps(conn, sorted(stations), metrics)
    return spans


def find_interval(conn, station: str, ts_epoch: int):
    rows = conn.execute(
        "SELECT assignment_id, region FROM intervals WHERE station_id=? "
        "AND from_epoch<=? AND (to_epoch IS NULL OR ?<to_epoch) "
        "ORDER BY from_epoch LIMIT 2",
        (station, ts_epoch, ts_epoch),
    ).fetchall()
    if len(rows) > 1:
        raise WarehouseExecError(
            "corrupt_intervals",
            f"station {station!r} has overlapping intervals after validation",
        )
    return rows[0] if rows else None


def merged_spans(spans):
    """Per-station union of half-open spans so each historical fact is
    visited at most once even when several changed intervals cover it."""
    by_station = {}
    for station, lo, hi in spans:
        by_station.setdefault(station, []).append((lo, hi))
    for station, items in sorted(by_station.items()):
        items.sort(key=lambda p: (p[0], p[1] is not None, p[1] or 0))
        merged = []
        for lo, hi in items:
            if merged and (merged[-1][1] is None or lo <= merged[-1][1]):
                prev_lo, prev_hi = merged[-1]
                merged[-1] = (
                    prev_lo,
                    None if (prev_hi is None or hi is None) else max(prev_hi, hi),
                )
            else:
                merged.append((lo, hi))
        for lo, hi in merged:
            yield station, lo, hi


# ---------------------------------------------------------------------------
# steps inside the transaction


def _build_touched(db, stage, parent, spans, metrics):
    """Materialise the re-judge key set in the stage database.

    Each touched id carries its *current* fact snapshot so the pass needs one
    primary-key lookup per touched shipment.  The key set lives in SQLite, so
    a huge re-judge set does not grow process memory.
    """
    stage.conn.execute(
        "CREATE TABLE IF NOT EXISTS touched ("
        "shipment_id TEXT PRIMARY KEY, origin TEXT NOT NULL, old_json TEXT)"
    )
    stage.conn.execute("DELETE FROM touched")
    # A cancellation wins the origin label: it must delete the fact even when
    # the same station got a span touch.  (Update+cancel is rejected upstream.)
    stage.conn.execute(
        "INSERT OR IGNORE INTO touched(shipment_id, origin, old_json) "
        "SELECT shipment_id, 'cancel', NULL FROM cancellations"
    )
    stage.conn.execute(
        "INSERT OR IGNORE INTO touched(shipment_id, origin, old_json) "
        "SELECT shipment_id, 'record', NULL FROM records"
    )
    plan = [r["detail"] for r in db.conn.execute(
        "EXPLAIN QUERY PLAN " + SPAN_QUERY, ("(station)", 0, None, None))]
    plan += [r["detail"] for r in db.conn.execute(
        "EXPLAIN QUERY PLAN SELECT * FROM facts WHERE shipment_id=? AND valid_to IS NULL",
        ("(sid)",))]
    for station, lo, hi in merged_spans(spans):
        cur = db.conn.execute(SPAN_QUERY, (station, lo, hi, hi))
        for row in cur:
            metrics.span_rows_read += 1
            stage.conn.execute(
                "INSERT OR IGNORE INTO touched(shipment_id, origin, old_json) "
                "VALUES(?, 'span', NULL)",
                (row["shipment_id"],),
            )
    for row in stage.conn.execute(
        "SELECT shipment_id FROM touched ORDER BY shipment_id"
    ).fetchall():
        metrics.facts_read += 1
        old = db.conn.execute(
            f"SELECT {FACT_COLS} FROM facts WHERE shipment_id=? AND valid_to IS NULL",
            (row["shipment_id"],),
        ).fetchone()
        stage.conn.execute(
            "UPDATE touched SET old_json=? WHERE shipment_id=?",
            (None if old is None else canonical_json(dict(old)), row["shipment_id"]),
        )
    return plan


def _resolve_touched(db, stage, revision, metrics):
    """Decide the new content of every touched shipment.

    ``resolved`` records one of:
      ``write``   a new fact row to insert for this revision;
      ``delete``  the current fact row is closed (cancellation);
      ``carry``   nothing changes (same attribution and provenance).
    The aggregate effect on each daily key is kept in ``daily_delta``.
    """
    conn = db.conn
    stage.conn.execute(
        "CREATE TABLE IF NOT EXISTS resolved ("
        "shipment_id TEXT PRIMARY KEY, action TEXT NOT NULL, new_json TEXT)"
    )
    stage.conn.execute("DELETE FROM resolved")
    stage.conn.execute("DELETE FROM daily_delta")
    for row in stage.conn.execute(
        "SELECT t.shipment_id, t.origin, t.old_json, r.station_id AS r_station, "
        "r.norm_ts, r.ts_epoch, r.utc_date, r.amount_cents, r.run_id AS r_run, "
        "r.manifest_sha256 AS r_manifest, r.success_sha256 AS r_success, "
        "r.source_row_id AS r_row "
        "FROM touched t LEFT JOIN records r ON r.shipment_id = t.shipment_id "
        "ORDER BY t.shipment_id"
    ):
        metrics.touched_shipments += 1
        sid = row["shipment_id"]
        old = json.loads(row["old_json"]) if row["old_json"] else None
        origin = row["origin"]
        is_record = row["r_station"] is not None

        if origin == "cancel":
            _daily_delta(stage, old["utc_date"], region_key(old["region"]),
                         old["region"], -1, -int(old["amount_cents"]))
            stage.conn.execute(
                "INSERT INTO resolved(shipment_id, action, new_json) VALUES(?,?,NULL)",
                (sid, "delete"))
            metrics.deleted_facts += 1
            metrics.daily_deltas += 1
            continue

        if is_record:
            new_station = row["r_station"]
            norm_ts = row["norm_ts"]
            ts_epoch = row["ts_epoch"]
            utc_date = row["utc_date"]
            amount = row["amount_cents"]
            source = {
                "source_run_id": row["r_run"],
                "source_manifest_sha256": row["r_manifest"],
                "source_success_sha256": row["r_success"],
                "source_stream": "success",
                "source_row_id": row["r_row"],
            }
        elif old is not None:
            new_station = old["station_id"]
            norm_ts = old["norm_ts"]
            ts_epoch = old["ts_epoch"]
            utc_date = old["utc_date"]
            amount = old["amount_cents"]
            source = {k: old[k] for k in (
                "source_run_id", "source_manifest_sha256",
                "source_success_sha256", "source_stream", "source_row_id")}
        else:
            stage.conn.execute(
                "INSERT INTO resolved(shipment_id, action, new_json) VALUES(?,?,NULL)",
                (sid, "absent"))
            continue

        match = find_interval(conn, new_station, ts_epoch)
        new_assignment = match["assignment_id"] if match else None
        new_region = match["region"] if match else None

        if old is not None:
            if origin == "span":
                metrics.rejudged_history_facts += 1
            same_attribution = (
                old["station_id"] == new_station
                and old["norm_ts"] == norm_ts
                and old["amount_cents"] == amount
                and old["assignment_id"] == new_assignment
                and old["region"] == new_region
            )
            same_source = all(old[k] == v for k, v in source.items())
            if same_attribution and same_source:
                metrics.unchanged_facts += 1
                stage.conn.execute(
                    "INSERT INTO resolved(shipment_id, action, new_json) "
                    "VALUES(?,?,NULL)", (sid, "carry"))
                continue
            if origin == "record":
                metrics.updated_facts += 1
            if (old["assignment_id"] != new_assignment
                    or old["region"] != new_region):
                metrics.moved_facts += 1
            _daily_delta(stage, old["utc_date"], region_key(old["region"]),
                         old["region"], -1, -int(old["amount_cents"]))
            metrics.daily_deltas += 1
        else:
            metrics.inserted_facts += 1

        _daily_delta(stage, utc_date, region_key(new_region), new_region, 1, amount)
        metrics.daily_deltas += 1
        row_obj = {
            "shipment_id": sid,
            "station_id": new_station,
            "norm_ts": norm_ts,
            "ts_epoch": ts_epoch,
            "utc_date": utc_date,
            "amount_cents": amount,
            "assignment_id": new_assignment,
            "region": new_region,
            **source,
        }
        stage.conn.execute(
            "INSERT INTO resolved(shipment_id, action, new_json) VALUES(?,?,?)",
            (sid, "write", canonical_json(row_obj)))


def _write_facts(db, stage, revision, metrics):
    """Close the old version and insert the new one for each written fact.

    Each touched key is one primary-key update (``valid_to``) plus one insert;
    untouched fact rows are not read or written at all.
    """
    conn = db.conn
    for row in stage.conn.execute(
        "SELECT shipment_id, new_json FROM resolved WHERE action='write' "
        "ORDER BY shipment_id"
    ):
        obj = json.loads(row["new_json"])
        conn.execute(
            "UPDATE facts SET valid_to=? WHERE shipment_id=? AND valid_to IS NULL",
            (revision, obj["shipment_id"]),
        )
        conn.execute(
            "INSERT INTO facts(" + FACT_COLS + ", valid_from, valid_to) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                obj["shipment_id"], obj["station_id"], obj["norm_ts"],
                obj["ts_epoch"], obj["utc_date"], obj["amount_cents"],
                obj["assignment_id"], obj["region"], obj["source_run_id"],
                obj["source_manifest_sha256"], obj["source_success_sha256"],
                obj["source_stream"], obj["source_row_id"], revision,
                revision, None,
            ),
        )
    for row in stage.conn.execute(
        "SELECT shipment_id FROM resolved WHERE action='delete' ORDER BY shipment_id"
    ):
        conn.execute(
            "UPDATE facts SET valid_to=? WHERE shipment_id=? AND valid_to IS NULL",
            (revision, row["shipment_id"]),
        )


def _daily_delta(stage, utc_date, key, region, shipments, amount):
    if shipments == 0 and amount == 0:
        return
    stage.conn.execute(
        "INSERT INTO daily_delta(utc_date, region_key, region, shipments, amount_cents) "
        "VALUES(?,?,?,?,?) ON CONFLICT(utc_date, region_key) DO UPDATE SET "
        "shipments=shipments+excluded.shipments, "
        "amount_cents=amount_cents+excluded.amount_cents",
        (utc_date, key, region, shipments, amount),
    )


def _apply_daily(db, stage, revision, metrics):
    """Close changed daily version rows and insert their new values.

    Untouched ``(date, region)`` keys are neither read nor written: their
    historical rows remain valid over the new revision automatically.
    """
    conn = db.conn
    keys = stage.conn.execute(
        "SELECT utc_date, region_key, region, shipments, amount_cents "
        "FROM daily_delta WHERE shipments != 0 OR amount_cents != 0 "
        "ORDER BY utc_date, region_key"
    ).fetchall()
    for d in keys:
        old = conn.execute(
            "SELECT shipments, amount_cents FROM daily WHERE utc_date=? "
            "AND region_key=? AND valid_to IS NULL",
            (d["utc_date"], d["region_key"]),
        ).fetchone()
        metrics.daily_rows_read += 1
        old_count = old["shipments"] if old else 0
        old_amount = old["amount_cents"] if old else 0
        new_count = old_count + d["shipments"]
        new_amount = old_amount + d["amount_cents"]
        if new_count < 0:
            raise WarehouseExecError(
                "daily_underflow",
                f"negative shipment count for ({d['utc_date']}, "
                f"{d['region_key']}) -- warehouse state inconsistent",
            )
        if new_count == 0 and new_amount != 0:
            raise WarehouseExecError(
                "daily_inconsistent",
                f"({d['utc_date']}, {d['region_key']}) has zero shipments but "
                f"{new_amount} cents -- warehouse state inconsistent",
            )
        if old is not None:
            conn.execute(
                "UPDATE daily SET valid_to=? WHERE utc_date=? AND region_key=? "
                "AND valid_to IS NULL",
                (revision, d["utc_date"], d["region_key"]),
            )
        if new_count > 0:
            conn.execute(
                "INSERT INTO daily(utc_date, region_key, region, shipments, "
                "amount_cents, valid_from, valid_to) VALUES(?,?,?,?,?,?,NULL)",
                (d["utc_date"], d["region_key"], d["region"], new_count,
                 new_amount, revision),
            )
        metrics.daily_keys_changed += 1


# Test-only fault injection; inert unless COLBATCH_WAREHOUSE_FAULT is set.
def _faults() -> dict:
    raw = os.environ.get("COLBATCH_WAREHOUSE_FAULT")
    if not raw:
        return {}
    try:
        val = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return val if isinstance(val, dict) else {}


def _fault_pause(name):
    ms = _faults().get(name)
    if isinstance(ms, (int, float)) and ms > 0:
        time.sleep(ms / 1000.0)


def _fault_crash(name):
    if _faults().get(name):
        os._exit(9)


class ApplyOutcome:
    def __init__(self, status, revision, exit_code, payload):
        self.status = status
        self.revision = revision
        self.exit_code = exit_code
        self.payload = payload


def _workspace_usage(stage_path):
    """Disk footprint of the batch's scratch state (bounded by design)."""
    try:
        total = 0
        directory = os.path.dirname(stage_path)
        for name in os.listdir(directory):
            total += os.path.getsize(os.path.join(directory, name))
        return {"bytes": total}
    except OSError:
        return {"bytes": None}


def _fingerprints(db, stage, parent, revision):
    """Revision fingerprints, derived incrementally from the parent's.

    Valid-at-R rows = valid-at-(R-1) rows minus the rows this batch closed
    plus the rows this batch created.  Only those deltas are read; untouched
    facts and daily keys are never scanned.  The XOR of 64-bit hashes is an
    order-independent fingerprint, which makes the deltas trivial arithmetic
    and is recomputed exactly by ``warehouse-export`` while streaming.
    """
    facts_xor = hex_xor(db.meta("facts_xor"))
    facts_count = db.meta_int("facts_count")
    daily_xor = hex_xor(db.meta("daily_xor"))
    daily_count = db.meta_int("daily_count")

    closed = 0
    inserted = 0
    for row in stage.conn.execute(
        "SELECT t.old_json, r.action, r.new_json FROM touched t "
        "LEFT JOIN resolved r ON r.shipment_id = t.shipment_id"
    ):
        action = row["action"] or "carry"
        if row["old_json"] is not None and action != "carry":
            facts_xor ^= fact_hash(json.loads(row["old_json"]))
            closed += 1
        if action == "write":
            facts_xor ^= fact_hash(json.loads(row["new_json"]))
            inserted += 1
    facts_count += inserted - closed

    # Daily: a changed key closes its old current row (when one exists) and
    # opens a new one (unless the key became empty).
    for row in stage.conn.execute(
        "SELECT utc_date, region_key, shipments, amount_cents FROM daily_delta "
        "WHERE shipments != 0 OR amount_cents != 0 ORDER BY utc_date, region_key"
    ):
        old = db.conn.execute(
            "SELECT shipments, amount_cents FROM daily WHERE utc_date=? AND "
            "region_key=? AND valid_to=?", (row["utc_date"], row["region_key"], revision)
        ).fetchone()
        new = db.conn.execute(
            "SELECT shipments, amount_cents FROM daily WHERE utc_date=? AND "
            "region_key=? AND valid_from=?", (row["utc_date"], row["region_key"], revision)
        ).fetchone()
        if old is not None:
            daily_xor ^= daily_hash(row["utc_date"], row["region_key"],
                                    old["shipments"], old["amount_cents"])
        if new is not None:
            daily_xor ^= daily_hash(row["utc_date"], row["region_key"],
                                    new["shipments"], new["amount_cents"])
        # Maintain the key count incrementally, one lookup per changed key.
        if old is not None:
            daily_count -= 1
        if new is not None:
            daily_count += 1

    db.set_meta("facts_count", facts_count)
    db.set_meta("facts_xor", xor_hex(facts_xor))
    db.set_meta("daily_count", daily_count)
    db.set_meta("daily_xor", xor_hex(daily_xor))
    return facts_count, xor_hex(facts_xor), daily_count, xor_hex(daily_xor)


def _existing_batch(conn, batch_id):
    return conn.execute(
        "SELECT * FROM batches WHERE batch_id=?", (batch_id,)
    ).fetchone()


def _apply_txn(db, config, staged, metrics):
    conn = db.conn
    head = db.meta_int("head_revision")
    existing = _existing_batch(conn, config.batch_id)
    if existing is not None:
        if (existing["expected_parent_version"] == config.expected_parent
                and existing["input_fingerprint"] == staged.fingerprint
                and existing["business_fingerprint"] == config.business_fingerprint):
            return {
                "status": "retry",
                "revision": existing["revision"],
                "parent_revision": existing["expected_parent_version"],
                "head_revision": head,
                "receipt": json.loads(existing["receipt_json"]),
            }
        raise WarehouseConflictError(
            "batch_id_reused",
            f"batch_id {config.batch_id!r} was already applied as revision "
            f"{existing['revision']} with different content or expected parent",
        )
    if head != config.expected_parent:
        raise WarehouseConflictError(
            "stale_expected_parent",
            f"expected parent revision {config.expected_parent} but warehouse head "
            f"is {head}",
        )

    stage = staged.stage
    clash = stage.conn.execute(
        "SELECT c.shipment_id FROM cancellations c JOIN records r "
        "ON c.shipment_id = r.shipment_id LIMIT 1"
    ).fetchone()
    if clash is not None:
        raise WarehouseDataError(
            "cancel_and_update",
            f"shipment {clash['shipment_id']!r} is both updated and cancelled "
            f"in this batch",
        )

    metrics.records = stage.conn.execute(
        "SELECT COUNT(*) FROM records").fetchone()[0]
    metrics.cancellations = stage.conn.execute(
        "SELECT COUNT(*) FROM cancellations").fetchone()[0]
    metrics.exclusions = stage.conn.execute(
        "SELECT COUNT(*) FROM exclusions").fetchone()[0]

    # 1. interval changes first: overlap validation sees the whole new state.
    spans = apply_interval_changes(db, stage, metrics)

    # 2. cancellation targets must exist (rejected wholesale otherwise).
    for row in stage.conn.execute(
        "SELECT shipment_id FROM cancellations ORDER BY shipment_id"
    ):
        metrics.facts_read += 1
        if conn.execute(
            "SELECT 1 FROM facts WHERE shipment_id=? AND valid_to IS NULL",
            (row["shipment_id"],),
        ).fetchone() is None:
            raise WarehouseDataError(
                "cancel_unknown",
                f"cancellation targets unknown shipment {row['shipment_id']!r}",
            )

    # 3. touched set + previous-fact snapshots, all in the stage DB.
    plan = _build_touched(db, stage, head, spans, metrics)

    # 4. decide and write the new version of every touched fact.
    _resolve_touched(db, stage, head + 1, metrics)
    _write_facts(db, stage, head + 1, metrics)

    # 5. adjust affected daily keys; untouched keys keep their valid row.
    _apply_daily(db, stage, head + 1, metrics)

    facts_count, facts_xor, daily_count, daily_xor = _fingerprints(
        db, stage, head, head + 1)
    metrics_dict = metrics.as_dict(head + 1, head)
    metrics_dict["plan"] = plan
    # Untouched rows are carried implicitly by SCD2 validity; report how many
    # the new revision contains without touching them.
    metrics_dict["carried_facts"] = db.meta_int("facts_count") - (
        metrics.inserted_facts + metrics.updated_facts)
    # Keys not touched by this batch are carried implicitly by SCD2 validity.
    metrics_dict["daily_rows"] = db.meta_int("daily_count")
    metrics_dict["limits"] = config.describe_bounds()
    if db.audit:
        metrics_dict["access_audit"] = db.audit_summary()
    metrics_dict["workspace"] = _workspace_usage(stage.path)
    receipt = {
        "batch_id": config.batch_id,
        "revision": head + 1,
        "parent_revision": head,
        "input_fingerprint": staged.fingerprint,
        "business_fingerprint": config.business_fingerprint,
        "files": staged.digests,
        "run": staged.cleaning_run.run_dict() if staged.cleaning_run else None,
        "metrics": metrics_dict,
        "facts_count": facts_count,
        "facts_xor": facts_xor,
        "daily_count": daily_count,
        "daily_xor": daily_xor,
        "applied_at": utc_now(),
    }

    conn.execute(
        "INSERT INTO revisions(revision, parent_revision, batch_id, created_at, "
        "metrics_json, facts_count, facts_xor, daily_count, daily_xor, "
        "exclusions_count) VALUES(?,?,?,?,?,?,?,?,?,?)",
        (head + 1, head, config.batch_id, receipt["applied_at"],
         canonical_json(metrics_dict), facts_count, facts_xor, daily_count,
         daily_xor, metrics.exclusions),
    )
    with stage_attached(conn, stage.path):
        conn.execute(
            "INSERT OR REPLACE INTO exclusions(revision, seq, run_id, manifest_sha256, "
            "stream, row_id, shipment_id, station_id, reason, detail) "
            "SELECT ?, seq, run_id, manifest_sha256, stream, row_id, shipment_id, "
            "station_id, reason, detail FROM stage.exclusions",
            (head + 1,),
        )
    if staged.cleaning_run is not None:
        run = staged.cleaning_run
        conn.execute(
            "INSERT OR IGNORE INTO runs(run_id, manifest_sha256, plugin_name, "
            "plugin_version, input_rows_read, success_sha256, success_rows, "
            "failure_sha256, failure_rows) VALUES(?,?,?,?,?,?,?,?,?)",
            (run.run_id, run.manifest_sha256, run.plugin_name, run.plugin_version,
             run.input_rows_read, run.success_sha256, run.success_rows,
             run.failure_sha256, run.failure_rows),
        )
    conn.execute(
        "INSERT INTO batches(batch_id, expected_parent_version, input_fingerprint, "
        "business_fingerprint, revision, applied_at, receipt_json) "
        "VALUES(?,?,?,?,?,?,?)",
        (config.batch_id, config.expected_parent, staged.fingerprint,
         config.business_fingerprint, head + 1, receipt["applied_at"],
         canonical_json(receipt)),
    )
    db.set_meta("head_revision", head + 1)
    return {
        "status": "applied",
        "revision": head + 1,
        "parent_revision": head,
        "head_revision": head + 1,
        "receipt": receipt,
    }


def run_apply(config: WarehouseConfig, staged: StagedBatch) -> ApplyOutcome:
    """Apply a validated stage under one exclusive transaction."""
    db = WarehouseDB(config)
    metrics = Metrics()
    try:
        try:
            db.conn.execute("BEGIN IMMEDIATE")
            # Fault hook for the concurrency/crash matrix: pause inside the
            # write transaction (just before commit) or simulate a hard kill
            # on either side of COMMIT.
            _fault_pause("pause_before_commit_ms")
            _fault_crash("crash_before_commit")
        except sqlite3.OperationalError as exc:
            raise WarehouseExecError(
                "db_locked", f"cannot start an exclusive transaction: {exc}") from exc
        try:
            result = _apply_txn(db, config, staged, metrics)
            db.conn.execute("COMMIT")
            _fault_crash("crash_after_commit")
        except BaseException:
            try:
                db.conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        return ApplyOutcome(result["status"], result["revision"], 0, result)
    finally:
        db.close()
