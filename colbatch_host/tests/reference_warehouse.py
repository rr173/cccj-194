#!/usr/bin/env python3
"""Independent in-memory reference for the delivery-attribution warehouse.

Deliberately shares **no code** with ``colbatch.warehouse``: it re-reads the
raw cleaning products / cancellation files / assignment files of every
batch, replays them version by version in plain Python dictionaries, and
recomputes facts, daily summary, exclusions and source coordinates from
scratch at each revision by direct interval matching and grouping.

This is the oracle used by ``tests/test_warehouse.py``.  It is slow and
loads everything in memory on purpose; the product must agree with it while
staying incremental and bounded.

Usage (library):

    ref = ReferenceWarehouse()
    ref.apply_batch({                     # same shape as a batch config
        "cleaning_run": {"manifest": ..., "success": ..., "failure": ...},
        "cancellations": ...,
        "assignments": ...,
    })
    view = ref.export(revision)           # dict as documented below
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone

HEX64 = re.compile(r"[0-9a-f]{64}")
AMOUNT = re.compile(r"-?\d+(?:\.\d{1,2})?")
TS_FMT = "%Y-%m-%dT%H:%M:%SZ"


class RefError(Exception):
    def __init__(self, reason, message):
        super().__init__(message)
        self.reason = reason
        self.message = message


def ts_epoch(text):
    dt = datetime.strptime(text, TS_FMT)
    if dt.strftime(TS_FMT) != text:
        raise RefError("bad_time", f"bad time {text!r}")
    return int(dt.replace(tzinfo=timezone.utc).timestamp())


def cents(text):
    if not AMOUNT.fullmatch(text):
        raise RefError("bad_amount", f"bad amount {text!r}")
    neg = text.startswith("-")
    body = text[1:] if neg else text
    whole, _, frac = body.partition(".")
    value = int(whole) * 100 + int((frac + "00")[:2])
    return -value if neg else value


def _read_jsonl(path):
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                yield json.loads(line)


def _sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


class ReferenceWarehouse:
    def __init__(self):
        self.revisions = []          # list of exported views
        self.batches = {}            # batch_id -> (fingerprint, revision)
        # mutable state
        self.facts = {}              # shipment_id -> fact dict
        self.intervals = {}          # (station, assignment) -> dict
        self.exclusions = []         # last revision's exclusion list

    # -- batch application ------------------------------------------------
    def _batch_fingerprint(self, cfg):
        files = {}
        run = cfg.get("cleaning_run") or {}
        if run:
            manifest = run["manifest"]
            with open(manifest, "r", encoding="utf-8") as fh:
                m = json.load(fh)
            files["manifest"] = _sha(manifest)
            files["success"] = m["outputs"]["success"]["sha256"]
            files["failure"] = m["outputs"]["failure"]["sha256"]
        if cfg.get("cancellations"):
            files["cancellations"] = _sha(cfg["cancellations"])
        if cfg.get("assignments"):
            files["assignments"] = _sha(cfg["assignments"])
        payload = {"business": "ref-v1",
                   "files": dict(sorted(files.items()))}
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":"),
                       ensure_ascii=False).encode()).hexdigest(), files

    def apply_batch(self, cfg):
        fingerprint, files = self._batch_fingerprint(cfg)
        batch_id = cfg["batch_id"]
        parent = cfg["expected_parent_version"]
        if batch_id in self.batches:
            old_fp, rev = self.batches[batch_id]
            if old_fp != fingerprint:
                raise RefError("conflict", f"batch {batch_id} reused with new content")
            return ("retry", rev)
        if parent != len(self.revisions):
            raise RefError("conflict", f"stale parent {parent} != {len(self.revisions)}")

        records = {}
        excluded = []
        run = cfg.get("cleaning_run")
        if run:
            records, excluded = self._read_run(run)
        cancels = []
        if cfg.get("cancellations"):
            seen = set()
            for rec in _read_jsonl(cfg["cancellations"]):
                sid = rec.get("shipment_id")
                if not isinstance(sid, str) or not sid:
                    raise RefError("bad_cancel", "empty shipment_id")
                if sid in seen:
                    raise RefError("bad_cancel", f"duplicate cancel {sid}")
                seen.add(sid)
                cancels.append(sid)
        changes = []
        if cfg.get("assignments"):
            changes = self._parse_assignments(cfg["assignments"])

        # apply interval changes then validate the entire state
        touched_stations = set()
        for ch in changes:
            touched_stations.add(ch["station_id"])
            key = (ch["station_id"], ch["assignment_id"])
            if ch["op"] == "delete":
                if key not in self.intervals:
                    raise RefError("delete_unknown", f"delete unknown {key}")
                del self.intervals[key]
            else:
                self.intervals[key] = {
                    "from": ch["from"], "from_epoch": ts_epoch(ch["from"]),
                    "to": ch["to"], "to_epoch": (
                        ts_epoch(ch["to"]) if ch["to"] else None),
                    "region": ch["region"],
                }
        for station in touched_stations:
            rows = sorted(
                (v for (s, a), v in self.intervals.items() if s == station),
                key=lambda v: (v["from_epoch"],
                               v["to_epoch"] is None, v["to_epoch"] or 0))
            for a, b in zip(rows, rows[1:]):
                if a["to_epoch"] is None or b["from_epoch"] < a["to_epoch"]:
                    raise RefError("interval_overlap", f"overlap at {station}")

        for sid in cancels:
            if sid not in self.facts:
                raise RefError("cancel_unknown", f"cancel unknown {sid}")
            if sid in records:
                raise RefError("cancel_and_update", f"{sid} updated and cancelled")
        for sid, rec in records.items():
            self.facts[sid] = rec
        for sid in cancels:
            del self.facts[sid]

        # recompute everything from scratch
        rev = self.export_scratch(excluded)
        self.revisions.append(rev)
        self.batches[batch_id] = (fingerprint, len(self.revisions))
        return ("applied", len(self.revisions))

    def _read_run(self, run):
        manifest = run["manifest"]
        with open(manifest, "r", encoding="utf-8") as fh:
            m = json.load(fh)
        if m.get("status") != "completed":
            raise RefError("run_not_completed", "not completed")
        base = os.path.dirname(os.path.abspath(manifest))
        s_path = run.get("success") or m["outputs"]["success"].get("path")
        f_path = run.get("failure") or m["outputs"]["failure"].get("path")
        if s_path and not os.path.isabs(s_path):
            s_path = os.path.join(base, s_path)
        if f_path and not os.path.isabs(f_path):
            f_path = os.path.join(base, f_path)
        records = {}
        excluded = []
        rows = {}
        for stream, path, declared in (
            ("success", s_path, m["outputs"]["success"]),
            ("failure", f_path, m["outputs"]["failure"]),
        ):
            got = []
            for rec in _read_jsonl(path):
                got.append(rec)
            rows[stream] = got
            if _sha(path) != declared["sha256"]:
                raise RefError("digest_mismatch", f"{stream} digest mismatch")
            if len(got) != declared["rows"]:
                raise RefError("row_count", f"{stream} row count mismatch")
        n = m["input_rows_read"]
        if len(rows["success"]) + len(rows["failure"]) != n:
            raise RefError("row_count", "success+failure != input rows")
        s_rows = {r["row_id"]: r for r in rows["success"]}
        f_rows = {r["row_id"]: r for r in rows["failure"]}
        if len(s_rows) != len(rows["success"]) or len(f_rows) != len(rows["failure"]):
            raise RefError("row_order", "duplicate row ids")
        if set(s_rows) & set(f_rows):
            raise RefError("cross_stream_collision", "row in both streams")
        if set(s_rows) | set(f_rows) != set(range(n)):
            raise RefError("row_coverage", "missing or extra row ids")
        run_id = hashlib.sha256(json.dumps({
            "manifest": _sha(manifest),
            "success": {"sha256": m["outputs"]["success"]["sha256"],
                        "rows": m["outputs"]["success"]["rows"]},
            "failure": {"sha256": m["outputs"]["failure"]["sha256"],
                        "rows": m["outputs"]["failure"]["rows"]},
        }, sort_keys=True, separators=(",", ":"),
            ensure_ascii=False).encode()).hexdigest()
        for rid in range(n):
            if rid in f_rows:
                rec = f_rows[rid]
                err = rec.get("error") or {}
                excluded.append({
                    "stream": "failure", "row_id": rid,
                    "shipment_id": (rec.get("source") or {}).get("shipment_id"),
                    "station_id": (rec.get("source") or {}).get("station_id"),
                    "reason": err.get("cause") or "cleaning_failure",
                })
                continue
            rec = s_rows[rid]
            out = rec["output"]
            src = rec["source"]
            if not out.get("is_valid"):
                excluded.append({
                    "stream": "success", "row_id": rid,
                    "shipment_id": src.get("shipment_id"),
                    "station_id": src.get("station_id"),
                    "reason": "invalid_row",
                })
                continue
            sid = src.get("shipment_id")
            station = src.get("station_id")
            nts = out.get("norm_ts")
            namt = out.get("norm_amount")
            missing = []
            if not isinstance(sid, str) or not sid:
                missing.append("shipment_id")
            if not isinstance(station, str) or not station:
                missing.append("station_id")
            if not isinstance(nts, str) or not nts:
                missing.append("norm_ts")
            if not isinstance(namt, str) or not namt:
                missing.append("norm_amount")
            if missing:
                excluded.append({
                    "stream": "success", "row_id": rid,
                    "shipment_id": sid, "station_id": station,
                    "reason": "missing_required",
                })
                continue
            try:
                se = ts_epoch(nts)
                amount = cents(namt)
            except RefError as exc:
                raise RefError("structure", f"row {rid}: {exc}")
            if sid in records:
                raise RefError("duplicate_shipment", f"dup {sid}")
            records[sid] = {
                "shipment_id": sid, "station_id": station,
                "norm_ts": nts, "ts_epoch": se, "utc_date": nts[:10],
                "amount_cents": amount,
                "source": {
                    "run_id": run_id,
                    "manifest_sha256": _sha(manifest),
                    "success_sha256": m["outputs"]["success"]["sha256"],
                    "stream": "success", "row_id": rid,
                },
            }
        return records, excluded

    def _parse_assignments(self, path):
        changes = []
        seen = set()
        for rec in _read_jsonl(path):
            key = (rec["station_id"], rec["assignment_id"])
            if key in seen:
                raise RefError("duplicate_interval_key", str(key))
            seen.add(key)
            op = "delete" if rec.get("op") == "delete" else "upsert"
            if op == "delete":
                changes.append({"station_id": key[0], "assignment_id": key[1],
                                "op": "delete"})
                continue
            if "from" not in rec:
                raise RefError("bad_interval", "missing from")
            if rec.get("to") is not None:
                if ts_epoch(rec["from"]) >= ts_epoch(rec["to"]):
                    raise RefError("bad_interval", "from >= to")
            if not isinstance(rec.get("region"), str) or not rec["region"]:
                raise RefError("bad_interval", "bad region")
            changes.append({"station_id": key[0], "assignment_id": key[1],
                            "op": "upsert", "from": rec["from"],
                            "to": rec.get("to"), "region": rec["region"]})
        return changes

    # -- from-scratch computation ----------------------------------------
    def _attribute(self, station, ts):
        best = None
        for (s, _a), v in self.intervals.items():
            if s != station:
                continue
            if v["from_epoch"] <= ts and (
                    v["to_epoch"] is None or ts < v["to_epoch"]):
                if best is not None:
                    raise RefError("interval_overlap", "ambiguous match")
                best = v
        if best is None:
            return None, None
        # identify assignment id
        for (s, a), v in self.intervals.items():
            if v is best:
                return a, v["region"]
        return None, None

    def export_scratch(self, exclusions=None):
        facts = []
        daily = {}
        for sid in sorted(self.facts):
            f = self.facts[sid]
            aid, region = self._attribute(f["station_id"], f["ts_epoch"])
            row = {
                "shipment_id": sid, "station_id": f["station_id"],
                "event_time": f["norm_ts"], "utc_date": f["utc_date"],
                "amount_cents": f["amount_cents"],
                "assignment_id": aid, "region": region,
                "source": dict(f["source"]),
            }
            facts.append(row)
            key = (f["utc_date"], "u" if region is None else "r:" + region)
            if key not in daily:
                daily[key] = {"utc_date": f["utc_date"], "region": region,
                              "shipments": 0, "amount_cents": 0}
            daily[key]["shipments"] += 1
            daily[key]["amount_cents"] += f["amount_cents"]
        daily_rows = sorted(
            daily.values(),
            key=lambda d: (d["utc_date"],
                           1 if d["region"] is None else 0,
                           d["region"] or ""))
        excl = exclusions if exclusions is not None else self.exclusions
        return {
            "facts": facts,
            "daily": daily_rows,
            "exclusions": sorted(excl, key=lambda e: (e["stream"], e["row_id"])),
        }

    def export(self, revision="latest"):
        if revision == "latest":
            revision = len(self.revisions)
        if revision < 1 or revision > len(self.revisions):
            raise RefError("unknown_revision", str(revision))
        return revision, self.revisions[revision - 1]
