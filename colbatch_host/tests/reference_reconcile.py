#!/usr/bin/env python3
"""Independent reference reconciler for ``colbatch compare``.

Deliberately does **not** import any product code (colbatch.compare and its
pairing/diff machinery).  It re-states the documented contract -- identity
pairing by row_id, the six-way JSON type lattice, JSON-Pointer field paths,
the non-business error key set -- with plain in-memory Python, so agreement
with the bounded-memory streaming product implementation is meaningful
cross-validation rather than a shared-code tautology.

Usage (library):

    from reference_reconcile import reference_reconcile
    result = reference_reconcile(base_success, base_failure,
                                 cand_success, cand_failure)

Each argument is a list of parsed stream records (dicts).  Returns
``{"detail": [...], "summary": {...}}`` where ``detail`` is the expected
content of the product's changes.jsonl (one entry per *changed* row, in
ascending row_id order) and ``summary`` matches the product report's
summary section.
"""

from __future__ import annotations

import json

# Restatement of the documented non-business error keys (runner mechanics:
# batch bookkeeping, attempt history, process/host identity, wall-clock
# fields, scratch paths).  Everything else in an error object is business
# evidence.
NON_BUSINESS_ERROR_KEYS = frozenset(
    {
        "batch_id",
        "attempts",
        "log",
        "pid",
        "worker_pid",
        "process_id",
        "slot",
        "worker_slot",
        "generation",
        "worker_generation",
        "timestamp",
        "ts",
        "time",
        "elapsed_ms",
        "duration_ms",
        "started_at",
        "finished_at",
        "tmp_path",
        "temp_path",
        "temp_dir",
        "tmp_dir",
        "hostname",
        "host",
        "machine",
    }
)


def ref_type_tag(value) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    return "object"


def ref_json_equal(a, b) -> bool:
    ta = ref_type_tag(a)
    if ta != ref_type_tag(b):
        return False
    if ta in ("null", "boolean", "string", "number"):
        return a == b
    if ta == "array":
        if len(a) != len(b):
            return False
        return all(ref_json_equal(x, y) for x, y in zip(a, b))
    if set(a.keys()) != set(b.keys()):
        return False
    return all(ref_json_equal(a[k], b[k]) for k in a)


def _esc(segment: str) -> str:
    return segment.replace("~", "~0").replace("/", "~1")


def _unesc(segment: str) -> str:
    return segment.replace("~1", "/").replace("~0", "~")


def ref_diff(base, cand, path, out):
    """Independent restatement of the stable-path diff contract."""
    if ref_json_equal(base, cand):
        return
    ta = ref_type_tag(base)
    tb = ref_type_tag(cand)
    if ta == "object" and tb == "object":
        for key in sorted(set(base) | set(cand)):
            p = path + "/" + _esc(key)
            if key in base and key in cand:
                ref_diff(base[key], cand[key], p, out)
            elif key in base:
                out.append({"path": p, "base": base[key]})
            else:
                out.append({"path": p, "candidate": cand[key]})
        return
    if ta == "array" and tb == "array":
        for i in range(max(len(base), len(cand))):
            p = f"{path}/{i}"
            if i < len(base) and i < len(cand):
                ref_diff(base[i], cand[i], p, out)
            elif i < len(base):
                out.append({"path": p, "base": base[i]})
            else:
                out.append({"path": p, "candidate": cand[i]})
        return
    out.append({"path": path, "base": base, "candidate": cand})


def ref_normalize_error(error: dict) -> dict:
    return {k: v for k, v in error.items() if k not in NON_BUSINESS_ERROR_KEYS}


def _index_stream(success_records, failure_records, side):
    """Pair each row_id with its single delivered record; the reference
    asserts exactly-once coverage by construction (dict uniqueness)."""
    index = {}
    for rec in success_records:
        rid = rec["row_id"]
        assert rid not in index, f"{side}: duplicate row_id {rid}"
        index[rid] = ("success", rec)
    for rec in failure_records:
        rid = rec["row_id"]
        assert rid not in index, f"{side}: row_id {rid} in both streams"
        index[rid] = ("failure", rec)
    return index


def reference_reconcile(base_success, base_failure, cand_success, cand_failure):
    """Reconcile two runs held fully in memory; returns the expected detail
    entries (changed rows only, ascending row_id) and the summary section."""
    base = _index_stream(base_success, base_failure, "baseline")
    cand = _index_stream(cand_success, cand_failure, "candidate")
    assert set(base) == set(cand), "coverage sets differ between runs"

    counts = {
        "success_fields_changed": 0,
        "success_to_failure": 0,
        "failure_to_success": 0,
        "failure_reason_changed": 0,
    }
    unchanged = 0
    changed_fields = {}
    detail = []

    for rid in sorted(base):
        b_kind, b_rec = base[rid]
        c_kind, c_rec = cand[rid]
        assert ref_json_equal(b_rec["source"], c_rec["source"]), (
            f"row_id {rid}: source records differ"
        )
        if b_kind == "success" and c_kind == "success":
            diffs = []
            ref_diff(b_rec["output"], c_rec["output"], "", diffs)
            if not diffs:
                unchanged += 1
                continue
            counts["success_fields_changed"] += 1
            # per-field summary counts changed rows, not diff entries
            for top in {_unesc(e["path"].split("/", 2)[1]) for e in diffs}:
                changed_fields[top] = changed_fields.get(top, 0) + 1
            detail.append(
                {"row_id": rid, "class": "success_fields_changed",
                 "fields": diffs}
            )
        elif b_kind == "success" and c_kind == "failure":
            counts["success_to_failure"] += 1
            detail.append(
                {"row_id": rid, "class": "success_to_failure",
                 "error": ref_normalize_error(c_rec["error"])}
            )
        elif b_kind == "failure" and c_kind == "success":
            counts["failure_to_success"] += 1
            detail.append(
                {"row_id": rid, "class": "failure_to_success",
                 "error": ref_normalize_error(b_rec["error"])}
            )
        else:
            b_err = ref_normalize_error(b_rec["error"])
            c_err = ref_normalize_error(c_rec["error"])
            if ref_json_equal(b_err, c_err):
                unchanged += 1
                continue
            counts["failure_reason_changed"] += 1
            detail.append(
                {"row_id": rid, "class": "failure_reason_changed",
                 "error": {"base": b_err, "candidate": c_err}}
            )

    summary = {
        "rows_compared": len(base),
        "unchanged": unchanged,
        "changed_rows": sum(counts.values()),
        "success_fields_changed": counts["success_fields_changed"],
        "success_to_failure": counts["success_to_failure"],
        "failure_to_success": counts["failure_to_success"],
        "failure_reason_changed": counts["failure_reason_changed"],
        "changed_fields": {k: changed_fields[k] for k in sorted(changed_fields)},
    }
    return {"detail": detail, "summary": summary}


def load_stream(path):
    """Test helper: parse a JSONL stream file into a record list."""
    records = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if line.strip():
                records.append(json.loads(line))
    return records
