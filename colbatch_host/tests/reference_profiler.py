#!/usr/bin/env python3
"""Independent reference profiler.

Deliberately does **not** import any product profiling code (snapshot /
profiler / quality rule evaluation).  It re-derives every statistic with
plain in-memory Python sets and dicts, so agreement with the product
implementation -- which keeps no per-row state in RAM -- is meaningful
cross-validation rather than a shared-code tautology.

The reference only covers rules and semantics documented for the product;
where a product rule has no reference counterpart the test harness skips
that comparison instead of weakening either side.

Usage (library):

    from reference_profiler import reference_profile, reference_evaluate
    stats = reference_profile(lines, columns, blank_mode, id_field)
    decision = reference_evaluate(rules, stats)
"""

from __future__ import annotations

import json
import math

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


def _is_int(v):
    return isinstance(v, int) and not isinstance(v, bool)


def _is_number(v):
    return (_is_int(v) or isinstance(v, float)) and not isinstance(v, bool)


def _blank(value: str, mode: str) -> bool:
    if mode == "off":
        return False
    if mode == "empty":
        return value == ""
    return value.strip() == ""


def ref_canonical_key(value) -> str:
    """Independent re-statement of the canonical key contract."""
    if isinstance(value, bool):
        return "b:" + ("true" if value else "false")
    if isinstance(value, int):
        return "i:" + str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return "f:nan"
        if value == 0.0:
            return "f:0.0"
        return "f:" + repr(value)
    if isinstance(value, str):
        return "s:" + value
    if value is None:
        return "n:null"
    return "j:" + json.dumps(value, ensure_ascii=False, sort_keys=True,
                              separators=(",", ":"), allow_nan=False)


def ref_parse_numeric(value, col_type):
    if col_type == "int64":
        if not _is_int(value) or not (INT64_MIN <= value <= INT64_MAX):
            return None, False
        return value, True
    if col_type == "float64":
        if isinstance(value, bool):
            return None, False
        if _is_int(value):
            return float(value), True
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                return None, False
            return value, True
        return None, False
    return None, False


def reference_profile(lines, columns, blank_mode="whitespace", id_field=None):
    """Profile an iterable of physical JSONL lines (strings, no newline).

    Returns a dict structurally comparable to the product report's
    profile-derived fields, using exact in-memory sets.
    """
    stats = {}
    for col in columns:
        stats[col["name"]] = {
            "field": col["field"],
            "type": col["type"],
            "missing_field": 0,
            "null": 0,
            "blank_string": 0,
            "present": 0,
            "distinct": set(),
            "parse_failures": 0,
            "num_values": [],
        }
    record_count = 0
    bad_lines = []
    id_keys = set()
    id_present = 0
    duplicates = 0

    for line_no, line in enumerate(lines, start=1):
        if not line.strip():
            bad_lines.append({"line": line_no, "reason": "empty line"})
            continue
        try:
            record = json.loads(line)
            if not isinstance(record, dict):
                raise ValueError("not an object")
        except (json.JSONDecodeError, ValueError):
            bad_lines.append({"line": line_no, "reason": "invalid JSON"})
            continue
        record_count += 1

        if id_field is not None and id_field in record and record[id_field] is not None:
            id_present += 1
            k = ref_canonical_key(record[id_field])
            if k in id_keys:
                duplicates += 1
            else:
                id_keys.add(k)

        for col in columns:
            agg = stats[col["name"]]
            field = col["field"]
            if field not in record:
                agg["missing_field"] += 1
                continue
            v = record[field]
            if v is None:
                agg["null"] += 1
                continue
            if isinstance(v, str) and _blank(v, blank_mode):
                agg["blank_string"] += 1
                continue
            agg["present"] += 1
            agg["distinct"].add(ref_canonical_key(v))
            if col["type"] in ("int64", "float64"):
                num, ok = ref_parse_numeric(v, col["type"])
                if not ok:
                    agg["parse_failures"] += 1
                else:
                    agg["num_values"].append(num)

    columns_out = {}
    for col in columns:
        agg = stats[col["name"]]
        missing = agg["missing_field"] + agg["null"] + agg["blank_string"]
        entry = {
            "present": agg["present"],
            "missing_total": missing,
            "missing_field": agg["missing_field"],
            "null": agg["null"],
            "blank_string": agg["blank_string"],
            "missing_ratio": (missing / record_count) if record_count else 0.0,
            "distinct": len(agg["distinct"]),
            "distinct_ratio": (len(agg["distinct"]) / record_count) if record_count else 0.0,
            "parse_failures": agg["parse_failures"],
        }
        if col["type"] in ("int64", "float64"):
            entry["numeric_min"] = min(agg["num_values"]) if agg["num_values"] else None
            entry["numeric_max"] = max(agg["num_values"]) if agg["num_values"] else None
            entry["numeric_count"] = len(agg["num_values"])
        columns_out[col["name"]] = entry

    return {
        "record_count": record_count,
        "bad_line_count": len(bad_lines),
        "bad_lines": bad_lines,
        "columns": columns_out,
        "id": (
            {
                "field": id_field,
                "present": id_present,
                "distinct": len(id_keys),
                "duplicates": duplicates,
            }
            if id_field is not None
            else None
        ),
        # kept for allowed_values evaluation below
        "_distinct_sets": {n: a["distinct"] for n, a in stats.items()},
    }


def reference_evaluate(rules, profile):
    """Independent rule evaluation; returns (admitted, per-rule results)."""
    results = []
    for rule in rules:
        kind = rule["rule"]
        col = profile["columns"].get(rule.get("column"))
        n = profile["record_count"]
        passed = True
        actual = None
        reason = ""
        if kind == "max_missing_ratio":
            actual = col["missing_ratio"]
            if actual > rule["value"]:
                passed, reason = False, "missing"
        elif kind == "min_distinct":
            actual = col["distinct"]
            if actual < rule["value"]:
                passed, reason = False, "distinct low"
        elif kind == "max_distinct":
            actual = col["distinct"]
            if actual > rule["value"]:
                passed, reason = False, "distinct high"
        elif kind == "min_distinct_ratio":
            actual = col["distinct_ratio"]
            if actual < rule["value"]:
                passed, reason = False, "ratio low"
        elif kind == "max_distinct_ratio":
            actual = col["distinct_ratio"]
            if actual > rule["value"]:
                passed, reason = False, "ratio high"
        elif kind == "numeric_min":
            if col["parse_failures"] or col["numeric_count"] == 0:
                passed, reason = False, "numeric unparseable/empty"
            else:
                actual = col["numeric_min"]
                if actual < rule["value"]:
                    passed, reason = False, "min"
        elif kind == "numeric_max":
            if col["parse_failures"] or col["numeric_count"] == 0:
                passed, reason = False, "numeric unparseable/empty"
            else:
                actual = col["numeric_max"]
                if actual > rule["value"]:
                    passed, reason = False, "max"
        elif kind == "allowed_values":
            allowed = {ref_canonical_key(v) for v in rule["values"]}
            violating = profile["_distinct_sets"][rule["column"]] - allowed
            actual = len(violating)
            if actual:
                passed, reason = False, "not allowed"
        elif kind == "max_duplicate_ids":
            actual = profile["id"]["duplicates"] if profile["id"] else 0
            if actual > rule["value"]:
                passed, reason = False, "duplicate id"
        else:
            raise ValueError(f"reference has no rule {kind!r}")
        results.append({"id": rule["id"], "passed": passed, "actual": actual,
                        "reason": reason})
    return all(r["passed"] for r in results), results
