#!/usr/bin/env python3
"""Verify host output against the independent reference transformer.

Checks, per run config:
  * success stream: row_ids strictly increasing; each row's output columns
    exactly equal the reference values; source record echoed correctly;
  * failure stream: row_ids strictly increasing; every failure row carries a
    non-empty cause; intake errors are reported as intake_error;
  * exactly-once: the union of both streams equals the expected row set
    (optionally restricted to a delivered prefix), with no duplicates;
  * rows of batches declared failed by the test appear exactly once, in the
    failure stream, with an execution-failure cause.

Usage:
    python3 tests/verify.py --config run.json [--expect-failed-batches 1,5]
                            [--through-batch K]
Exit 0 = PASS, 1 = FAIL.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from reference import expected_rows, profile_for_plugin  # noqa: E402


def _load_jsonl(path):
    rows = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--expect-failed-batches", default="")
    ap.add_argument("--through-batch", type=int, default=None)
    args = ap.parse_args()

    with open(args.config, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)
    base = os.path.dirname(os.path.abspath(args.config))

    def resolve(p):
        return p if os.path.isabs(p) else os.path.join(base, p)

    input_path = resolve(cfg["input"])
    success_path = resolve(cfg["success_out"])
    failure_path = resolve(cfg["failure_out"])
    batch_size = int(cfg["batch_size"])
    profile = profile_for_plugin(cfg["plugin"]["path"])

    failed_batches = set()
    if args.expect_failed_batches:
        failed_batches = {int(x) for x in args.expect_failed_batches.split(",")}

    # build expectations
    expected = {}  # row_id -> ("success", outdict, record) | ("failure", cause, record_or_None)
    n_intake_errors = 0
    for row_id, kind, payload in expected_rows(input_path, profile):
        if kind == "failure":
            expected[row_id] = ("failure", "intake_error")
            n_intake_errors += 1
        else:
            batch_id = row_id // batch_size
            if batch_id in failed_batches:
                expected[row_id] = ("failure", "execution")
            else:
                expected[row_id] = ("success", payload)

    if n_intake_errors and failed_batches:
        print("FAIL: verifier cannot combine intake errors with --expect-failed-batches "
              "(poison rows shift batch numbering)")
        return 1

    # attach source records
    records = {}
    with open(input_path, "r", encoding="utf-8") as fh:
        for row_id, line in enumerate(fh):
            try:
                rec = json.loads(line)
                if isinstance(rec, dict):
                    records[row_id] = rec
            except json.JSONDecodeError:
                pass

    if args.through_batch is not None:
        expected = {
            rid: exp
            for rid, exp in expected.items()
            if rid // batch_size < args.through_batch
        }

    success_rows = _load_jsonl(success_path) if os.path.exists(success_path) else []
    failure_rows = _load_jsonl(failure_path) if os.path.exists(failure_path) else []

    problems = []

    succ_ids = [r["row_id"] for r in success_rows]
    fail_ids = [r["row_id"] for r in failure_rows]
    if succ_ids != sorted(succ_ids):
        problems.append("success stream row_ids not increasing")
    if fail_ids != sorted(fail_ids):
        problems.append("failure stream row_ids not increasing")
    if len(set(succ_ids)) != len(succ_ids):
        problems.append("duplicate row_ids in success stream")
    if len(set(fail_ids)) != len(fail_ids):
        problems.append("duplicate row_ids in failure stream")
    overlap = set(succ_ids) & set(fail_ids)
    if overlap:
        problems.append(f"row_ids in BOTH streams: {sorted(overlap)[:5]}")

    for obj in success_rows:
        rid = obj["row_id"]
        exp = expected.get(rid)
        if exp is None:
            problems.append(f"row {rid}: unexpected success row (not in expected set)")
            continue
        if exp[0] != "success":
            problems.append(f"row {rid}: expected failure ({exp[1]}), found success")
            continue
        if obj.get("output") != exp[1]:
            problems.append(
                f"row {rid}: output mismatch: got {obj.get('output')} want {exp[1]}"
            )
        if obj.get("source") != records.get(rid):
            problems.append(f"row {rid}: source record altered")

    for obj in failure_rows:
        rid = obj["row_id"]
        exp = expected.get(rid)
        if exp is None:
            problems.append(f"row {rid}: unexpected failure row")
            continue
        if exp[0] != "failure":
            problems.append(f"row {rid}: expected success, found failure")
            continue
        err = obj.get("error") or {}
        cause = err.get("cause")
        if not isinstance(cause, str) or not cause:
            problems.append(f"row {rid}: failure row without cause")
            continue
        if exp[1] == "intake_error" and not cause.startswith("intake_error"):
            problems.append(f"row {rid}: expected intake_error, got {cause}")
        if exp[1] == "execution" and cause.startswith("intake_error"):
            problems.append(f"row {rid}: expected execution failure, got {cause}")
        if exp[1] == "intake_error" and "_raw" not in (obj.get("source") or {}):
            problems.append(f"row {rid}: intake error row should carry _raw source")
        if exp[1] == "execution" and obj.get("source") != records.get(rid):
            problems.append(f"row {rid}: failure row source record altered")

    got_ids = set(succ_ids) | set(fail_ids)
    want_ids = set(expected)
    missing = want_ids - got_ids
    extra = got_ids - want_ids
    if missing:
        problems.append(f"missing row_ids (first 5): {sorted(missing)[:5]} (total {len(missing)})")
    if extra:
        problems.append(f"extra row_ids (first 5): {sorted(extra)[:5]} (total {len(extra)})")

    if problems:
        print(f"FAIL ({len(problems)} problems)")
        for p in problems[:20]:
            print("  -", p)
        return 1
    print(
        f"PASS: {len(succ_ids)} success rows, {len(fail_ids)} failure rows, "
        f"{len(want_ids)} expected rows, exactly-once OK"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
