#!/usr/bin/env python3
"""Deterministic generator for warehouse demo data (cleaning products).

Generates raw intake JSONL, runs the real ``normalize_text`` cleaning plugin
through ``colbatch run``, and derives cancellation / station-assignment
change files.  Nothing here imports warehouse code: the generator only makes
inputs.

Profiles
--------
``slice``    the documented time-slice scenario: six 1-unit shipments at
             station A on one UTC day (01,02,04,06,08,10) plus one 2-unit
             shipment at station B, plus the assignment changes for the two
             revisions.
``history``  a batch whose cleaning run carries an extra shipment for an
             existing id at a new station (the move-and-edit batch).
Scale data is *not* stored in the repo: ``tests/test_warehouse.py
(scale_locality)`` generates it at runtime.

Usage:
    python3 scripts/gen_warehouse_demo.py --profile slice --outdir demo
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DAY = "2025-06-01"

# (raw_ts, raw_amount) of each raw intake row
SLICE_ROWS = [
    (f"{DAY} 01:00", "1"),    # A-1
    (f"{DAY} 02:00", "1"),    # A-2
    (f"{DAY} 04:00", "1"),    # A-4
    (f"{DAY} 06:00", "1"),    # A-6
    (f"{DAY} 08:00", "1"),    # A-8
    (f"{DAY} 10:00", "1"),    # A-10
    (f"{DAY} 09:00", "2"),    # B-9 (east)
]


def _write_raw(path, rows, station):
    with open(path, "w", encoding="utf-8") as fh:
        for i, (ts, amount) in enumerate(rows):
            fh.write(json.dumps({
                "shipment_id": f"{station}-{i}",
                "station_id": station,
                "raw_ts": ts,
                "raw_amount": amount,
            }) + "\n")


def _run_config(tmpdir: str, name: str, raw: str) -> str:
    """A colbatch run config whose columns pick up the warehouse fields."""
    cfg = {
        "input": raw,
        "success_out": os.path.join(tmpdir, f"{name}_success.jsonl"),
        "failure_out": os.path.join(tmpdir, f"{name}_failure.jsonl"),
        "log_dir": os.path.join(tmpdir, "logs"),
        "stats_path": os.path.join(tmpdir, "stats.json"),
        "manifest_path": os.path.join(tmpdir, "manifest.json"),
        "plugin": {"path": os.path.join(ROOT, "examples/warehouse_normalize.py")},
        "columns": [
            {"name": "shipment_id", "type": "utf8", "field": "shipment_id"},
            {"name": "station_id", "type": "utf8", "field": "station_id"},
            {"name": "raw_ts", "type": "utf8", "field": "raw_ts"},
            {"name": "raw_amount", "type": "utf8", "field": "raw_amount"},
        ],
        "batch_size": 4,
        "workers": 2,
        "max_attempts": 2,
        "batch_timeout_ms": 5000,
        "kill_grace_ms": 1000,
        "max_inflight_batches": 8,
        "max_pending_batches": 4,
        "max_pending_bytes": 67108864,
        "max_frame_bytes": 268435456,
        "writer_queue_batches": 4,
        "rlimits": {"cpu_seconds": 0, "as_bytes": 0, "nofile": 0},
    }
    path = os.path.join(tmpdir, f"{name}_run.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, indent=1)
    # The manifest/output paths in the config are relative to the config's
    # own directory; keep the run fully contained in tmpdir.
    env = dict(os.environ)
    py = sys.executable
    subprocess.run([py, "-m", "colbatch.cli", "run", "--config", path],
                   cwd=ROOT, check=True, env=env,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return path


def _batch_config(tmpdir, name, manifest, cancellations=None, assignments=None,
                  parent=0, batch_id=None, db=None, success=None, failure=None):
    cfg = {
        "db": db or os.path.join(tmpdir, "warehouse.sqlite"),
        "batch_id": batch_id or name,
        "expected_parent_version": parent,
        "cleaning_run": {"manifest": manifest},
        "workspace_dir": os.path.join(tmpdir, "work"),
        "receipt": os.path.join(tmpdir, f"receipt_{name}.json"),
    }
    if success:
        cfg["cleaning_run"]["success"] = success
    if failure:
        cfg["cleaning_run"]["failure"] = failure
    if cancellations:
        cfg["cancellations"] = cancellations
    if assignments:
        cfg["assignments"] = assignments
    path = os.path.join(tmpdir, f"{name}_batch.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, indent=1)
    return path


def gen_slice(outdir: str, py):
    os.makedirs(outdir, exist_ok=True)
    # batch 1: six A rows + one B row
    raw1 = os.path.join(outdir, "raw_v1.jsonl")
    _write_raw(raw1, SLICE_ROWS[:6], "A")
    with open(raw1, "a", encoding="utf-8") as fh:
        fh.write(json.dumps({
            "shipment_id": "B-0", "station_id": "B",
            "raw_ts": f"{DAY} 09:00", "raw_amount": "2"}) + "\n")
    _run_config(outdir, "v1", raw1)

    # batch 2: the same seven rows, re-cleaned (normalize is deterministic),
    # plus an assignment narrowing A-west and adding A-east.
    raw2 = os.path.join(outdir, "raw_v2.jsonl")
    _write_raw(raw2, SLICE_ROWS[:6], "A")
    with open(raw2, "a", encoding="utf-8") as fh:
        fh.write(json.dumps({
            "shipment_id": "B-0", "station_id": "B",
            "raw_ts": f"{DAY} 09:00", "raw_amount": "2"}) + "\n")
    _run_config(outdir, "v2", raw2)

    a1 = os.path.join(outdir, "assign_v1.jsonl")
    with open(a1, "w", encoding="utf-8") as fh:
        fh.write(json.dumps({
            "station_id": "A", "assignment_id": "A-west",
            "from": f"{DAY}T02:00:00Z", "to": f"{DAY}T08:00:00Z",
            "region": "west"}) + "\n")
        fh.write(json.dumps({
            "station_id": "B", "assignment_id": "B-west",
            "from": f"{DAY}T00:00:00Z", "to": None, "region": "west"}) + "\n")

    a2 = os.path.join(outdir, "assign_v2.jsonl")
    with open(a2, "w", encoding="utf-8") as fh:
        fh.write(json.dumps({
            "station_id": "A", "assignment_id": "A-west",
            "from": f"{DAY}T04:00:00Z", "to": f"{DAY}T06:00:00Z",
            "region": "west"}) + "\n")
        fh.write(json.dumps({
            "station_id": "A", "assignment_id": "A-east",
            "from": f"{DAY}T06:00:00Z", "to": f"{DAY}T10:00:00Z",
            "region": "east"}) + "\n")

    # The host writes one manifest per run; keep each run's products under
    # its own name and point the batch configs at them explicitly.
    import shutil
    v1_manifest = os.path.join(outdir, "manifest_v1.json")
    shutil.copyfile(os.path.join(outdir, "manifest.json"), v1_manifest)
    b1 = _batch_config(
        outdir, "b1", v1_manifest,
        success=os.path.join(outdir, "v1_success.jsonl"),
        failure=os.path.join(outdir, "v1_failure.jsonl"),
        assignments=a1, batch_id="slice-1", parent=0)
    v2_manifest = os.path.join(outdir, "manifest_v2.json")
    shutil.copyfile(os.path.join(outdir, "manifest.json"), v2_manifest)
    b2 = _batch_config(
        outdir, "b2", v2_manifest,
        success=os.path.join(outdir, "v2_success.jsonl"),
        failure=os.path.join(outdir, "v2_failure.jsonl"),
        assignments=a2, batch_id="slice-2", parent=1)
    print(json.dumps({"batch1": b1, "batch2": b2, "db": os.path.join(outdir, "warehouse.sqlite")}))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", choices=["slice"], required=True)
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()
    if args.profile != "slice":
        raise SystemExit(
            "scale data is generated at test time by tests/test_warehouse.py "
            "(scale_locality); see scripts/gen_input.py for raw inputs")
    gen_slice(args.outdir, sys.executable)


if __name__ == "__main__":
    main()
