#!/usr/bin/env python3
"""End-to-end demo of the delivery-attribution warehouse.

Runs the real ``colbatch run`` cleaning path, then publishes two revisions and
exports them, showing the documented time slice:

  * revision 1: station A [02,08) -> west; B -> west
    => west 4 shipments / 500 cents, unattributed 3 / 300
  * revision 2: A [04,06) -> west, A [06,10) -> east
    => west 2 / 300, east 2 / 200, unattributed 3 / 300

The script then re-exports revision 1 to prove historical reads are stable.
It is intentionally tiny: the scale scenario lives in
``tests/test_warehouse.py::scale_locality`` and generates its data at runtime.

Usage:
    python3 scripts/warehouse_demo.py --outdir out/warehouse_demo
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DAY = "2025-06-01"
sys.path.insert(0, ROOT)

PY = os.environ.get("COLBATCH_PYTHON", sys.executable)


def rows_v1():
    return [
        {"shipment_id": "A-01", "station_id": "A", "raw_ts": f"{DAY} 01:00", "raw_amount": "1"},
        {"shipment_id": "A-02", "station_id": "A", "raw_ts": f"{DAY} 02:00", "raw_amount": "1"},
        {"shipment_id": "A-04", "station_id": "A", "raw_ts": f"{DAY} 04:00", "raw_amount": "1"},
        {"shipment_id": "A-06", "station_id": "A", "raw_ts": f"{DAY} 06:00", "raw_amount": "1"},
        {"shipment_id": "A-08", "station_id": "A", "raw_ts": f"{DAY} 08:00", "raw_amount": "1"},
        {"shipment_id": "A-10", "station_id": "A", "raw_ts": f"{DAY} 10:00", "raw_amount": "1"},
        {"shipment_id": "B-09", "station_id": "B", "raw_ts": f"{DAY} 09:00", "raw_amount": "2"},
    ]


def write_jsonl(path, records):
    with open(path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")


def run(cmd):
    proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout + proc.stderr)
        raise SystemExit(f"command failed ({proc.returncode}): {' '.join(cmd)}")
    return proc


def clean(outdir, tag, rows):
    raw = os.path.join(outdir, f"{tag}_raw.jsonl")
    write_jsonl(raw, rows)
    cfg = {
        "input": raw,
        "success_out": os.path.join(outdir, f"{tag}_success.jsonl"),
        "failure_out": os.path.join(outdir, f"{tag}_failure.jsonl"),
        "log_dir": os.path.join(outdir, f"{tag}_logs"),
        "stats_path": os.path.join(outdir, f"{tag}_stats.json"),
        "manifest_path": os.path.join(outdir, f"{tag}_manifest.json"),
        "plugin": {"path": os.path.join(ROOT, "examples/warehouse_normalize.py")},
        "columns": [
            {"name": "shipment_id", "type": "utf8", "field": "shipment_id"},
            {"name": "station_id", "type": "utf8", "field": "station_id"},
            {"name": "raw_ts", "type": "utf8", "field": "raw_ts"},
            {"name": "raw_amount", "type": "utf8", "field": "raw_amount"},
        ],
        "batch_size": 4, "workers": 2, "max_attempts": 2,
        "batch_timeout_ms": 5000, "kill_grace_ms": 1000,
        "max_inflight_batches": 8, "max_pending_batches": 4,
        "max_pending_bytes": 67108864, "max_frame_bytes": 268435456,
        "writer_queue_batches": 4,
        "rlimits": {"cpu_seconds": 0, "as_bytes": 0, "nofile": 0},
    }
    path = os.path.join(outdir, f"{tag}_run.json")
    json.dump(cfg, open(path, "w"), indent=1)
    run([PY, "-m", "colbatch.cli", "run", "--config", path])
    return {"manifest": cfg["manifest_path"], "success": cfg["success_out"],
            "failure": cfg["failure_out"]}


def batch(outdir, tag, run_out, parent, batch_id, assignments=None):
    cfg = {
        "db": os.path.join(outdir, "warehouse.sqlite"),
        "batch_id": batch_id,
        "expected_parent_version": parent,
        "cleaning_run": {"manifest": run_out["manifest"],
                         "success": run_out["success"],
                         "failure": run_out["failure"]},
        "workspace_dir": os.path.join(outdir, "work"),
        "receipt": os.path.join(outdir, f"receipt_{tag}.json"),
    }
    if assignments:
        cfg["assignments"] = assignments
    path = os.path.join(outdir, f"{tag}_batch.json")
    json.dump(cfg, open(path, "w"), indent=1)
    proc = run([PY, "-m", "colbatch.cli", "warehouse-apply", "--config", path])
    return json.loads(proc.stdout)


def export(outdir, revision):
    proc = run([PY, "-m", "colbatch.cli", "warehouse-export",
                "--db", os.path.join(outdir, "warehouse.sqlite"),
                "--revision", str(revision)])
    header = None
    daily = []
    for line in proc.stdout.splitlines():
        rec = json.loads(line)
        if rec["type"] == "header":
            header = rec
        elif rec["type"] == "daily":
            daily.append(rec)
    return header, daily


def show(tag, header, daily):
    print(f"\n== {tag}: revision {header['revision']} "
          f"(parent {header['parent_revision']}, {header['facts']} facts)")
    for d in daily:
        label = d["region"] if d["region"] is not None else "<unattributed>"
        print(f"   {d['utc_date']}  {label:<15} {d['shipments']} shipments "
              f"{d['amount_cents']} cents")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default=os.path.join(ROOT, "out", "warehouse_demo"))
    args = ap.parse_args()
    outdir = os.path.abspath(args.outdir)
    if os.path.isdir(outdir):
        shutil.rmtree(outdir)
    os.makedirs(outdir, exist_ok=True)

    v1 = clean(outdir, "v1", rows_v1())
    v2 = clean(outdir, "v2", rows_v1())

    a1 = os.path.join(outdir, "assign_v1.jsonl")
    write_jsonl(a1, [
        {"station_id": "A", "assignment_id": "A-west",
         "from": f"{DAY}T02:00:00Z", "to": f"{DAY}T08:00:00Z", "region": "west"},
        {"station_id": "B", "assignment_id": "B-west",
         "from": f"{DAY}T00:00:00Z", "to": None, "region": "west"},
    ])
    a2 = os.path.join(outdir, "assign_v2.jsonl")
    write_jsonl(a2, [
        {"station_id": "A", "assignment_id": "A-west",
         "from": f"{DAY}T04:00:00Z", "to": f"{DAY}T06:00:00Z", "region": "west"},
        {"station_id": "A", "assignment_id": "A-east",
         "from": f"{DAY}T06:00:00Z", "to": f"{DAY}T10:00:00Z", "region": "east"},
    ])

    r1 = batch(outdir, "b1", v1, 0, "demo-1", a1)
    show("after batch 1", *export(outdir, 1))
    print("   rejudged history facts:", r1["metrics"]["rejudged_history_facts"])

    r2 = batch(outdir, "b2", v2, 1, "demo-2", a2)
    show("after batch 2", *export(outdir, 2))
    m = r2["metrics"]
    print(f"   rejudged history facts: {m['rejudged_history_facts']}, "
          f"moved facts: {m['moved_facts']}, "
          f"daily keys changed: {m['daily_keys_changed']}")

    # Historical re-export *after* revision 2 was published.
    show("re-export of revision 1 (after rev 2 published)",
         *export(outdir, 1))

    print("\nartifacts:", outdir)
    print("  warehouse.sqlite, receipt_b1.json, receipt_b2.json, "
          "v1_*/v2_* cleaning products")


if __name__ == "__main__":
    main()
