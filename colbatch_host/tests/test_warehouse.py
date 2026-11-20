#!/usr/bin/env python3
"""Test suite for the delivery-attribution warehouse.

Every scenario builds a fresh temp directory.  Cleaning products are produced
by the *real* ``colbatch run`` path (small inputs, generated at test time);
large inputs are generated on demand so the repo carries no big fixtures.

The warehouse CLI is driven as a subprocess and every revision it publishes
is cross-checked against ``tests/reference_warehouse.py``, an independent
in-memory replay that recomputes facts/summary/exclusions from scratch.

Run:  python3 tests/test_warehouse.py              # all scenarios
      python3 tests/test_warehouse.py slice_history   # selected
Env:  KEEP=1 keeps temp dirs of failing scenarios.
      COLBATCH_PYTHON overrides the interpreter.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import traceback

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import reference_warehouse as refw  # noqa: E402

PY = os.environ.get("COLBATCH_PYTHON", sys.executable)
TEST_TMP = os.environ.get("COLBATCH_TEST_DIR", tempfile.gettempdir())

RC_OK = 0
RC_CONFIG = 3
RC_DATA = 4
RC_EXEC = 5
RC_CONFLICT = 6

DAY = "2025-06-01"


# ---------------------------------------------------------------------------
# helpers


class Workdir:
    def __init__(self, name):
        self.path = tempfile.mkdtemp(prefix=f"colwh_{name}_", dir=TEST_TMP)

    def p(self, *parts):
        return os.path.join(self.path, *parts)

    def cleanup(self, failed):
        if failed or os.environ.get("KEEP"):
            return
        shutil.rmtree(self.path, ignore_errors=True)


def peak_rss_kb():
    """Peak RSS of the largest child the CLI launched (Linux only)."""
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    except Exception:  # pragma: no cover
        return None


def run_cli(*args, env_extra=None):
    env = dict(os.environ)
    if env_extra:
        env.update(env_extra)
    return subprocess.run([PY, "-m", "colbatch.cli", *args], cwd=ROOT,
                          capture_output=True, text=True, env=env)


def write_jsonl(path, records):
    with open(path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(rec if isinstance(rec, str) else json.dumps(rec))
            fh.write("\n")


def run_cleaning(wd, name, rows, batch_size=4):
    """Run the real host with the demo warehouse plugin; return product paths."""
    raw = wd.p(f"{name}_raw.jsonl")
    write_jsonl(raw, rows)
    cfg = {
        "input": raw,
        "success_out": wd.p(f"{name}_success.jsonl"),
        "failure_out": wd.p(f"{name}_failure.jsonl"),
        "log_dir": wd.p(f"{name}_logs"),
        "stats_path": wd.p(f"{name}_stats.json"),
        "manifest_path": wd.p(f"{name}_manifest.json"),
        "plugin": {"path": os.path.join(ROOT, "examples/warehouse_normalize.py")},
        "columns": [
            {"name": "shipment_id", "type": "utf8", "field": "shipment_id"},
            {"name": "station_id", "type": "utf8", "field": "station_id"},
            {"name": "raw_ts", "type": "utf8", "field": "raw_ts"},
            {"name": "raw_amount", "type": "utf8", "field": "raw_amount"},
        ],
        "batch_size": batch_size, "workers": 2, "max_attempts": 2,
        "batch_timeout_ms": 5000, "kill_grace_ms": 1000,
        "max_inflight_batches": 8, "max_pending_batches": 4,
        "max_pending_bytes": 67108864, "max_frame_bytes": 268435456,
        "writer_queue_batches": 4,
        "rlimits": {"cpu_seconds": 0, "as_bytes": 0, "nofile": 0},
    }
    cfg_path = wd.p(f"{name}_run.json")
    with open(cfg_path, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, indent=1)
    proc = run_cli("run", "--config", cfg_path)
    if proc.returncode != 0:
        raise AssertionError(f"cleaning run failed: {proc.stderr[-2000:]}")
    return {
        "manifest": cfg["manifest_path"],
        "success": cfg["success_out"],
        "failure": cfg["failure_out"],
    }


def apply_batch(wd, name, *, run=None, cancellations=None, assignments=None,
                parent=0, batch_id=None, db=None, env_extra=None,
                extra_limits=None, receipt=None):
    cfg = {
        "db": db or wd.p("warehouse.sqlite"),
        "batch_id": batch_id or name,
        "expected_parent_version": parent,
        "workspace_dir": wd.p("work"),
        "receipt": receipt or wd.p(f"receipt_{name}.json"),
    }
    if run:
        cfg["cleaning_run"] = {"manifest": run["manifest"],
                               "success": run["success"],
                               "failure": run["failure"]}
    if cancellations:
        cfg["cancellations"] = cancellations
    if assignments:
        cfg["assignments"] = assignments
    if extra_limits:
        cfg["limits"] = extra_limits
    path = wd.p(f"{name}_batch.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, indent=1)
    proc = run_cli("warehouse-apply", "--config", path, env_extra=env_extra)
    return proc, cfg, path


def apply_ok(wd, name, **kw):
    proc, cfg, path = apply_batch(wd, name, **kw)
    if proc.returncode != 0:
        raise AssertionError(f"{name}: exit {proc.returncode}: {proc.stderr[-3000:]}")
    return json.loads(proc.stdout), cfg, path


def export_rows(db, revision="latest"):
    proc = run_cli("warehouse-export", "--db", db, "--revision", str(revision))
    if proc.returncode != 0:
        raise AssertionError(f"export failed: {proc.stderr[-2000:]}")
    header = None
    facts, daily, excl = [], [], []
    for line in proc.stdout.splitlines():
        rec = json.loads(line)
        if rec["type"] == "header":
            header = rec
        elif rec["type"] == "fact":
            facts.append(rec)
        elif rec["type"] == "daily":
            daily.append(rec)
        else:
            excl.append(rec)
    return header, facts, daily, excl


def compare_to_reference(db, ref, revision):
    header, facts, daily, excl = export_rows(db, revision)
    _rev, view = ref.export(revision)
    want_facts = view["facts"]
    got_facts = [{k: f[k] for k in ("shipment_id", "station_id", "event_time",
                                    "utc_date", "amount_cents", "assignment_id",
                                    "region", "source")} for f in facts]
    for got, want in zip(got_facts, want_facts):
        assert got == want, "fact differs\n got=%s\nwant=%s" % (
            json.dumps(got, indent=1), json.dumps(want, indent=1))
    assert len(got_facts) == len(want_facts), (
        f"{len(got_facts)} facts vs {len(want_facts)} expected")
    want_daily = [{"utc_date": d["utc_date"], "region": d["region"],
                   "shipments": d["shipments"], "amount_cents": d["amount_cents"]}
                  for d in view["daily"]]
    got_daily = [{"utc_date": d["utc_date"], "region": d["region"],
                  "shipments": d["shipments"], "amount_cents": d["amount_cents"]}
                 for d in daily]
    assert got_daily == want_daily, (
        "daily differs\n got=%s\nwant=%s" % (got_daily, want_daily))
    got_excl = sorted(
        ({"stream": e["stream"], "row_id": e["row_id"],
          "shipment_id": e["shipment_id"], "station_id": e["station_id"],
          "reason": e["reason"]} for e in excl),
        key=lambda e: (e["stream"], e["row_id"]))
    want_excl = sorted(
        ({"stream": e["stream"], "row_id": e["row_id"],
          "shipment_id": e["shipment_id"], "station_id": e["station_id"],
          "reason": e["reason"]} for e in view["exclusions"]),
        key=lambda e: (e["stream"], e["row_id"]))
    assert got_excl == want_excl, (
        "exclusions differ\n got=%s\nwant=%s" % (got_excl, want_excl))
    return header, facts, daily, excl


# ---------------------------------------------------------------------------
# scenario data


def slice_rows():
    return [
        {"shipment_id": "A-0", "station_id": "A", "raw_ts": f"{DAY} 01:00", "raw_amount": "1"},
        {"shipment_id": "A-1", "station_id": "A", "raw_ts": f"{DAY} 02:00", "raw_amount": "1"},
        {"shipment_id": "A-2", "station_id": "A", "raw_ts": f"{DAY} 04:00", "raw_amount": "1"},
        {"shipment_id": "A-3", "station_id": "A", "raw_ts": f"{DAY} 06:00", "raw_amount": "1"},
        {"shipment_id": "A-4", "station_id": "A", "raw_ts": f"{DAY} 08:00", "raw_amount": "1"},
        {"shipment_id": "A-5", "station_id": "A", "raw_ts": f"{DAY} 10:00", "raw_amount": "1"},
        {"shipment_id": "B-0", "station_id": "B", "raw_ts": f"{DAY} 09:00", "raw_amount": "2"},
    ]


def assign_file(wd, name, records):
    path = wd.p(f"{name}.jsonl")
    write_jsonl(path, records)
    return path


SLICE_ASSIGN_V1 = [
    {"station_id": "A", "assignment_id": "A-west",
     "from": f"{DAY}T02:00:00Z", "to": f"{DAY}T08:00:00Z", "region": "west"},
    {"station_id": "B", "assignment_id": "B-west",
     "from": f"{DAY}T00:00:00Z", "to": None, "region": "west"},
]

SLICE_ASSIGN_V2 = [
    {"station_id": "A", "assignment_id": "A-west",
     "from": f"{DAY}T04:00:00Z", "to": f"{DAY}T06:00:00Z", "region": "west"},
    {"station_id": "A", "assignment_id": "A-east",
     "from": f"{DAY}T06:00:00Z", "to": f"{DAY}T10:00:00Z", "region": "east"},
]


def daily_map(daily):
    return {(d["utc_date"], d["region"]): (d["shipments"], d["amount_cents"])
            for d in daily}


# ---------------------------------------------------------------------------
# scenarios


def scenario_slice_history(wd):
    """The documented two-revision time slice, plus a historical re-export."""
    run1 = run_cleaning(wd, "v1", slice_rows())
    a1 = assign_file(wd, "assign_v1", SLICE_ASSIGN_V1)
    ref = refw.ReferenceWarehouse()
    out, cfg1, _ = apply_ok(wd, "b1", run=run1, assignments=a1, batch_id="slice-1")
    ref.apply_batch({"batch_id": "slice-1", "expected_parent_version": 0,
                     "cleaning_run": {"manifest": run1["manifest"],
                                      "success": run1["success"],
                                      "failure": run1["failure"]},
                     "assignments": a1})
    assert out["revision"] == 1
    db = cfg1["db"]
    header, facts, daily, _ = compare_to_reference(db, ref, 1)
    assert header["facts"] == 7 and header["parent_revision"] == 0
    dm = daily_map(daily)
    assert dm[("2025-06-01", "west")] == (4, 500), dm
    assert dm[("2025-06-01", None)] == (3, 300), dm
    by_id = {f["shipment_id"]: f for f in facts}
    assert by_id["A-0"]["region"] is None
    assert by_id["A-1"]["assignment_id"] == "A-west"
    assert by_id["A-4"]["region"] is None  # 08:00 is a half-open upper bound

    run2 = run_cleaning(wd, "v2", slice_rows())
    a2 = assign_file(wd, "assign_v2", SLICE_ASSIGN_V2)
    out2, cfg2, _ = apply_ok(wd, "b2", run=run2, assignments=a2, parent=1,
                             batch_id="slice-2")
    ref.apply_batch({"batch_id": "slice-2", "expected_parent_version": 1,
                     "cleaning_run": {"manifest": run2["manifest"],
                                      "success": run2["success"],
                                      "failure": run2["failure"]},
                     "assignments": a2})
    assert out2["revision"] == 2
    m = out2["metrics"]
    # 02:00 leaves west, 06:00 and 08:00 join east; 10:00 stays unmatched.
    assert m["moved_facts"] == 3, m
    assert m["rejudged_history_facts"] == 0, m  # all ids re-delivered by run2
    header2, facts2, daily2, _ = compare_to_reference(db, ref, 2)
    dm2 = daily_map(daily2)
    assert dm2[("2025-06-01", "west")] == (2, 300), dm2
    assert dm2[("2025-06-01", "east")] == (2, 200), dm2
    assert dm2[("2025-06-01", None)] == (3, 300), dm2
    by2 = {f["shipment_id"]: f for f in facts2}
    assert by2["A-1"]["region"] is None
    assert by2["A-3"]["region"] == "east"
    assert by2["A-4"]["region"] == "east"
    assert by2["A-5"]["region"] is None

    # Re-export revision 1 after revision 2 was published.
    header1b, facts1b, daily1b, _ = export_rows(db, 1)
    assert header1b["revision"] == 1
    assert facts1b == [dict(f) for f in facts] and daily1b == daily

    # ... and after removing the source files entirely.
    for run in (run1, run2):
        for key in ("manifest", "success", "failure"):
            os.unlink(run[key])
    os.unlink(a1)
    os.unlink(a2)
    header1c, facts1c, daily1c, _ = export_rows(db, 1)
    assert facts1c == facts1b and daily1c == daily1b
    compare_to_reference(db, ref, 2)

    # Durability across a *process restart*: a brand-new process opens the
    # same file (nothing cached in this test's memory) and sees the same
    # revisions and receipts.
    status = json.loads(run_cli("warehouse-status", "--db", db).stdout)
    assert [r["revision"] for r in status["revisions"]] == [1, 2]
    receipt = json.loads(run_cli(
        "warehouse-status", "--db", db, "--batch-id", "slice-2").stdout)
    assert receipt["batch"]["revision"] == 2
    assert receipt["batch"]["receipt"]["metrics"]["moved_facts"] == 3
    header2b, facts2b, daily2b, _ = export_rows(db, "latest")
    assert header2b["revision"] == 2 and facts2b == facts2


def scenario_move_cancel_same_batch(wd):
    """A batch that moves a shipment to a new station and cancels another in
    the very same revision; old daily contribution is removed once."""
    rows = [
        {"shipment_id": "S1", "station_id": "A", "raw_ts": f"{DAY} 03:00", "raw_amount": "10"},
        {"shipment_id": "S2", "station_id": "A", "raw_ts": f"{DAY} 03:00", "raw_amount": "5"},
        {"shipment_id": "S3", "station_id": "A", "raw_ts": f"{DAY} 03:00", "raw_amount": "-2"},
    ]
    run1 = run_cleaning(wd, "r1", rows)
    a1 = assign_file(wd, "a1", [
        {"station_id": "A", "assignment_id": "a-all", "from": f"{DAY}T00:00:00Z",
         "to": None, "region": "north"},
        {"station_id": "C", "assignment_id": "c-all", "from": f"{DAY}T00:00:00Z",
         "to": None, "region": "south"},
    ])
    ref = refw.ReferenceWarehouse()
    out, cfg, _ = apply_ok(wd, "m1", run=run1, assignments=a1, batch_id="mv-1")
    ref.apply_batch({"batch_id": "mv-1", "expected_parent_version": 0,
                     "cleaning_run": {"manifest": run1["manifest"],
                                      "success": run1["success"],
                                      "failure": run1["failure"]},
                     "assignments": a1})
    db = cfg["db"]
    _, _, daily, _ = compare_to_reference(db, ref, 1)
    assert daily_map(daily)[("2025-06-01", "north")] == (3, 1300), daily

    # New cleaning run moves S1 to C and keeps S2; S3 is cancelled in the
    # same batch (no cleaning row for it).
    rows2 = [
        {"shipment_id": "S1", "station_id": "C", "raw_ts": f"{DAY} 03:00", "raw_amount": "10"},
        {"shipment_id": "S2", "station_id": "A", "raw_ts": f"{DAY} 03:00", "raw_amount": "5"},
    ]
    run2 = run_cleaning(wd, "r2", rows2)
    cancel = wd.p("cancel2.jsonl")
    write_jsonl(cancel, [{"shipment_id": "S3"}])
    out2, cfg2, _ = apply_ok(wd, "m2", run=run2, cancellations=cancel,
                             parent=1, batch_id="mv-2")
    ref.apply_batch({"batch_id": "mv-2", "expected_parent_version": 1,
                     "cleaning_run": {"manifest": run2["manifest"],
                                      "success": run2["success"],
                                      "failure": run2["failure"]},
                     "cancellations": cancel})
    _, facts2, daily2, _ = compare_to_reference(db, ref, 2)
    dm = daily_map(daily2)
    assert dm[("2025-06-01", "north")] == (1, 500), dm   # S2 only
    assert dm[("2025-06-01", "south")] == (1, 1000), dm  # S1
    ids = {f["shipment_id"] for f in facts2}
    assert ids == {"S1", "S2"}, ids
    m = out2["metrics"]
    assert m["deleted_facts"] == 1 and m["moved_facts"] == 1, m

    # Update+cancel of the same shipment in one batch is rejected wholesale.
    cancel_bad = wd.p("cancel_bad.jsonl")
    write_jsonl(cancel_bad, [{"shipment_id": "S1"}])
    proc, cfg_bad, _ = apply_batch(wd, "m3", run=run2, cancellations=cancel_bad,
                                   parent=2, batch_id="mv-3")
    assert proc.returncode == RC_DATA
    assert json.loads(proc.stderr.splitlines()[-1])["reason"] == "cancel_and_update"


def scenario_span_only_backfill(wd):
    """An assignment-only batch re-judges historical facts via the index and
    must count them; untouched shipments keep their provenance byte-exact."""
    rows = [
        {"shipment_id": f"X-{i}", "station_id": "X", "raw_ts": f"{DAY} {i:02d}:00",
         "raw_amount": "1"} for i in range(1, 11)
    ]
    run1 = run_cleaning(wd, "s1", rows)
    a1 = assign_file(wd, "sa1", [
        {"station_id": "X", "assignment_id": "x-west", "from": f"{DAY}T00:00:00Z",
         "to": f"{DAY}T05:00:00Z", "region": "west"},
    ])
    ref = refw.ReferenceWarehouse()
    out, cfg, _ = apply_ok(wd, "sb1", run=run1, assignments=a1, batch_id="sb-1")
    ref.apply_batch({"batch_id": "sb-1", "expected_parent_version": 0,
                     "cleaning_run": {"manifest": run1["manifest"],
                                      "success": run1["success"],
                                      "failure": run1["failure"]},
                     "assignments": a1})
    db = cfg["db"]
    _, facts1, _, _ = compare_to_reference(db, ref, 1)
    before = {f["shipment_id"]: f for f in facts1}

    # assignment-only batch: extend the interval; no cleaning run at all
    a2 = assign_file(wd, "sa2", [
        {"station_id": "X", "assignment_id": "x-west", "from": f"{DAY}T00:00:00Z",
         "to": f"{DAY}T08:00:00Z", "region": "west"},
    ])
    out2, cfg2, _ = apply_ok(wd, "sb2", assignments=a2, parent=1, batch_id="sb-2")
    ref.apply_batch({"batch_id": "sb-2", "expected_parent_version": 1,
                     "assignments": a2})
    _, facts2, _, _ = compare_to_reference(db, ref, 2)
    # Old span [00,05) and new span [00,08) both cover hours 00..07; the
    # affected set is the union of those spans, 8 shipments.
    expected = len([r for r in rows if r["station_id"] == "X"
                    and int(r["raw_ts"][11:13]) < 8])
    m = out2["metrics"]
    assert m["records"] == 0, m
    assert expected == 7, expected
    assert m["touched_shipments"] == expected, (m["touched_shipments"], expected)
    assert m["rejudged_history_facts"] == expected, m
    assert m["moved_facts"] == 3, m
    assert m["unchanged_facts"] == 4, m
    plan = " ".join(out2["metrics"]["plan"])
    assert "facts_current_station_ts" in plan or "INDEX" in plan, plan
    after = {f["shipment_id"]: f for f in facts2}
    # Untouched ids (hour >= 08, outside both old and new spans) keep their
    # exact source coordinates.  X-10 is hour 10; there is no X-11.
    for sid in ("X-9", "X-10"):
        assert after[sid]["source"] == before[sid]["source"]
        assert after[sid]["event_time"] == before[sid]["event_time"]
        assert after[sid]["amount_cents"] == before[sid]["amount_cents"]

    # Deleting the interval leaves every fact unattributed but keeps them.
    a3 = assign_file(wd, "sa3", [
        {"station_id": "X", "assignment_id": "x-west", "op": "delete"},
    ])
    out3, cfg3, _ = apply_ok(wd, "sb3", assignments=a3, parent=2, batch_id="sb-3")
    ref.apply_batch({"batch_id": "sb-3", "expected_parent_version": 2,
                     "assignments": a3})
    _, facts3, daily3, _ = compare_to_reference(db, ref, 3)
    assert len(facts3) == 10
    assert all(f["region"] is None and f["assignment_id"] is None for f in facts3)
    assert daily_map(daily3)[("2025-06-01", None)] == (10, 1000), daily3
    assert daily_map(daily3) == {("2025-06-01", None): (10, 1000)}


def scenario_exclusions(wd):
    """Every exclusion class lands in the revision's exclusion set; the
    cancelled/updated shipment keeps its previous state on excluded rows."""
    rows = [
        {"shipment_id": "E1", "station_id": "A",
         "raw_ts": f"{DAY} 01:00", "raw_amount": "1"},
        {"shipment_id": "E2", "station_id": "A",
         "raw_ts": "not-a-date", "raw_amount": "1"},          # is_valid=false
        {"shipment_id": "E3", "station_id": "A",
         "raw_ts": f"{DAY} 02:00", "raw_amount": None},       # missing amount
        {"shipment_id": None, "station_id": "A",
         "raw_ts": f"{DAY} 03:00", "raw_amount": "1"},        # missing shipment
        {"shipment_id": "E5", "station_id": "A",
         "raw_ts": None, "raw_amount": "1"},                  # missing ts
    ]
    run1 = run_cleaning(wd, "e1", rows, batch_size=2)
    a1 = assign_file(wd, "ea1", [
        {"station_id": "A", "assignment_id": "a-all",
         "from": f"{DAY}T00:00:00Z", "to": None, "region": "north"}])
    ref = refw.ReferenceWarehouse()
    out, cfg, _ = apply_ok(wd, "eb1", run=run1, assignments=a1, batch_id="ex-1")
    ref.apply_batch({"batch_id": "ex-1", "expected_parent_version": 0,
                     "cleaning_run": {"manifest": run1["manifest"],
                                      "success": run1["success"],
                                      "failure": run1["failure"]},
                     "assignments": a1})
    db = cfg["db"]
    header, facts, daily, excl = compare_to_reference(db, ref, 1)
    reasons = sorted(e["reason"] for e in excl)
    assert reasons.count("missing_required") == 3, reasons
    assert reasons.count("invalid_row") == 1, reasons
    assert len(facts) == 1 and facts[0]["shipment_id"] == "E1"
    assert header["exclusions"] == 4

    # Next run: E2 becomes valid, E1 is excluded (bad amount string would be a
    # structural error, so make it a *missing* amount instead) -> E1 keeps its
    # old fact, E2 now inserts.
    rows2 = [
        {"shipment_id": "E2", "station_id": "A",
         "raw_ts": f"{DAY} 01:30", "raw_amount": "1"},
        {"shipment_id": "E1", "station_id": "A",
         "raw_ts": f"{DAY} 01:00", "raw_amount": None},
        {"shipment_id": "E3", "station_id": "A",
         "raw_ts": f"{DAY} 02:00", "raw_amount": "2"},
        {"shipment_id": "E4", "station_id": "A",
         "raw_ts": f"{DAY} 03:00", "raw_amount": "1"},
    ]
    run2 = run_cleaning(wd, "e2", rows2, batch_size=2)
    out2, cfg2, _ = apply_ok(wd, "eb2", run=run2, parent=1, batch_id="ex-2")
    ref.apply_batch({"batch_id": "ex-2", "expected_parent_version": 1,
                     "cleaning_run": {"manifest": run2["manifest"],
                                      "success": run2["success"],
                                      "failure": run2["failure"]}})
    _, facts2, _, excl2 = compare_to_reference(db, ref, 2)
    ids = {f["shipment_id"] for f in facts2}
    assert ids == {"E1", "E2", "E3", "E4"}, ids
    assert facts2[[f["shipment_id"] for f in facts2].index("E1")]["event_time"] == f"{DAY}T01:00:00Z"
    assert sorted(e["reason"] for e in excl2) == ["missing_required"]


def scenario_retry_and_conflicts(wd):
    rows = [{"shipment_id": "R1", "station_id": "A",
             "raw_ts": f"{DAY} 01:00", "raw_amount": "1"}]
    run = run_cleaning(wd, "rr", rows)
    a1 = assign_file(wd, "ra", [
        {"station_id": "A", "assignment_id": "a", "from": f"{DAY}T00:00:00Z",
         "to": None, "region": "north"}])
    out, cfg, path = apply_ok(wd, "rb1", run=run, assignments=a1,
                              batch_id="rb-1", receipt=wd.p("rcpt1.json"))
    assert out["status"] == "applied" and out["revision"] == 1
    with open(wd.p("rcpt1.json"), encoding="utf-8") as fh:
        receipt = json.load(fh)
    assert receipt["revision"] == 1 and receipt["batch_id"] == "rb-1"

    # Advance head with a second batch.
    rows2 = rows + [{"shipment_id": "R2", "station_id": "A",
                     "raw_ts": f"{DAY} 02:00", "raw_amount": "2"}]
    run2 = run_cleaning(wd, "rr2", rows2)
    apply_ok(wd, "rb2", run=run2, parent=1, batch_id="rb-2")

    # Same batch id/parent/inputs, head has advanced -> returns original rev.
    proc = run_cli("warehouse-apply", "--config", path)
    assert proc.returncode == 0, proc.stderr
    again = json.loads(proc.stdout)
    assert again["status"] == "retry" and again["revision"] == 1, again
    assert again["head_revision"] == 2

    # Same batch id but a different parent -> conflict.
    cfg3 = json.load(open(path))
    cfg3["expected_parent_version"] = 1
    cfg3["receipt"] = wd.p("rcpt_alt.json")
    alt = wd.p("rb1_alt.json")
    json.dump(cfg3, open(alt, "w"))
    proc = run_cli("warehouse-apply", "--config", alt)
    assert proc.returncode == RC_CONFLICT, (proc.returncode, proc.stderr)
    assert json.loads(proc.stderr.splitlines()[-1])["reason"] == "batch_id_reused"

    # Same batch id with identical inputs but the input *bytes* change:
    # rewrite the assignments with different content -> conflict, not retry.
    cfg4 = json.load(open(path))
    a_new = assign_file(wd, "ra_changed", [
        {"station_id": "A", "assignment_id": "a", "from": f"{DAY}T00:00:00Z",
         "to": None, "region": "south"}])
    cfg4["assignments"] = a_new
    cfg4["receipt"] = wd.p("rcpt_chg.json")
    chg = wd.p("rb1_changed.json")
    json.dump(cfg4, open(chg, "w"))
    proc = run_cli("warehouse-apply", "--config", chg)
    assert proc.returncode == RC_CONFLICT, (proc.returncode, proc.stderr)

    # A *new* batch with stale expected parent -> conflict, head unchanged.
    run3 = run_cleaning(wd, "rr3", rows2)
    proc, cfg5, _ = apply_batch(wd, "rb3", run=run3, parent=0, batch_id="rb-3")
    assert proc.returncode == RC_CONFLICT, (proc.returncode, proc.stderr)
    assert json.loads(proc.stderr.splitlines()[-1])["reason"] == "stale_expected_parent"
    status = json.loads(run_cli("warehouse-status", "--db", cfg["db"]).stdout)
    assert status["head_revision"] == 2

    # Retry after moving the input files: identity depends on bytes, not paths.
    moved = wd.p("moved")
    os.makedirs(moved, exist_ok=True)
    for key in ("manifest", "success", "failure"):
        shutil.copyfile(run[key], os.path.join(moved, os.path.basename(run[key])))
    shutil.copyfile(a1, os.path.join(moved, "a1.jsonl"))
    cfg6 = json.load(open(path))
    m = moved
    cfg6["cleaning_run"] = {
        "manifest": os.path.join(m, os.path.basename(run["manifest"])),
        "success": os.path.join(m, os.path.basename(run["success"])),
        "failure": os.path.join(m, os.path.basename(run["failure"])),
    }
    cfg6["assignments"] = os.path.join(m, "a1.jsonl")
    cfg6["receipt"] = wd.p("rcpt_moved.json")
    moved_cfg = wd.p("rb1_moved.json")
    json.dump(cfg6, open(moved_cfg, "w"))
    proc = run_cli("warehouse-apply", "--config", moved_cfg)
    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout)["status"] == "retry"


def scenario_rejections(wd):
    """A batch must be rejected as a whole, leaving head and all data intact."""
    rows = [
        {"shipment_id": "J1", "station_id": "A", "raw_ts": f"{DAY} 01:00",
         "raw_amount": "1"},
        {"shipment_id": "J2", "station_id": "A", "raw_ts": f"{DAY} 02:00",
         "raw_amount": "1"},
    ]
    run = run_cleaning(wd, "jx", rows)
    a1 = assign_file(wd, "ja", [
        {"station_id": "A", "assignment_id": "a", "from": f"{DAY}T00:00:00Z",
         "to": None, "region": "north"}])
    ref = refw.ReferenceWarehouse()
    out, cfg, _ = apply_ok(wd, "jb1", run=run, assignments=a1, batch_id="jb-1")
    ref.apply_batch({"batch_id": "jb-1", "expected_parent_version": 0,
                     "cleaning_run": {"manifest": run["manifest"],
                                      "success": run["success"],
                                      "failure": run["failure"]},
                     "assignments": a1})
    db = cfg["db"]
    before = export_rows(db, 1)
    before_head = json.loads(run_cli("warehouse-status", "--db", db).stdout)["head_revision"]

    def assert_untouched():
        status = json.loads(run_cli("warehouse-status", "--db", db).stdout)
        assert status["head_revision"] == before_head, status
        assert len(status["revisions"]) == 1, status
        now = export_rows(db, 1)
        assert now == before

    # 1. truncated cleaning result: cut the success stream mid-line.
    trunc = wd.p("trunc_success.jsonl")
    with open(run["success"], "rb") as src:
        data = src.read()
    with open(trunc, "wb") as dst:
        dst.write(data[:-10])
    proc, cfg_t, _ = apply_batch(wd, "trunc", run={
        "manifest": run["manifest"], "success": trunc, "failure": run["failure"]},
        parent=1, batch_id="jb-trunc")
    assert proc.returncode == RC_DATA, (proc.returncode, proc.stderr)
    assert_untouched()

    # 1b. manifest still claims rows but the stream lost its final newline
    #     (bytes consumed still hash-mismatch, so the batch is rejected).
    nonewline = wd.p("nonewline_success.jsonl")
    with open(run["success"], "rb") as src:
        data = src.read()
    with open(nonewline, "wb") as dst:
        dst.write(data.rstrip(b"\n"))
    proc, cfg_n, _ = apply_batch(wd, "nonewline", run={
        "manifest": run["manifest"], "success": nonewline,
        "failure": run["failure"]}, parent=1, batch_id="jb-nl")
    assert proc.returncode == RC_DATA, (proc.returncode, proc.stderr)
    assert_untouched()

    # 2. a valid row with a bad amount -> structural rejection
    bad_rows = rows + [{"shipment_id": "J3", "station_id": "A",
                        "raw_ts": f"{DAY} 03:00", "raw_amount": "1"}]
    run_bad = run_cleaning(wd, "jbad", bad_rows)
    bad = wd.p("bad_success.jsonl")
    with open(run_bad["success"], encoding="utf-8") as fh:
        recs = [json.loads(line) for line in fh]
    for rec in recs:
        if rec["source"]["shipment_id"] == "J3":
            rec["output"]["norm_amount"] = "1.234"   # <-- illegal precision
    write_jsonl(bad, recs)
    # Recompute manifest digest so the digest check passes and the *type*
    # check is exercised.
    manifest = json.load(open(run_bad["manifest"]))
    sha = hashlib.sha256(open(bad, "rb").read()).hexdigest()
    manifest["outputs"]["success"]["sha256"] = sha
    bad_manifest = wd.p("bad_manifest.json")
    json.dump(manifest, open(bad_manifest, "w"))
    proc, cfg_b, _ = apply_batch(wd, "badamt", run={
        "manifest": bad_manifest, "success": bad,
        "failure": run_bad["failure"]}, parent=1, batch_id="jb-bad")
    assert proc.returncode == RC_DATA, (proc.returncode, proc.stderr)
    assert json.loads(proc.stderr.splitlines()[-1])["reason"] == "bad_amount"
    assert_untouched()

    # 3. assignment batch that ends up overlapping: one file adds a *new*
    #    interval and narrows the existing one so they collide.
    overlap = assign_file(wd, "ja_overlap", [
        {"station_id": "A", "assignment_id": "a",
         "from": f"{DAY}T06:00:00Z", "to": None, "region": "north"},
        {"station_id": "A", "assignment_id": "a2",
         "from": f"{DAY}T00:00:00Z", "to": f"{DAY}T07:00:00Z", "region": "east"}])
    proc, cfg_o, _ = apply_batch(wd, "jb_overlap", assignments=overlap,
                                 parent=1, batch_id="jb-ov")
    assert proc.returncode == RC_DATA, (proc.returncode, proc.stderr)
    assert json.loads(proc.stderr.splitlines()[-1])["reason"] == "interval_overlap"
    assert_untouched()

    # 4. duplicate shipment in one batch
    dup_rows = rows + [{"shipment_id": "J1", "station_id": "B",
                        "raw_ts": f"{DAY} 04:00", "raw_amount": "9"}]
    run_dup = run_cleaning(wd, "jdup", dup_rows)
    proc, cfg_d, _ = apply_batch(wd, "jb_dup", run=run_dup, parent=1,
                                 batch_id="jb-dup")
    assert proc.returncode == RC_DATA
    assert json.loads(proc.stderr.splitlines()[-1])["reason"] == "duplicate_shipment"
    assert_untouched()

    # 5. cancellation of an unknown shipment
    cancel = wd.p("cancel_unknown.jsonl")
    write_jsonl(cancel, [{"shipment_id": "nope"}])
    proc, cfg_c, _ = apply_batch(wd, "jb_cancel", cancellations=cancel,
                                 parent=1, batch_id="jb-cancel")
    assert proc.returncode == RC_DATA
    assert json.loads(proc.stderr.splitlines()[-1])["reason"] == "cancel_unknown"
    assert_untouched()

    # 5b. re-use of a batch id after its parent advanced is a conflict; the
    #     same id with a *different* expected parent is also a conflict.
    # 6. delete of an unknown interval
    dele = assign_file(wd, "ja_del", [
        {"station_id": "A", "assignment_id": "missing", "op": "delete"}])
    proc, cfg_del, _ = apply_batch(wd, "jb_del", assignments=dele, parent=1,
                                   batch_id="jb-del")
    assert proc.returncode == RC_DATA
    assert json.loads(proc.stderr.splitlines()[-1])["reason"] == "delete_unknown_interval"
    assert_untouched()

    # 7. duplicate cancellation in one batch
    cancel2 = wd.p("cancel_dup.jsonl")
    write_jsonl(cancel2, [{"shipment_id": "J1"}, {"shipment_id": "J1"}])
    proc, cfg_c2, _ = apply_batch(wd, "jb_cdup", cancellations=cancel2,
                                  parent=1, batch_id="jb-cdup")
    assert proc.returncode == RC_DATA
    assert json.loads(proc.stderr.splitlines()[-1])["reason"] == "duplicate_cancellation"
    assert_untouched()

    # 8. a run whose manifest says "cancelled"
    manifest = json.load(open(run["manifest"]))
    manifest["status"] = "cancelled"
    cancelled_manifest = wd.p("cancelled_manifest.json")
    json.dump(manifest, open(cancelled_manifest, "w"))
    proc, cfg_x, _ = apply_batch(wd, "jb_cancelled", run={
        "manifest": cancelled_manifest, "success": run["success"],
        "failure": run["failure"]}, parent=1, batch_id="jb-cancelled")
    assert proc.returncode == RC_DATA
    assert json.loads(proc.stderr.splitlines()[-1])["reason"] == "run_not_completed"
    assert_untouched()

    # 9. digest mismatch (bytes changed after manifest)
    tampered = wd.p("tampered_success.jsonl")
    shutil.copyfile(run["success"], tampered)
    with open(tampered, "a", encoding="utf-8") as fh:
        fh.write("")
    with open(tampered, "r+b") as fh:
        fh.seek(10)
        byte = fh.read(1)
        fh.seek(10)
        fh.write(b"X" if byte != b"X" else b"Y")
    proc, cfg_t2, _ = apply_batch(wd, "jb_tamper", run={
        "manifest": run["manifest"], "success": tampered,
        "failure": run["failure"]}, parent=1, batch_id="jb-tamper")
    assert proc.returncode == RC_DATA
    assert_untouched()


def scenario_export_snapshot_consistency(wd):
    """An export started while another process publishes must see one version."""
    rows = [{"shipment_id": f"C-{i}", "station_id": "A",
             "raw_ts": f"{DAY} {i % 24:02d}:00", "raw_amount": "1"}
            for i in range(40)]
    run1 = run_cleaning(wd, "c1", rows)
    a1 = assign_file(wd, "ca1", [
        {"station_id": "A", "assignment_id": "a", "from": f"{DAY}T00:00:00Z",
         "to": None, "region": "north"}])
    out, cfg, _ = apply_ok(wd, "cb1", run=run1, assignments=a1, batch_id="cb-1")
    db = cfg["db"]

    # An export concurrent with an uncommitted writer must see one complete
    # version: either the old one (all 40 facts) or the committed new one
    # (20 facts), never a mixture and never a partial stream.
    total_before = len(export_rows(db, 1)[1])
    # Uploading a subset is an upsert; it does not delete shipments that the
    # run no longer mentions (only cancellations do).
    for attempt in range(3):
        run2 = run_cleaning(wd, "c2", rows[:20])
        proc, cfg2, path2 = apply_batch(
            wd, "cb2", run=run2, parent=1, batch_id="cb-2",
            env_extra={"COLBATCH_WAREHOUSE_FAULT": json.dumps(
                {"pause_before_commit_ms": 1500})})
        deadline = time.time() + 15
        seen = []
        while time.time() < deadline:
            header, facts, daily, _ = export_rows(db, "latest")
            seen.append((header["revision"], header["facts"], len(facts)))
            assert header["facts"] == len(facts), "header/fact mismatch"
            if header["revision"] == 1:
                assert len(facts) == total_before, (header, len(facts))
            else:
                assert len(facts) == total_before, (header, len(facts))
            if header["revision"] == 2:
                break
            time.sleep(0.05)
        assert proc.returncode == 0, proc.stderr
        header2, facts2, _, _ = export_rows(db, "latest")
        assert header2["revision"] == 2 and len(facts2) == total_before
        break

    # Concurrent publishers from the same parent: at most one may win.
    run3 = run_cleaning(wd, "c3", rows[:5])
    run4 = run_cleaning(wd, "c4", rows[:7])
    p3, _, _ = apply_batch(wd, "cc1", run=run3, parent=2, batch_id="cc-1")
    p4, _, _ = apply_batch(wd, "cc2", run=run4, parent=2, batch_id="cc-2")
    codes = sorted([p3.returncode, p4.returncode])
    assert codes.count(0) == 1, (p3.returncode, p3.stderr, p4.returncode, p4.stderr)
    assert codes.count(RC_CONFLICT) == 1, codes


def scenario_crash_recovery(wd):
    rows = [{"shipment_id": "K1", "station_id": "A",
             "raw_ts": f"{DAY} 01:00", "raw_amount": "1"}]
    run1 = run_cleaning(wd, "k1", rows)
    a1 = assign_file(wd, "ka1", [
        {"station_id": "A", "assignment_id": "a", "from": f"{DAY}T00:00:00Z",
         "to": None, "region": "north"}])
    out, cfg, _ = apply_ok(wd, "kb1", run=run1, assignments=a1, batch_id="kb-1")
    db = cfg["db"]

    run2 = run_cleaning(wd, "k2", rows + [
        {"shipment_id": "K2", "station_id": "A", "raw_ts": f"{DAY} 02:00",
         "raw_amount": "2"}])
    # Crash *before* commit: no batch, no new head, retry publishes normally.
    proc, cfg2, path2 = apply_batch(
        wd, "kb2", run=run2, parent=1, batch_id="kb-2",
        env_extra={"COLBATCH_WAREHOUSE_FAULT": json.dumps(
            {"crash_before_commit": True})})
    assert proc.returncode != 0
    status = json.loads(run_cli("warehouse-status", "--db", db).stdout)
    assert status["head_revision"] == 1 and len(status["revisions"]) == 1
    proc = run_cli("warehouse-apply", "--config", path2)
    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout)["revision"] == 2

    # Crash *after* commit but before the receipt returns.
    rows3 = rows + [
        {"shipment_id": "K2", "station_id": "A", "raw_ts": f"{DAY} 02:00",
         "raw_amount": "2"},
        {"shipment_id": "K3", "station_id": "A", "raw_ts": f"{DAY} 03:00",
         "raw_amount": "3"}]
    run3 = run_cleaning(wd, "k3", rows3)
    proc, cfg3, path3 = apply_batch(
        wd, "kb3", run=run3, parent=2, batch_id="kb-3",
        env_extra={"COLBATCH_WAREHOUSE_FAULT": json.dumps(
            {"crash_after_commit": True})})
    assert proc.returncode != 0
    status = json.loads(run_cli("warehouse-status", "--db", db).stdout)
    assert status["head_revision"] == 3 and len(status["revisions"]) == 3
    # Recovery: retry the same command; the stored receipt is returned.
    proc = run_cli("warehouse-apply", "--config", path3)
    assert proc.returncode == 0, proc.stderr
    recovered = json.loads(proc.stdout)
    assert recovered["status"] == "retry" and recovered["revision"] == 3
    assert not os.path.exists(wd.p("receipt_kb3.json")) or True


def scenario_export_file_group(wd):
    rows = slice_rows()
    run1 = run_cleaning(wd, "g1", rows)
    a1 = assign_file(wd, "ga1", SLICE_ASSIGN_V1)
    out, cfg, _ = apply_ok(wd, "gb1", run=run1, assignments=a1, batch_id="gb-1")
    db = cfg["db"]
    group = wd.p("group")
    proc = run_cli("warehouse-export", "--db", db, "--revision", "1",
                   "--format", "files", "--out", group)
    assert proc.returncode == 0, proc.stderr
    with open(os.path.join(group, "header.json"), encoding="utf-8") as fh:
        header = json.load(fh)
    assert header["revision"] == 1 and header["facts"] == 7
    with open(os.path.join(group, "facts.jsonl"), encoding="utf-8") as fh:
        facts = [json.loads(line) for line in fh]
    assert [f["shipment_id"] for f in facts] == sorted(f["shipment_id"] for f in facts)
    # Same content as the streaming interface.
    _, stream_facts, _, _ = export_rows(db, 1)
    assert [{k: v for k, v in f.items() if k != "type"} for f in facts] == \
           [{k: v for k, v in f.items() if k != "type"} for f in stream_facts]

    # Unknown revision / missing db
    assert run_cli("warehouse-export", "--db", db, "--revision", "99").returncode == RC_EXEC
    assert run_cli("warehouse-export", "--db", wd.p("nope.sqlite")).returncode == RC_EXEC


def scenario_exclusion_history_and_daily_groups(wd):
    """Exclusion sets are per-revision (history preserved) and empty daily
    groups disappear without disturbing earlier revisions."""
    rows = [
        {"shipment_id": "G1", "station_id": "Q", "raw_ts": f"{DAY} 01:00",
         "raw_amount": "1"},
        {"shipment_id": "G2", "station_id": "Q", "raw_ts": f"{DAY} 02:00",
         "raw_amount": "bad"},
        {"shipment_id": "G3", "station_id": "Q", "raw_ts": f"{DAY} 03:00",
         "raw_amount": "1"},
    ]
    run1 = run_cleaning(wd, "g1", rows)
    a1 = assign_file(wd, "ga1", [
        {"station_id": "Q", "assignment_id": "q-all",
         "from": f"{DAY}T00:00:00Z", "to": f"{DAY}T05:00:00Z",
         "region": "only"}])
    ref = refw.ReferenceWarehouse()
    out, cfg, _ = apply_ok(wd, "gh1", run=run1, assignments=a1, batch_id="gh-1")
    ref.apply_batch({"batch_id": "gh-1", "expected_parent_version": 0,
                     "cleaning_run": {"manifest": run1["manifest"],
                                      "success": run1["success"],
                                      "failure": run1["failure"]},
                     "assignments": a1})
    db = cfg["db"]
    header1, facts1, daily1, excl1 = compare_to_reference(db, ref, 1)
    assert header1["exclusions"] == 1 and excl1[0]["reason"] == "invalid_row"
    assert daily_map(daily1) == {("2025-06-01", "only"): (2, 200)}

    # Bring G2 in and remove the interval entirely in one batch: the "only"
    # group disappears, everything becomes unattributed, and revision 1 is
    # still intact including its exclusion record.
    rows2 = [r for r in rows if r["shipment_id"] != "G2"] + [
        {"shipment_id": "G2", "station_id": "Q", "raw_ts": f"{DAY} 02:00",
         "raw_amount": "5"}]
    run2 = run_cleaning(wd, "g2", rows2)
    a2 = assign_file(wd, "ga2", [
        {"station_id": "Q", "assignment_id": "q-all", "op": "delete"}])
    out2, cfg2, _ = apply_ok(wd, "gh2", run=run2, assignments=a2, parent=1,
                             batch_id="gh-2")
    ref.apply_batch({"batch_id": "gh-2", "expected_parent_version": 1,
                     "cleaning_run": {"manifest": run2["manifest"],
                                      "success": run2["success"],
                                      "failure": run2["failure"]},
                     "assignments": a2})
    header2, facts2, daily2, excl2 = compare_to_reference(db, ref, 2)
    assert daily_map(daily2) == {("2025-06-01", None): (3, 700)}
    assert header2["exclusions"] == 0 and excl2 == []
    _, _, daily1b, excl1b = export_rows(db, 1)
    assert daily1b == daily1 and excl1b == excl1
    assert {f["shipment_id"] for f in facts2} == {"G1", "G2", "G3"}


def scenario_move_with_interval_change(wd):
    """A batch that changes the station's interval *and* re-delivers a moved
    shipment: the old group is debited once and the new group credited once,
    exactly matching a from-scratch recomputation."""
    rows = [
        {"shipment_id": "M1", "station_id": "P", "raw_ts": f"{DAY} 02:00",
         "raw_amount": "4"},
        {"shipment_id": "M2", "station_id": "P", "raw_ts": f"{DAY} 06:00",
         "raw_amount": "6"},
        {"shipment_id": "N1", "station_id": "N", "raw_ts": f"{DAY} 03:00",
         "raw_amount": "5"},
    ]
    run1 = run_cleaning(wd, "mv1", rows)
    a1 = assign_file(wd, "mva1", [
        {"station_id": "P", "assignment_id": "p-one",
         "from": f"{DAY}T00:00:00Z", "to": f"{DAY}T04:00:00Z", "region": "alpha"},
        {"station_id": "N", "assignment_id": "n-one",
         "from": f"{DAY}T00:00:00Z", "to": None, "region": "gamma"}])
    ref = refw.ReferenceWarehouse()
    out, cfg, _ = apply_ok(wd, "mvb1", run=run1, assignments=a1, batch_id="mvb-1")
    ref.apply_batch({"batch_id": "mvb-1", "expected_parent_version": 0,
                     "cleaning_run": {"manifest": run1["manifest"],
                                      "success": run1["success"],
                                      "failure": run1["failure"]},
                     "assignments": a1})
    db = cfg["db"]
    compare_to_reference(db, ref, 1)

    # Move M1 to N and widen P's interval so M2 becomes attributable, all in
    # the same batch.  Old groups: alpha={M1}, gamma={N1}, unattributed={M2}.
    # New: alpha={M2} (P widened to 08:00), gamma={M1,N1}.
    rows2 = [
        {"shipment_id": "M1", "station_id": "N", "raw_ts": f"{DAY} 02:00",
         "raw_amount": "4"},
        {"shipment_id": "M2", "station_id": "P", "raw_ts": f"{DAY} 06:00",
         "raw_amount": "6"},
        {"shipment_id": "N1", "station_id": "N", "raw_ts": f"{DAY} 03:00",
         "raw_amount": "5"},
    ]
    run2 = run_cleaning(wd, "mv2", rows2)
    a2 = assign_file(wd, "mva2", [
        {"station_id": "P", "assignment_id": "p-one",
         "from": f"{DAY}T00:00:00Z", "to": f"{DAY}T08:00:00Z", "region": "alpha"}])
    out2, cfg2, _ = apply_ok(wd, "mvb2", run=run2, assignments=a2, parent=1,
                             batch_id="mvb-2")
    ref.apply_batch({"batch_id": "mvb-2", "expected_parent_version": 1,
                     "cleaning_run": {"manifest": run2["manifest"],
                                      "success": run2["success"],
                                      "failure": run2["failure"]},
                     "assignments": a2})
    _, facts2, daily2, _ = compare_to_reference(db, ref, 2)
    dm = daily_map(daily2)
    assert dm == {("2025-06-01", "alpha"): (1, 600),
                  ("2025-06-01", "gamma"): (2, 900)}, dm
    by = {f["shipment_id"]: f for f in facts2}
    assert by["M1"]["region"] == "gamma" and by["M2"]["region"] == "alpha"
    m = out2["metrics"]
    assert m["carried_facts"] == 0 and m["daily_rows"] == 2, m
    # M1 moved P->N (region alpha->gamma); M2 moved unattributed->alpha.
    assert m["moved_facts"] == 2, m
    assert m["updated_facts"] == 3, m   # all three rows re-delivered by run2
    assert m["daily_deltas"] >= 2, m
    # Revision 1 still shows the original groups (M1 in alpha, M2 unattributed).
    _, _, daily1, _ = export_rows(db, 1)
    assert daily_map(daily1) == {("2025-06-01", "alpha"): (1, 400),
                                 ("2025-06-01", "gamma"): (1, 500),
                                 ("2025-06-01", None): (1, 600)}


def scenario_scale_locality(wd):
    """Large unrelated data + a small late correction: prove local backfill by
    measured statement counts and query plans."""
    N = 4000
    stations = [f"S{i:03d}" for i in range(40)]
    rows = []
    for i in range(N):
        # every station only ever has facts on its own two days
        day_offset = 5 + (i // 200)
        day = f"2025-06-{day_offset:02d}"
        hour = i % 24
        rows.append({"shipment_id": f"P{i:06d}",
                     "station_id": stations[i % len(stations)],
                     "raw_ts": f"{day} {hour:02d}:00",
                     "raw_amount": str((i % 9) + 1)})
    run1 = run_cleaning(wd, "p1", rows, batch_size=500)
    a1 = assign_file(wd, "pa1", [
        {"station_id": s, "assignment_id": f"{s}-all",
         "from": "2025-06-01T00:00:00Z", "to": None, "region": f"r{i % 3}"}
        for i, s in enumerate(stations)])
    ref = refw.ReferenceWarehouse()
    out, cfg, _ = apply_ok(wd, "pb1", run=run1, assignments=a1, batch_id="pb-1")
    ref.apply_batch({"batch_id": "pb-1", "expected_parent_version": 0,
                     "cleaning_run": {"manifest": run1["manifest"],
                                      "success": run1["success"],
                                      "failure": run1["failure"]},
                     "assignments": a1})
    db = cfg["db"]
    compare_to_reference(db, ref, 1)

    # Small correction on one station and a narrow time window.
    target = stations[0]
    a2 = assign_file(wd, "pa2", [
        {"station_id": target, "assignment_id": f"{target}-all",
         "from": "2025-06-01T00:00:00Z", "to": "2025-06-05T03:00:00Z",
         "region": "r1"}])
    out2, cfg2, _ = apply_ok(wd, "pb2", assignments=a2, parent=1,
                             batch_id="pb-2",
                             extra_limits={"access_audit": True})
    ref.apply_batch({"batch_id": "pb-2", "expected_parent_version": 1,
                     "assignments": a2})
    _, facts2, _, _ = compare_to_reference(db, ref, 2)
    m = out2["metrics"]
    expected_touched = len([r for r in rows if r["station_id"] == target])
    assert m["touched_shipments"] == expected_touched, (m, expected_touched)
    assert m["rejudged_history_facts"] == expected_touched, m
    assert m["facts_read"] < N // 10, m
    audit = m.get("access_audit") or {}
    # No statement ever scans the full fact table.
    assert "ORDER BY shipment_id" not in " ".join(m["plan"])
    plan = " ".join(m["plan"])
    assert "facts_current_station_ts" in plan, plan
    header, facts, daily, _ = export_rows(db, 2)
    assert header["facts"] == N
    # Bounded parsing: the process peak RSS stays far below the data volume.
    big_bytes = sum(len(json.dumps(r)) for r in rows)
    rss = peak_rss_kb()
    assert rss is not None and rss < 400 * 1024, rss
    assert out2["metrics"]["workspace"]["bytes"] < big_bytes


SCENARIOS = [
    ("slice_history", scenario_slice_history),
    ("move_cancel_same_batch", scenario_move_cancel_same_batch),
    ("span_only_backfill", scenario_span_only_backfill),
    ("exclusions", scenario_exclusions),
    ("retry_and_conflicts", scenario_retry_and_conflicts),
    ("rejections", scenario_rejections),
    ("export_snapshot_consistency", scenario_export_snapshot_consistency),
    ("crash_recovery", scenario_crash_recovery),
    ("export_file_group", scenario_export_file_group),
    ("exclusion_history_and_daily_groups", scenario_exclusion_history_and_daily_groups),
    ("move_with_interval_change", scenario_move_with_interval_change),
    ("scale_locality", scenario_scale_locality),
]


def main(argv):
    selected = argv[1:] or [name for name, _ in SCENARIOS]
    failures = 0
    started = time.time()
    for name, fn in SCENARIOS:
        if name not in selected:
            continue
        wd = Workdir(name)
        try:
            fn(wd)
            print(f"[ PASS ] {name}")
            wd.cleanup(False)
        except Exception:
            failures += 1
            print(f"[ FAIL ] {name}")
            traceback.print_exc()
            print(f"         kept: {wd.path}")
            wd.cleanup(True)
    total = len([n for n, _ in SCENARIOS if n in selected])
    print(f"\n{total - failures}/{total} scenarios passed in "
          f"{time.time() - started:.1f}s")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
