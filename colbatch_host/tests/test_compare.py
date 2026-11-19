#!/usr/bin/env python3
"""Behavioural and cross-validation test-suite for ``colbatch compare``.

Every scenario builds a fresh temp directory and synthesises two run
directories (manifest + success/failure streams) dynamically -- large
inputs are generated at test time; the repo carries no big fixtures.  The
product CLI is driven as a subprocess and its report is cross-checked
against the independent in-memory reference reconciler in
``reference_reconcile.py`` (which shares no code with the product).

Run:  python3 tests/test_compare.py                 # all scenarios
      python3 tests/test_compare.py cross_validate  # selected
Env:  KEEP=1 keeps temp dirs of failing scenarios.
      COLBATCH_PYTHON overrides the interpreter (defaults to sys.executable).
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import traceback

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import reference_reconcile as refr  # noqa: E402

PY = os.environ.get("COLBATCH_PYTHON", sys.executable)
TEST_TMP = os.environ.get("COLBATCH_TEST_DIR", tempfile.gettempdir())

RC_OK = 0
RC_CONFIG = 3
RC_DATA = 4
RC_EXEC = 5

SNAP_A = "aa" * 32
SNAP_B = "bb" * 32


# ---------------------------------------------------------------------------
# helpers
#


class Workdir:
    def __init__(self, name):
        self.path = tempfile.mkdtemp(prefix=f"colcmp_{name}_", dir=TEST_TMP)

    def p(self, *parts):
        return os.path.join(self.path, *parts)

    def cleanup(self, failed):
        if failed or os.environ.get("KEEP"):
            return
        shutil.rmtree(self.path, ignore_errors=True)


def _sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(1 << 20)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def write_jsonl(path, records, trailing_newline=True):
    """Write stream records; a record may be a dict (JSON-encoded) or a raw
    string (written as-is, for corruption injection).  Accepts any iterable
    (generators included) and returns the number of rows written, so large
    streams never materialise in the test process."""
    n = 0
    with open(path, "w", encoding="utf-8") as fh:
        first = True
        for rec in records:
            line = rec if isinstance(rec, str) else json.dumps(
                rec, ensure_ascii=False)
            if not first:
                fh.write("\n")
            fh.write(line)
            first = False
            n += 1
        if n and trailing_newline:
            fh.write("\n")
    return n


def synth_run(wd, name, success_records, failure_records, *,
              snapshot_sha=SNAP_A, plugin_name="synth", plugin_version="1.0",
              status="completed", manifest_mutate=None,
              success_name="success.jsonl", failure_name="failure.jsonl",
              trailing_newline=True):
    """Create a synthetic run directory (streams + manifest) and return its
    compare-config side dict.  Manifest digests/rows always describe the
    files actually written unless ``manifest_mutate`` tampers afterwards."""
    d = wd.p(name)
    os.makedirs(d, exist_ok=True)
    succ_path = os.path.join(d, success_name)
    fail_path = os.path.join(d, failure_name)
    # every stream line (dict or raw injected line) counts as a delivered row
    n_succ = write_jsonl(succ_path, success_records,
                         trailing_newline=trailing_newline)
    n_fail = write_jsonl(fail_path, failure_records,
                         trailing_newline=trailing_newline)
    n = n_succ + n_fail
    manifest = {
        "status": status,
        "abort_reason": None,
        "plugin": {"name": plugin_name, "version": plugin_version},
        "input_source": "quality_gate_snapshot",
        "input_snapshot": {
            "path": os.path.join(d, "snap.jsonl"),
            "sha256": snapshot_sha,
            "rule_summary": "sha256:" + "11" * 32,
            "report_status": "admitted",
        },
        "input_rows_read": n,
        "batches_formed": (n + 9) // 10,
        "delivered_batches": (n + 9) // 10,
        "delivered_rows_success": n_succ,
        "delivered_rows_failure": n_fail,
        "last_delivered_row_id": n - 1,
        "outputs": {
            "success": {"path": succ_path, "sha256": _sha256_of(succ_path),
                        "rows": n_succ},
            "failure": {"path": fail_path, "sha256": _sha256_of(fail_path),
                        "rows": n_fail},
        },
    }
    if manifest_mutate is not None:
        manifest_mutate(manifest)
    manifest_path = os.path.join(d, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=1)
    return {"manifest": manifest_path, "success": succ_path,
            "failure": fail_path, "dir": d}


def compare_config(wd, base, cand, extra=None, report_dir="recon"):
    cfg = {
        "baseline": {k: base[k] for k in ("manifest", "success", "failure")},
        "candidate": {k: cand[k] for k in ("manifest", "success", "failure")},
        "report_dir": wd.p(report_dir),
    }
    if extra:
        cfg.update(extra)
    path = wd.p("compare.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, indent=1)
    return path


def run_cli(*args, env_extra=None):
    env = dict(os.environ)
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        [PY, "-m", "colbatch.cli", *args],
        cwd=ROOT, capture_output=True, text=True, env=env,
    )


def do_compare(wd, cfg_path):
    return run_cli("compare", "--config", cfg_path)


def read_report(wd, report_dir="recon"):
    with open(wd.p(report_dir, "report.json"), encoding="utf-8") as fh:
        report = json.load(fh)
    detail = []
    detail_path = wd.p(report_dir, report["detail"]["ref"])
    with open(detail_path, encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                detail.append(json.loads(line))
    return report, detail


def read_detail_raw(wd, report_dir="recon"):
    with open(wd.p(report_dir, "report.json"), encoding="utf-8") as fh:
        report = json.load(fh)
    with open(wd.p(report_dir, report["detail"]["ref"]), "rb") as fh:
        return fh.read()


def list_temp(wd, report_dir="recon"):
    tdir = wd.p(report_dir, "tmp")
    out = []
    if os.path.isdir(tdir):
        for root, _dirs, files in os.walk(tdir):
            for f in files:
                out.append(os.path.join(root, f))
    return out


def changes_files(wd, report_dir="recon"):
    cdir = wd.p(report_dir, "changes")
    return sorted(os.listdir(cdir)) if os.path.isdir(cdir) else []


def last_error(wd, report_dir="recon"):
    path = wd.p(report_dir, "last_error.json")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def src(i, **extra):
    rec = {"id": i, "name": f"row-{i}", "val": i * 1.5}
    rec.update(extra)
    return rec


def succ(rid, output, source=None):
    return {"row_id": rid, "source": source if source is not None else src(rid),
            "output": output}


def fail(rid, cause, source=None, **err_extra):
    err = {"cause": cause, "batch_id": rid // 10, "attempts": 1,
           "log": [{"attempt": 1, "cause": cause}]}
    err.update(err_extra)
    return {"row_id": rid, "source": source if source is not None else src(rid),
            "error": err}


def assert_reference_agrees(wd, base, cand, report, detail):
    """Full cross-check of the product report against the independent
    in-memory reference reconciler."""
    ref = refr.reference_reconcile(
        refr.load_stream(base["success"]), refr.load_stream(base["failure"]),
        refr.load_stream(cand["success"]), refr.load_stream(cand["failure"]),
    )
    assert report["summary"] == ref["summary"], (
        "summary mismatch:\nproduct=%s\nreference=%s"
        % (json.dumps(report["summary"], indent=1, sort_keys=True),
           json.dumps(ref["summary"], indent=1, sort_keys=True)))
    assert detail == ref["detail"], (
        "detail mismatch (first 3 product=%s, reference=%s)"
        % (detail[:3], ref["detail"][:3]))
    # detail is strictly ordered by row_id and covers changed rows only
    rids = [d["row_id"] for d in detail]
    assert rids == sorted(rids) and len(set(rids)) == len(rids)
    assert report["summary"]["changed_rows"] == len(detail)
    assert (report["summary"]["rows_compared"]
            == report["summary"]["unchanged"] + len(detail))


def assert_canonical_detail(wd, report_dir="recon"):
    """Every detail line is canonical JSON (sorted keys, tight separators)."""
    raw = read_detail_raw(wd, report_dir).decode("utf-8")
    for line in raw.splitlines():
        obj = json.loads(line)
        canon = json.dumps(obj, ensure_ascii=False, sort_keys=True,
                           separators=(",", ":"))
        assert line == canon, f"non-canonical detail line: {line[:120]}"


# ---------------------------------------------------------------------------
# scenarios
#


def cross_validate(wd):
    """Rich mixed transitions; product detail/summary == reference exactly."""
    base_succ, base_fail, cand_succ, cand_fail = [], [], [], []
    for i in range(200):
        s = src(i, payload={"nested": {"x": [1, 2, {"y": i}]}, "标签": f"值{i}"})
        out_base = {
            "amount": f"{i}.00",
            "score": i % 7,
            "meta": {"tries": 1, "tags": ["a", f"b{i}"]},
            "note": None,
        }
        kind = i % 10
        if kind == 0:  # unchanged success
            base_succ.append(succ(i, out_base, s))
            cand_succ.append(succ(i, out_base, s))
        elif kind == 1:  # scalar field change
            base_succ.append(succ(i, out_base, s))
            cand_succ.append(succ(i, {**out_base, "amount": f"{i}.01"}, s))
        elif kind == 2:  # nested change + key added/removed
            base_succ.append(succ(i, out_base, s))
            new_out = {"amount": out_base["amount"],
                       "meta": {"tries": 2, "tags": ["a", f"b{i}", "c"]},
                       "extra": {"deep": [True, None]}}
            cand_succ.append(succ(i, new_out, s))
        elif kind == 3:  # success -> failure
            base_succ.append(succ(i, out_base, s))
            cand_fail.append(fail(i, "plugin_error:plugin_exception", s))
        elif kind == 4:  # failure -> success
            base_fail.append(fail(i, "batch_timeout", s))
            cand_succ.append(succ(i, out_base, s))
        elif kind == 5:  # failure -> failure, same business cause
            base_fail.append(fail(i, "worker_crash", s, pid=100 + i))
            cand_fail.append(fail(i, "worker_crash", s, pid=900 + i,
                                  timestamp="2026-11-19T00:00:00Z"))
        elif kind == 6:  # failure -> failure, reason changed
            base_fail.append(fail(i, "batch_timeout", s))
            cand_fail.append(fail(i, "worker_crash", s))
        elif kind == 7:  # type-level change: int -> string
            base_succ.append(succ(i, {**out_base, "score": 7}, s))
            cand_succ.append(succ(i, {**out_base, "score": "7"}, s))
        elif kind == 8:  # array reorder is a real change
            base_succ.append(succ(i, out_base, s))
            out2 = json.loads(json.dumps(out_base))
            out2["meta"]["tags"] = list(reversed(out2["meta"]["tags"]))
            cand_succ.append(succ(i, out2, s))
        else:  # object key order must NOT be a change
            base_succ.append(succ(i, out_base, s))
            reordered = {k: out_base[k] for k in reversed(list(out_base))}
            reordered["meta"] = {k: out_base["meta"][k]
                                 for k in reversed(list(out_base["meta"]))}
            cand_succ.append(succ(i, reordered, s))
    base = synth_run(wd, "base", base_succ, base_fail, plugin_version="1.0")
    cand = synth_run(wd, "cand", cand_succ, cand_fail, plugin_version="2.0")
    cfg = compare_config(wd, base, cand)
    res = do_compare(wd, cfg)
    assert res.returncode == RC_OK, res.stderr
    report, detail = read_report(wd)
    assert_reference_agrees(wd, base, cand, report, detail)
    assert_canonical_detail(wd)

    s = report["summary"]
    assert s["rows_compared"] == 200
    assert s["success_to_failure"] == 20 and s["failure_to_success"] == 20
    assert s["failure_reason_changed"] == 20
    assert s["success_fields_changed"] == 80  # kinds 1,2,7,8
    # report structure: identities, digests, relative refs only
    assert report["runs"]["baseline"]["plugin"] == {"name": "synth",
                                                    "version": "1.0"}
    assert report["runs"]["candidate"]["plugin"] == {"name": "synth",
                                                     "version": "2.0"}
    assert report["input"]["snapshot_sha256"] == SNAP_A
    blob = open(wd.p("recon", "report.json"), encoding="utf-8").read()
    assert wd.path not in blob, "report leaks a machine absolute path"
    assert report["runs"]["baseline"]["manifest_ref"] == "base/manifest.json"
    assert report["detail"]["ref"].startswith("changes/changes-")
    # compare-status agrees
    st = run_cli("compare-status", "--config", cfg)
    assert st.returncode == RC_OK, st.stderr
    assert json.loads(st.stdout)["summary"] == report["summary"]


def json_type_semantics(wd):
    """The six-way JSON type lattice drives equality, not text form."""
    cases = [
        # (base_value, cand_value, expect_change)
        (1, 1.0, False),                # numbers compare numerically
        (1, "1", True),
        (1, True, True),
        (0, False, True),
        (True, "true", True),
        (None, 0, True),
        (None, "null", True),
        (-0.0, 0.0, False),
        (1e10, 10000000000.0, False),
        (9007199254740993, 9007199254740993, False),  # big ints exact
        ([1, 2], [2, 1], True),         # array order matters
        ([1, [2, 3]], [1, [2, 3]], False),
        ({"a": 1, "b": 2}, {"b": 2, "a": 1}, False),  # key order ignored
        ({"a": {"x": 1}}, {"a": {"x": 1.0}}, False),
        ({"a": {"x": 1}}, {"a": {"x": "1"}}, True),
        ("", None, True),
        ([], {}, True),
        ([], None, True),
        ({}, None, True),
    ]
    base_succ, cand_succ = [], []
    expected_changed = 0
    for i, (bv, cv, changed) in enumerate(cases):
        base_succ.append(succ(i, {"f": bv, "g": "same"}))
        cand_succ.append(succ(i, {"f": cv, "g": "same"}))
        expected_changed += 1 if changed else 0
    base = synth_run(wd, "base", base_succ, [])
    cand = synth_run(wd, "cand", cand_succ, [])
    cfg = compare_config(wd, base, cand)
    res = do_compare(wd, cfg)
    assert res.returncode == RC_OK, res.stderr
    report, detail = read_report(wd)
    assert_reference_agrees(wd, base, cand, report, detail)
    assert report["summary"]["changed_rows"] == expected_changed
    assert report["summary"]["unchanged"] == len(cases) - expected_changed
    by_rid = {d["row_id"]: d for d in detail}
    # int -> string: both values present with their JSON types preserved
    e = by_rid[1]["fields"][0]
    assert e == {"path": "/f", "base": 1, "candidate": "1"}
    # int -> bool
    e = by_rid[2]["fields"][0]
    assert e == {"path": "/f", "base": 1, "candidate": True}
    # null -> 0
    e = by_rid[5]["fields"][0]
    assert e == {"path": "/f", "base": None, "candidate": 0}


def nested_and_pointer_paths(wd):
    """Nested diffs get stable JSON-Pointer paths; weird keys are escaped."""
    base_succ = [
        succ(0, {"a": {"b": {"c": [1, 2, {"d": "x"}]}}, "keep": 1}),
        succ(1, {"a/b": {"~key": 1}, "": 2, "标签": {"嵌套": True}}),
        succ(2, {"arr": [1, 2, 3]}),
        succ(3, {"obj": {"only_base": 1}}),
    ]
    cand_succ = [
        succ(0, {"a": {"b": {"c": [1, 2, {"d": "y"}]}}, "keep": 1}),
        succ(1, {"a/b": {"~key": 2}, "": 2, "标签": {"嵌套": False}}),
        succ(2, {"arr": [1, 2, 3, 4]}),
        succ(3, {"obj": {"only_cand": 1}}),
    ]
    base = synth_run(wd, "base", base_succ, [])
    cand = synth_run(wd, "cand", cand_succ, [])
    cfg = compare_config(wd, base, cand)
    res = do_compare(wd, cfg)
    assert res.returncode == RC_OK, res.stderr
    report, detail = read_report(wd)
    assert_reference_agrees(wd, base, cand, report, detail)
    by_rid = {d["row_id"]: d for d in detail}
    assert by_rid[0]["fields"] == [
        {"path": "/a/b/c/2/d", "base": "x", "candidate": "y"}]
    paths1 = {f["path"] for f in by_rid[1]["fields"]}
    assert paths1 == {"/a~1b/~0key", "/标签/嵌套"}, paths1
    assert by_rid[2]["fields"] == [{"path": "/arr/3", "candidate": 4}]
    fields3 = sorted(by_rid[3]["fields"], key=lambda f: f["path"])
    assert fields3 == [
        {"path": "/obj/only_base", "base": 1},
        {"path": "/obj/only_cand", "candidate": 1},
    ]
    # per-field summary aggregates by top-level output field
    cf = report["summary"]["changed_fields"]
    assert cf == {"a": 1, "a/b": 1, "arr": 1, "obj": 1, "标签": 1}, cf


def failure_reason_semantics(wd):
    """Runner fields never count as changes; business error identity does."""
    n = 0

    def rid():
        nonlocal n
        n += 1
        return n - 1

    base_fail, cand_fail = [], []
    # same business cause, different runner mechanics -> unchanged
    base_fail.append(fail(rid(), "batch_timeout", batch_id=3, attempts=2,
                          log=[{"attempt": 1, "cause": "batch_timeout"},
                               {"attempt": 2, "cause": "batch_timeout"}],
                          pid=111, tmp_path="/tmp/work/1"))
    cand_fail.append(fail(0, "batch_timeout", batch_id=9, attempts=1,
                          log=[{"attempt": 1, "cause": "batch_timeout"}],
                          pid=999, tmp_path="/var/tmp/other",
                          elapsed_ms=1234, hostname="worker-7"))
    # business cause changed -> failure_reason_changed
    base_fail.append(fail(rid(), "batch_timeout"))
    cand_fail.append(fail(1, "worker_crash"))
    # business detail changed -> failure_reason_changed
    base_fail.append(fail(rid(), "intake_error",
                          detail="invalid JSON: Expecting value"))
    cand_fail.append(fail(2, "intake_error",
                          detail="invalid JSON: Expecting property name"))
    # plugin-added business field changed -> failure_reason_changed
    base_fail.append(fail(rid(), "plugin_error:plugin_exception",
                          plugin_message="ValueError: bad ts"))
    cand_fail.append(fail(3, "plugin_error:plugin_exception",
                          plugin_message="ValueError: bad amount"))
    # identical -> unchanged
    base_fail.append(fail(rid(), "worker_crash"))
    cand_fail.append(fail(4, "worker_crash"))
    base = synth_run(wd, "base", [], base_fail)
    cand = synth_run(wd, "cand", [], cand_fail)
    cfg = compare_config(wd, base, cand)
    res = do_compare(wd, cfg)
    assert res.returncode == RC_OK, res.stderr
    report, detail = read_report(wd)
    assert_reference_agrees(wd, base, cand, report, detail)
    s = report["summary"]
    assert s["rows_compared"] == 5
    assert s["unchanged"] == 2, s
    assert s["failure_reason_changed"] == 3, s
    # evidence carries the normalised business error, runner fields stripped
    e = next(d for d in detail if d["row_id"] == 0 - 0 + 1)
    assert e["error"]["base"] == {"cause": "batch_timeout"}
    assert e["error"]["candidate"] == {"cause": "worker_crash"}


def empty_and_edge_streams(wd):
    """Empty runs, one-sided streams, no trailing newline, huge lines."""
    # 1) completely empty runs (0 input rows)
    base = synth_run(wd, "base", [], [])
    cand = synth_run(wd, "cand", [], [])
    cfg = compare_config(wd, base, cand)
    res = do_compare(wd, cfg)
    assert res.returncode == RC_OK, res.stderr
    report, detail = read_report(wd)
    assert report["summary"]["rows_compared"] == 0
    assert report["summary"]["changed_rows"] == 0 and detail == []

    # 2) all-failure vs all-failure (zero success rows), no trailing newline
    b_fail = [fail(i, "batch_timeout") for i in range(50)]
    c_fail = [fail(i, "batch_timeout") for i in range(50)]
    c_fail[7] = fail(7, "worker_crash")
    base = synth_run(wd, "base2", [], b_fail, trailing_newline=False)
    cand = synth_run(wd, "cand2", [], c_fail, trailing_newline=False)
    cfg = compare_config(wd, base, cand)
    res = do_compare(wd, cfg)
    assert res.returncode == RC_OK, res.stderr
    report, detail = read_report(wd)
    assert report["summary"]["failure_reason_changed"] == 1
    assert report["summary"]["unchanged"] == 49

    # 3) all-success vs all-success (zero failure rows)
    base = synth_run(wd, "base3", [succ(i, {"v": i}) for i in range(30)], [])
    cand = synth_run(wd, "cand3", [succ(i, {"v": i}) for i in range(30)], [])
    cfg = compare_config(wd, base, cand)
    res = do_compare(wd, cfg)
    assert res.returncode == RC_OK, res.stderr
    report, _ = read_report(wd)
    assert report["summary"]["unchanged"] == 30

    # 4) very long lines (a single ~3 MiB record) compare correctly
    big_val = "x" * (3 << 20)
    base = synth_run(wd, "base4", [succ(0, {"big": big_val}),
                                   succ(1, {"big": big_val, "k": 1})], [])
    cand = synth_run(wd, "cand4", [succ(0, {"big": big_val}),
                                   succ(1, {"big": big_val, "k": 2})], [])
    cfg = compare_config(wd, base, cand)
    res = do_compare(wd, cfg)
    assert res.returncode == RC_OK, res.stderr
    report, detail = read_report(wd)
    assert report["summary"]["unchanged"] == 1
    assert report["summary"]["success_fields_changed"] == 1
    assert detail[0]["fields"] == [{"path": "/k", "base": 1, "candidate": 2}]

    # 5) a line beyond the configured max_line_bytes is a data error
    base = synth_run(wd, "base5", [succ(0, {"big": "y" * 9000})], [])
    cand = synth_run(wd, "cand5", [succ(0, {"big": "y" * 9000})], [])
    cfg = compare_config(wd, base, cand, extra={"max_line_bytes": 4096},
                       report_dir="recon5")
    res = do_compare(wd, cfg)
    assert res.returncode == RC_DATA, (res.returncode, res.stderr)
    assert "line_too_long" in res.stderr
    assert not os.path.exists(wd.p("recon5", "report.json"))

    # 6) degenerate self-comparison: a run reconciled against itself
    cfg = compare_config(wd, base, base, report_dir="recon6")
    res = do_compare(wd, cfg)
    assert res.returncode == RC_OK, res.stderr
    report, detail = read_report(wd, "recon6")
    assert report["summary"]["rows_compared"] == 1
    assert report["summary"]["changed_rows"] == 0 and detail == []


def unicode_and_sources(wd):
    """Unicode field names/values and source-mismatch detection."""
    s0 = {"id": 0, "名前": "値✓", "emoji": "🚀"}
    base = synth_run(wd, "base", [succ(0, {"结果": "甲", "lst": ["é", "β"]},
                                       s0)], [])
    cand = synth_run(wd, "cand", [succ(0, {"结果": "乙", "lst": ["é", "β"]},
                                       s0)], [])
    cfg = compare_config(wd, base, cand)
    res = do_compare(wd, cfg)
    assert res.returncode == RC_OK, res.stderr
    report, detail = read_report(wd)
    assert detail == [{"row_id": 0, "class": "success_fields_changed",
                       "fields": [{"path": "/结果", "base": "甲",
                                   "candidate": "乙"}]}]
    blob = open(wd.p("recon", report["detail"]["ref"]), encoding="utf-8").read()
    assert "结果" in blob and "甲" in blob  # ensure_ascii=False preserved

    # same row_id but different source records -> data error, no report
    cand2 = synth_run(wd, "cand2", [succ(0, {"结果": "甲"}, {"id": 0, "x": 1})],
                      [])
    cfg2 = compare_config(wd, base, cand2, report_dir="recon2")
    res = do_compare(wd, cfg2)
    assert res.returncode == RC_DATA, (res.returncode, res.stderr)
    assert "source_mismatch" in res.stderr
    assert not os.path.exists(wd.p("recon2", "report.json"))


def manifest_validation(wd):
    """Every manifest-level inconsistency is a data error (exit 4)."""
    good_succ = [succ(i, {"v": i}) for i in range(6)]
    good_fail = [fail(6, "batch_timeout"), fail(7, "worker_crash")]

    def fresh_pair(base_mut=None, cand_mut=None, cand_kw=None):
        base = synth_run(wd, "base", list(good_succ), list(good_fail),
                         manifest_mutate=base_mut)
        kw = dict(cand_kw or {})
        cand = synth_run(wd, "cand", list(good_succ), list(good_fail),
                         manifest_mutate=cand_mut, **kw)
        return base, cand

    def expect_data_error(tag, base_mut=None, cand_mut=None, cand_kw=None,
                          reason=None):
        shutil.rmtree(wd.p("base"), ignore_errors=True)
        shutil.rmtree(wd.p("cand"), ignore_errors=True)
        base, cand = fresh_pair(base_mut, cand_mut, cand_kw)
        cfg = compare_config(wd, base, cand, report_dir=f"recon_{tag}")
        res = do_compare(wd, cfg)
        assert res.returncode == RC_DATA, (tag, res.returncode, res.stderr)
        if reason:
            assert reason in res.stderr, (tag, reason, res.stderr)
        assert not os.path.exists(wd.p(f"recon_{tag}", "report.json")), tag
        err = last_error(wd, f"recon_{tag}")
        assert err and err["status"] == "error", tag
        assert err["error"]["kind"] == "data_error", tag

    def set_status(m, s):
        def mut(man):
            man["status"] = s
        return mut

    expect_data_error("cancelled", base_mut=set_status(None, "cancelled"),
                      reason="run_not_completed")
    expect_data_error("aborted", cand_mut=set_status(None, "aborted"),
                      reason="run_not_completed")

    def abort_reason(man):
        man["abort_reason"] = "worker spawn failed repeatedly"

    expect_data_error("abort_reason", base_mut=abort_reason,
                      reason="run_not_completed")

    def ungated(man):
        man["input_source"] = "configured_input"
        man["input_snapshot"] = None

    expect_data_error("ungated", base_mut=ungated,
                      reason="run_not_bound_to_snapshot")

    def bad_snap(man):
        man["input_snapshot"]["sha256"] = "not-a-digest"

    expect_data_error("bad_snap", cand_mut=bad_snap,
                      reason="run_not_bound_to_snapshot")
    expect_data_error("snap_diff", cand_kw={"snapshot_sha": SNAP_B},
                      reason="snapshot_mismatch")

    def partial(man):
        man["delivered_rows_success"] = 4  # 4+2 != 8 input rows

    expect_data_error("partial", base_mut=partial, reason="partial_delivery")

    def bad_last(man):
        man["last_delivered_row_id"] = 6

    expect_data_error("bad_last", cand_mut=bad_last,
                      reason="inconsistent_counts")

    def no_plugin(man):
        man["plugin"] = None

    expect_data_error("no_plugin", base_mut=no_plugin,
                      reason="incomplete_run_identity")

    def empty_plugin(man):
        man["plugin"] = {"name": "", "version": "1.0"}

    expect_data_error("empty_plugin", cand_mut=empty_plugin,
                      reason="incomplete_run_identity")

    def no_outputs(man):
        del man["outputs"]

    expect_data_error("no_outputs", base_mut=no_outputs,
                      reason="incomplete_outputs")

    def bad_out_rows(man):
        man["outputs"]["success"]["rows"] = 99

    expect_data_error("bad_out_rows", cand_mut=bad_out_rows,
                      reason="inconsistent_counts")

    def bad_out_sha(man):
        man["outputs"]["failure"]["sha256"] = "zz"

    expect_data_error("bad_out_sha", base_mut=bad_out_sha,
                      reason="incomplete_outputs")

    def fewer_rows(man):
        # candidate claims 7 input rows while baseline claims 8
        man["input_rows_read"] = 7
        man["delivered_rows_success"] = 5
        man["delivered_rows_failure"] = 2
        man["last_delivered_row_id"] = 6
        man["outputs"]["success"]["rows"] = 5
        man["outputs"]["failure"]["rows"] = 2

    expect_data_error("coverage", cand_mut=fewer_rows,
                      reason="coverage_mismatch")

    def not_json(man):
        pass  # handled below

    # manifest file is not JSON at all
    base, cand = fresh_pair()
    with open(cand["manifest"], "w") as fh:
        fh.write("{broken")
    cfg = compare_config(wd, base, cand, report_dir="recon_broken")
    res = do_compare(wd, cfg)
    assert res.returncode == RC_DATA and "manifest_invalid" in res.stderr

    # manifest file missing
    os.unlink(cand["manifest"])
    cfg = compare_config(wd, base, cand, report_dir="recon_missing")
    res = do_compare(wd, cfg)
    assert res.returncode == RC_DATA and "manifest_unreadable" in res.stderr


def artifact_verification(wd):
    """Digest/count verification of the four delivered streams."""
    base = synth_run(wd, "base", [succ(i, {"v": i}) for i in range(5)],
                     [fail(5, "batch_timeout")])
    cand = synth_run(wd, "cand", [succ(i, {"v": i}) for i in range(5)],
                     [fail(5, "batch_timeout")])

    # 1) tamper the candidate success file after the manifest was written
    with open(cand["success"], "a", encoding="utf-8") as fh:
        fh.write("")  # no-op to keep counts; now real tamper:
    with open(cand["success"], "r+b") as fh:
        fh.seek(0)
        first = fh.read(1)
        fh.seek(0)
        fh.write(b"0" if first != b"0" else b"1")
    cfg = compare_config(wd, base, cand)
    res = do_compare(wd, cfg)
    assert res.returncode == RC_DATA, (res.returncode, res.stderr)
    assert "artifact_digest_mismatch" in res.stderr or \
        "invalid_json" in res.stderr or "invalid_record" in res.stderr
    assert not os.path.exists(wd.p("recon", "report.json"))

    # 2) manifest digest does not match a regenerated file
    base = synth_run(wd, "base2", [succ(i, {"v": i}) for i in range(5)],
                     [fail(5, "batch_timeout")])
    cand = synth_run(wd, "cand2", [succ(i, {"v": i}) for i in range(5)],
                     [fail(5, "batch_timeout")])

    def wrong_digest(man):
        man["outputs"]["success"]["sha256"] = "00" * 32

    cand = synth_run(wd, "cand2", [succ(i, {"v": i}) for i in range(5)],
                     [fail(5, "batch_timeout")], manifest_mutate=wrong_digest)
    cfg = compare_config(wd, base, cand, report_dir="recon2")
    res = do_compare(wd, cfg)
    assert res.returncode == RC_DATA and "artifact_digest_mismatch" in res.stderr

    # 3) artifact file missing entirely
    os.unlink(cand["failure"])
    cfg = compare_config(wd, base, cand, report_dir="recon3")
    res = do_compare(wd, cfg)
    assert res.returncode == RC_DATA and "artifact_unreadable" in res.stderr

    # 4) success/failure swapped in the compare config
    base = synth_run(wd, "base4", [succ(i, {"v": i}) for i in range(5)],
                     [fail(5, "batch_timeout")])
    cand = synth_run(wd, "cand4", [succ(i, {"v": i}) for i in range(5)],
                     [fail(5, "batch_timeout")])
    cfg_path = compare_config(wd, base, cand, report_dir="recon4")
    cfg = json.load(open(cfg_path))
    cfg["candidate"]["success"], cfg["candidate"]["failure"] = (
        cfg["candidate"]["failure"], cfg["candidate"]["success"])
    json.dump(cfg, open(cfg_path, "w"))
    res = do_compare(wd, cfg_path)
    assert res.returncode == RC_DATA, (res.returncode, res.stderr)


def stream_integrity(wd):
    """Row-identity violations inside streams are data errors, never
    silently zipped by position."""
    good_base = synth_run(wd, "base", [succ(i, {"v": i}) for i in range(6)],
                          [])

    def run_case(tag, cand_succ, cand_fail, reason, mutate=None):
        shutil.rmtree(wd.p("cand"), ignore_errors=True)
        cand = synth_run(wd, "cand", cand_succ, cand_fail,
                         manifest_mutate=mutate)
        cfg = compare_config(wd, good_base, cand, report_dir=f"recon_{tag}")
        res = do_compare(wd, cfg)
        assert res.returncode == RC_DATA, (tag, res.returncode, res.stderr)
        assert reason in res.stderr, (tag, reason, res.stderr)
        assert not os.path.exists(wd.p(f"recon_{tag}", "report.json")), tag
        assert list_temp(wd, f"recon_{tag}") == [], tag

    s = [succ(i, {"v": i}) for i in range(6)]
    # duplicate row_id inside the success stream
    dup = [succ(i, {"v": i}) for i in [0, 1, 2, 2, 3, 4]]
    run_case("dup_succ", dup, [], "duplicate_row_id")
    # duplicate inside the failure stream
    run_case("dup_fail", [succ(i, {"v": i}) for i in range(4)],
             [fail(4, "x"), fail(4, "x")], "duplicate_row_id")
    # out-of-order success stream
    oo = [succ(i, {"v": i}) for i in [0, 1, 3, 2, 4, 5]]
    run_case("oo_succ", oo, [], "row_id_out_of_order")
    # out-of-order failure stream
    run_case("oo_fail", [succ(i, {"v": i}) for i in range(4)],
             [fail(5, "x"), fail(4, "x")], "row_id_out_of_order")
    # row_id beyond the delivered range
    run_case("oor", [succ(i, {"v": i}) for i in [0, 1, 2, 3, 4, 6]], [],
             "row_id_out_of_range")
    # negative row_id
    run_case("neg", [succ(i, {"v": i}) for i in [-1, 0, 1, 2, 3, 4]], [],
             "row_id_out_of_range")
    # non-integer row_id
    bad = succ(0, {"v": 0})
    bad["row_id"] = "0"
    run_case("strid", [bad] + [succ(i, {"v": i}) for i in range(1, 6)], [],
             "invalid_row_id")
    bad2 = succ(0, {"v": 0})
    bad2["row_id"] = True
    run_case("boolid", [bad2] + [succ(i, {"v": i}) for i in range(1, 6)], [],
             "invalid_row_id")

    def claims_six_success(man):
        # the manifest claims full coverage that the stream does not have
        man["input_rows_read"] = 6
        man["delivered_rows_success"] = 6
        man["delivered_rows_failure"] = 0
        man["last_delivered_row_id"] = 5
        man["outputs"]["success"]["rows"] = 6
        man["outputs"]["failure"]["rows"] = 0

    # a gap: row 3 never delivered (manifest claims full coverage)
    run_case("gap", [succ(i, {"v": i}) for i in [0, 1, 2, 4, 5]], [],
             "missing_row_id", mutate=claims_six_success)
    # trailing rows missing
    run_case("tail", [succ(i, {"v": i}) for i in range(4)], [],
             "missing_row_id", mutate=claims_six_success)
    # same row delivered as success AND failure within one run
    run_case("conflict", [succ(i, {"v": i}) for i in range(5)],
             [fail(2, "batch_timeout")], "identity_conflict")
    # corrupt JSONL inside a stream
    run_case("broken", ['{"row_id": 0, BROKEN'] + s[1:], [], "invalid_json")
    # empty line inside a stream
    run_case("emptyline", [s[0], s[1], "", s[2], s[3], s[4]], [], "empty_line")
    # non-object record
    run_case("nonobj", ["[1, 2]"] + s[1:], [], "invalid_record")
    # success record without an output object
    noout = {"row_id": 0, "source": src(0)}
    run_case("noout", [noout] + s[1:], [], "invalid_record")
    # failure record with a non-object error
    baderr = {"row_id": 5, "source": src(5), "error": "boom"}
    run_case("baderr", s[:5], [baderr], "invalid_record")


def config_errors(wd):
    """Malformed compare configs are config errors (exit 3), distinct from
    data errors."""
    base = synth_run(wd, "base", [succ(0, {"v": 0})], [])
    cand = synth_run(wd, "cand", [succ(0, {"v": 0})], [])

    def expect_config_error(tag, mutate=None, raw=None):
        if raw is not None:
            cfg_path = wd.p(f"cfg_{tag}.json")
            with open(cfg_path, "w") as fh:
                fh.write(raw)
        else:
            cfg_path = compare_config(wd, base, cand,
                                      report_dir=f"recon_{tag}")
            cfg = json.load(open(cfg_path))
            mutate(cfg)
            json.dump(cfg, open(cfg_path, "w"))
        res = do_compare(wd, cfg_path)
        assert res.returncode == RC_CONFIG, (tag, res.returncode, res.stderr)
        assert not os.path.exists(wd.p(f"recon_{tag}", "report.json")), tag

    expect_config_error("unknown_top", lambda c: c.update({"bogus": 1}))
    expect_config_error("unknown_side",
                        lambda c: c["baseline"].update({"bogus": 1}))
    expect_config_error("missing_side_key",
                        lambda c: c["candidate"].pop("success"))
    expect_config_error("no_report_dir", lambda c: c.pop("report_dir"))
    expect_config_error("bad_block",
                        lambda c: c.update({"read_block_bytes": 10}))
    expect_config_error("bad_maxline",
                        lambda c: c.update({"max_line_bytes": "big"}))
    expect_config_error("not_object", raw="[1, 2]")
    expect_config_error("broken_json", raw="{nope")

    # report_dir exists as a regular file
    blocker = wd.p("blocked")
    with open(blocker, "w") as fh:
        fh.write("x")
    expect_config_error("report_dir_file",
                        lambda c: c.update({"report_dir": blocker}))
    # temp_dir exists as a regular file
    expect_config_error("temp_dir_file",
                        lambda c: c.update({"temp_dir": blocker}))
    # config file itself missing
    res = do_compare(wd, wd.p("does_not_exist.json"))
    assert res.returncode == RC_CONFIG


def atomic_publish(wd):
    """Failures never touch a valid old report; success replaces the group."""
    base = synth_run(wd, "base", [succ(i, {"v": i}) for i in range(10)], [])
    cand = synth_run(wd, "cand", [succ(i, {"v": i + 1}) for i in range(10)],
                     [])
    cfg = compare_config(wd, base, cand)
    res = do_compare(wd, cfg)
    assert res.returncode == RC_OK, res.stderr
    report_v1, detail_v1 = read_report(wd)
    blob_v1 = open(wd.p("recon", "report.json"), "rb").read()
    changes_v1 = changes_files(wd)
    assert len(changes_v1) == 1
    assert report_v1["summary"]["success_fields_changed"] == 10

    # determinism: identical inputs -> byte-identical report, reused detail
    res = do_compare(wd, cfg)
    assert res.returncode == RC_OK
    assert open(wd.p("recon", "report.json"), "rb").read() == blob_v1
    assert changes_files(wd) == changes_v1

    # break the candidate (digest mismatch) -> old report survives byte-wise
    with open(cand["success"], "r+b") as fh:
        fh.seek(0)
        b = fh.read(1)
        fh.seek(0)
        fh.write(b"9" if b != b"9" else b"8")
    res = do_compare(wd, cfg)
    assert res.returncode == RC_DATA
    assert open(wd.p("recon", "report.json"), "rb").read() == blob_v1
    assert changes_files(wd) == changes_v1
    err = last_error(wd)
    assert err["error"]["kind"] == "data_error"
    assert list_temp(wd) == []

    # fix the candidate with *different* results -> new group replaces old
    cand2 = synth_run(wd, "cand", [succ(i, {"v": i + 2}) for i in range(10)],
                      [])
    res = do_compare(wd, cfg)
    assert res.returncode == RC_OK, res.stderr
    report_v2, _ = read_report(wd)
    assert report_v2["detail"]["sha256"] != report_v1["detail"]["sha256"]
    new_changes = changes_files(wd)
    assert len(new_changes) == 1 and new_changes != changes_v1  # old GC'd
    assert last_error(wd) is None  # superseded by the success
    # the published detail is read-only (immutable artifact)
    detail_path = wd.p("recon", report_v2["detail"]["ref"])
    assert not (os.stat(detail_path).st_mode & 0o222)


def publish_interruption(wd):
    """A failed publish rolls back the new detail and keeps the old report."""
    base = synth_run(wd, "base", [succ(i, {"v": i}) for i in range(8)], [])
    cand = synth_run(wd, "cand", [succ(i, {"v": i}) for i in range(8)], [])
    cfg = compare_config(wd, base, cand)
    assert do_compare(wd, cfg).returncode == RC_OK
    blob_v1 = open(wd.p("recon", "report.json"), "rb").read()
    changes_v1 = changes_files(wd)

    # make the changes store read-only: the detail rename must fail and the
    # staged temp file must be cleaned up, report untouched
    cand2 = synth_run(wd, "cand", [succ(i, {"v": i + 1}) for i in range(8)],
                      [])
    os.chmod(wd.p("recon", "changes"), 0o555)
    try:
        res = do_compare(wd, cfg)
    finally:
        os.chmod(wd.p("recon", "changes"), 0o755)
    assert res.returncode == RC_EXEC, (res.returncode, res.stderr)
    assert "publish_error" in res.stderr
    assert open(wd.p("recon", "report.json"), "rb").read() == blob_v1
    assert changes_files(wd) == changes_v1
    assert list_temp(wd) == []

    # report.json replaced by a directory: report rename fails after the
    # detail was published -> the new detail is rolled back
    os.unlink(wd.p("recon", "report.json"))
    os.makedirs(wd.p("recon", "report.json"))
    res = do_compare(wd, cfg)
    assert res.returncode == RC_EXEC, (res.returncode, res.stderr)
    assert changes_files(wd) == changes_v1  # new detail removed again
    assert list_temp(wd) == []
    shutil.rmtree(wd.p("recon", "report.json"))

    # temp_dir not creatable -> execution error, old report still intact
    blocked = wd.p("blocked_tmp")
    os.makedirs(blocked)
    os.chmod(blocked, 0o000)
    cfg2 = compare_config(wd, base, cand, report_dir="recon2",
                          extra={"temp_dir": blocked})
    try:
        res = do_compare(wd, cfg2)
    finally:
        os.chmod(blocked, 0o700)
    assert res.returncode == RC_EXEC, (res.returncode, res.stderr)
    assert not os.path.exists(wd.p("recon2", "report.json"))


def input_mutation(wd):
    """In-place rewrite mid-compare is refused; a replaced path still
    consumes the originally opened byte version."""
    rows = 60000
    big_out = {"v": "payload-" + "z" * 200}
    base_succ = [succ(i, big_out) for i in range(rows)]
    cand_succ = [succ(i, big_out) for i in range(rows)]
    base = synth_run(wd, "base", base_succ, [])
    cand = synth_run(wd, "cand", cand_succ, [])
    cfg = compare_config(wd, base, cand)

    # --- case 1: in-place rewrite of the candidate stream during compare
    proc = subprocess.Popen(
        [PY, "-m", "colbatch.cli", "compare", "--config", cfg],
        cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if list_temp(wd):
            break
        time.sleep(0.02)
    time.sleep(0.2)  # let the merge get going
    with open(cand["success"], "r+b") as fh:
        fh.seek(1 << 20)
        chunk = fh.read(64)
        fh.seek(1 << 20)
        fh.write(chunk.replace(b"z", b"Q", 1))
    out, err = proc.communicate(timeout=120)
    assert proc.returncode in (RC_DATA, RC_EXEC), (proc.returncode, err)
    assert not os.path.exists(wd.p("recon", "report.json")), \
        "a mutated input must never publish a report"
    assert list_temp(wd) == []

    # --- case 2: path replaced mid-compare -> the opened fd keeps the
    # original bytes; the comparison completes against that one version
    cand2 = synth_run(wd, "cand2", cand_succ, [])
    cfg2 = compare_config(wd, base, cand2, report_dir="recon2")
    orig_digest = _sha256_of(cand2["success"])
    proc = subprocess.Popen(
        [PY, "-m", "colbatch.cli", "compare", "--config", cfg2],
        cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    # wait until the merge actually started (staged detail exists), proving
    # all four streams were already opened
    deadline = time.monotonic() + 30
    staged = None
    while time.monotonic() < deadline:
        tmps = list_temp(wd, "recon2")
        if tmps:
            staged = tmps[0]
            break
        time.sleep(0.02)
    assert staged, "comparator never started staging"
    replacement = wd.p("replacement.jsonl")
    write_jsonl(replacement, [succ(i, {"v": "TOTALLY DIFFERENT"})
                              for i in range(rows)])
    os.replace(replacement, cand2["success"])
    out, err = proc.communicate(timeout=120)
    assert proc.returncode == RC_OK, (proc.returncode, err)
    report, _ = read_report(wd, "recon2")
    # the report describes the originally opened byte version, not the
    # replacement that landed at the same path mid-run
    assert report["runs"]["candidate"]["outputs"]["success"]["sha256"] == \
        orig_digest
    assert report["summary"]["unchanged"] == rows


def cancel_cleanup(wd):
    """SIGINT mid-compare cleans staged artifacts and publishes nothing."""
    rows = 150000
    recs = [succ(i, {"v": "p" * 100}) for i in range(rows)]
    base = synth_run(wd, "base", recs, [])
    cand = synth_run(wd, "cand", recs, [])
    cfg = compare_config(wd, base, cand)
    proc = subprocess.Popen(
        [PY, "-m", "colbatch.cli", "compare", "--config", cfg],
        cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if list_temp(wd):
            break
        time.sleep(0.02)
    proc.send_signal(signal.SIGINT)
    try:
        rc = proc.wait(timeout=60)
    except subprocess.TimeoutExpired:
        proc.kill()
        raise AssertionError("compare did not react to SIGINT")
    assert rc in (RC_EXEC, 130), rc
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and list_temp(wd):
        time.sleep(0.05)
    assert list_temp(wd) == []
    assert not os.path.exists(wd.p("recon", "report.json"))


def compare_status_query(wd):
    """The read-only query validates report + detail as a group."""
    base = synth_run(wd, "base", [succ(i, {"v": i}) for i in range(4)], [])
    cand = synth_run(wd, "cand", [succ(i, {"v": i}) for i in range(4)], [])
    cfg = compare_config(wd, base, cand)

    # no report yet -> exit 4 (and mentions the absence)
    res = run_cli("compare-status", "--config", cfg)
    assert res.returncode == RC_DATA, (res.returncode, res.stderr)
    assert "no_report" in res.stderr

    assert do_compare(wd, cfg).returncode == RC_OK
    res = run_cli("compare-status", "--config", cfg)
    assert res.returncode == RC_OK, res.stderr
    assert json.loads(res.stdout)["summary"]["rows_compared"] == 4

    # corrupt the detail file -> the query reports the mismatch
    report, _ = read_report(wd)
    detail_path = wd.p("recon", report["detail"]["ref"])
    os.chmod(detail_path, 0o600)
    with open(detail_path, "r+b") as fh:
        fh.seek(0)
        fh.write(b" ")
    res = run_cli("compare-status", "--config", cfg)
    assert res.returncode == RC_DATA and "detail_mismatch" in res.stderr

    # delete the detail file
    os.unlink(detail_path)
    res = run_cli("compare-status", "--config", cfg)
    assert res.returncode == RC_DATA

    # corrupt the report itself
    with open(wd.p("recon", "report.json"), "w") as fh:
        fh.write("{broken")
    res = run_cli("compare-status", "--config", cfg)
    assert res.returncode == RC_DATA and "report_unreadable" in res.stderr

    # config error is still exit 3 for the query command
    res = run_cli("compare-status", "--config", wd.p("missing.json"))
    assert res.returncode == RC_CONFIG


def scale_bounded_memory(wd):
    """Large inputs: exact results, bounded RSS, staging cleaned up."""
    rows = 60000
    filler = "q" * 120

    def gen(side):
        # generator: the test process itself never holds the run in memory,
        # which keeps its own maxrss floor out of the child measurement
        for i in range(rows):
            out = {"amount": f"{i}.00", "score": i % 13, "pad": filler,
                   "meta": {"a": i, "b": [i, i + 1]}}
            if side == "cand":
                if i % 7 == 0:
                    out["score"] = (i % 13) + 1
                if i % 11 == 0:
                    out["meta"] = {"a": i, "b": [i + 1, i]}
            yield succ(i, out)

    base = synth_run(wd, "base", gen("base"), [])
    cand = synth_run(wd, "cand", gen("cand"), [])
    cfg = compare_config(wd, base, cand, report_dir="recon")
    res = do_compare(wd, cfg)
    assert res.returncode == RC_OK, res.stderr

    # Measure the product's peak RSS in a fresh process BEFORE this test
    # process loads any bulk data (report detail / reference): a forked
    # child inherits the parent's maxrss high-water mark and would report
    # the parent's footprint instead of the comparator's.
    rss_kb = _measure_child_rss(cfg)
    input_bytes = os.path.getsize(base["success"]) + \
        os.path.getsize(cand["success"])

    report, detail = read_report(wd)

    # exact expected counts from the generator
    n_field = sum(1 for i in range(rows) if i % 7 == 0 or i % 11 == 0)
    s = report["summary"]
    assert s["rows_compared"] == rows
    assert s["success_fields_changed"] == n_field
    assert s["unchanged"] == rows - n_field
    assert len(detail) == n_field
    # cross-check against the independent reference (run in a subprocess so
    # this test process does not inherit its in-memory footprint)
    ref_json = wd.p("ref.json")
    driver = (
        "import json,sys;"
        f"sys.path.insert(0,{os.path.dirname(os.path.abspath(__file__))!r});"
        "import reference_reconcile as r;"
        "ref=r.reference_reconcile("
        f"r.load_stream({base['success']!r}),r.load_stream({base['failure']!r}),"
        f"r.load_stream({cand['success']!r}),r.load_stream({cand['failure']!r}));"
        "json.dump(ref,open(sys.argv[1],'w'))"
    )
    proc = subprocess.run([PY, "-I", "-c", driver, ref_json], cwd=ROOT,
                          capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    with open(ref_json) as fh:
        ref = json.load(fh)
    assert report["summary"] == ref["summary"]
    assert detail == ref["detail"]

    # bounded-memory proof: fresh-process peak RSS of the product stays a
    # small near-constant footprint, far below the stream sizes
    bound_kb = max(80_000, input_bytes // 4 // 1024)
    assert rss_kb < bound_kb, \
        f"product RSS {rss_kb} KB exceeded bound {bound_kb} KB"
    # staging area is fully cleaned; only the published group remains
    assert list_temp(wd) == []
    assert len(changes_files(wd)) == 1
    print(f"\n    scale: {rows} rows/side, {input_bytes/1e6:.1f} MB streams, "
          f"product peak RSS {rss_kb/1024:.1f} MB")


def _measure_child_rss(cfg_path):
    """Peak RSS (KB) of a compare subprocess (fresh interpreter).

    The child reports its own VmHWM (the kernel high-water mark of the
    post-exec address space) rather than getrusage(): ``ru_maxrss`` is
    inherited across fork/exec, so a test parent that has itself held
    large data would silently floor the child's reported value.  VmHWM
    belongs to the fresh post-exec mm and reflects only the comparator's
    own footprint.
    """
    driver = (
        "import runpy,sys;"
        f"sys.path.insert(0,{ROOT!r});"
        "sys.argv=['colbatch','compare','--config',sys.argv[1]];"
        "rc=0\n"
        "try:\n"
        " runpy.run_module('colbatch.cli',run_name='__main__')\n"
        "except SystemExit as e:\n"
        " rc=e.code or 0\n"
        "hwm=0\n"
        "with open('/proc/self/status') as _fh:\n"
        " for _l in _fh:\n"
        "  if _l.startswith('VmHWM:'): hwm=int(_l.split()[1])\n"
        "print('PEAK_RSS_KB',hwm,file=sys.stderr);sys.exit(rc)"
    )
    proc = subprocess.run([PY, "-I", "-c", driver, cfg_path], cwd=ROOT,
                          capture_output=True, text=True)
    assert proc.returncode == RC_OK, proc.stderr
    for line in proc.stderr.splitlines():
        if line.startswith("PEAK_RSS_KB"):
            return int(line.split()[1])
    raise AssertionError(f"no RSS marker in stderr:\n{proc.stderr}")


def real_runs_end_to_end(wd):
    """Full pipeline: preflight gate -> two gated runs -> compare."""
    columns = [
        {"name": "kid", "type": "int64", "field": "kid"},
        {"name": "cat", "type": "utf8", "field": "cat"},
        {"name": "price", "type": "utf8", "field": "price"},
        {"name": "note", "type": "utf8", "field": "note"},
        {"name": "nested", "type": "utf8", "field": "nested"},
    ]
    rows = []
    for i in range(120):
        rows.append(json.dumps({
            "kid": i, "cat": f"cat{i % 5}", "price": f"{i}.5",
            "note": f"n{i}", "nested": "1"}, ensure_ascii=False))
    with open(wd.p("input.jsonl"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(rows) + "\n")

    def run_cfg(name, plugin):
        cfg = {
            "input": wd.p("input.jsonl"),
            "success_out": wd.p(name, "success.jsonl"),
            "failure_out": wd.p(name, "failure.jsonl"),
            "log_dir": wd.p(name, "logs"),
            "stats_path": wd.p(name, "logs", "stats.json"),
            "manifest_path": wd.p(name, "logs", "manifest.json"),
            "plugin": {"path": os.path.join(ROOT, "tests", plugin)},
            "columns": columns,
            "batch_size": 16,
            "workers": 2,
            "quality": {
                "enabled": True,
                "columns": columns,
                "rules": [{"id": "r", "rule": "max_missing_ratio",
                           "column": "note", "value": 1.0}],
            },
        }
        path = wd.p(f"run_{name}.json")
        with open(path, "w") as fh:
            json.dump(cfg, fh)
        return path

    base_cfg = run_cfg("base", "echo_plugin.py")
    cand_cfg = run_cfg("cand", "echo_plugin_v2.py")
    # one preflight publishes the admitted report both runs bind to
    res = run_cli("preflight", "--config", base_cfg)
    assert res.returncode == RC_OK, res.stderr
    res = run_cli("run", "--config", base_cfg)
    assert res.returncode == 0, res.stderr
    res = run_cli("run", "--config", cand_cfg)
    assert res.returncode == 0, res.stderr

    base = {"manifest": wd.p("base", "logs", "manifest.json"),
            "success": wd.p("base", "success.jsonl"),
            "failure": wd.p("base", "failure.jsonl")}
    cand = {"manifest": wd.p("cand", "logs", "manifest.json"),
            "success": wd.p("cand", "success.jsonl"),
            "failure": wd.p("cand", "failure.jsonl")}
    # the host manifest really records the new digest/identity fields
    m = json.load(open(base["manifest"]))
    assert m["outputs"]["success"]["rows"] == 120
    assert len(m["outputs"]["success"]["sha256"]) == 64
    assert m["plugin"] == {"name": "preflight_echo", "version": "1.0"}
    m2 = json.load(open(cand["manifest"]))
    assert m2["plugin"] == {"name": "preflight_echo", "version": "2.0"}
    assert m["input_snapshot"]["sha256"] == m2["input_snapshot"]["sha256"]

    cfg = compare_config(wd, base, cand)
    res = do_compare(wd, cfg)
    assert res.returncode == RC_OK, res.stderr
    report, detail = read_report(wd)
    assert_reference_agrees(wd, base, cand, report, detail)
    # the v2 plugin uppercases cat on kid % 10 == 0 -> exactly those change
    s = report["summary"]
    assert s["rows_compared"] == 120
    assert s["success_fields_changed"] == 12, s
    assert s["changed_fields"] == {"cat": 12}, s
    changed_rids = {d["row_id"] for d in detail}
    assert changed_rids == {i for i in range(120) if i % 10 == 0}
    # plugin identities of both runs are recorded for the audit
    assert report["runs"]["baseline"]["plugin"]["version"] == "1.0"
    assert report["runs"]["candidate"]["plugin"]["version"] == "2.0"


SCENARIOS = [
    ("cross_validate", cross_validate),
    ("json_type_semantics", json_type_semantics),
    ("nested_and_pointer_paths", nested_and_pointer_paths),
    ("failure_reason_semantics", failure_reason_semantics),
    ("empty_and_edge_streams", empty_and_edge_streams),
    ("unicode_and_sources", unicode_and_sources),
    ("manifest_validation", manifest_validation),
    ("artifact_verification", artifact_verification),
    ("stream_integrity", stream_integrity),
    ("config_errors", config_errors),
    ("atomic_publish", atomic_publish),
    ("publish_interruption", publish_interruption),
    ("input_mutation", input_mutation),
    ("cancel_cleanup", cancel_cleanup),
    ("compare_status_query", compare_status_query),
    ("scale_bounded_memory", scale_bounded_memory),
    ("real_runs_end_to_end", real_runs_end_to_end),
]


def main():
    selected = sys.argv[1:]
    scenarios = [s for s in SCENARIOS if not selected or s[0] in selected]
    failed = []
    t0 = time.monotonic()
    for name, fn in scenarios:
        wd = Workdir(name)
        print(f"[ RUN  ] {name}  ({wd.path})", flush=True)
        ok = True
        try:
            fn(wd)
        except Exception:
            ok = False
            failed.append(name)
            traceback.print_exc()
        print(f"[ {'FAIL' if not ok else 'PASS'} ] {name}", flush=True)
        wd.cleanup(failed=not ok)
    dt = time.monotonic() - t0
    print(f"\n{len(scenarios) - len(failed)}/{len(scenarios)} scenarios passed "
          f"in {dt:.1f}s")
    if failed:
        print("failed:", ", ".join(failed))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
