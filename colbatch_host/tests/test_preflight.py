#!/usr/bin/env python3
"""Behavioural and cross-validation test-suite for colbatch preflight.

Every scenario builds a fresh temp directory, generates its input
dynamically (large inputs are generated at test time; the repo only carries
small fixtures), drives the real ``colbatch`` CLI as a subprocess, and
cross-checks the product report against the independent in-memory reference
profiler in ``reference_profiler.py``.

Run:  python3 tests/test_preflight.py                 # all scenarios
      python3 tests/test_preflight.py determinism gate_block   # selected
Env:  KEEP=1 keeps temp dirs of failing scenarios.
      COLBATCH_PYTHON overrides the interpreter (defaults to sys.executable).
"""

from __future__ import annotations

import json
import os
import resource
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import traceback

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import reference_profiler as refp  # noqa: E402

PY = os.environ.get("COLBATCH_PYTHON", sys.executable)
TEST_TMP = os.environ.get("COLBATCH_TEST_DIR", tempfile.gettempdir())

# Exit codes documented by the CLI.
RC_OK = 0
RC_QUALITY = 2
RC_CONFIG = 3
RC_DATA = 4
RC_EXEC = 5

COLUMNS = [
    {"name": "kid", "type": "int64", "field": "kid"},
    {"name": "cat", "type": "utf8", "field": "cat"},
    {"name": "price", "type": "float64", "field": "price"},
    {"name": "note", "type": "utf8", "field": "note"},
    {"name": "nested", "type": "utf8", "field": "nested"},
]
ID_FIELD = "kid"


# ---------------------------------------------------------------------------
# helpers
#


class Workdir:
    def __init__(self, name):
        self.path = tempfile.mkdtemp(prefix=f"colpf_{name}_", dir=TEST_TMP)

    def p(self, *parts):
        return os.path.join(self.path, *parts)

    def cleanup(self, failed):
        if failed or os.environ.get("KEEP"):
            return
        shutil.rmtree(self.path, ignore_errors=True)


def write_input(wd, rows_gen, name="input.jsonl", no_trailing_newline=False):
    path = wd.p(name)
    lines = list(rows_gen)
    with open(path, "w", encoding="utf-8") as fh:
        if not lines:
            return path  # genuinely empty file: zero bytes, zero lines
        first = True
        for line in lines:
            if not first:
                fh.write("\n")
            fh.write(line)
            first = False
        if not no_trailing_newline:
            fh.write("\n")
    return path


def make_config(wd, rules, *, input_name="input.jsonl", relative_input=False,
                quality_enabled=True,
                blank_policy="whitespace", id_field=ID_FIELD, extra_quality=None,
                float_columns=()):
    # Host plugin-schema columns stay utf8/int64; the quality section gets
    # its own column list where the preflight-only float64 type is allowed.
    host_columns = [
        {**c, "type": "utf8"} if c["name"] in float_columns or c["type"] == "float64"
        else dict(c)
        for c in COLUMNS
    ]
    quality_columns = [dict(c) for c in COLUMNS]
    cfg = {
        "input": input_name if relative_input else wd.p(input_name),
        "success_out": wd.p("success.jsonl"),
        "failure_out": wd.p("failure.jsonl"),
        "log_dir": wd.p("logs"),
        "stats_path": wd.p("logs", "stats.json"),
        "manifest_path": wd.p("logs", "manifest.json"),
        "plugin": {"path": os.path.join(ROOT, "tests", "echo_plugin.py")},
        "columns": host_columns,
        "batch_size": 16,
        "workers": 2,
        "quality": {
            "enabled": quality_enabled,
            "blank_policy": blank_policy,
            "id_field": id_field,
            "columns": quality_columns,
            "rules": rules,
        },
    }
    if extra_quality:
        cfg["quality"].update(extra_quality)
    path = wd.p("run.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, indent=1)
    return path, cfg


def run_cli(wd, *args, env_extra=None, stdin=None):
    env = dict(os.environ)
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        [PY, "-m", "colbatch.cli", *args],
        cwd=ROOT, capture_output=True, text=True, env=env, stdin=stdin,
    )


def preflight(wd, rules=None, cfg_path=None, **kw):
    if cfg_path is None:
        cfg_path, _ = make_config(wd, rules or [], **kw)
    return run_cli(wd, "preflight", "--config", cfg_path), cfg_path


def load_report(wd):
    with open(wd.p(".colbatch_preflight", "report.json"), encoding="utf-8") as fh:
        return json.load(fh)


def report_at(path):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def snapshot_files(wd):
    sdir = wd.p(".colbatch_preflight", "snapshots")
    return sorted(os.listdir(sdir)) if os.path.isdir(sdir) else []


def list_temp(wd):
    tdir = wd.p(".colbatch_preflight", "tmp")
    out = []
    if os.path.isdir(tdir):
        for root, _dirs, files in os.walk(tdir):
            for f in files:
                out.append(os.path.join(root, f))
    return out


def reference_for(cfg, input_path):
    with open(input_path, encoding="utf-8") as fh:
        lines = [ln.rstrip("\n") for ln in fh]
    return refp.reference_profile(
        lines,
        [{"name": c["name"], "field": c["field"],
          "type": "float64" if c["name"] == "price" else c["type"]}
         for c in COLUMNS],
        blank_mode=cfg["quality"]["blank_policy"],
        id_field=cfg["quality"].get("id_field"),
    )


def assert_matches_reference(report, ref):
    """Compare every shared statistic against the independent reference."""
    assert report["summary"]["record_count"] == ref["record_count"]
    assert report["summary"]["bad_line_count"] == ref["bad_line_count"]
    if ref["id"]:
        rid = report["id"]
        assert rid["present"] == ref["id"]["present"]
        assert rid["distinct"] == ref["id"]["distinct"]
        assert rid["duplicates"] == ref["id"]["duplicates"]
    for col in report["columns"]:
        r = ref["columns"][col["name"]]
        assert col["present"] == r["present"], col["name"]
        assert col["missing"]["total"] == r["missing_total"], col["name"]
        assert col["missing"]["missing_field"] == r["missing_field"]
        assert col["missing"]["null"] == r["null"]
        assert col["missing"]["blank_string"] == r["blank_string"]
        assert abs(col["missing"]["ratio"] - r["missing_ratio"]) < 1e-12
        assert col["distinct"] == r["distinct"], (
            col["name"], col["distinct"], r["distinct"])
        assert abs(col["distinct_ratio"] - r["distinct_ratio"]) < 1e-12
        assert col["parse_failures"] == r["parse_failures"]
        if col["numeric"] is not None:
            assert col["numeric"]["min"] == r["numeric_min"]
            assert col["numeric"]["max"] == r["numeric_max"]
            assert col["numeric"]["count"] == r["numeric_count"]


def assert_rule_agreement(report, ref, rules):
    admitted, ref_results = refp.reference_evaluate(rules, ref)
    ref_by_id = {r["id"]: r for r in ref_results}
    for rule in report["rules"]:
        rr = ref_by_id[rule["id"]]
        assert rule["passed"] == rr["passed"], (rule["id"], rule, rr)
        if isinstance(rr["actual"], float):
            assert abs(rule["actual"] - rr["actual"]) < 1e-12
        else:
            assert rule["actual"] == rr["actual"], (rule["id"], rule["actual"], rr["actual"])
    assert report["status"] == ("admitted" if admitted else "rejected")


# ---------------------------------------------------------------------------
# data generators
#


def gen_mixed(rows, seed=1):
    """Multi-batch input with repeats, types, missing/null/blank, Unicode."""
    import random
    rng = random.Random(seed)
    cats = ["alpha", "βήτα", "gamma", "delta", "1", "true", ""]
    for i in range(rows):
        rec = {"kid": i % (rows // 3 + 1)}  # deliberate duplicate ids
        rec["cat"] = rng.choice(cats)
        kind = rng.random()
        if kind < 0.12:
            rec["price"] = None
        elif kind < 0.20:
            rec["price"] = rng.choice(["N/A", "12.3.4"])  # non-numeric strings
        elif kind < 0.30:
            rec["price"] = rng.randint(-100, 100)  # ints in a float64 column
        else:
            rec["price"] = round(rng.uniform(-900.0, 900.0), 4)
        if kind >= 0.80:
            rec["note"] = "   "  # blank under whitespace policy
        elif rng.random() < 0.1:
            rec.pop("note", None)  # absent field
        elif rng.random() < 0.1:
            rec["note"] = None
        else:
            rec["note"] = f"n{i % 7}"
        # nested values + type-collision probes: string "1" vs int 1 vs float 1.0
        rec["nested"] = rng.choice(
            [{"a": 1}, [1, 2], "1", 1, 1.0, True, "true", {"a": "1"}]
        )
        yield json.dumps(rec, ensure_ascii=False)


# ---------------------------------------------------------------------------
# scenarios
#


def cross_validate(wd):
    """Product report and independent reference agree on every statistic."""
    rules = [
        {"id": "r1", "rule": "max_missing_ratio", "column": "note", "value": 0.5},
        {"id": "r2", "rule": "min_distinct", "column": "cat", "value": 1},
        {"id": "r3", "rule": "max_distinct", "column": "kid", "value": 10 ** 9},
        # numeric rules on the always-numeric int64 kid column; the price
        # column deliberately contains non-numeric strings (handled by the
        # dedicated non_numeric_not_skipped scenario instead).
        {"id": "r4", "rule": "numeric_min", "column": "kid", "value": 0},
        {"id": "r5", "rule": "numeric_max", "column": "kid", "value": 200},
        {"id": "r6", "rule": "allowed_values", "column": "cat",
         "values": ["alpha", "βήτα", "gamma", "delta", "1", "true", ""]},
        {"id": "r7", "rule": "max_duplicate_ids", "column": "kid", "value": 10 ** 6},
        {"id": "r8", "rule": "min_distinct_ratio", "column": "note", "value": 0.0},
        {"id": "r9", "rule": "max_distinct_ratio", "column": "kid", "value": 1.0},
    ]
    write_input(wd, gen_mixed(600, seed=3))
    (cfg_path, cfg) = make_config(wd, rules, float_columns=("price",))
    res = run_cli(wd, "preflight", "--config", cfg_path)
    assert res.returncode == RC_OK, res.stderr
    report = load_report(wd)
    ref = reference_for(cfg, wd.p("input.jsonl"))
    assert_matches_reference(report, ref)
    assert_rule_agreement(report, ref, rules)


def type_collision(wd):
    """Same textual form across JSON types must never collide."""
    rows = [
        json.dumps({"kid": 1, "cat": "1", "price": 1.0, "note": "x", "nested": "1"}),
        json.dumps({"kid": 1, "cat": 1, "price": 1, "note": "x", "nested": 1}),
        json.dumps({"kid": 1, "cat": 1.0, "price": 1, "note": "x", "nested": 1.0}),
        json.dumps({"kid": 1, "cat": True, "price": 1, "note": "x", "nested": True}),
        json.dumps({"kid": 1, "cat": "true", "price": 1, "note": "x", "nested": "true"}),
        json.dumps({"kid": 1, "cat": "1", "price": 1, "note": "x",
                    "nested": {"a": 1}}),
        json.dumps({"kid": 1, "cat": "1", "price": 1, "note": "x",
                    "nested": {"a": "1"}}),
    ]
    write_input(wd, rows)
    (cfg_path, cfg) = make_config(wd, [], float_columns=("price",))
    res = run_cli(wd, "preflight", "--config", cfg_path)
    assert res.returncode == RC_OK, res.stderr
    report = load_report(wd)
    ref = reference_for(cfg, wd.p("input.jsonl"))
    assert_matches_reference(report, ref)
    # cat: "1", 1, 1.0, true, "true" -> 5 distinct; nested: "1",1,1.0,true,
    # "true", {"a":1}, {"a":"1"} -> 7 distinct.
    cat = next(c for c in report["columns"] if c["name"] == "cat")
    nested = next(c for c in report["columns"] if c["name"] == "nested")
    assert cat["distinct"] == 5, cat
    assert nested["distinct"] == 7, nested


def missing_semantics(wd):
    """Absent / null / blank counted separately; policy changes the totals."""
    rows = [
        json.dumps({"kid": 1, "cat": "a", "price": 1.0, "nested": 1}),          # note absent
        json.dumps({"kid": 2, "cat": "a", "price": 1.0, "note": None, "nested": 1}),
        json.dumps({"kid": 3, "cat": "a", "price": 1.0, "note": "", "nested": 1}),
        json.dumps({"kid": 4, "cat": "a", "price": 1.0, "note": "   ", "nested": 1}),
        json.dumps({"kid": 5, "cat": "a", "price": 1.0, "note": "x", "nested": 1}),
    ]
    write_input(wd, rows)
    for mode, expect_blank in (("whitespace", 2), ("empty", 1), ("off", 0)):
        (cfg_path, cfg) = make_config(
            wd, [], blank_policy=mode, float_columns=("price",))
        res = run_cli(wd, "preflight", "--config", cfg_path)
        assert res.returncode == RC_OK, (mode, res.stderr)
        note = next(c for c in load_report(wd)["columns"] if c["name"] == "note")
        assert note["missing"]["missing_field"] == 1
        assert note["missing"]["null"] == 1
        assert note["missing"]["blank_string"] == expect_blank, (mode, note)
        assert note["present"] == 5 - 2 - expect_blank
        assert note["missing"]["total"] == 2 + expect_blank


def boundaries(wd):
    """Equality at a rule boundary passes; one step beyond fails."""
    rows = [
        json.dumps({"kid": 0, "cat": "a", "price": 0.0, "note": "x", "nested": 1}),
        json.dumps({"kid": -5, "cat": "a", "price": -1.5, "note": "x", "nested": 1}),
        json.dumps({"kid": 9223372036854775807, "cat": "a", "price": 10.25,
                    "note": "x", "nested": 1}),
        json.dumps({"kid": -9223372036854775808, "cat": "a", "price": 0.0,
                    "note": "x", "nested": 1}),
    ]
    write_input(wd, rows)
    rules_eq = [
        {"id": "min", "rule": "numeric_min", "column": "price", "value": -1.5},
        {"id": "max", "rule": "numeric_max", "column": "price", "value": 10.25},
        {"id": "miss", "rule": "max_missing_ratio", "column": "note", "value": 0.0},
    ]
    (cfg_path, cfg) = make_config(wd, rules_eq, float_columns=("price",))
    res = run_cli(wd, "preflight", "--config", cfg_path)
    assert res.returncode == RC_OK, res.stderr  # equality passes
    assert load_report(wd)["status"] == "admitted"

    rules_over = [
        {"id": "min", "rule": "numeric_min", "column": "price", "value": -1.49},
        {"id": "max", "rule": "numeric_max", "column": "price", "value": 10.24},
    ]
    (cfg_path2, _) = make_config(wd, rules_over, float_columns=("price",))
    res = run_cli(wd, "preflight", "--config", cfg_path2)
    assert res.returncode == RC_QUALITY
    rep = load_report(wd)
    assert {r["id"] for r in rep["rules"] if not r["passed"]} == {"min", "max"}
    # huge int64 boundary values were profiled exactly
    kid = next(c for c in rep["columns"] if c["name"] == "kid")
    assert kid["numeric"]["min"] == -9223372036854775808
    assert kid["numeric"]["max"] == 9223372036854775807


def non_numeric_not_skipped(wd):
    """Non-numeric values fail numeric rules instead of being skipped."""
    rows = [
        json.dumps({"kid": 1, "cat": "a", "price": "N/A", "note": "x", "nested": 1}),
        json.dumps({"kid": 2, "cat": "a", "price": 5.0, "note": "x", "nested": 1}),
    ]
    write_input(wd, rows)
    rules = [{"id": "max", "rule": "numeric_max", "column": "price", "value": 100.0}]
    (cfg_path, _) = make_config(wd, rules, float_columns=("price",))
    res = run_cli(wd, "preflight", "--config", cfg_path)
    assert res.returncode == RC_QUALITY
    rule = load_report(wd)["rules"][0]
    assert rule["passed"] is False and "non-numeric" in rule["reason"]


def bad_lines_and_no_newline(wd):
    """Bad JSON lines are a data error; missing final newline is well-defined."""
    good = [json.dumps({"kid": i, "cat": "a", "price": 1.0,
                        "note": "x", "nested": 1}) for i in range(4)]
    lines = good[:2] + ['{"kid": 3, BROKEN', '   ', '{"kid": 5} not json'] + good[2:]
    write_input(wd, lines)
    (cfg_path, _) = make_config(wd, [], float_columns=("price",))
    res = run_cli(wd, "preflight", "--config", cfg_path)
    assert res.returncode == RC_DATA, res.stderr
    err = report_at(wd.p(".colbatch_preflight", "last_error.json"))
    assert err["status"] == "error" and err["error"]["kind"] == "data_error"
    assert err["error"]["detail"]["bad_lines"]["count"] == 3
    # no report/snapshot pair may be published on a data error
    assert not os.path.exists(wd.p(".colbatch_preflight", "report.json"))
    assert snapshot_files(wd) == []
    assert list_temp(wd) == []

    # final line without trailing newline still counts exactly once
    write_input(wd, good, no_trailing_newline=True)
    res = run_cli(wd, "preflight", "--config", cfg_path)
    assert res.returncode == RC_OK, res.stderr
    assert load_report(wd)["summary"]["record_count"] == 4


def empty_dataset(wd):
    """Empty input: zero rows, deterministic zeroed stats, ratio rules pass."""
    write_input(wd, [])
    rules = [
        {"id": "m", "rule": "max_missing_ratio", "column": "note", "value": 0.0},
        {"id": "d", "rule": "min_distinct", "column": "cat", "value": 0},
    ]
    (cfg_path, cfg) = make_config(wd, rules, float_columns=("price",))
    res = run_cli(wd, "preflight", "--config", cfg_path)
    assert res.returncode == RC_OK, res.stderr
    rep = load_report(wd)
    assert rep["summary"]["record_count"] == 0
    assert all(c["distinct"] == 0 and c["missing"]["ratio"] == 0.0
               for c in rep["columns"])
    ref = reference_for(cfg, wd.p("input.jsonl"))
    assert_matches_reference(rep, ref)


def determinism(wd):
    """Same file + rules -> byte-identical business report, twice."""
    write_input(wd, gen_mixed(400, seed=11))
    rules = [
        {"id": "m", "rule": "max_missing_ratio", "column": "note", "value": 0.4},
        {"id": "d", "rule": "min_distinct", "column": "cat", "value": 2},
    ]
    (cfg_path, _) = make_config(wd, rules, float_columns=("price",))
    r1 = run_cli(wd, "preflight", "--config", cfg_path)
    assert r1.returncode in (RC_OK, RC_QUALITY)
    rep1 = open(wd.p(".colbatch_preflight", "report.json"), "rb").read()
    snaps1 = snapshot_files(wd)
    r2 = run_cli(wd, "preflight", "--config", cfg_path)
    assert r2.returncode == r1.returncode
    rep2 = open(wd.p(".colbatch_preflight", "report.json"), "rb").read()
    assert rep1 == rep2, "report content must be deterministic"
    assert snapshot_files(wd) == snaps1, "identical input must reuse one snapshot"


def config_errors(wd):
    """Unknown columns and type mismatches are config errors, not failures."""
    write_input(wd, gen_mixed(20, seed=2))
    bad_rules, expected_fragment = [
        ([{"id": "x", "rule": "max_missing_ratio", "column": "ghost",
           "value": 0.1}], "unknown column"),
        ([{"id": "x", "rule": "numeric_min", "column": "cat", "value": 1}],
         "numeric column"),
        ([{"id": "x", "rule": "min_distinct", "column": "cat", "value": -1}],
         "non-negative integer"),
        ([{"id": "x", "rule": "max_missing_ratio", "column": "cat",
           "value": 1.5}], "[0, 1]"),
        ([{"id": "x", "rule": "allowed_values", "column": "kid",
           "values": ["not-an-int"]}], "not a valid int64"),
    ], None
    for rules, fragment in bad_rules:
        (cfg_path, _) = make_config(wd, rules, float_columns=("price",))
        res = run_cli(wd, "preflight", "--config", cfg_path)
        assert res.returncode == RC_CONFIG, (rules, res.returncode, res.stderr)
        assert fragment in res.stderr, (fragment, res.stderr)
    # no report published for config errors
    assert not os.path.exists(wd.p(".colbatch_preflight", "report.json"))


def gate_block_and_snapshot_input(wd):
    """Rejected report blocks run; admitted run reads the bound snapshot."""
    write_input(wd, gen_mixed(120, seed=4))
    # rule guaranteed to reject
    rules = [{"id": "strict", "rule": "max_missing_ratio",
              "column": "note", "value": 0.0001}]
    (cfg_path, _) = make_config(wd, rules, float_columns=("price",))
    res = run_cli(wd, "preflight", "--config", cfg_path)
    assert res.returncode == RC_QUALITY
    run = run_cli(wd, "run", "--config", cfg_path)
    assert run.returncode == 1 and "quality gate blocked" in run.stderr
    assert not os.path.exists(wd.p("success.jsonl"))
    assert not os.path.exists(wd.p("failure.jsonl"))

    # relax the rule -> admit; then mutate the *source*: run must still use
    # the immutable snapshot, not the changed file.
    rules2 = [{"id": "loose", "rule": "max_missing_ratio",
               "column": "note", "value": 1.0}]
    (cfg2, _) = make_config(wd, rules2, float_columns=("price",))
    res = run_cli(wd, "preflight", "--config", cfg2)
    assert res.returncode == RC_OK, res.stderr
    report = load_report(wd)
    with open(wd.p("input.jsonl"), "ab") as fh:
        fh.write(b'{"kid": 999, "cat": "injected", "price": 1.0, '
                 b'"note": "x", "nested": 1}\n')
    run = run_cli(wd, "run", "--config", cfg2)
    assert run.returncode == 0, run.stderr
    manifest = report_at(wd.p("logs", "manifest.json"))
    assert manifest["input_source"] == "quality_gate_snapshot"
    assert manifest["input_snapshot"]["sha256"] == \
        report["input"]["snapshot"]["sha256"]
    # 120 profiled rows processed despite the 121st injected source row
    assert manifest["input_rows_read"] == report["summary"]["record_count"] == 120


def stale_summary_and_corruption(wd):
    """Rule mismatch, missing report and corrupted snapshot all block run."""
    write_input(wd, gen_mixed(80, seed=5))
    (cfg_path, _) = make_config(
        wd, [{"id": "a", "rule": "min_distinct", "column": "cat", "value": 1}],
        float_columns=("price",))
    assert run_cli(wd, "preflight", "--config", cfg_path).returncode == RC_OK
    rep = load_report(wd)

    # 1) tamper the rule summary
    tampered = wd.p(".colbatch_preflight", "report.json")
    bad = dict(rep); bad["rule_summary"] = "sha256:deadbeef"
    json.dump(bad, open(tampered, "w"))
    run = run_cli(wd, "run", "--config", cfg_path)
    assert run.returncode == 1 and "rule_summary" in run.stderr

    # 2) restore, then corrupt the snapshot
    json.dump(rep, open(tampered, "w"))
    snap = wd.p(".colbatch_preflight", "snapshots", snapshot_files(wd)[0])
    os.chmod(snap, 0o600)
    with open(snap, "r+b") as fh:
        fh.seek(0); fh.write(b"X")
    run = run_cli(wd, "run", "--config", cfg_path)
    assert run.returncode == 1 and "snapshot" in run.stderr.lower()

    # 3) missing report
    os.unlink(tampered)
    run = run_cli(wd, "run", "--config", cfg_path)
    assert run.returncode == 1 and "no report" in run.stderr


def source_changes_during_copy(wd):
    """A source replaced while copying must not publish a snapshot/report."""
    # Large input so the copy window is wide enough to mutate reliably.
    big = wd.p("big.jsonl")
    with open(big, "w", encoding="utf-8") as fh:
        for i in range(200000):
            fh.write(json.dumps({"kid": i, "cat": "x" * 200, "price": 1.0,
                                 "note": "n", "nested": 1}) + "\n")
    rules = [{"id": "a", "rule": "min_distinct", "column": "cat", "value": 1}]
    (cfg_path, _) = make_config(
        wd, rules, input_name="big.jsonl", float_columns=("price",))

    proc = subprocess.Popen(
        [PY, "-m", "colbatch.cli", "preflight", "--config", cfg_path],
        cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    # Replace the source mid-flight (same path, fresh inode + content).
    time.sleep(0.15)
    replacement = wd.p("big2.jsonl")
    with open(replacement, "w", encoding="utf-8") as fh:
        for i in range(50):
            fh.write(json.dumps({"kid": i, "cat": "other", "price": 2.0,
                                 "note": "n", "nested": 1}) + "\n")
    os.replace(replacement, big)
    out, err = proc.communicate(timeout=60)
    assert proc.returncode == RC_EXEC, (proc.returncode, err)
    assert "changed" in err or "replaced" in err or "size" in err, err
    assert not os.path.exists(wd.p(".colbatch_preflight", "report.json"))
    assert snapshot_files(wd) == []
    assert list_temp(wd) == []


def atomic_group_replace(wd):
    """Re-running keeps a valid old pair until the new pair is fully published."""
    write_input(wd, gen_mixed(60, seed=6))
    (cfg_path, _) = make_config(
        wd, [{"id": "a", "rule": "min_distinct", "column": "cat", "value": 1}],
        float_columns=("price",))
    assert run_cli(wd, "preflight", "--config", cfg_path).returncode == RC_OK
    rep1 = load_report(wd)
    snap1 = snapshot_files(wd)
    assert len(snap1) == 1

    # different content -> new digest pair; old report remains readable until
    # the atomic rename and the orphaned old snapshot is GC'd afterwards.
    with open(wd.p("input.jsonl"), "a", encoding="utf-8") as fh:
        for i in range(20):
            fh.write(json.dumps({"kid": i, "cat": f"new{i}", "price": 1.0,
                                 "note": "x", "nested": 1}) + "\n")
    assert run_cli(wd, "preflight", "--config", cfg_path).returncode == RC_OK
    rep2 = load_report(wd)
    assert rep2["input"]["snapshot"]["sha256"] != \
        rep1["input"]["snapshot"]["sha256"]
    snaps = snapshot_files(wd)
    assert len(snaps) == 1 and snaps != snap1  # old unreferenced snapshot GC'd
    # every report on disk always points at an existing snapshot
    sha = rep2["input"]["snapshot"]["sha256"]
    assert f"snap-{sha}.jsonl" in snaps

    # a foreign file in the store is never deleted by GC
    foreign = wd.p(".colbatch_preflight", "snapshots", "foreign-task.txt")
    open(foreign, "w").write("keep me")
    assert run_cli(wd, "preflight", "--config", cfg_path).returncode == RC_OK
    assert os.path.exists(foreign)


def no_absolute_paths(wd):
    """Reports carry stable relative identifiers, never machine paths."""
    write_input(wd, gen_mixed(30, seed=7))
    (cfg_path, _) = make_config(wd, [], float_columns=("price",))
    assert run_cli(wd, "preflight", "--config", cfg_path).returncode == RC_OK
    blob = open(wd.p(".colbatch_preflight", "report.json"), encoding="utf-8").read()
    assert wd.path not in blob
    report = json.loads(blob)
    assert report["input"]["snapshot"]["ref"].startswith("snapshots/snap-")
    # input next to the config renders as a plain relative id
    assert report["input"]["source_ref"] == "input.jsonl"

    # an input *outside* the config dir gets the stable external marker,
    # never the machine absolute path
    ext_dir = tempfile.mkdtemp(prefix="colpf_external_", dir=TEST_TMP)
    ext_input = os.path.join(ext_dir, "outside.jsonl")
    with open(ext_input, "w", encoding="utf-8") as fh:
        for line in gen_mixed(10, seed=72):
            fh.write(line + "\n")
    (ext_cfg_path, ext_cfg) = make_config(wd, [], float_columns=("price",))
    ext_cfg["input"] = ext_input
    with open(ext_cfg_path, "w", encoding="utf-8") as fh:
        json.dump(ext_cfg, fh)
    assert run_cli(wd, "preflight", "--config", ext_cfg_path).returncode == RC_OK
    ext_report = load_report(wd)
    assert ext_report["input"]["source_ref"].startswith("external:")
    assert not ext_report["input"]["source_ref"].startswith("external:/")
    assert ext_dir not in open(wd.p(".colbatch_preflight", "report.json")).read()
    shutil.rmtree(ext_dir, ignore_errors=True)

    # when the input lives next to the config, it renders as a plain relative id
    rel_dir = wd.p("nested"); os.makedirs(rel_dir, exist_ok=True)
    rel_input = os.path.join("nested", "input.jsonl")
    write_input(wd, gen_mixed(10, seed=71), name=os.path.join("nested", "input.jsonl"))
    (rel_cfg, _) = make_config(
        wd, [], input_name=rel_input, relative_input=True,
        float_columns=("price",))
    assert run_cli(wd, "preflight", "--config", rel_cfg).returncode == RC_OK
    rel_report = load_report(wd)
    assert rel_report["input"]["source_ref"] == os.path.join("nested", "input.jsonl")
    assert wd.path not in open(wd.p(".colbatch_preflight", "report.json")).read()


def temp_failure_and_cleanup(wd):
    """An unwritable temp dir fails as an execution error, leaves nothing."""
    write_input(wd, gen_mixed(30, seed=8))
    blocked = wd.p("blocked_tmp")
    os.makedirs(blocked)
    os.chmod(blocked, 0o000)
    (cfg_path, _) = make_config(
        wd, [], float_columns=("price",),
        extra_quality={"temp_dir": blocked})
    res = run_cli(wd, "preflight", "--config", cfg_path)
    os.chmod(blocked, 0o700)
    assert res.returncode == RC_EXEC, (res.returncode, res.stderr)
    assert not os.path.exists(wd.p(".colbatch_preflight", "report.json"))
    assert snapshot_files(wd) == []


def cancel_cleanup(wd):
    """SIGINT mid-scan cleans every temp file and publishes nothing."""
    big = wd.p("big.jsonl")
    with open(big, "w", encoding="utf-8") as fh:
        for i in range(300000):
            fh.write(json.dumps({"kid": i, "cat": "x" * 100, "price": 1.0,
                                 "note": "n", "nested": 1}) + "\n")
    (cfg_path, _) = make_config(
        wd, [{"id": "a", "rule": "min_distinct", "column": "cat", "value": 1}],
        input_name="big.jsonl", float_columns=("price",))
    proc = subprocess.Popen(
        [PY, "-m", "colbatch.cli", "preflight", "--config", cfg_path],
        cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if list_temp(wd) or snapshot_files(wd):
            break
        time.sleep(0.02)
    proc.send_signal(signal.SIGINT)
    try:
        rc = proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        raise AssertionError("preflight did not react to SIGINT")
    assert rc in (RC_EXEC, 130), rc
    # allow the finally blocks a beat, then prove full cleanup
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and (list_temp(wd) or snapshot_files(wd)):
        time.sleep(0.05)
    assert list_temp(wd) == []
    assert snapshot_files(wd) == []
    assert not os.path.exists(wd.p(".colbatch_preflight", "report.json"))


def scale_bounded_memory(wd):
    """Large data stays exact while RSS is bounded; disk state is cleaned."""
    rows = 150000
    cats = [f"cat-{i}" for i in range(60)]
    notes = [f"note-{i}" for i in range(300)]

    def gen():
        for i in range(rows):
            yield json.dumps({
                "kid": i,
                "cat": cats[i % len(cats)],
                "price": (i % 701) - 350 + 0.5,
                "note": None if i % 11 == 0 else notes[i % len(notes)],
                "nested": {"k": i % 17, "v": f"s{i % 23}"},
            }, ensure_ascii=False)

    write_input(wd, gen())
    rules = [
        {"id": "miss", "rule": "max_missing_ratio", "column": "note",
         "value": 0.2},
        {"id": "dc", "rule": "min_distinct", "column": "cat", "value": 50},
        {"id": "lo", "rule": "numeric_min", "column": "price", "value": -350.0},
        {"id": "hi", "rule": "numeric_max", "column": "price", "value": 351.0},
        {"id": "dup", "rule": "max_duplicate_ids", "column": "kid", "value": 0},
        {"id": "allow", "rule": "allowed_values", "column": "cat",
         "values": cats},
    ]
    (cfg_path, cfg) = make_config(
        wd, rules, float_columns=("price",),
        extra_quality={"insert_batch_rows": 500, "read_block_bytes": 1 << 16})
    t0 = time.monotonic()
    res = run_cli(wd, "preflight", "--config", cfg_path)
    elapsed = time.monotonic() - t0
    assert res.returncode == RC_OK, res.stderr

    # Measure the product's peak RSS in a *fresh* process BEFORE this test
    # process loads the in-memory reference set.  A forked child otherwise
    # inherits (copy-on-write) the reference's pages and ru_maxrss would
    # report the parent's footprint, not the profiler's.
    rss_kb = _measure_child_rss(cfg_path)
    input_bytes = os.path.getsize(wd.p("input.jsonl"))
    # Same discipline for the independent reference, which deliberately
    # materialises every distinct key in RAM.
    ref_rss_kb = _measure_reference_rss(wd.p("input.jsonl"))

    report = load_report(wd)
    ref = reference_for(cfg, wd.p("input.jsonl"))
    assert_matches_reference(report, ref)
    assert_rule_agreement(report, ref, rules)

    # exact cardinalities on the big columns
    cat = next(c for c in report["columns"] if c["name"] == "cat")
    note = next(c for c in report["columns"] if c["name"] == "note")
    assert cat["distinct"] == 60
    assert note["distinct"] == 300
    assert report["id"]["distinct"] == rows and report["id"]["duplicates"] == 0

    # The product keeps no per-row state in memory, so its fresh-process
    # RSS is a small near-constant footprint -- comfortably below both the
    # independent reference (which materialises every distinct key in RAM)
    # and the on-disk input size.  This is the bounded-memory proof.
    bound_kb = max(45_000, input_bytes // 2 // 1024)
    assert rss_kb < bound_kb, f"product RSS {rss_kb} KB exceeded bound {bound_kb} KB"
    assert rss_kb < ref_rss_kb, (
        f"product RSS {rss_kb} KB should be below the in-memory reference "
        f"{ref_rss_kb} KB")

    # disk temp state existed during the run and is gone afterwards; the
    # published snapshot alone remains.
    assert list_temp(wd) == []
    assert len(snapshot_files(wd)) == 1
    print(f"\n    scale: {rows} rows, {input_bytes/1e6:.1f} MB input, "
          f"product peak RSS {rss_kb/1024:.1f} MB vs reference "
          f"{ref_rss_kb/1024:.1f} MB, {elapsed:.1f}s")


def _measure_reference_rss(input_path):
    """Fresh-process peak RSS of the in-memory independent reference."""
    cols = [{"name": c["name"], "field": c["field"],
             "type": ("float64" if c["name"] == "price" else c["type"])}
            for c in COLUMNS]
    driver = (
        "import json,resource,sys;"
        f"sys.path.insert(0,{os.path.dirname(os.path.abspath(__file__))!r});"
        "import reference_profiler as r;"
        f"cols={cols!r};"
        "lines=[l.rstrip(chr(10)) for l in open(sys.argv[1],encoding='utf-8')];"
        "r.reference_profile(lines,cols,'whitespace','kid');"
        "print('REF_PEAK_RSS_KB',"
        "resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,file=sys.stderr)")
    proc = subprocess.run(
        [PY, "-I", "-c", driver, input_path], cwd=ROOT,
        capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    for line in proc.stderr.splitlines():
        if line.startswith("REF_PEAK_RSS_KB"):
            return int(line.split()[1])
    raise AssertionError(f"no reference RSS marker:\n{proc.stderr}")


def _measure_child_rss(cfg_path):
    """Return peak RSS (KB) of a preflight subprocess.

    A small driver adds the project root to ``sys.path`` (needed under
    ``-I``), runs the CLI in-process, and prints its own ru_maxrss.
    """
    driver = (
        "import resource,runpy,sys;"
        f"sys.path.insert(0,{ROOT!r});"
        "sys.argv=['colbatch','preflight','--config',sys.argv[1]];"
        "rc=0\n"
        "try:\n"
        " runpy.run_module('colbatch.cli',run_name='__main__')\n"
        "except SystemExit as e:\n"
        " rc=e.code or 0\n"
        "print('PEAK_RSS_KB',resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,"
        "file=sys.stderr);sys.exit(rc)"
    )
    proc = subprocess.run(
        [PY, "-I", "-c", driver, cfg_path], cwd=ROOT, capture_output=True, text=True)
    assert proc.returncode == RC_OK, proc.stderr
    for line in proc.stderr.splitlines():
        if line.startswith("PEAK_RSS_KB"):
            return int(line.split()[1])
    raise AssertionError(f"no RSS marker in stderr:\n{proc.stderr}")


def gate_disabled_keeps_legacy_run(wd):
    """With the gate disabled, run never looks at preflight artifacts."""
    write_input(wd, gen_mixed(50, seed=9))
    (cfg_path, _) = make_config(wd, [], quality_enabled=False)
    # no preflight at all: legacy behaviour
    run = run_cli(wd, "run", "--config", cfg_path)
    assert run.returncode == 0, run.stderr
    manifest = report_at(wd.p("logs", "manifest.json"))
    assert manifest["input_source"] == "configured_input"
    assert manifest["input_snapshot"] is None


def gc_respects_other_reports(wd):
    """A snapshot still referenced by another stored report is never GC'd."""
    write_input(wd, gen_mixed(40, seed=61))
    (cfg_path, _) = make_config(
        wd, [{"id": "a", "rule": "min_distinct", "column": "cat", "value": 1}],
        float_columns=("price",))
    assert run_cli(wd, "preflight", "--config", cfg_path).returncode == RC_OK
    first_sha = snapshot_files(wd)[0]
    first_report = load_report(wd)

    # Preserve the first report under a second name in the work dir.
    saved_report = wd.p(".colbatch_preflight", "report_kept.json")
    shutil.copy(wd.p(".colbatch_preflight", "report.json"), saved_report)

    # Change the input -> a new digest pair; GC must retain the snapshot the
    # saved (other) report still references.
    with open(wd.p("input.jsonl"), "a", encoding="utf-8") as fh:
        for i in range(15):
            fh.write(json.dumps({"kid": i, "cat": f"fresh{i}", "price": 1.0,
                                 "note": "x", "nested": 1}) + "\n")
    assert run_cli(wd, "preflight", "--config", cfg_path).returncode == RC_OK
    remaining = snapshot_files(wd)
    assert first_sha in remaining, "another report's snapshot was wrongly GC'd"
    assert len(remaining) == 2
    # the saved report's binding still verifies on disk
    saved = report_at(saved_report)
    assert f"snap-{saved['input']['snapshot']['sha256']}.jsonl" in remaining


SCENARIOS = [
    ("cross_validate", cross_validate),
    ("type_collision", type_collision),
    ("missing_semantics", missing_semantics),
    ("boundaries", boundaries),
    ("non_numeric_not_skipped", non_numeric_not_skipped),
    ("bad_lines_and_no_newline", bad_lines_and_no_newline),
    ("empty_dataset", empty_dataset),
    ("determinism", determinism),
    ("config_errors", config_errors),
    ("gate_block_and_snapshot_input", gate_block_and_snapshot_input),
    ("stale_summary_and_corruption", stale_summary_and_corruption),
    ("source_changes_during_copy", source_changes_during_copy),
    ("atomic_group_replace", atomic_group_replace),
    ("gc_respects_other_reports", gc_respects_other_reports),
    ("no_absolute_paths", no_absolute_paths),
    ("temp_failure_and_cleanup", temp_failure_and_cleanup),
    ("cancel_cleanup", cancel_cleanup),
    ("scale_bounded_memory", scale_bounded_memory),
    ("gate_disabled_keeps_legacy_run", gate_disabled_keeps_legacy_run),
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
