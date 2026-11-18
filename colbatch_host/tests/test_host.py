#!/usr/bin/env python3
"""Fault-injection and behaviour test-suite for the colbatch host.

Every scenario builds a fresh temp directory, generates its input
dynamically, runs the real host as a subprocess, synchronises through the
host's stats file (polling, never fixed sleeps), and finishes by checking
the output against the independent reference transformer.

Run:  python3 tests/test_host.py              # all scenarios
      python3 tests/test_host.py barrier_stale cancel   # selected
Env:  KEEP=1 keeps temp dirs of failing scenarios.
"""

from __future__ import annotations

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
PY = os.environ.get("COLBATCH_PYTHON", sys.executable)
TEST_TMP = os.environ.get("COLBATCH_TEST_DIR", tempfile.gettempdir())

NORMALIZE_COLUMNS = [
    {"name": "raw_ts", "type": "utf8", "field": "raw_ts"},
    {"name": "raw_amount", "type": "utf8", "field": "raw_amount"},
]
CROSSFIELD_COLUMNS = [
    {"name": "qty", "type": "int64", "field": "qty"},
    {"name": "unit_price", "type": "utf8", "field": "unit_price"},
    {"name": "currency", "type": "utf8", "field": "currency"},
    {"name": "start_ts", "type": "utf8", "field": "start_ts"},
    {"name": "end_ts", "type": "utf8", "field": "end_ts"},
]
PROFILES = {
    "normalize": ("colbatch/plugins/normalize_text.py", NORMALIZE_COLUMNS),
    "crossfield": ("colbatch/plugins/crossfield_derive.py", CROSSFIELD_COLUMNS),
}


# ---------------------------------------------------------------------------
# helpers


def sh_gen(profile, rows, seed, out, malformed_every=0):
    cmd = [
        PY,
        os.path.join(ROOT, "scripts", "gen_input.py"),
        "--profile",
        profile,
        "--rows",
        str(rows),
        "--seed",
        str(seed),
        "--out",
        out,
    ]
    if malformed_every:
        cmd += ["--malformed-every", str(malformed_every)]
    subprocess.run(cmd, check=True, cwd=ROOT)


def make_cfg(workdir, profile, **over):
    plugin, columns = PROFILES[profile]
    cfg = {
        "input": os.path.join(workdir, "input.jsonl"),
        "success_out": os.path.join(workdir, "success.jsonl"),
        "failure_out": os.path.join(workdir, "failure.jsonl"),
        "log_dir": os.path.join(workdir, "logs"),
        "stats_path": os.path.join(workdir, "stats.json"),
        "manifest_path": os.path.join(workdir, "manifest.json"),
        "plugin": {"path": os.path.join(ROOT, plugin)},
        "columns": columns,
        "batch_size": 10,
        "workers": 2,
        "max_attempts": 2,
        "batch_timeout_ms": 2000,
        "kill_grace_ms": 1000,
        "handshake_timeout_ms": 20000,
        "bye_timeout_ms": 3000,
        "max_inflight_batches": 6,
        "max_pending_batches": 4,
        "max_pending_bytes": 1 << 24,
        "max_frame_bytes": 1 << 26,
        "writer_queue_batches": 2,
        "stats_interval_ms": 100,
        "rlimits": {"cpu_seconds": 0, "as_bytes": 0, "nofile": 0},
    }
    cfg.update(over)
    path = os.path.join(workdir, "run.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, indent=1)
    return path


def write_faults(workdir, spec):
    path = os.path.join(workdir, "faults.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(spec, fh)
    return path


def run_host(cfg_path, workdir):
    out = open(os.path.join(workdir, "host.stdout.log"), "w")
    err = open(os.path.join(workdir, "host.stderr.log"), "w")
    proc = subprocess.Popen(
        [PY, "-m", "colbatch.cli", "run", "--config", cfg_path],
        cwd=ROOT,
        stdout=out,
        stderr=err,
    )
    proc._out_fh = out
    proc._err_fh = err
    return proc


def wait_host(proc, timeout):
    try:
        rc = proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        raise AssertionError(f"host did not exit within {timeout}s")
    finally:
        proc._out_fh.close()
        proc._err_fh.close()
    return rc


def get_stats(workdir):
    with open(os.path.join(workdir, "stats.json"), "r", encoding="utf-8") as fh:
        return json.load(fh)


def get_manifest(workdir):
    with open(os.path.join(workdir, "manifest.json"), "r", encoding="utf-8") as fh:
        return json.load(fh)


def wait_stats(workdir, pred, timeout, desc):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = get_stats(workdir)
            if pred(last):
                return last
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        time.sleep(0.05)
    excerpt = json.dumps(last, indent=1)[:3000] if last else "<no stats>"
    raise AssertionError(f"timeout waiting for: {desc}\nlast stats:\n{excerpt}")


def collect_stats(workdir, proc, interval=0.1):
    """Sample stats while the host runs; returns list of snapshots."""
    samples = []
    while proc.poll() is None:
        try:
            samples.append(get_stats(workdir))
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        time.sleep(interval)
    try:
        samples.append(get_stats(workdir))
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    return samples


def verify(cfg_path, expect_failed=(), through_batch=None):
    cmd = [PY, os.path.join(ROOT, "tests", "verify.py"), "--config", cfg_path]
    if expect_failed:
        cmd += ["--expect-failed-batches", ",".join(str(b) for b in expect_failed)]
    if through_batch is not None:
        cmd += ["--through-batch", str(through_batch)]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        raise AssertionError(f"verify failed:\n{res.stdout}\n{res.stderr}")
    return res.stdout.strip()


def colbatch_processes():
    """PIDs of any leftover colbatch worker / fault-child processes."""
    found = []
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as fh:
                cmd = fh.read().decode("utf-8", "replace")
        except (FileNotFoundError, PermissionError):
            continue
        if "colbatch" in cmd and "worker.py" in cmd:
            found.append((int(pid), cmd[:120]))
        elif "SIG_IGN" in cmd and "time.sleep" in cmd:
            found.append((int(pid), cmd[:120]))
    return found


def assert_no_leftovers():
    # workers may take a moment to disappear after SIGKILL; poll briefly
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        left = colbatch_processes()
        if not left:
            return
        time.sleep(0.1)
    raise AssertionError(f"leftover processes: {left}")


def completed_entry(manifest, batch_id):
    for entry in manifest["completed_log"]:
        if entry["batch"] == batch_id:
            return entry
    return None


class Workdir:
    def __init__(self, name):
        self.name = name
        self.path = tempfile.mkdtemp(prefix=f"colbatch_{name}_", dir=TEST_TMP)

    def cleanup(self, failed):
        if failed or os.environ.get("KEEP"):
            return  # keep the workdir for inspection
        shutil.rmtree(self.path, ignore_errors=True)


# ---------------------------------------------------------------------------
# scenarios


def happy_normalize(wd):
    sh_gen("normalize", 200, 7, os.path.join(wd.path, "input.jsonl"))
    cfg = make_cfg(wd.path, "normalize")
    proc = run_host(cfg, wd.path)
    samples = collect_stats(wd.path, proc)
    rc = wait_host(proc, 60)
    assert rc == 0, f"exit code {rc}"
    m = get_manifest(wd.path)
    assert m["status"] == "completed", m["status"]
    assert m["delivered_rows_success"] == 200
    assert m["delivered_rows_failure"] == 0
    assert m["counters"]["clean_bye"] == 2, "workers should exit via BYE"
    assert m["hwm"]["retained_batches"] <= m["limits"]["max_inflight_batches"]
    assert all(e.get("maxrss_kb", 0) > 0 for e in m["worker_exit_log"]), "RSS proof"
    assert any(
        w.get("rss_bytes") for s in samples for w in s["workers"] if w["state"] != "dead"
    ), "live RSS sampled"
    verify(cfg)


def happy_crossfield(wd):
    sh_gen("crossfield", 200, 8, os.path.join(wd.path, "input.jsonl"))
    cfg = make_cfg(wd.path, "crossfield")
    proc = run_host(cfg, wd.path)
    rc = wait_host(proc, 60)
    assert rc == 0, f"exit code {rc}"
    m = get_manifest(wd.path)
    assert m["status"] == "completed"
    assert m["delivered_rows_success"] == 200
    verify(cfg)


def scale_normalize(wd):
    sh_gen("normalize", 5000, 21, os.path.join(wd.path, "input.jsonl"))
    cfg = make_cfg(wd.path, "normalize", batch_size=50, max_inflight_batches=8)
    proc = run_host(cfg, wd.path)
    samples = collect_stats(wd.path, proc, interval=0.05)
    rc = wait_host(proc, 120)
    assert rc == 0, f"exit code {rc}"
    m = get_manifest(wd.path)
    assert m["delivered_rows_success"] == 5000
    assert m["hwm"]["retained_batches"] <= 8
    for s in samples:
        g = s["gauges"]
        assert g["retained_batches"] <= 8, f"budget exceeded: {g}"
    verify(cfg)


def barrier_stale(wd):
    """Batch 2 completes first; batch 1 hangs and times out; the pool reuses
    workers for batch 3; then the old generation's late batch-1 response
    becomes visible and must be discarded while batch 3 stays accurate."""
    sh_gen("normalize", 80, 31, os.path.join(wd.path, "input.jsonl"))
    faults = write_faults(
        wd.path,
        {
            "b0@1": ["task_delay:1.0"],
            "b1@1": [
                "ignore_term",
                f"barrier:{wd.path}/bar:2",
                f"gated_result_then_exit:{wd.path}/g1",
            ],
            "b2@1": [f"barrier:{wd.path}/bar:2"],
            "b3@1": ["task_delay:2.0"],
            "b4@1": ["task_delay:0.6"],
            "b5@1": ["task_delay:0.6"],
            "b6@1": ["task_delay:0.6"],
            "b7@1": ["task_delay:0.6"],
        },
    )
    cfg = make_cfg(
        wd.path,
        "normalize",
        max_attempts=1,
        batch_timeout_ms=2500,
        kill_grace_ms=30000,
        fault_spec=faults,
    )
    proc = run_host(cfg, wd.path)

    # wait until batch 1 timed out AND batch 3 is in flight, then make the
    # old generation's batch-1 response visible.
    def ready(s):
        return s["counters"].get("worker_timeouts", 0) >= 1 and any(
            i["batch"] == 3 for i in s["inflight"]
        )

    wait_stats(wd.path, ready, 30, "batch1 timed out while batch3 in flight")
    open(os.path.join(wd.path, "g1"), "w").close()  # release the stale result

    rc = wait_host(proc, 90)
    assert rc == 0, f"exit code {rc}"
    m = get_manifest(wd.path)
    assert m["status"] == "completed"
    assert m["counters"].get("stale_frames", 0) >= 1, "stale frame not observed/discarded"
    assert m["counters"].get("worker_timeouts") == 1
    e1 = completed_entry(m, 1)
    assert e1 and e1["outcome"] == "failed" and "batch_timeout" in e1["cause"], e1
    e3 = completed_entry(m, 3)
    assert e3 and e3["outcome"] == "success", e3
    assert m["counters"]["workers_spawned"] == 3, "slot should be reused with a new generation"
    gen2_batches = [e for e in m["completed_log"] if e.get("generation") == 2]
    assert gen2_batches, "a reused slot (new generation) should process later batches"
    assert m["hwm"]["pending_batches"] >= 1, "reorder buffer never engaged"
    verify(cfg, expect_failed=[1])


def exit_crash(wd):
    sh_gen("normalize", 150, 41, os.path.join(wd.path, "input.jsonl"))
    faults = write_faults(wd.path, {"*": ["task_delay:0.15"], "b2@1": ["exit:42"]})
    cfg = make_cfg(wd.path, "normalize", fault_spec=faults)
    proc = run_host(cfg, wd.path)
    rc = wait_host(proc, 60)
    assert rc == 0, f"exit code {rc}"
    m = get_manifest(wd.path)
    assert m["counters"]["worker_crashes"] == 1
    assert m["counters"]["retries"] == 1
    assert any(e.get("exit_code") == 42 for e in m["worker_exit_log"]), m["worker_exit_log"]
    assert m["counters"]["workers_spawned"] == 3
    gen2 = [e for e in m["completed_log"] if e.get("generation") == 2]
    assert gen2, "reused slot generation should process batches"
    verify(cfg)


def half_frame(wd):
    sh_gen("normalize", 50, 42, os.path.join(wd.path, "input.jsonl"))
    faults = write_faults(wd.path, {"b3@1": ["half_frame"]})
    cfg = make_cfg(wd.path, "normalize", fault_spec=faults)
    proc = run_host(cfg, wd.path)
    rc = wait_host(proc, 60)
    assert rc == 0, f"exit code {rc}"
    m = get_manifest(wd.path)
    assert m["counters"].get("viol_truncated_frame") == 1, m["counters"]
    assert m["counters"]["worker_crashes"] == 1
    assert m["counters"]["retries"] == 1
    verify(cfg)


def huge_len(wd):
    sh_gen("normalize", 50, 43, os.path.join(wd.path, "input.jsonl"))
    faults = write_faults(wd.path, {"b2@1": ["huge_len"]})
    cfg = make_cfg(wd.path, "normalize", max_attempts=1, fault_spec=faults)
    proc = run_host(cfg, wd.path)
    rc = wait_host(proc, 60)
    assert rc == 0, f"exit code {rc}"
    m = get_manifest(wd.path)
    assert m["counters"].get("viol_oversized_frame") == 1, m["counters"]
    assert m["hwm"]["ipc_buffer_bytes"] < (1 << 26), "host must not buffer the giant frame"
    e2 = completed_entry(m, 2)
    assert e2 and e2["outcome"] == "failed" and "oversized_frame" in e2["cause"]
    verify(cfg, expect_failed=[2])


def tamper_row_id(wd):
    sh_gen("normalize", 50, 44, os.path.join(wd.path, "input.jsonl"))
    faults = write_faults(wd.path, {"b1@1": ["tamper_row_id"]})
    cfg = make_cfg(wd.path, "normalize", fault_spec=faults)
    proc = run_host(cfg, wd.path)
    rc = wait_host(proc, 60)
    assert rc == 0, f"exit code {rc}"
    m = get_manifest(wd.path)
    assert m["counters"].get("viol_row_id_mismatch") == 1, m["counters"]
    assert m["counters"]["retries"] == 1
    verify(cfg)


def bad_row_count(wd):
    sh_gen("normalize", 50, 45, os.path.join(wd.path, "input.jsonl"))
    faults = write_faults(wd.path, {"b1@1": ["bad_row_count"]})
    cfg = make_cfg(wd.path, "normalize", max_attempts=1, fault_spec=faults)
    proc = run_host(cfg, wd.path)
    rc = wait_host(proc, 60)
    assert rc == 0, f"exit code {rc}"
    m = get_manifest(wd.path)
    assert m["counters"].get("viol_row_count_mismatch") == 1, m["counters"]
    verify(cfg, expect_failed=[1])


def bad_schema(wd):
    sh_gen("normalize", 50, 46, os.path.join(wd.path, "input.jsonl"))
    faults = write_faults(wd.path, {"b2@1": ["bad_schema"]})
    cfg = make_cfg(wd.path, "normalize", max_attempts=1, fault_spec=faults)
    proc = run_host(cfg, wd.path)
    rc = wait_host(proc, 60)
    assert rc == 0, f"exit code {rc}"
    m = get_manifest(wd.path)
    assert m["counters"].get("viol_schema_mismatch") == 1, m["counters"]
    verify(cfg, expect_failed=[2])


def duplicate_response(wd):
    sh_gen("normalize", 50, 47, os.path.join(wd.path, "input.jsonl"))
    faults = write_faults(wd.path, {"b1@1": ["duplicate_response"]})
    cfg = make_cfg(wd.path, "normalize", fault_spec=faults)
    proc = run_host(cfg, wd.path)
    rc = wait_host(proc, 60)
    assert rc == 0, f"exit code {rc}"
    m = get_manifest(wd.path)
    assert m["counters"]["protocol_violations"] >= 1, m["counters"]
    assert m["counters"]["batches_completed"] == 5, "each batch completes exactly once"
    verify(cfg)


def spawn_child(wd):
    sh_gen("normalize", 50, 48, os.path.join(wd.path, "input.jsonl"))
    pidfile = os.path.join(wd.path, "child.pid")
    faults = write_faults(wd.path, {"b1@1": [f"spawn_child_then_hang:{pidfile}"]})
    cfg = make_cfg(
        wd.path,
        "normalize",
        max_attempts=1,
        batch_timeout_ms=1500,
        kill_grace_ms=1000,
        fault_spec=faults,
    )
    proc = run_host(cfg, wd.path)
    # wait for the child to exist
    deadline = time.monotonic() + 30
    child_pid = None
    while time.monotonic() < deadline:
        if os.path.exists(pidfile):
            child_pid = int(open(pidfile).read().strip())
            break
        time.sleep(0.05)
    assert child_pid, "fault child never appeared"
    rc = wait_host(proc, 60)
    assert rc == 0, f"exit code {rc}"
    # the SIGTERM-ignoring grandchild must have been group-killed
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if not os.path.exists(f"/proc/{child_pid}"):
            break
        time.sleep(0.1)
    assert not os.path.exists(f"/proc/{child_pid}"), f"orphan child {child_pid} survived"
    m = get_manifest(wd.path)
    e1 = completed_entry(m, 1)
    assert e1 and e1["outcome"] == "failed" and "batch_timeout" in e1["cause"]
    verify(cfg, expect_failed=[1])


def slow_sink(wd):
    sh_gen("normalize", 200, 49, os.path.join(wd.path, "input.jsonl"))
    cfg = make_cfg(
        wd.path,
        "normalize",
        sink_delay_ms_per_batch=150,
        max_inflight_batches=6,
        max_pending_batches=2,
        writer_queue_batches=2,
    )
    proc = run_host(cfg, wd.path)
    t0 = time.monotonic()
    samples = collect_stats(wd.path, proc, interval=0.1)
    rc = wait_host(proc, 120)
    elapsed = time.monotonic() - t0
    assert rc == 0, f"exit code {rc}"
    m = get_manifest(wd.path)
    assert m["status"] == "completed"
    # sink-limited: 20 batches * 150ms = 3s of sink time
    assert elapsed >= 2.5, f"sink did not pace the run ({elapsed:.2f}s)"
    # budgets held at every sample
    for s in samples:
        g = s["gauges"]
        assert g["retained_batches"] <= 6, f"inflight budget exceeded: {g}"
        assert g["pending_batches"] <= 6, f"pending budget exceeded: {g}"
        assert g["writer_queue"] <= 2
    # backpressure actually engaged at some point
    assert any(s["gauges"]["pending_batches"] >= 2 for s in samples), (
        "pending cap never reached; backpressure not exercised"
    )
    # while the sink was slow, reading was throttled: read never ran ahead
    # further than the buffering budgets allow (6 retained + 2 writer queue
    # batches of 10 rows, plus one batch in formation)
    for s in samples:
        ahead = s["read_rows"] - (
            s["delivered"]["success_rows"] + s["delivered"]["failure_rows"]
        )
        assert ahead <= 90, f"reader ran too far ahead of sink: {ahead}"
    assert m["hwm"]["retained_batches"] <= 6
    verify(cfg)


def cancel(wd):
    sh_gen("normalize", 500, 50, os.path.join(wd.path, "input.jsonl"))
    faults = write_faults(wd.path, {"*": ["task_delay:0.1"]})
    cfg = make_cfg(wd.path, "normalize", fault_spec=faults)
    proc = run_host(cfg, wd.path)
    wait_stats(
        wd.path,
        lambda s: s["delivered"]["success_rows"] >= 50,
        30,
        "at least 5 batches delivered",
    )
    proc.send_signal(signal.SIGINT)
    rc = wait_host(proc, 30)
    assert rc == 3, f"cancelled run should exit 3, got {rc}"
    m = get_manifest(wd.path)
    assert m["status"] == "cancelled", m["status"]
    k = m["delivered_batches"]
    assert 5 <= k < 50, f"delivered prefix {k}"
    assert m["last_delivered_row_id"] == k * 10 - 1
    verify(cfg, through_batch=k)
    assert_no_leftovers()


def rlimit_as(wd):
    sh_gen("normalize", 50, 51, os.path.join(wd.path, "input.jsonl"))
    faults = write_faults(wd.path, {"b1@1": ["alloc:4000"]})
    cfg = make_cfg(
        wd.path,
        "normalize",
        max_attempts=1,
        rlimits={"cpu_seconds": 0, "as_bytes": 2_000_000_000, "nofile": 0},
        fault_spec=faults,
    )
    proc = run_host(cfg, wd.path)
    rc = wait_host(proc, 60)
    assert rc == 0, f"exit code {rc}"
    m = get_manifest(wd.path)
    e1 = completed_entry(m, 1)
    assert e1 and e1["outcome"] == "failed", e1
    assert "plugin_error:memory_error" in e1["cause"], e1
    assert m["counters"].get("plugin_errors") == 1
    assert m["counters"].get("workers_killed", 0) == 0, "memory error is data, not a crash"
    verify(cfg, expect_failed=[1])


def rlimit_cpu(wd):
    sh_gen("normalize", 50, 52, os.path.join(wd.path, "input.jsonl"))
    faults = write_faults(wd.path, {"b1@1": ["burn_cpu"]})
    cfg = make_cfg(
        wd.path,
        "normalize",
        max_attempts=1,
        batch_timeout_ms=30000,
        rlimits={"cpu_seconds": 2, "as_bytes": 0, "nofile": 0},
        fault_spec=faults,
    )
    proc = run_host(cfg, wd.path)
    rc = wait_host(proc, 60)
    assert rc == 0, f"exit code {rc}"
    m = get_manifest(wd.path)
    assert m["counters"]["worker_crashes"] == 1
    sigxcpu = [e for e in m["worker_exit_log"] if e.get("signal") == 24]
    assert sigxcpu, f"expected SIGXCPU kill, got {m['worker_exit_log']}"
    verify(cfg, expect_failed=[1])


def intake_errors(wd):
    sh_gen("normalize", 47, 53, os.path.join(wd.path, "input.jsonl"), malformed_every=15)
    cfg = make_cfg(wd.path, "normalize")
    proc = run_host(cfg, wd.path)
    rc = wait_host(proc, 60)
    assert rc == 0, f"exit code {rc}"
    m = get_manifest(wd.path)
    assert m["counters"]["intake_errors"] == 3, m["counters"]
    assert m["delivered_rows_failure"] == 3
    assert m["delivered_rows_success"] == 44
    verify(cfg)


def bad_plugin_path(wd):
    sh_gen("normalize", 20, 54, os.path.join(wd.path, "input.jsonl"))
    cfg = make_cfg(wd.path, "normalize")
    with open(cfg, "r") as fh:
        raw = json.load(fh)
    raw["plugin"]["path"] = os.path.join(wd.path, "does_not_exist.py")
    with open(cfg, "w") as fh:
        json.dump(raw, fh)
    proc = run_host(cfg, wd.path)
    rc = wait_host(proc, 60)
    assert rc == 1, f"broken plugin should abort with 1, got {rc}"
    m = get_manifest(wd.path)
    assert m["status"] == "aborted"
    assert m["counters"]["spawn_failures"] >= 4
    assert_no_leftovers()


SCENARIOS = [
    ("happy_normalize", happy_normalize),
    ("happy_crossfield", happy_crossfield),
    ("scale_normalize", scale_normalize),
    ("barrier_stale", barrier_stale),
    ("exit_crash", exit_crash),
    ("half_frame", half_frame),
    ("huge_len", huge_len),
    ("tamper_row_id", tamper_row_id),
    ("bad_row_count", bad_row_count),
    ("bad_schema", bad_schema),
    ("duplicate_response", duplicate_response),
    ("spawn_child", spawn_child),
    ("slow_sink", slow_sink),
    ("cancel", cancel),
    ("rlimit_as", rlimit_as),
    ("rlimit_cpu", rlimit_cpu),
    ("intake_errors", intake_errors),
    ("bad_plugin_path", bad_plugin_path),
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
            assert_no_leftovers()
        except Exception:
            ok = False
            failed.append(name)
            traceback.print_exc()
        print(f"[ {'FAIL' if not ok else 'PASS'} ] {name}", flush=True)
        wd.cleanup(failed=not ok)
    dt = time.monotonic() - t0
    print(f"\n{len(scenarios) - len(failed)}/{len(scenarios)} scenarios passed in {dt:.1f}s")
    if failed:
        print("failed:", ", ".join(failed))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
