"""The colbatch host.

One host process supervises a pool of worker OS processes.  It reads JSONL
intake records, forms Arrow RecordBatches of ``batch_size`` rows, dispatches
them to workers over per-worker pipes, validates every result against the
batch contract (row count, exact row_id sequence, exact schema), and
delivers success / failure rows as JSONL in input order.

Design notes (see README.md for the full description):

  * batch identity = (batch_id, worker generation, attempt); results that
    do not match the *current* identity of the worker they arrived on are
    rejected; frames from doomed/old-generation workers are discarded as
    stale -- a queue never "completes" a task by itself;
  * a worker handles at most one batch at a time, so a pipe is never
    ambiguous about which batch a frame belongs to;
  * worker processes are started with setsid and killed by process group
    (SIGTERM -> grace -> SIGKILL) so plugin-spawned children are reaped too;
  * all in-flight + completed-undelivered batches and IPC buffers are
    bounded by explicit budgets; a slow sink back-pressures the reader;
  * delivery is a contiguous in-order prefix: on cancel the manifest states
    exactly up to which input batch/row the output is complete.
"""

from __future__ import annotations

import collections
import hashlib
import json
import os
import queue
import resource
import selectors
import signal
import subprocess
import sys
import threading
import time
import traceback

import pyarrow as pa

from . import protocol as proto
from .sdk import (
    ContractError,
    batch_to_bytes,
    bytes_to_batch,
    schema_from_bytes,
    validate_result,
)

_COLUMN_TYPES = {
    "utf8": pa.string(),
    "int64": pa.int64(),
}

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


# ---------------------------------------------------------------------------
# configuration


class ConfigError(Exception):
    pass


class Config:
    def __init__(self, raw: dict, base_dir: str = "."):
        def path(key, default=None):
            val = raw.get(key, default)
            if val is None:
                return None
            if not os.path.isabs(val):
                val = os.path.join(base_dir, val)
            return os.path.abspath(val)

        self.input = path("input")
        self.success_out = path("success_out")
        self.failure_out = path("failure_out")
        self.log_dir = path("log_dir", "colbatch_logs")
        self.stats_path = path("stats_path", os.path.join(self.log_dir, "stats.json"))
        self.manifest_path = path(
            "manifest_path", os.path.join(self.log_dir, "manifest.json")
        )

        plugin = raw.get("plugin") or {}
        plugin_path = plugin.get("path")
        if not plugin_path:
            raise ConfigError("config needs plugin.path")
        if not os.path.isabs(plugin_path):
            plugin_path = os.path.join(base_dir, plugin_path)
        self.plugin_path = os.path.abspath(plugin_path)

        self.columns = raw.get("columns")
        if not self.columns:
            raise ConfigError("config needs columns")
        for col in self.columns:
            if col.get("type") not in _COLUMN_TYPES:
                raise ConfigError(f"unsupported column type {col.get('type')!r}")
            if not col.get("name") or not col.get("field"):
                raise ConfigError("column entries need name and field")

        self.batch_size = int(raw.get("batch_size", 100))
        self.workers = int(raw.get("workers", 2))
        self.max_attempts = int(raw.get("max_attempts", 2))
        self.batch_timeout_ms = int(raw.get("batch_timeout_ms", 5000))
        self.kill_grace_ms = int(raw.get("kill_grace_ms", 1000))
        self.handshake_timeout_ms = int(raw.get("handshake_timeout_ms", 15000))
        self.bye_timeout_ms = int(raw.get("bye_timeout_ms", 3000))
        self.max_inflight_batches = int(raw.get("max_inflight_batches", 8))
        self.max_pending_batches = int(raw.get("max_pending_batches", 4))
        self.max_pending_bytes = int(raw.get("max_pending_bytes", 64 * 1024 * 1024))
        self.max_frame_bytes = int(raw.get("max_frame_bytes", 256 * 1024 * 1024))
        self.writer_queue_batches = int(raw.get("writer_queue_batches", 4))
        self.stats_interval_ms = int(raw.get("stats_interval_ms", 200))
        self.sink_delay_ms_per_batch = float(raw.get("sink_delay_ms_per_batch", 0))
        self.max_spawn_failures = int(raw.get("max_spawn_failures", 4))

        rlimits = raw.get("rlimits") or {}
        self.rlimit_cpu = int(rlimits.get("cpu_seconds", 0))
        self.rlimit_as = int(rlimits.get("as_bytes", 0))
        self.rlimit_nofile = int(rlimits.get("nofile", 0))

        fault_spec = raw.get("fault_spec")
        if fault_spec:
            if not os.path.isabs(fault_spec):
                fault_spec = os.path.join(base_dir, fault_spec)
            self.fault_spec = os.path.abspath(fault_spec)
        else:
            self.fault_spec = None

        if not self.input or not os.path.exists(self.input):
            raise ConfigError(f"input not found: {self.input}")
        if not self.success_out or not self.failure_out:
            raise ConfigError("config needs success_out and failure_out")
        if self.success_out == self.failure_out:
            raise ConfigError("success_out and failure_out must differ")
        if self.batch_size < 1:
            raise ConfigError("batch_size must be >= 1")
        if self.workers < 1:
            raise ConfigError("workers must be >= 1")
        if self.max_attempts < 1:
            raise ConfigError("max_attempts must be >= 1")
        if self.max_inflight_batches < 1 or self.max_pending_batches < 1:
            raise ConfigError("batch budgets must be >= 1")
        if self.writer_queue_batches < 1:
            raise ConfigError("writer_queue_batches must be >= 1")

        fields = [pa.field("row_id", pa.int64(), nullable=False)]
        for col in self.columns:
            fields.append(pa.field(col["name"], _COLUMN_TYPES[col["type"]]))
        self.input_schema = pa.schema(fields)

    def limits_dict(self):
        return {
            "batch_size": self.batch_size,
            "workers": self.workers,
            "max_attempts": self.max_attempts,
            "batch_timeout_ms": self.batch_timeout_ms,
            "kill_grace_ms": self.kill_grace_ms,
            "max_inflight_batches": self.max_inflight_batches,
            "max_pending_batches": self.max_pending_batches,
            "max_pending_bytes": self.max_pending_bytes,
            "max_frame_bytes": self.max_frame_bytes,
            "writer_queue_batches": self.writer_queue_batches,
            "rlimits": {
                "cpu_seconds": self.rlimit_cpu,
                "as_bytes": self.rlimit_as,
                "nofile": self.rlimit_nofile,
            },
        }


# ---------------------------------------------------------------------------
# state


class BatchState:
    __slots__ = (
        "batch_id",
        "row_ids",
        "records",
        "ipc_blob",
        "nbytes",
        "state",
        "attempt",
        "deadline",
        "result",
        "error",
        "attempts_log",
    )

    def __init__(self, batch_id, row_ids, records, ipc_blob, nbytes):
        self.batch_id = batch_id
        self.row_ids = row_ids
        self.records = records
        self.ipc_blob = ipc_blob
        self.nbytes = nbytes
        self.state = "queued"  # queued|dispatched|done_success|done_failed|dropped
        self.attempt = 1
        self.deadline = None
        self.result = None
        self.error = None
        self.attempts_log = []


class WorkerSlot:
    def __init__(self, slot):
        self.slot = slot
        self.generation = 0
        self.proc = None
        self.pid = None
        self.state = "dead"  # starting|idle|busy|doomed|stopping|dead
        self.reaped = True
        self.rfd = -1
        self.wfd = -1
        self.rbuf = bytearray()
        self.wbuf = bytearray()
        self.parse_enabled = True
        self.hello_received = False
        self.batch_id = None
        self.attempt = 0
        self.handshake_deadline = 0.0
        self.grace_deadline = 0.0
        self.term_sent = False
        self.kill_sent = False
        self.bye_ok = False
        self.doom_reason = None
        self.write_registered = False
        self.stderr_path = None


# ---------------------------------------------------------------------------
# host


class Host:
    def __init__(self, cfg: Config, gate=None):
        self.cfg = cfg
        # When a quality gate is enabled, the immutable snapshot bound to the
        # verified admission report replaces the mutable source path.
        self.gate = gate
        self.input_path = gate.snapshot_path if gate is not None else cfg.input
        self.sel = selectors.DefaultSelector()
        self.workers = [WorkerSlot(i) for i in range(cfg.workers)]

        self.batches = {}
        self.dispatch_queue = collections.deque()
        self.completed = {}  # batch_id -> BatchState, done but undelivered
        self.next_batch_id = 0
        self.next_row_id = 0
        self.next_deliver_id = 0

        self.inflight = 0  # queued + dispatched batches
        self.n_pending = 0  # completed, undelivered batches
        self.pending_bytes = 0

        self.source_eof = False
        self.read_rows = 0
        self.phase = "running"  # running|stopping|cancelled|aborted
        self.sealing = False
        self.cancel_requested = False
        self.abort_reason = None
        self.bye_deadline = None

        self.counters = collections.Counter()
        self.hwm = collections.Counter()
        self.completed_log = collections.deque(maxlen=20000)
        self.worker_exit_log = collections.deque(maxlen=500)
        self.consec_spawn_failures = 0
        self.expected_out_schema = None
        # Plugin identity reported by the first worker handshake; every later
        # handshake must agree (a plugin file swapped mid-run is an integrity
        # problem, not a new generation).  Recorded in the manifest so that
        # `colbatch compare` can attribute results to a run/plugin identity.
        self.plugin_identity = None

        self.input_f = open(self.input_path, "r", encoding="utf-8")
        self.start_time = time.monotonic()
        self.last_snapshot = 0.0

        self.writer_q = queue.Queue(maxsize=cfg.writer_queue_batches)
        self.written = {"items": 0, "success_rows": 0, "failure_rows": 0, "last_row_id": -1}
        self.written_lock = threading.Lock()
        # SHA-256 over the exact bytes delivered to each output stream; the
        # writer thread is the only writer, so it hashes every line as it is
        # emitted.  Published in the manifest for `colbatch compare` to
        # verify artifacts against.
        self.out_digests = {"success": hashlib.sha256(), "failure": hashlib.sha256()}
        self.items_enqueued = 0
        self.writer_error = None
        self.writer_thread = threading.Thread(
            target=self._writer_main, name="colbatch-writer", daemon=True
        )

    # ------------------------------------------------------------------
    # public entry

    def run(self) -> int:
        os.makedirs(self.cfg.log_dir, exist_ok=True)
        os.makedirs(os.path.dirname(self.cfg.stats_path), exist_ok=True)
        os.makedirs(os.path.dirname(self.cfg.manifest_path), exist_ok=True)
        for path in (self.cfg.success_out, self.cfg.failure_out):
            d = os.path.dirname(path)
            if d:
                os.makedirs(d, exist_ok=True)

        self._install_signals()
        self.writer_thread.start()
        for w in self.workers:
            self._spawn(w)

        try:
            while True:
                if self.cancel_requested and self.phase == "running":
                    self._begin_cancel()
                timeout = self._compute_timeout()
                events = self.sel.select(timeout)
                for key, mask in events:
                    kind, w = key.data
                    if kind == "r":
                        self._do_read(w)
                    else:
                        self._do_write(w)
                self._process_timers()
                if self.phase == "running":
                    self._maybe_read_source()
                    self._maybe_dispatch()
                self._maybe_deliver()
                self._maybe_transition()
                self._maybe_snapshot()
                if self._finished():
                    break
            return self._finalize()
        except Exception:
            traceback.print_exc()
            self._abort("host_internal_error")
            return self._finalize()

    # ------------------------------------------------------------------
    # signals / phases

    def _install_signals(self):
        def _handler(signum, frame):
            self.cancel_requested = True

        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)

    def _begin_cancel(self):
        self.phase = "cancelled"
        self.sealing = True
        self.counters["cancels"] += 1
        for w in self.workers:
            if w.state != "dead":
                self._doom(w, "host_cancel", grace_ms=min(self.cfg.kill_grace_ms, 1000), fail_batch=False)

    def _abort(self, reason):
        if self.phase in ("cancelled", "aborted"):
            return
        self.phase = "aborted"
        self.sealing = True
        self.abort_reason = reason
        for w in self.workers:
            if w.state != "dead":
                self._doom(w, "host_abort", grace_ms=min(self.cfg.kill_grace_ms, 1000), fail_batch=False)

    def _begin_shutdown(self):
        self.phase = "stopping"
        self.sealing = True
        for w in self.workers:
            if w.state == "idle":
                self._send_frame(w, proto.SHUTDOWN, {})
                w.state = "stopping"
            elif w.state == "starting":
                self._doom(w, "shutdown")
        self.bye_deadline = time.monotonic() + self.cfg.bye_timeout_ms / 1000.0

    def _finished(self):
        if self.phase == "running":
            return False
        for w in self.workers:
            if w.state != "dead" or not w.reaped:
                return False
        return self._writer_drained()

    def _writer_drained(self):
        with self.written_lock:
            return self.items_enqueued == self.written["items"]

    def _maybe_transition(self):
        if self.phase != "running":
            return
        if (
            self.source_eof
            and self.inflight == 0
            and self.n_pending == 0
            and not self.dispatch_queue
            and self.next_deliver_id == self.next_batch_id
            and self._writer_drained()
        ):
            self._begin_shutdown()

    # ------------------------------------------------------------------
    # worker lifecycle

    def _make_preexec(self):
        cfg = self.cfg

        def apply_limits():
            if cfg.rlimit_cpu:
                # hard limit slightly above soft so the kernel delivers
                # SIGXCPU (catchable/observable) before it would SIGKILL.
                resource.setrlimit(
                    resource.RLIMIT_CPU, (cfg.rlimit_cpu, cfg.rlimit_cpu + 1)
                )
            if cfg.rlimit_as:
                resource.setrlimit(resource.RLIMIT_AS, (cfg.rlimit_as, cfg.rlimit_as))
            if cfg.rlimit_nofile:
                resource.setrlimit(
                    resource.RLIMIT_NOFILE, (cfg.rlimit_nofile, cfg.rlimit_nofile)
                )

        if cfg.rlimit_cpu or cfg.rlimit_as or cfg.rlimit_nofile:
            return apply_limits
        return None

    def _spawn(self, w: WorkerSlot):
        w.generation += 1
        w.stderr_path = os.path.join(
            self.cfg.log_dir, f"worker_s{w.slot}_g{w.generation}.stderr.log"
        )
        env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": os.environ.get("HOME", "/tmp"),
            "LANG": "C.UTF-8",
        }
        if self.cfg.fault_spec:
            env["COLBATCH_FAULT_SPEC"] = self.cfg.fault_spec
        worker_py = os.path.join(os.path.dirname(os.path.abspath(__file__)), "worker.py")
        try:
            stderr_fh = open(w.stderr_path, "ab")
            proc = subprocess.Popen(
                [
                    sys.executable,
                    "-I",
                    worker_py,
                    "--plugin",
                    self.cfg.plugin_path,
                    "--max-frame-bytes",
                    str(self.cfg.max_frame_bytes),
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=stderr_fh,
                bufsize=0,
                start_new_session=True,  # own process group -> group kill
                preexec_fn=self._make_preexec(),
                env=env,
                close_fds=True,
            )
            stderr_fh.close()  # child keeps its own dup
        except Exception:
            self.counters["spawn_failures"] += 1
            self.consec_spawn_failures += 1
            w.state = "dead"
            w.reaped = True
            w.pid = None
            if self.consec_spawn_failures >= self.cfg.max_spawn_failures:
                self._abort("worker spawn failed repeatedly")
            return

        w.proc = proc
        w.pid = proc.pid
        w.rfd = proc.stdout.fileno()
        w.wfd = proc.stdin.fileno()
        os.set_blocking(w.rfd, False)
        os.set_blocking(w.wfd, False)
        w.rbuf = bytearray()
        w.wbuf = bytearray()
        w.parse_enabled = True
        w.hello_received = False
        w.batch_id = None
        w.attempt = 0
        w.state = "starting"
        w.reaped = False
        w.term_sent = False
        w.kill_sent = False
        w.bye_ok = False
        w.doom_reason = None
        w.write_registered = False
        w.handshake_deadline = time.monotonic() + self.cfg.handshake_timeout_ms / 1000.0
        self.sel.register(w.rfd, selectors.EVENT_READ, ("r", w))
        self.counters["workers_spawned"] += 1

    def _doom(self, w: WorkerSlot, reason, grace_ms=None, fail_batch=True):
        """Terminate a worker: SIGTERM its process group, SIGKILL after grace."""
        if w.state in ("dead", "doomed"):
            return
        if w.state == "busy":
            batch = self.batches.get(w.batch_id)
            if batch is not None and batch.state == "dispatched":
                if fail_batch:
                    self._fail_attempt(batch, reason)
                else:
                    batch.state = "dropped"
                    self.inflight -= 1
                    self.counters["batches_dropped"] += 1
        w.state = "doomed"
        w.doom_reason = reason
        w.batch_id = None
        self._unregister_write(w)
        w.wbuf = bytearray()
        if w.pid is not None:
            try:
                os.killpg(w.pid, signal.SIGTERM)
                w.term_sent = True
            except (ProcessLookupError, PermissionError):
                pass
        grace = self.cfg.kill_grace_ms if grace_ms is None else grace_ms
        w.grace_deadline = time.monotonic() + grace / 1000.0
        self.counters["workers_killed"] += 1

    def _try_reap(self, w: WorkerSlot):
        if w.reaped or w.pid is None:
            return
        try:
            pid, status, rusage = os.wait4(w.pid, os.WNOHANG)
        except ChildProcessError:
            w.reaped = True
            return
        if pid == 0:
            return
        w.reaped = True
        entry = {
            "slot": w.slot,
            "generation": w.generation,
            "pid": w.pid,
            "reason": w.doom_reason or "exit",
            "maxrss_kb": rusage.ru_maxrss,
        }
        if os.WIFEXITED(status):
            entry["exit_code"] = os.WEXITSTATUS(status)
        elif os.WIFSIGNALED(status):
            entry["signal"] = os.WTERMSIG(status)
        self.worker_exit_log.append(entry)
        if w.proc is not None:
            try:
                w.proc.returncode = os.waitstatus_to_exitcode(status)
            except (ValueError, AttributeError):
                pass
            w.proc = None

    def _maybe_respawn(self, w: WorkerSlot):
        if self.sealing or self.phase != "running":
            return
        if w.state != "dead" or not w.reaped:
            return
        if self.consec_spawn_failures >= self.cfg.max_spawn_failures:
            self._abort("too many consecutive worker spawn failures")
            return
        self._spawn(w)

    # ------------------------------------------------------------------
    # IO on worker pipes

    def _unregister_write(self, w: WorkerSlot):
        if w.write_registered and w.wfd >= 0:
            try:
                self.sel.unregister(w.wfd)
            except (KeyError, ValueError):
                pass
        w.write_registered = False

    def _unregister_all(self, w: WorkerSlot):
        if w.rfd >= 0:
            try:
                self.sel.unregister(w.rfd)
            except (KeyError, ValueError):
                pass
        self._unregister_write(w)

    def _close_fds(self, w: WorkerSlot):
        proc = w.proc
        if proc is not None:
            for fobj in (proc.stdout, proc.stdin):
                try:
                    if fobj is not None:
                        fobj.close()
                except OSError:
                    pass
        # NOTE: keep w.proc referenced until _try_reap() has collected the
        # exit status via wait4; dropping it earlier lets Popen.__del__
        # reap the child first and the exit status would be lost.
        w.rfd = -1
        w.wfd = -1

    def _send_frame(self, w: WorkerSlot, mtype, meta, blob=b""):
        if w.state in ("dead", "doomed") or w.wfd < 0:
            return
        w.wbuf += proto.encode(mtype, meta, blob)
        if not w.write_registered:
            try:
                self.sel.register(w.wfd, selectors.EVENT_WRITE, ("w", w))
                w.write_registered = True
            except (KeyError, ValueError, OSError):
                pass

    def _do_read(self, w: WorkerSlot):
        if w.rfd < 0:
            return
        try:
            data = os.read(w.rfd, 65536)
        except BlockingIOError:
            return
        except OSError:
            self._on_eof(w)
            return
        if data == b"":
            self._on_eof(w)
            return
        w.rbuf += data
        if not w.parse_enabled:
            # protocol already violated: drain (to notice EOF) but never parse
            if len(w.rbuf) > 2 * self.cfg.max_frame_bytes:
                w.rbuf.clear()
            return
        while w.parse_enabled:
            if len(w.rbuf) < proto.HEADER_SIZE:
                break
            try:
                mtype, length = proto.parse_header(w.rbuf)
            except proto.ProtocolError as exc:
                self._protocol_violation(w, f"bad_header:{exc}")
                break
            if length > self.cfg.max_frame_bytes:
                self._protocol_violation(w, "oversized_frame")
                break
            if len(w.rbuf) < proto.HEADER_SIZE + length:
                break
            payload = bytes(w.rbuf[proto.HEADER_SIZE : proto.HEADER_SIZE + length])
            del w.rbuf[: proto.HEADER_SIZE + length]
            self._handle_message(w, mtype, payload)

    def _do_write(self, w: WorkerSlot):
        if w.wfd < 0 or not w.wbuf:
            self._unregister_write(w)
            return
        try:
            n = os.write(w.wfd, w.wbuf)
        except BlockingIOError:
            return
        except OSError:
            # broken pipe: the read side will report EOF shortly
            self._unregister_write(w)
            return
        del w.wbuf[:n]
        if not w.wbuf:
            self._unregister_write(w)

    def _on_eof(self, w: WorkerSlot):
        self._unregister_all(w)
        self._close_fds(w)
        if w.state == "busy":
            batch = self.batches.get(w.batch_id)
            if w.parse_enabled and len(w.rbuf) > 0:
                self.counters["protocol_violations"] += 1
                self.counters["viol_truncated_frame"] += 1
                if batch is not None and batch.state == "dispatched":
                    self._fail_attempt(batch, "protocol_violation:truncated_frame")
            else:
                if batch is not None and batch.state == "dispatched":
                    self._fail_attempt(batch, "worker_crash")
            self.counters["worker_crashes"] += 1
        elif w.state == "starting":
            self.counters["spawn_failures"] += 1
            self.consec_spawn_failures += 1
        elif w.state == "idle":
            self.counters["worker_unexpected_exit"] += 1
        w.state = "dead"
        w.batch_id = None
        self._try_reap(w)
        self._maybe_respawn(w)

    # ------------------------------------------------------------------
    # message handling

    def _handle_message(self, w: WorkerSlot, mtype, payload):
        if w.state in ("doomed", "dead"):
            # late frame from an old generation / abandoned attempt: discard,
            # never let it complete or contaminate anything.
            if mtype in (proto.RESULT, proto.BATCH_ERROR):
                self.counters["stale_frames"] += 1
            return
        try:
            meta, blob = proto.decode_payload(payload)
        except proto.ProtocolError as exc:
            self._protocol_violation(w, f"bad_payload:{exc}")
            return

        if mtype == proto.HELLO:
            self._on_hello(w, meta)
        elif mtype == proto.READY:
            self._on_ready(w)
        elif mtype == proto.RESULT:
            self._on_result(w, meta, blob)
        elif mtype == proto.BATCH_ERROR:
            self._on_batch_error(w, meta)
        elif mtype == proto.BYE:
            if w.state == "stopping":
                w.bye_ok = True
                self.counters["clean_bye"] += 1
            else:
                self._protocol_violation(w, "unexpected_bye")
        else:
            self._protocol_violation(w, f"invalid_direction:{proto.NAMES.get(mtype, mtype)}")

    def _identity_ok(self, w: WorkerSlot, meta):
        return (
            meta.get("batch_id") == w.batch_id
            and meta.get("generation") == w.generation
            and meta.get("attempt") == w.attempt
        )

    def _on_hello(self, w: WorkerSlot, meta):
        if w.state != "starting" or w.hello_received:
            self._protocol_violation(w, "unexpected_hello")
            return
        try:
            in_schema = schema_from_bytes(bytes.fromhex(meta.get("in_schema", "")))
            out_schema = schema_from_bytes(bytes.fromhex(meta.get("out_schema", "")))
        except Exception:
            self._protocol_violation(w, "bad_hello")
            return
        w.hello_received = True
        if meta.get("proto") != proto.PROTOCOL_VERSION:
            self._send_frame(w, proto.HELLO_ACK, {"ok": False, "error": "protocol version mismatch"})
            self._spawn_failed(w, "version_mismatch")
            return
        plugin_meta = meta.get("plugin") or {}
        identity = {
            "name": str(plugin_meta.get("name", "")),
            "version": str(plugin_meta.get("version", "")),
        }
        if self.plugin_identity is None:
            self.plugin_identity = identity
        elif identity != self.plugin_identity:
            # A worker of the same job reporting a different plugin identity
            # means the plugin file changed under a running job; refuse to
            # mix results across identities.
            self._send_frame(
                w, proto.HELLO_ACK, {"ok": False, "error": "plugin identity changed mid-run"}
            )
            self._spawn_failed(w, "plugin_identity_mismatch")
            return
        if not in_schema.equals(self.cfg.input_schema):
            self._send_frame(
                w,
                proto.HELLO_ACK,
                {
                    "ok": False,
                    "error": f"input schema mismatch: plugin {in_schema} vs config {self.cfg.input_schema}",
                },
            )
            self._spawn_failed(w, "schema_negotiation_failed")
            return
        if self.expected_out_schema is None:
            self.expected_out_schema = out_schema
        elif not out_schema.equals(self.expected_out_schema):
            self._send_frame(w, proto.HELLO_ACK, {"ok": False, "error": "inconsistent output schema"})
            self._spawn_failed(w, "inconsistent_out_schema")
            return
        self._send_frame(
            w,
            proto.HELLO_ACK,
            {
                "ok": True,
                "max_frame_bytes": self.cfg.max_frame_bytes,
                "slot": w.slot,
                "generation": w.generation,
            },
        )

    def _spawn_failed(self, w: WorkerSlot, reason):
        self.counters["spawn_failures"] += 1
        self.consec_spawn_failures += 1
        self._doom(w, reason)

    def _on_ready(self, w: WorkerSlot):
        if w.state != "starting" or not w.hello_received:
            self._protocol_violation(w, "unexpected_ready")
            return
        w.state = "idle"
        self.consec_spawn_failures = 0
        self.counters["handshakes"] += 1

    def _on_result(self, w: WorkerSlot, meta, blob):
        if w.state != "busy":
            self._protocol_violation(w, "unexpected_result")
            return
        if not self._identity_ok(w, meta):
            self._protocol_violation(w, "identity_mismatch")
            return
        batch = self.batches[w.batch_id]
        try:
            rb = bytes_to_batch(blob)
        except Exception:
            self._protocol_violation(w, "bad_ipc")
            return
        try:
            validate_result(batch.row_ids, rb, self.expected_out_schema)
        except ContractError as exc:
            self._protocol_violation(w, str(exc))
            return

        batch.state = "done_success"
        batch.result = {
            name: rb.column(name).to_pylist()
            for name in rb.schema.names
            if name != "row_id"
        }
        batch.ipc_blob = None  # free the input blob; records stay for output
        self.completed[batch.batch_id] = batch
        self.inflight -= 1
        self.n_pending += 1
        self.pending_bytes += batch.nbytes
        self.counters["batches_completed"] += 1
        self.counters["rows_success"] += len(batch.row_ids)
        self.completed_log.append(
            {
                "batch": batch.batch_id,
                "outcome": "success",
                "slot": w.slot,
                "generation": w.generation,
                "attempts": batch.attempt,
                "rows": len(batch.row_ids),
            }
        )
        w.state = "idle"
        w.batch_id = None
        self._update_hwm()

    def _on_batch_error(self, w: WorkerSlot, meta):
        if w.state != "busy":
            self._protocol_violation(w, "unexpected_batch_error")
            return
        if not self._identity_ok(w, meta):
            self._protocol_violation(w, "identity_mismatch")
            return
        batch = self.batches[w.batch_id]
        self.counters["plugin_errors"] += 1
        self._fail_attempt(batch, f"plugin_error:{meta.get('code', 'unknown')}")
        w.state = "idle"
        w.batch_id = None

    def _protocol_violation(self, w: WorkerSlot, reason):
        key = "viol_" + reason.split(":", 1)[0][:40]
        self.counters["protocol_violations"] += 1
        self.counters[key] += 1
        if w.state == "busy":
            batch = self.batches.get(w.batch_id)
            if batch is not None and batch.state == "dispatched":
                self._fail_attempt(batch, f"protocol_violation:{reason}")
        w.parse_enabled = False
        self._doom(w, f"protocol:{reason}")

    # ------------------------------------------------------------------
    # batch state machine

    def _fail_attempt(self, batch: BatchState, cause):
        if batch.state != "dispatched":
            return
        batch.attempts_log.append({"attempt": batch.attempt, "cause": cause})
        if batch.attempt < self.cfg.max_attempts:
            batch.state = "queued"
            batch.attempt += 1
            batch.deadline = None
            self.dispatch_queue.appendleft(batch.batch_id)
            self.counters["retries"] += 1
        else:
            batch.state = "done_failed"
            batch.ipc_blob = None
            batch.error = {
                "cause": cause,
                "batch_id": batch.batch_id,
                "attempts": batch.attempt,
                "log": list(batch.attempts_log),
            }
            self.completed[batch.batch_id] = batch
            self.inflight -= 1
            self.n_pending += 1
            self.pending_bytes += batch.nbytes
            self.counters["batches_failed"] += 1
            self.counters["rows_failed"] += len(batch.row_ids)
            self.completed_log.append(
                {
                    "batch": batch.batch_id,
                    "outcome": "failed",
                    "cause": cause,
                    "attempts": batch.attempt,
                    "rows": len(batch.row_ids),
                }
            )
        self._update_hwm()

    # ------------------------------------------------------------------
    # source reading / batch forming

    def _capacity_ok(self):
        return (
            (self.inflight + self.n_pending) < self.cfg.max_inflight_batches
            and self.n_pending < self.cfg.max_pending_batches
            and self.pending_bytes < self.cfg.max_pending_bytes
        )

    def _maybe_read_source(self):
        while not self.source_eof and self._capacity_ok():
            lines = []
            for _ in range(self.cfg.batch_size):
                line = self.input_f.readline()
                if line == "":
                    self.source_eof = True
                    break
                self.read_rows += 1
                lines.append(line)
            if not lines:
                break
            self._form_batches(lines)

    def _check_record(self, rec):
        for col in self.cfg.columns:
            val = rec.get(col["field"])
            if val is None:
                continue
            if col["type"] == "utf8":
                if not isinstance(val, str):
                    return f"field {col['field']}: expected string"
            elif col["type"] == "int64":
                if not isinstance(val, int) or isinstance(val, bool):
                    return f"field {col['field']}: expected integer"
                if val < _INT64_MIN or val > _INT64_MAX:
                    return f"field {col['field']}: int64 overflow"
        return None

    def _form_batches(self, lines):
        good = []  # (row_id, record, raw_len)

        def flush():
            if good:
                self._make_batch(good)
                good.clear()

        for line in lines:
            rid = self.next_row_id
            self.next_row_id += 1
            try:
                rec = json.loads(line)
                if not isinstance(rec, dict):
                    raise ValueError("intake record is not a JSON object")
                problem = self._check_record(rec)
                if problem:
                    raise ValueError(problem)
            except (json.JSONDecodeError, ValueError) as exc:
                flush()
                self._make_poison_batch(rid, line, str(exc)[:200])
                continue
            good.append((rid, rec, len(line)))
        flush()

    def _make_batch(self, good):
        row_ids = [g[0] for g in good]
        records = [g[1] for g in good]
        nbytes = sum(g[2] for g in good)
        arrays = [pa.array(row_ids, type=pa.int64())]
        for col in self.cfg.columns:
            arrays.append(
                pa.array(
                    [rec.get(col["field"]) for rec in records],
                    type=_COLUMN_TYPES[col["type"]],
                )
            )
        rb = pa.RecordBatch.from_arrays(arrays, schema=self.cfg.input_schema)
        ipc_blob = batch_to_bytes(rb)
        batch = BatchState(
            self.next_batch_id, row_ids, records, ipc_blob, nbytes + len(ipc_blob)
        )
        if len(ipc_blob) + proto.HEADER_SIZE > self.cfg.max_frame_bytes:
            # the host would exceed its own frame limit: fail the batch at
            # intake instead of crashing workers with an undeliverable TASK
            batch.state = "done_failed"
            batch.ipc_blob = None
            batch.error = {
                "cause": "oversized_batch",
                "detail": f"batch frame {len(ipc_blob)} exceeds max_frame_bytes",
                "batch_id": batch.batch_id,
                "attempts": 0,
                "log": [],
            }
            self.batches[batch.batch_id] = batch
            self.completed[batch.batch_id] = batch
            self.next_batch_id += 1
            self.n_pending += 1
            self.pending_bytes += batch.nbytes
            self.counters["batches_failed"] += 1
            self.completed_log.append(
                {
                    "batch": batch.batch_id,
                    "outcome": "failed",
                    "cause": "oversized_batch",
                    "attempts": 0,
                    "rows": len(row_ids),
                }
            )
            self._update_hwm()
            return
        self.batches[batch.batch_id] = batch
        self.dispatch_queue.append(batch.batch_id)
        self.next_batch_id += 1
        self.inflight += 1
        self.counters["batches_formed"] += 1
        self._update_hwm()

    def _make_poison_batch(self, row_id, line, detail):
        batch = BatchState(
            self.next_batch_id,
            [row_id],
            [{"_raw": line[:500]}],
            None,
            min(len(line), 4096),
        )
        batch.state = "done_failed"
        batch.error = {
            "cause": "intake_error",
            "detail": detail,
            "batch_id": batch.batch_id,
            "attempts": 0,
            "log": [],
        }
        self.batches[batch.batch_id] = batch
        self.completed[batch.batch_id] = batch
        self.next_batch_id += 1
        self.n_pending += 1
        self.pending_bytes += batch.nbytes
        self.counters["intake_errors"] += 1
        self.counters["batches_failed"] += 1
        self.completed_log.append(
            {
                "batch": batch.batch_id,
                "outcome": "failed",
                "cause": "intake_error",
                "attempts": 0,
                "rows": 1,
            }
        )
        self._update_hwm()

    # ------------------------------------------------------------------
    # dispatch / delivery

    def _maybe_dispatch(self):
        while self.dispatch_queue:
            worker = None
            for w in self.workers:
                if w.state == "idle":
                    worker = w
                    break
            if worker is None:
                return
            bid = self.dispatch_queue.popleft()
            batch = self.batches[bid]
            if batch.state != "queued":
                continue
            batch.state = "dispatched"
            batch.deadline = time.monotonic() + self.cfg.batch_timeout_ms / 1000.0
            worker.state = "busy"
            worker.batch_id = bid
            worker.attempt = batch.attempt
            self._send_frame(
                worker,
                proto.TASK,
                {
                    "batch_id": bid,
                    "generation": worker.generation,
                    "attempt": batch.attempt,
                    "rows": len(batch.row_ids),
                },
                batch.ipc_blob,
            )
            self.counters["batches_dispatched"] += 1

    def _maybe_deliver(self):
        while self.next_deliver_id in self.completed:
            if self.writer_q.full():
                return
            batch = self.completed.pop(self.next_deliver_id)
            self.n_pending -= 1
            self.pending_bytes -= batch.nbytes
            self.writer_q.put(batch)
            self.items_enqueued += 1
            self.next_deliver_id += 1
            self._update_hwm()

    def _writer_main(self):
        try:
            succ = open(self.cfg.success_out, "w", encoding="utf-8")
            fail = open(self.cfg.failure_out, "w", encoding="utf-8")
        except Exception as exc:
            # writer cannot start: keep draining so the main loop never
            # deadlocks on a full queue; the run will abort at finalize.
            traceback.print_exc()
            self.writer_error = exc
            while True:
                batch = self.writer_q.get()
                if batch is None:
                    return
                with self.written_lock:
                    self.written["items"] += 1

        try:
            while True:
                batch = self.writer_q.get()
                if batch is None:
                    break
                if self.cfg.sink_delay_ms_per_batch:
                    time.sleep(self.cfg.sink_delay_ms_per_batch / 1000.0)
                if batch.state == "done_success":
                    cols = batch.result
                    for i, rid in enumerate(batch.row_ids):
                        out = {name: vals[i] for name, vals in cols.items()}
                        line = (
                            json.dumps(
                                {"row_id": rid, "source": batch.records[i], "output": out},
                                ensure_ascii=False,
                            )
                            + "\n"
                        )
                        succ.write(line)
                        self.out_digests["success"].update(line.encode("utf-8"))
                    with self.written_lock:
                        self.written["success_rows"] += len(batch.row_ids)
                else:
                    for i, rid in enumerate(batch.row_ids):
                        line = (
                            json.dumps(
                                {"row_id": rid, "source": batch.records[i], "error": batch.error},
                                ensure_ascii=False,
                            )
                            + "\n"
                        )
                        fail.write(line)
                        self.out_digests["failure"].update(line.encode("utf-8"))
                    with self.written_lock:
                        self.written["failure_rows"] += len(batch.row_ids)
                succ.flush()
                fail.flush()
                with self.written_lock:
                    self.written["items"] += 1
                    if batch.row_ids:
                        self.written["last_row_id"] = batch.row_ids[-1]
                batch.result = None
                batch.records = None
        finally:
            succ.close()
            fail.close()

    # ------------------------------------------------------------------
    # timers

    def _compute_timeout(self):
        now = time.monotonic()
        deadline = now + 0.25
        for w in self.workers:
            if w.state == "busy":
                batch = self.batches.get(w.batch_id)
                if batch is not None and batch.deadline is not None:
                    deadline = min(deadline, batch.deadline)
            elif w.state == "starting":
                deadline = min(deadline, w.handshake_deadline)
            elif w.state == "doomed" and not w.kill_sent:
                deadline = min(deadline, w.grace_deadline)
        if self.phase == "stopping" and self.bye_deadline:
            deadline = min(deadline, self.bye_deadline)
        return max(0.02, deadline - now)

    def _process_timers(self):
        now = time.monotonic()
        for w in self.workers:
            if w.state == "busy":
                batch = self.batches.get(w.batch_id)
                if batch is not None and batch.deadline is not None and now >= batch.deadline:
                    self.counters["worker_timeouts"] += 1
                    self._fail_attempt(batch, "batch_timeout")
                    self._doom(w, "batch_timeout")
            elif w.state == "starting" and now >= w.handshake_deadline:
                self._spawn_failed(w, "handshake_timeout")
            if w.state == "doomed" and not w.kill_sent and now >= w.grace_deadline:
                if w.pid is not None:
                    try:
                        os.killpg(w.pid, signal.SIGKILL)
                    except (ProcessLookupError, PermissionError):
                        pass
                w.kill_sent = True
            if w.state == "dead":
                if not w.reaped:
                    self._try_reap(w)
                if w.reaped:
                    self._maybe_respawn(w)
        if self.phase == "stopping" and self.bye_deadline and now >= self.bye_deadline:
            for w in self.workers:
                if w.state == "stopping":
                    self._doom(w, "bye_timeout", grace_ms=500)
            self.bye_deadline = None

    # ------------------------------------------------------------------
    # stats / manifest

    def _update_hwm(self):
        self.hwm["inflight_batches"] = max(self.hwm["inflight_batches"], self.inflight)
        self.hwm["pending_batches"] = max(self.hwm["pending_batches"], self.n_pending)
        self.hwm["pending_bytes"] = max(self.hwm["pending_bytes"], self.pending_bytes)
        self.hwm["retained_batches"] = max(
            self.hwm["retained_batches"], self.inflight + self.n_pending
        )

    @staticmethod
    def _read_rss_bytes(pid):
        try:
            with open(f"/proc/{pid}/status", "r") as fh:
                for line in fh:
                    if line.startswith("VmRSS:"):
                        return int(line.split()[1]) * 1024
        except (FileNotFoundError, PermissionError):
            pass
        return None

    def _build_stats(self):
        workers = []
        ipc_bytes = 0
        for w in self.workers:
            ipc_bytes += len(w.rbuf) + len(w.wbuf)
            workers.append(
                {
                    "slot": w.slot,
                    "generation": w.generation,
                    "pid": w.pid,
                    "state": w.state,
                    "current_batch": w.batch_id,
                    "rss_bytes": self._read_rss_bytes(w.pid) if w.pid else None,
                    "ipc_rbuf_bytes": len(w.rbuf),
                    "ipc_wbuf_bytes": len(w.wbuf),
                }
            )
        self.hwm["ipc_buffer_bytes"] = max(self.hwm["ipc_buffer_bytes"], ipc_bytes)
        inflight = []
        for w in self.workers:
            if w.state == "busy" and w.batch_id is not None:
                batch = self.batches.get(w.batch_id)
                inflight.append(
                    {
                        "batch": w.batch_id,
                        "slot": w.slot,
                        "generation": w.generation,
                        "attempt": w.attempt,
                        "rows": len(batch.row_ids) if batch else None,
                    }
                )
        with self.written_lock:
            written = dict(self.written)
        return {
            "ts": time.time(),
            "phase": self.phase,
            "read_rows": self.read_rows,
            "read_batches": self.next_batch_id,
            "source_eof": self.source_eof,
            "next_deliver_batch": self.next_deliver_id,
            "delivered": {
                "success_rows": written["success_rows"],
                "failure_rows": written["failure_rows"],
                "last_row_id": written["last_row_id"],
            },
            "gauges": {
                "inflight_batches": self.inflight,
                "pending_batches": self.n_pending,
                "pending_bytes": self.pending_bytes,
                "retained_batches": self.inflight + self.n_pending,
                "dispatch_queue": len(self.dispatch_queue),
                "ipc_buffer_bytes": ipc_bytes,
                "writer_queue": self.writer_q.qsize(),
            },
            "inflight": inflight,
            "workers": workers,
            "counters": dict(self.counters),
            "hwm": dict(self.hwm),
            "completed_log": list(self.completed_log)[-500:],
            "worker_exit_log": list(self.worker_exit_log),
            "limits": self.cfg.limits_dict(),
        }

    def _maybe_snapshot(self, force=False):
        now = time.monotonic()
        if not force and (now - self.last_snapshot) < self.cfg.stats_interval_ms / 1000.0:
            return
        self.last_snapshot = now
        stats = self._build_stats()
        tmp = self.cfg.stats_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(stats, fh)
        os.replace(tmp, self.cfg.stats_path)

    # ------------------------------------------------------------------
    # finalize

    def _sweep_workers(self, timeout=5.0):
        """Last-resort cleanup: SIGKILL every remaining group and reap."""
        for w in self.workers:
            if w.state != "dead" and w.pid is not None:
                try:
                    os.killpg(w.pid, signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    pass
                w.kill_sent = True
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            done = True
            for w in self.workers:
                if w.state != "dead":
                    self._unregister_all(w)
                    self._close_fds(w)
                    w.state = "dead"
                if not w.reaped:
                    self._try_reap(w)
                done = done and w.reaped
            if done:
                return
            time.sleep(0.02)

    def _finalize(self):
        exit_code = {"stopping": 0, "cancelled": 3, "aborted": 1}.get(self.phase, 1)
        try:
            self._sweep_workers()
        except Exception:
            traceback.print_exc()
        try:
            self.writer_q.put(None, timeout=10)
            self.writer_thread.join(timeout=10)
        except Exception:
            traceback.print_exc()
        if self.writer_error is not None and self.phase == "stopping":
            exit_code = 1
            manifest_status = "aborted"
        else:
            manifest_status = {
                "stopping": "completed",
                "cancelled": "cancelled",
                "aborted": "aborted",
            }.get(self.phase, "aborted")
        manifest = {
            "status": manifest_status,
            "abort_reason": self.abort_reason,
            "plugin": self.plugin_identity,
            "input_source": (
                "quality_gate_snapshot" if self.gate is not None else "configured_input"
            ),
            "input_snapshot": (
                {
                    "path": self.gate.snapshot_path,
                    "sha256": self.gate.snapshot_sha256,
                    "rule_summary": self.gate.report.get("rule_summary"),
                    "report_status": self.gate.report.get("status"),
                }
                if self.gate is not None
                else None
            ),
            "input_rows_read": self.read_rows,
            "batches_formed": self.next_batch_id,
            "delivered_batches": self.next_deliver_id,
            "delivered_rows_success": self.written["success_rows"],
            "delivered_rows_failure": self.written["failure_rows"],
            "last_delivered_row_id": self.written["last_row_id"],
            # Content digests of the delivered streams, computed over the
            # exact bytes written; `colbatch compare` re-hashes the artifacts
            # and refuses to reconcile a run whose outputs do not match the
            # digests and row counts recorded here.
            "outputs": {
                "success": {
                    "path": self.cfg.success_out,
                    "sha256": self.out_digests["success"].hexdigest(),
                    "rows": self.written["success_rows"],
                },
                "failure": {
                    "path": self.cfg.failure_out,
                    "sha256": self.out_digests["failure"].hexdigest(),
                    "rows": self.written["failure_rows"],
                },
            },
            "elapsed_ms": int((time.monotonic() - self.start_time) * 1000),
            "counters": dict(self.counters),
            "hwm": dict(self.hwm),
            "limits": self.cfg.limits_dict(),
            "completed_log": list(self.completed_log),
            "worker_exit_log": list(self.worker_exit_log),
        }
        try:
            with open(self.cfg.manifest_path, "w", encoding="utf-8") as fh:
                json.dump(manifest, fh, indent=1)
        except Exception:
            traceback.print_exc()
        try:
            self._maybe_snapshot(force=True)
        except Exception:
            traceback.print_exc()
        try:
            self.input_f.close()
        except Exception:
            pass
        return exit_code
