"""Worker process entry point.

Runs inside a dedicated OS process spawned by the host.  Loads one plugin,
performs the startup handshake on its stdout pipe, then serves TASK frames
from stdin one at a time (a worker never juggles two batches at once, which
keeps batch identity unambiguous per pipe).

Launched as:  python -I <abs path>/worker.py --plugin <path> [--fault-spec <path>]

-I (isolated) keeps sys.path clean; we add only the project root ourselves.
Plugins are loaded by absolute path and must be self-contained.

Fault injection (env COLBATCH_FAULT_SPEC, JSON file) exists ONLY to let the
test-suite break this worker in controlled ways.  It is compiled in here --
rather than in the plugins -- so the demo plugins stay realistic.  Never
used in normal runs.

Fault spec format:
    { "<selector>": ["action", ...], ... }
selector: "b<batch_id>@<attempt>" | "b<batch_id>" | "*"
actions (applied in order):
    pre-transform:
        ignore_term                     ignore SIGTERM from now on
        exit:<code>                     os._exit(<code>) immediately
        hang:<seconds>                  sleep (host will time out / kill)
        task_delay:<seconds>            benign sleep, then continue
        barrier:<dir>:<n>               file barrier: wait until <n> parties
        burn_cpu                        busy-loop (for RLIMIT_CPU tests)
        alloc:<megabytes>               hold a bytearray of this size
        spawn_child_then_hang:<pidfile> spawn a SIGTERM-ignoring child that
                                        writes its pid to <pidfile>, then hang
    post-transform (send path):
        gated_result_then_exit:<gate>   wait for <gate> file, send RESULT, exit
        tamper_row_id                   swap two row_ids in the result
        bad_row_count                   drop the last result row
        bad_schema                      rename the last output column
        half_frame                      write a truncated frame, then _exit(1)
        huge_len                        write a header declaring 2^40 bytes,
                                        then _exit(1)
        duplicate_response              send the RESULT frame twice
"""

from __future__ import annotations

import json
import os
import signal
import struct
import sys
import time
import traceback

# -I strips the script directory from sys.path; add the project root so the
# colbatch package is importable.  Only the project root is added.
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from colbatch import protocol as proto  # noqa: E402
from colbatch.sdk import (  # noqa: E402
    ContractError,
    batch_to_bytes,
    bytes_to_batch,
    load_plugin,
    schema_to_bytes,
    validate_result,
)

_IN = 0   # stdin:  host -> worker
_OUT = 1  # stdout: worker -> host


# ---------------------------------------------------------------------------
# low-level IO (blocking; the worker is deliberately simple and synchronous)

def _read_exact(n: int) -> bytes:
    chunks = []
    left = n
    while left > 0:
        data = os.read(_IN, left)
        if not data:
            raise EOFError("host closed the pipe")
        chunks.append(data)
        left -= len(data)
    return b"".join(chunks)


def read_frame(max_frame_bytes: int):
    header = _read_exact(proto.HEADER_SIZE)
    mtype, length = proto.parse_header(header)
    if length > max_frame_bytes:
        raise proto.ProtocolError(f"declared length {length} exceeds limit")
    payload = _read_exact(length) if length else b""
    meta, blob = proto.decode_payload(payload)
    return mtype, meta, blob


def send_frame(mtype: int, meta: dict, blob: bytes = b"") -> None:
    data = proto.encode(mtype, meta, blob)
    view = memoryview(data)
    while view:
        written = os.write(_OUT, view)
        view = view[written:]


# ---------------------------------------------------------------------------
# fault injection (test-only)

class FaultTimeout(Exception):
    pass


def _wait_gate(path: str, timeout: float = 300.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if os.path.exists(path):
            return
        time.sleep(0.02)
    raise FaultTimeout(f"gate {path} not released")


def _barrier(path: str, parties: int, timeout: float = 120.0) -> None:
    os.makedirs(path, exist_ok=True)
    marker = os.path.join(path, f"p{os.getpid()}_{time.time_ns()}")
    with open(marker, "w") as fh:
        fh.write("x")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if len(os.listdir(path)) >= parties:
                return
        except FileNotFoundError:
            pass
        time.sleep(0.02)
    raise FaultTimeout(f"barrier {path} timed out")


class Faults:
    """Parsed fault spec; consulted once per task."""

    def __init__(self, path: str | None):
        self.spec = {}
        if path:
            with open(path, "r", encoding="utf-8") as fh:
                self.spec = json.load(fh)

    def actions_for(self, batch_id: int, attempt: int) -> list[str]:
        actions = []
        actions += self.spec.get("*", [])
        actions += self.spec.get(f"b{batch_id}", [])
        actions += self.spec.get(f"b{batch_id}@{attempt}", [])
        return actions


_KEEPALIVE = []  # keeps fault allocations alive for the duration of a task


def apply_pre_faults(actions) -> None:
    for action in actions:
        name, _, arg = action.partition(":")
        if name == "ignore_term":
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
        elif name == "exit":
            os._exit(int(arg))
        elif name == "hang":
            time.sleep(float(arg))
        elif name == "task_delay":
            time.sleep(float(arg))
        elif name == "barrier":
            path, _, n = arg.rpartition(":")
            _barrier(path, int(n))
        elif name == "burn_cpu":
            end = time.monotonic() + 600.0
            x = 0
            while time.monotonic() < end:
                x += 1
        elif name == "alloc":
            _KEEPALIVE.append(bytearray(int(float(arg) * 1024 * 1024)))
        elif name == "spawn_child_then_hang":
            pidfile = arg
            child_code = (
                "import os,signal,time,sys;"
                "signal.signal(signal.SIGTERM, signal.SIG_IGN);"
                "open(sys.argv[1],'w').write(str(os.getpid()));"
                "time.sleep(600)"
            )
            # intentionally NOT isolated/new-session: the child stays in this
            # worker's process group so the host's group-kill must reap it.
            import subprocess

            subprocess.Popen([sys.executable, "-c", child_code, pidfile])
            time.sleep(600.0)
        elif name in (
            "gated_result_then_exit",
            "tamper_row_id",
            "bad_row_count",
            "bad_schema",
            "half_frame",
            "huge_len",
            "duplicate_response",
        ):
            pass  # handled post-transform
        else:
            raise FaultTimeout(f"unknown fault action {action!r}")


def apply_send_faults(actions, result, meta, blob) -> bool:
    """Apply post-transform faults.  Returns True if the normal RESULT was
    already (or should not be) sent by the fault itself."""
    sent_normally = False
    for action in actions:
        name, _, arg = action.partition(":")
        if name == "tamper_row_id":
            import pyarrow as pa

            ids = result.column(0).to_pylist()
            if len(ids) >= 2:
                ids[0], ids[1] = ids[1], ids[0]
            arrays = [pa.array(ids, type=pa.int64())] + [
                result.column(i) for i in range(1, result.num_columns)
            ]
            result = type(result).from_arrays(arrays, schema=result.schema)
            blob = batch_to_bytes(result)
        elif name == "bad_row_count":
            result = result.slice(0, max(0, result.num_rows - 1))
            blob = batch_to_bytes(result)
        elif name == "bad_schema":
            import pyarrow as pa

            schema = result.schema
            fields = list(schema)
            last = fields[-1]
            fields[-1] = last.with_name(last.name + "_x")
            result = type(result).from_arrays(
                [result.column(i) for i in range(result.num_columns)],
                schema=pa.schema(fields),
            )
            blob = batch_to_bytes(result)
        elif name == "gated_result_then_exit":
            _wait_gate(arg)
            send_frame(proto.RESULT, meta, blob)
            os._exit(0)
        elif name == "half_frame":
            frame = proto.encode(proto.RESULT, meta, blob)
            cut = max(proto.HEADER_SIZE + 1, (len(frame) * 3) // 4)
            os.write(_OUT, frame[:cut])
            os._exit(1)
        elif name == "huge_len":
            header = struct.pack("<4sBBHQ", proto.MAGIC, proto.RESULT, 0, 0, 1 << 40)
            os.write(_OUT, header)
            os._exit(1)
        elif name == "duplicate_response":
            send_frame(proto.RESULT, meta, blob)
            send_frame(proto.RESULT, meta, blob)
            sent_normally = True
    if sent_normally:
        return True, result, blob
    return False, result, blob


# ---------------------------------------------------------------------------

def main() -> int:
    # Protect the protocol: anything a plugin print()s goes to stderr,
    # stdout (fd 1) stays reserved for frames.
    sys.stdout = sys.stderr

    plugin_path = None
    max_frame_bytes = 1 << 28
    argv = sys.argv[1:]
    while argv:
        arg = argv.pop(0)
        if arg == "--plugin":
            plugin_path = argv.pop(0)
        elif arg == "--max-frame-bytes":
            max_frame_bytes = int(argv.pop(0))
        else:
            raise SystemExit(f"unknown worker argument {arg!r}")
    if not plugin_path:
        raise SystemExit("worker needs --plugin")

    faults = Faults(os.environ.get("COLBATCH_FAULT_SPEC") or None)

    try:
        plugin = load_plugin(plugin_path)
    except Exception:
        traceback.print_exc()
        return 2

    send_frame(
        proto.HELLO,
        {
            "proto": proto.PROTOCOL_VERSION,
            "plugin": {"name": plugin.name, "version": plugin.version},
            "pid": os.getpid(),
            "in_schema": schema_to_bytes(plugin.input_schema()).hex(),
            "out_schema": schema_to_bytes(plugin.output_schema()).hex(),
        },
    )
    mtype, meta, _ = read_frame(max_frame_bytes)
    if mtype != proto.HELLO_ACK or not meta.get("ok"):
        sys.stderr.write(f"worker: handshake rejected: {meta}\n")
        return 3
    send_frame(proto.READY, {})

    while True:
        try:
            mtype, meta, blob = read_frame(max_frame_bytes)
        except EOFError:
            return 0
        if mtype == proto.SHUTDOWN:
            send_frame(proto.BYE, {})
            return 0
        if mtype != proto.TASK:
            sys.stderr.write(f"worker: unexpected frame {proto.NAMES.get(mtype)}\n")
            return 4

        batch_id = meta["batch_id"]
        attempt = meta["attempt"]
        actions = faults.actions_for(batch_id, attempt)
        try:
            apply_pre_faults(actions)
            batch = bytes_to_batch(blob)
            row_ids = batch.column(0).to_pylist()
            try:
                result = plugin.transform(batch)
            except MemoryError:
                send_frame(
                    proto.BATCH_ERROR,
                    {
                        "batch_id": batch_id,
                        "generation": meta["generation"],
                        "attempt": attempt,
                        "code": "memory_error",
                        "message": "MemoryError inside plugin.transform",
                    },
                )
                continue
            except Exception as exc:  # plugin bug -> batch execution failure
                send_frame(
                    proto.BATCH_ERROR,
                    {
                        "batch_id": batch_id,
                        "generation": meta["generation"],
                        "attempt": attempt,
                        "code": "plugin_exception",
                        "message": f"{type(exc).__name__}: {exc}"[:500],
                    },
                )
                continue
            try:
                validate_result(row_ids, result, plugin.output_schema())
            except ContractError as exc:
                send_frame(
                    proto.BATCH_ERROR,
                    {
                        "batch_id": batch_id,
                        "generation": meta["generation"],
                        "attempt": attempt,
                        "code": "contract_violation",
                        "message": str(exc)[:500],
                    },
                )
                continue
            out_blob = batch_to_bytes(result)
            out_meta = {
                "batch_id": batch_id,
                "generation": meta["generation"],
                "attempt": attempt,
                "rows": result.num_rows,
            }
            already, result, out_blob = apply_send_faults(actions, result, out_meta, out_blob)
            if not already:
                send_frame(proto.RESULT, out_meta, out_blob)
        except FaultTimeout as exc:
            send_frame(
                proto.BATCH_ERROR,
                {
                    "batch_id": batch_id,
                    "generation": meta["generation"],
                    "attempt": attempt,
                    "code": "fault_timeout",
                    "message": str(exc)[:500],
                },
            )
        except MemoryError:
            send_frame(
                proto.BATCH_ERROR,
                {
                    "batch_id": batch_id,
                    "generation": meta["generation"],
                    "attempt": attempt,
                    "code": "memory_error",
                    "message": "MemoryError while preparing the task",
                },
            )
        finally:
            _KEEPALIVE.clear()


if __name__ == "__main__":
    sys.exit(main())
