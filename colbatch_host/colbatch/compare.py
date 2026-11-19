"""Run-to-run reconciliation: ``colbatch compare`` / ``colbatch compare-status``.

Given two *completed* cleaning runs of the same immutable quality snapshot
(a baseline and a candidate), reconcile their delivered results row by row
and publish an auditable change set:

  * every source row of the shared snapshot is paired exactly once across
    the two runs, by ``row_id`` -- never by file line number or by position
    inside the success stream alone;
  * each row is classified as ``unchanged``, ``success_fields_changed``,
    ``success_to_failure``, ``failure_to_success`` or
    ``failure_reason_changed``;
  * the per-row detail (JSONL, ascending ``row_id``) and the summary report
    are published as one atomic group: any failure leaves a previously
    published valid report untouched and no half-written new one.

Trust model: the run manifest is the run's own verifiable statement -- it
records the bound snapshot digest, the plugin identity, the delivered row
counts and the SHA-256 of each delivered stream.  The comparator re-hashes
every artifact it consumes and refuses to reconcile when any digest, count
or identity disagrees.  Streams are opened exactly once and consumed
through the open file object, so a replaced path can never mix two byte
versions into one comparison, while an in-place rewrite is detected via
the descriptor's identity before anything is published.

Memory is bounded: the four streams (two per run) are already delivered in
``row_id`` order by the host, so each side is merged by a two-way merge and
the two sides by a synchronised merge-join; at most one record per stream
is held in RAM.  Disorder, duplicates, gaps or identity conflicts inside a
stream are *detected and rejected*, never silently repaired.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from dataclasses import dataclass

from .preflight import stable_relative
from .quality import canonical_dumps, canonical_jsonable

REPORT_SCHEMA_VERSION = 1
DETAIL_SCHEMA_VERSION = 1

EXIT_OK = 0
EXIT_CONFIG_ERROR = 3
EXIT_DATA_ERROR = 4
EXIT_EXEC_ERROR = 5

# Row classifications recorded in the detail stream and summarised in the
# report.  "unchanged" rows are counted but not written to the detail file.
CLASS_UNCHANGED = "unchanged"
CLASS_FIELDS_CHANGED = "success_fields_changed"
CLASS_SUCCESS_TO_FAILURE = "success_to_failure"
CLASS_FAILURE_TO_SUCCESS = "failure_to_success"
CLASS_FAILURE_REASON_CHANGED = "failure_reason_changed"
CHANGED_CLASSES = (
    CLASS_FIELDS_CHANGED,
    CLASS_SUCCESS_TO_FAILURE,
    CLASS_FAILURE_TO_SUCCESS,
    CLASS_FAILURE_REASON_CHANGED,
)

# Keys of a failure record's ``error`` object that describe the runner, not
# the business outcome: batch bookkeeping, attempt history, process/host
# identity, wall-clock fields and scratch paths.  They legitimately differ
# between two runs of identical logic (different batching, different pids)
# and must never be read as a transformation change.  Everything else in
# the error object is business evidence and participates in the comparison.
NON_BUSINESS_ERROR_KEYS = frozenset(
    {
        "batch_id",
        "attempts",
        "log",
        "pid",
        "worker_pid",
        "process_id",
        "slot",
        "worker_slot",
        "generation",
        "worker_generation",
        "timestamp",
        "ts",
        "time",
        "elapsed_ms",
        "duration_ms",
        "started_at",
        "finished_at",
        "tmp_path",
        "temp_path",
        "temp_dir",
        "tmp_dir",
        "hostname",
        "host",
        "machine",
    }
)

_SIDE_NAMES = ("baseline", "candidate")
_SIDE_KEYS = ("manifest", "success", "failure")
_TOP_KEYS = (
    "baseline",
    "candidate",
    "report_dir",
    "temp_dir",
    "read_block_bytes",
    "max_line_bytes",
)

_HEX64 = set("0123456789abcdef")


class CompareConfigError(Exception):
    """The compare configuration itself is invalid (exit 3)."""


class CompareDataError(Exception):
    """The two runs cannot be reconciled: missing, inconsistent, corrupt or
    mutually incompatible inputs (exit 4).  Carries a stable ``reason``."""

    def __init__(self, reason: str, message: str):
        super().__init__(message)
        self.reason = reason


class CompareExecError(Exception):
    """The comparator itself could not complete: I/O, mutation during the
    comparison, cancellation (exit 5).  Carries a stable ``reason``."""

    def __init__(self, reason: str, message: str):
        super().__init__(message)
        self.reason = reason


def _reject_constant(value: str):
    # Python's json module accepts non-standard NaN/Infinity tokens; they are
    # malformed for this contract (same stance as the preflight profiler).
    raise ValueError(f"non-standard JSON constant: {value}")


def _is_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_sha256(value) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(c in _HEX64 for c in value)
    )


# ---------------------------------------------------------------------------
# configuration
#


class CompareConfig:
    """Parsed compare config.  Unknown keys are configuration errors."""

    def __init__(self, raw: dict, base_dir: str):
        if not isinstance(raw, dict):
            raise CompareConfigError("compare config must be a JSON object")
        unknown = sorted(set(raw) - set(_TOP_KEYS))
        if unknown:
            raise CompareConfigError(
                f"unknown compare config field(s): {', '.join(unknown)}"
            )
        # Run-artifact references inside the report are rendered relative to
        # the compare config's own directory (falling back to the stable
        # ``external:`` marker), never as machine absolute paths.
        self.base_dir = base_dir

        def resolve(p):
            if not isinstance(p, str) or not p:
                raise CompareConfigError("path values must be non-empty strings")
            if not os.path.isabs(p):
                p = os.path.join(base_dir, p)
            return os.path.abspath(p)

        self.sides = {}
        for name in _SIDE_NAMES:
            raw_side = raw.get(name)
            if not isinstance(raw_side, dict):
                raise CompareConfigError(f"compare config needs a {name!r} object")
            unknown_side = sorted(set(raw_side) - set(_SIDE_KEYS))
            if unknown_side:
                raise CompareConfigError(
                    f"unknown {name} field(s): {', '.join(unknown_side)}"
                )
            missing = [k for k in _SIDE_KEYS if k not in raw_side]
            if missing:
                raise CompareConfigError(
                    f"{name} is missing field(s): {', '.join(missing)}"
                )
            self.sides[name] = {
                "manifest": resolve(raw_side["manifest"]),
                "success": resolve(raw_side["success"]),
                "failure": resolve(raw_side["failure"]),
            }

        report_dir = raw.get("report_dir")
        if report_dir is None:
            raise CompareConfigError("compare config needs report_dir")
        self.report_dir = resolve(report_dir)
        if os.path.exists(self.report_dir) and not os.path.isdir(self.report_dir):
            raise CompareConfigError(
                f"report_dir exists and is not a directory: {self.report_dir}"
            )
        temp_dir = raw.get("temp_dir")
        self.temp_dir = (
            resolve(temp_dir)
            if temp_dir is not None
            else os.path.join(self.report_dir, "tmp")
        )
        if os.path.exists(self.temp_dir) and not os.path.isdir(self.temp_dir):
            raise CompareConfigError(
                f"temp_dir exists and is not a directory: {self.temp_dir}"
            )

        self.read_block_bytes = raw.get("read_block_bytes", 1 << 20)
        if not _is_int(self.read_block_bytes) or self.read_block_bytes < 4096:
            raise CompareConfigError("read_block_bytes must be an integer >= 4096")
        self.max_line_bytes = raw.get("max_line_bytes", 1 << 26)
        if not _is_int(self.max_line_bytes) or self.max_line_bytes < 4096:
            raise CompareConfigError("max_line_bytes must be an integer >= 4096")

        self.report_path = os.path.join(self.report_dir, "report.json")
        self.error_path = os.path.join(self.report_dir, "last_error.json")
        self.changes_dir = os.path.join(self.report_dir, "changes")


# ---------------------------------------------------------------------------
# JSON value semantics (documented in docs/compare.md)
#


def json_type_tag(value) -> str:
    """The six-way JSON type lattice used for comparison."""
    if value is None:
        return "null"
    if isinstance(value, bool):  # before int: bool is a subclass of int
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    return "object"


def json_equal(a, b) -> bool:
    """Semantic JSON equality.

    Values of different lattice types are never equal (``1`` != ``true`` !=
    ``"1"`` != ``null``).  Numbers compare by numeric value (``1`` equals
    ``1.0``; ``-0.0`` equals ``0.0``).  Arrays are ordered; objects compare
    as key->value maps, so key order never manufactures a difference.
    """
    ta = json_type_tag(a)
    if ta != json_type_tag(b):
        return False
    if ta in ("null", "boolean", "string", "number"):
        return a == b
    if ta == "array":
        return len(a) == len(b) and all(json_equal(x, y) for x, y in zip(a, b))
    if set(a.keys()) != set(b.keys()):
        return False
    return all(json_equal(a[k], b[k]) for k in a)


def _escape_pointer(segment: str) -> str:
    return segment.replace("~", "~0").replace("/", "~1")


def unescape_pointer(segment: str) -> str:
    # RFC 6901: evaluate ~1 first, then ~0 ("~01" decodes to "~1").
    return segment.replace("~1", "/").replace("~0", "~")


def diff_values(base, cand, path: str, out: list) -> None:
    """Append stable-path diff entries for two JSON values.

    Entry shapes (presence of a key marks presence of that side's value):
      ``{"path": p, "base": v, "candidate": w}`` -- both sides present;
      ``{"path": p, "base": v}``                    -- removed by candidate;
      ``{"path": p, "candidate": w}``               -- added by candidate.
    Object keys are visited sorted, array indices ascending: the emitted
    sequence is deterministic.
    """
    if json_equal(base, cand):
        return
    ta = json_type_tag(base)
    tb = json_type_tag(cand)
    if ta == "object" and tb == "object":
        for key in sorted(set(base) | set(cand)):
            p = path + "/" + _escape_pointer(key)
            in_b = key in base
            in_c = key in cand
            if in_b and in_c:
                diff_values(base[key], cand[key], p, out)
            elif in_b:
                out.append({"path": p, "base": base[key]})
            else:
                out.append({"path": p, "candidate": cand[key]})
        return
    if ta == "array" and tb == "array":
        for i in range(max(len(base), len(cand))):
            p = f"{path}/{i}"
            in_b = i < len(base)
            in_c = i < len(cand)
            if in_b and in_c:
                diff_values(base[i], cand[i], p, out)
            elif in_b:
                out.append({"path": p, "base": base[i]})
            else:
                out.append({"path": p, "candidate": cand[i]})
        return
    out.append({"path": path, "base": base, "candidate": cand})


def normalize_error(error: dict) -> dict:
    """Business identity of a failure record's error object.

    Runner mechanics (batch ids, attempt logs, pids, wall-clock fields,
    scratch paths) are stripped at the top level; what remains -- cause,
    detail and any plugin-emitted business fields -- is the evidence a
    reviewer cares about.
    """
    return {k: v for k, v in error.items() if k not in NON_BUSINESS_ERROR_KEYS}


# ---------------------------------------------------------------------------
# manifest loading and cross-checks
#


def _require(cond, reason, message):
    if not cond:
        raise CompareDataError(reason, message)


def _manifest_int(manifest, key, label):
    val = manifest.get(key)
    _require(
        _is_int(val) and val >= 0,
        "manifest_invalid",
        f"{label}: manifest field {key!r} must be a non-negative integer, "
        f"got {val!r}",
    )
    return val


@dataclass
class RunSide:
    label: str
    manifest_path: str
    success_path: str
    failure_path: str
    manifest_fh: object = None
    manifest_sha256: str = ""
    manifest: dict = None
    plugin: dict = None
    snapshot_sha256: str = ""
    rule_summary: str = None
    input_rows: int = 0
    outputs: dict = None


def _load_side(label: str, paths: dict) -> RunSide:
    side = RunSide(
        label=label,
        manifest_path=paths["manifest"],
        success_path=paths["success"],
        failure_path=paths["failure"],
    )
    try:
        fh = open(side.manifest_path, "rb")
    except OSError as exc:
        raise CompareDataError(
            "manifest_unreadable",
            f"{label}: cannot open manifest {paths['manifest']}: {exc}",
        ) from exc
    side.manifest_fh = fh
    try:
        _load_side_body(side)
    except BaseException:
        # validation failures happen before the side is tracked by the
        # caller; do not leak the descriptor
        try:
            fh.close()
        except OSError:
            pass
        side.manifest_fh = None
        raise
    return side


def _load_side_body(side: RunSide) -> None:
    label = side.label
    fh = side.manifest_fh
    try:
        raw = fh.read()
    except OSError as exc:
        raise CompareExecError(
            "io_error", f"{label}: cannot read manifest: {exc}"
        ) from exc
    side.manifest_sha256 = hashlib.sha256(raw).hexdigest()
    try:
        text = raw.decode("utf-8")
        manifest = json.loads(text, parse_constant=_reject_constant)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise CompareDataError(
            "manifest_invalid", f"{label}: manifest is not valid JSON: {exc}"
        ) from exc
    _require(
        isinstance(manifest, dict),
        "manifest_invalid",
        f"{label}: manifest is not a JSON object",
    )
    side.manifest = manifest

    status = manifest.get("status")
    _require(
        status == "completed",
        "run_not_completed",
        f"{label}: run status is {status!r}; only fully completed runs "
        "can be reconciled (cancelled/aborted/partial runs are refused)",
    )
    _require(
        manifest.get("abort_reason") is None,
        "run_not_completed",
        f"{label}: manifest records abort_reason "
        f"{manifest.get('abort_reason')!r}",
    )

    snap = manifest.get("input_snapshot")
    _require(
        isinstance(snap, dict) and _is_sha256(snap.get("sha256")),
        "run_not_bound_to_snapshot",
        f"{label}: run is not bound to an immutable quality snapshot "
        "(manifest input_snapshot.sha256 missing or malformed); runs "
        "without the quality gate cannot be reconciled",
    )
    side.snapshot_sha256 = snap["sha256"]
    rule_summary = snap.get("rule_summary")
    _require(
        rule_summary is None or isinstance(rule_summary, str),
        "manifest_invalid",
        f"{label}: input_snapshot.rule_summary must be a string",
    )
    side.rule_summary = rule_summary

    plugin = manifest.get("plugin")
    _require(
        isinstance(plugin, dict)
        and isinstance(plugin.get("name"), str)
        and plugin.get("name") != ""
        and isinstance(plugin.get("version"), str)
        and plugin.get("version") != "",
        "incomplete_run_identity",
        f"{label}: manifest lacks a complete plugin identity "
        "(plugin.name / plugin.version)",
    )
    side.plugin = {"name": plugin["name"], "version": plugin["version"]}

    n_rows = _manifest_int(manifest, "input_rows_read", label)
    succ_rows = _manifest_int(manifest, "delivered_rows_success", label)
    fail_rows = _manifest_int(manifest, "delivered_rows_failure", label)
    _require(
        succ_rows + fail_rows == n_rows,
        "partial_delivery",
        f"{label}: delivered rows ({succ_rows}+{fail_rows}) do not cover "
        f"the input ({n_rows}); the run did not deliver every source row",
    )
    last_rid = manifest.get("last_delivered_row_id")
    _require(
        _is_int(last_rid) and last_rid == n_rows - 1,
        "inconsistent_counts",
        f"{label}: last_delivered_row_id {last_rid!r} disagrees with "
        f"input_rows_read {n_rows}",
    )
    side.input_rows = n_rows

    outputs = manifest.get("outputs")
    _require(
        isinstance(outputs, dict)
        and isinstance(outputs.get("success"), dict)
        and isinstance(outputs.get("failure"), dict),
        "incomplete_outputs",
        f"{label}: manifest lacks the outputs section with per-stream "
        "digests (re-run with a host that records output digests)",
    )
    side.outputs = {}
    for kind, expected_rows in (("success", succ_rows), ("failure", fail_rows)):
        entry = outputs[kind]
        sha = entry.get("sha256")
        rows = entry.get("rows")
        _require(
            _is_sha256(sha),
            "incomplete_outputs",
            f"{label}: manifest outputs.{kind}.sha256 missing or malformed",
        )
        _require(
            _is_int(rows) and rows >= 0,
            "incomplete_outputs",
            f"{label}: manifest outputs.{kind}.rows must be a non-negative "
            "integer",
        )
        _require(
            rows == expected_rows,
            "inconsistent_counts",
            f"{label}: manifest outputs.{kind}.rows ({rows}) disagrees with "
            f"delivered_rows_{kind} ({expected_rows})",
        )
        side.outputs[kind] = {"sha256": sha, "rows": rows}
    return side


def _cross_check_sides(base: RunSide, cand: RunSide) -> None:
    _require(
        base.snapshot_sha256 == cand.snapshot_sha256,
        "snapshot_mismatch",
        "baseline and candidate are bound to different input snapshots "
        f"({base.snapshot_sha256[:16]}… vs {cand.snapshot_sha256[:16]}…); "
        "only runs of the same immutable snapshot can be reconciled",
    )
    _require(
        base.input_rows == cand.input_rows,
        "coverage_mismatch",
        f"baseline covers {base.input_rows} input rows but candidate covers "
        f"{cand.input_rows}; the two runs do not cover the same source set",
    )


# ---------------------------------------------------------------------------
# streaming readers and merges
#


class _StreamReader:
    """One delivered stream, opened once and consumed sequentially.

    Hashes every byte as read (for the manifest digest check), enforces the
    per-line size bound, parses and validates each record, and requires a
    strictly increasing, in-range ``row_id`` sequence.
    """

    def __init__(self, path, kind, side_label, n_rows, max_line_bytes, block_bytes):
        self.path = path
        self.kind = kind
        self.side = side_label
        self.n_rows = n_rows
        self.max_line_bytes = max_line_bytes
        self.block_bytes = block_bytes
        try:
            self.fh = open(path, "rb")
        except OSError as exc:
            raise CompareDataError(
                "artifact_unreadable",
                f"{side_label}: cannot open {kind} stream {path}: {exc}",
            ) from exc
        st = os.fstat(self.fh.fileno())
        self.identity = (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns)
        self.hasher = hashlib.sha256()
        self.rows = 0
        self.prev_row_id = None
        self.buf = bytearray()
        self.eof = False
        self.line_no = 0

    def _where(self):
        return f"{self.side} {self.kind} stream line {self.line_no}"

    def _fail(self, reason, message):
        raise CompareDataError(reason, f"{self._where()}: {message}")

    def close(self):
        try:
            self.fh.close()
        except OSError:
            pass

    def check_identity_stable(self):
        st = os.fstat(self.fh.fileno())
        current = (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns)
        if current != self.identity:
            raise CompareExecError(
                "input_mutated",
                f"{self.side} {self.kind} stream {self.path} was modified "
                "during the comparison; refusing to publish",
            )

    def finish(self, expected_sha256, expected_rows):
        """End-of-stream verification against the manifest statement."""
        if self.rows != expected_rows:
            raise CompareDataError(
                "artifact_row_count_mismatch",
                f"{self.side} {self.kind} stream delivered {self.rows} "
                f"row(s) but the manifest records {expected_rows}",
            )
        digest = self.hasher.hexdigest()
        if digest != expected_sha256:
            raise CompareDataError(
                "artifact_digest_mismatch",
                f"{self.side} {self.kind} stream SHA-256 {digest[:16]}… "
                f"does not match the manifest digest {expected_sha256[:16]}…",
            )
        self.check_identity_stable()

    def next(self):
        """Return the next parsed record, or None at end of stream."""
        while True:
            nl = self.buf.find(b"\n")
            if nl >= 0:
                raw = bytes(self.buf[:nl])
                del self.buf[: nl + 1]
                return self._record(raw)
            if self.eof:
                if self.buf:
                    raw = bytes(self.buf)
                    self.buf.clear()
                    return self._record(raw)
                return None
            try:
                chunk = self.fh.read(self.block_bytes)
            except OSError as exc:
                raise CompareExecError(
                    "io_error",
                    f"{self.side} {self.kind} stream read failed: {exc}",
                ) from exc
            if not chunk:
                self.eof = True
                continue
            self.hasher.update(chunk)
            self.buf += chunk
            if len(self.buf) > self.max_line_bytes and self.buf.find(b"\n") < 0:
                self._fail(
                    "line_too_long",
                    f"line exceeds max_line_bytes ({self.max_line_bytes})",
                )

    def _record(self, raw: bytes):
        self.line_no += 1
        if len(raw) > self.max_line_bytes:
            self._fail(
                "line_too_long",
                f"line of {len(raw)} bytes exceeds max_line_bytes "
                f"({self.max_line_bytes})",
            )
        if not raw.strip():
            self._fail("empty_line", "empty line inside a delivered stream")
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            self._fail("invalid_utf8", f"not valid UTF-8: {exc}")
        try:
            rec = json.loads(text, parse_constant=_reject_constant)
        except (json.JSONDecodeError, ValueError) as exc:
            self._fail("invalid_json", f"invalid JSON: {str(exc)[:160]}")
        if not isinstance(rec, dict):
            self._fail("invalid_record", "stream record is not a JSON object")

        rid = rec.get("row_id")
        if not _is_int(rid):
            self._fail(
                "invalid_row_id",
                f"row_id must be a JSON integer, got {rec.get('row_id')!r}",
            )
        if rid < 0 or rid >= self.n_rows:
            self._fail(
                "row_id_out_of_range",
                f"row_id {rid} is outside the delivered range "
                f"[0, {self.n_rows})",
            )
        if self.prev_row_id is not None:
            if rid == self.prev_row_id:
                self._fail("duplicate_row_id", f"duplicate row_id {rid}")
            if rid < self.prev_row_id:
                self._fail(
                    "row_id_out_of_order",
                    f"row_id {rid} follows {self.prev_row_id}; delivered "
                    "streams must be in ascending row_id order",
                )
        self.prev_row_id = rid

        if "source" not in rec:
            self._fail("invalid_record", "record lacks the source field")
        if self.kind == "success":
            if not isinstance(rec.get("output"), dict):
                self._fail(
                    "invalid_record",
                    "success record lacks an output object",
                )
        else:
            if not isinstance(rec.get("error"), dict):
                self._fail(
                    "invalid_record",
                    "failure record lacks an error object",
                )
        self.rows += 1
        return rec


class _SideMerger:
    """Merge one run's success and failure streams by row_id.

    Both streams are strictly increasing (verified by the readers); the
    merged sequence must be exactly ``0, 1, …, n_rows-1`` -- a gap is a
    missing source row, an equal head in both streams is an identity
    conflict (one source row delivered as both success and failure).
    """

    def __init__(self, success_reader, failure_reader, side_label, n_rows):
        self.side = side_label
        self.n_rows = n_rows
        self.s_reader = success_reader
        self.f_reader = failure_reader
        self.s_rec = success_reader.next()
        self.f_rec = failure_reader.next()
        self.expected = 0

    def next(self):
        """Return ``(row_id, kind, record)`` or None when exhausted."""
        if self.s_rec is None and self.f_rec is None:
            return None
        if self.f_rec is None:
            kind, rec = "success", self.s_rec
            self.s_rec = self.s_reader.next()
        elif self.s_rec is None:
            kind, rec = "failure", self.f_rec
            self.f_rec = self.f_reader.next()
        else:
            s_rid = self.s_rec["row_id"]
            f_rid = self.f_rec["row_id"]
            if s_rid == f_rid:
                raise CompareDataError(
                    "identity_conflict",
                    f"{self.side}: row_id {s_rid} appears in both the "
                    "success and the failure stream; one source row was "
                    "delivered twice",
                )
            if s_rid < f_rid:
                kind, rec = "success", self.s_rec
                self.s_rec = self.s_reader.next()
            else:
                kind, rec = "failure", self.f_rec
                self.f_rec = self.f_reader.next()
        rid = rec["row_id"]
        if rid != self.expected:
            raise CompareDataError(
                "missing_row_id",
                f"{self.side}: source row {self.expected} was never "
                f"delivered (next delivered row_id is {rid}); the run's "
                "coverage of the snapshot is incomplete",
            )
        self.expected += 1
        return rid, kind, rec

    def check_complete(self):
        if self.expected != self.n_rows:
            raise CompareDataError(
                "missing_row_id",
                f"{self.side}: delivered coverage ends at row_id "
                f"{self.expected - 1} but the run covers {self.n_rows} "
                "input rows; trailing source rows were never delivered",
            )


# ---------------------------------------------------------------------------
# outcome / publish helpers
#


@dataclass
class CompareOutcome:
    report_path: str
    exit_code: int
    report: dict
    error: dict | None = None


def _atomic_write_json(path: str, payload: dict) -> None:
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".report-", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=True)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _remove_readonly(path: str) -> None:
    try:
        os.chmod(path, 0o600)
        os.unlink(path)
    except OSError:
        pass


def _write_last_error(config, kind: str, reason: str, message: str) -> None:
    """Best-effort structured error next to the report; never overwrites a
    valid report and is removed by the next successful comparison."""
    try:
        os.makedirs(config.report_dir, exist_ok=True)
        _atomic_write_json(
            config.error_path,
            {
                "schema_version": REPORT_SCHEMA_VERSION,
                "status": "error",
                "error": {"kind": kind, "reason": reason, "message": message},
            },
        )
    except OSError:
        pass


def _gc_changes(config, keep_name: str) -> None:
    """Remove content-addressed detail files no report references any more.

    Conservative: only ``changes-*.jsonl`` names inside the configured
    changes directory are candidates; every ``*.json`` file directly under
    the report directory is inspected for a detail reference first.
    """
    referenced = {keep_name}
    try:
        names = os.listdir(config.report_dir)
    except OSError:
        names = []
    for fname in names:
        if not fname.endswith(".json"):
            continue
        try:
            with open(
                os.path.join(config.report_dir, fname), "r", encoding="utf-8"
            ) as fh:
                doc = json.load(fh)
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(doc, dict):
            continue
        detail = doc.get("detail")
        if isinstance(detail, dict) and _is_sha256(detail.get("sha256")):
            referenced.add(f"changes-{detail['sha256']}.jsonl")
    try:
        existing = os.listdir(config.changes_dir)
    except OSError:
        return
    for name in existing:
        if not (name.startswith("changes-") and name.endswith(".jsonl")):
            continue
        if name in referenced:
            continue
        _remove_readonly(os.path.join(config.changes_dir, name))


# ---------------------------------------------------------------------------
# the comparison itself
#


def _classify(rid, b_kind, b_rec, c_kind, c_rec, changed_fields):
    """Classify one aligned pair; returns (class, detail_entry_or_None)."""
    if not json_equal(b_rec["source"], c_rec["source"]):
        raise CompareDataError(
            "source_mismatch",
            f"row_id {rid}: baseline and candidate disagree about the "
            "source record; at least one artifact does not reflect the "
            "bound snapshot",
        )
    if b_kind == "success" and c_kind == "success":
        diffs: list = []
        diff_values(b_rec["output"], c_rec["output"], "", diffs)
        if not diffs:
            return CLASS_UNCHANGED, None
        # Per-field summary counts changed *rows*: several diffs under one
        # top-level output field still count as one changed row for it.
        top_fields = {
            unescape_pointer(entry["path"].split("/", 2)[1]) for entry in diffs
        }
        for name in top_fields:
            changed_fields[name] = changed_fields.get(name, 0) + 1
        return CLASS_FIELDS_CHANGED, {
            "row_id": rid,
            "class": CLASS_FIELDS_CHANGED,
            "fields": diffs,
        }
    if b_kind == "success" and c_kind == "failure":
        return CLASS_SUCCESS_TO_FAILURE, {
            "row_id": rid,
            "class": CLASS_SUCCESS_TO_FAILURE,
            "error": normalize_error(c_rec["error"]),
        }
    if b_kind == "failure" and c_kind == "success":
        return CLASS_FAILURE_TO_SUCCESS, {
            "row_id": rid,
            "class": CLASS_FAILURE_TO_SUCCESS,
            "error": normalize_error(b_rec["error"]),
        }
    base_err = normalize_error(b_rec["error"])
    cand_err = normalize_error(c_rec["error"])
    if json_equal(base_err, cand_err):
        return CLASS_UNCHANGED, None
    return CLASS_FAILURE_REASON_CHANGED, {
        "row_id": rid,
        "class": CLASS_FAILURE_REASON_CHANGED,
        "error": {"base": base_err, "candidate": cand_err},
    }


def run_compare(config: CompareConfig, cancel_flag=None) -> CompareOutcome:
    """Full reconciliation pipeline; never raises for data/exec failures.

    Config errors propagate as :class:`CompareConfigError`; every other
    failure becomes a structured outcome with a documented exit code, the
    staged detail is removed, and any previously published report stays
    untouched.
    """
    try:
        os.makedirs(config.report_dir, exist_ok=True)
        os.makedirs(config.temp_dir, exist_ok=True)
        stage_dir = tempfile.mkdtemp(prefix=".compare-", dir=config.temp_dir)
    except OSError as exc:
        _write_last_error(config, "exec_error", "temp_error", str(exc))
        return CompareOutcome(
            report_path=config.report_path,
            exit_code=EXIT_EXEC_ERROR,
            report={},
            error={"kind": "exec_error", "reason": "temp_error",
                   "message": str(exc)},
        )

    readers = []
    sides = []
    try:
        try:
            outcome = _compare_body(config, stage_dir, readers, sides, cancel_flag)
        except CompareDataError as exc:
            _write_last_error(config, "data_error", exc.reason, str(exc))
            return CompareOutcome(
                report_path=config.report_path,
                exit_code=EXIT_DATA_ERROR,
                report={},
                error={"kind": "data_error", "reason": exc.reason,
                       "message": str(exc)},
            )
        except CompareExecError as exc:
            _write_last_error(config, "exec_error", exc.reason, str(exc))
            return CompareOutcome(
                report_path=config.report_path,
                exit_code=EXIT_EXEC_ERROR,
                report={},
                error={"kind": "exec_error", "reason": exc.reason,
                       "message": str(exc)},
            )
        # A successful publication supersedes any previous failure record.
        try:
            if os.path.exists(config.error_path):
                os.unlink(config.error_path)
        except OSError:
            pass
        return outcome
    finally:
        for reader in readers:
            reader.close()
        for side in sides:
            if side.manifest_fh is not None:
                try:
                    side.manifest_fh.close()
                except OSError:
                    pass
        shutil.rmtree(stage_dir, ignore_errors=True)


def _compare_body(config, stage_dir, readers, sides, cancel_flag):
    # 1. load and cross-check both manifests
    for label in _SIDE_NAMES:
        sides.append(_load_side(label, config.sides[label]))
    base, cand = sides
    _cross_check_sides(base, cand)
    n_rows = base.input_rows

    # 2. open the four delivered streams exactly once
    for side in sides:
        for kind in ("success", "failure"):
            path = (
                side.success_path if kind == "success" else side.failure_path
            )
            readers.append(
                _StreamReader(
                    path,
                    kind,
                    side.label,
                    n_rows,
                    config.max_line_bytes,
                    config.read_block_bytes,
                )
            )
    base_s, base_f, cand_s, cand_f = readers
    base_merge = _SideMerger(base_s, base_f, base.label, n_rows)
    cand_merge = _SideMerger(cand_s, cand_f, cand.label, n_rows)

    # 3. synchronised merge-join of the two runs, classifying each row
    counts = {cls: 0 for cls in CHANGED_CLASSES}
    unchanged = 0
    changed_fields: dict = {}
    detail_hasher = hashlib.sha256()
    detail_rows = 0
    staged_detail = os.path.join(stage_dir, "changes.jsonl")
    rows_done = 0
    with open(staged_detail, "w", encoding="utf-8") as detail_fh:
        while True:
            if cancel_flag is not None and cancel_flag():
                raise CompareExecError("cancelled", "comparison cancelled")
            b_item = base_merge.next()
            c_item = cand_merge.next()
            if b_item is None and c_item is None:
                break
            if b_item is None or c_item is None:
                # Both runs cover the same number of source rows and the two
                # mergers advance in lockstep, so the side that exhausted
                # first is necessarily incomplete: let it report exactly
                # which source row was never delivered.
                exhausted = cand_merge if c_item is None else base_merge
                exhausted.check_complete()
                raise CompareDataError(  # pragma: no cover - defensive
                    "coverage_mismatch",
                    "one run delivered fewer rows than the other; the two "
                    "runs do not cover the same source set",
                )
            b_rid, b_kind, b_rec = b_item
            c_rid, c_kind, c_rec = c_item
            if b_rid != c_rid:  # unreachable given per-side checks; defensive
                raise CompareDataError(
                    "coverage_mismatch",
                    f"row_id divergence between runs: {b_rid} vs {c_rid}",
                )
            cls, entry = _classify(
                b_rid, b_kind, b_rec, c_kind, c_rec, changed_fields
            )
            if cls == CLASS_UNCHANGED:
                unchanged += 1
            else:
                counts[cls] += 1
                line = canonical_dumps(entry) + "\n"
                detail_fh.write(line)
                detail_hasher.update(line.encode("utf-8"))
                detail_rows += 1
            rows_done += 1
        detail_fh.flush()
        os.fsync(detail_fh.fileno())

    base_merge.check_complete()
    cand_merge.check_complete()

    # 4. per-stream manifest verification (row counts, SHA-256, stability)
    for side, s_reader, f_reader in (
        (base, base_s, base_f),
        (cand, cand_s, cand_f),
    ):
        s_reader.finish(side.outputs["success"]["sha256"],
                        side.outputs["success"]["rows"])
        f_reader.finish(side.outputs["failure"]["sha256"],
                        side.outputs["failure"]["rows"])

    # 5. re-verify that manifests were not rewritten while comparing
    for side in sides:
        try:
            side.manifest_fh.seek(0)
            raw = side.manifest_fh.read()
        except OSError as exc:
            raise CompareExecError(
                "io_error", f"{side.label}: cannot re-read manifest: {exc}"
            ) from exc
        if hashlib.sha256(raw).hexdigest() != side.manifest_sha256:
            raise CompareExecError(
                "manifest_mutated",
                f"{side.label}: manifest changed during the comparison; "
                "refusing to publish",
            )

    # 5b. final pre-publish stability check of every consumed stream: an
    # in-place rewrite that landed after a stream's own EOF check is still
    # caught here before anything becomes visible.
    for reader in readers:
        reader.check_identity_stable()

    # 6. publish detail + summary as one atomic group
    detail_sha = detail_hasher.hexdigest()
    detail_name = f"changes-{detail_sha}.jsonl"
    try:
        os.makedirs(config.changes_dir, exist_ok=True)
    except OSError as exc:
        raise CompareExecError(
            "publish_error", f"cannot create changes directory: {exc}"
        ) from exc
    dest = os.path.join(config.changes_dir, detail_name)
    created = False
    try:
        if not os.path.exists(dest):
            os.chmod(staged_detail, 0o444)
            os.replace(staged_detail, dest)
            created = True
        else:
            # Identical detail already published by an earlier comparison;
            # verify its content instead of trusting the name.
            _verify_detail_file(dest, detail_sha, detail_rows)
    except OSError as exc:
        raise CompareExecError(
            "publish_error", f"cannot publish the detail file: {exc}"
        ) from exc

    report = _build_report(
        config, base, cand, n_rows, unchanged, counts,
        changed_fields, detail_name, detail_sha, detail_rows,
    )
    try:
        _atomic_write_json(config.report_path, report)
    except OSError as exc:
        if created:
            _remove_readonly(dest)
        raise CompareExecError(
            "publish_error", f"cannot publish the report: {exc}"
        ) from exc

    _gc_changes(config, keep_name=detail_name)
    return CompareOutcome(
        report_path=config.report_path,
        exit_code=EXIT_OK,
        report=report,
    )


def _verify_detail_file(path: str, expected_sha256: str, expected_rows: int) -> None:
    """Re-hash a previously published detail file (content-addressed store)."""
    hasher = hashlib.sha256()
    rows = 0
    try:
        with open(path, "rb") as fh:
            while True:
                chunk = fh.read(1 << 20)
                if not chunk:
                    break
                hasher.update(chunk)
                rows += chunk.count(b"\n")
    except OSError as exc:
        raise CompareExecError(
            "store_corrupt", f"published detail unreadable: {exc}"
        ) from exc
    if hasher.hexdigest() != expected_sha256 or rows != expected_rows:
        raise CompareExecError(
            "store_corrupt",
            "previously published detail file fails its content digest; "
            "the report store is corrupt",
        )


def _side_report(config, side: RunSide) -> dict:
    return {
        "manifest_ref": stable_relative(side.manifest_path, config.base_dir),
        "plugin": dict(side.plugin),
        "input_rows": side.input_rows,
        "snapshot_sha256": side.snapshot_sha256,
        "rule_summary": side.rule_summary,
        "outputs": {
            "success": {
                "ref": stable_relative(side.success_path, config.base_dir),
                "sha256": side.outputs["success"]["sha256"],
                "rows": side.outputs["success"]["rows"],
            },
            "failure": {
                "ref": stable_relative(side.failure_path, config.base_dir),
                "sha256": side.outputs["failure"]["sha256"],
                "rows": side.outputs["failure"]["rows"],
            },
        },
    }


def _build_report(config, base, cand, n_rows, unchanged, counts,
                  changed_fields, detail_name, detail_sha, detail_rows) -> dict:
    changed_total = sum(counts.values())
    return canonical_jsonable(
        {
            "schema_version": REPORT_SCHEMA_VERSION,
            "status": "completed",
            "input": {
                "snapshot_sha256": base.snapshot_sha256,
                "baseline": {
                    "snapshot_sha256": base.snapshot_sha256,
                    "rule_summary": base.rule_summary,
                },
                "candidate": {
                    "snapshot_sha256": cand.snapshot_sha256,
                    "rule_summary": cand.rule_summary,
                },
            },
            "runs": {
                "baseline": _side_report(config, base),
                "candidate": _side_report(config, cand),
            },
            "summary": {
                "rows_compared": n_rows,
                "unchanged": unchanged,
                "changed_rows": changed_total,
                "success_fields_changed": counts[CLASS_FIELDS_CHANGED],
                "success_to_failure": counts[CLASS_SUCCESS_TO_FAILURE],
                "failure_to_success": counts[CLASS_FAILURE_TO_SUCCESS],
                "failure_reason_changed": counts[CLASS_FAILURE_REASON_CHANGED],
                "changed_fields": {
                    name: changed_fields[name]
                    for name in sorted(changed_fields)
                },
            },
            "detail": {
                "schema_version": DETAIL_SCHEMA_VERSION,
                "ref": os.path.join("changes", detail_name),
                "sha256": detail_sha,
                "rows": detail_rows,
            },
        }
    )


# ---------------------------------------------------------------------------
# read-only status query (`colbatch compare-status`)
#


@dataclass
class QueryOutcome:
    exit_code: int
    report: dict
    error: dict | None = None


def query_status(config: CompareConfig) -> QueryOutcome:
    """Read-only inspection of the published report and its detail file.

    Exit 0 only when a complete, internally consistent report exists;
    anything missing, malformed or digest-mismatched is exit 4.
    """
    path = config.report_path
    if not os.path.exists(path):
        error = {"kind": "no_report",
                 "message": "no comparison report has been published"}
        try:
            with open(config.error_path, "r", encoding="utf-8") as fh:
                last = json.load(fh)
            if isinstance(last, dict) and isinstance(last.get("error"), dict):
                error["last_error"] = last["error"]
        except (OSError, json.JSONDecodeError):
            pass
        return QueryOutcome(exit_code=EXIT_DATA_ERROR, report={}, error=error)
    try:
        with open(path, "r", encoding="utf-8") as fh:
            report = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        return QueryOutcome(
            exit_code=EXIT_DATA_ERROR,
            report={},
            error={"kind": "report_unreadable",
                   "message": f"report is missing or malformed: {exc}"},
        )
    if not isinstance(report, dict):
        return QueryOutcome(
            exit_code=EXIT_DATA_ERROR,
            report={},
            error={"kind": "report_unreadable",
                   "message": "report is not a JSON object"},
        )
    if report.get("schema_version") != REPORT_SCHEMA_VERSION:
        return QueryOutcome(
            exit_code=EXIT_DATA_ERROR,
            report={},
            error={"kind": "report_unsupported",
                   "message": f"report schema_version "
                              f"{report.get('schema_version')!r} unsupported"},
        )
    if report.get("status") != "completed":
        return QueryOutcome(
            exit_code=EXIT_DATA_ERROR,
            report={},
            error={"kind": "report_incomplete",
                   "message": f"report status is {report.get('status')!r}"},
        )
    detail = report.get("detail")
    if not isinstance(detail, dict) or not _is_sha256(detail.get("sha256")):
        return QueryOutcome(
            exit_code=EXIT_DATA_ERROR,
            report={},
            error={"kind": "report_incomplete",
                   "message": "report lacks a verifiable detail section"},
        )
    expected = os.path.join(
        config.changes_dir, f"changes-{detail['sha256']}.jsonl"
    )
    candidate = os.path.normpath(os.path.join(config.report_dir,
                                              detail.get("ref", "")))
    if os.path.abspath(candidate) != os.path.abspath(expected):
        return QueryOutcome(
            exit_code=EXIT_DATA_ERROR,
            report={},
            error={"kind": "report_incomplete",
                   "message": "report detail ref does not resolve inside "
                              "the changes store"},
        )
    rows = detail.get("rows")
    if not _is_int(rows) or rows < 0:
        return QueryOutcome(
            exit_code=EXIT_DATA_ERROR,
            report={},
            error={"kind": "report_incomplete",
                   "message": "report detail rows count is invalid"},
        )
    try:
        _verify_detail_file(expected, detail["sha256"], rows)
    except CompareExecError as exc:
        return QueryOutcome(
            exit_code=EXIT_DATA_ERROR,
            report={},
            error={"kind": "detail_mismatch", "message": str(exc)},
        )
    return QueryOutcome(exit_code=EXIT_OK, report=report)
