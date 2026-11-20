"""Warehouse configuration, strict validation and content fingerprints.

Every path in a warehouse config is resolved relative to the *config file's*
directory (never the process cwd).  Decisions that affect stored facts are
made from content digests and business settings only -- absolute paths are
recorded for audit but never enter a retry fingerprint, so moving the input
files does not change a batch's identity.
"""

from __future__ import annotations

import hashlib
import json
import os

from . import SCHEMA_NAME, SCHEMA_VERSION

DEFAULT_FIELDS = {"shipment_id": "shipment_id", "station_id": "station_id"}
DEFAULT_LIMITS = {
    "max_line_bytes": 8 * 1024 * 1024,
    "max_manifest_bytes": 4 * 1024 * 1024,
    "read_block_bytes": 1024 * 1024,
    "sqlite_cache_kib": 16384,
    "busy_timeout_ms": 30000,
    "access_audit": False,
}
_SQLITE_MAGIC = b"SQLite format 3\x00"


class WarehouseConfigError(Exception):
    """The config itself is invalid (exit code 3)."""


class WarehouseDataError(Exception):
    """A batch is structurally invalid and is rejected as a whole (exit 4)."""

    def __init__(self, reason: str, message: str):
        super().__init__(message)
        self.reason = reason
        self.message = message


class WarehouseConflictError(Exception):
    """Batch identity / expected-parent conflict (exit 6)."""

    def __init__(self, reason: str, message: str):
        super().__init__(message)
        self.reason = reason
        self.message = message


class WarehouseExecError(Exception):
    """I/O, cancellation or other execution failure (exit 5)."""

    def __init__(self, reason: str, message: str):
        super().__init__(message)
        self.reason = reason
        self.message = message


def _is_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def canonical_json(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def stable_relative(path: str, base_dir: str) -> str:
    """A machine-independent identifier for audit records."""
    try:
        rel = os.path.relpath(path, base_dir)
    except ValueError:
        rel = None
    if rel is not None and not rel.startswith(".."):
        return rel.replace(os.sep, "/")
    return "external:" + os.path.abspath(path).replace(os.sep, "/").lstrip("/")


class Ink:
    """Bounded line reader with a hard per-line ceiling.

    The whole line is assembled in one buffer; a line above ``max_line_bytes``
    aborts the batch instead of being silently truncated.  ``digest`` covers
    exactly the bytes handed to the caller (including newlines), which is what
    the manifest verification uses.
    """

    __slots__ = ("path", "block", "max_line", "sha", "nbytes", "nlines")

    def __init__(self, path: str, max_line_bytes: int, block_bytes: int):
        if max_line_bytes < 1 or block_bytes < 4096:
            raise WarehouseConfigError("line/block limits are too small")
        self.path = path
        self.block = block_bytes
        self.max_line = max_line_bytes
        self.sha = hashlib.sha256()
        self.nbytes = 0
        self.nlines = 0

    def __iter__(self):
        buf = bytearray()
        with open(self.path, "rb") as fh:
            while True:
                chunk = fh.read(self.block)
                if not chunk:
                    break
                self.sha.update(chunk)
                self.nbytes += len(chunk)
                buf.extend(chunk)
                while True:
                    nl = buf.find(b"\n")
                    if nl < 0:
                        break
                    raw = bytes(buf[: nl + 1])
                    del buf[: nl + 1]
                    if len(raw) > self.max_line:
                        raise WarehouseDataError(
                            "line_too_long",
                            f"{self.path}: line {self.nlines + 1} exceeds "
                            f"max_line_bytes={self.max_line}",
                        )
                    self.nlines += 1
                    yield self.nlines, raw
                if len(buf) > self.max_line:
                    raise WarehouseDataError(
                        "line_too_long",
                        f"{self.path}: line {self.nlines + 1} exceeds "
                        f"max_line_bytes={self.max_line}",
                    )
        if buf:
            if len(buf) > self.max_line:
                raise WarehouseDataError(
                    "line_too_long",
                    f"{self.path}: final line exceeds max_line_bytes={self.max_line}",
                )
            self.nlines += 1
            yield self.nlines, bytes(buf)

    def digest(self) -> str:
        return self.sha.hexdigest()


class WarehouseConfig:
    def __init__(self, raw: dict, base_dir: str):
        if not isinstance(raw, dict):
            raise WarehouseConfigError("config must be a JSON object")
        self.base_dir = os.path.abspath(base_dir)

        def path_of(value, what):
            if not isinstance(value, str) or not value:
                raise WarehouseConfigError(f"{what} must be a non-empty path")
            if not os.path.isabs(value):
                value = os.path.join(self.base_dir, value)
            return os.path.abspath(value)

        self.db_path = path_of(raw.get("db"), "db")
        self.batch_id = raw.get("batch_id")
        self.expected_parent = raw.get("expected_parent_version")
        self.apply = raw.get("_mode", "apply") == "apply"
        if self.apply:
            if not isinstance(self.batch_id, str) or not self.batch_id.strip():
                raise WarehouseConfigError("batch_id must be a non-empty string")
            parent = self.expected_parent
            if not _is_int(parent) or parent < 0:
                raise WarehouseConfigError(
                    "expected_parent_version must be a non-negative integer"
                )

        fields = raw.get("fields") or DEFAULT_FIELDS
        if not isinstance(fields, dict):
            raise WarehouseConfigError("fields must be an object")
        for key in ("shipment_id", "station_id"):
            val = fields.get(key)
            if not isinstance(val, str) or not val:
                raise WarehouseConfigError(f"fields.{key} must be a non-empty name")
        self.shipment_field = fields["shipment_id"]
        self.station_field = fields["station_id"]

        run = raw.get("cleaning_run")
        self.cleaning_manifest = None
        self.cleaning_success = None
        self.cleaning_failure = None
        if run is not None:
            if not isinstance(run, dict):
                raise WarehouseConfigError("cleaning_run must be an object")
            if not run.get("manifest"):
                raise WarehouseConfigError("cleaning_run.manifest is required")
            self.cleaning_manifest = path_of(
                run["manifest"], "cleaning_run.manifest")
            # Streams may be omitted; they are then resolved from the
            # manifest's own `outputs` section (relative to the manifest).
            if run.get("success"):
                self.cleaning_success = path_of(
                    run["success"], "cleaning_run.success")
            if run.get("failure"):
                self.cleaning_failure = path_of(
                    run["failure"], "cleaning_run.failure")

        self.cancellations = (
            path_of(raw["cancellations"], "cancellations")
            if raw.get("cancellations") else None
        )
        self.assignments = (
            path_of(raw["assignments"], "assignments")
            if raw.get("assignments") else None
        )
        if (self.apply and run is None and self.cancellations is None
                and self.assignments is None):
            raise WarehouseConfigError(
                "batch needs at least one of cleaning_run, cancellations, assignments"
            )

        self.workspace_dir = (
            path_of(raw["workspace_dir"], "workspace_dir")
            if raw.get("workspace_dir")
            else self.db_path + ".work"
        )
        self.receipt_path = (
            path_of(raw["receipt"], "receipt") if raw.get("receipt") else None
        )

        limits = raw.get("limits") or {}
        if not isinstance(limits, dict):
            raise WarehouseConfigError("limits must be an object")
        merged = dict(DEFAULT_LIMITS)
        for key, default in DEFAULT_LIMITS.items():
            if key not in limits:
                continue
            val = limits[key]
            if key == "access_audit":
                if not isinstance(val, bool):
                    raise WarehouseConfigError("limits.access_audit must be boolean")
                merged[key] = val
            else:
                if not _is_int(val) or val < 1:
                    raise WarehouseConfigError(f"limits.{key} must be a positive integer")
                merged[key] = val
        for key in ("max_line_bytes", "max_manifest_bytes", "read_block_bytes",
                    "sqlite_cache_kib", "busy_timeout_ms"):
            if merged[key] < 1:
                raise WarehouseConfigError(f"limits.{key} must be >= 1")
        if merged["read_block_bytes"] < 4096:
            raise WarehouseConfigError("limits.read_block_bytes must be >= 4096")
        if merged["max_line_bytes"] < merged["read_block_bytes"]:
            # Not a hard requirement, but a line can never be larger than a
            # read block unless it spans blocks; keep the relationship sane.
            pass
        self.limits = merged

        self.business_fingerprint = sha256_text(canonical_json({
            "schema": SCHEMA_NAME,
            "schema_version": SCHEMA_VERSION,
            "shipment_field": self.shipment_field,
            "station_field": self.station_field,
        }))

    def describe_bounds(self) -> dict:
        return {
            "max_line_bytes": self.limits["max_line_bytes"],
            "max_manifest_bytes": self.limits["max_manifest_bytes"],
            "read_block_bytes": self.limits["read_block_bytes"],
            "sqlite_cache_kib": self.limits["sqlite_cache_kib"],
            "busy_timeout_ms": self.limits["busy_timeout_ms"],
            "access_audit": self.limits["access_audit"],
            "workspace_dir": self.workspace_dir,
        }

    def db_exists(self) -> bool:
        return os.path.exists(self.db_path) and os.path.getsize(self.db_path) > 0

    def check_db_identity(self) -> None:
        """Refuse to treat a non-warehouse file as our database."""
        if not os.path.exists(self.db_path):
            return
        with open(self.db_path, "rb") as fh:
            head = fh.read(16)
        if head != _SQLITE_MAGIC:
            raise WarehouseConfigError(
                f"{self.db_path} exists but is not a SQLite database"
            )
