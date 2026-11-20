"""Warehouse schema, connection pragmas and incremental state fingerprints.

Facts and daily aggregates are the *only* materialised state.  Each applied
revision carries an incrementally maintained XOR fingerprint of every fact row
and every daily row, so an export can prove that the bytes it streamed belong
to the revision named in its header without re-scanning another table.

All facts and summary rows are touched through primary keys or the
``(station_id, ts_epoch)`` index; nothing here scans the fact table.
"""

from __future__ import annotations

import hashlib
import sqlite3

from . import SCHEMA_NAME, SCHEMA_VERSION
from .config import WarehouseConfig, WarehouseConfigError, canonical_json

DDL = [
    """CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS revisions (
        revision INTEGER PRIMARY KEY,
        parent_revision INTEGER NOT NULL,
        batch_id TEXT NOT NULL UNIQUE,
        created_at TEXT NOT NULL,
        metrics_json TEXT NOT NULL,
        facts_count INTEGER NOT NULL,
        facts_xor TEXT NOT NULL,
        daily_count INTEGER NOT NULL,
        daily_xor TEXT NOT NULL,
        exclusions_count INTEGER NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS batches (
        batch_id TEXT PRIMARY KEY,
        expected_parent_version INTEGER NOT NULL,
        input_fingerprint TEXT NOT NULL,
        business_fingerprint TEXT NOT NULL,
        revision INTEGER NOT NULL REFERENCES revisions(revision),
        applied_at TEXT NOT NULL,
        receipt_json TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS runs (
        run_id TEXT PRIMARY KEY,
        manifest_sha256 TEXT NOT NULL,
        plugin_name TEXT,
        plugin_version TEXT,
        input_rows_read INTEGER NOT NULL,
        success_sha256 TEXT NOT NULL,
        success_rows INTEGER NOT NULL,
        failure_sha256 TEXT NOT NULL,
        failure_rows INTEGER NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS facts (
        shipment_id TEXT NOT NULL,
        station_id TEXT NOT NULL,
        norm_ts TEXT NOT NULL,
        ts_epoch INTEGER NOT NULL,
        utc_date TEXT NOT NULL,
        amount_cents INTEGER NOT NULL,
        assignment_id TEXT,
        region TEXT,
        source_run_id TEXT NOT NULL,
        source_manifest_sha256 TEXT NOT NULL,
        source_success_sha256 TEXT NOT NULL,
        source_stream TEXT NOT NULL,
        source_row_id INTEGER NOT NULL,
        updated_revision INTEGER NOT NULL,
        valid_from INTEGER NOT NULL,
        valid_to INTEGER,
        PRIMARY KEY (shipment_id, valid_from)
    )""",
    "CREATE INDEX IF NOT EXISTS facts_current_station_ts "
    "ON facts(station_id, ts_epoch) WHERE valid_to IS NULL",
    "CREATE INDEX IF NOT EXISTS facts_valid_span "
    "ON facts(valid_from, valid_to)",
    """CREATE TABLE IF NOT EXISTS daily (
        utc_date TEXT NOT NULL,
        region_key TEXT NOT NULL,
        region TEXT,
        shipments INTEGER NOT NULL,
        amount_cents INTEGER NOT NULL,
        valid_from INTEGER NOT NULL,
        valid_to INTEGER,
        PRIMARY KEY (utc_date, region_key, valid_from)
    )""",
    "CREATE INDEX IF NOT EXISTS daily_valid_span "
    "ON daily(valid_from, valid_to)",
    """CREATE TABLE IF NOT EXISTS intervals (
        station_id TEXT NOT NULL,
        assignment_id TEXT NOT NULL,
        from_ts TEXT NOT NULL,
        from_epoch INTEGER NOT NULL,
        to_ts TEXT,
        to_epoch INTEGER,
        region TEXT NOT NULL,
        updated_revision INTEGER NOT NULL,
        PRIMARY KEY (station_id, assignment_id)
    )""",
    "CREATE INDEX IF NOT EXISTS intervals_station_from "
    "ON intervals(station_id, from_epoch)",
    """CREATE TABLE IF NOT EXISTS exclusions (
        revision INTEGER NOT NULL,
        seq INTEGER NOT NULL,
        run_id TEXT NOT NULL,
        manifest_sha256 TEXT NOT NULL,
        stream TEXT NOT NULL,
        row_id INTEGER NOT NULL,
        shipment_id TEXT,
        station_id TEXT,
        reason TEXT NOT NULL,
        detail TEXT,
        PRIMARY KEY (revision, stream, row_id)
    )""",
    "CREATE INDEX IF NOT EXISTS exclusions_revision ON exclusions(revision, seq)",
]

META_DEFAULTS = {
    "schema_name": SCHEMA_NAME,
    "schema_version": str(SCHEMA_VERSION),
    "head_revision": "0",
    "facts_count": "0",
    "facts_xor": "0" * 16,
    "daily_count": "0",
    "daily_xor": "0" * 16,
}


def _db(directory: str, name: str) -> str:
    return directory.rstrip("/") + "/" + name


class WarehouseDB:
    """A single process-wide connection with manual transaction control."""

    def __init__(self, config: WarehouseConfig, readonly: bool = False):
        self.config = config
        self.readonly = readonly
        # Observable read/write range: when enabled every SQL statement is
        # counted by category, and distinct query plans are kept for the
        # receipt.  This is how locality claims are audited, not asserted.
        self.audit = config.limits.get("access_audit", False)
        self.audit_counts = {"select": 0, "insert": 0, "update": 0,
                             "delete": 0, "other": 0}
        self.audit_tables = {}
        self.conn = sqlite3.connect(
            config.db_path,
            timeout=config.limits["busy_timeout_ms"] / 1000.0,
            isolation_level=None,
        )
        self.conn.row_factory = sqlite3.Row
        self._meta = None
        self._apply_pragmas()
        if self.audit:
            self.conn.set_trace_callback(self._trace)
        if readonly:
            self.conn.execute("PRAGMA query_only=ON")
        else:
            self.ensure_schema()

    def _trace(self, statement: str):
        head = statement.lstrip().split(None, 1)[0].upper()
        key = {"SELECT": "select", "INSERT": "insert", "UPDATE": "update",
               "DELETE": "delete"}.get(head, "other")
        self.audit_counts[key] += 1
        for table in ("facts", "daily", "intervals", "exclusions", "batches",
                      "revisions", "runs", "meta"):
            if table in statement:
                self.audit_tables[table] = self.audit_tables.get(table, 0) + 1

    def audit_summary(self):
        return {"statements": dict(self.audit_counts),
                "statements_per_table": dict(self.audit_tables)}

    # -- pragmas ---------------------------------------------------------
    def _apply_pragmas(self):
        c = self.conn
        c.execute(f"PRAGMA busy_timeout={self.config.limits['busy_timeout_ms']}")
        # Bounded page cache: the working set never depends on database size.
        c.execute(f"PRAGMA cache_size=-{self.config.limits['sqlite_cache_kib']}")
        c.execute("PRAGMA temp_store=FILE")
        if not self.readonly:
            c.execute("PRAGMA journal_mode=WAL")
            # FULL: a committed receipt survives an OS crash, not just a
            # process crash -- required for "abort either side of commit"
            # recovery to be verifiable.
            c.execute("PRAGMA synchronous=FULL")
        # Staging and delta tables live in the workspace directory, never in
        # process memory; SQLite spills temp b-trees to disk too.

    # -- schema ----------------------------------------------------------
    def ensure_schema(self):
        version = self.conn.execute("PRAGMA user_version").fetchone()[0]
        if version == 0:
            with self.transaction():
                for stmt in DDL:
                    self.conn.execute(stmt)
                self.conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                for key, value in META_DEFAULTS.items():
                    self.conn.execute(
                        "INSERT OR IGNORE INTO meta(key, value) VALUES(?, ?)",
                        (key, value),
                    )
        elif version != SCHEMA_VERSION:
            raise WarehouseConfigError(
                f"unsupported warehouse schema version {version}; expected {SCHEMA_VERSION}"
            )
        else:
            got = self.meta("schema_name")
            if got != SCHEMA_NAME:
                raise WarehouseConfigError(
                    f"{self.config.db_path} is a SQLite database, not a warehouse"
                )

    def close(self):
        try:
            self.conn.close()
        except sqlite3.Error:
            pass

    # -- transactions ----------------------------------------------------
    def transaction(self, immediate: bool = True):
        db = self

        class _Txn:
            def __enter__(self):
                db.conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
                return db.conn

            def __exit__(self, exc_type, exc, tb):
                if exc_type is None:
                    db.conn.execute("COMMIT")
                else:
                    try:
                        db.conn.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                return False

        return _Txn()

    # -- meta ------------------------------------------------------------
    def _load_meta(self):
        self._meta = {
            row["key"]: row["value"]
            for row in self.conn.execute("SELECT key, value FROM meta")
        }

    def meta(self, key: str) -> str:
        if self._meta is None:
            self._load_meta()
        return self._meta.get(key)

    def meta_int(self, key: str) -> int:
        return int(self.meta(key))

    def set_meta(self, key: str, value: str):
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )
        if self._meta is not None:
            self._meta[key] = str(value)


def fact_hash(row) -> int:
    """64-bit fingerprint of one fact's business content and provenance.

    Only fields that define *what the fact says* take part, so a fact carried
    unchanged from revision N-1 to N keeps its hash and rolls into the new
    revision fingerprint by XOR without touching the row.  Field order is
    fixed here so the product and the reference tool agree without sharing
    a row object.
    """
    payload = canonical_json(
        [
            row["shipment_id"],
            row["station_id"],
            row["norm_ts"],
            row["amount_cents"],
            row["assignment_id"],
            row["region"],
            row["source_run_id"],
            row["source_manifest_sha256"],
            row["source_success_sha256"],
            row["source_stream"],
            row["source_row_id"],
        ]
    )
    return int.from_bytes(
        hashlib.blake2b(payload.encode("utf-8"), digest_size=8).digest(), "little"
    )


def daily_hash(utc_date: str, region_key: str, shipments: int, amount: int) -> int:
    payload = canonical_json([utc_date, region_key, shipments, amount])
    return int.from_bytes(
        hashlib.blake2b(payload.encode("utf-8"), digest_size=8).digest(), "little"
    )


def xor_hex(value: int) -> str:
    return format(value & ((1 << 64) - 1), "016x")


def hex_xor(value: str) -> int:
    return int(value, 16)


def attach_stage(conn: sqlite3.Connection, path) -> None:
    conn.execute("ATTACH DATABASE ? AS stage", (str(path),))


def detach_stage(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("DETACH DATABASE stage")
    except sqlite3.Error:
        pass


class stage_attached:
    """``with stage_attached(conn, path): ...`` -- attach for one read block."""

    def __init__(self, conn, path):
        self.conn = conn
        self.path = path

    def __enter__(self):
        attach_stage(self.conn, self.path)
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        detach_stage(self.conn)
        return False


__all__ = [
    "DDL",
    "WarehouseDB",
    "attach_stage",
    "daily_hash",
    "detach_stage",
    "fact_hash",
    "hex_xor",
    "xor_hex",
]
