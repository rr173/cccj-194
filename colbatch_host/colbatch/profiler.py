"""Single-pass, memory-bounded data profiler with *exact* distinct counts.

The profiler reads the immutable snapshot exactly once, sequentially, with a
fixed-size read buffer.  Per-row state is never held in Python memory:

  * exact distinct-value counting uses a temporary on-disk SQLite unique
    index (``WITHOUT ROWID`` primary key on ``(column, canonical_key)``).
    Approximate cardinality (HLL et al.) and per-batch sums are explicitly
    forbidden -- a value either exists in the index or not, which is exact;
  * only fixed-size per-column aggregates live in RAM (counters, min/max);
    their size is O(columns), independent of the row count;
  * the SQLite page cache is capped so the working set stays on disk;
  * every temp file lives in one exclusive directory that is always removed
    (success, exception or cancellation).

Bad JSON lines do not abort the scan silently: they are counted (with a
bounded sample of 1-based line numbers) and surfaced as a data error after
the full pass, so a single report run enumerates every problem found.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile

from .quality import (
    BlankPolicy,
    canonical_key,
    classify_presence,
    parse_numeric,
)

BAD_LINE_SAMPLE_LIMIT = 20
SQLITE_CACHE_PAGES = "-2000"  # ~2 MiB page cache ceiling


class ProfilerError(Exception):
    """Profiling could not complete (I/O failure or cancellation)."""


def _reject_constant(value: str):
    # Python's json module accepts non-standard NaN/Infinity tokens; JSON
    # input containing them is malformed for our contract.
    raise ValueError(f"non-standard JSON constant: {value}")


def iter_jsonl_lines(path: str, block_bytes: int):
    """Yield ``(line_no, raw_text, decode_error)`` for one physical line each.

    Single sequential pass, ``block_bytes`` buffer.  A final line without a
    trailing newline is still yielded exactly once; a trailing newline does
    not manufacture an empty extra line.
    """
    line_no = 0
    buf = bytearray()
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(block_bytes)
            if not chunk:
                break
            buf.extend(chunk)
            while True:
                nl = buf.find(b"\n")
                if nl < 0:
                    break
                line_no += 1
                raw = bytes(buf[:nl])
                del buf[: nl + 1]
                yield line_no, raw
        if buf:
            line_no += 1
            yield line_no, bytes(buf)


class _ColumnAgg:
    """Fixed-size per-column aggregate (no per-row retention)."""

    __slots__ = (
        "name",
        "field",
        "col_type",
        "missing_field",
        "nulls",
        "blanks",
        "present",
        "parse_failures",
        "num_min",
        "num_max",
        "num_count",
    )

    def __init__(self, col):
        self.name = col["name"]
        self.field = col["field"]
        self.col_type = col["type"]
        self.missing_field = 0
        self.nulls = 0
        self.blanks = 0
        self.present = 0
        self.parse_failures = 0
        self.num_min = None
        self.num_max = None
        self.num_count = 0

    def observe_numeric(self, numeric):
        self.num_count += 1
        if self.num_min is None or numeric < self.num_min:
            self.num_min = numeric
        if self.num_max is None or numeric > self.num_max:
            self.num_max = numeric


def profile_snapshot(
    snapshot_path: str,
    quality,
    cancel_flag=None,
) -> dict:
    """Profile ``snapshot_path`` under the validated :class:`QualityConfig`.

    Returns the deterministic ``profile`` section of the report.  Raises
    :class:`QualityConfigError` only for configuration problems discovered
    through data; bad input lines are collected and raised as a data error
    by the caller via the returned ``bad_lines`` summary.
    """
    blank: BlankPolicy = quality.blank
    work_dir = tempfile.mkdtemp(prefix=".profile-", dir=quality.temp_dir)
    db_path = os.path.join(work_dir, "distinct.db")
    conn = None
    pending = 0
    bad_lines: list[dict] = []
    bad_line_count = 0
    record_count = 0
    duplicate_id_count = 0
    id_present = 0

    aggs = [_ColumnAgg(c) for c in quality.columns]
    try:
        conn = sqlite3.connect(db_path)
        conn.execute(f"PRAGMA cache_size={SQLITE_CACHE_PAGES}")
        conn.execute("PRAGMA temp_store=MEMORY")
        # WITHOUT ROWID clustered primary key: exact on-disk uniqueness,
        # no separate index b-tree, bounded cache.
        conn.execute(
            "CREATE TABLE dist (col_idx INTEGER NOT NULL, "
            "key TEXT NOT NULL, PRIMARY KEY(col_idx, key)) WITHOUT ROWID"
        )
        if quality.id_field is not None:
            conn.execute(
                "CREATE TABLE idkeys (key TEXT PRIMARY KEY) WITHOUT ROWID"
            )
        conn.commit()

        def flush():
            nonlocal pending
            if pending:
                conn.commit()
                pending = 0

        for line_no, raw in iter_jsonl_lines(
            snapshot_path, quality.read_block_bytes
        ):
            if cancel_flag is not None and cancel_flag():
                raise ProfilerError("profile cancelled")
            if len(raw.strip()) == 0:
                bad_line_count += 1
                if len(bad_lines) < BAD_LINE_SAMPLE_LIMIT:
                    bad_lines.append({"line": line_no, "reason": "empty line"})
                continue
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                bad_line_count += 1
                if len(bad_lines) < BAD_LINE_SAMPLE_LIMIT:
                    bad_lines.append(
                        {"line": line_no, "reason": f"utf-8 decode error: {exc}"}
                    )
                continue
            try:
                record = json.loads(text, parse_constant=_reject_constant)
                if not isinstance(record, dict):
                    raise ValueError("JSON value is not an object")
            except (json.JSONDecodeError, ValueError) as exc:
                bad_line_count += 1
                if len(bad_lines) < BAD_LINE_SAMPLE_LIMIT:
                    bad_lines.append(
                        {"line": line_no, "reason": f"invalid JSON: {str(exc)[:160]}"}
                    )
                continue

            record_count += 1

            if quality.id_field is not None:
                id_val = record.get(quality.id_field)
                if id_val is not None and quality.id_field in record:
                    id_present += 1
                    cur = conn.execute(
                        "INSERT OR IGNORE INTO idkeys(key) VALUES (?)",
                        (canonical_key(id_val),),
                    )
                    pending += 1
                    if cur.rowcount == 0:
                        duplicate_id_count += 1

            for idx, (col, agg) in enumerate(zip(quality.columns, aggs)):
                presence = classify_presence(record, col["field"], blank)
                if presence == "missing_field":
                    agg.missing_field += 1
                    continue
                if presence == "null":
                    agg.nulls += 1
                    continue
                if presence == "blank_string":
                    agg.blanks += 1
                    continue
                agg.present += 1
                value = record[col["field"]]
                conn.execute(
                    "INSERT OR IGNORE INTO dist(col_idx, key) VALUES (?, ?)",
                    (idx, canonical_key(value)),
                )
                pending += 1
                if col["type"] in ("int64", "float64"):
                    numeric, ok = parse_numeric(value, col["type"])
                    if not ok:
                        agg.parse_failures += 1
                    else:
                        agg.observe_numeric(numeric)
                if pending >= quality.insert_batch_rows:
                    flush()
        flush()

        columns_out = []
        for idx, (col, agg) in enumerate(zip(quality.columns, aggs)):
            distinct = conn.execute(
                "SELECT COUNT(*) FROM dist WHERE col_idx = ?", (idx,)
            ).fetchone()[0]
            missing_total = agg.missing_field + agg.nulls + agg.blanks
            entry = {
                "name": agg.name,
                "field": agg.field,
                "type": agg.col_type,
                "records": record_count,
                "present": agg.present,
                "missing": {
                    "total": missing_total,
                    "missing_field": agg.missing_field,
                    "null": agg.nulls,
                    "blank_string": agg.blanks,
                    "ratio": (missing_total / record_count) if record_count else 0.0,
                },
                "distinct": distinct,
                "distinct_ratio": (distinct / record_count) if record_count else 0.0,
                "parse_failures": agg.parse_failures,
            }
            if col["type"] in ("int64", "float64"):
                entry["numeric"] = {
                    "count": agg.num_count,
                    "min": agg.num_min,
                    "max": agg.num_max,
                }
            else:
                entry["numeric"] = None
            columns_out.append(entry)

        id_section = None
        if quality.id_field is not None:
            distinct_ids = conn.execute("SELECT COUNT(*) FROM idkeys").fetchone()[0]
            id_section = {
                "field": quality.id_field,
                "present": id_present,
                "distinct": distinct_ids,
                "duplicates": duplicate_id_count,
            }

        # Exact evidence for allowed_values rules: count distinct canonical
        # keys present in the column index but outside the allowed set.
        rule_evidence = {}
        col_index = {c["name"]: i for i, c in enumerate(quality.columns)}
        for rule in quality.rules:
            if rule["rule"] != "allowed_values":
                continue
            idx = col_index[rule["column"]]
            allowed_keys = {canonical_key(v) for v in rule["values"]}
            placeholders = ",".join("?" for _ in allowed_keys)
            rows = conn.execute(
                f"SELECT key FROM dist WHERE col_idx = ? AND key NOT IN "
                f"({placeholders})",
                (idx, *sorted(allowed_keys)),
            ).fetchall()
            rule_evidence[rule["id"]] = {
                "distinct_violating": len(rows),
                "sample_keys": [r[0] for r in rows[:BAD_LINE_SAMPLE_LIMIT]],
            }

        return {
            "record_count": record_count,
            "bad_lines": {
                "count": bad_line_count,
                "sample": bad_lines,
                "sample_limit": BAD_LINE_SAMPLE_LIMIT,
            },
            "id": id_section,
            "columns": columns_out,
            "rule_evidence": rule_evidence,
            "temp_db_bytes": os.path.getsize(db_path),
        }
    except sqlite3.Error as exc:
        raise ProfilerError(f"profiler storage failure: {exc}") from exc
    finally:
        if conn is not None:
            conn.close()
        shutil.rmtree(work_dir, ignore_errors=True)
