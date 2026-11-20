"""Consistent, read-only export of one published revision.

The whole export runs in a single read transaction on one connection, so a
concurrent writer publishing a newer revision cannot leak a mixed state:
SQLite's WAL snapshot pins the facts, daily summary and revision header to
the revision transaction that was open when the read began.

Three record kinds share one JSONL stream (a stable ``type`` field tells
them apart):

  * ``header``    -- exactly one, first line: selected revision, parent,
                    counts and the revision's stored fingerprints;
  * ``fact``      -- one per valid shipment, ascending shipment_id;
  * ``daily``     -- one per (utc_date, region), date then region order;
  * ``exclusion`` -- one per excluded source row for the selected revision.

The export verifies its own output against the revision fingerprint while
streaming: any corruption is reported and the caller publishes nothing.
"""

from __future__ import annotations

import json
import os
import tempfile

from .config import WarehouseConfig, WarehouseExecError
from .schema import WarehouseDB, daily_hash, fact_hash, hex_xor


class ExportOutcome:
    def __init__(self, exit_code, payload, error=None):
        self.exit_code = exit_code
        self.payload = payload
        self.error = error


FACT_COLUMNS = (
    "shipment_id, station_id, norm_ts, utc_date, amount_cents, assignment_id, "
    "region, source_run_id, source_manifest_sha256, source_success_sha256, "
    "source_stream, source_row_id, updated_revision"
)

DAILY_ORDER = (
    "ORDER BY utc_date, CASE WHEN region_key = 'u' THEN 1 ELSE 0 END, region_key"
)

# SCD2 validity: a row is part of revision R iff it was created at or before R
# and closed strictly after R.  Both clauses are served by the partial/range
# indexes on (valid_from, valid_to).
FACTS_AT_REV = (
    "WHERE valid_from<=? AND (valid_to IS NULL OR valid_to>?) "
    "ORDER BY shipment_id"
)
DAILY_AT_REV = (
    "WHERE valid_from<=? AND (valid_to IS NULL OR valid_to>?) "
)


def resolve_revision(db: WarehouseDB, revision):
    """Return the integer revision for ``latest`` / an explicit number."""
    head = db.meta_int("head_revision")
    if revision in (None, "latest"):
        selected = head
    else:
        try:
            selected = int(revision)
        except (TypeError, ValueError):
            raise WarehouseExecError(
                "bad_revision", f"revision must be 'latest' or an integer, got {revision!r}"
            )
    if selected < 1:
        raise WarehouseExecError(
            "bad_revision", "the warehouse has no published revision to export"
        )
    row = db.conn.execute(
        "SELECT * FROM revisions WHERE revision=?", (selected,)
    ).fetchone()
    if row is None:
        raise WarehouseExecError(
            "unknown_revision", f"revision {selected} does not exist"
        )
    return selected, row


class _Writer:
    def __init__(self, out_dir: str | None, stream, final_name: str | None = None):
        self.out_dir = out_dir
        self.stream = stream
        self.final_name = final_name
        self.fh = None
        self.tmp = None
        self.final = None
        if out_dir is not None:
            os.makedirs(out_dir, exist_ok=True)
            fd, self.tmp = tempfile.mkstemp(
                prefix=".export-", suffix=".jsonl", dir=out_dir)
            self.fh = os.fdopen(fd, "w", encoding="utf-8")
        self.count = {"fact": 0, "daily": 0, "exclusion": 0}

    def write(self, obj):
        line = json.dumps(obj, ensure_ascii=False, sort_keys=False)
        if self.fh is not None:
            self.fh.write(line + "\n")
        if self.stream is not None:
            self.stream.write(line + "\n")

    def commit(self):
        if self.fh is None:
            return None
        self.fh.flush()
        os.fsync(self.fh.fileno())
        self.fh.close()
        self.final = os.path.join(self.out_dir, self.final_name)
        os.replace(self.tmp, self.final)
        return self.final

    def abort(self):
        if self.fh is not None and not self.fh.closed:
            self.fh.close()
        if self.tmp and os.path.exists(self.tmp):
            os.unlink(self.tmp)


def _fact_obj(row):
    return {
        "shipment_id": row["shipment_id"],
        "station_id": row["station_id"],
        "event_time": row["norm_ts"],
        "utc_date": row["utc_date"],
        "amount_cents": row["amount_cents"],
        "assignment_id": row["assignment_id"],
        "region": row["region"],
        "source": {
            "run_id": row["source_run_id"],
            "manifest_sha256": row["source_manifest_sha256"],
            "success_sha256": row["source_success_sha256"],
            "stream": row["source_stream"],
            "row_id": row["source_row_id"],
        },
    }


def _daily_obj(row):
    return {
        "utc_date": row["utc_date"],
        "region": row["region"],
        "shipments": row["shipments"],
        "amount_cents": row["amount_cents"],
    }


def export_revision(config: WarehouseConfig, revision, fmt: str = "jsonl",
                    output_path: str | None = None, stream=None,
                    verify: bool = True) -> ExportOutcome:
    if not config.db_exists():
        raise WarehouseExecError("missing_db", f"warehouse not found: {config.db_path}")
    db = WarehouseDB(config, readonly=True)
    writer = None
    try:
        conn = db.conn
        # One transaction => one WAL snapshot for header, facts and summary.
        conn.execute("BEGIN")
        selected, rev = resolve_revision(db, revision)
        facts_xor = hex_xor(rev["facts_xor"])
        daily_xor = hex_xor(rev["daily_xor"])
        facts_seen = 0
        daily_seen = 0

        if fmt == "jsonl":
            out_dir = None
            if output_path:
                out_dir = os.path.dirname(os.path.abspath(output_path))
            name = os.path.basename(output_path) if output_path else "export.jsonl"
            writer = _Writer(out_dir, stream,
                             final_name=name if out_dir is not None else None)
            header = {
                "type": "header",
                "schema": "colbatch-warehouse-export",
                "schema_version": 1,
                "revision": selected,
                "parent_revision": rev["parent_revision"],
                "batch_id": rev["batch_id"],
                "created_at": rev["created_at"],
                "facts": rev["facts_count"],
                "daily": rev["daily_count"],
                "exclusions": rev["exclusions_count"],
                "facts_xor": rev["facts_xor"],
                "daily_xor": rev["daily_xor"],
            }
            writer.write(header)

            for row in conn.execute(
                f"SELECT {FACT_COLUMNS} FROM facts {FACTS_AT_REV}",
                (selected, selected),
            ):
                facts_xor ^= fact_hash(row)
                facts_seen += 1
                writer.write({"type": "fact", "revision": selected, **_fact_obj(row)})
            for row in conn.execute(
                "SELECT utc_date, region_key, region, shipments, amount_cents "
                f"FROM daily {DAILY_AT_REV} {DAILY_ORDER}",
                (selected, selected),
            ):
                daily_xor ^= daily_hash(row["utc_date"], row["region_key"],
                                        row["shipments"], row["amount_cents"])
                daily_seen += 1
                writer.write({"type": "daily", "revision": selected,
                              **_daily_obj(row)})
            for row in conn.execute(
                "SELECT seq, run_id, manifest_sha256, stream, row_id, shipment_id, "
                "station_id, reason, detail FROM exclusions WHERE revision=? "
                "ORDER BY stream, row_id",
                (selected,),
            ):
                writer.write({
                    "type": "exclusion",
                    "revision": selected,
                    "run_id": row["run_id"],
                    "manifest_sha256": row["manifest_sha256"],
                    "stream": row["stream"],
                    "row_id": row["row_id"],
                    "shipment_id": row["shipment_id"],
                    "station_id": row["station_id"],
                    "reason": row["reason"],
                    "detail": row["detail"],
                })
                writer.count["exclusion"] += 1
            writer.count["fact"] = facts_seen
            writer.count["daily"] = daily_seen

            ok = True
            problems = []
            if verify:
                if facts_seen != rev["facts_count"]:
                    ok = False
                    problems.append(
                        f"fact count {facts_seen} != revision {rev['facts_count']}")
                if daily_seen != rev["daily_count"]:
                    ok = False
                    problems.append(
                        f"daily count {daily_seen} != revision {rev['daily_count']}")
                if facts_xor != 0:
                    ok = False
                    problems.append("fact fingerprint mismatch")
                if daily_xor != 0:
                    ok = False
                    problems.append("daily fingerprint mismatch")
                if writer.count["exclusion"] != rev["exclusions_count"]:
                    ok = False
                    problems.append(
                        f"exclusion count {writer.count['exclusion']} != revision "
                        f"{rev['exclusions_count']}")
            if not ok:
                writer.abort()
                raise WarehouseExecError(
                    "export_inconsistent",
                    "exported data does not match the revision fingerprint: "
                    + "; ".join(problems),
                )
            final = writer.commit()
            conn.execute("COMMIT")
            return ExportOutcome(0, {
                "revision": selected,
                "parent_revision": rev["parent_revision"],
                "batch_id": rev["batch_id"],
                "facts": facts_seen,
                "daily": daily_seen,
                "exclusions": writer.count["exclusion"],
                "file": final,
                "verified": verify,
            })

        # ---- equivalent file-group interface -------------------------
        if output_path is None:
            raise WarehouseExecError(
                "bad_output", "file-group exports need an output directory")
        out_dir = os.path.abspath(output_path)
        os.makedirs(out_dir, exist_ok=True)
        staged = tempfile.mkdtemp(prefix=".export-group-", dir=out_dir)
        try:
            paths = {k: os.path.join(staged, f"{k}.jsonl")
                     for k in ("facts", "daily", "exclusions")}
            files = {k: open(p, "w", encoding="utf-8") for k, p in paths.items()}
            try:
                for row in conn.execute(
                    f"SELECT {FACT_COLUMNS} FROM facts {FACTS_AT_REV}",
                    (selected, selected),
                ):
                    facts_xor ^= fact_hash(row)
                    facts_seen += 1
                    files["facts"].write(json.dumps(
                        {"type": "fact", "revision": selected, **_fact_obj(row)},
                        ensure_ascii=False) + "\n")
                for row in conn.execute(
                    "SELECT utc_date, region_key, region, shipments, amount_cents "
                    f"FROM daily {DAILY_AT_REV} {DAILY_ORDER}",
                    (selected, selected),
                ):
                    daily_xor ^= daily_hash(row["utc_date"], row["region_key"],
                                            row["shipments"], row["amount_cents"])
                    daily_seen += 1
                    files["daily"].write(json.dumps(
                        {"type": "daily", "revision": selected, **_daily_obj(row)},
                        ensure_ascii=False) + "\n")
                n_excl = 0
                for row in conn.execute(
                    "SELECT seq, run_id, manifest_sha256, stream, row_id, "
                    "shipment_id, station_id, reason, detail FROM exclusions "
                    "WHERE revision=? ORDER BY stream, row_id", (selected,),
                ):
                    files["exclusions"].write(json.dumps({
                        "type": "exclusion", "revision": selected,
                        "run_id": row["run_id"],
                        "manifest_sha256": row["manifest_sha256"],
                        "stream": row["stream"], "row_id": row["row_id"],
                        "shipment_id": row["shipment_id"],
                        "station_id": row["station_id"],
                        "reason": row["reason"], "detail": row["detail"],
                    }, ensure_ascii=False) + "\n")
                    n_excl += 1
            finally:
                for fh in files.values():
                    fh.flush()
                    os.fsync(fh.fileno())
                    fh.close()
            if verify and (facts_seen != rev["facts_count"]
                           or daily_seen != rev["daily_count"]
                           or facts_xor != 0 or daily_xor != 0
                           or n_excl != rev["exclusions_count"]):
                raise WarehouseExecError(
                    "export_inconsistent",
                    "file-group export does not match the revision fingerprint",
                )
            header = {
                "schema": "colbatch-warehouse-export",
                "schema_version": 1,
                "revision": selected,
                "parent_revision": rev["parent_revision"],
                "batch_id": rev["batch_id"],
                "created_at": rev["created_at"],
                "facts": facts_seen,
                "daily": daily_seen,
                "exclusions": n_excl,
                "facts_xor": rev["facts_xor"],
                "daily_xor": rev["daily_xor"],
                "files": {
                    "facts": "facts.jsonl",
                    "daily": "daily.jsonl",
                    "exclusions": "exclusions.jsonl",
                },
            }
            with open(os.path.join(staged, "header.json"), "w",
                      encoding="utf-8") as fh:
                json.dump(header, fh, ensure_ascii=False, indent=1)
                fh.write("\n")
                fh.flush()
                os.fsync(fh.fileno())
            # Publish the group atomically-ish: header last, so a reader that
            # sees header.json sees every file it names.
            for key, path in paths.items():
                os.replace(path, os.path.join(out_dir, f"{key}.jsonl"))
            os.replace(os.path.join(staged, "header.json"),
                       os.path.join(out_dir, "header.json"))
            conn.execute("COMMIT")
            return ExportOutcome(0, {
                "revision": selected,
                "parent_revision": rev["parent_revision"],
                "batch_id": rev["batch_id"],
                "facts": facts_seen,
                "daily": daily_seen,
                "exclusions": n_excl,
                "file": out_dir,
                "verified": verify,
            })
        finally:
            import shutil
            shutil.rmtree(staged, ignore_errors=True)
    except BaseException:
        if writer is not None:
            writer.abort()
        raise
    finally:
        db.close()
