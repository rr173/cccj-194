"""Preflight orchestration: snapshot -> profile -> rules -> atomic publish.

Public surface:

  * :func:`run_preflight` -- the implementation of ``colbatch preflight``;
  * :class:`GateDecision` / :func:`load_gate_decision` -- the verified
    admission result consumed by ``colbatch run``;
  * dedicated exception types whose names map 1:1 to documented exit codes.

Publishing contract: snapshot and report are a *group*.  The snapshot is
content-addressed (digest in its file name) and staged under an exclusive
temp directory; it is renamed into the store only after the full scan
succeeds, and the report referencing it is published with a
write-temp-then-``os.replace`` step.  Any failure before the report rename
removes both artifacts, leaving any previously published pair untouched.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import dataclass

from .profiler import profile_snapshot
from .quality import (
    QualityConfigError,
    QualityDataError,
    canonical_dumps,
    canonical_jsonable,
)
from .snapshot import SnapshotError, create_snapshot, verify_snapshot

REPORT_SCHEMA_VERSION = 1
EXIT_OK = 0
EXIT_QUALITY_FAILED = 2
EXIT_CONFIG_ERROR = 3
EXIT_DATA_ERROR = 4
EXIT_PROFILE_ERROR = 5


class PreflightError(Exception):
    """Base class for execution-time preflight failures."""


class ReportError(PreflightError):
    """A stored report is missing, malformed or out of sync with its data."""


# ---------------------------------------------------------------------------
# path helpers (no machine absolute paths in reports)
#


def stable_relative(path: str, base_dir: str) -> str:
    """Return a stable relative identifier for ``path``.

    Paths inside ``base_dir`` are rendered relative to it; anything outside
    is rendered with a fixed ``external:`` marker plus the absolute path
    stripped to its non-machine-specific tail (drive/root removed).
    """
    try:
        rel = os.path.relpath(path, base_dir)
    except ValueError:
        rel = None
    if rel is not None and not rel.startswith(".."):
        return rel
    return "external:" + path.replace("\\", "/").lstrip("/")


# ---------------------------------------------------------------------------
# rule evaluation
#


def _evaluate_rules(quality, profile: dict) -> list[dict]:
    col_stats = {c["name"]: c for c in profile["columns"]}
    results = []
    for rule in quality.rules:
        col = col_stats[rule["column"]]
        kind = rule["rule"]
        n = profile["record_count"]
        actual = None
        passed = True
        reason = ""

        if kind == "max_missing_ratio":
            actual = col["missing"]["ratio"]
            # Boundary equality passes (ratio <= bound).
            if actual > rule["value"]:
                passed, reason = False, (
                    f"missing ratio {actual:.6f} exceeds bound {rule['value']:.6f}"
                )
        elif kind == "min_distinct":
            actual = col["distinct"]
            if actual < rule["value"]:
                passed, reason = False, (
                    f"distinct count {actual} below minimum {rule['value']}"
                )
        elif kind == "max_distinct":
            actual = col["distinct"]
            if actual > rule["value"]:
                passed, reason = False, (
                    f"distinct count {actual} above maximum {rule['value']}"
                )
        elif kind == "min_distinct_ratio":
            actual = col["distinct_ratio"]
            if actual < rule["value"]:
                passed, reason = False, (
                    f"distinct ratio {actual:.6f} below minimum {rule['value']:.6f}"
                )
        elif kind == "max_distinct_ratio":
            actual = col["distinct_ratio"]
            if actual > rule["value"]:
                passed, reason = False, (
                    f"distinct ratio {actual:.6f} above maximum {rule['value']:.6f}"
                )
        elif kind == "numeric_min":
            if col["parse_failures"]:
                passed, reason, actual = False, (
                    f"{col['parse_failures']} non-numeric value(s) present; "
                    "numeric checks never skip non-numeric values"
                ), None
            elif col["numeric"]["count"] == 0:
                passed, reason, actual = False, (
                    "no parseable numeric values; numeric_min cannot be satisfied"
                ), None
            else:
                actual = col["numeric"]["min"]
                # Boundary equality passes (min >= bound).
                if actual < rule["value"]:
                    passed = False
                    reason = f"minimum {actual} below bound {rule['value']}"
        elif kind == "numeric_max":
            if col["parse_failures"]:
                passed, reason, actual = False, (
                    f"{col['parse_failures']} non-numeric value(s) present; "
                    "numeric checks never skip non-numeric values"
                ), None
            elif col["numeric"]["count"] == 0:
                passed, reason, actual = False, (
                    "no parseable numeric values; numeric_max cannot be satisfied"
                ), None
            else:
                actual = col["numeric"]["max"]
                # Boundary equality passes (max <= bound).
                if actual > rule["value"]:
                    passed = False
                    reason = f"maximum {actual} above bound {rule['value']}"
        elif kind == "allowed_values":
            evidence = profile["rule_evidence"][rule["id"]]
            actual = evidence["distinct_violating"]
            if actual > 0:
                passed = False
                reason = (
                    f"{actual} distinct value(s) outside the allowed set; "
                    f"samples: {evidence['sample_keys'][:5]}"
                )
        elif kind == "max_duplicate_ids":
            idsec = profile.get("id")
            if idsec is None:
                raise QualityConfigError(
                    f"rule {rule['id']!r} requires quality.id_field to be configured"
                )
            actual = idsec["duplicates"]
            if actual > rule["value"]:
                passed = False
                reason = f"duplicate id count {actual} exceeds maximum {rule['value']}"
        else:  # pragma: no cover - validated at config time
            raise QualityConfigError(f"unimplemented rule {kind!r}")

        results.append(
            {
                "id": rule["id"],
                "rule": kind,
                "column": rule["column"],
                "threshold": _jsonable_threshold(rule),
                "actual": actual,
                "passed": passed,
                "reason": reason,
            }
        )
    return results


def _jsonable_threshold(rule: dict):
    if "value" in rule:
        return rule["value"]
    if "values" in rule:
        return sorted(rule["values"], key=lambda v: canonical_dumps(v))
    return None


def _column_rules_map(results: list[dict]) -> dict:
    out: dict = {}
    for r in results:
        out.setdefault(r["column"], []).append(r["id"])
    return out


# ---------------------------------------------------------------------------
# preflight
#


@dataclass
class PreflightOutcome:
    report_path: str
    admitted: bool
    report: dict
    exit_code: int
    error: dict | None = None


def run_preflight(
    quality,
    source_path: str,
    base_dir: str,
    cancel_flag=None,
) -> PreflightOutcome:
    """Full preflight pipeline; never raises for data/quality failures.

    Config errors propagate as :class:`QualityConfigError`; every other
    failure is converted into a structured outcome with a documented exit
    code, and all temp/private artifacts are cleaned up.
    """
    try:
        os.makedirs(quality.work_dir, exist_ok=True)
        os.makedirs(quality.snapshot_store, exist_ok=True)
        # temp_dir creation may itself fail (permissions): treat every such
        # setup I/O error as an execution failure, never a quality rejection.
        os.makedirs(quality.temp_dir, exist_ok=True)
    except OSError as exc:
        return _fail_outcome(quality, base_dir, "temp_error", str(exc),
                             EXIT_PROFILE_ERROR)
    try:
        stage_dir = tempfile.mkdtemp(prefix=".preflight-", dir=quality.temp_dir)
    except OSError as exc:
        return _fail_outcome(quality, base_dir, "temp_error", str(exc),
                             EXIT_PROFILE_ERROR)
    staged_path = os.path.join(stage_dir, "snapshot.jsonl")
    try:
        try:
            snap = create_snapshot(
                source_path,
                staged_path,
                stage_dir,
                block_bytes=quality.read_block_bytes,
                cancel_flag=cancel_flag,
            )
        except SnapshotError as exc:
            return _fail_outcome(quality, base_dir, "snapshot_error", str(exc),
                                 EXIT_PROFILE_ERROR)

        try:
            profile = profile_snapshot(staged_path, quality, cancel_flag=cancel_flag)
        except Exception as exc:  # storage/cancel/IO -> execution failure
            return _fail_outcome(quality, base_dir, "profile_error", str(exc),
                                 EXIT_PROFILE_ERROR)

        if profile["bad_lines"]["count"]:
            # Data errors are distinct from quality failures: no report and
            # no snapshot are published for unprofileable input.
            return _fail_outcome(
                quality,
                base_dir,
                "data_error",
                f"{profile['bad_lines']['count']} unparseable line(s)",
                EXIT_DATA_ERROR,
                detail={"bad_lines": profile["bad_lines"]},
            )

        rule_results = _evaluate_rules(quality, profile)
        admitted = all(r["passed"] for r in rule_results)

        # Publish the content-addressed snapshot first, then the report.
        dest_name = f"snap-{snap.sha256}.jsonl"
        dest_path = os.path.join(quality.snapshot_store, dest_name)
        created_this_run = False
        try:
            if not os.path.exists(dest_path):
                os.replace(staged_path, dest_path)
                os.chmod(dest_path, 0o444)
                created_this_run = True
            else:
                # Identical snapshot already published by an earlier run;
                # verify its content instead of trusting the path.
                verify_snapshot(dest_path, snap.sha256, quality.read_block_bytes)
        except (OSError, SnapshotError) as exc:
            return _fail_outcome(quality, base_dir, "publish_error", str(exc),
                                 EXIT_PROFILE_ERROR)

        report = _build_report(
            quality, source_path, base_dir, profile, rule_results, admitted,
            snap.sha256, snap.size_bytes,
        )
        try:
            _atomic_write_json(quality.report_path, report)
        except OSError as exc:
            # Report failed: roll back only a snapshot *this* run created, so
            # no orphan group appears.  A pre-existing identical snapshot is
            # left untouched (it may back another report).
            if created_this_run:
                _remove_readonly(dest_path)
            return _fail_outcome(quality, base_dir, "publish_error", str(exc),
                                 EXIT_PROFILE_ERROR)

        _gc_old_snapshots(quality, keep_sha=snap.sha256)
        # A successful publication supersedes any previous execution error.
        err_path = os.path.join(quality.work_dir, "last_error.json")
        try:
            if os.path.exists(err_path):
                os.unlink(err_path)
        except OSError:
            pass
        return PreflightOutcome(
            report_path=quality.report_path,
            admitted=admitted,
            report=report,
            exit_code=EXIT_OK if admitted else EXIT_QUALITY_FAILED,
        )
    finally:
        shutil.rmtree(stage_dir, ignore_errors=True)


def _remove_readonly(path: str):
    try:
        os.chmod(path, 0o600)
        os.unlink(path)
    except OSError:
        pass


def _gc_old_snapshots(quality, keep_sha: str):
    """Remove snapshots no longer referenced by any report we own.

    Conservative by construction:

      * only digest-named files inside the configured snapshot store are
        considered for deletion; files outside it (other tasks' outputs) and
        non-``snap-*.jsonl`` names are never touched;
      * every ``*.json`` file under the work directory is inspected, so a
        snapshot still referenced by *any* report -- not just the one being
        rewritten -- is retained.  Replacing or deleting one report can
        therefore never remove another task's snapshot;
      * the just-published snapshot is always retained even if scanning the
        reports fails.
    """
    try:
        names = os.listdir(quality.snapshot_store)
    except OSError:
        return
    referenced = {keep_sha}
    for root, dirs, files in os.walk(quality.work_dir):
        # never inspect the snapshot store itself or the temp area as reports
        dirs[:] = [
            d for d in dirs
            if os.path.abspath(os.path.join(root, d))
            not in (os.path.abspath(quality.snapshot_store),
                    os.path.abspath(quality.temp_dir))
        ]
        for fname in files:
            if not fname.endswith(".json"):
                continue
            try:
                with open(os.path.join(root, fname), "r", encoding="utf-8") as fh:
                    doc = json.load(fh)
            except (OSError, json.JSONDecodeError):
                continue
            digest = (
                doc.get("input", {}).get("snapshot", {}).get("sha256")
                if isinstance(doc, dict)
                else None
            )
            if digest:
                referenced.add(digest)
    for name in names:
        if not (name.startswith("snap-") and name.endswith(".jsonl")):
            continue
        sha = name[len("snap-"):-len(".jsonl")]
        if sha in referenced:
            continue
        _remove_readonly(os.path.join(quality.snapshot_store, name))


def _build_report(quality, source_path, base_dir, profile, rule_results,
                  admitted, sha256, size_bytes) -> dict:
    col_rules = _column_rules_map(rule_results)
    columns = []
    for col in profile["columns"]:
        entry = dict(col)
        entry["rule_ids"] = col_rules.get(col["name"], [])
        columns.append(entry)
    return canonical_jsonable(
        {
            "schema_version": REPORT_SCHEMA_VERSION,
            "status": "admitted" if admitted else "rejected",
            "input": {
                "source_ref": stable_relative(source_path, base_dir),
                "snapshot": {
                    "ref": stable_relative(
                        _abs_inside_workdir(quality, sha256),
                        os.path.dirname(quality.report_path),
                    ),
                    "sha256": sha256,
                    "size_bytes": size_bytes,
                    "content_addressed": True,
                    "immutable": True,
                },
            },
            "rule_summary": quality.rule_summary,
            "blank_policy": quality.blank.to_dict(),
            "id_field": quality.id_field,
            "id": profile["id"],
            "summary": {
                "record_count": profile["record_count"],
                "bad_line_count": profile["bad_lines"]["count"],
                "column_count": len(profile["columns"]),
                "rule_count": len(rule_results),
                "rules_passed": sum(1 for r in rule_results if r["passed"]),
                "rules_failed": sum(1 for r in rule_results if not r["passed"]),
                "profiler_temp_db_bytes": profile["temp_db_bytes"],
            },
            "columns": columns,
            "rules": rule_results,
        }
    )


def _abs_inside_workdir(quality, sha256: str) -> str:
    return os.path.join(quality.snapshot_store, f"snap-{sha256}.jsonl")


def _atomic_write_json(path: str, payload: dict):
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


def _fail_outcome(quality, base_dir, kind, message, exit_code, detail=None) -> PreflightOutcome:
    err = {"kind": kind, "message": message}
    if detail:
        err["detail"] = canonical_jsonable(detail)
    # Persist the structured execution error next to the work directory so
    # callers can distinguish "profiling failed" from "quality rejected"
    # without scraping stderr.  Never overwrites a valid report.
    try:
        err_path = os.path.join(quality.work_dir, "last_error.json")
        _atomic_write_json(
            err_path,
            {"schema_version": REPORT_SCHEMA_VERSION, "status": "error", "error": err},
        )
    except OSError:
        pass
    return PreflightOutcome(
        report_path=quality.report_path,
        admitted=False,
        report={},
        exit_code=exit_code,
        error=err,
    )


# ---------------------------------------------------------------------------
# gate verification used by `colbatch run`
#


@dataclass
class GateDecision:
    admitted: bool
    reason: str
    report: dict
    snapshot_path: str
    snapshot_sha256: str


def load_gate_decision(quality) -> GateDecision:
    """Verify the stored report and return the binding gate decision.

    Every failure mode raises :class:`ReportError` with a distinct reason;
    callers must treat all of them as "do not start workers".
    """
    path = quality.report_path
    if not os.path.exists(path):
        raise ReportError(
            f"quality gate enabled but no report found at {os.path.basename(path)}; "
            "run 'colbatch preflight' first"
        )
    try:
        with open(path, "r", encoding="utf-8") as fh:
            report = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        raise ReportError(f"report unreadable or malformed: {exc}") from exc

    if report.get("schema_version") != REPORT_SCHEMA_VERSION:
        raise ReportError(
            f"report schema_version {report.get('schema_version')!r} unsupported"
        )
    if report.get("rule_summary") != quality.rule_summary:
        raise ReportError(
            "report rule_summary does not match current quality configuration "
            "(stale report; re-run preflight)"
        )
    if report.get("status") != "admitted":
        raise ReportError(
            f"report status is {report.get('status')!r}; quality gate not passed"
        )
    snap_meta = report.get("input", {}).get("snapshot") or {}
    sha = snap_meta.get("sha256")
    if not sha or not snap_meta.get("content_addressed"):
        raise ReportError("report is not bound to a content-addressed snapshot")

    # Resolve the stored relative ref against the report directory and
    # re-hash the snapshot now.  The mutable source path is never reopened.
    report_dir = os.path.dirname(os.path.abspath(path))
    ref = snap_meta.get("ref", "")
    candidate = os.path.normpath(os.path.join(report_dir, ref))
    expected = os.path.join(quality.snapshot_store, f"snap-{sha}.jsonl")
    if os.path.abspath(candidate) != os.path.abspath(expected):
        raise ReportError("report snapshot ref does not resolve inside the snapshot store")
    try:
        verify_snapshot(expected, sha, quality.read_block_bytes)
    except SnapshotError as exc:
        raise ReportError(f"bound snapshot failed verification: {exc}") from exc

    if not all(r.get("passed") is True for r in report.get("rules", [])):
        raise ReportError("report marked admitted but contains failed rules")

    return GateDecision(
        admitted=True,
        reason="admitted",
        report=report,
        snapshot_path=expected,
        snapshot_sha256=sha,
    )
