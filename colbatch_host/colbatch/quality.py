"""Quality-gate configuration, value semantics and rule evaluation.

This module contains *pure* logic shared by the preflight profiler and the
run-time gate check:

  * configuration parsing and validation (errors are always
    :class:`QualityConfigError`, never confused with a failed quality rule);
  * the documented missing-value semantics (absent field / JSON null /
    configurable blank strings);
  * the canonical, collision-free representation of JSON values used for
    exact distinct-value counting;
  * rule definitions and their boundary semantics.

Nothing in here opens input files or keeps per-row state, so the semantics
can be unit-tested and independently re-implemented without dragging in the
snapshot / profiler machinery.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from typing import Any

# Column families understood by the profiler.  ``utf8``/``int64`` match the
# host's existing column contract; ``float64`` is additionally accepted by
# preflight so numeric ranges can be profiled on fractional data.
COLUMN_TYPES = ("utf8", "int64", "float64")

# Rule kinds and which value shape their parameters must have.
RULE_KINDS = (
    "max_missing_ratio",
    "min_distinct",
    "max_distinct",
    "min_distinct_ratio",
    "max_distinct_ratio",
    "numeric_min",
    "numeric_max",
    "allowed_values",
    "max_duplicate_ids",
)

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


class QualityConfigError(Exception):
    """The quality configuration itself is invalid.

    Distinct from :class:`QualityDataError`: a bad rule must never be
    reported as "the data failed the gate".
    """


class QualityDataError(Exception):
    """The input data cannot be profiled (e.g. unparseable JSON lines)."""


# ---------------------------------------------------------------------------
# configuration
#


class BlankPolicy:
    """Which string values count as "blank" (missing) values.

    ``mode``:
      * ``"off"``      -- no string is ever blank;
      * ``"empty"``    -- only ``""``;
      * ``"whitespace"`` -- ``""`` or any all-whitespace string (default).
    """

    __slots__ = ("mode",)

    def __init__(self, mode: str = "whitespace"):
        if mode not in ("off", "empty", "whitespace"):
            raise QualityConfigError(
                f"blank_policy.mode must be one of off|empty|whitespace, got {mode!r}"
            )
        self.mode = mode

    def is_blank(self, value: str) -> bool:
        if self.mode == "off":
            return False
        if self.mode == "empty":
            return value == ""
        return value.strip() == ""

    def to_dict(self) -> dict:
        return {"mode": self.mode}


def _is_int(value: Any) -> bool:
    # bool is a subclass of int in Python; the contract rejects booleans
    # wherever an integer is expected.
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: Any) -> bool:
    return (_is_int(value) or isinstance(value, float)) and not isinstance(value, bool)


class QualityConfig:
    """Parsed ``quality`` section of a run config.

    Attributes:
        enabled: gate on/off for ``run`` (preflight works either way);
        columns: list of ``{name, field, type}`` dicts, order preserved;
        rules: validated rule dicts with stable key ordering;
        blank: :class:`BlankPolicy`;
        id_field: optional record field whose duplicate values are counted;
        snapshot_path/report_path/temp_dir: resolved absolute paths;
        insert_batch_rows: bounded SQLite transaction batch size;
        read_block_bytes: bounded read buffer for the single-pass scan.
    """

    def __init__(self, raw: dict | None, host_columns: list[dict], base_dir: str):
        raw = raw or {}
        self.enabled = bool(raw.get("enabled", False))
        self.blank = BlankPolicy(raw.get("blank_policy", "whitespace"))
        # The quality section may declare its own columns (float64 allowed);
        # otherwise it inherits the host contract columns.
        q_columns = raw.get("columns") if raw.get("columns") else host_columns
        self.columns = []
        for c in q_columns:
            if c.get("type") not in COLUMN_TYPES:
                raise QualityConfigError(
                    f"unsupported quality column type {c.get('type')!r}"
                )
            if not c.get("name") or not c.get("field"):
                raise QualityConfigError("quality column entries need name and field")
            self.columns.append(
                {"name": c["name"], "field": c["field"], "type": c["type"]}
            )
        self._col_names = {c["name"] for c in self.columns}
        self._col_by_name = {c["name"]: c for c in self.columns}

        def resolve(p):
            if p is None:
                return None
            if not os.path.isabs(p):
                p = os.path.join(base_dir, p)
            return os.path.abspath(p)

        # Defaults live next to the config file under .colbatch_preflight/.
        default_dir = os.path.join(base_dir, ".colbatch_preflight")
        self.work_dir = resolve(raw.get("work_dir")) or default_dir
        self.report_path = resolve(raw.get("report_path")) or os.path.join(
            self.work_dir, "report.json"
        )
        # Content-addressed immutable snapshot store; a report only ever
        # references one digest-named file inside it.
        self.snapshot_store = resolve(raw.get("snapshot_store")) or os.path.join(
            self.work_dir, "snapshots"
        )
        self.temp_dir = resolve(raw.get("temp_dir")) or os.path.join(self.work_dir, "tmp")
        self.insert_batch_rows = int(raw.get("insert_batch_rows", 2000))
        self.read_block_bytes = int(raw.get("read_block_bytes", 1 << 20))
        if self.insert_batch_rows < 1:
            raise QualityConfigError("insert_batch_rows must be >= 1")
        if self.read_block_bytes < 4096:
            raise QualityConfigError("read_block_bytes must be >= 4096")

        self.id_field = raw.get("id_field")
        if self.id_field is not None and not isinstance(self.id_field, str):
            raise QualityConfigError("id_field must be a string")

        self.rules = self._validate_rules(raw.get("rules") or [])
        self.rule_summary = self._compute_rule_summary()

    def _validate_rules(self, raw_rules: list) -> list[dict]:
        if not isinstance(raw_rules, list):
            raise QualityConfigError("quality.rules must be a list")
        seen_ids: set[str] = set()
        normalized: list[dict] = []
        for idx, rr in enumerate(raw_rules):
            if not isinstance(rr, dict):
                raise QualityConfigError(f"rule #{idx} must be an object")
            kind = rr.get("rule")
            if kind not in RULE_KINDS:
                raise QualityConfigError(
                    f"rule #{idx}: unknown rule {kind!r}; expected one of {', '.join(RULE_KINDS)}"
                )
            rule_id = rr.get("id") or f"{kind}:{rr.get('column')}#{idx}"
            if not isinstance(rule_id, str) or not rule_id:
                raise QualityConfigError(f"rule #{idx}: id must be a non-empty string")
            if rule_id in seen_ids:
                raise QualityConfigError(f"duplicate rule id {rule_id!r}")
            seen_ids.add(rule_id)

            col = rr.get("column")
            if col not in self._col_names:
                raise QualityConfigError(
                    f"rule {rule_id!r} references unknown column {col!r}; "
                    f"known columns: {sorted(self._col_names)}"
                )
            col_type = self._col_by_name[col]["type"]
            rule = {"id": rule_id, "rule": kind, "column": col}

            if kind in (
                "max_missing_ratio",
                "min_distinct_ratio",
                "max_distinct_ratio",
            ):
                bound = self._require_number(rr, "value", rule_id)
                if not (0.0 <= bound <= 1.0):
                    raise QualityConfigError(
                        f"rule {rule_id!r}: value must be within [0, 1], got {bound}"
                    )
                rule["value"] = float(bound)
            elif kind in ("min_distinct", "max_distinct", "max_duplicate_ids"):
                bound = rr.get("value")
                if not _is_int(bound) or bound < 0:
                    raise QualityConfigError(
                        f"rule {rule_id!r}: value must be a non-negative integer"
                    )
                rule["value"] = int(bound)
            elif kind in ("numeric_min", "numeric_max"):
                if col_type not in ("int64", "float64"):
                    raise QualityConfigError(
                        f"rule {rule_id!r}: {kind} requires a numeric column, "
                        f"column {col!r} is {col_type}"
                    )
                bound = self._require_number(rr, "value", rule_id)
                if col_type == "int64" and not _is_int(bound):
                    raise QualityConfigError(
                        f"rule {rule_id!r}: integer column needs an integer bound, "
                        f"got {bound!r}"
                    )
                if isinstance(bound, float) and (math.isnan(bound) or math.isinf(bound)):
                    raise QualityConfigError(
                        f"rule {rule_id!r}: bound must be finite, got {bound}"
                    )
                rule["value"] = bound
            elif kind == "allowed_values":
                allowed = rr.get("values")
                if not isinstance(allowed, list) or not allowed:
                    raise QualityConfigError(
                        f"rule {rule_id!r}: values must be a non-empty list"
                    )
                for v in allowed:
                    self._check_value_for_column(rule_id, col, col_type, v)
                # de-duplicate while preserving order; the canonical form is sorted
                rule["values"] = allowed
            normalized.append(rule)
        return normalized

    @staticmethod
    def _require_number(rr, key, rule_id):
        val = rr.get(key)
        if not _is_number(val):
            raise QualityConfigError(
                f"rule {rule_id!r}: {key} must be a JSON number, got {val!r}"
            )
        return val

    def _check_value_for_column(self, rule_id, col, col_type, value):
        if value is None:
            raise QualityConfigError(
                f"rule {rule_id!r}: null is not an allowed value "
                "(missing values have their own semantics)"
            )
        if col_type == "utf8":
            if not isinstance(value, str):
                raise QualityConfigError(
                    f"rule {rule_id!r}: allowed value {value!r} is not a string"
                )
        elif col_type == "int64":
            if not _is_int(value) or not (INT64_MIN <= value <= INT64_MAX):
                raise QualityConfigError(
                    f"rule {rule_id!r}: allowed value {value!r} is not a valid int64"
                )
        elif col_type == "float64":
            if not _is_number(value) or isinstance(value, float) and (
                math.isnan(value) or math.isinf(value)
            ):
                raise QualityConfigError(
                    f"rule {rule_id!r}: allowed value {value!r} is not a finite number"
                )

    def _compute_rule_summary(self) -> str:
        """Stable fingerprint of everything that affects a gate decision.

        Built from the canonical JSON form of the validated rules plus the
        blank policy and profiled column set.  Two runs share a rule summary
        only when they would apply identical quality semantics.
        """
        payload = {
            "blank_policy": self.blank.to_dict(),
            "id_field": self.id_field,
            "columns": [
                {"name": c["name"], "field": c["field"], "type": c["type"]}
                for c in self.columns
            ],
            "rules": canonical_jsonable(self.rules),
        }
        digest = hashlib.sha256(
            canonical_dumps(payload).encode("utf-8")
        ).hexdigest()
        return f"sha256:{digest}"


# ---------------------------------------------------------------------------
# value semantics
#


def classify_presence(record: dict, field: str, blank: BlankPolicy) -> str:
    """Return one of ``present|missing_field|null|blank_string``.

    * field absent from the JSON object       -> ``missing_field``
    * field present but JSON null             -> ``null``
    * string value classified blank by policy  -> ``blank_string``
    * everything else                          -> ``present``
    """
    if field not in record:
        return "missing_field"
    value = record[field]
    if value is None:
        return "null"
    if isinstance(value, str) and blank.is_blank(value):
        return "blank_string"
    return "present"


def canonical_key(value: Any) -> str:
    """Collision-free canonical key for a present JSON value.

    A one-character type tag prefixes the encoded payload so values of
    different JSON types can never collide, even when their textual form
    matches (e.g. the string ``"1"``, the integer ``1`` and the float
    ``1.0`` get three distinct keys).  Nested JSON values use the same
    deterministic canonical JSON encoding as the rest of the report.

    Tags: ``i`` integer, ``f`` float, ``s`` string, ``b`` boolean,
          ``j`` array/object nested JSON.
    """
    if isinstance(value, bool):
        return "b:" + ("true" if value else "false")
    if isinstance(value, int):
        return "i:" + str(value)
    if isinstance(value, float):
        # Bit-exact representation: NaN payloads, +0.0/-0.0 and distinct
        # decimal encodings that compare equal stay distinguishable only
        # when their IEEE-754 bits differ (which is the correct semantic
        # for JSON numbers once parsed).
        return "f:" + _float_key(value)
    if isinstance(value, str):
        return "s:" + value
    if value is None:
        # null presence is handled via classify_presence; keep a tag anyway
        return "n:null"
    return "j:" + canonical_dumps(value)


def _float_key(value: float) -> str:
    if math.isnan(value):
        # Normalise all NaN payloads: JSON has a single NaN concept.
        return "nan"
    if value == 0.0:
        # Collapse signed zero: JSON has no signed-zero semantic.
        return "0.0"
    return repr(value)


def parse_numeric(value: Any, col_type: str):
    """Validate a present value against the configured column type.

    Returns ``(numeric_value, ok)`` where ``numeric_value`` is a Python
    int/float when the value is parseable, otherwise ``None``.  Non-numeric
    values are reported as failures (``ok=False``), never silently skipped.
    Booleans are never accepted as numbers.
    """
    if col_type == "int64":
        if not _is_int(value):
            return None, False
        if not (INT64_MIN <= value <= INT64_MAX):
            return None, False
        return value, True
    if col_type == "float64":
        if isinstance(value, bool):
            return None, False
        if _is_int(value):
            return float(value), True
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                return None, False
            return value, True
        return None, False
    # utf8 columns have no numeric parse concept.
    return None, False


# ---------------------------------------------------------------------------
# deterministic JSON encoding
#


def canonical_jsonable(obj: Any) -> Any:
    """Return a structure with dict keys sorted and stable ordering."""
    if isinstance(obj, dict):
        return {k: canonical_jsonable(obj[k]) for k in sorted(obj.keys())}
    if isinstance(obj, list):
        return [canonical_jsonable(v) for v in obj]
    return obj


def canonical_dumps(obj: Any) -> str:
    """Deterministic JSON encoding used for hashes and the report body."""
    return json.dumps(
        canonical_jsonable(obj),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
