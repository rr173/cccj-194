"""Independent reference transformer.

Recomputes the expected per-row success/invalid results directly from the
source JSONL, WITHOUT importing the plugins.  The verifier compares host
output against this reference row by row (order, multiplicity, values).

The business spec lives in README.md; this is a second, independent
implementation of it.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from decimal import Decimal, ROUND_HALF_EVEN

# ---------------------------------------------------------------------------
# normalize profile

_NORM_TS_FORMATS = [
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M:%SZ",
    "%d/%m/%Y",
    "%Y-%m-%d",
]
_NUM_RE = re.compile(r"^\d+(\.\d+)?$")


def _ref_canon_ts(raw):
    if raw is None:
        return None, None
    s = raw.strip()
    if s == "":
        return None, "bad_ts"
    dt = None
    for fmt in _NORM_TS_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            break
        except ValueError:
            pass
    if dt is None:
        return None, "bad_ts"
    return "%04d-%02d-%02dT%02d:%02d:%02dZ" % (
        dt.year,
        dt.month,
        dt.day,
        dt.hour,
        dt.minute,
        dt.second,
    ), None


def _ref_canon_amount(raw):
    if raw is None:
        return None, None
    s = raw.strip()
    if s == "":
        return None, "bad_amount"
    neg = s.startswith("(") and s.endswith(")")
    if neg:
        s = s[1:-1].strip()
    s = s.replace("€", "").replace("$", "").replace("¥", "")
    s = s.replace(",", "").strip()
    if s.startswith("-"):
        if neg:
            return None, "bad_amount"
        neg = True
        s = s[1:].strip()
    if not _NUM_RE.match(s):
        return None, "bad_amount"
    cents = (Decimal(s) * 100).to_integral_value(rounding=ROUND_HALF_EVEN)
    if neg:
        cents = -cents
    cents = int(cents)
    sign = "-" if cents < 0 else ""
    return f"{sign}{abs(cents) // 100}.{abs(cents) % 100:02d}", None


def ref_normalize(rec):
    errors = []
    norm_ts, e1 = _ref_canon_ts(rec.get("raw_ts"))
    if e1:
        errors.append(e1)
    norm_amount, e2 = _ref_canon_amount(rec.get("raw_amount"))
    if e2:
        errors.append(e2)
    return {
        "norm_ts": norm_ts,
        "norm_amount": norm_amount,
        "is_valid": not errors,
        "invalid_reason": ";".join(errors) if errors else None,
    }


# ---------------------------------------------------------------------------
# crossfield profile

_CROSS_TS_FORMATS = ["%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"]
_PRICE_RE = re.compile(r"^-?\d+(\.\d+)?$")
_VALID_CCY = {"USD", "EUR", "CNY"}


def _ref_parse_dt(raw):
    if raw is None:
        return None, True
    s = raw.strip()
    if s == "":
        return None, False
    for fmt in _CROSS_TS_FORMATS:
        try:
            return datetime.strptime(s, fmt), True
        except ValueError:
            pass
    return None, False


def ref_crossfield(rec):
    errors = []

    qty = rec.get("qty")
    qty_ok = isinstance(qty, int) and not isinstance(qty, bool) and qty > 0
    if not qty_ok:
        errors.append("bad_qty")

    if rec.get("currency") not in _VALID_CCY:
        errors.append("bad_currency")

    price_raw = rec.get("unit_price")
    price = None
    price_ok = True
    if price_raw is not None:
        s = price_raw.strip()
        if s == "" or not _PRICE_RE.match(s):
            price_ok = False
        else:
            price = Decimal(s)
    if not price_ok:
        errors.append("bad_price")

    total = None
    if qty_ok and price_ok and price is not None:
        cents = int((Decimal(qty) * price * 100).to_integral_value(rounding=ROUND_HALF_EVEN))
        sign = "-" if cents < 0 else ""
        total = f"{sign}{abs(cents) // 100}.{abs(cents) % 100:02d}"

    start, ok_s = _ref_parse_dt(rec.get("start_ts"))
    end, ok_e = _ref_parse_dt(rec.get("end_ts"))
    if not (ok_s and ok_e):
        errors.append("bad_ts")

    duration = None
    if start is not None and end is not None:
        delta = int((end - start).total_seconds())
        if delta < 0:
            errors.append("bad_window")
        else:
            duration = delta

    return {
        "total": total,
        "duration_s": duration,
        "is_valid": not errors,
        "invalid_reason": ";".join(errors) if errors else None,
    }


# ---------------------------------------------------------------------------

PROFILES = {
    "normalize": {
        "fields": {"raw_ts": "utf8", "raw_amount": "utf8"},
        "transform": ref_normalize,
    },
    "crossfield": {
        "fields": {
            "qty": "int64",
            "unit_price": "utf8",
            "currency": "utf8",
            "start_ts": "utf8",
            "end_ts": "utf8",
        },
        "transform": ref_crossfield,
    },
}


def intake_error(profile, rec):
    """Mirror of the host's per-row intake type checks."""
    for name, typ in PROFILES[profile]["fields"].items():
        val = rec.get(name)
        if val is None:
            continue
        if typ == "utf8" and not isinstance(val, str):
            return f"field {name}: expected string"
        if typ == "int64" and (not isinstance(val, int) or isinstance(val, bool)):
            return f"field {name}: expected integer"
    return None


def profile_for_plugin(plugin_path: str) -> str:
    base = plugin_path.rsplit("/", 1)[-1]
    if "normalize" in base:
        return "normalize"
    if "crossfield" in base:
        return "crossfield"
    raise ValueError(f"cannot infer profile from plugin path {plugin_path}")


def expected_rows(input_path: str, profile: str):
    """Yield (row_id, kind, payload) per source line.

    kind == "success"  -> payload is the expected output column dict
    kind == "failure"  -> payload is "intake_error" (execution failures are
                          injected by tests, not derivable from source)
    """
    transform = PROFILES[profile]["transform"]
    with open(input_path, "r", encoding="utf-8") as fh:
        for row_id, line in enumerate(fh):
            try:
                rec = json.loads(line)
                if not isinstance(rec, dict):
                    raise ValueError("not an object")
                err = intake_error(profile, rec)
                if err:
                    raise ValueError(err)
            except (json.JSONDecodeError, ValueError):
                yield row_id, "failure", "intake_error"
                continue
            yield row_id, "success", transform(rec)
