"""Shared implementation of the ``normalize_text`` value semantics.

Extracted so demo plugins (``normalize_text`` itself and
``examples/warehouse_normalize.py``) use exactly the same parsers: a delivery
warehouse demo must consume the real cleaning product, not a look-alike.
Byte-for-byte behaviour is identical to the original plugin body.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
import re

TS_FORMATS = (
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M:%SZ",
    "%d/%m/%Y",
    "%Y-%m-%d",
)

_AMOUNT_BODY = re.compile(r"\d+(\.\d+)?")
_CURRENCY_SYMBOLS = ("€", "$", "¥")
_CENT = Decimal("0.01")


def canon_ts(raw):
    """Return (normalized_or_None, error_or_None)."""
    if raw is None:
        return None, None
    text = raw.strip()
    if not text:
        return None, "bad_ts"
    for fmt in TS_FORMATS:
        try:
            dt = datetime.strptime(text, fmt)
        except ValueError:
            continue
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ"), None
    return None, "bad_ts"


def canon_amount(raw):
    """Return (normalized_or_None, error_or_None)."""
    if raw is None:
        return None, None
    text = raw.strip()
    if not text:
        return None, "bad_amount"
    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1].strip()
    for sym in _CURRENCY_SYMBOLS:
        text = text.replace(sym, "")
    text = text.replace(",", "").strip()
    if text.startswith("-"):
        if negative:
            return None, "bad_amount"
        negative = True
        text = text[1:].strip()
    if not _AMOUNT_BODY.fullmatch(text):
        return None, "bad_amount"
    value = Decimal(text)
    if negative:
        value = -value
    return str(value.quantize(_CENT)), None
