"""Demo plugin 1: normalize free-text timestamps and amounts.

Input columns : row_id:int64, raw_ts:utf8 (nullable), raw_amount:utf8 (nullable)
Output columns: row_id:int64, norm_ts:utf8 (nullable), norm_amount:utf8 (nullable),
                is_valid:bool, invalid_reason:utf8 (nullable)

Business rules (per row; a bad value is a normal data result, not an error):

  * raw_ts null            -> norm_ts null, still valid
  * raw_ts formats         -> "YYYY-MM-DD HH:MM", "YYYY-MM-DDTHH:MM:SSZ",
                              "DD/MM/YYYY", "YYYY-MM-DD"; normalized to
                              "YYYY-MM-DDTHH:MM:SSZ" (midnight if no time)
  * unparseable raw_ts     -> norm_ts null, invalid ("bad_ts")
  * raw_amount null        -> norm_amount null, still valid
  * raw_amount             -> optional currency symbol (EUR/USD/YEN sign),
                              thousands commas, "(x)" means negative,
                              optional leading '-'; canonical 2-decimal string
  * unparseable raw_amount -> norm_amount null, invalid ("bad_amount")
  * invalid_reason         -> "bad_ts", "bad_amount" or "bad_ts;bad_amount"

No external side effects: pure in-memory transformation.
"""

from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal

import pyarrow as pa

from colbatch.sdk import Plugin

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


class NormalizeTextPlugin(Plugin):
    name = "normalize_text"
    version = "1.0"

    def input_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("row_id", pa.int64(), nullable=False),
                pa.field("raw_ts", pa.string()),
                pa.field("raw_amount", pa.string()),
            ]
        )

    def output_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("row_id", pa.int64(), nullable=False),
                pa.field("norm_ts", pa.string()),
                pa.field("norm_amount", pa.string()),
                pa.field("is_valid", pa.bool_(), nullable=False),
                pa.field("invalid_reason", pa.string()),
            ]
        )

    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        raw_ts = batch.column("raw_ts").to_pylist()
        raw_amount = batch.column("raw_amount").to_pylist()
        norm_ts, norm_amount, is_valid, invalid_reason = [], [], [], []
        for ts_val, am_val in zip(raw_ts, raw_amount):
            errors = []
            nt, err = canon_ts(ts_val)
            if err:
                errors.append(err)
            na, err = canon_amount(am_val)
            if err:
                errors.append(err)
            norm_ts.append(nt)
            norm_amount.append(na)
            is_valid.append(not errors)
            invalid_reason.append(";".join(errors) if errors else None)
        return pa.RecordBatch.from_arrays(
            [
                batch.column("row_id"),
                pa.array(norm_ts, type=pa.string()),
                pa.array(norm_amount, type=pa.string()),
                pa.array(is_valid, type=pa.bool_()),
                pa.array(invalid_reason, type=pa.string()),
            ],
            schema=self.output_schema(),
        )


def create() -> NormalizeTextPlugin:
    return NormalizeTextPlugin()
