"""Demo plugin 2: cross-column validation and derived fields.

Input columns : row_id:int64, qty:int64 (nullable), unit_price:utf8 (nullable),
                currency:utf8 (nullable), start_ts:utf8 (nullable),
                end_ts:utf8 (nullable)
Output columns: row_id:int64, total:utf8 (nullable), duration_s:int64 (nullable),
                is_valid:bool, invalid_reason:utf8 (nullable)

Business rules (per row):

  * qty must be present and > 0                       else "bad_qty"
  * currency must be one of USD/EUR/CNY               else "bad_currency"
  * unit_price null -> total null (valid); a non-null unit_price must be a
    plain decimal (optional leading '-', digits, optional fraction)
                                                      else "bad_price"
  * total   = qty * unit_price, canonical 2-decimal string; null when
              qty invalid or unit_price null/invalid
  * start_ts / end_ts: null -> duration_s null (valid); accepted formats
    "YYYY-MM-DD HH:MM", "YYYY-MM-DDTHH:MM:SSZ", "YYYY-MM-DD";
    unparseable                                          else "bad_ts"
  * duration_s = end - start in whole seconds when both parse;
    end before start -> duration_s null and "bad_window"
  * invalid_reason joins failing checks in this fixed order:
    bad_qty, bad_currency, bad_price, bad_ts, bad_window

No external side effects: pure in-memory transformation.
"""

from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal

import pyarrow as pa

from colbatch.sdk import Plugin

CURRENCIES = ("USD", "EUR", "CNY")
TS_FORMATS = (
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%d",
)
_PRICE = re.compile(r"-?\d+(\.\d+)?")
_CENT = Decimal("0.01")


def _parse_dt(raw):
    """Return (datetime_or_None, ok)."""
    if raw is None:
        return None, True
    text = raw.strip()
    if not text:
        return None, False
    for fmt in TS_FORMATS:
        try:
            return datetime.strptime(text, fmt), True
        except ValueError:
            continue
    return None, False


def _parse_price(raw):
    """Return (Decimal_or_None, ok)."""
    if raw is None:
        return None, True
    text = raw.strip()
    if not text or not _PRICE.fullmatch(text):
        return None, False
    return Decimal(text), True


class CrossfieldPlugin(Plugin):
    name = "crossfield_derive"
    version = "1.0"

    def input_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("row_id", pa.int64(), nullable=False),
                pa.field("qty", pa.int64()),
                pa.field("unit_price", pa.string()),
                pa.field("currency", pa.string()),
                pa.field("start_ts", pa.string()),
                pa.field("end_ts", pa.string()),
            ]
        )

    def output_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("row_id", pa.int64(), nullable=False),
                pa.field("total", pa.string()),
                pa.field("duration_s", pa.int64()),
                pa.field("is_valid", pa.bool_(), nullable=False),
                pa.field("invalid_reason", pa.string()),
            ]
        )

    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        qtys = batch.column("qty").to_pylist()
        prices = batch.column("unit_price").to_pylist()
        currencies = batch.column("currency").to_pylist()
        starts = batch.column("start_ts").to_pylist()
        ends = batch.column("end_ts").to_pylist()

        totals, durations, valids, reasons = [], [], [], []
        for qty, price_raw, currency, start_raw, end_raw in zip(
            qtys, prices, currencies, starts, ends
        ):
            errors = []

            qty_ok = qty is not None and qty > 0
            if not qty_ok:
                errors.append("bad_qty")

            if currency not in CURRENCIES:
                errors.append("bad_currency")

            price, price_ok = _parse_price(price_raw)
            if not price_ok:
                errors.append("bad_price")

            total = None
            if qty_ok and price_ok and price is not None:
                total = str((Decimal(qty) * price).quantize(_CENT))

            start, start_ok = _parse_dt(start_raw)
            end, end_ok = _parse_dt(end_raw)
            if not (start_ok and end_ok):
                errors.append("bad_ts")

            duration = None
            if start is not None and end is not None:
                delta = int((end - start).total_seconds())
                if delta < 0:
                    errors.append("bad_window")
                else:
                    duration = delta

            totals.append(total)
            durations.append(duration)
            valids.append(not errors)
            reasons.append(";".join(errors) if errors else None)

        return pa.RecordBatch.from_arrays(
            [
                batch.column("row_id"),
                pa.array(totals, type=pa.string()),
                pa.array(durations, type=pa.int64()),
                pa.array(valids, type=pa.bool_()),
                pa.array(reasons, type=pa.string()),
            ],
            schema=self.output_schema(),
        )


def create() -> CrossfieldPlugin:
    return CrossfieldPlugin()
