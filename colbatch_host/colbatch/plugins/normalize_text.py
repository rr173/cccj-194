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

import importlib.util
import os

import pyarrow as pa

from colbatch.sdk import Plugin


def _load_impl():
    """Load the sibling implementation file *by path*.

    The worker runs the plugin as a standalone file (``python -I worker.py
    --plugin ...``), so this module is not imported as part of a package
    there; a relative import would fail.  Loading by path works in both
    package and standalone contexts.
    """
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "normalize_text_impl.py")
    spec = importlib.util.spec_from_file_location("_colbatch_norm_impl", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_impl = _load_impl()
canon_ts = _impl.canon_ts
canon_amount = _impl.canon_amount


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
