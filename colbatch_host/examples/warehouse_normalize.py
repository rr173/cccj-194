"""Demo plugin: ``normalize_text`` plus shipment/station passthrough.

The stock ``normalize_text`` plugin only accepts ``raw_ts``/``raw_amount``.
A delivery cleaning run has to carry the business keys too, and the host only
delivers the columns declared in the run config, so this plugin fronts the
same normalisation with ``shipment_id``/``station_id`` in its input schema.

The two business keys are validated as *non-empty strings* by the warehouse,
not by the plugin: the plugin copies them through untouched (nulls stay null)
so the warehouse can distinguish "cleaning failed" (is_valid=false) from
"cleaning succeeded but a required business value is missing".

Output columns are exactly those of ``normalize_text`` so the resulting
success/failure streams remain ordinary colbatch products.
"""

from __future__ import annotations

import importlib.util
import os

import pyarrow as pa

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_impl():
    """Load the warehouse's *real* normalisation parsers by path.

    The plugin is executed as a standalone file by the worker, so normal
    package imports of sibling modules are not available; loading by path
    keeps the demo on the exact same value semantics as ``normalize_text``.
    """
    path = os.path.join(ROOT, "colbatch", "plugins", "normalize_text_impl.py")
    spec = importlib.util.spec_from_file_location("_warehouse_norm_impl", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_impl = _load_impl()
canon_ts = _impl.canon_ts
canon_amount = _impl.canon_amount

from colbatch.sdk import Plugin  # noqa: E402


class WarehouseNormalizePlugin(Plugin):
    name = "warehouse_normalize"
    version = "1.0"

    def input_schema(self) -> pa.Schema:
        return pa.schema([
            pa.field("row_id", pa.int64(), nullable=False),
            pa.field("shipment_id", pa.string()),
            pa.field("station_id", pa.string()),
            pa.field("raw_ts", pa.string()),
            pa.field("raw_amount", pa.string()),
        ])

    def output_schema(self) -> pa.Schema:
        return pa.schema([
            pa.field("row_id", pa.int64(), nullable=False),
            pa.field("norm_ts", pa.string()),
            pa.field("norm_amount", pa.string()),
            pa.field("is_valid", pa.bool_(), nullable=False),
            pa.field("invalid_reason", pa.string()),
        ])

    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        norm_ts, norm_amount, is_valid, invalid_reason = [], [], [], []
        for ts_val, am_val in zip(batch.column("raw_ts").to_pylist(),
                                  batch.column("raw_amount").to_pylist()):
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


def create() -> WarehouseNormalizePlugin:
    return WarehouseNormalizePlugin()
