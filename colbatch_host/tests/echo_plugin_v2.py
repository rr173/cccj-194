"""Test-only variant of the echo plugin simulating a cleaning-logic upgrade.

Same input schema as ``echo_plugin.py``; the "v2" cleaning logic uppercases
the ``cat`` value of every tenth row (kid % 10 == 0) and bumps the plugin
identity.  Used by tests/test_compare.py to produce a real candidate run
whose delivered results differ from the baseline in a fully deterministic
way.  Not shipped as a demo plugin.
"""

import pyarrow as pa


class EchoPluginV2:
    name = "preflight_echo"
    version = "2.0"

    def input_schema(self):
        return pa.schema([
            pa.field("row_id", pa.int64(), nullable=False),
            pa.field("kid", pa.int64()),
            pa.field("cat", pa.utf8()),
            pa.field("price", pa.utf8()),
            pa.field("note", pa.utf8()),
            pa.field("nested", pa.utf8()),
        ])

    def output_schema(self):
        return self.input_schema().append(pa.field("echoed", pa.bool_()))

    def transform(self, batch):
        kids = batch.column("kid").to_pylist()
        cats = batch.column("cat").to_pylist()
        cats = [
            c.upper() if (c is not None and k is not None and k % 10 == 0) else c
            for k, c in zip(kids, cats)
        ]
        n = batch.num_rows
        arrays = [
            batch.column("row_id"),
            batch.column("kid"),
            pa.array(cats, type=pa.utf8()),
            batch.column("price"),
            batch.column("note"),
            batch.column("nested"),
            pa.array([True] * n, type=pa.bool_()),
        ]
        return pa.RecordBatch.from_arrays(arrays, schema=self.output_schema())


def create():
    return EchoPluginV2()
