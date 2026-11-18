"""Test-only echo plugin for preflight gate scenarios.

It accepts the full 5-column schema used by tests/test_preflight.py and
returns every input value unchanged plus a constant flag column.  This keeps
the gate tests independent of the business plugins' narrower schemas.  Not
shipped as a demo plugin.
"""

import pyarrow as pa


class EchoPlugin:
    name = "preflight_echo"
    version = "1.0"

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
        arrays = [batch.column(i) for i in range(batch.num_columns)]
        n = batch.num_rows
        arrays.append(pa.array([True] * n, type=pa.bool_()))
        return pa.RecordBatch.from_arrays(arrays, schema=self.output_schema())


def create():
    return EchoPlugin()
