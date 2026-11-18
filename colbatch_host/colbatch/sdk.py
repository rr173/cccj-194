"""Plugin SDK: the contract a cleaning plugin must implement.

A plugin is a single Python file providing either a ``create()`` function
or a module-level ``PLUGIN`` object implementing:

    name            str, plugin identifier
    version         str, free-form
    input_schema()  -> pyarrow.Schema, first field MUST be row_id:int64
    output_schema() -> pyarrow.Schema, first field MUST be row_id:int64
    transform(batch: pyarrow.RecordBatch) -> pyarrow.RecordBatch

Contract (enforced on both sides of the pipe):

  * the input batch has exactly ``input_schema()`` columns;
  * the output batch MUST have exactly ``output_schema()`` columns
    (names, order and types);
  * the output row count MUST equal the input row count;
  * the output row_id column MUST equal the input row_id column, value by
    value, in order -- reordering, dropping, duplicating or inventing
    row_ids is a contract violation and the host rejects the whole batch;

Per-row business outcomes ("this timestamp is unparseable") are DATA: the
plugin reports them through its own validity columns (e.g. is_valid /
invalid_reason) and the batch as a whole is a success.  Raising inside
transform() is an execution failure of the whole batch (the worker reports
BATCH_ERROR); crashing the process, hanging or breaking the wire protocol
are execution failures detected by the host itself.
"""

from __future__ import annotations

import importlib.util
import os

import pyarrow as pa


class ContractError(Exception):
    """A plugin result violates the batch contract."""


class Plugin:
    name = "unnamed"
    version = "0"

    def input_schema(self) -> pa.Schema:  # pragma: no cover - interface
        raise NotImplementedError

    def output_schema(self) -> pa.Schema:  # pragma: no cover - interface
        raise NotImplementedError

    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:  # pragma: no cover
        raise NotImplementedError


def _check_row_id_field(schema: pa.Schema, which: str) -> None:
    if len(schema) == 0 or schema[0].name != "row_id" or schema[0].type != pa.int64():
        raise ContractError(f"{which} schema must start with row_id:int64, got {schema}")


def load_plugin(path: str) -> Plugin:
    """Load a plugin module from an absolute path and return its instance."""
    path = os.path.abspath(path)
    spec = importlib.util.spec_from_file_location("colbatch_user_plugin", path)
    if spec is None or spec.loader is None:
        raise ContractError(f"cannot load plugin from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    plugin = module.create() if hasattr(module, "create") else getattr(module, "PLUGIN", None)
    if plugin is None:
        raise ContractError(f"plugin {path} defines neither create() nor PLUGIN")
    in_s = plugin.input_schema()
    out_s = plugin.output_schema()
    _check_row_id_field(in_s, "input")
    _check_row_id_field(out_s, "output")
    return plugin


def validate_result(row_ids, result, out_schema: pa.Schema) -> None:
    """Validate a plugin result against the contract.

    ``row_ids`` is the exact list of row_ids sent in the input batch.
    Raises ContractError on any violation; never trusts positions alone.
    """
    if not isinstance(result, pa.RecordBatch):
        raise ContractError(f"result is not a RecordBatch: {type(result).__name__}")
    if result.num_rows != len(row_ids):
        raise ContractError(f"row_count_mismatch: got {result.num_rows}, want {len(row_ids)}")
    if not result.schema.equals(out_schema):
        raise ContractError(
            f"schema_mismatch: got {result.schema}, want {out_schema}"
        )
    got = result.column(0).to_pylist()
    if got != list(row_ids):
        raise ContractError("row_id_mismatch: row_id column altered, reordered or dropped")


def schema_to_bytes(schema: pa.Schema) -> bytes:
    """Serialize a schema as an (empty) Arrow IPC stream."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema):
        pass
    return sink.getvalue().to_pybytes()


def schema_from_bytes(data: bytes) -> pa.Schema:
    with pa.ipc.open_stream(pa.BufferReader(data)) as reader:
        return reader.schema


def batch_to_bytes(batch: pa.RecordBatch) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def bytes_to_batch(data: bytes) -> pa.RecordBatch:
    with pa.ipc.open_stream(pa.BufferReader(data)) as reader:
        return reader.read_next_batch()
