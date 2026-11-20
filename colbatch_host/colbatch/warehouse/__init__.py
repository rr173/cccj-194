"""SQLite shipment-attribution warehouse for cleaned delivery batch output.

The warehouse consumes the *products* of ``colbatch run`` (manifest plus the
success/failure JSONL streams) together with optional cancellation and
station-assignment change files, and publishes an incrementally maintained,
revisioned fact/daily-summary database.

Public entry points live in :mod:`colbatch.warehouse.api`; the command line
handlers are wired into :mod:`colbatch.cli` as ``warehouse-apply``,
``warehouse-export`` and ``warehouse-status``.
"""

from __future__ import annotations

SCHEMA_NAME = "colbatch-warehouse"
SCHEMA_VERSION = 1

__all__ = ["SCHEMA_NAME", "SCHEMA_VERSION"]
