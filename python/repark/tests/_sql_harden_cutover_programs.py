"""SQL-HARDEN-1 inventory - the cutover pipeline cutover shapes S1-S7.

pins: sql-harden-1-cutover-shapes/C-001
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import NamedTuple

import pyarrow as pa
import pyarrow.parquet as pq

_CATALOG = "sqlh1"
_NAMESPACE = "cut"

_TARGET_FILE_SIZE = "268435456"

_MOR_V2 = (
    "'format-version' = 2, "
    "'write.delete.mode' = 'merge-on-read', "
    "'write.update.mode' = 'merge-on-read', "
    "'write.merge.mode' = 'merge-on-read', "
    f"'write.target-file-size-bytes' = '{_TARGET_FILE_SIZE}'"
)
_MOR_V3 = (
    "'format-version' = 3, "
    "'write.delete.mode' = 'merge-on-read', "
    "'write.update.mode' = 'merge-on-read', "
    "'write.merge.mode' = 'merge-on-read', "
    f"'write.target-file-size-bytes' = '{_TARGET_FILE_SIZE}'"
)
_COW_V2 = (
    "'format-version' = 2, "
    "'write.delete.mode' = 'copy-on-write', "
    "'write.update.mode' = 'copy-on-write', "
    "'write.merge.mode' = 'copy-on-write', "
    f"'write.target-file-size-bytes' = '{_TARGET_FILE_SIZE}'"
)
_COW_V3 = (
    "'format-version' = 3, "
    "'write.delete.mode' = 'copy-on-write', "
    "'write.update.mode' = 'copy-on-write', "
    "'write.merge.mode' = 'copy-on-write', "
    f"'write.target-file-size-bytes' = '{_TARGET_FILE_SIZE}'"
)
_WRITE_MERGE_ON_READ = "merge-on-read"
_WRITE_COPY_ON_WRITE = "copy-on-write"

_BRONZE_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False),
        pa.field("ingestion_timestamp", pa.timestamp("us"), nullable=False),
        pa.field("amount", pa.decimal128(10, 4), nullable=True),
        pa.field("units", pa.int32(), nullable=True),
        pa.field("note", pa.string(), nullable=True),
        pa.field("part", pa.int32(), nullable=False),
    ]
)

_BRONZE_ROWS = [
    ("A", datetime(2026, 1, 1, 10, 0, 0), Decimal("1.2500"), 1, "first", 10),
    ("A", datetime(2026, 1, 2, 10, 0, 0), None, None, None, 10),
    ("B", datetime(2026, 1, 1, 11, 0, 0), Decimal("2.5000"), 2, "keep", 20),
]


class _Program(NamedTuple):
    """One cutover shape: format version and the runner that executes it."""

    name: str
    shape: str
    format_version: int
    runner: str
    write_mode: str


_PROGRAMS: tuple[_Program, ...] = (
    _Program("s1-ctas-if-fresh", "S1", 2, "ctas", _WRITE_MERGE_ON_READ),
    _Program("s2-merge-idempotent", "S2", 2, "merge", _WRITE_MERGE_ON_READ),
    _Program("s3-dedup-coalesce-cast", "S3", 2, "dedup", _WRITE_MERGE_ON_READ),
    _Program("s4-overwrite-partitions", "S4", 2, "overwrite", _WRITE_MERGE_ON_READ),
    _Program("s5-maintenance-calls", "S5", 2, "maint", _WRITE_MERGE_ON_READ),
    _Program("s6-gold-incremental", "S6", 2, "gold", _WRITE_MERGE_ON_READ),
    _Program("s7-ctas-if-fresh", "S7", 3, "ctas", _WRITE_MERGE_ON_READ),
    _Program("s7-merge-idempotent", "S7", 3, "merge", _WRITE_MERGE_ON_READ),
    _Program("s7-overwrite-partitions", "S7", 3, "overwrite", _WRITE_MERGE_ON_READ),
    _Program("s8-ctas-cow", "S8", 2, "ctas", _WRITE_COPY_ON_WRITE),
    _Program("s8-merge-idempotent-cow", "S8", 2, "merge", _WRITE_COPY_ON_WRITE),
    _Program("s8-overwrite-partitions-cow", "S8", 2, "overwrite", _WRITE_COPY_ON_WRITE),
    _Program("s9-ctas-cow", "S9", 3, "ctas", _WRITE_COPY_ON_WRITE),
    _Program("s9-merge-idempotent-cow", "S9", 3, "merge", _WRITE_COPY_ON_WRITE),
    _Program("s9-overwrite-partitions-cow", "S9", 3, "overwrite", _WRITE_COPY_ON_WRITE),
)


def mor_properties(format_version: int) -> str:
    """The pipeline TBLPROPERTIES block at ``format_version``."""
    return _MOR_V3 if format_version == 3 else _MOR_V2


def cow_properties(format_version: int) -> str:
    return _COW_V3 if format_version == 3 else _COW_V2


def table_properties(program: _Program) -> str:
    if program.write_mode == _WRITE_COPY_ON_WRITE:
        return cow_properties(program.format_version)
    return mor_properties(program.format_version)


def write_bronze_parquet(path: object) -> None:
    """Write the single-file synthetic bronze parquet (real column types)."""
    table = pa.table(
        {
            "id": pa.array([row[0] for row in _BRONZE_ROWS], type=pa.string()),
            "ingestion_timestamp": pa.array(
                [row[1] for row in _BRONZE_ROWS], type=pa.timestamp("us")
            ),
            "amount": pa.array([row[2] for row in _BRONZE_ROWS], type=pa.decimal128(10, 4)),
            "units": pa.array([row[3] for row in _BRONZE_ROWS], type=pa.int32()),
            "note": pa.array([row[4] for row in _BRONZE_ROWS], type=pa.string()),
            "part": pa.array([row[5] for row in _BRONZE_ROWS], type=pa.int32()),
        },
        schema=_BRONZE_SCHEMA,
    )
    pq.write_table(table, str(path), compression="snappy")
