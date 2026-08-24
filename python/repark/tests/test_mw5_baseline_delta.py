"""MW-5: re-run the MW-0 merge-on-read growth demo and pin the compact delta.

MW-0 measured ten sequential MERGEs into a 1,000-row v2 merge-on-read table,
each touching the same 200 ids: delete files grew one per merge and were never
reclaimed, and ``COUNT(*)`` scan cost tracked that growth 2.1x while the answer
stayed 1,000. This module re-runs that shape, then
``rewrite_position_delete_files`` + ``rewrite_data_files`` + ``expire_snapshots``.

CI pins the deterministic half (delete-file counts and Arrow row identity).
Wall-clock scan times are measured on the same run and recorded in the unit
ledger; they are not asserted, because a timing pin on CI hardware is not the
MW-0 claim.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import pyarrow as pa
from _acceptance import (
    EXPIRE_OLDER_THAN_FUTURE_MS,
    maintenance_call_sql,
    merge_sql,
    position_delete_file_count,
    require_snapshot_expired,
    require_snapshot_readable,
    snapshot_ids_oldest_first,
)

from repark import ReparkSession

# pins: mw-5-campaign-close/C-001, C-002, C-003, C-004, C-005, C-006, C-007
# pins: mw-5-campaign-close/C-008, C-009, C-010, C-011, C-012, C-013

LOGGER = logging.getLogger(__name__)

CATALOG = "mem"
NAMESPACE = "ns"
TABLE_NAME = "mw0demo"
TABLE = f"{CATALOG}.{NAMESPACE}.{TABLE_NAME}"
TABLE_ARG = f"{NAMESPACE}.{TABLE_NAME}"
SEED_VIEW = "mw5_seed"
MERGE_VIEW = "mw5_merge"

SEED_ROW_COUNT = 1000
MERGE_COUNT = 10
IDS_PER_MERGE = 200

# pins: mw-9-delete-granularity/C-008 — 1→10 is one delete file per MERGE at
# `'partition'` (unpartitioned table). Spark-default `file` would grow with the
# data-file fan-out of each MATCHED UPDATE.
MOR_PROPERTIES = (
    "'format-version' = '2', "
    "'write.delete.mode' = 'merge-on-read', "
    "'write.update.mode' = 'merge-on-read', "
    "'write.merge.mode' = 'merge-on-read', "
    "'write.delete.granularity' = 'partition'"
)


def _seed_rows() -> list[tuple[int, str]]:
    """1,000 ``(id, name)`` pairs matching the MW-0 seed."""
    return [(index, f"n{index}") for index in range(1, SEED_ROW_COUNT + 1)]


def _merge_rows() -> list[tuple[int, str]]:
    """The same 200 ids MW-0 touched on every MERGE."""
    return [(index, f"m{index}") for index in range(1, IDS_PER_MERGE + 1)]


def _live_rows_after_merges() -> list[dict[str, object]]:
    """Oracle after the ten MERGEs: ids 1..200 renamed ``mN``, the rest ``nN``."""
    rows: list[dict[str, object]] = []
    for index in range(1, SEED_ROW_COUNT + 1):
        name = f"m{index}" if index <= IDS_PER_MERGE else f"n{index}"
        rows.append({"id": index, "name": name})
    return rows


def _data_file_count(spark: ReparkSession, table: str) -> int:
    """Live data files (``files.content = 0``) on ``table``."""
    files: pa.Table = spark.sql(f"SELECT content FROM {table}.files").to_arrow()
    contents: list[object] = files.column("content").to_pylist()
    return sum(1 for value in contents if value is not None and int(value) == 0)


def _count_star(spark: ReparkSession, table: str) -> tuple[int, pa.DataType, float]:
    """``COUNT(*)`` value, Arrow type, and wall seconds on the collect path."""
    started: float = time.perf_counter()
    arrow: pa.Table = spark.sql(f"SELECT COUNT(*) AS n FROM {table}").to_arrow()
    elapsed: float = time.perf_counter() - started
    field: pa.Field = arrow.schema.field("n")
    value: int = int(arrow.column("n")[0].as_py())
    return value, field.type, elapsed


def _merge_two_hundred(spark: ReparkSession) -> None:
    """One MERGE of the 200-id source. One commit, one position-delete file at `'partition'`."""
    spark.createDataFrame(_merge_rows(), ["id", "name"]).createOrReplaceTempView(MERGE_VIEW)
    spark.sql(merge_sql(TABLE, MERGE_VIEW, "id"))
    spark.catalog.dropTempView(MERGE_VIEW)


def test_mw0_demo_delete_files_grow_then_compact_reclaims(tmp_path: Path) -> None:
    """Ten MERGEs grow delete files 1→10; compact+expire reclaims and keeps 1,000 rows."""
    spark = ReparkSession.builder.appName("pytest-mw5-baseline").getOrCreate()
    try:
        spark.register_memory_catalog(CATALOG, tmp_path)
        owned: Path = tmp_path / "owned"
        spark.sql(f"CREATE NAMESPACE {CATALOG}.{NAMESPACE} LOCATION '{owned}'")

        spark.createDataFrame(_seed_rows(), ["id", "name"]).createOrReplaceTempView(SEED_VIEW)
        spark.sql(
            f"CREATE TABLE {TABLE} USING iceberg "
            f"TBLPROPERTIES ({MOR_PROPERTIES}) AS SELECT * FROM {SEED_VIEW}"
        )
        spark.catalog.dropTempView(SEED_VIEW)

        first_snapshot_id: int = snapshot_ids_oldest_first(spark, TABLE)[0]
        require_snapshot_readable(
            spark, TABLE, first_snapshot_id, "id", expected_rows=SEED_ROW_COUNT
        )
        scan_at_merge: dict[int, float] = {}

        merge_index: int
        for merge_index in range(1, MERGE_COUNT + 1):
            _merge_two_hundred(spark)
            delete_files: int = position_delete_file_count(spark, TABLE)
            assert delete_files == merge_index, (
                f"after MERGE {merge_index}: expected {merge_index} position-delete "
                f"files, got {delete_files}"
            )
            count_value, count_type, scan_seconds = _count_star(spark, TABLE)
            assert count_value == SEED_ROW_COUNT, (
                f"after MERGE {merge_index}: COUNT(*)={count_value}, expected {SEED_ROW_COUNT}"
            )
            assert count_type == pa.int64(), f"COUNT(*) type {count_type} != int64"
            scan_at_merge[merge_index] = scan_seconds

        ctas_via_as_of: pa.Table = spark.sql(
            f"SELECT id, name FROM {TABLE} VERSION AS OF {first_snapshot_id} ORDER BY id"
        ).to_arrow()
        assert ctas_via_as_of.num_rows == SEED_ROW_COUNT
        assert ctas_via_as_of.to_pylist() == [
            {"id": index, "name": f"n{index}"} for index in range(1, SEED_ROW_COUNT + 1)
        ]

        deletes_before_compact: int = position_delete_file_count(spark, TABLE)
        data_files_before_compact: int = _data_file_count(spark, TABLE)
        assert deletes_before_compact == MERGE_COUNT

        rows_before: pa.Table = spark.sql(f"SELECT id, name FROM {TABLE} ORDER BY id").to_arrow()
        assert rows_before.schema.field("id").type == pa.int64()
        assert rows_before.schema.field("name").type == pa.string()
        assert rows_before.to_pylist() == _live_rows_after_merges()

        spark.sql(
            maintenance_call_sql(CATALOG, "rewrite_position_delete_files", TABLE_ARG)
        ).to_arrow()
        deletes_after_compact: int = position_delete_file_count(spark, TABLE)
        assert deletes_after_compact == 1, (
            f"rewrite_position_delete_files: expected 1 live position-delete file, "
            f"got {deletes_before_compact} → {deletes_after_compact}"
        )

        spark.sql(maintenance_call_sql(CATALOG, "rewrite_data_files", TABLE_ARG)).to_arrow()

        older_than_ms: int = int(time.time() * 1000) + EXPIRE_OLDER_THAN_FUTURE_MS
        expire_extra: str = f"older_than => {older_than_ms}, retain_last => 1"
        spark.sql(
            maintenance_call_sql(CATALOG, "expire_snapshots", TABLE_ARG, extra=expire_extra)
        ).to_arrow()

        rows_after: pa.Table = spark.sql(f"SELECT id, name FROM {TABLE} ORDER BY id").to_arrow()
        assert rows_after.to_pylist() == rows_before.to_pylist()
        assert rows_after.schema.field("id").type == pa.int64()
        assert rows_after.schema.field("name").type == pa.string()

        count_warm, count_warm_type, _scan_warm = _count_star(spark, TABLE)
        assert count_warm == SEED_ROW_COUNT
        assert count_warm_type == pa.int64()
        # Merge 1 was the cold-start outlier in MW-0; the post-expire scan is
        # the same class. The timed figure is the second COUNT(*).
        count_after, count_after_type, scan_after = _count_star(spark, TABLE)
        assert count_after == SEED_ROW_COUNT
        assert count_after_type == pa.int64()

        data_files_after: int = _data_file_count(spark, TABLE)
        assert data_files_after == 1, (
            f"rewrite_data_files+expire: expected 1 live data file, "
            f"got {data_files_before_compact} → {data_files_after}"
        )
        require_snapshot_expired(spark, TABLE, first_snapshot_id)

        LOGGER.info(
            "mw5 scan seconds: merge2=%.4f merge10=%.4f after_compact_expire=%.4f "
            "delete_files %d → %d data_files %d → %d",
            scan_at_merge[2],
            scan_at_merge[10],
            scan_after,
            deletes_before_compact,
            deletes_after_compact,
            data_files_before_compact,
            data_files_after,
        )
    finally:
        spark.stop()
