"""V3-5: facade rewrite_data_files drops in-scope v3 deletion vectors.

pins: v3-5-dv-compaction/C-002, C-003, C-004
pins: b-mor-3-rewrite-position-deletes-v3/C-003
MUTATION: assert removed_delete_files_count == 0 after compact → this REDs.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from repark import ReparkSession

_ALLOW_CREATE_V3_KEY = "repark.sql.allowCreateFormatVersion3"


def _int_list(table: pa.Table, column: str) -> list[int]:
    """Return sorted non-null integers from ``column``."""
    values = [int(value) for value in table.column(column).to_pylist() if value is not None]
    values.sort()
    return values


def test_facade_rewrite_data_files_drops_scoped_v3_deletion_vectors(tmp_path: Path) -> None:
    """Six-file v3 MOR compact drops six DVs and reports removed_delete_files_count=6."""
    spark = (
        ReparkSession.builder.appName("v3-5-dv-compact")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
    try:
        spark.register_memory_catalog("ice", tmp_path)
        spark.sql("CREATE NAMESPACE ice.sales")
        spark.sql(
            "CREATE TABLE ice.sales.v3dv (id INT, name STRING) USING iceberg "
            "TBLPROPERTIES ('format-version' = '3', "
            "'write.delete.mode' = 'merge-on-read', "
            "'write.merge.mode' = 'merge-on-read')"
        ).collect()
        for index in range(1, 7):
            spark.sql(
                f"INSERT INTO ice.sales.v3dv VALUES ({index}, 'a'), ({index + 10}, 'b')"
            ).collect()
        spark.sql("DELETE FROM ice.sales.v3dv WHERE id <= 6").collect()
        before_ids = _int_list(
            spark.sql("SELECT id FROM ice.sales.v3dv ORDER BY id").to_arrow(),
            "id",
        )
        assert before_ids == [11, 12, 13, 14, 15, 16]
        before_deletes = spark.sql("SELECT file_format FROM ice.sales.v3dv.delete_files").to_arrow()
        assert before_deletes.num_rows == 6
        rpdf = spark.sql(
            "CALL ice.system.rewrite_position_delete_files(table => 'sales.v3dv')"
        ).to_arrow()
        assert rpdf.column("rewritten_delete_files_count")[0].as_py() == 0
        assert rpdf.column("added_delete_files_count")[0].as_py() == 0
        assert rpdf.column("rewritten_bytes_count")[0].as_py() == 0
        assert rpdf.column("added_bytes_count")[0].as_py() == 0
        mid_deletes = spark.sql("SELECT file_format FROM ice.sales.v3dv.delete_files").to_arrow()
        assert mid_deletes.num_rows == 6
        before_lineage = spark.sql(
            "SELECT id, _row_id, _last_updated_sequence_number FROM ice.sales.v3dv ORDER BY id"
        ).to_arrow()
        assert before_lineage.schema.field("_row_id").type == pa.int64()
        result = spark.sql("CALL ice.system.rewrite_data_files(table => 'sales.v3dv')").to_arrow()
        assert result.schema.field("rewritten_data_files_count").type == pa.int32()
        assert result.schema.field("removed_delete_files_count").type == pa.int32()
        assert result.column("rewritten_data_files_count")[0].as_py() == 6
        assert result.column("added_data_files_count")[0].as_py() == 1
        assert result.column("removed_delete_files_count")[0].as_py() == 6
        after_ids = _int_list(
            spark.sql("SELECT id FROM ice.sales.v3dv ORDER BY id").to_arrow(),
            "id",
        )
        assert after_ids == before_ids
        after_deletes = spark.sql("SELECT file_format FROM ice.sales.v3dv.delete_files").to_arrow()
        assert after_deletes.num_rows == 0
        after_lineage = spark.sql(
            "SELECT id, _row_id, _last_updated_sequence_number FROM ice.sales.v3dv ORDER BY id"
        ).to_arrow()
        assert (
            after_lineage.column("_row_id").to_pylist()
            == before_lineage.column("_row_id").to_pylist()
        )
        assert after_lineage.column("_last_updated_sequence_number").to_pylist() == (
            before_lineage.column("_last_updated_sequence_number").to_pylist()
        )
    finally:
        spark.stop()


def _delete_pairs(spark: ReparkSession, table: str) -> list[tuple[str, int]]:
    arrow = spark.sql(f"SELECT file_format, record_count FROM {table}.delete_files").to_arrow()
    rows = [
        (str(fmt).upper(), int(count))
        for fmt, count in zip(
            arrow.column("file_format").to_pylist(),
            arrow.column("record_count").to_pylist(),
            strict=True,
        )
    ]
    rows.sort()
    return rows


def test_facade_rewrite_position_delete_files_converts_five_upgraded_parquet_deletes(
    tmp_path: Path,
) -> None:
    """Five file-scoped parquet deletes upgrade and rewrite to one PUFFIN per data file."""
    spark = (
        ReparkSession.builder.appName("v3-b-mor-3-cell-b")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
    try:
        spark.register_memory_catalog("ice", tmp_path)
        spark.sql("CREATE NAMESPACE ice.sales")
        spark.sql(
            "CREATE TABLE ice.sales.cellb (id INT, name STRING) USING iceberg "
            "TBLPROPERTIES ('format-version' = '2', "
            "'write.delete.mode' = 'merge-on-read', "
            "'write.merge.mode' = 'merge-on-read', "
            "'write.update.mode' = 'merge-on-read', "
            "'write.delete.granularity' = 'file')"
        )
        for ident in range(1, 6):
            spark.sql(
                f"INSERT INTO ice.sales.cellb VALUES ({ident}, 'a'), ({ident + 100}, 'b')"
            ).collect()
        for ident in range(1, 6):
            spark.sql(f"DELETE FROM ice.sales.cellb WHERE id = {ident}").collect()
        spark.sql(
            "ALTER TABLE ice.sales.cellb SET TBLPROPERTIES ('format-version' = '3')"
        ).collect()
        assert _delete_pairs(spark, "ice.sales.cellb") == [("PARQUET", 1)] * 5
        before_ids = _int_list(
            spark.sql("SELECT id FROM ice.sales.cellb ORDER BY id").to_arrow(),
            "id",
        )
        result = spark.sql(
            "CALL ice.system.rewrite_position_delete_files(table => 'sales.cellb')"
        ).to_arrow()
        assert result.column("rewritten_delete_files_count")[0].as_py() == 5
        assert result.column("added_delete_files_count")[0].as_py() == 5
        assert _delete_pairs(spark, "ice.sales.cellb") == [("PUFFIN", 1)] * 5
        after_ids = _int_list(
            spark.sql("SELECT id FROM ice.sales.cellb ORDER BY id").to_arrow(),
            "id",
        )
        assert after_ids == before_ids == [101, 102, 103, 104, 105]
        second = spark.sql(
            "CALL ice.system.rewrite_position_delete_files(table => 'sales.cellb')"
        ).to_arrow()
        assert second.column("rewritten_delete_files_count")[0].as_py() == 0
        assert second.column("added_delete_files_count")[0].as_py() == 0
    finally:
        spark.stop()
