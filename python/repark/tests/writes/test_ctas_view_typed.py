"""Unpartitioned CTAS from a parquet-read temp view (Utf8View batches)."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from repark import ReparkSession


def test_unpartitioned_ctas_from_parquet_temp_view_round_trips(tmp_path: Path) -> None:
    """Parquet-read Utf8View batches CTAS into Iceberg and read back equal."""
    parquet_path = tmp_path / "src.parquet"
    source = pa.table(
        {
            "name": pa.array(["a", None, "c"], type=pa.string()),
            "payload": pa.array([b"x", b"y", None], type=pa.binary()),
            "id": pa.array([1, 2, 3], type=pa.int32()),
        }
    )
    pq.write_table(source, parquet_path)
    spark = ReparkSession.builder.appName("ctas-view-1").getOrCreate()
    try:
        spark.register_memory_catalog("c", tmp_path / "warehouse")
        spark.sql("CREATE NAMESPACE c.ns")
        frame = spark.read.format("parquet").load(str(parquet_path))
        frame.createOrReplaceTempView("my_df")
        spark.sql("CREATE TABLE c.ns.t USING iceberg AS SELECT * FROM my_df")
        got = spark.sql("SELECT id, name, payload FROM c.ns.t ORDER BY id").to_arrow()
        assert got.column("id").to_pylist() == [1, 2, 3]
        assert got.column("name").to_pylist() == ["a", None, "c"]
        assert got.column("payload").to_pylist() == [b"x", b"y", None]
    finally:
        spark.stop()
