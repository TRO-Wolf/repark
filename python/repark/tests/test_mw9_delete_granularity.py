"""MW-9: facade Spark `.sql()` honors `write.delete.granularity`.

pins: mw-9-delete-granularity/C-001, C-010
"""

from __future__ import annotations

from pathlib import Path


def test_unset_granularity_writes_one_delete_file_per_data_file(tmp_path: Path) -> None:
    """Default session: one MERGE across six data files writes six position-delete files."""
    from repark import ReparkSession

    spark = ReparkSession.builder.appName("mw-9-default").getOrCreate()
    try:
        spark.register_memory_catalog("ice", tmp_path)
        spark.sql("CREATE NAMESPACE ice.sales")
        spark.sql(
            "CREATE TABLE ice.sales.g (id INT, v STRING) USING iceberg "
            "TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read')"
        )
        for identifier in range(1, 7):
            spark.sql(f"INSERT INTO ice.sales.g VALUES ({identifier}, 'v{identifier}')")
        spark.sql(
            "MERGE INTO ice.sales.g AS t USING "
            "(SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL "
            " SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6) AS s "
            "ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = 'merged'"
        )
        deletes = spark.sql("SELECT * FROM ice.sales.g.files WHERE content = 1").to_arrow()
        assert deletes.num_rows == 6, f"default file granularity: {deletes.num_rows} delete files"
        live = spark.sql("SELECT * FROM ice.sales.g").to_arrow()
        assert live.num_rows == 6
    finally:
        spark.stop()
