"""V3R-1: facade Spark `.sql()` copy-on-write DML on an adopted v3 table refuses (V3-COW-1).

pins: v3r-1-rulings/C-006
"""

from __future__ import annotations

import re
from pathlib import Path

import pyarrow as pa
import pytest

from repark.errors import UnsupportedOperationException

_ALLOW_CREATE_V3_KEY = "repark.sql.allowCreateFormatVersion3"
_FORMAT_V3_ONLY = "'format-version' = '3'"
_COW_V3 = (
    "'format-version' = '3', "
    "'write.delete.mode' = 'copy-on-write', "
    "'write.update.mode' = 'copy-on-write', "
    "'write.merge.mode' = 'copy-on-write'"
)
_VERSION_UUID_METADATA = re.compile(r"^(\d+)-[0-9a-fA-F-]+\.metadata\.json$")


def _metadata_file_version(path: Path) -> int:
    matched = _VERSION_UUID_METADATA.match(path.name)
    assert matched, path.name
    return int(matched.group(1))


def _latest_version_uuid_metadata(namespace_location: Path, table: str) -> str:
    """Return the engine-written version-uuid metadata pointer under the namespace LOCATION."""
    table_metadata = namespace_location / table / "metadata"
    assert table_metadata.is_dir(), f"missing metadata dir for {table}: {table_metadata}"
    matches = [
        path
        for path in table_metadata.glob("*.metadata.json")
        if _VERSION_UUID_METADATA.match(path.name)
    ]
    assert matches, f"no version-uuid metadata for {table} under {table_metadata}"
    return str(max(matches, key=_metadata_file_version))


def _id_name_rows(table: pa.Table) -> list[tuple[int, str]]:
    assert table.schema.field("id").type == pa.int32(), table.schema
    assert table.schema.field("name").type == pa.string(), table.schema
    ids = table.column("id").to_pylist()
    names = table.column("name").to_pylist()
    pairs = list(zip(ids, names, strict=True))
    pairs.sort(key=lambda row: row[0])
    return pairs


def test_facade_adopted_v3_cow_dml_refuses_and_leaves_the_table_untouched(
    tmp_path: Path,
) -> None:
    """Adopted v3 COW MERGE, DELETE and UPDATE raise naming V3-COW-1; rows stay put."""
    from repark import ReparkSession

    spark = (
        ReparkSession.builder.appName("v3r-1-cow")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
    try:
        spark.register_memory_catalog("ice", tmp_path)
        sales = tmp_path / "sales"
        spark.sql(f"CREATE NAMESPACE ice.sales LOCATION '{sales}'")
        spark.sql(
            "CREATE TABLE ice.sales.seed_mrg (id INT, name STRING) USING iceberg "
            f"TBLPROPERTIES ({_FORMAT_V3_ONLY})"
        )
        spark.sql("INSERT INTO ice.sales.seed_mrg VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        metadata_file = _latest_version_uuid_metadata(sales, "seed_mrg")
        spark.sql(
            "CALL ice.system.register_table("
            f"table => 'sales.adopt_mrg', metadata_file => '{metadata_file}')"
        )
        seeded = [(1, "a"), (2, "b"), (3, "c")]
        with pytest.raises(UnsupportedOperationException, match="V3-COW-1"):
            spark.sql(
                "MERGE INTO ice.sales.adopt_mrg AS t USING "
                "(SELECT 2 AS id, 'm' AS name UNION ALL SELECT 4 AS id, 'n' AS name) AS s "
                "ON t.id = s.id "
                "WHEN MATCHED THEN UPDATE SET t.name = s.name "
                "WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)"
            ).collect()
        merged = spark.sql("SELECT id, name FROM ice.sales.adopt_mrg").to_arrow()
        assert _id_name_rows(merged) == seeded
        with pytest.raises(UnsupportedOperationException, match="row lineage"):
            spark.sql("CALL ice.system.rewrite_data_files(table => 'sales.adopt_mrg')").collect()

        spark.sql(
            "CREATE TABLE ice.sales.seed_del (id INT, name STRING) USING iceberg "
            f"TBLPROPERTIES ({_COW_V3})"
        )
        spark.sql("INSERT INTO ice.sales.seed_del VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        delete_metadata = _latest_version_uuid_metadata(sales, "seed_del")
        spark.sql(
            "CALL ice.system.register_table("
            f"table => 'sales.adopt_del', metadata_file => '{delete_metadata}')"
        )
        with pytest.raises(UnsupportedOperationException, match="V3-COW-1"):
            spark.sql("DELETE FROM ice.sales.adopt_del WHERE id = 2").collect()
        with pytest.raises(UnsupportedOperationException, match="V3-COW-1"):
            spark.sql("UPDATE ice.sales.adopt_del SET name = 'x' WHERE id = 2").collect()
        untouched = spark.sql("SELECT id, name FROM ice.sales.adopt_del").to_arrow()
        assert _id_name_rows(untouched) == seeded
    finally:
        spark.stop()
