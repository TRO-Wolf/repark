"""E-CASE-PARTITION-FIELD facade pins: partition sources bind case-sensitively."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import PySparkException


@pytest.fixture()
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-u11-edge-partition").getOrCreate()
    session.register_memory_catalog("sc", tmp_path)
    session.sql("CREATE NAMESPACE sc.ns")
    return session


def _fresh_table(spark: ReparkSession, tag: str) -> str:
    """Create a two-column table and return its three-part name."""
    name = f"sc.ns.tp_{tag}"
    spark.sql(f"CREATE TABLE {name} (id INT, cat STRING) USING iceberg")
    return name


def _spec_fields(warehouse: Path, table: str) -> list[str]:
    """Return the current default-spec partition field names from table metadata."""
    short = table.split(".")[-1]
    paths = sorted(
        warehouse.rglob(f"ns/{short}/metadata/*.metadata.json"),
        key=lambda path: int(path.name.split("-")[0]),
    )
    metadata = json.loads(paths[-1].read_text(encoding="utf-8"))
    specs = {spec["spec-id"]: spec for spec in metadata["partition-specs"]}
    return [field["name"] for field in specs[metadata["default-spec-id"]]["fields"]]


def test_add_partition_field_wrong_case_refuses(spark: ReparkSession, tmp_path: Path) -> None:
    """ADD PARTITION FIELD CAT refuses with the ValidationException text, table unchanged.

    pins: u11-edge-1/C-009
    """
    table = _fresh_table(spark, "add")
    with pytest.raises(PySparkException) as error:
        spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD CAT")
    assert str(error.value).endswith(
        "org.apache.iceberg.exceptions.ValidationException: Cannot find field 'CAT' in "
        "struct: struct<1: id: optional int, 2: cat: optional string>"
    )
    assert _spec_fields(tmp_path, table) == []


def test_add_partition_field_bucket_wrong_case_refuses(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """ADD PARTITION FIELD bucket(4, ID) refuses with the ValidationException text.

    pins: u11-edge-1/C-010
    """
    table = _fresh_table(spark, "bucket")
    with pytest.raises(PySparkException) as error:
        spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD bucket(4, ID)")
    assert "Cannot find field 'ID' in struct" in str(error.value)
    assert _spec_fields(tmp_path, table) == []


def test_drop_partition_field_missing_refuses(spark: ReparkSession, tmp_path: Path) -> None:
    """DROP PARTITION FIELD CAT refuses; the spec stays empty.

    pins: u11-edge-1/C-011
    """
    table = _fresh_table(spark, "drop")
    with pytest.raises(PySparkException, match="Cannot find partition field to remove: CAT"):
        spark.sql(f"ALTER TABLE {table} DROP PARTITION FIELD CAT")
    assert _spec_fields(tmp_path, table) == []


def test_replace_partition_field_wrong_case_refuses(spark: ReparkSession) -> None:
    """REPLACE PARTITION FIELD cat WITH CAT refuses with the ValidationException text.

    pins: u11-edge-1/C-012
    """
    table = _fresh_table(spark, "replace")
    spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD cat")
    with pytest.raises(PySparkException) as error:
        spark.sql(f"ALTER TABLE {table} REPLACE PARTITION FIELD cat WITH CAT")
    assert "Cannot find field 'CAT' in struct" in str(error.value)


def test_partition_field_case_rules_ignore_case_sensitive_conf(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """Partition sources bind case-sensitively under either caseSensitive setting.

    pins: u11-edge-1/C-013
    """
    for conf in ("true", "false"):
        table = _fresh_table(spark, f"conf{conf}")
        spark.sql(f"SET spark.sql.caseSensitive={conf}")
        try:
            with pytest.raises(PySparkException, match="Cannot find field 'CAT' in struct"):
                spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD CAT")
            assert _spec_fields(tmp_path, table) == []
        finally:
            spark.sql("SET spark.sql.caseSensitive=false")


def test_write_ordered_by_accepts_any_case(spark: ReparkSession) -> None:
    """WRITE ORDERED BY CAT is accepted under the default caseSensitive=false, like Spark.

    pins: u11-edge-1/C-014
    """
    table = _fresh_table(spark, "order")
    spark.sql(f"ALTER TABLE {table} WRITE ORDERED BY CAT")
