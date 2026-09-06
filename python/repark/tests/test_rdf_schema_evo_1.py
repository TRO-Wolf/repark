"""RDF-SCHEMA-EVO-1: rewrite_data_files after schema evolution, pinned against Spark."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession

SEVEN = "(id INT, a STRING, b BIGINT, c DOUBLE, d BOOLEAN, e DATE, f TIMESTAMP)"


def _row_values(i: int) -> str:
    return (
        f"({i}, 'a{i}', {100 + i}, {1.5 + i}, true, "
        f"DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:0{i}')"
    )


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-rdf-schema-evo-1").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


@pytest.fixture
def spark_v3(tmp_path: Path) -> ReparkSession:
    session = (
        ReparkSession.builder.appName("pytest-rdf-schema-evo-1-v3")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def _seed(spark: ReparkSession, table: str, partitioned_by: str = "") -> None:
    part = f" PARTITIONED BY ({partitioned_by})" if partitioned_by else ""
    spark.sql(f"CREATE TABLE {table} {SEVEN} USING iceberg{part}")
    for i in range(6):
        spark.sql(f"INSERT INTO {table} VALUES {_row_values(i)}")


def _ids(table: pa.Table) -> list[int]:
    return [int(v) for v in table.column("id").to_pylist()]


def _file_cells(spark: ReparkSession, table: str) -> list[tuple[int, int]]:
    files = spark.sql(f"SELECT spec_id, record_count FROM {table}.files").to_arrow()
    spec_ids = files.column("spec_id").to_pylist()
    counts = files.column("record_count").to_pylist()
    cells = list(zip(spec_ids, counts, strict=True))
    return sorted((int(s), int(n)) for s, n in cells)


def test_rewrite_after_add_column_and_partition_field_matches_spark(spark: ReparkSession) -> None:
    spark.sql(f"CREATE TABLE mem.ns.evo {SEVEN} USING iceberg")
    for i in range(6):
        spark.sql(f"INSERT INTO mem.ns.evo VALUES {_row_values(i)}")
    before = spark.sql("SELECT * FROM mem.ns.evo ORDER BY id").to_arrow()
    spark.sql("ALTER TABLE mem.ns.evo ADD COLUMN note STRING")
    spark.sql("ALTER TABLE mem.ns.evo ADD PARTITION FIELD bucket(4, id)")
    result = spark.sql("CALL mem.system.rewrite_data_files(table => 'ns.evo')").to_arrow()
    assert result.column("rewritten_data_files_count")[0].as_py() == 6
    assert result.column("added_data_files_count")[0].as_py() == 3
    after = spark.sql("SELECT * FROM mem.ns.evo ORDER BY id").to_arrow()
    assert _ids(after) == [0, 1, 2, 3, 4, 5]
    assert after.column("note").to_pylist() == [None] * 6
    assert after.schema.field("note").type == pa.string()
    assert after.num_columns == before.num_columns + 1
    assert [after.column(n).to_pylist() for n in before.schema.names] == [
        before.column(n).to_pylist() for n in before.schema.names
    ]
    assert _file_cells(spark, "mem.ns.evo") == [(1, 1), (1, 2), (1, 3)]


def test_rewrite_after_add_column_only_partitioned(spark: ReparkSession) -> None:
    _seed(spark, "mem.ns.addp", partitioned_by="d")
    spark.sql("ALTER TABLE mem.ns.addp ADD COLUMN note STRING")
    result = spark.sql("CALL mem.system.rewrite_data_files(table => 'ns.addp')").to_arrow()
    assert result.column("rewritten_data_files_count")[0].as_py() == 6
    assert result.column("added_data_files_count")[0].as_py() == 1
    after = spark.sql("SELECT * FROM mem.ns.addp ORDER BY id").to_arrow()
    assert _ids(after) == [0, 1, 2, 3, 4, 5]
    assert after.column("note").to_pylist() == [None] * 6
    assert _file_cells(spark, "mem.ns.addp") == [(0, 6)]


def test_rewrite_after_add_column_only_unpartitioned_is_unaffected(spark: ReparkSession) -> None:
    _seed(spark, "mem.ns.addu")
    spark.sql("ALTER TABLE mem.ns.addu ADD COLUMN note STRING")
    result = spark.sql("CALL mem.system.rewrite_data_files(table => 'ns.addu')").to_arrow()
    assert result.column("rewritten_data_files_count")[0].as_py() == 6
    assert result.column("added_data_files_count")[0].as_py() == 1
    after = spark.sql("SELECT * FROM mem.ns.addu ORDER BY id").to_arrow()
    assert _ids(after) == [0, 1, 2, 3, 4, 5]
    assert after.column("note").to_pylist() == [None] * 6


def test_rewrite_after_drop_column_matches_spark(spark: ReparkSession) -> None:
    _seed(spark, "mem.ns.dropc")
    spark.sql("ALTER TABLE mem.ns.dropc ADD PARTITION FIELD bucket(4, id)")
    spark.sql("ALTER TABLE mem.ns.dropc DROP COLUMN f")
    result = spark.sql("CALL mem.system.rewrite_data_files(table => 'ns.dropc')").to_arrow()
    assert result.column("rewritten_data_files_count")[0].as_py() == 6
    assert result.column("added_data_files_count")[0].as_py() == 3
    after = spark.sql("SELECT * FROM mem.ns.dropc ORDER BY id").to_arrow()
    assert _ids(after) == [0, 1, 2, 3, 4, 5]
    assert [f.name for f in after.schema] == ["id", "a", "b", "c", "d", "e"]
    assert _file_cells(spark, "mem.ns.dropc") == [(1, 1), (1, 2), (1, 3)]


def test_rewrite_after_rename_column_matches_spark(spark: ReparkSession) -> None:
    _seed(spark, "mem.ns.renc")
    spark.sql("ALTER TABLE mem.ns.renc ADD PARTITION FIELD bucket(4, id)")
    spark.sql("ALTER TABLE mem.ns.renc RENAME COLUMN f TO f2")
    result = spark.sql("CALL mem.system.rewrite_data_files(table => 'ns.renc')").to_arrow()
    assert result.column("rewritten_data_files_count")[0].as_py() == 6
    assert result.column("added_data_files_count")[0].as_py() == 3
    after = spark.sql("SELECT * FROM mem.ns.renc ORDER BY id").to_arrow()
    assert _ids(after) == [0, 1, 2, 3, 4, 5]
    assert [f.name for f in after.schema] == ["id", "a", "b", "c", "d", "e", "f2"]
    assert _file_cells(spark, "mem.ns.renc") == [(1, 1), (1, 2), (1, 3)]


def test_rewrite_after_promote_column_matches_spark(spark: ReparkSession) -> None:
    spark.sql("CREATE TABLE mem.ns.promc (id INT, v BIGINT) USING iceberg PARTITIONED BY (v)")
    for i in range(6):
        spark.sql(f"INSERT INTO mem.ns.promc VALUES ({i}, 100)")
    spark.sql("ALTER TABLE mem.ns.promc ALTER COLUMN id TYPE BIGINT")
    result = spark.sql("CALL mem.system.rewrite_data_files(table => 'ns.promc')").to_arrow()
    assert result.column("rewritten_data_files_count")[0].as_py() == 6
    assert result.column("added_data_files_count")[0].as_py() == 1
    after = spark.sql("SELECT id, v FROM mem.ns.promc ORDER BY id").to_arrow()
    assert after.column("id").to_pylist() == [0, 1, 2, 3, 4, 5]
    assert after.schema.field("id").type == pa.int64()
    assert _file_cells(spark, "mem.ns.promc") == [(0, 6)]


def test_rewrite_after_promote_partition_source_matches_spark(spark: ReparkSession) -> None:
    spark.sql("CREATE TABLE mem.ns.promp (id INT, v BIGINT) USING iceberg PARTITIONED BY (id)")
    for i in range(6):
        spark.sql(f"INSERT INTO mem.ns.promp VALUES (7, {100 + i})")
    spark.sql("ALTER TABLE mem.ns.promp ALTER COLUMN id TYPE BIGINT")
    result = spark.sql("CALL mem.system.rewrite_data_files(table => 'ns.promp')").to_arrow()
    assert result.column("rewritten_data_files_count")[0].as_py() == 6
    assert result.column("added_data_files_count")[0].as_py() == 1
    after = spark.sql("SELECT id, v FROM mem.ns.promp ORDER BY v").to_arrow()
    assert after.column("id").to_pylist() == [7] * 6
    assert after.schema.field("id").type == pa.int64()
    assert _file_cells(spark, "mem.ns.promp") == [(0, 6)]


def test_rewrite_v3_deletion_vectors_after_evolution_matches_spark(spark_v3: ReparkSession) -> None:
    spark_v3.sql(
        "CREATE TABLE mem.ns.v3dv (id INT, v BIGINT) USING iceberg "
        "TBLPROPERTIES ('format-version' = '3', 'write.delete.mode' = 'merge-on-read')"
    )
    for i in range(6):
        spark_v3.sql(f"INSERT INTO mem.ns.v3dv VALUES ({i}, {100 + i})")
    spark_v3.sql("DELETE FROM mem.ns.v3dv WHERE id = 1")
    assert spark_v3.sql("SELECT * FROM mem.ns.v3dv.delete_files").to_arrow().num_rows == 1
    spark_v3.sql("ALTER TABLE mem.ns.v3dv ADD COLUMN note STRING")
    spark_v3.sql("ALTER TABLE mem.ns.v3dv ADD PARTITION FIELD bucket(4, id)")
    result = spark_v3.sql("CALL mem.system.rewrite_data_files(table => 'ns.v3dv')").to_arrow()
    assert result.column("rewritten_data_files_count")[0].as_py() == 6
    assert result.column("added_data_files_count")[0].as_py() == 3
    assert result.column("removed_delete_files_count")[0].as_py() == 1
    after = spark_v3.sql("SELECT id, v, note FROM mem.ns.v3dv ORDER BY id").to_arrow()
    assert after.column("id").to_pylist() == [0, 2, 3, 4, 5]
    assert after.column("note").to_pylist() == [None] * 5
    assert _file_cells(spark_v3, "mem.ns.v3dv") == [(1, 1), (1, 2), (1, 2)]
    assert spark_v3.sql("SELECT * FROM mem.ns.v3dv.delete_files").to_arrow().num_rows == 0


def test_rewrite_with_post_evolution_write_is_unaffected(spark: ReparkSession) -> None:
    _seed(spark, "mem.ns.both")
    spark.sql("ALTER TABLE mem.ns.both ADD COLUMN note STRING")
    spark.sql("ALTER TABLE mem.ns.both ADD PARTITION FIELD bucket(4, id)")
    for i in range(6, 8):
        spark.sql(
            f"INSERT INTO mem.ns.both VALUES ({i}, 'a{i}', {100 + i}, {1.5 + i}, true, "
            f"DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:0{i}', 'n{i}')"
        )
    result = spark.sql("CALL mem.system.rewrite_data_files(table => 'ns.both')").to_arrow()
    assert result.column("rewritten_data_files_count")[0].as_py() >= 6
    after = spark.sql("SELECT id, note FROM mem.ns.both ORDER BY id").to_arrow()
    assert _ids(after) == [0, 1, 2, 3, 4, 5, 6, 7]
    assert after.column("note").to_pylist() == [None] * 6 + ["n6", "n7"]


def test_rewrite_position_delete_files_on_evolved_table(spark: ReparkSession) -> None:
    spark.sql(
        "CREATE TABLE mem.ns.rpd (id INT, v BIGINT) USING iceberg "
        "TBLPROPERTIES ('write.delete.mode' = 'merge-on-read')"
    )
    for i in range(6):
        spark.sql(f"INSERT INTO mem.ns.rpd VALUES ({i}, {100 + i})")
    spark.sql("DELETE FROM mem.ns.rpd WHERE id = 1")
    spark.sql("ALTER TABLE mem.ns.rpd ADD COLUMN note STRING")
    spark.sql("ALTER TABLE mem.ns.rpd ADD PARTITION FIELD bucket(4, id)")
    result = spark.sql(
        "CALL mem.system.rewrite_position_delete_files(table => 'ns.rpd')"
    ).to_arrow()
    assert result.column("rewritten_delete_files_count")[0].as_py() == 0
    assert result.column("added_delete_files_count")[0].as_py() == 0
    after = spark.sql("SELECT id FROM mem.ns.rpd ORDER BY id").to_arrow()
    assert after.column("id").to_pylist() == [0, 2, 3, 4, 5]


def test_rewrite_manifests_on_evolved_table(spark: ReparkSession) -> None:
    _seed(spark, "mem.ns.rm")
    spark.sql("ALTER TABLE mem.ns.rm ADD COLUMN note STRING")
    spark.sql("ALTER TABLE mem.ns.rm ADD PARTITION FIELD bucket(4, id)")
    result = spark.sql("CALL mem.system.rewrite_manifests(table => 'ns.rm')").to_arrow()
    assert result.column("rewritten_manifests_count")[0].as_py() == 0
    assert result.column("added_manifests_count")[0].as_py() == 0
    after = spark.sql("SELECT * FROM mem.ns.rm ORDER BY id").to_arrow()
    assert _ids(after) == [0, 1, 2, 3, 4, 5]
