"""V3 facade `.sql()` v3 DML pins: UPDATE, sequential COW DELETE, and MERGE keep `_row_id`.

pins: v3-8-subquery-where-lineage/C-002
pins: v3-7-merge-lineage/C-002
pins: rp-6-fork-repin/C-002, C-003
pins: rp-2-fork-repin/C-003, C-005
pins: rp-3-fork-repin/C-004
pins: rp-4-fork-repin/C-003
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pyarrow as pa
import pytest

_ALLOW_CREATE_V3_KEY = "repark.sql.allowCreateFormatVersion3"
_FORMAT_V3_ONLY = "'format-version' = '3'"
_COW_V3 = (
    "'format-version' = '3', "
    "'write.delete.mode' = 'copy-on-write', "
    "'write.update.mode' = 'copy-on-write', "
    "'write.merge.mode' = 'copy-on-write'"
)
_MOR_V3 = "'format-version' = '3', 'write.delete.mode' = 'merge-on-read'"
_MOR_V3_BOTH = (
    "'format-version' = '3', "
    "'write.delete.mode' = 'merge-on-read', "
    "'write.update.mode' = 'merge-on-read'"
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


def _current_lineage(metadata_file: str) -> tuple[int, int, int]:
    metadata = json.loads(Path(metadata_file).read_text(encoding="utf-8"))
    current_snapshot_id = metadata["current-snapshot-id"]
    snapshot = next(
        item for item in metadata["snapshots"] if item["snapshot-id"] == current_snapshot_id
    )
    return (
        int(metadata["next-row-id"]),
        int(snapshot["first-row-id"]),
        int(snapshot["added-rows"]),
    )


def _lineage_triples(table: pa.Table) -> list[tuple[int, int, int]]:
    return list(
        zip(
            table.column("id").to_pylist(),
            table.column("_row_id").to_pylist(),
            table.column("_last_updated_sequence_number").to_pylist(),
            strict=True,
        )
    )


def _id_name_rows(table: pa.Table) -> list[tuple[int, str]]:
    assert table.schema.field("id").type == pa.int32(), table.schema
    assert table.schema.field("name").type == pa.string(), table.schema
    ids = table.column("id").to_pylist()
    names = table.column("name").to_pylist()
    pairs = list(zip(ids, names, strict=True))
    pairs.sort(key=lambda row: row[0])
    return pairs


def test_facade_adopted_v3_cow_dml_keeps_row_id(
    tmp_path: Path,
) -> None:
    """Adopted v3 MERGE matched-update and UPDATE keep `_row_id`."""
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
        spark.sql(
            "MERGE INTO ice.sales.adopt_mrg AS t USING "
            "(SELECT 2 AS id, 'm' AS name) AS s "
            "ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET t.name = s.name"
        ).collect()
        merged = spark.sql(
            "SELECT id, name, _row_id, _last_updated_sequence_number "
            "FROM ice.sales.adopt_mrg ORDER BY id"
        ).to_arrow()
        assert _id_name_rows(merged) == [(1, "a"), (2, "m"), (3, "c")]
        assert list(
            zip(
                merged.column("id").to_pylist(),
                merged.column("_row_id").to_pylist(),
                merged.column("_last_updated_sequence_number").to_pylist(),
                strict=True,
            )
        ) == [(1, 0, 1), (2, 1, 2), (3, 2, 1)]
        assert _current_lineage(_latest_version_uuid_metadata(sales, "seed_mrg")) == (6, 3, 3)
        spark.sql(
            "CREATE TABLE ice.sales.rw_lin (id INT) USING iceberg "
            f"TBLPROPERTIES ({_FORMAT_V3_ONLY})"
        )
        for index in range(1, 7):
            spark.sql(f"INSERT INTO ice.sales.rw_lin SELECT {index} AS id")
        before = spark.sql(
            "SELECT id, _row_id, _last_updated_sequence_number FROM ice.sales.rw_lin ORDER BY id"
        ).to_arrow()
        assert before.schema.field("_row_id").type == pa.int64()
        assert before.schema.field("_last_updated_sequence_number").type == pa.int64()
        before_rows = list(
            zip(
                before.column("id").to_pylist(),
                before.column("_row_id").to_pylist(),
                before.column("_last_updated_sequence_number").to_pylist(),
                strict=True,
            )
        )
        result = spark.sql("CALL ice.system.rewrite_data_files(table => 'sales.rw_lin')").to_arrow()
        assert result.column("rewritten_data_files_count")[0].as_py() == 6
        after = spark.sql(
            "SELECT id, _row_id, _last_updated_sequence_number FROM ice.sales.rw_lin ORDER BY id"
        ).to_arrow()
        after_rows = list(
            zip(
                after.column("id").to_pylist(),
                after.column("_row_id").to_pylist(),
                after.column("_last_updated_sequence_number").to_pylist(),
                strict=True,
            )
        )
        assert after.schema.field("_row_id").type == pa.int64()
        assert after_rows == before_rows

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
        spark.sql("DELETE FROM ice.sales.adopt_del WHERE id = 2").collect()
        deleted = spark.sql("SELECT id, name FROM ice.sales.adopt_del").to_arrow()
        assert _id_name_rows(deleted) == [(1, "a"), (3, "c")]
        spark.sql("UPDATE ice.sales.adopt_del SET name = 'x' WHERE id = 3").collect()
        updated = spark.sql(
            "SELECT id, name, _row_id, _last_updated_sequence_number "
            "FROM ice.sales.adopt_del ORDER BY id"
        ).to_arrow()
        assert _id_name_rows(updated) == [(1, "a"), (3, "x")]
        update_lineage = list(
            zip(
                updated.column("id").to_pylist(),
                updated.column("_row_id").to_pylist(),
                updated.column("_last_updated_sequence_number").to_pylist(),
                strict=True,
            )
        )
        assert update_lineage == [(1, 0, 1), (3, 2, 3)]
    finally:
        spark.stop()


def test_facade_created_v3_cow_update_keeps_row_id(tmp_path: Path) -> None:
    """Created v3 COW UPDATE keeps `_row_id` and bumps seq on the changed row."""
    from repark import ReparkSession

    spark = (
        ReparkSession.builder.appName("rp-6-created-upd")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
    try:
        spark.register_memory_catalog("ice", tmp_path)
        spark.sql("CREATE NAMESPACE ice.sales")
        spark.sql(
            "CREATE TABLE ice.sales.created_upd (id INT, name STRING) USING iceberg "
            f"TBLPROPERTIES ({_COW_V3})"
        )
        spark.sql("INSERT INTO ice.sales.created_upd VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        spark.sql("UPDATE ice.sales.created_upd SET name = 'x' WHERE id = 2").collect()
        updated = spark.sql(
            "SELECT id, _row_id, _last_updated_sequence_number "
            "FROM ice.sales.created_upd ORDER BY id"
        ).to_arrow()
        assert list(
            zip(
                updated.column("id").to_pylist(),
                updated.column("_row_id").to_pylist(),
                updated.column("_last_updated_sequence_number").to_pylist(),
                strict=True,
            )
        ) == [(1, 0, 1), (2, 1, 2), (3, 2, 1)]
    finally:
        spark.stop()


def test_facade_created_v3_cow_merge_matched_update_keeps_row_id(tmp_path: Path) -> None:
    """Created v3 COW MERGE matched-update keeps `_row_id` and bumps seq on the match."""
    from repark import ReparkSession

    spark = (
        ReparkSession.builder.appName("v3-7-created-mrg")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
    try:
        spark.register_memory_catalog("ice", tmp_path)
        spark.sql("CREATE NAMESPACE ice.sales")
        spark.sql(
            "CREATE TABLE ice.sales.created_mrg (id INT, name STRING) USING iceberg "
            f"TBLPROPERTIES ({_COW_V3})"
        )
        spark.sql("INSERT INTO ice.sales.created_mrg VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        spark.sql(
            "MERGE INTO ice.sales.created_mrg AS t USING "
            "(SELECT 2 AS id, 'm' AS name) AS s "
            "ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET t.name = s.name"
        ).collect()
        merged = spark.sql(
            "SELECT id, _row_id, _last_updated_sequence_number "
            "FROM ice.sales.created_mrg ORDER BY id"
        ).to_arrow()
        assert list(
            zip(
                merged.column("id").to_pylist(),
                merged.column("_row_id").to_pylist(),
                merged.column("_last_updated_sequence_number").to_pylist(),
                strict=True,
            )
        ) == [(1, 0, 1), (2, 1, 2), (3, 2, 1)]
    finally:
        spark.stop()


def test_facade_adopted_v3_cow_second_delete_keeps_survivor_row_id(
    tmp_path: Path,
) -> None:
    """The second COW DELETE keeps the survivor `_row_id` at Spark's next-row-id 6."""
    from repark import ReparkSession

    spark = (
        ReparkSession.builder.appName("rp-3-cow-sequential")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
    try:
        spark.register_memory_catalog("ice", tmp_path)
        sales = tmp_path / "sales"
        spark.sql(f"CREATE NAMESPACE ice.sales LOCATION '{sales}'")
        spark.sql(
            "CREATE TABLE ice.sales.seed_seq (id INT, name STRING) USING iceberg "
            f"TBLPROPERTIES ({_COW_V3})"
        )
        spark.sql("INSERT INTO ice.sales.seed_seq VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        metadata_file = _latest_version_uuid_metadata(sales, "seed_seq")
        spark.sql(
            "CALL ice.system.register_table("
            f"table => 'sales.adopt_seq', metadata_file => '{metadata_file}')"
        )
        spark.sql("DELETE FROM ice.sales.adopt_seq WHERE id = 2").collect()
        latest = _latest_version_uuid_metadata(sales, "seed_seq")
        before_lineage = _current_lineage(latest)
        assert before_lineage == (5, 3, 2)
        spark.sql("DELETE FROM ice.sales.adopt_seq WHERE id = 3").collect()
        live = spark.sql(
            "SELECT id, name, _row_id, _last_updated_sequence_number "
            "FROM ice.sales.adopt_seq ORDER BY id"
        ).to_arrow()
        assert _id_name_rows(live) == [(1, "a")]
        assert list(
            zip(
                live.column("id").to_pylist(),
                live.column("_row_id").to_pylist(),
                live.column("_last_updated_sequence_number").to_pylist(),
                strict=True,
            )
        ) == [(1, 0, 1)]
        assert _current_lineage(_latest_version_uuid_metadata(sales, "seed_seq")) == (6, 5, 1)
    finally:
        spark.stop()


def _objects_under(root: Path) -> list[str]:
    return sorted(str(path) for path in root.rglob("*") if path.is_file())


def _delete_file_kinds(spark: object, table: str) -> list[str]:
    files = spark.sql(f"SELECT file_format FROM ice.sales.{table}.delete_files").to_arrow()
    return [str(kind).upper() for kind in files.column("file_format").to_pylist()]


def test_facade_v3_mor_first_delete_commits_a_deletion_vector_and_a_second_merges(
    tmp_path: Path,
) -> None:
    """First MOR DELETE commits a Puffin DV; the second merges into that live vector."""
    from repark import ReparkSession

    spark = (
        ReparkSession.builder.appName("rp-2-mor").config(_ALLOW_CREATE_V3_KEY, "true").getOrCreate()
    )
    try:
        spark.register_memory_catalog("ice", tmp_path)
        sales = tmp_path / "sales"
        spark.sql(f"CREATE NAMESPACE ice.sales LOCATION '{sales}'")
        spark.sql(
            "CREATE TABLE ice.sales.seed_mor (id INT, name STRING) USING iceberg "
            f"TBLPROPERTIES ({_MOR_V3})"
        )
        spark.sql("INSERT INTO ice.sales.seed_mor VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        metadata_file = _latest_version_uuid_metadata(sales, "seed_mor")
        spark.sql(
            "CALL ice.system.register_table("
            f"table => 'sales.adopt_mor', metadata_file => '{metadata_file}')"
        )
        spark.sql("DELETE FROM ice.sales.adopt_mor WHERE id = 2").collect()
        survivors = [(1, "a"), (3, "c")]
        first = spark.sql("SELECT id, name FROM ice.sales.adopt_mor").to_arrow()
        assert _id_name_rows(first) == survivors
        delete_files = spark.sql(
            "SELECT file_format FROM ice.sales.adopt_mor.delete_files"
        ).to_arrow()
        kinds = [str(kind).upper() for kind in delete_files.column("file_format").to_pylist()]
        assert kinds == ["PUFFIN"], kinds
        spark.sql("DELETE FROM ice.sales.adopt_mor WHERE id = 3").collect()
        second = spark.sql("SELECT id, name FROM ice.sales.adopt_mor").to_arrow()
        assert _id_name_rows(second) == [(1, "a")]
        delete_files_after = spark.sql(
            "SELECT file_format FROM ice.sales.adopt_mor.delete_files"
        ).to_arrow()
        kinds_after = [
            str(kind).upper() for kind in delete_files_after.column("file_format").to_pylist()
        ]
        assert kinds_after == ["PUFFIN"], kinds_after
    finally:
        spark.stop()


def test_facade_adopted_v3_cow_subquery_where_dml_keeps_row_lineage(tmp_path: Path) -> None:
    """Subquery-WHERE UPDATE and DELETE keep stored `_row_id` on an adopted v3 COW table.

    pins: v3-8-subquery-where-lineage/C-002
    """
    from repark import ReparkSession

    spark = (
        ReparkSession.builder.appName("v3-8-subquery-where")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
    try:
        spark.register_memory_catalog("ice", tmp_path)
        sales = tmp_path / "sales"
        spark.sql(f"CREATE NAMESPACE ice.sales LOCATION '{sales}'")
        spark.sql("CREATE TABLE ice.sales.srcids (id INT) USING iceberg")
        spark.sql("INSERT INTO ice.sales.srcids VALUES (2)")
        for seed, adopted in (("seed_su", "adopt_su"), ("seed_sd", "adopt_sd")):
            spark.sql(
                f"CREATE TABLE ice.sales.{seed} (id INT, name STRING) USING iceberg "
                f"TBLPROPERTIES ({_COW_V3})"
            )
            spark.sql(f"INSERT INTO ice.sales.{seed} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            metadata_file = _latest_version_uuid_metadata(sales, seed)
            spark.sql(
                "CALL ice.system.register_table("
                f"table => 'sales.{adopted}', metadata_file => '{metadata_file}')"
            )
        spark.sql(
            "UPDATE ice.sales.adopt_su SET name = 'm' WHERE id IN (SELECT id FROM ice.sales.srcids)"
        ).collect()
        updated = spark.sql(
            "SELECT id, name, _row_id, _last_updated_sequence_number "
            "FROM ice.sales.adopt_su ORDER BY id"
        ).to_arrow()
        assert _id_name_rows(updated) == [(1, "a"), (2, "m"), (3, "c")]
        assert _lineage_triples(updated) == [(1, 0, 1), (2, 1, 2), (3, 2, 1)]
        assert _current_lineage(_latest_version_uuid_metadata(sales, "seed_su")) == (6, 3, 3)
        spark.sql(
            "DELETE FROM ice.sales.adopt_sd WHERE id IN (SELECT id FROM ice.sales.srcids)"
        ).collect()
        deleted = spark.sql(
            "SELECT id, name, _row_id, _last_updated_sequence_number "
            "FROM ice.sales.adopt_sd ORDER BY id"
        ).to_arrow()
        assert _id_name_rows(deleted) == [(1, "a"), (3, "c")]
        assert _lineage_triples(deleted) == [(1, 0, 1), (3, 2, 1)]
        assert _current_lineage(_latest_version_uuid_metadata(sales, "seed_sd")) == (5, 3, 2)
    finally:
        spark.stop()


def test_facade_v3_subquery_update_outside_the_hole_still_refuses(tmp_path: Path) -> None:
    """`UPDATE ... WHERE NOT IN` is refused by the pre-existing subquery-shape guard.

    pins: v3-8-subquery-where-lineage/C-002
    """
    from repark import ReparkSession
    from repark.errors import AnalysisException

    spark = (
        ReparkSession.builder.appName("v3-8-subquery-outside")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
    try:
        spark.register_memory_catalog("ice", tmp_path)
        sales = tmp_path / "sales"
        spark.sql(f"CREATE NAMESPACE ice.sales LOCATION '{sales}'")
        spark.sql("CREATE TABLE ice.sales.srcids (id INT) USING iceberg")
        spark.sql("INSERT INTO ice.sales.srcids VALUES (2)")
        spark.sql(
            "CREATE TABLE ice.sales.outside (id INT, name STRING) USING iceberg "
            f"TBLPROPERTIES ({_COW_V3})"
        )
        spark.sql("INSERT INTO ice.sales.outside VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        with pytest.raises(AnalysisException) as raised:
            spark.sql(
                "UPDATE ice.sales.outside SET name = 'm' "
                "WHERE id NOT IN (SELECT id FROM ice.sales.srcids)"
            ).collect()
        assert "G3-E8" in str(raised.value)
        assert "V3-COW-1" not in str(raised.value)
        after = spark.sql("SELECT id, name FROM ice.sales.outside").to_arrow()
        assert _id_name_rows(after) == [(1, "a"), (2, "b"), (3, "c")]
    finally:
        spark.stop()


def test_facade_adopted_v3_mor_subquery_where_dml_writes_deletion_vectors(tmp_path: Path) -> None:
    """Subquery-WHERE MOR DELETE and UPDATE write one file-scoped Puffin DV on adopted v3.

    pins: v3-9-mor-predicate-dml-dv/C-003
    """
    from repark import ReparkSession

    spark = (
        ReparkSession.builder.appName("v3-9-mor-subquery")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
    try:
        spark.register_memory_catalog("ice", tmp_path)
        sales = tmp_path / "sales"
        spark.sql(f"CREATE NAMESPACE ice.sales LOCATION '{sales}'")
        spark.sql("CREATE TABLE ice.sales.srcids (id INT) USING iceberg")
        spark.sql("INSERT INTO ice.sales.srcids VALUES (2)")
        for seed, adopted in (("seed_msd", "adopt_msd"), ("seed_msu", "adopt_msu")):
            spark.sql(
                f"CREATE TABLE ice.sales.{seed} (id INT, name STRING) USING iceberg "
                f"TBLPROPERTIES ({_MOR_V3_BOTH})"
            )
            spark.sql(f"INSERT INTO ice.sales.{seed} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            metadata_file = _latest_version_uuid_metadata(sales, seed)
            spark.sql(
                "CALL ice.system.register_table("
                f"table => 'sales.{adopted}', metadata_file => '{metadata_file}')"
            )
        spark.sql(
            "DELETE FROM ice.sales.adopt_msd WHERE id IN (SELECT id FROM ice.sales.srcids)"
        ).collect()
        deleted = spark.sql(
            "SELECT id, name, _row_id, _last_updated_sequence_number "
            "FROM ice.sales.adopt_msd ORDER BY id"
        ).to_arrow()
        assert _id_name_rows(deleted) == [(1, "a"), (3, "c")]
        assert _lineage_triples(deleted) == [(1, 0, 1), (3, 2, 1)]
        assert _current_lineage(_latest_version_uuid_metadata(sales, "seed_msd")) == (3, 3, 0)
        assert _delete_file_kinds(spark, "adopt_msd") == ["PUFFIN"]
        spark.sql(
            "UPDATE ice.sales.adopt_msu SET name = 'm' "
            "WHERE id IN (SELECT id FROM ice.sales.srcids)"
        ).collect()
        updated = spark.sql(
            "SELECT id, name, _row_id, _last_updated_sequence_number "
            "FROM ice.sales.adopt_msu ORDER BY id"
        ).to_arrow()
        assert _id_name_rows(updated) == [(1, "a"), (2, "m"), (3, "c")]
        assert _lineage_triples(updated) == [(1, 0, 1), (2, 1, 2), (3, 2, 1)]
        assert _current_lineage(_latest_version_uuid_metadata(sales, "seed_msu")) == (4, 3, 1)
        assert _delete_file_kinds(spark, "adopt_msu") == ["PUFFIN"]
    finally:
        spark.stop()
