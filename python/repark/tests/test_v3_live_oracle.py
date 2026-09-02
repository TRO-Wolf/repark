"""V3E-5 live oracle for Spark-written v3 fixtures.

pins: v3e-5-nightly-v3-oracle/C-002, C-003, C-004, C-005, C-007, C-008, C-011
pins: v3-7-merge-lineage/C-002
pins: v3-8-subquery-where-lineage/C-001, C-002, C-003
"""

from __future__ import annotations

import os
import shutil
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from pathlib import Path

import pyarrow as pa
import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
_PART_DV_SRC = _REPO_ROOT / "crates/repark-spark/src/tests/fixtures/v3-spark-part-dv"
_EQ_DV_SRC = _REPO_ROOT / "crates/repark-spark/src/tests/fixtures/v3-spark-eq-dv"
_PART_DV_DEST = Path("/tmp/repark-v3e3-partdv/ns/v3part")
_EQ_DV_DEST = Path("/tmp/repark-v3e3-eqdel/ns/v3eq")
_LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
_LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — live v3 oracle skipped (routine CI is JVM-free)"
_V37_LEDGER_NAME = "v3-7-merge-lineage-ledger.md"
_V38_LEDGER_NAME = "v3-8-subquery-where-lineage-ledger.md"
_ALLOW_CREATE_V3_KEY = "repark.sql.allowCreateFormatVersion3"
_COW_V3 = (
    "'format-version' = '3', "
    "'write.delete.mode' = 'copy-on-write', "
    "'write.update.mode' = 'copy-on-write', "
    "'write.merge.mode' = 'copy-on-write'"
)
_MOR_V3 = (
    "'format-version' = '3', "
    "'write.delete.mode' = 'merge-on-read', "
    "'write.update.mode' = 'merge-on-read', "
    "'write.merge.mode' = 'merge-on-read'"
)
_MATCHED_UPDATE_LINEAGE = [(1, 0, 1), (2, 1, 2), (3, 2, 1)]


class _DirLock:
    """Cross-process lock for the shared /tmp fixture copies."""

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        while True:
            try:
                self.path.mkdir()
                return
            except FileExistsError:
                if time.monotonic() - started > 120:
                    raise TimeoutError(f"fixture lock {path} held for 2 minutes") from None
                time.sleep(0.025)

    def close(self) -> None:
        with suppress(OSError):
            self.path.rmdir()


@contextmanager
def _materialize(src: Path, dest: Path) -> Iterator[str]:
    """Copy a Hadoop fixture to dest and yield its latest metadata file.

    pins: v3e-5-nightly-v3-oracle/C-003
    """
    lock = _DirLock(Path(str(dest) + ".lock"))
    try:
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(src, dest)
        metadata = dest / "metadata"
        versions = sorted(
            metadata.glob("v*.metadata.json"),
            key=lambda path: int(path.name[1:].split(".", 1)[0]),
        )
        assert versions, f"no Hadoop metadata under {metadata}"
        yield str(versions[-1])
    finally:
        lock.close()


def _ledger_path(name: str) -> Path:
    """The one bin (staging, completed, or archive) holding a unit ledger.

    pins: v3-8-subquery-where-lineage/C-003
    """
    candidates = [
        _REPO_ROOT / "task/ledgers/staging" / name,
        _REPO_ROOT / "task/ledgers/completed" / name,
        *sorted((_REPO_ROOT / "task/ledgers/archive").glob(f"*/*-{name}")),
    ]
    found = [path for path in candidates if path.is_file()]
    assert len(found) == 1, found
    return found[0]


def _objects_under(root: Path) -> list[str]:
    return sorted(str(path) for path in root.rglob("*") if path.is_file())


def _id_name_part_rows(table: pa.Table) -> list[tuple[int, str, int]]:
    """Sorted (id, name, part) rows with Arrow type check.

    pins: v3e-5-nightly-v3-oracle/C-007
    """
    assert table.schema.field("id").type == pa.int32(), table.schema
    assert table.schema.field("name").type == pa.string(), table.schema
    assert table.schema.field("part").type == pa.int32(), table.schema
    rows = list(
        zip(
            table.column("id").to_pylist(),
            table.column("name").to_pylist(),
            table.column("part").to_pylist(),
            strict=True,
        )
    )
    rows.sort(key=lambda row: row[0])
    return rows


def _id_row_id_seq(table: pa.Table) -> list[tuple[int, int, int]]:
    """Sorted (id, _row_id, seq) triples.

    pins: v3-7-merge-lineage/C-002
    """
    rows = list(
        zip(
            table.column("id").to_pylist(),
            table.column("_row_id").to_pylist(),
            table.column("_last_updated_sequence_number").to_pylist(),
            strict=True,
        )
    )
    rows.sort(key=lambda row: row[0])
    return rows


def _merge_matched_update_sql(target: str) -> str:
    """Matched-UPDATE MERGE at the V3-7 seed.

    pins: v3-7-merge-lineage/C-002
    """
    return (
        f"MERGE INTO {target} AS t USING "
        "(SELECT 2 AS id, 'm' AS name) AS s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET t.name = s.name"
    )


def _id_name_rows(table: pa.Table) -> list[tuple[int, str]]:
    """Sorted (id, name) rows with Arrow type check.

    pins: v3e-5-nightly-v3-oracle/C-007
    """
    assert table.schema.field("id").type == pa.int32(), table.schema
    assert table.schema.field("name").type == pa.string(), table.schema
    pairs = list(zip(table.column("id").to_pylist(), table.column("name").to_pylist(), strict=True))
    pairs.sort(key=lambda row: row[0])
    return pairs


def test_partitioned_dv_live_repark_matches_spark(tmp_path: Path) -> None:
    """Partitioned-DV fixture live rows repark == pinned golden and prunes match Spark.

    pins: v3e-5-nightly-v3-oracle/C-003
    """
    from repark import ReparkSession

    expected = [(1, "a", 0), (3, "c", 0), (4, "d", 1), (6, "f", 1)]
    session = ReparkSession.builder.appName("v3e-5-part-dv-live").getOrCreate()
    try:
        session.register_memory_catalog("ice", tmp_path)
        session.sql("CREATE NAMESPACE ice.sales")
        with _materialize(_PART_DV_SRC, _PART_DV_DEST) as metadata_file:
            session.sql(
                "CALL ice.system.register_table("
                f"table => 'sales.partdv', metadata_file => '{metadata_file}')"
            )
            live = session.sql("SELECT id, name, part FROM ice.sales.partdv").to_arrow()
            assert _id_name_part_rows(live) == expected
            pruned0 = session.sql("SELECT id, name FROM ice.sales.partdv WHERE part = 0").to_arrow()
            assert _id_name_rows(pruned0) == [(1, "a"), (3, "c")]
            pruned1 = session.sql("SELECT id, name FROM ice.sales.partdv WHERE part = 1").to_arrow()
            assert _id_name_rows(pruned1) == [(4, "d"), (6, "f")]
            if not _LIVE:
                pytest.skip(_LIVE_SKIP)
            _assert_partitioned_dv_live_against_spark(metadata_file, expected)
    finally:
        session.stop()


def _assert_partitioned_dv_live_against_spark(
    metadata_file: str, expected: list[tuple[int, str, int]]
) -> None:
    """Live Spark re-derivation for the partitioned-DV fixture.

    pins: v3e-5-nightly-v3-oracle/C-003
    """
    import tempfile

    import _live_parity as live_parity
    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV

    catalog = live_parity.LIFECYCLE_SPARK_CATALOG
    warehouse = Path(tempfile.mkdtemp(prefix="repark-v3e-5-live-part-"))
    try:
        engine = live_parity.build_spark_iceberg_engine(warehouse)
        try:
            engine.session.sql("CREATE NAMESPACE IF NOT EXISTS local.sales")
            engine.session.sql(
                f"CALL {catalog}.system.register_table(table => 'sales.partdv', "
                f"metadata_file => '{metadata_file}')"
            )
            spark_rows = engine.session.sql(
                "SELECT id, name, part FROM local.sales.partdv ORDER BY id"
            ).toArrow()
            assert _id_name_part_rows(spark_rows) == expected
            prune0 = engine.session.sql(
                "SELECT id, name FROM local.sales.partdv WHERE part = 0 ORDER BY id"
            ).toArrow()
            assert _id_name_rows(prune0) == [(1, "a"), (3, "c")]
            prune1 = engine.session.sql(
                "SELECT id, name FROM local.sales.partdv WHERE part = 1 ORDER BY id"
            ).toArrow()
            assert _id_name_rows(prune1) == [(4, "d"), (6, "f")]
        finally:
            engine.session.stop()
    finally:
        shutil.rmtree(warehouse, ignore_errors=True)
    assert ICEBERG_SPARK_RUNTIME_GAV == "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"


def test_equality_delete_live_repark_matches_spark(tmp_path: Path) -> None:
    """Equality-delete alongside DV live rows repark == Spark.

    pins: v3e-5-nightly-v3-oracle/C-004
    """
    from repark import ReparkSession

    expected = [(2, "b", 0), (3, "c", 1)]
    session = ReparkSession.builder.appName("v3e-5-eq-dv-live").getOrCreate()
    try:
        session.register_memory_catalog("ice", tmp_path)
        session.sql("CREATE NAMESPACE ice.sales")
        with _materialize(_EQ_DV_SRC, _EQ_DV_DEST) as metadata_file:
            session.sql(
                "CALL ice.system.register_table("
                f"table => 'sales.eqdv', metadata_file => '{metadata_file}')"
            )
            live = session.sql("SELECT id, name, part FROM ice.sales.eqdv").to_arrow()
            assert _id_name_part_rows(live) == expected
            if not _LIVE:
                pytest.skip(_LIVE_SKIP)
            _assert_equality_delete_live_against_spark(metadata_file, expected)
    finally:
        session.stop()


def _assert_equality_delete_live_against_spark(
    metadata_file: str, expected: list[tuple[int, str, int]]
) -> None:
    """Live Spark re-derivation for the equality-delete fixture.

    pins: v3e-5-nightly-v3-oracle/C-004
    """
    import tempfile

    import _live_parity as live_parity
    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV

    catalog = live_parity.LIFECYCLE_SPARK_CATALOG
    warehouse = Path(tempfile.mkdtemp(prefix="repark-v3e-5-live-eq-"))
    try:
        engine = live_parity.build_spark_iceberg_engine(warehouse)
        try:
            engine.session.sql("CREATE NAMESPACE IF NOT EXISTS local.sales")
            engine.session.sql(
                f"CALL {catalog}.system.register_table(table => 'sales.eqdv', "
                f"metadata_file => '{metadata_file}')"
            )
            spark_rows = engine.session.sql(
                "SELECT id, name, part FROM local.sales.eqdv ORDER BY id"
            ).toArrow()
            assert _id_name_part_rows(spark_rows) == expected
        finally:
            engine.session.stop()
    finally:
        shutil.rmtree(warehouse, ignore_errors=True)
    assert ICEBERG_SPARK_RUNTIME_GAV == "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"


def test_delete_files_live_kinds_match_spark(tmp_path: Path) -> None:
    """Delete-files kinds live repark == Spark: content 1 PUFFIN and 2 PARQUET equality_ids=[1].

    pins: v3e-5-nightly-v3-oracle/C-005
    """
    from repark import ReparkSession

    session = ReparkSession.builder.appName("v3e-5-delete-files-live").getOrCreate()
    try:
        session.register_memory_catalog("ice", tmp_path)
        session.sql("CREATE NAMESPACE ice.sales")
        with (
            _materialize(_PART_DV_SRC, _PART_DV_DEST) as part_meta,
            _materialize(_EQ_DV_SRC, _EQ_DV_DEST) as eq_meta,
        ):
            session.sql(
                "CALL ice.system.register_table("
                f"table => 'sales.partdv', metadata_file => '{part_meta}')"
            )
            part_deletes = session.sql(
                "SELECT content, file_format, record_count, equality_ids "
                "FROM ice.sales.partdv.delete_files"
            ).to_arrow()
            part_contents = part_deletes.column("content").to_pylist()
            assert set(part_contents) == {1}, part_contents
            assert all(
                str(v).upper() == "PUFFIN" for v in part_deletes.column("file_format").to_pylist()
            ), part_deletes.column("file_format").to_pylist()
            assert all(v in (None, []) for v in part_deletes.column("equality_ids").to_pylist()), (
                part_deletes.column("equality_ids").to_pylist()
            )
            session.sql(
                "CALL ice.system.register_table("
                f"table => 'sales.eqdv2', metadata_file => '{eq_meta}')"
            )
            eq_deletes = session.sql(
                "SELECT content, file_format, record_count, equality_ids "
                "FROM ice.sales.eqdv2.delete_files"
            ).to_arrow()
            eq_contents = eq_deletes.column("content").to_pylist()
            assert set(eq_contents) == {1, 2}, eq_contents
            assert len(eq_deletes) == 2
            by_content = {int(row["content"]): row for row in eq_deletes.to_pylist()}
            assert str(by_content[1]["file_format"]).upper() == "PUFFIN"
            assert str(by_content[2]["file_format"]).upper() == "PARQUET"
            assert list(by_content[2]["equality_ids"]) == [1]
            assert by_content[1]["equality_ids"] in (None, [])
            if not _LIVE:
                pytest.skip(_LIVE_SKIP)
            _assert_delete_files_live_against_spark(part_meta, eq_meta)
    finally:
        session.stop()


def _assert_delete_files_live_against_spark(part_meta: str, eq_meta: str) -> None:
    """Live Spark delete_files kinds for both fixtures.

    pins: v3e-5-nightly-v3-oracle/C-005
    """
    import tempfile

    import _live_parity as live_parity
    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV

    catalog = live_parity.LIFECYCLE_SPARK_CATALOG
    warehouse = Path(tempfile.mkdtemp(prefix="repark-v3e-5-live-del-"))
    try:
        engine = live_parity.build_spark_iceberg_engine(warehouse)
        try:
            engine.session.sql("CREATE NAMESPACE IF NOT EXISTS local.sales")
            engine.session.sql(
                f"CALL {catalog}.system.register_table(table => 'sales.partdv', "
                f"metadata_file => '{part_meta}')"
            )
            part = engine.session.sql(
                "SELECT content, file_format, equality_ids FROM local.sales.partdv.delete_files"
            ).toArrow()
            assert set(part.column("content").to_pylist()) == {1}
            assert all(
                str(v).upper() == "PUFFIN" for v in part.column("file_format").to_pylist()
            ), part.column("file_format").to_pylist()
            assert all(v in (None, []) for v in part.column("equality_ids").to_pylist()), (
                part.column("equality_ids").to_pylist()
            )
            engine.session.sql(
                f"CALL {catalog}.system.register_table(table => 'sales.eqdv2', "
                f"metadata_file => '{eq_meta}')"
            )
            eq_del = engine.session.sql(
                "SELECT content, file_format, equality_ids FROM local.sales.eqdv2.delete_files"
            ).toArrow()
            contents = eq_del.column("content").to_pylist()
            assert set(contents) == {1, 2}, contents
            assert len(eq_del) == 2
            by_content = {int(row["content"]): row for row in eq_del.to_pylist()}
            assert str(by_content[1]["file_format"]).upper() == "PUFFIN"
            assert str(by_content[2]["file_format"]).upper() == "PARQUET"
            assert list(by_content[2]["equality_ids"]) == [1]
            assert by_content[1]["equality_ids"] in (None, [])
        finally:
            engine.session.stop()
    finally:
        shutil.rmtree(warehouse, ignore_errors=True)
    assert ICEBERG_SPARK_RUNTIME_GAV == "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"


def test_partitioned_dv_update_commits_and_rewrite_still_refuses(tmp_path: Path) -> None:
    """Partitioned-DV UPDATE keeps `_row_id` and bumps seq; rewrite_position_delete_files refuses.

    pins: v3e-5-nightly-v3-oracle/C-008
    pins: rp-3-fork-repin/C-004, C-011
    pins: rp-6-fork-repin/C-003
    """
    from repark import ReparkSession
    from repark.errors import UnsupportedOperationException

    session = ReparkSession.builder.appName("v3e-5-bmor3-control").getOrCreate()
    try:
        session.register_memory_catalog("ice", tmp_path)
        session.sql("CREATE NAMESPACE ice.sales")
        with _materialize(_PART_DV_SRC, _PART_DV_DEST) as metadata_file:
            session.sql(
                "CALL ice.system.register_table("
                f"table => 'sales.partdv', metadata_file => '{metadata_file}')"
            )
            before_objects = _objects_under(_PART_DV_DEST)
            session.sql("UPDATE ice.sales.partdv SET name = 'x' WHERE id = 1").collect()
            after_update = session.sql("SELECT id, name, part FROM ice.sales.partdv").to_arrow()
            updated_rows = _id_name_part_rows(after_update)
            assert (1, "x", 0) in updated_rows
            lineage = session.sql(
                "SELECT id, _row_id, _last_updated_sequence_number "
                "FROM ice.sales.partdv ORDER BY id"
            ).to_arrow()
            assert list(
                zip(
                    lineage.column("id").to_pylist(),
                    lineage.column("_row_id").to_pylist(),
                    lineage.column("_last_updated_sequence_number").to_pylist(),
                    strict=True,
                )
            ) == [(1, 0, 3), (3, 2, 1), (4, 3, 1), (6, 5, 1)]
            after_update_objects = _objects_under(_PART_DV_DEST)
            assert after_update_objects != before_objects
            with pytest.raises(UnsupportedOperationException, match="Puffin deletion vector"):
                session.sql(
                    "CALL ice.system.rewrite_position_delete_files(table => 'sales.partdv')"
                ).collect()
            refused = session.sql("SELECT id, name, part FROM ice.sales.partdv").to_arrow()
            assert _id_name_part_rows(refused) == updated_rows
            assert _objects_under(_PART_DV_DEST) == after_update_objects
            session.sql("DELETE FROM ice.sales.partdv WHERE id = 1").collect()
            live = session.sql("SELECT id, name, part FROM ice.sales.partdv").to_arrow()
            assert _id_name_part_rows(live) == [(3, "c", 0), (4, "d", 1), (6, "f", 1)]
    finally:
        session.stop()


def test_v3_merge_matched_update_live_cow_and_mor(tmp_path: Path) -> None:
    """COW and MoR matched-UPDATE MERGE lineage matches the in-repo V3-7 ledger transcript.

    pins: v3-7-merge-lineage/C-002
    """
    from repark import ReparkSession

    ledger = _ledger_path(_V37_LEDGER_NAME).read_text(encoding="utf-8")
    assert "/tmp/v3-7-oracle" not in ledger
    assert "| COW matched-UPDATE |" in ledger
    assert "| MoR matched-UPDATE |" in ledger
    assert "(1,a,0,1),(2,m,1,2),(3,c,2,1)" in ledger
    session = (
        ReparkSession.builder.appName("v3-7-merge-live")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
    try:
        session.register_memory_catalog("ice", tmp_path)
        session.sql("CREATE NAMESPACE ice.sales")
        for table, props in (("cow_mu", _COW_V3), ("mor_mu", _MOR_V3)):
            session.sql(
                f"CREATE TABLE ice.sales.{table} (id INT, name STRING) USING iceberg "
                f"TBLPROPERTIES ({props})"
            )
            session.sql(f"INSERT INTO ice.sales.{table} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            session.sql(_merge_matched_update_sql(f"ice.sales.{table}")).collect()
            lineage = session.sql(
                "SELECT id, _row_id, _last_updated_sequence_number "
                f"FROM ice.sales.{table} ORDER BY id"
            ).to_arrow()
            assert _id_row_id_seq(lineage) == _MATCHED_UPDATE_LINEAGE, table
        if not _LIVE:
            pytest.skip(_LIVE_SKIP)
        _assert_merge_matched_update_live_against_spark()
    finally:
        session.stop()


def _v37_iceberg_runtime_jar() -> str | None:
    """Local Iceberg Spark runtime JAR when Ivy cannot write the default cache."""
    candidates = (
        os.environ.get("V37_ICEBERG_RUNTIME_JAR"),
        "/tmp/rp6-oracle/iceberg-spark-runtime-4.1_2.13-1.11.0.jar",
        "/tmp/iceberg-spark-runtime-4.1_2.13-1.11.0.jar",
    )
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return candidate
    return None


def _assert_merge_matched_update_live_against_spark() -> None:
    """Live Spark COW and MoR matched-UPDATE MERGE at the V3-7 single-file seed.

    pins: v3-7-merge-lineage/C-002
    """
    import tempfile

    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV
    from pyspark.sql import SparkSession
    from pyspark.sql import types as spark_types

    catalog = "local"
    warehouse = Path(tempfile.mkdtemp(prefix="repark-v3-7-live-mrg-"))
    ivy_home = Path(tempfile.mkdtemp(prefix="repark-v3-7-ivy-"))
    schema = spark_types.StructType(
        [
            spark_types.StructField("id", spark_types.IntegerType(), False),
            spark_types.StructField("name", spark_types.StringType(), False),
        ]
    )
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("v3-7-merge-live")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.ivy", str(ivy_home))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", str(warehouse))
    )
    jar = _v37_iceberg_runtime_jar()
    if jar is not None:
        os.environ.pop("PYSPARK_SUBMIT_ARGS", None)
        builder = builder.config("spark.jars", jar)
    else:
        builder = builder.config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    try:
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.sales")
        for table, props in (("cow_mu", _COW_V3), ("mor_mu", _MOR_V3)):
            target = f"{catalog}.sales.{table}"
            session.sql(
                f"CREATE TABLE {target} (id INT, name STRING) USING iceberg TBLPROPERTIES ({props})"
            )
            frame = session.createDataFrame([(1, "a"), (2, "b"), (3, "c")], schema)
            frame.coalesce(1).writeTo(target).append()
            session.sql(_merge_matched_update_sql(target))
            spark_rows = session.sql(
                f"SELECT id, _row_id, _last_updated_sequence_number FROM {target} ORDER BY id"
            ).toArrow()
            assert _id_row_id_seq(spark_rows) == _MATCHED_UPDATE_LINEAGE, table
    finally:
        session.stop()
        shutil.rmtree(warehouse, ignore_errors=True)
        shutil.rmtree(ivy_home, ignore_errors=True)
    assert ICEBERG_SPARK_RUNTIME_GAV == "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"


_SUBQUERY_DELETE_LINEAGE = [(1, 0, 1), (3, 2, 1)]
_SUBQUERY_UPDATE_LINEAGE = [(1, 0, 1), (2, 1, 2), (3, 2, 1)]


def _subquery_delete_sql(target: str, source: str) -> str:
    """Allow-listed subquery-WHERE DELETE at the V3-8 seed.

    pins: v3-8-subquery-where-lineage/C-002
    """
    return f"DELETE FROM {target} WHERE id IN (SELECT id FROM {source})"


def _subquery_update_sql(target: str, source: str) -> str:
    """Allow-listed subquery-WHERE UPDATE at the V3-8 seed.

    pins: v3-8-subquery-where-lineage/C-002
    """
    return f"UPDATE {target} SET name = 'm' WHERE id IN (SELECT id FROM {source})"


def test_v3_subquery_where_dml_live_cow(tmp_path: Path) -> None:
    """COW subquery-WHERE DELETE and UPDATE lineage matches the in-repo V3-8 transcript.

    pins: v3-8-subquery-where-lineage/C-001, C-002, C-003
    """
    from repark import ReparkSession

    ledger = _ledger_path(_V38_LEDGER_NAME).read_text(encoding="utf-8")
    assert "/tmp/v3-8-oracle" not in ledger
    assert "| COW DELETE … IN |" in ledger
    assert "| COW UPDATE … IN |" in ledger
    assert "(1,a,0,1),(3,c,2,1)" in ledger
    assert "(1,a,0,1),(2,m,1,2),(3,c,2,1)" in ledger
    session = (
        ReparkSession.builder.appName("v3-8-subquery-live")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
    try:
        session.register_memory_catalog("ice", tmp_path)
        session.sql("CREATE NAMESPACE ice.sales")
        session.sql("CREATE TABLE ice.sales.srcids (id INT) USING iceberg")
        session.sql("INSERT INTO ice.sales.srcids VALUES (2)")
        for table, statement, expected in (
            ("cow_sd", _subquery_delete_sql, _SUBQUERY_DELETE_LINEAGE),
            ("cow_su", _subquery_update_sql, _SUBQUERY_UPDATE_LINEAGE),
        ):
            session.sql(
                f"CREATE TABLE ice.sales.{table} (id INT, name STRING) USING iceberg "
                f"TBLPROPERTIES ({_COW_V3})"
            )
            session.sql(f"INSERT INTO ice.sales.{table} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            session.sql(statement(f"ice.sales.{table}", "ice.sales.srcids")).collect()
            lineage = session.sql(
                "SELECT id, _row_id, _last_updated_sequence_number "
                f"FROM ice.sales.{table} ORDER BY id"
            ).to_arrow()
            assert _id_row_id_seq(lineage) == expected, table
        if not _LIVE:
            pytest.skip(_LIVE_SKIP)
        _assert_subquery_where_dml_live_against_spark()
    finally:
        session.stop()


def _assert_subquery_where_dml_live_against_spark() -> None:
    """Live Spark COW subquery-WHERE DELETE and UPDATE at the V3-8 single-file seed.

    pins: v3-8-subquery-where-lineage/C-002
    """
    import tempfile

    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV
    from pyspark.sql import SparkSession
    from pyspark.sql import types as spark_types

    catalog = "local"
    warehouse = Path(tempfile.mkdtemp(prefix="repark-v3-8-live-sub-"))
    ivy_home = Path(tempfile.mkdtemp(prefix="repark-v3-8-ivy-"))
    schema = spark_types.StructType(
        [
            spark_types.StructField("id", spark_types.IntegerType(), False),
            spark_types.StructField("name", spark_types.StringType(), False),
        ]
    )
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("v3-8-subquery-live")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.ivy", str(ivy_home))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", str(warehouse))
    )
    jar = _v37_iceberg_runtime_jar()
    if jar is not None:
        os.environ.pop("PYSPARK_SUBMIT_ARGS", None)
        builder = builder.config("spark.jars", jar)
    else:
        builder = builder.config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    try:
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.sales")
        source = f"{catalog}.sales.srcids"
        session.sql(f"CREATE TABLE {source} (id INT) USING iceberg")
        source_schema = spark_types.StructType(
            [spark_types.StructField("id", spark_types.IntegerType(), False)]
        )
        session.createDataFrame([(2,)], source_schema).coalesce(1).writeTo(source).append()
        for table, statement, expected in (
            ("cow_sd", _subquery_delete_sql, _SUBQUERY_DELETE_LINEAGE),
            ("cow_su", _subquery_update_sql, _SUBQUERY_UPDATE_LINEAGE),
        ):
            target = f"{catalog}.sales.{table}"
            session.sql(
                f"CREATE TABLE {target} (id INT, name STRING) USING iceberg "
                f"TBLPROPERTIES ({_COW_V3})"
            )
            frame = session.createDataFrame([(1, "a"), (2, "b"), (3, "c")], schema)
            frame.coalesce(1).writeTo(target).append()
            session.sql(statement(target, source))
            spark_rows = session.sql(
                f"SELECT id, _row_id, _last_updated_sequence_number FROM {target} ORDER BY id"
            ).toArrow()
            assert _id_row_id_seq(spark_rows) == expected, table
    finally:
        session.stop()
        shutil.rmtree(warehouse, ignore_errors=True)
        shutil.rmtree(ivy_home, ignore_errors=True)


def test_northstar_nightly_v3_leg_is_v3e_5() -> None:
    """Northstar nightly row cites V3E-5 as the live leg.

    pins: v3e-5-nightly-v3-oracle/C-009
    """
    northstar = _REPO_ROOT / "task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md"
    text = northstar.read_text(encoding="utf-8")
    row = next(line for line in text.splitlines() if line.startswith("| Nightly oracle: v3 leg |"))
    assert "V3E-5 (2026-08-27)" in row
    assert "PySpark 4.1.2 + Iceberg 1.11.0" in row
    assert "❌ none" not in row


def test_v3_live_oracle_pins_cover_all_clauses() -> None:
    """Meta-pin that every PROVEN clause in the ledger has a pins cite.

    pins: v3e-5-nightly-v3-oracle/C-001, C-002, C-006, C-009, C-010, C-011, C-012
    """
    import subprocess

    text = _ledger_path("v3e-5-nightly-v3-oracle-ledger.md").read_text(encoding="utf-8")
    assert "VERDICT: PASS" in text
    assert "Nightly oracle: v3 leg" in (
        _REPO_ROOT / "task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md"
    ).read_text(encoding="utf-8")
    assert (
        _REPO_ROOT / "task/ledgers/archive/2026-08/2026-08-27-production-file-size-ledger.md"
    ).is_file()
    assert (
        _REPO_ROOT / "task/ledgers/archive/2026-08/2026-08-27-rust-catalog-registration-ledger.md"
    ).is_file()
    dual = subprocess.run(
        ["python3", "scripts/check_parity_live_dual_wire.py"],
        capture_output=True,
        text=True,
        cwd=str(_REPO_ROOT),
    )
    assert dual.returncode == 0, dual.stderr
    landed_diff = subprocess.run(
        [
            "git",
            "show",
            "--format=",
            "--name-only",
            "ecbd6a4162a365f96e216a856becda6f1876956b",
        ],
        capture_output=True,
        text=True,
        cwd=str(_REPO_ROOT),
    )
    if landed_diff.returncode != 0:
        pytest.skip("V3E-5 landing commit not available in wheel test environment")
    allowed_prefixes = (
        ".github/workflows/",
        "python/repark/tests/",
        "task/ledgers/",
        "task/roadmap/",
    )
    for line in landed_diff.stdout.splitlines():
        if not line.strip():
            continue
        assert line.startswith(allowed_prefixes) or line in (
            "task/ledgers/archive/2026-08/map.md",
            "task/ledgers/archive/map.md",
            "task/ledgers/completed/map.md",
            "Cargo.lock",
            "deny.toml",
        ), f"unexpected diff path {line!r} for C-010"
