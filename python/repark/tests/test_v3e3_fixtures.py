"""V3E-3: facade reads of Spark-written partitioned v3 DV and equality-delete fixtures.

pins: v3e-3-partitioned-eqdel-fixtures/C-009, C-010
pins: rp-3-fork-repin/C-004, C-007, C-011
"""

from __future__ import annotations

import shutil
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from pathlib import Path

import pyarrow as pa
import pytest

from repark.errors import UnsupportedOperationException

_REPO_ROOT = Path(__file__).resolve().parents[3]
_PART_DV_SRC = _REPO_ROOT / "crates/repark-spark/src/tests/fixtures/v3-spark-part-dv"
_EQ_DV_SRC = _REPO_ROOT / "crates/repark-spark/src/tests/fixtures/v3-spark-eq-dv"
_PART_DV_DEST = Path("/tmp/repark-v3e3-partdv/ns/v3part")
_EQ_DV_DEST = Path("/tmp/repark-v3e3-eqdel/ns/v3eq")


class _DirLock:
    """Cross-process lock so facade tests do not clobber the Rust fixture copies."""

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
                    raise TimeoutError(
                        f"fixture lock {path} held for 2 minutes (no steal)"
                    ) from None
                time.sleep(0.025)

    def close(self) -> None:
        with suppress(OSError):
            self.path.rmdir()


@contextmanager
def _materialize(src: Path, dest: Path) -> Iterator[str]:
    lock = _DirLock(Path(str(dest) + ".lock"))
    try:
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(src, dest)
        metadata = dest / "metadata"
        versions = sorted(metadata.glob("v*.metadata.json"), key=lambda path: path.name)
        assert versions, f"no Hadoop metadata under {metadata}"
        yield str(versions[-1])
    finally:
        lock.close()


def _id_name_rows(table: pa.Table) -> list[tuple[int, str]]:
    assert table.schema.field("id").type == pa.int32(), table.schema
    assert table.schema.field("name").type == pa.string(), table.schema
    pairs = list(zip(table.column("id").to_pylist(), table.column("name").to_pylist(), strict=True))
    pairs.sort(key=lambda row: row[0])
    return pairs


def _id_name_part_rows(table: pa.Table) -> list[tuple[int, str, int]]:
    assert table.schema.field("id").type == pa.int32(), table.schema
    assert table.schema.field("name").type == pa.string(), table.schema
    assert table.schema.field("part").type == pa.int32(), table.schema
    ids = table.column("id").to_pylist()
    names = table.column("name").to_pylist()
    parts = table.column("part").to_pylist()
    rows = list(zip(ids, names, parts, strict=True))
    rows.sort(key=lambda row: row[0])
    return rows


def test_facade_partitioned_v3_dv_matches_spark_live_rows(tmp_path: Path) -> None:
    """Adopted partitioned v3 DV fixture matches Spark's four live rows."""
    from repark import ReparkSession

    spark = ReparkSession.builder.appName("v3e-3-part-dv").getOrCreate()
    try:
        spark.register_memory_catalog("ice", tmp_path)
        spark.sql("CREATE NAMESPACE ice.sales")
        with _materialize(_PART_DV_SRC, _PART_DV_DEST) as metadata_file:
            spark.sql(
                "CALL ice.system.register_table("
                f"table => 'sales.partdv', metadata_file => '{metadata_file}')"
            )
            live = spark.sql("SELECT id, name, part FROM ice.sales.partdv").to_arrow()
            assert _id_name_part_rows(live) == [
                (1, "a", 0),
                (3, "c", 0),
                (4, "d", 1),
                (6, "f", 1),
            ]
            pruned0 = spark.sql("SELECT id, name FROM ice.sales.partdv WHERE part = 0").to_arrow()
            assert _id_name_rows(pruned0) == [(1, "a"), (3, "c")]
            pruned1 = spark.sql("SELECT id, name FROM ice.sales.partdv WHERE part = 1").to_arrow()
            assert _id_name_rows(pruned1) == [(4, "d"), (6, "f")]
            with pytest.raises(UnsupportedOperationException, match="Puffin deletion vector"):
                spark.sql(
                    "CALL ice.system.rewrite_position_delete_files(table => 'sales.partdv')"
                ).collect()
    finally:
        spark.stop()


def test_facade_partitioned_v3_dv_delete_merges_into_one_file(tmp_path: Path) -> None:
    """A facade DELETE merges positions into the Spark-written partition-zero DV."""
    from repark import ReparkSession

    spark = ReparkSession.builder.appName("rp-3-part-dv-one").getOrCreate()
    try:
        spark.register_memory_catalog("ice", tmp_path)
        spark.sql("CREATE NAMESPACE ice.sales")
        with _materialize(_PART_DV_SRC, _PART_DV_DEST) as metadata_file:
            spark.sql(
                "CALL ice.system.register_table("
                f"table => 'sales.partdv', metadata_file => '{metadata_file}')"
            )
            spark.sql("DELETE FROM ice.sales.partdv WHERE id = 3").collect()
            live = spark.sql("SELECT id, name, part FROM ice.sales.partdv").to_arrow()
            assert _id_name_part_rows(live) == [(1, "a", 0), (4, "d", 1), (6, "f", 1)]
            deletes = spark.sql(
                "SELECT content, file_format, record_count FROM ice.sales.partdv.delete_files"
            ).to_arrow()
            assert len(deletes) == 2
            assert all(int(row["content"]) == 1 for row in deletes.to_pylist())
            assert all(str(row["file_format"]).upper() == "PUFFIN" for row in deletes.to_pylist())
    finally:
        spark.stop()


def test_facade_partitioned_v3_dv_delete_across_files_keeps_partitions(tmp_path: Path) -> None:
    """A facade DELETE across partitions leaves one Puffin DV for each data file."""
    from repark import ReparkSession

    spark = ReparkSession.builder.appName("rp-3-part-dv-all").getOrCreate()
    try:
        spark.register_memory_catalog("ice", tmp_path)
        spark.sql("CREATE NAMESPACE ice.sales")
        with _materialize(_PART_DV_SRC, _PART_DV_DEST) as metadata_file:
            spark.sql(
                "CALL ice.system.register_table("
                f"table => 'sales.partdv', metadata_file => '{metadata_file}')"
            )
            spark.sql("DELETE FROM ice.sales.partdv WHERE id IN (1, 4)").collect()
            live = spark.sql("SELECT id, name, part FROM ice.sales.partdv").to_arrow()
            assert _id_name_part_rows(live) == [(3, "c", 0), (6, "f", 1)]
            deletes = spark.sql(
                "SELECT content, file_format, record_count FROM ice.sales.partdv.delete_files"
            ).to_arrow()
            assert len(deletes) == 2
            assert all(int(row["content"]) == 1 for row in deletes.to_pylist())
            assert all(str(row["file_format"]).upper() == "PUFFIN" for row in deletes.to_pylist())
            assert {int(row["record_count"]) for row in deletes.to_pylist()} == {2}
    finally:
        spark.stop()


def test_facade_equality_delete_alongside_dv_matches_spark(tmp_path: Path) -> None:
    """Adopted v3 table with a Puffin DV and an equality-delete matches Spark."""
    from repark import ReparkSession

    spark = ReparkSession.builder.appName("v3e-3-eq-dv").getOrCreate()
    try:
        spark.register_memory_catalog("ice", tmp_path)
        spark.sql("CREATE NAMESPACE ice.sales")
        with _materialize(_EQ_DV_SRC, _EQ_DV_DEST) as metadata_file:
            spark.sql(
                "CALL ice.system.register_table("
                f"table => 'sales.eqdv', metadata_file => '{metadata_file}')"
            )
            live = spark.sql("SELECT id, name, part FROM ice.sales.eqdv").to_arrow()
            assert _id_name_part_rows(live) == [(2, "b", 0), (3, "c", 1)]
            deletes = spark.sql(
                "SELECT content, file_format, record_count, equality_ids "
                "FROM ice.sales.eqdv.delete_files"
            ).to_arrow()
            contents = deletes.column("content").to_pylist()
            assert 1 in contents and 2 in contents, contents
            by_content = {int(row["content"]): row for row in deletes.to_pylist()}
            assert str(by_content[1]["file_format"]).upper() == "PUFFIN"
            assert by_content[1]["equality_ids"] in (None, [])
            assert str(by_content[2]["file_format"]).upper() == "PARQUET"
            eq_ids = by_content[2]["equality_ids"]
            assert list(eq_ids) == [1], eq_ids
    finally:
        spark.stop()


def test_facade_equality_delete_and_dv_keep_both_delete_classes_after_delete(
    tmp_path: Path,
) -> None:
    """A facade DELETE retains both the Puffin DV and the Parquet equality delete."""
    from repark import ReparkSession

    spark = ReparkSession.builder.appName("rp-3-eq-dv").getOrCreate()
    try:
        spark.register_memory_catalog("ice", tmp_path)
        spark.sql("CREATE NAMESPACE ice.sales")
        with _materialize(_EQ_DV_SRC, _EQ_DV_DEST) as metadata_file:
            spark.sql(
                "CALL ice.system.register_table("
                f"table => 'sales.eqdv', metadata_file => '{metadata_file}')"
            )
            spark.sql("DELETE FROM ice.sales.eqdv WHERE id = 2").collect()
            live = spark.sql("SELECT id, name, part FROM ice.sales.eqdv").to_arrow()
            assert _id_name_part_rows(live) == [(3, "c", 1)]
            deletes = spark.sql(
                "SELECT content, file_format, record_count, equality_ids "
                "FROM ice.sales.eqdv.delete_files"
            ).to_arrow()
            assert len(deletes) == 2
            by_content = {int(row["content"]): row for row in deletes.to_pylist()}
            assert str(by_content[1]["file_format"]).upper() == "PUFFIN"
            assert int(by_content[1]["record_count"]) == 2
            assert by_content[1]["equality_ids"] in (None, [])
            assert str(by_content[2]["file_format"]).upper() == "PARQUET"
            assert int(by_content[2]["record_count"]) == 1
            assert list(by_content[2]["equality_ids"]) == [1]
    finally:
        spark.stop()
