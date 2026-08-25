"""V3E-4: facade refs, time travel over DVs, expire/orphans on format-v3.

pins: v3e-4-refs-time-travel/C-012, C-008, C-009, C-011, C-014
"""

from __future__ import annotations

import shutil
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from pathlib import Path

import pyarrow as pa
import pytest

from repark.errors import AnalysisException, UnsupportedOperationException

_REPO_ROOT = Path(__file__).resolve().parents[3]
_PART_DV_SRC = _REPO_ROOT / "crates/repark-spark/src/tests/fixtures/v3-spark-part-dv"
_PART_DV_DEST = Path("/tmp/repark-v3e3-partdv/ns/v3part")
_DV_LIVE = [(1, "a", 0), (3, "c", 0), (4, "d", 1), (6, "f", 1)]
_UUID_METADATA = "3-aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee.metadata.json"


class _DirLock:
    """Cross-process lock so facade tests do not clobber the Rust fixture copies."""

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        while True:
            try:
                path.mkdir()
                return
            except FileExistsError:
                if time.monotonic() - started > 120:
                    raise TimeoutError(f"fixture lock {path} held for 2 minutes") from None
                time.sleep(0.025)

    def close(self) -> None:
        with suppress(OSError):
            self.path.rmdir()


@contextmanager
def _materialize_writable(src: Path, dest: Path) -> Iterator[str]:
    """Copy the Hadoop fixture and adopt a version-uuid pointer (V3-ADOPT-1)."""
    lock = _DirLock(Path(str(dest) + ".lock"))
    try:
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(src, dest)
        hadoop = dest / "metadata" / "v3.metadata.json"
        rewritten = dest / "metadata" / _UUID_METADATA
        shutil.copy(hadoop, rewritten)
        yield str(rewritten)
    finally:
        lock.close()


def _id_name_part_rows(table: pa.Table) -> list[tuple[int, str, int]]:
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


def _current_snapshot_id(spark: object, table: str) -> int:
    snaps = spark._testing_list_snapshots(table)
    assert snaps, table
    return int(snaps[-1][0])


def test_facade_v3_refs_time_travel_expire_orphan(tmp_path: Path) -> None:
    """Branch/tag, VERSION AS OF over DVs, rollback, expire dual-probe, orphan floor."""
    from repark import ReparkSession

    spark = ReparkSession.builder.appName("v3e-4-refs").getOrCreate()
    try:
        spark.register_memory_catalog("ice", tmp_path)
        spark.sql("CREATE NAMESPACE ice.sales")
        with _materialize_writable(_PART_DV_SRC, _PART_DV_DEST) as metadata_file:
            spark.sql(
                "CALL ice.system.register_table("
                f"table => 'sales.partdv', metadata_file => '{metadata_file}')"
            )
            table = "ice.sales.partdv"
            s_dv = _current_snapshot_id(spark, table)
            assert (
                _id_name_part_rows(spark.sql(f"SELECT id, name, part FROM {table}").to_arrow())
                == _DV_LIVE
            )

            spark.sql(f"INSERT INTO {table} SELECT 7 AS id, 'g' AS name, 0 AS part")
            s_mid = _current_snapshot_id(spark, table)
            assert s_mid != s_dv
            spark.sql(f"INSERT INTO {table} SELECT 8 AS id, 'h' AS name, 1 AS part")
            s_head = _current_snapshot_id(spark, table)
            assert s_head != s_mid

            spark.sql(f"ALTER TABLE {table} CREATE BRANCH audit")
            spark.sql(f"ALTER TABLE {table} CREATE TAG keep_dv AS OF VERSION {s_dv}")

            at_dv = spark.sql(f"SELECT id, name, part FROM {table} VERSION AS OF {s_dv}").to_arrow()
            assert _id_name_part_rows(at_dv) == _DV_LIVE
            at_tag = spark.sql(
                f"SELECT id, name, part FROM {table} VERSION AS OF 'keep_dv'"
            ).to_arrow()
            assert _id_name_part_rows(at_tag) == _DV_LIVE

            spark.sql(
                "CALL ice.system.rollback_to_snapshot("
                f"table => 'sales.partdv', snapshot_id => {s_dv})"
            )
            assert _current_snapshot_id(spark, table) == s_dv
            assert (
                _id_name_part_rows(spark.sql(f"SELECT id, name, part FROM {table}").to_arrow())
                == _DV_LIVE
            )

            spark.sql(f"INSERT INTO {table} SELECT 7 AS id, 'g' AS name, 0 AS part")
            spark.sql(f"INSERT INTO {table} SELECT 8 AS id, 'h' AS name, 1 AS part")
            s_head = _current_snapshot_id(spark, table)
            older_than_ms = int(time.time() * 1000) + 86_400_000
            expired = spark.sql(
                "CALL ice.system.expire_snapshots("
                f"table => 'sales.partdv', older_than => {older_than_ms}, retain_last => 1)"
            ).to_arrow()
            assert list(expired.schema.names) == [
                "deleted_data_files_count",
                "deleted_position_delete_files_count",
                "deleted_equality_delete_files_count",
                "deleted_manifest_files_count",
                "deleted_manifest_lists_count",
                "deleted_statistics_files_count",
            ]
            tagged = spark.sql(
                f"SELECT id, name, part FROM {table} VERSION AS OF {s_dv}"
            ).to_arrow()
            assert _id_name_part_rows(tagged) == _DV_LIVE

            planted = _PART_DV_DEST / "data" / "orphan-v3e4.parquet"
            planted.parent.mkdir(parents=True, exist_ok=True)
            planted.write_bytes(b"not really parquet")
            now_ms = int(time.time() * 1000)
            with pytest.raises(AnalysisException, match="less than 24 hours"):
                spark.sql(
                    "CALL ice.system.remove_orphan_files("
                    f"table => 'sales.partdv', older_than => {now_ms}, dry_run => false)"
                )
            assert planted.is_file()

            with pytest.raises(UnsupportedOperationException, match="V3-COW-1"):
                spark.sql(f"DELETE FROM {table} WHERE id = 1")
    finally:
        spark.stop()
