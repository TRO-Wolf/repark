"""V3-4: facade serves `_row_id` and `_last_updated_sequence_number` on v3 reads.

pins: v3-4-serve-lineage-columns/C-004, C-005, C-007, C-008
"""

from __future__ import annotations

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
                    raise TimeoutError(f"fixture lock {path} held for 2 minutes") from None
                time.sleep(0.025)

    def close(self) -> None:
        with suppress(OSError):
            self.path.rmdir()


@contextmanager
def _materialize(src: Path, dest: Path) -> Iterator[str]:
    """Copy a Hadoop fixture to dest and yield its latest metadata file."""
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


def _lineage_rows(table: pa.Table) -> list[tuple[int, int | None, int | None]]:
    """Return (id, _row_id, _last_updated_sequence_number) with Arrow type checks."""
    assert table.schema.field("_row_id").type == pa.int64()
    assert table.schema.field("_row_id").nullable
    assert table.schema.field("_last_updated_sequence_number").type == pa.int64()
    assert table.schema.field("_last_updated_sequence_number").nullable
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


def test_facade_partitioned_v3_dv_serves_spark_equal_lineage(tmp_path: Path) -> None:
    """Facade SQL serves Spark-equal lineage on the partitioned-DV fixture."""
    from repark import ReparkSession

    session = ReparkSession.builder.appName("v3-4-part-dv-lineage").getOrCreate()
    try:
        session.register_memory_catalog("ice", tmp_path)
        session.sql("CREATE NAMESPACE ice.sales")
        with _materialize(_PART_DV_SRC, _PART_DV_DEST) as metadata_file:
            session.sql(
                "CALL ice.system.register_table("
                f"table => 'sales.partdv', metadata_file => '{metadata_file}')"
            )
            star = session.sql("SELECT * FROM ice.sales.partdv").to_arrow()
            assert star.column_names == ["id", "name", "part"]
            live = session.sql(
                "SELECT id, _row_id, _last_updated_sequence_number FROM ice.sales.partdv"
            ).to_arrow()
            assert _lineage_rows(live) == [
                (1, 0, 1),
                (3, 2, 1),
                (4, 3, 1),
                (6, 5, 1),
            ]
    finally:
        session.stop()


def test_facade_equality_delete_v3_serves_spark_equal_lineage(tmp_path: Path) -> None:
    """Facade SQL serves Spark-equal lineage on the equality-delete + DV fixture."""
    from repark import ReparkSession

    session = ReparkSession.builder.appName("v3-4-eq-dv-lineage").getOrCreate()
    try:
        session.register_memory_catalog("ice", tmp_path)
        session.sql("CREATE NAMESPACE ice.sales")
        with _materialize(_EQ_DV_SRC, _EQ_DV_DEST) as metadata_file:
            session.sql(
                "CALL ice.system.register_table("
                f"table => 'sales.eqdv', metadata_file => '{metadata_file}')"
            )
            live = session.sql(
                "SELECT id, _row_id, _last_updated_sequence_number FROM ice.sales.eqdv"
            ).to_arrow()
            assert _lineage_rows(live) == [(2, 1, 1), (3, 2, 1)]
    finally:
        session.stop()


def test_facade_v2_table_lineage_columns_are_unresolved(tmp_path: Path) -> None:
    """A v2 table must not plan `_row_id` (Spark UNRESOLVED_COLUMN, not NULL)."""
    from repark import ReparkSession

    session = ReparkSession.builder.appName("v3-4-v2-lineage").getOrCreate()
    try:
        session.register_memory_catalog("ice", tmp_path)
        session.sql("CREATE NAMESPACE ice.sales")
        session.sql("CREATE TABLE ice.sales.lin2 (id INT, name STRING) USING iceberg")
        session.sql("INSERT INTO ice.sales.lin2 VALUES (1, 'a')")
        with pytest.raises(Exception, match="_row_id") as raised:
            session.sql("SELECT id, _row_id FROM ice.sales.lin2").collect()
        assert "_row_id" in str(raised.value)
    finally:
        session.stop()
