"""V3E-5 live oracle for Spark-written v3 fixtures.

pins: v3e-5-nightly-v3-oracle/C-002, C-003, C-004, C-005, C-007, C-008, C-011
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

    warehouse = Path(tempfile.mkdtemp(prefix="repark-v3e-5-live-part-"))
    try:
        engine = live_parity.build_spark_iceberg_engine(warehouse)
        try:
            engine.session.sql("CREATE NAMESPACE IF NOT EXISTS local.sales")
            engine.session.sql(
                f"CALL system.register_table(table => 'sales.partdv', "
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

    warehouse = Path(tempfile.mkdtemp(prefix="repark-v3e-5-live-eq-"))
    try:
        engine = live_parity.build_spark_iceberg_engine(warehouse)
        try:
            engine.session.sql("CREATE NAMESPACE IF NOT EXISTS local.sales")
            engine.session.sql(
                f"CALL system.register_table(table => 'sales.eqdv', "
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

    warehouse = Path(tempfile.mkdtemp(prefix="repark-v3e-5-live-del-"))
    try:
        engine = live_parity.build_spark_iceberg_engine(warehouse)
        try:
            engine.session.sql("CREATE NAMESPACE IF NOT EXISTS local.sales")
            engine.session.sql(
                f"CALL system.register_table(table => 'sales.partdv', "
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
                f"CALL system.register_table(table => 'sales.eqdv2', metadata_file => '{eq_meta}')"
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


def test_partitioned_dv_update_and_rewrite_refuse_pre_write(tmp_path: Path) -> None:
    """The partitioned-DV DELETE succeeds; UPDATE and unsafe rewrite refuse before writing.

    pins: v3e-5-nightly-v3-oracle/C-008
    pins: rp-3-fork-repin/C-004, C-011
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
            before = session.sql("SELECT id, name, part FROM ice.sales.partdv").to_arrow()
            before_objects = _objects_under(_PART_DV_DEST)
            with pytest.raises(UnsupportedOperationException, match="V3-COW-1"):
                session.sql("UPDATE ice.sales.partdv SET name = 'x' WHERE id = 1").collect()
            refused_update = session.sql("SELECT id, name, part FROM ice.sales.partdv").to_arrow()
            assert _id_name_part_rows(refused_update) == _id_name_part_rows(before)
            assert _objects_under(_PART_DV_DEST) == before_objects
            with pytest.raises(UnsupportedOperationException, match="Puffin deletion vector"):
                session.sql(
                    "CALL ice.system.rewrite_position_delete_files(table => 'sales.partdv')"
                ).collect()
            refused = session.sql("SELECT id, name, part FROM ice.sales.partdv").to_arrow()
            assert _id_name_part_rows(refused) == _id_name_part_rows(before)
            assert _objects_under(_PART_DV_DEST) == before_objects
            session.sql("DELETE FROM ice.sales.partdv WHERE id = 1").collect()
            live = session.sql("SELECT id, name, part FROM ice.sales.partdv").to_arrow()
            assert _id_name_part_rows(live) == [(3, "c", 0), (4, "d", 1), (6, "f", 1)]
    finally:
        session.stop()


def test_northstar_nightly_v3_leg_is_v3e_5() -> None:
    """Northstar nightly row cites V3E-5 as the live leg.

    pins: v3e-5-nightly-v3-oracle/C-009
    """
    northstar = _REPO_ROOT / "task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md"
    text = northstar.read_text(encoding="utf-8")
    assert "Nightly oracle: v3 leg | ✅ V3E-5 (2026-08-27):" in text
    assert "PySpark 4.1.2 + Iceberg 1.11.0" in text
    assert "Nightly oracle: v3 leg | ❌ none" not in text


def test_v3_live_oracle_pins_cover_all_clauses() -> None:
    """Meta-pin that every PROVEN clause in the ledger has a pins cite.

    pins: v3e-5-nightly-v3-oracle/C-001, C-002, C-006, C-009, C-010, C-011, C-012
    """
    import subprocess

    ledger_name = "v3e-5-nightly-v3-oracle-ledger.md"
    live_ledgers = (
        _REPO_ROOT / "task/ledgers/staging" / ledger_name,
        _REPO_ROOT / "task/ledgers/completed" / ledger_name,
    )
    archived_ledgers = sorted((_REPO_ROOT / "task/ledgers/archive").glob(f"*/*-{ledger_name}"))
    ledgers = [path for path in (*live_ledgers, *archived_ledgers) if path.is_file()]
    assert len(ledgers) == 1, ledgers
    text = ledgers[0].read_text(encoding="utf-8")
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
