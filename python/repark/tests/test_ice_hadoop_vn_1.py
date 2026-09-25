"""ICE-HADOOP-VN-1 — a stale Hadoop `vN` writer raises loud and loses nothing.

Two RePark memory catalogs adopt the Spark-written `v2` fixture; the first writer's
commit lands `v3`, and every later commit from the stale pointer — snapshot writes
and ``CREATE OR REPLACE`` alike — raises ``PySparkException`` with a
``CatalogCommitConflicts``-leading message while the winner's bytes stay intact and
no uuid file is minted (``ICE-HADOOP-VN-1-R-001``, FIXED 2026-09-18 at fork #293).
Live tier replays the Spark-first shape and the cross-engine reads.

pins: ice-hadoop-vn-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
"""

from __future__ import annotations

import json
import os
import shutil
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
_FIXTURE_SRC = _REPO_ROOT / "python/repark-parity/fixtures/torture/data/ice_hadoop_vn_1"
_TABLE_ROOT = Path("/tmp/repark-ice-hadoop-vn-1/ns/conc")
_WAREHOUSE = Path("/tmp/repark-ice-hadoop-vn-1")
_ORACLE_PATH = Path(__file__).resolve().parent / "ice_hadoop_vn_1_spark_oracle.json"
_LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
_LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — live Spark cell skipped (routine CI is JVM-free)"
_CATALOG_ONE = "rp"
_CATALOG_TWO = "rp2"
_CATALOG_FRESH = "rp3"
_NAMESPACE = "ns"
_TABLE = "conc"
_SPARK_CATALOG = "vn"
_SPARK_TABLE = f"{_SPARK_CATALOG}.{_NAMESPACE}.{_TABLE}"

_ORACLE_DOC: dict[str, Any] = json.loads(_ORACLE_PATH.read_text(encoding="utf-8"))


class _DirLock:
    """Cross-process lock so concurrent facade tests do not clobber the fixture copy."""

    def __init__(self, path: Path) -> None:
        """Claim the lock directory, waiting up to two minutes for its holder."""
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
        """Release the lock directory."""
        with suppress(OSError):
            self.path.rmdir()


@contextmanager
def _materialize() -> Iterator[Path]:
    """Copy the checked-in Spark-written table to its baked-in canonical location."""
    lock = _DirLock(Path(str(_TABLE_ROOT) + ".lock"))
    try:
        if _TABLE_ROOT.exists():
            shutil.rmtree(_TABLE_ROOT)
        _TABLE_ROOT.parent.mkdir(parents=True, exist_ok=True)
        for name in ("metadata", "data"):
            shutil.copytree(_FIXTURE_SRC / name, _TABLE_ROOT / name)
        yield _TABLE_ROOT
    finally:
        with suppress(OSError):
            if _TABLE_ROOT.exists():
                shutil.rmtree(_TABLE_ROOT)
        lock.close()


def _metadata_names(table_root: Path) -> list[str]:
    """Sorted `*.metadata.json` names under the table's metadata directory."""
    return sorted(path.name for path in (table_root / "metadata").glob("*.metadata.json"))


def _select_rows(session: Any, table: str) -> list[list[Any]]:
    """All `(id, s)` rows of `table` ordered by id."""
    rows = session.sql(f"SELECT id, s FROM {table}").to_arrow().to_pylist()
    return [[row["id"], row["s"]] for row in sorted(rows, key=repr)]


def _new_session(tmp_path: Path) -> Any:
    """A fresh RePark session with three memory catalogs and namespaces."""
    from repark import ReparkSession

    session = ReparkSession.builder.appName("ice-hadoop-vn-1").getOrCreate()
    session.register_memory_catalog(_CATALOG_ONE, tmp_path / "rp_wh")
    session.register_memory_catalog(_CATALOG_TWO, tmp_path / "rp2_wh")
    session.register_memory_catalog(_CATALOG_FRESH, tmp_path / "rp3_wh")
    session.sql(f"CREATE NAMESPACE {_CATALOG_ONE}.{_NAMESPACE}")
    session.sql(f"CREATE NAMESPACE {_CATALOG_TWO}.{_NAMESPACE}")
    session.sql(f"CREATE NAMESPACE {_CATALOG_FRESH}.{_NAMESPACE}")
    return session


def _adopt(session: Any, catalog: str, table: str, metadata_file: Path) -> None:
    """Register `metadata_file` as `catalog.ns.table`."""
    session.sql(
        f"CALL {catalog}.system.register_table("
        f"table => '{_NAMESPACE}.{table}', metadata_file => '{metadata_file}')"
    ).collect()


def _run_conc_shape(session: Any, table_root: Path) -> bytes:
    """Adopt `v2` in two catalogs; catalog one commits `v3`; return `v3` bytes."""
    _adopt(session, _CATALOG_ONE, _TABLE, table_root / "metadata" / "v2.metadata.json")
    _adopt(session, _CATALOG_TWO, _TABLE, table_root / "metadata" / "v2.metadata.json")
    session.sql(f"INSERT INTO {_CATALOG_ONE}.{_NAMESPACE}.{_TABLE} VALUES (2,'rp-cat1')").collect()
    assert _metadata_names(table_root) == [
        "v1.metadata.json",
        "v2.metadata.json",
        "v3.metadata.json",
    ]
    return (table_root / "metadata" / "v3.metadata.json").read_bytes()


def _assert_stale_write_raises(
    session: Any,
    write: Callable[[], object],
    table_root: Path,
    prefix: str | None = None,
) -> str:
    """Run a stale-pointer write; it must raise the recorded conflict; return its text.

    ``prefix`` replaces the contract's message start for a door that stages a replace.
    """
    from repark.errors import PySparkException

    contract = _ORACLE_DOC["repark_stale_commit"]
    v3_bytes = (table_root / "metadata" / "v3.metadata.json").read_bytes()
    with pytest.raises(PySparkException, match="CatalogCommitConflicts") as excinfo:
        write()
    message = str(excinfo.value)
    assert type(excinfo.value) is PySparkException
    assert message.startswith(prefix or contract["message_starts_with"]), message[:200]
    assert contract["message_contains"] in message
    assert "v3.metadata.json" in message
    assert _metadata_names(table_root) == [
        "v1.metadata.json",
        "v2.metadata.json",
        "v3.metadata.json",
    ]
    assert (table_root / "metadata" / "v3.metadata.json").read_bytes() == v3_bytes
    return message


def _assert_stale_commit_raises(session: Any, sql: str, table_root: Path) -> str:
    """Run a stale-pointer SQL write; it must raise the recorded conflict; return its text."""
    return _assert_stale_write_raises(session, lambda: session.sql(sql).collect(), table_root)


def test_conc_stale_writers_raise_and_winner_bytes_survive(tmp_path: Path) -> None:
    """C-001: INSERT, MERGE, DELETE and UPDATE from the stale pointer raise loud."""
    session = _new_session(tmp_path)
    try:
        with _materialize() as table_root:
            _run_conc_shape(session, table_root)
            stale = f"{_CATALOG_TWO}.{_NAMESPACE}.{_TABLE}"
            _assert_stale_commit_raises(
                session, f"INSERT INTO {stale} VALUES (3,'rp-cat2')", table_root
            )
            _assert_stale_commit_raises(
                session,
                f"MERGE INTO {stale} t USING (SELECT 1 AS id, 'merged' AS s) src "
                "ON t.id = src.id WHEN MATCHED THEN UPDATE SET s = src.s",
                table_root,
            )
            _assert_stale_commit_raises(session, f"DELETE FROM {stale} WHERE id = 1", table_root)
            _assert_stale_commit_raises(
                session, f"UPDATE {stale} SET s = 'stale' WHERE id = 1", table_root
            )
    finally:
        session.stop()


def test_conc_winner_rows_read_back(tmp_path: Path) -> None:
    """C-001: the winning catalog reads the winner's rows; the stale read stays stale."""
    session = _new_session(tmp_path)
    try:
        with _materialize() as table_root:
            _run_conc_shape(session, table_root)
            assert (
                _select_rows(session, f"{_CATALOG_ONE}.{_NAMESPACE}.{_TABLE}")
                == _ORACLE_DOC["rows_after_conc"]
            )
            assert _select_rows(session, f"{_CATALOG_TWO}.{_NAMESPACE}.{_TABLE}") == [[1, "seed"]]
    finally:
        session.stop()


def test_stale_handle_stays_wedged_loud(tmp_path: Path) -> None:
    """C-001: the stale pointer never advances — every later snapshot commit raises the same way."""
    session = _new_session(tmp_path)
    try:
        with _materialize() as table_root:
            _run_conc_shape(session, table_root)
            stale = f"{_CATALOG_TWO}.{_NAMESPACE}.{_TABLE}"
            _assert_stale_commit_raises(
                session, f"INSERT INTO {stale} VALUES (5,'wedged')", table_root
            )
            _assert_stale_commit_raises(
                session, f"INSERT INTO {stale} VALUES (6,'wedged-again')", table_root
            )
    finally:
        session.stop()


def test_exception_contract_matches_occ(tmp_path: Path) -> None:
    """C-004: the stale-commit surface is the OCC contract — base class, kind-led message."""
    session = _new_session(tmp_path)
    try:
        with _materialize() as table_root:
            _run_conc_shape(session, table_root)
            contract = _ORACLE_DOC["repark_stale_commit"]
            assert contract["class"] == "repark.errors.PySparkException"
            message = _assert_stale_commit_raises(
                session,
                f"INSERT INTO {_CATALOG_TWO}.{_NAMESPACE}.{_TABLE} VALUES (3,'rp-cat2')",
                table_root,
            )
            assert message.split(" => ", 1)[0] == "CatalogCommitConflicts"
    finally:
        session.stop()


def test_dataframe_doors_stale_writer_raises(tmp_path: Path) -> None:
    """C-005: `writeTo().append()` and `saveAsTable(append)` raise the same conflict."""
    session = _new_session(tmp_path)
    try:
        with _materialize() as table_root:
            _run_conc_shape(session, table_root)
            stale = f"{_CATALOG_TWO}.{_NAMESPACE}.{_TABLE}"
            _assert_stale_write_raises(
                session,
                lambda: session.sql("SELECT 9 AS id, 'df-writeto' AS s").writeTo(stale).append(),
                table_root,
            )
            _assert_stale_write_raises(
                session,
                lambda: (
                    session.createDataFrame([(10, "df-save")], ["id", "s"])
                    .write.mode("append")
                    .saveAsTable(stale)
                ),
                table_root,
            )
    finally:
        session.stop()


def test_stale_overwrite_shapes_raise(tmp_path: Path) -> None:
    """L-03: stale INSERT OVERWRITE, TRUNCATE and ALTER SET TBLPROPERTIES raise too."""
    session = _new_session(tmp_path)
    try:
        with _materialize() as table_root:
            _run_conc_shape(session, table_root)
            stale = f"{_CATALOG_TWO}.{_NAMESPACE}.{_TABLE}"
            _assert_stale_commit_raises(
                session, f"INSERT OVERWRITE {stale} SELECT 9 AS id, 'ovw' AS s", table_root
            )
            _assert_stale_commit_raises(session, f"TRUNCATE TABLE {stale}", table_root)
            _assert_stale_commit_raises(
                session, f"ALTER TABLE {stale} SET TBLPROPERTIES ('x'='y')", table_root
            )
    finally:
        session.stop()


def test_stale_overwrite_doors_raise(tmp_path: Path) -> None:
    """L-03: saveAsTable(overwrite) is a stale replace; writeTo().overwrite refuses declared."""
    from repark.errors import UnsupportedOperationException

    session = _new_session(tmp_path)
    try:
        with _materialize() as table_root:
            _run_conc_shape(session, table_root)
            stale = f"{_CATALOG_TWO}.{_NAMESPACE}.{_TABLE}"
            fresh = f"{_CATALOG_ONE}.{_NAMESPACE}.{_TABLE}"
            frame = session.createDataFrame([(8, "df-ovw")], ["id", "s"])
            message = _assert_stale_write_raises(
                session,
                lambda: frame.write.mode("overwrite").saveAsTable(stale),
                table_root,
                prefix="CatalogCommitConflicts => Cannot stage replace to",
            )
            version_three = table_root / "metadata" / "v3.metadata.json"
            assert message == (
                f"CatalogCommitConflicts => Cannot stage replace to {version_three}: "
                f"version file already exists ({version_three})"
            )
            with pytest.raises(UnsupportedOperationException, match="overwrite"):
                session.sql("SELECT 7 AS id, 'df-wt-ovw' AS s").writeTo(stale).overwrite("true")
            with pytest.raises(UnsupportedOperationException, match="overwrite"):
                session.sql("SELECT 7 AS id, 'df-wt-ovw' AS s").writeTo(fresh).overwrite("true")
            assert _metadata_names(table_root) == [
                "v1.metadata.json",
                "v2.metadata.json",
                "v3.metadata.json",
            ]
    finally:
        session.stop()


def test_stale_replace_raises_conflict(tmp_path: Path) -> None:
    """R-001, SQL door: the stale replace raises the typed conflict, no uuid file."""
    from repark.errors import PySparkException

    session = _new_session(tmp_path)
    try:
        with _materialize() as table_root:
            _run_conc_shape(session, table_root)
            stale = f"{_CATALOG_TWO}.{_NAMESPACE}.{_TABLE}"
            with pytest.raises(PySparkException, match="CatalogCommitConflicts"):
                session.sql(
                    f"CREATE OR REPLACE TABLE {stale} USING iceberg AS SELECT 99 AS id, 'rtas' AS s"
                ).collect()
            assert _metadata_names(table_root) == [
                "v1.metadata.json",
                "v2.metadata.json",
                "v3.metadata.json",
            ]
    finally:
        session.stop()


def test_stale_df_replace_raises_conflict(tmp_path: Path) -> None:
    """R-001, DataFrame door: replace and createOrReplace raise the typed conflict."""
    from repark.errors import PySparkException

    session = _new_session(tmp_path)
    try:
        with _materialize() as table_root:
            _run_conc_shape(session, table_root)
            stale = f"{_CATALOG_TWO}.{_NAMESPACE}.{_TABLE}"
            with pytest.raises(PySparkException, match="CatalogCommitConflicts"):
                session.sql("SELECT 98 AS id, 'df-rpl' AS s").writeTo(stale).replace()
            with pytest.raises(PySparkException, match="CatalogCommitConflicts"):
                session.sql("SELECT 97 AS id, 'df-cor' AS s").writeTo(stale).createOrReplace()
            assert _metadata_names(table_root) == [
                "v1.metadata.json",
                "v2.metadata.json",
                "v3.metadata.json",
            ]
    finally:
        session.stop()


def test_same_name_reregister_refuses(tmp_path: Path) -> None:
    """L-01: re-registering an existing name refuses TableAlreadyExists; nothing changes."""
    from repark.errors import AnalysisException

    session = _new_session(tmp_path)
    try:
        with _materialize() as table_root:
            _run_conc_shape(session, table_root)
            with pytest.raises(AnalysisException, match="TableAlreadyExists"):
                _adopt(
                    session,
                    _CATALOG_TWO,
                    _TABLE,
                    table_root / "metadata" / "v3.metadata.json",
                )
            assert _metadata_names(table_root) == [
                "v1.metadata.json",
                "v2.metadata.json",
                "v3.metadata.json",
            ]
    finally:
        session.stop()


def test_drop_stale_handle_deletes_pointer_file(tmp_path: Path) -> None:
    """L-01: DROP on the stale handle deletes the pointer's metadata file, not the table."""
    session = _new_session(tmp_path)
    try:
        with _materialize() as table_root:
            v3_bytes = _run_conc_shape(session, table_root)
            session.sql(f"DROP TABLE {_CATALOG_TWO}.{_NAMESPACE}.{_TABLE}").collect()
            assert not (table_root / "metadata" / "v2.metadata.json").exists()
            assert (table_root / "metadata" / "v3.metadata.json").read_bytes() == v3_bytes
            assert (
                _select_rows(session, f"{_CATALOG_ONE}.{_NAMESPACE}.{_TABLE}")
                == _ORACLE_DOC["rows_after_conc"]
            )
    finally:
        session.stop()


def test_planted_orphan_wedges_repark_loud(tmp_path: Path) -> None:
    """L-01: a planted orphan v(N+1) wedges the next RePark commit loud, then clears."""
    from repark.errors import PySparkException

    session = _new_session(tmp_path)
    try:
        with _materialize() as table_root:
            _run_conc_shape(session, table_root)
            planted = table_root / "metadata" / "v4.metadata.json"
            planted.write_bytes((table_root / "metadata" / "v2.metadata.json").read_bytes())
            _adopt(
                session,
                _CATALOG_FRESH,
                _TABLE,
                table_root / "metadata" / "v3.metadata.json",
            )
            with pytest.raises(PySparkException, match="CatalogCommitConflicts") as excinfo:
                session.sql(
                    f"INSERT INTO {_CATALOG_FRESH}.{_NAMESPACE}.{_TABLE} VALUES (5,'rp-planted')"
                ).collect()
            assert "v4.metadata.json" in str(excinfo.value)
            assert (
                planted.read_bytes() == (table_root / "metadata" / "v2.metadata.json").read_bytes()
            )
            planted.unlink()
            session.sql(
                f"INSERT INTO {_CATALOG_FRESH}.{_NAMESPACE}.{_TABLE} VALUES (5,'rp-planted')"
            ).collect()
            assert _metadata_names(table_root) == [
                "v1.metadata.json",
                "v2.metadata.json",
                "v3.metadata.json",
                "v4.metadata.json",
            ]
            assert _select_rows(session, f"{_CATALOG_FRESH}.{_NAMESPACE}.{_TABLE}") == [
                [1, "seed"],
                [2, "rp-cat1"],
                [5, "rp-planted"],
            ]
    finally:
        session.stop()


def test_oracle_spark_scan_forward_keys() -> None:
    """L-01: the frozen Spark scan-forward claims read back from the oracle JSON."""
    scan = _ORACLE_DOC["spark_stale_commit"]
    assert scan["raises"] is False
    assert scan["planted_next_version_insert"]["class"] == "COMMITTED"
    race = _ORACLE_DOC["spark_race_400k"]
    assert race["outcome"] == "committed"
    assert race["row_count"] == 400002


def test_recovery_fresh_registration_commits(tmp_path: Path) -> None:
    """C-003: a fresh handle registered at the newest version commits; all rows read back."""
    session = _new_session(tmp_path)
    try:
        with _materialize() as table_root:
            _run_conc_shape(session, table_root)
            _adopt(
                session,
                _CATALOG_FRESH,
                _TABLE,
                table_root / "metadata" / "v3.metadata.json",
            )
            session.sql(
                f"INSERT INTO {_CATALOG_FRESH}.{_NAMESPACE}.{_TABLE} VALUES (4,'rp-recovered')"
            ).collect()
            assert _metadata_names(table_root) == [
                "v1.metadata.json",
                "v2.metadata.json",
                "v3.metadata.json",
                "v4.metadata.json",
            ]
            assert (
                _select_rows(session, f"{_CATALOG_FRESH}.{_NAMESPACE}.{_TABLE}")
                == _ORACLE_DOC["rows_after_recovery"]
            )
    finally:
        session.stop()


def _live_spark_session() -> Any:
    """The shared live PySpark session with a private Hadoop catalog over the bake path."""
    import _live_parity as lp

    return lp.build_spark_iceberg_engine(_WAREHOUSE, catalog=_SPARK_CATALOG).session


def _live_spark_rows(spark: Any) -> list[list[Any]]:
    """All `(id, s)` rows Spark reads from the adopted table, ordered by id."""
    rows = spark.sql(f"SELECT id, s FROM {_SPARK_TABLE}").toArrow().to_pylist()
    return [[row["id"], row["s"]] for row in sorted(rows, key=repr)]


def _slow_race_insert(slow: Any, outcome_box: dict[str, str]) -> None:
    """Run the 400k-row Spark INSERT, recording whether it committed or raised."""
    try:
        slow.sql(
            f"INSERT INTO {_SPARK_TABLE} SELECT id, id FROM range(0, 400000, 1, 200)"
        ).collect()
        outcome_box["spark"] = "committed"
    except Exception as exc:
        outcome_box["spark"] = f"{type(exc).__module__}.{type(exc).__name__}"


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_live_conc_spark_cross_read(tmp_path: Path) -> None:
    """C-001 live: after the RePark commit race, Spark reads the winner's rows."""
    session = _new_session(tmp_path)
    try:
        with _materialize():
            _run_conc_shape(session, _TABLE_ROOT)
            spark = _live_spark_session()
            spark.sql(f"REFRESH TABLE {_SPARK_TABLE}")
            assert _live_spark_rows(spark) == _ORACLE_DOC["spark_rows_after_conc"]
    finally:
        session.stop()


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_live_conc2_stale_repark_raises(tmp_path: Path) -> None:
    """C-002: Spark commits first; the stale RePark INSERT raises; Spark keeps committing."""
    session = _new_session(tmp_path)
    try:
        with _materialize() as table_root:
            spark = _live_spark_session()
            spark.sql(f"REFRESH TABLE {_SPARK_TABLE}")
            spark.sql(f"INSERT INTO {_SPARK_TABLE} VALUES (2,'spark')")
            assert _metadata_names(table_root) == [
                "v1.metadata.json",
                "v2.metadata.json",
                "v3.metadata.json",
            ]
            v3_bytes = (table_root / "metadata" / "v3.metadata.json").read_bytes()
            _adopt(
                session,
                _CATALOG_ONE,
                "conc_stale",
                table_root / "metadata" / "v2.metadata.json",
            )
            _assert_stale_commit_raises(
                session,
                f"INSERT INTO {_CATALOG_ONE}.{_NAMESPACE}.conc_stale VALUES (3,'repark-stale')",
                table_root,
            )
            assert (table_root / "metadata" / "v3.metadata.json").read_bytes() == v3_bytes
            spark.sql(f"REFRESH TABLE {_SPARK_TABLE}")
            assert _live_spark_rows(spark) == _ORACLE_DOC["rows_after_conc2"]
            spark.sql(f"INSERT INTO {_SPARK_TABLE} VALUES (4,'spark-again')")
            spark.sql(f"REFRESH TABLE {_SPARK_TABLE}")
            assert _live_spark_rows(spark) == _ORACLE_DOC["rows_after_conc2_second"]
    finally:
        session.stop()


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_live_recovery_spark_reads_all(tmp_path: Path) -> None:
    """C-003 live: after the fresh-handle recovery commit, Spark reads every row."""
    session = _new_session(tmp_path)
    try:
        with _materialize() as table_root:
            _run_conc_shape(session, table_root)
            _adopt(
                session,
                _CATALOG_FRESH,
                _TABLE,
                table_root / "metadata" / "v3.metadata.json",
            )
            session.sql(
                f"INSERT INTO {_CATALOG_FRESH}.{_NAMESPACE}.{_TABLE} VALUES (4,'rp-recovered')"
            ).collect()
            spark = _live_spark_session()
            spark.sql(f"REFRESH TABLE {_SPARK_TABLE}")
            assert _live_spark_rows(spark) == _ORACLE_DOC["rows_after_recovery"]
    finally:
        session.stop()


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_live_stale_replace_conflicts_spark_reads_winner(tmp_path: Path) -> None:
    """R-001 live: the stale replace raises conflict; Spark still reads the winner's rows."""
    from repark.errors import PySparkException

    session = _new_session(tmp_path)
    try:
        with _materialize() as table_root:
            _run_conc_shape(session, table_root)
            stale = f"{_CATALOG_TWO}.{_NAMESPACE}.{_TABLE}"
            with pytest.raises(PySparkException, match="CatalogCommitConflicts"):
                session.sql(
                    f"CREATE OR REPLACE TABLE {stale} USING iceberg AS SELECT 99 AS id, 'rtas' AS s"
                ).collect()
            assert _metadata_names(table_root) == [
                "v1.metadata.json",
                "v2.metadata.json",
                "v3.metadata.json",
            ]
            spark = _live_spark_session()
            spark.sql(f"REFRESH TABLE {_SPARK_TABLE}")
            assert _live_spark_rows(spark) == _ORACLE_DOC["spark_rows_after_conc"]
    finally:
        session.stop()


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_live_spark_replace_after_repark_commit(tmp_path: Path) -> None:
    """R-001 live mirror: Spark's replace continues the version chain; RePark stays stale."""
    session = _new_session(tmp_path)
    try:
        with _materialize() as table_root:
            _adopt(
                session,
                _CATALOG_ONE,
                "rpl3",
                table_root / "metadata" / "v2.metadata.json",
            )
            session.sql(
                f"INSERT INTO {_CATALOG_ONE}.{_NAMESPACE}.rpl3 VALUES (2,'rp-cat1')"
            ).collect()
            assert _metadata_names(table_root) == [
                "v1.metadata.json",
                "v2.metadata.json",
                "v3.metadata.json",
            ]
            spark = _live_spark_session()
            spark.sql(f"REFRESH TABLE {_SPARK_TABLE}")
            spark_table = _SPARK_TABLE
            spark.sql(
                f"CREATE OR REPLACE TABLE {spark_table} USING iceberg "
                "AS SELECT 100 AS id, 'spark-rtas' AS s"
            )
            mirror = _ORACLE_DOC["spark_replace_after_repark"]
            assert _metadata_names(table_root) == mirror["versions"]
            spark.sql(f"REFRESH TABLE {_SPARK_TABLE}")
            assert _live_spark_rows(spark) == mirror["spark_rows"]
            assert (
                _select_rows(session, f"{_CATALOG_ONE}.{_NAMESPACE}.rpl3")
                == mirror["repark_stale_rows"]
            )
    finally:
        session.stop()


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_live_planted_next_version_commits(tmp_path: Path) -> None:
    """L-01 live: Spark commits past a planted next-version file, leaving it intact."""
    session = _new_session(tmp_path)
    try:
        with _materialize() as table_root:
            spark = _live_spark_session()
            spark.sql(f"REFRESH TABLE {_SPARK_TABLE}")
            planted = table_root / "metadata" / "v3.metadata.json"
            planted.write_bytes((table_root / "metadata" / "v2.metadata.json").read_bytes())
            spark.sql(f"INSERT INTO {_SPARK_TABLE} VALUES (5,'planted-live')")
            assert (
                planted.read_bytes() == (table_root / "metadata" / "v2.metadata.json").read_bytes()
            )
            names = _metadata_names(table_root)
            assert "v1.metadata.json" in names and "v2.metadata.json" in names
            assert len([name for name in names if name != "v3.metadata.json"]) == 3
            planted.unlink()
            spark.sql(f"INSERT INTO {_SPARK_TABLE} VALUES (6,'planted-again')")
            spark.sql(f"REFRESH TABLE {_SPARK_TABLE}")
            assert _live_spark_rows(spark) == [
                [1, "seed"],
                [5, "planted-live"],
                [6, "planted-again"],
            ]
    finally:
        session.stop()


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_live_spark_race_scan_forwards(tmp_path: Path) -> None:
    """L-01 live: a 400k-row Spark INSERT beside a RePark commit scan-forwards; count frozen."""
    session = _new_session(tmp_path)
    try:
        with _materialize() as table_root:
            spark = _live_spark_session()
            spark.sql(f"REFRESH TABLE {_SPARK_TABLE}")
            field = _ORACLE_DOC["spark_race_400k"]
            outcome_box: dict[str, str] = {}
            row_count = 0
            for round_no in range(3):
                _adopt(
                    session,
                    _CATALOG_ONE,
                    f"race{round_no}",
                    table_root / "metadata" / "v2.metadata.json",
                )
                slow = spark.newSession()
                data_root = table_root / "data"
                before = {str(path) for path in data_root.rglob("*.parquet")}
                thread = threading.Thread(target=_slow_race_insert, args=(slow, outcome_box))
                thread.start()
                deadline = time.time() + 180
                while time.time() < deadline:
                    current = {str(path) for path in data_root.rglob("*.parquet")}
                    if len(current - before) >= 1:
                        break
                    time.sleep(0.5)
                target = f"{_CATALOG_ONE}.{_NAMESPACE}.race{round_no}"
                try:
                    session.sql(f"INSERT INTO {target} VALUES (400001,'repark-race')").collect()
                    repark_won = True
                except Exception:
                    repark_won = False
                thread.join(timeout=300)
                if not repark_won:
                    continue
                spark.sql(f"REFRESH TABLE {_SPARK_TABLE}")
                row_count = (
                    spark.sql(f"SELECT count(*) AS n FROM {_SPARK_TABLE}")
                    .toArrow()
                    .to_pylist()[0]["n"]
                )
                if outcome_box.get("spark") == "committed" and row_count == 400002:
                    break
            assert outcome_box.get("spark") == "committed", outcome_box
            assert row_count == field["row_count"] == 400002
    finally:
        session.stop()
