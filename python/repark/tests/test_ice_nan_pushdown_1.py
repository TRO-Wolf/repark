"""ICE-NAN-PUSHDOWN-1 — NaN filter pushdown answers Spark end to end on Iceberg scans.

Oracle: ``ice_nan_pushdown_1_oracle.json`` beside this file, recorded from live
PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0 by
``_record_ice_nan_pushdown_1.py`` (re-run verifies, ``--rewrite`` re-records).
Rating rows V2-26 and V3-14: before fork #284 (``56cbdbdd``,
F-ICE-NAN-PUSHDOWN-1) at pin ``75da2b58``, ``d = NaN`` and ``d IN (NaN)``
answered ``[]`` on Iceberg scans where Spark answers the NaN rows; the fork
conversion now pushes ``IsNan`` / ``NotNan`` / ``IsNan OR In(rest)`` and leaves
ranges and NOT IN with NaN unpushed.

Every assertion runs on the Arrow path (id values AND the ``int32`` Arrow type)
on both doors: ``session.sql`` carries the full clause grid, and the DataFrame
API carries ``filter`` / ``isin`` / ``F.expr`` representatives. Shapes: NaN-only
file, NaN + finite + NULL in one file, two files; v2 and v3; RePark-written
tables built in the test plus the checked-in Spark-written warehouses. The live
tier re-derives the grid from live Spark and asserts
repark == pinned truth == live Spark.

pins: ice-nan-pushdown-1/C-001, C-002, C-003, C-004, C-005, C-006
pins: ice-nan-pushdown-1/C-007, C-008, C-009, C-010, C-011, C-012
"""

from __future__ import annotations

import json
import os
import shutil
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from _record_ice_nan_pushdown_1 import (
    NAN_D,
    ONE_F,
    _double_clauses,
    _float_clauses,
)

_HERE = Path(__file__).resolve().parent
_TRUTH = json.loads((_HERE / "ice_nan_pushdown_1_oracle.json").read_text(encoding="utf-8"))
_FIXTURE_SRC = _HERE / "fixtures" / "ice_nan_pushdown_1"
_CANONICAL_ROOT = Path("/tmp/repark-ice-nan-pushdown-1/wh")
_LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
_LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — live Spark cell skipped (routine CI is JVM-free)"
_CATALOG = "ice_nan_1"
_NAMESPACE = "ns"
_ALLOW_CREATE_V3_KEY = "repark.sql.allowCreateFormatVersion3"
_MIXED_FILE_ONE = (
    "(1, CAST('NaN' AS DOUBLE), CAST('NaN' AS FLOAT)), "
    "(2, CAST(1.0 AS DOUBLE), CAST(1.0 AS FLOAT)), (3, NULL, NULL)"
)
_MIXED_FILE_TWO = (
    "(4, CAST(0.5 AS DOUBLE), CAST(0.5 AS FLOAT)), "
    "(5, CAST('NaN' AS DOUBLE), CAST('NaN' AS FLOAT)), "
    "(6, CAST(-2.0 AS DOUBLE), CAST(-2.0 AS FLOAT))"
)
_NAN_ONLY_ROWS = (
    "(1, CAST('NaN' AS DOUBLE), CAST('NaN' AS FLOAT)), "
    "(2, CAST('NaN' AS DOUBLE), CAST('NaN' AS FLOAT))"
)
_NAN_ONLY_FLOAT_LEGS = ("eq", "neq", "in_nan", "isnan")


class _DirLock:
    """Cross-process lock so concurrent facade tests do not clobber the fixture copy."""

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
def _materialize(version: str, shape: str) -> Iterator[Path]:
    """Copy one fixture warehouse to its canonical path; yield newest metadata file."""
    dest = _CANONICAL_ROOT / "ns" / f"nan_{shape}_v{version}"
    lock = _DirLock(Path(str(dest) + ".lock"))
    try:
        if dest.exists():
            shutil.rmtree(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(_FIXTURE_SRC / f"v{version}" / shape, dest)
        versions = sorted(
            (dest / "metadata").glob("v*.metadata.json"),
            key=lambda path: int(path.name[1:].split(".", 1)[0]),
        )
        assert versions, f"no Hadoop metadata under {dest}/metadata"
        yield versions[-1]
    finally:
        with suppress(OSError):
            if dest.exists():
                shutil.rmtree(dest)
        lock.close()


def _ids(table: pa.Table) -> list[int]:
    """Sorted id column as ints, failing unless the Arrow type is ``int32``."""
    assert table.schema.field("id").type == pa.int32(), table.schema
    return sorted(int(value) for value in table.column("id").to_pylist())


def _select_ids(session: Any, table: str, where: str) -> list[int]:
    """One SQL-door read leg on the Arrow path."""
    return _ids(session.sql(f"SELECT id FROM {table} WHERE {where} ORDER BY id").to_arrow())


def _shape_cells(shape: str) -> list[tuple[str, str]]:
    """The (truth cell, WHERE text) legs for one shape."""
    legs = [(f"d_{name}", where) for name, where in _double_clauses().items()]
    float_names = list(_float_clauses().keys()) if shape == "mixed" else list(_NAN_ONLY_FLOAT_LEGS)
    legs.extend((f"f_{name}", _float_clauses()[name]) for name in float_names)
    return legs


def _assert_grid(session: Any, table: str, key: str) -> None:
    """Fail unless every clause leg on one table equals the recorded oracle cell."""
    for cell, where in _shape_cells(key.split("/")[1]):
        assert _select_ids(session, table, where) == _TRUTH["reads"][key][cell], (key, cell)


def _new_session(app: str) -> Any:
    """A RePark session allowed to create v3 tables."""
    from repark import ReparkSession

    return ReparkSession.builder.appName(app).config(_ALLOW_CREATE_V3_KEY, "true").getOrCreate()


def _register(session: Any, table_arg: str, metadata_file: Path) -> None:
    """Adopt one Hadoop metadata file into the memory catalog."""
    session.sql(
        f"CALL {_CATALOG}.system.register_table("
        f"table => '{table_arg}', metadata_file => '{metadata_file}')"
    )


def _build_repark_table(session: Any, table: str, version: str, shape: str) -> None:
    """Create and seed one RePark-written table mirroring the fixture shapes."""
    session.sql(
        f"CREATE TABLE {table} (id INT, d DOUBLE, f FLOAT) USING iceberg "
        f"TBLPROPERTIES ('format-version'='{version}')"
    )
    if shape == "mixed":
        session.sql(f"INSERT INTO {table} VALUES {_MIXED_FILE_ONE}")
        session.sql(f"INSERT INTO {table} VALUES {_MIXED_FILE_TWO}")
    else:
        session.sql(f"INSERT INTO {table} VALUES {_NAN_ONLY_ROWS}")


def _frame_ids(frame: Any) -> list[int]:
    """Sorted id column of a DataFrame on the Arrow path."""
    return _ids(frame.select("id").orderBy("id").to_arrow())


def _assert_frame_legs(session: Any, table: str, key: str) -> None:
    """Fail unless the DataFrame door matches the SQL door per clause representative."""
    from repark import functions as F  # noqa: N812 — PySpark idiom

    frame = session.table(table)
    nan_ids = _TRUTH["reads"][key]["d_eq"]
    assert _frame_ids(frame.filter(F.col("d") == float("nan"))) == nan_ids, key
    assert _frame_ids(frame.filter(F.col("d").isin(float("nan")))) == nan_ids, key
    assert _frame_ids(frame.filter(F.expr(f"d = {NAN_D}"))) == nan_ids, key
    if key.endswith("mixed"):
        both = _TRUTH["reads"][key]["d_in_nan_1"]
        assert _frame_ids(frame.filter(F.col("d").isin(float("nan"), 1.0))) == both, key


def test_repark_written_tables_answer_the_oracle(tmp_path: Path) -> None:
    """The full grid on RePark-written tables: every shape at v2 and v3, SQL door."""
    session = _new_session("ice-nan-pushdown-1-repark")
    try:
        session.register_memory_catalog(_CATALOG, tmp_path / "warehouse")
        session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
        for version in ("2", "3"):
            for shape in ("mixed", "nan_only"):
                table = f"{_CATALOG}.{_NAMESPACE}.rp_{shape}_v{version}"
                _build_repark_table(session, table, version, shape)
                _assert_grid(session, table, f"v{version}/{shape}")
    finally:
        session.stop()


def test_spark_written_fixture_tables_answer_the_oracle() -> None:
    """The full grid on the checked-in Spark-written warehouses, SQL door."""
    session = _new_session("ice-nan-pushdown-1-spark")
    try:
        session.register_memory_catalog(_CATALOG, Path("/tmp/repark-ice-nan-1-mem"))
        session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
        for version in ("2", "3"):
            for shape in ("mixed", "nan_only"):
                with _materialize(version, shape) as metadata_file:
                    table_arg = f"{_NAMESPACE}.sp_{shape}_v{version}"
                    _register(session, table_arg, metadata_file)
                    _assert_grid(session, f"{_CATALOG}.{table_arg}", f"v{version}/{shape}")
    finally:
        session.stop()


def test_dataframe_door_matches_the_sql_door(tmp_path: Path) -> None:
    """DataFrame representatives on RePark-written and Spark-written tables."""
    session = _new_session("ice-nan-pushdown-1-frame")
    try:
        session.register_memory_catalog(_CATALOG, tmp_path / "warehouse")
        session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
        for version in ("2", "3"):
            for shape in ("mixed", "nan_only"):
                table = f"{_CATALOG}.{_NAMESPACE}.rp_{shape}_v{version}"
                _build_repark_table(session, table, version, shape)
                _assert_frame_legs(session, table, f"v{version}/{shape}")
                with _materialize(version, shape) as metadata_file:
                    table_arg = f"{_NAMESPACE}.sp_{shape}_v{version}"
                    _register(session, table_arg, metadata_file)
                    _assert_frame_legs(session, f"{_CATALOG}.{table_arg}", f"v{version}/{shape}")
    finally:
        session.stop()


def _all_ids(session: Any, table: str) -> list[int]:
    """Every id in a table in order."""
    return _ids(session.sql(f"SELECT id FROM {table} ORDER BY id").to_arrow())


def _dml_outcome(session: Any, table: str) -> tuple[list[int], list[int], list[int]]:
    """UPDATE then DELETE outcomes on one fresh mixed-shape table."""
    session.sql(f"UPDATE {table} SET f = {ONE_F} WHERE d = {NAN_D}")
    touched = _select_ids(session, table, f"f = {ONE_F} AND d = {NAN_D}")
    untouched = _select_ids(session, table, f"NOT (f = {ONE_F})")
    session.sql(f"DELETE FROM {table} WHERE d = {NAN_D}")
    return _all_ids(session, table), touched, untouched


def test_delete_and_update_touch_exactly_the_nan_rows(tmp_path: Path) -> None:
    """DML row outcomes at v2 and v3 equal the recorded Spark DML oracle."""
    session = _new_session("ice-nan-pushdown-1-dml")
    try:
        session.register_memory_catalog(_CATALOG, tmp_path / "warehouse")
        session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
        for version in ("2", "3"):
            table = f"{_CATALOG}.{_NAMESPACE}.dml_v{version}"
            _build_repark_table(session, table, version, "mixed")
            remaining, touched, untouched = _dml_outcome(session, table)
            assert remaining == _TRUTH["dml"]["delete_remaining"], version
            assert touched == _TRUTH["dml"]["update_touched"], version
            assert untouched == _TRUTH["dml"]["update_untouched"], version
    finally:
        session.stop()


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_live_grid_replays_spark(tmp_path: Path) -> None:
    """Live Spark re-derives the grid; RePark matches truth and live on both doors."""
    import _live_parity as lp

    warehouse = tmp_path / "spark-warehouse"
    spark = lp.build_spark_iceberg_engine(warehouse, catalog="ice_nan_1_live").session
    live_names: dict[str, str] = {}
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice_nan_1_live.ns")
    for version in ("2", "3"):
        for shape in ("mixed", "nan_only"):
            table = f"ice_nan_1_live.ns.live_{shape}_v{version}"
            spark.sql(
                f"CREATE TABLE {table} (id INT, d DOUBLE, f FLOAT) USING iceberg "
                f"TBLPROPERTIES ('format-version'='{version}')"
            )
            if shape == "mixed":
                spark.sql(
                    "INSERT INTO " + table + " SELECT /*+ COALESCE(1) */ * "
                    f"FROM VALUES {_MIXED_FILE_ONE} AS x(id, d, f)"
                )
                spark.sql(
                    "INSERT INTO " + table + " SELECT /*+ COALESCE(1) */ * "
                    f"FROM VALUES {_MIXED_FILE_TWO} AS x(id, d, f)"
                )
            else:
                spark.sql(
                    "INSERT INTO " + table + " SELECT /*+ COALESCE(1) */ * "
                    f"FROM VALUES {_NAN_ONLY_ROWS} AS x(id, d, f)"
                )
            live_names[f"v{version}/{shape}"] = table
    for key, table in live_names.items():
        for cell, where in _shape_cells(key.split("/")[1]):
            live = sorted(
                row[0] for row in spark.sql(f"SELECT id FROM {table} WHERE {where}").collect()
            )
            assert live == _TRUTH["reads"][key][cell], ("oracle drift", key, cell)
    repark = _new_session("ice-nan-pushdown-1-live")
    try:
        repark.register_memory_catalog(_CATALOG, tmp_path / "repark-warehouse")
        repark.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
        for key, table in live_names.items():
            root = warehouse / "ns" / table.split(".")[-1]
            versions = sorted(
                (root / "metadata").glob("v*.metadata.json"),
                key=lambda path: int(path.name[1:].split(".", 1)[0]),
            )
            assert versions, f"no Hadoop metadata under {root}/metadata"
            table_arg = f"{_NAMESPACE}.live_{key.replace('/', '_')}"
            _register(repark, table_arg, versions[-1])
            fq = f"{_CATALOG}.{table_arg}"
            _assert_grid(repark, fq, key)
            _assert_frame_legs(repark, fq, key)
    finally:
        repark.stop()
