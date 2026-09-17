"""ICE-NAN-PUSHDOWN-1 — NaN filter pushdown answers Spark end to end on Iceberg scans.

Oracle: ``ice_nan_pushdown_1_oracle.json`` beside this file, recorded from live
PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0 by
``_record_ice_nan_pushdown_1.py`` (re-run verifies, ``--rewrite`` re-records).
Rating rows V2-26 and V3-14: before fork #284 (``56cbdbdd``,
F-ICE-NAN-PUSHDOWN-1) at pin ``75da2b58``, ``d = NaN`` and ``d IN (NaN)``
answered ``[]`` on Iceberg scans where Spark answers the NaN rows. The pushed
shapes are pinned structurally in ``crates/repark-spark/src/tests/nan_pushdown.rs``
(``=`` pushes ``Unary IsNan``; ``IN (NaN, 1.0)`` pushes ``Or(IsNan, Eq)`` with a
NaN-free literal; ``<=>`` and NaN-literal ranges push nothing; single-element
``NOT IN`` reaches ``!=``).

Every assertion runs on the Arrow path (id values AND the ``int32`` Arrow type)
on both doors: ``session.sql`` carries the full clause grid, and the DataFrame
API carries every grid cell except the ``eq_rev`` spelling twin. Shapes:
NaN-only file, NaN + finite + NULL in one file, two mixed files, and the split
shape (an all-NaN file beside a finite-only file, so the ``is_nan`` prune-away
is exercised); v2 and v3; RePark-written tables built in the test plus the
checked-in Spark-written warehouses. ``d_between`` is the finite-leg file prune
artefact (the ``>= 1.0`` leg pushes and drops the sub-``1.0`` file with its NaN);
``d_between_nan_nan`` is the pure NaN-bound residual. The live tier re-derives
the grid from live Spark and asserts repark == pinned truth == live Spark,
including DELETE/UPDATE outcomes at v2 and v3. Bare-decimal-literal spellings
stay a loud error (ICE-NAN-DECIMAL-LITERAL-1, BACKLOG): the pins codify the
current needles and the recorded Spark answers.

pins: ice-nan-pushdown-1/C-001, C-002, C-003, C-004, C-005, C-006
pins: ice-nan-pushdown-1/C-007, C-008, C-009, C-010, C-011, C-012
pins: ice-nan-pushdown-1/C-013, C-014
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
    _MIXED_FILE_ONE,
    _MIXED_FILE_TWO,
    _NAN_ONLY_ROWS,
    _SPLIT_FILE_A,
    _SPLIT_FILE_B,
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
_VERSIONS = ("2", "3")
_SHAPES = ("mixed", "nan_only", "split")
_NAN_ONLY_FLOAT_LEGS = ("eq", "neq", "in_nan", "isnan")
_DECIMAL_EQ_NEEDLE = "Overflowing on NaN"
_DECIMAL_IN_NEEDLE = "Cannot cast to Decimal128"


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
    if shape == "nan_only":
        float_names: tuple[str, ...] = _NAN_ONLY_FLOAT_LEGS
    else:
        float_names = tuple(_float_clauses().keys())
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
    elif shape == "split":
        session.sql(f"INSERT INTO {table} VALUES {_SPLIT_FILE_A}")
        session.sql(f"INSERT INTO {table} VALUES {_SPLIT_FILE_B}")
    else:
        session.sql(f"INSERT INTO {table} VALUES {_NAN_ONLY_ROWS}")


def _frame_ids(frame: Any) -> list[int]:
    """Sorted id column of a DataFrame on the Arrow path."""
    return _ids(frame.select("id").orderBy("id").to_arrow())


def _assert_frame_legs(session: Any, table: str, key: str) -> None:
    """Fail unless the DataFrame door answers every grid cell but the eq_rev twin."""
    from repark import functions as F  # noqa: N812 — PySpark idiom

    nan = float("nan")
    frame = session.table(table)
    reads = _TRUTH["reads"][key]
    assert _frame_ids(frame.filter(F.col("d") == nan)) == reads["d_eq"], (key, "d_eq")
    assert _frame_ids(frame.filter(F.col("d").eqNullSafe(nan))) == reads["d_nullsafe"], (
        key,
        "d_nullsafe",
    )
    assert _frame_ids(frame.filter(F.col("d") != nan)) == reads["d_neq"], (key, "d_neq")
    assert _frame_ids(frame.filter(F.col("d") < nan)) == reads["d_lt"], (key, "d_lt")
    assert _frame_ids(frame.filter(F.col("d") <= nan)) == reads["d_le"], (key, "d_le")
    assert _frame_ids(frame.filter(F.col("d") > nan)) == reads["d_gt"], (key, "d_gt")
    assert _frame_ids(frame.filter(F.col("d") >= nan)) == reads["d_ge"], (key, "d_ge")
    assert _frame_ids(frame.filter(F.col("d").isin(nan))) == reads["d_in_nan"], (key, "d_in_nan")
    assert _frame_ids(frame.filter(~F.col("d").isin(nan))) == reads["d_not_in_nan"], (
        key,
        "d_not_in_nan",
    )
    assert _frame_ids(frame.filter(F.col("d").between(1.0, nan))) == reads["d_between"], (
        key,
        "d_between",
    )
    assert _frame_ids(frame.filter(F.col("d").between(nan, nan))) == reads["d_between_nan_nan"], (
        key,
        "d_between_nan_nan",
    )
    assert _frame_ids(frame.filter(~(F.col("d") == nan))) == reads["d_not_eq"], (key, "d_not_eq")
    assert _frame_ids(frame.filter(F.isnan(F.col("d")))) == reads["d_isnan"], (key, "d_isnan")
    assert _frame_ids(frame.filter(F.expr(f"d = {NAN_D}"))) == reads["d_eq"], (key, "d_eq_expr")
    if not key.endswith("nan_only"):
        assert _frame_ids(frame.filter(F.col("d").isin(nan, 1.0))) == reads["d_in_nan_1"], (
            key,
            "d_in_nan_1",
        )
        assert _frame_ids(frame.filter(F.col("f") == nan)) == reads["f_eq"], (key, "f_eq")
        assert _frame_ids(frame.filter(F.col("f").isin(nan))) == reads["f_in_nan"], (
            key,
            "f_in_nan",
        )
    else:
        assert _frame_ids(frame.filter(F.col("d").isin(nan, 1.0))) == reads["d_in_nan_1"], (
            key,
            "d_in_nan_1",
        )
        assert _frame_ids(frame.filter(F.col("f") == nan)) == reads["f_eq"], (key, "f_eq")
        assert _frame_ids(frame.filter(F.col("f").isin(nan))) == reads["f_in_nan"], (
            key,
            "f_in_nan",
        )


def test_repark_written_tables_answer_the_oracle(tmp_path: Path) -> None:
    """The full grid on RePark-written tables: every shape at v2 and v3, SQL door."""
    session = _new_session("ice-nan-pushdown-1-repark")
    try:
        session.register_memory_catalog(_CATALOG, tmp_path / "warehouse")
        session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
        for version in _VERSIONS:
            for shape in _SHAPES:
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
        for version in _VERSIONS:
            for shape in _SHAPES:
                with _materialize(version, shape) as metadata_file:
                    table_arg = f"{_NAMESPACE}.sp_{shape}_v{version}"
                    _register(session, table_arg, metadata_file)
                    _assert_grid(session, f"{_CATALOG}.{table_arg}", f"v{version}/{shape}")
    finally:
        session.stop()


def test_dataframe_door_matches_the_sql_door(tmp_path: Path) -> None:
    """The DataFrame door answers every grid cell but the eq_rev twin, both writers."""
    session = _new_session("ice-nan-pushdown-1-frame")
    try:
        session.register_memory_catalog(_CATALOG, tmp_path / "warehouse")
        session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
        for version in _VERSIONS:
            for shape in _SHAPES:
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


def _assert_dml_outcome(session: Any, table: str, version: str) -> None:
    """Fail unless one table's DML outcomes equal the recorded Spark DML oracle."""
    expected = _TRUTH["dml"][version]
    remaining, touched, untouched = _dml_outcome(session, table)
    assert remaining == expected["delete_remaining"], (version, table)
    assert touched == expected["update_touched"], (version, table)
    assert untouched == expected["update_untouched"], (version, table)


def test_delete_and_update_touch_exactly_the_nan_rows(tmp_path: Path) -> None:
    """DML row outcomes at v2 and v3 equal the recorded Spark DML oracle."""
    session = _new_session("ice-nan-pushdown-1-dml")
    try:
        session.register_memory_catalog(_CATALOG, tmp_path / "warehouse")
        session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
        for version in _VERSIONS:
            table = f"{_CATALOG}.{_NAMESPACE}.dml_v{version}"
            _build_repark_table(session, table, version, "mixed")
            _assert_dml_outcome(session, table, version)
    finally:
        session.stop()


def test_bare_decimal_literal_spellings_raise_loud(tmp_path: Path) -> None:
    """ICE-NAN-DECIMAL-LITERAL-1 (BACKLOG): bare decimals fail loud, never silent.

    The pins codify today's needles so the fix reds them on purpose; the
    recorded Spark answers in ``decimal_literal`` are the fix target.
    """
    assert _TRUTH["decimal_literal"]["d_eq_1_0_bare"] == [2]
    assert _TRUTH["decimal_literal"]["d_in_nan_1_0_bare"] == [1, 2, 5]
    session = _new_session("ice-nan-pushdown-1-decimal")
    try:
        session.register_memory_catalog(_CATALOG, tmp_path / "warehouse")
        session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
        table = f"{_CATALOG}.{_NAMESPACE}.dec_v2"
        _build_repark_table(session, table, "2", "mixed")
        with pytest.raises(Exception) as eq_error:
            session.sql(f"SELECT id FROM {table} WHERE d = 1.0").to_arrow()
        assert _DECIMAL_EQ_NEEDLE in str(eq_error.value), str(eq_error.value)
        with pytest.raises(Exception) as in_error:
            session.sql(f"SELECT id FROM {table} WHERE d IN ({NAN_D}, 1.0)").to_arrow()
        assert _DECIMAL_IN_NEEDLE in str(in_error.value), str(in_error.value)
    finally:
        session.stop()


def _build_live_tables(spark: Any, catalog: str) -> dict[str, str]:
    """Create and seed the six live tables; return shape key to qualified name."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.ns")
    live_names: dict[str, str] = {}
    for version in _VERSIONS:
        for shape in _SHAPES:
            table = f"{catalog}.ns.live_{shape}_v{version}"
            spark.sql(
                f"CREATE TABLE {table} (id INT, d DOUBLE, f FLOAT) USING iceberg "
                f"TBLPROPERTIES ('format-version'='{version}')"
            )
            if shape == "mixed":
                batches = (_MIXED_FILE_ONE, _MIXED_FILE_TWO)
            elif shape == "split":
                batches = (_SPLIT_FILE_A, _SPLIT_FILE_B)
            else:
                batches = (_NAN_ONLY_ROWS,)
            for batch in batches:
                spark.sql(
                    "INSERT INTO " + table + " SELECT /*+ COALESCE(1) */ * "
                    f"FROM VALUES {batch} AS x(id, d, f)"
                )
            live_names[f"v{version}/{shape}"] = table
    return live_names


def _live_ids(spark: Any, table: str, where: str) -> list[int]:
    """Sorted id column of one live Spark read leg."""
    return sorted(row[0] for row in spark.sql(f"SELECT id FROM {table} WHERE {where}").collect())


def _live_all_ids(spark: Any, table: str) -> list[int]:
    """Every id in one live table in order."""
    return sorted(row[0] for row in spark.sql(f"SELECT id FROM {table}").collect())


def _assert_live_reads(spark: Any, live_names: dict[str, str]) -> None:
    """Fail unless live Spark re-derives every recorded read cell (oracle drift)."""
    for key, table in live_names.items():
        for cell, where in _shape_cells(key.split("/")[1]):
            assert _live_ids(spark, table, where) == _TRUTH["reads"][key][cell], (
                "oracle drift",
                key,
                cell,
            )


def _newest_live_metadata(warehouse: Path, table: str) -> Path:
    """Newest Hadoop metadata file of one live table."""
    root = warehouse / "ns" / table.split(".")[-1]
    versions = sorted(
        (root / "metadata").glob("v*.metadata.json"),
        key=lambda path: int(path.name[1:].split(".", 1)[0]),
    )
    assert versions, f"no Hadoop metadata under {root}/metadata"
    return versions[-1]


def _assert_live_dml(spark: Any, catalog: str) -> None:
    """Fail unless live Spark DML outcomes equal the recorded DML oracle."""
    for version in _VERSIONS:
        delete_table = f"{catalog}.ns.live_dml_v{version}"
        spark.sql(
            f"CREATE TABLE {delete_table} (id INT, d DOUBLE, f FLOAT) USING iceberg "
            f"TBLPROPERTIES ('format-version'='{version}')"
        )
        for batch in (_MIXED_FILE_ONE, _MIXED_FILE_TWO):
            spark.sql(
                "INSERT INTO " + delete_table + " SELECT /*+ COALESCE(1) */ * "
                f"FROM VALUES {batch} AS x(id, d, f)"
            )
        spark.sql(f"DELETE FROM {delete_table} WHERE d = {NAN_D}")
        remaining = _live_all_ids(spark, delete_table)
        update_table = f"{catalog}.ns.live_upd_v{version}"
        spark.sql(
            f"CREATE TABLE {update_table} (id INT, d DOUBLE, f FLOAT) USING iceberg "
            f"TBLPROPERTIES ('format-version'='{version}')"
        )
        for batch in (_MIXED_FILE_ONE, _MIXED_FILE_TWO):
            spark.sql(
                "INSERT INTO " + update_table + " SELECT /*+ COALESCE(1) */ * "
                f"FROM VALUES {batch} AS x(id, d, f)"
            )
        spark.sql(f"UPDATE {update_table} SET f = {ONE_F} WHERE d = {NAN_D}")
        touched = _live_ids(spark, update_table, f"f = {ONE_F} AND d = {NAN_D}")
        untouched = _live_ids(spark, update_table, f"NOT (f = {ONE_F})")
        expected = _TRUTH["dml"][version]
        assert remaining == expected["delete_remaining"], ("live dml", version)
        assert touched == expected["update_touched"], ("live dml", version)
        assert untouched == expected["update_untouched"], ("live dml", version)


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_live_grid_replays_spark(tmp_path: Path) -> None:
    """Live Spark re-derives the grid and DML; RePark matches truth and live."""
    import _live_parity as lp

    catalog = "ice_nan_1_live"
    warehouse = tmp_path / "spark-warehouse"
    spark = lp.build_spark_iceberg_engine(warehouse, catalog=catalog).session
    live_names = _build_live_tables(spark, catalog)
    _assert_live_reads(spark, live_names)
    _assert_live_dml(spark, catalog)
    repark = _new_session("ice-nan-pushdown-1-live")
    try:
        repark.register_memory_catalog(_CATALOG, tmp_path / "repark-warehouse")
        repark.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
        adopted: dict[str, str] = {}
        for key in live_names:
            table_arg = f"{_NAMESPACE}.live_{key.replace('/', '_')}"
            _register(repark, table_arg, _newest_live_metadata(warehouse, live_names[key]))
            fq = f"{_CATALOG}.{table_arg}"
            adopted[key] = fq
            _assert_grid(repark, fq, key)
            _assert_frame_legs(repark, fq, key)
        for version in _VERSIONS:
            _assert_dml_outcome(repark, adopted[f"v{version}/mixed"], version)
    finally:
        repark.stop()
