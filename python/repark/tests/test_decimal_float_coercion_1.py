"""WO-2 (xo-muse8 UNIT1 fix-b): decimal literals against DOUBLE/FLOAT widen the literal.

A decimal literal compared with a DOUBLE or FLOAT column widens to DOUBLE
(``d = CAST(0.0 AS DOUBLE)``, Spark's analyzed shape), never the column to
decimal — so filters over NaN-holding columns answer instead of dying with
``Overflowing on NaN``. The FLOAT column itself promotes to double, exactly
like Spark (``CAST(f AS DOUBLE) = CAST(0.1 AS DOUBLE)``, which is why rows
holding float32 ``0.1`` do not match ``f = 0.1``).

Oracle: ``decimal_float_coercion_1_spark_oracle.json`` beside this file,
recorded from live PySpark 4.1.2 (ANSI on, UTC) 2026-09-21 over the ten-row
fixture quoted there. Both doors run on a memory-catalog Iceberg table seeded
by SQL with payload-preserving spellings (``CAST('-0.0' AS DOUBLE)`` keeps
the sign bit that a bare ``-0.0`` literal loses at parse): the SQL door
carries 46 legs, the DataFrame door 18 legs (``F.col`` comparisons, ``isin``,
``between``, string predicates, ``F.lit``). The R-NAN-FILTER replica runs the
scoreboard cell's exact SQL and asserts the orchestrator-recorded Spark
targets. Seventeen zero-bound legs pin the separate pre-existing signed-zero
kernel divergence (the float eq kernel keeps ``-0.0`` distinct from ``0.0``)
with Spark's answer beside RePark's; every other leg equals Spark, value and
Arrow type.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812 — PySpark idiom

_ORACLE = json.loads(
    (Path(__file__).parent / "decimal_float_coercion_1_spark_oracle.json").read_text(
        encoding="utf-8"
    )
)

_SEED = """(1, CAST('NaN' AS DOUBLE), CAST('NaN' AS FLOAT), 10.50),
(2, CAST('-0.0' AS DOUBLE), CAST('-0.0' AS FLOAT), -3.25),
(3, 0.0, CAST(0.0 AS FLOAT), 0.00),
(4, 1.5, CAST(1.5 AS FLOAT), 0.01),
(5, NULL, NULL, NULL),
(6, CAST('Infinity' AS DOUBLE), CAST('Infinity' AS FLOAT), 999.99),
(7, CAST('-1e300' AS DOUBLE), CAST('-1e30' AS FLOAT), -99.99),
(8, 2.0, CAST(2.0 AS FLOAT), 7.00),
(9, 0.1, CAST(0.1 AS FLOAT), 1.00),
(10, 3.0, CAST(0.1 AS FLOAT), 42.42)"""

_MATCH_SQL: dict[str, str] = {
    "d_gt_1": "d > 1.0",
    "d_gt_0": "d > 0.0",
    "d_lte_0": "d <= 0.0",
    "d_lte_01": "d <= 0.1",
    "d_gte_1": "d >= 1.0",
    "d_ne_01": "d <> 0.1",
    "d_between_1_3": "d BETWEEN 1.0 AND 3.0",
    "d_not_between_1_3": "d NOT BETWEEN 1.0 AND 3.0",
    "d_eq_neg": "d = -0.5",
    "d_gt_neg": "d > -1.0",
    "f_gt_1": "f > 1.0",
    "f_gt_0": "f > 0.0",
    "f_gte_1": "f >= 1.0",
    "f_lte_0": "f <= 0.0",
    "f_lt_01": "f < 0.1",
    "f_eq_01": "f = 0.1",
    "f_ne_01": "f <> 0.1",
    "f_between_1_2": "f BETWEEN 1.0 AND 2.0",
    "f_not_between_1_2": "f NOT BETWEEN 1.0 AND 2.0",
    "f_eq_rev": "0.1 = f",
    "f_eq_15": "f = 1.5",
    "dec_eq_0": "dec = 0",
    "dec_eq_00": "dec = 0.0",
    "dec_gt_10": "dec > 10",
    "dec_lt_0": "dec < 0",
    "dec_in": "dec IN (0.0, 1.00)",
    "isnan_d": "isnan(d)",
    "d_eq_nan": "d = CAST('NaN' AS DOUBLE)",
    "d_notnull": "d IS NOT NULL",
}

_DIVERGE_SQL: dict[str, tuple[str, list[int]]] = {
    "d_eq_0": ("d = 0.0", [3]),
    "d_eq_rev": ("0.0 = d", [3]),
    "d_ne_0": ("d <> 0.0", [1, 2, 4, 6, 7, 8, 9, 10]),
    "d_lt_0": ("d < 0.0", [2, 7]),
    "d_gte_0": ("d >= 0.0", [1, 3, 4, 6, 8, 9, 10]),
    "d_between": ("d BETWEEN 0.0 AND 2.0", [3, 4, 8, 9]),
    "d_not_between": ("d NOT BETWEEN 0.0 AND 2.0", [1, 2, 6, 7, 10]),
    "d_in": ("d IN (0.0, 1.5)", [3, 4]),
    "d_notin": ("d NOT IN (0.0, 1.5)", [1, 2, 6, 7, 8, 9, 10]),
    "f_eq_0": ("f = 0.0", [3]),
    "f_ne_0": ("f <> 0.0", [1, 2, 4, 6, 7, 8, 9, 10]),
    "f_lt_0": ("f < 0.0", [2, 7]),
    "f_gte_0": ("f >= 0.0", [1, 3, 4, 6, 8, 9, 10]),
    "f_between": ("f BETWEEN 0.0 AND 1.5", [3, 4, 9, 10]),
    "f_not_between": ("f NOT BETWEEN 0.0 AND 1.5", [1, 2, 6, 7, 8]),
    "f_in": ("f IN (0.0, 0.1)", [3]),
    "f_notin": ("f NOT IN (0.0, 0.1)", [1, 2, 4, 6, 7, 8, 9, 10]),
}

_MATCH_DF: tuple[str, ...] = (
    "df_d_gt",
    "df_d_lte",
    "df_d_gte",
    "df_f_eq",
    "df_f_lit",
    "df_f_gt",
    "df_dec_eq",
)

_DIVERGE_DF: dict[str, list[int]] = {
    "df_d_eq": [3],
    "df_d_lit": [3],
    "df_d_ne": [1, 2, 4, 6, 7, 8, 9, 10],
    "df_d_lt": [2, 7],
    "df_d_in": [3, 4],
    "df_d_between": [3, 4, 8, 9],
    "df_d_not_between": [1, 2, 6, 7, 10],
    "df_f_in": [3],
    "df_f_between": [3, 4, 9, 10],
}


@pytest.fixture
def seeded(tmp_path: Path) -> Any:
    """The sweep table seeded on an isolated memory catalog; yields session and name."""
    session = ReparkSession.builder.appName("pytest-decimal-float-coercion").getOrCreate()
    session.register_memory_catalog("dec_float_1", tmp_path / "warehouse")
    session.sql("CREATE NAMESPACE dec_float_1.ns")
    table = "dec_float_1.ns.sweep"
    session.sql(f"CREATE TABLE {table} (id INT, d DOUBLE, f FLOAT, dec DECIMAL(6,2)) USING iceberg")
    session.sql(f"INSERT INTO {table} VALUES {_SEED}")
    yield session, table
    session.stop()


def _oracle_ids(key: str) -> list[int]:
    """Sorted recorded Spark ids for one oracle cell."""
    return sorted(row[0] for row in _ORACLE["cells"][key]["rows"])


def _ids(table: pa.Table) -> list[int]:
    """Sorted id column as ints, failing unless the Arrow type is ``int32``."""
    assert table.schema.field("id").type == pa.int32(), table.schema
    return sorted(int(value) for value in table.column("id").to_pylist())


def _df_condition(key: str) -> Any:
    """The DataFrame-door condition for one oracle cell."""
    if key == "df_d_eq":
        return F.col("d") == 0.0
    if key == "df_d_gt":
        return F.col("d") > 1.0
    if key == "df_d_lit":
        return F.col("d") == F.lit(0.0)
    if key == "df_f_eq":
        return F.col("f") == 0.1
    if key == "df_f_lit":
        return F.col("f") == F.lit(0.1)
    if key == "df_f_gt":
        return F.col("f") > 1.0
    if key == "df_d_ne":
        return F.col("d") != 0.0
    if key == "df_d_lt":
        return F.col("d") < 0.0
    if key == "df_d_lte":
        return F.col("d") <= 0.0
    if key == "df_d_gte":
        return F.col("d") >= 1.0
    if key == "df_d_in":
        return F.col("d").isin(0.0, 1.5)
    if key == "df_d_between":
        return F.col("d").between(0.0, 2.0)
    if key == "df_d_not_between":
        return ~F.col("d").between(0.0, 2.0)
    if key == "df_f_in":
        return F.col("f").isin(0.0, 0.1)
    if key == "df_f_between":
        return F.col("f").between(0.0, 1.5)
    if key == "df_dec_eq":
        return F.col("dec") == 0.0
    raise AssertionError(f"unknown df cell {key}")


def test_sql_door_sweep_matches_spark(seeded: Any) -> None:
    """SQL-door sweep legs equal the recorded Spark answers, value and type."""
    session, table = seeded
    for key, predicate in _MATCH_SQL.items():
        got = _ids(session.sql(f"SELECT id FROM {table} WHERE {predicate} ORDER BY id").to_arrow())
        assert got == _oracle_ids(key), key
    for key, (predicate, repark) in _DIVERGE_SQL.items():
        got = _ids(session.sql(f"SELECT id FROM {table} WHERE {predicate} ORDER BY id").to_arrow())
        assert got == repark, (
            f"{key}: spark answers {_oracle_ids(key)}; "
            "the float eq kernel keeps -0.0 distinct from 0.0"
        )


def test_df_door_sweep_matches_spark(seeded: Any) -> None:
    """DataFrame-door legs equal the recorded Spark answers, value and type."""
    session, table = seeded
    frame = session.table(table)
    for key in _MATCH_DF:
        got = _ids(frame.filter(_df_condition(key)).select("id").to_arrow())
        assert got == _oracle_ids(key), key
    for key, repark in _DIVERGE_DF.items():
        got = _ids(frame.filter(_df_condition(key)).select("id").to_arrow())
        assert got == repark, (
            f"{key}: spark answers {_oracle_ids(key)}; "
            "the float eq kernel keeps -0.0 distinct from 0.0"
        )
    assert _ids(frame.filter("d = 0.0").select("id").to_arrow()) == [3], (
        f"df_d_str: spark answers {_oracle_ids('df_d_str')}; "
        "the float eq kernel keeps -0.0 distinct from 0.0"
    )
    assert _ids(frame.filter("f = 0.1").select("id").to_arrow()) == _oracle_ids("df_f_str")


def test_r_nan_filter_replica_matches_recorded_targets(tmp_path: Path) -> None:
    """The scoreboard R-NAN-FILTER cell's exact SQL hits every recorded target."""
    session = ReparkSession.builder.appName("pytest-decimal-float-r-nan").getOrCreate()
    try:
        session.register_memory_catalog("dec_float_1", tmp_path / "warehouse")
        session.sql("CREATE NAMESPACE dec_float_1.ns")
        table = "dec_float_1.ns.r_nan"
        session.sql(f"CREATE TABLE {table} (id INT, d DOUBLE) USING iceberg")
        session.sql(
            f"INSERT INTO {table} VALUES (1, CAST('NaN' AS DOUBLE)), (2, -0.0), "
            "(3, 0.0), (4, NULL), (5, 1.5)"
        )
        assert _ids(session.sql(f"SELECT id FROM {table} WHERE d = 0.0").to_arrow()) == [
            2,
            3,
        ]
        assert _ids(session.sql(f"SELECT id FROM {table} WHERE d > 1.0").to_arrow()) == [
            1,
            5,
        ]
        assert _ids(session.sql(f"SELECT id FROM {table} WHERE d IS NOT NULL").to_arrow()) == [
            1,
            2,
            3,
            5,
        ]
        assert _ids(
            session.sql(f"SELECT id FROM {table} WHERE d = CAST('NaN' AS DOUBLE)").to_arrow()
        ) == [1]
        assert _ids(session.sql(f"SELECT id FROM {table} WHERE isnan(d)").to_arrow()) == [1]
    finally:
        session.stop()
