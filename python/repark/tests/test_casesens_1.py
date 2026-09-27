"""WO CASESENS-1 slice 1: nested scopes keep the written spelling; MERGE folds its source.

The oracle is ``casesens_1_spark_oracle.json`` (Spark 4.1.2 + Iceberg 1.11.0,
measured 2026-09-27): each step carries its door, statement, the
``caseSensitive`` value in force, and Spark's answer. Every success step
compares column names, types and rows exactly after the probe's normalization
(values normalized as the probe normalized them, rows sorted by repr). DML and
DDL success steps assert success only: RePark's empty-result framing (``[[]]``
where Spark answers ``[]``, a count row for INSERT) predates this unit and is
out of its fence. Refusal steps compare the class, condition, SQLSTATE, the
message head up to ``Did you mean one of the following? [`` and the candidate
set (R12), after stripping Spark's ``; line L pos P`` suffix and RePark's
``Error during planning: `` prefix (R13). S1b lands R-CS1-1 and C-003:
``p1/r5_cte_outer`` joins the nested legs and the catalog-view legs replay in
``test_s1_catalog_view_keeps_its_spelling``.

pins: casesens-1/C-001, C-002, C-003, C-004
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession

ORACLE_PATH: Path = Path(__file__).with_name("casesens_1_spark_oracle.json")
_ORACLE: dict[str, Any] = json.loads(ORACLE_PATH.read_text(encoding="utf-8"))["steps"]

_NESTED_KEYS: tuple[str, ...] = (
    "p1/r5_subq_inner_ID",
    "p1/r5_cte_ID",
    "p1/r5_cte_outer",
    "p1/r5_cte_mixed",
    "p1/r5_union_subq",
    "p1/r5_subq_ID",
    "p1/r5_subq_both",
    "p1/r5_subq_alias_qual",
    "p1/r5_in_subq",
    "p1/r5_temp_view_star",
    "p1/r5_temp_view_lower",
    "p3/n_nested2",
    "p3/n_join_derived",
    "p3/n_col_alias_list",
    "p3/n_df_sql_subq",
    "p3/n_group_having",
    "p3/n_cte_star_outer_upper",
    "p3/n_exists",
)

_SPARK_POSITION_SUFFIX: re.Pattern[str] = re.compile(r"; line \d+ pos \d+")
_REPARK_PREFIX: str = "Error during planning: "
_SUGGESTION_MARK: str = "Did you mean one of the following? ["


def _open(warehouse: Path) -> ReparkSession:
    """Open a session with hadoop catalog sc."""
    return (
        ReparkSession.builder.appName("casesens-1-s1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", str(warehouse / "sc"))
        .getOrCreate()
    )


def _setup_tables(session: ReparkSession) -> None:
    """Create the probe's sc tables and the tv temp view."""
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    session.sql(
        "CREATE TABLE sc.ns.t (id INT, Data STRING, s STRUCT<a: INT>) USING iceberg"
    ).collect()
    session.sql(
        "INSERT INTO sc.ns.t VALUES (1, 'a', named_struct('a', 5)), (2, 'b', named_struct('a', 6))"
    ).collect()
    session.sql("CREATE TABLE sc.ns.u (id INT, Data STRING) USING iceberg").collect()
    session.sql("INSERT INTO sc.ns.u VALUES (1, 'x'), (5, 'y')").collect()
    session.sql("CREATE OR REPLACE TEMPORARY VIEW tv AS SELECT ID, DATA FROM sc.ns.t").collect()


def _norm(value: Any) -> Any:
    """Normalize one collected value the way the recording probe did."""
    if hasattr(value, "asDict"):
        return {key: _norm(item) for key, item in value.asDict().items()}
    if isinstance(value, (list, tuple)):
        return [_norm(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _norm(item) for key, item in value.items()}
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return repr(value)


def _rows(frame: Any) -> list[list[Any]]:
    """Collect a frame into probe-normalized rows sorted by repr."""
    return sorted([[_norm(value) for value in row] for row in frame.collect()], key=repr)


def _dtypes(frame: Any) -> list[list[str]]:
    """Read a frame's dtypes as name/type pairs."""
    return [[name, dtype] for name, dtype in frame.dtypes]


def _condition(error: BaseException) -> Any:
    """Read the attached Spark condition, if the error carries one."""
    method = getattr(error, "getCondition", None)
    if not callable(method):
        return None
    return method()


def _sql_state(error: BaseException) -> Any:
    """Read the attached SQLSTATE, if the error carries one."""
    method = getattr(error, "getSqlState", None)
    if not callable(method):
        return None
    return method()


def _plain_message(message: str) -> str:
    """Strip the engine framings R13 registers, keeping the first line."""
    first = message.splitlines()[0]
    first = first.removeprefix(_REPARK_PREFIX)
    return _SPARK_POSITION_SUFFIX.sub("", first)


def _candidates(message: str) -> set[str]:
    """Read the suggestion-list candidate set from a refusal message."""
    _, _, tail = message.partition(_SUGGESTION_MARK)
    head, _, _ = tail.partition("]")
    return {entry.strip() for entry in head.split(",") if entry.strip()}


def _assert_error(key: str, error: BaseException, spark: dict[str, Any]) -> None:
    """Replay one oracle refusal against Spark's recorded answer per R12."""
    assert type(error).__name__ == spark["error"], key
    assert _condition(error) == spark.get("getCondition"), key
    assert _sql_state(error) == spark.get("getSqlState"), key
    mine = _plain_message(str(error))
    want = _plain_message(spark["msg"])
    assert mine.split(_SUGGESTION_MARK)[0] == want.split(_SUGGESTION_MARK)[0], key
    if _SUGGESTION_MARK in want:
        assert _candidates(mine) == _candidates(want), key


def _sql_of(step: dict[str, Any]) -> str:
    """Read the runnable SQL of an oracle step, unwrapping dataframe lambdas."""
    statement = step["statement"]
    prefix = 'lambda: S.sql("'
    if statement.startswith(prefix) and statement.endswith('")'):
        return statement[len(prefix) : -len('")')]
    return statement


def _assert_step(session: ReparkSession, key: str) -> None:
    """Run one oracle step under its recorded flag and compare with Spark."""
    step = _ORACLE[key]
    session.conf.set("spark.sql.caseSensitive", "true" if step["case_sensitive"] else "false")
    spark = step["spark"]
    if "error" in spark:
        try:
            session.sql(_sql_of(step)).collect()
        except Exception as error:
            _assert_error(key, error, spark)
        else:
            raise AssertionError(f"{key} answered instead of refusing")
        return
    frame = session.sql(_sql_of(step))
    if spark["cols"]:
        assert _dtypes(frame) == spark["cols"], key
        assert _rows(frame) == spark["rows"], key
    else:
        frame.collect()


@pytest.mark.parametrize("key", list(_NESTED_KEYS))
def test_s1_nested_scopes_keep_the_written_spelling(key: str, tmp_path: Path) -> None:
    """Each S1 nested-scope SELECT replays Spark's names and rows."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        _assert_step(session, key)
    finally:
        session.stop()


def test_s1_catalog_view_keeps_its_spelling(tmp_path: Path) -> None:
    """Each C-003 catalog-view step replays Spark's names and rows (S1b)."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        session.register_memory_catalog("vc", tmp_path / "vc")
        for key in (
            "p1/vc_ns",
            "p1/vc_t",
            "p1/vc_t_ins",
            "p1/r5_view_create",
            "p1/r5_view_star",
            "p1/r5_view_lower",
            "p1/r5_view_describe",
            "p1/r5_view2_create",
            "p1/r5_view2_upper",
        ):
            _assert_step(session, key)
    finally:
        session.stop()


def test_s1_merge_derived_source(tmp_path: Path) -> None:
    """MERGE over a derived source spelled in another case replays Spark."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        session.sql("CREATE TABLE sc.ns.m (id INT, Data STRING) USING iceberg").collect()
        session.sql("INSERT INTO sc.ns.m VALUES (1, 'p')").collect()
        _assert_step(session, "p1/r30_merge_derived")
        _assert_step(session, "p1/r30_after")
        _assert_step(session, "p1/r30_merge_upper_set")
        _assert_step(session, "p1/r30_after2")
    finally:
        session.stop()
