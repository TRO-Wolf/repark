"""SQL-DOOR-SESSION-FN-1 — session functions on the Spark SQL door (value plus Arrow type)."""

from __future__ import annotations

import _live_parity as lp
import pyarrow as pa
import pytest

import repark
from repark import ReparkSession
from repark.errors import AnalysisException, ParseException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def _session() -> ReparkSession:
    return ReparkSession.builder.appName("sql-door-session-fn-1").getOrCreate()


def _table(frame: object) -> pa.Table:
    return frame.to_arrow()  # type: ignore[attr-defined]


def _is_string(field_type: pa.DataType) -> bool:
    return pa.types.is_string(field_type) or pa.types.is_large_string(field_type)


def _text_values(table: pa.Table, name: str) -> list[str]:
    return [str(value) for value in table.column(name).to_pylist()]


def test_spark_door_user_calls_answer_facade_identity() -> None:
    session = _session()
    expect = _text_values(_table(session.range(1).select(F.current_user().alias("u"))), "u")
    for call in ("user()", "current_user()", "session_user()"):
        table = _table(session.sql(f"SELECT {call}"))
        assert _text_values(table, table.schema.names[0]) == expect == ["repark"]
        assert _is_string(table.schema.field(0).type)
        assert not table.schema.field(0).nullable


def test_spark_door_version_call_answers_facade_version() -> None:
    session = _session()
    table = _table(session.sql("SELECT version()"))
    value = _text_values(table, table.schema.names[0])
    facade = _text_values(_table(session.range(1).select(F.version().alias("v"))), "v")
    assert value == facade == [session.version]
    assert value[0].startswith("repark-")
    assert "DataFusion" not in value[0]
    assert _is_string(table.schema.field(0).type)
    assert not table.schema.field(0).nullable


def test_spark_door_session_calls_inside_expressions() -> None:
    session = _session()
    session.range(3).createOrReplaceTempView("sqlsess_probe")
    table = _table(session.sql("SELECT concat(user(), 'x')"))
    assert _text_values(table, table.schema.names[0]) == ["reparkx"]
    table = _table(session.sql("SELECT concat(version(), 'x')"))
    assert _text_values(table, table.schema.names[0]) == [f"{session.version}x"]
    table = _table(session.sql("SELECT current_user() = 'repark'"))
    assert table.column(0).to_pylist() == [True]
    assert pa.types.is_boolean(table.schema.field(0).type)
    table = _table(session.sql("SELECT current_user() AS c, id FROM sqlsess_probe ORDER BY id"))
    assert _text_values(table, "c") == ["repark", "repark", "repark"]
    assert table.column("id").to_pylist() == [0, 1, 2]
    table = _table(session.sql("SELECT id FROM sqlsess_probe WHERE current_user() = 'repark'"))
    assert table.num_rows == 3
    table = _table(
        session.sql("SELECT id FROM sqlsess_probe WHERE current_user() = 'nosuchuser_xyz'")
    )
    assert table.num_rows == 0


def test_spark_door_bare_names_keep_column_or_error() -> None:
    session = _session()
    session.sql(
        "SELECT 'colval' AS user, 'cval' AS current_user, 'sval' AS session_user, 'vval' AS version"
    ).createOrReplaceTempView("sqlsess_shadow")
    table = _table(
        session.sql("SELECT user, current_user, session_user, version FROM sqlsess_shadow")
    )
    assert _text_values(table, "user") == ["colval"]
    assert _text_values(table, "current_user") == ["cval"]
    assert _text_values(table, "session_user") == ["sval"]
    assert _text_values(table, "version") == ["vval"]
    with pytest.raises(AnalysisException, match="No field named user"):
        session.sql("SELECT user").to_arrow()


def test_spark_door_user_call_arity_refuses() -> None:
    session = _session()
    with pytest.raises(AnalysisException, match="user"):
        session.sql("SELECT user(1)").to_arrow()


@pytest.mark.parametrize(
    ("name", "sql"),
    [
        ("user", "SELECT user() OVER () FROM sqlsess_window"),
        (
            "current_user",
            "SELECT current_user() OVER (PARTITION BY id) FROM sqlsess_window",
        ),
        (
            "session_user",
            "SELECT session_user() OVER w FROM sqlsess_window WINDOW w AS (ORDER BY id)",
        ),
        ("version", "SELECT concat(version() OVER (), 'x') FROM sqlsess_window"),
        ("user", 'SELECT "user"() OVER () FROM sqlsess_window'),
        ("version", 'SELECT "version"() OVER () FROM sqlsess_window'),
    ],
)
def test_spark_door_session_scalars_refuse_window_use(name: str, sql: str) -> None:
    """Reject zero-argument session scalars used as windows."""
    session = _session()
    session.range(3).createOrReplaceTempView("sqlsess_window")
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_EXPR_FOR_WINDOW") as raised:
        session.sql(sql).to_arrow()
    assert f'"{name}()"' in str(raised.value)
    assert "SQLSTATE: 42P20" in str(raised.value)


@pytest.mark.parametrize("name", ["user", "current_user", "session_user", "version"])
def test_spark_door_session_scalar_window_arguments_reach_planner(name: str) -> None:
    """Keep argument-bearing session scalar windows on planner validation."""
    session = _session()
    session.range(1).createOrReplaceTempView("sqlsess_window")
    with pytest.raises(AnalysisException) as raised:
        session.sql(f"SELECT {name}(1) OVER () FROM sqlsess_window").to_arrow()
    assert "UNSUPPORTED_EXPR_FOR_WINDOW" not in str(raised.value)
    assert name in str(raised.value)
    with pytest.raises(AnalysisException) as raised:
        session.sql("SELECT user(DISTINCT) OVER () FROM sqlsess_window").to_arrow()
    assert "UNSUPPORTED_EXPR_FOR_WINDOW" not in str(raised.value)


def test_spark_door_real_window_and_quoted_shadow_column_work() -> None:
    """Keep a real window and quoted shadow column valid."""
    session = _session()
    session.range(3).createOrReplaceTempView("sqlsess_window")
    table = _table(session.sql("SELECT row_number() OVER (ORDER BY id) AS n FROM sqlsess_window"))
    assert table.column("n").to_pylist() == [1, 2, 3]
    session.sql("SELECT 'colval' AS user").createOrReplaceTempView("sqlsess_quoted_shadow")
    table = _table(session.sql('SELECT "user" FROM sqlsess_quoted_shadow'))
    assert _text_values(table, "user") == ["colval"]


def test_native_door_session_cells_unchanged() -> None:
    with pytest.raises(ParseException, match=r"Expected: end of statement, found: \("):
        repark.sql("SELECT user()").to_arrow()
    table = repark.sql("SELECT version()").to_arrow()
    value = _text_values(table, table.schema.names[0])
    assert len(value) == 1
    assert "DataFusion" in value[0]
    table = repark.sql("SELECT version() OVER ()").to_arrow()
    assert "DataFusion" in _text_values(table, table.schema.names[0])[0]


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_session_function_shapes_match_spark(spark_engine: lp.Engine) -> None:
    spark_user = spark_engine.session.sql("SELECT user()").collect()[0][0]
    spark_current = spark_engine.session.sql("SELECT current_user()").collect()[0][0]
    spark_session_user = spark_engine.session.sql("SELECT session_user()").collect()[0][0]
    spark_version = spark_engine.session.sql("SELECT version()").collect()[0][0]
    assert isinstance(spark_user, str) and spark_user != ""
    assert spark_current == spark_user
    assert spark_session_user == spark_user
    assert isinstance(spark_version, str) and spark_version != ""
    session = _session()
    door_user = session.sql("SELECT user()").collect()[0][0]
    door_current = session.sql("SELECT current_user()").collect()[0][0]
    door_session_user = session.sql("SELECT session_user()").collect()[0][0]
    door_version = session.sql("SELECT version()").collect()[0][0]
    assert isinstance(door_user, str) and door_user != ""
    assert door_current == door_user
    assert door_session_user == door_user
    assert isinstance(door_version, str) and door_version != ""
    facade_version = session.range(1).select(F.version().alias("v")).collect()[0][0]
    assert door_version == facade_version


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
@pytest.mark.parametrize("name", ["user", "current_user", "session_user", "version"])
def test_live_session_scalar_window_refusal(spark_engine: lp.Engine, name: str) -> None:
    """Keep the session-window refusal aligned with the live Spark oracle."""
    from pyspark.errors import AnalysisException as SparkAnalysisException

    query = f"SELECT {name}() OVER ()"
    with pytest.raises(SparkAnalysisException, match="UNSUPPORTED_EXPR_FOR_WINDOW") as spark_raised:
        spark_engine.session.sql(query).toArrow()
    assert f'"{name}()"' in str(spark_raised.value)
    assert "SQLSTATE: 42P20" in str(spark_raised.value)
    with pytest.raises(AnalysisException, match="UNSUPPORTED_EXPR_FOR_WINDOW") as repark_raised:
        _session().sql(query).to_arrow()
    assert f'"{name}()"' in str(repark_raised.value)
    assert "SQLSTATE: 42P20" in str(repark_raised.value)
