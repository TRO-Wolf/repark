from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark import functions as spark_functions
from repark.spark.session import _reset_active_session_for_tests
from repark.spark.types import TimestampNTZType, TimestampType

FIXTURE = Path(__file__).with_name("fexpr_timestamp_ntz_1_narrow_spark.json")
_CELLS: dict[str, dict[str, Any]] = json.loads(FIXTURE.read_text(encoding="utf-8"))["cells"]
_ZONE = "America/New_York"
_GAP = "2024-03-10 02:30:00"

pytestmark = pytest.mark.xdist_group("test_fexpr_timestamp_ntz_1_narrow")


def _keys(*kinds: str) -> list[str]:
    return sorted(key for key, cell in _CELLS.items() if cell["kind"] in kinds)


def _session(session: str, ansi: str, warehouse: Path) -> Any:
    _reset_active_session_for_tests()
    timestamp_type = "TIMESTAMP_NTZ" if session == "ntz_built" else "TIMESTAMP_LTZ"
    spark = (
        ReparkSession.builder.appName("fexpr-timestamp-ntz-1-narrow")
        .config("spark.sql.catalog.fx", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.fx.type", "hadoop")
        .config("spark.sql.catalog.fx.warehouse", str(warehouse))
        .config("spark.sql.session.timeZone", _ZONE)
        .config("spark.sql.ansi.enabled", ansi)
        .config("spark.sql.timestampType", timestamp_type)
        .getOrCreate()
    )
    if session == "rt_after":
        spark.conf.set("spark.sql.timestampType", "TIMESTAMP_NTZ")
    return spark


def _frame(spark: Any, value: str) -> Any:
    base = spark.createDataFrame([(value,)], "s string")
    return base.select(
        "s",
        base.s.cast(TimestampType()).alias("t"),
        base.s.cast(TimestampNTZType()).alias("n"),
    )


def _observe(frame: Any) -> dict[str, Any]:
    text = [row[0] for row in frame.selectExpr("CAST(v AS STRING)").collect()]
    field = frame.schema.fields[0]
    return {"dtype": frame.dtypes[0][1], "nullable": bool(field.nullable), "text": text}


def _measure(spark: Any, cell: dict[str, Any], ordinal: int, root: Path) -> dict[str, Any]:
    kind = cell["kind"]
    if kind == "matrix":
        frame = _frame(spark, cell["value"])
        if cell["door"] == "select":
            return _observe(frame.select(spark_functions.expr(cell["expr"]).alias("v")))
        frame.createOrReplaceTempView("fx_src")
        return _observe(spark.sql(f"SELECT {cell['expr']} AS v FROM fx_src"))
    if kind == "route_sql":
        spark.createDataFrame([(cell["input"],)], "s string").createOrReplaceTempView("fx_src")
        return _observe(spark.sql(f"SELECT {cell['expr']} AS v FROM fx_src"))
    if kind == "harden":
        frame = spark.createDataFrame([(_GAP, "é")], "s string, `café` string")
        observed = _observe(frame.select(spark_functions.expr(cell["expr"]).alias("v")))
        return {"dtype": observed["dtype"], "text": observed["text"]}
    if kind == "write":
        frame = _frame(spark, _GAP).select(
            spark_functions.lit(ordinal).alias("id"),
            spark_functions.expr(cell["expr"]).alias("v"),
        )
        if cell["sink"] == "parquet":
            path = root / f"pq_{ordinal}"
            frame.write.parquet(str(path))
            stored = spark.read.parquet(str(path))
        else:
            table = f"fx.w.t_{ordinal}"
            spark.sql(f"CREATE TABLE {table} (id INT, v TIMESTAMP_NTZ) USING iceberg")
            frame.writeTo(table).append()
            stored = spark.table(table)
        return {
            "stored": stored.selectExpr("CAST(v AS STRING)").collect()[0][0],
            "type": stored.select("v").dtypes[0][1],
        }
    frame = _frame(spark, _GAP).select(spark_functions.expr(cell["expr"]).alias("v"))
    spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")
    try:
        view = frame.selectExpr("CAST(v AS STRING)", "hour(v)", "unix_timestamp(v)")
        rows = [[str(value) for value in row] for row in view.collect()]
    finally:
        spark.conf.set("spark.sql.session.timeZone", _ZONE)
    return {"dtype": frame.dtypes[0][1], "rows": rows}


def _value_for(cell: dict[str, Any]) -> dict[str, Any]:
    if cell["kind"] == "write":
        return {"stored": cell["stored"], "type": cell["type"]}
    if cell["kind"] == "zone":
        return {"dtype": cell["dtype"], "rows": cell["rows"]}
    if cell["kind"] == "harden":
        return {"dtype": cell["dtype"], "text": cell["text"]}
    return {"dtype": cell["dtype"], "nullable": cell["nullable"], "text": cell["text"]}


@pytest.fixture(scope="module")
def measured(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, Any]]:
    root = tmp_path_factory.mktemp("fexpr-timestamp-ntz-1-narrow")
    groups: dict[tuple[str, str], list[str]] = {}
    for key, cell in _CELLS.items():
        groups.setdefault((cell["session"], cell.get("ansi", "true")), []).append(key)
    results: dict[str, Any] = {}
    for ordinal, ((session, ansi), keys) in enumerate(sorted(groups.items())):
        spark = _session(session, ansi, root / f"wh_{ordinal}")
        try:
            spark.sql("CREATE NAMESPACE IF NOT EXISTS fx.w")
            for index, key in enumerate(sorted(keys)):
                try:
                    results[key] = _measure(spark, _CELLS[key], ordinal * 10_000 + index, root)
                except Exception as error:
                    results[key] = {"error": type(error).__name__, "message": str(error)[:300]}
        finally:
            spark.stop()
            _reset_active_session_for_tests()
    yield results


def test_the_oracle_fixture_covers_every_door() -> None:
    kinds = {cell["kind"] for cell in _CELLS.values()}
    assert kinds == {"matrix", "route_sql", "harden", "write", "zone"}
    assert sum(cell["class"] == "TARGET" for cell in _CELLS.values()) > 200


@pytest.mark.parametrize("key", _keys("matrix", "route_sql", "harden"))
def test_fragment_and_sql_door_types_and_values_match_spark(
    measured: dict[str, Any], key: str
) -> None:
    assert measured[key] == _value_for(_CELLS[key]), key


@pytest.mark.parametrize("key", _keys("write"))
def test_fragment_writes_store_spark_ntz_walls(measured: dict[str, Any], key: str) -> None:
    assert measured[key] == _value_for(_CELLS[key]), key


@pytest.mark.parametrize("key", _keys("zone"))
def test_fragment_ntz_walls_survive_a_session_zone_change(
    measured: dict[str, Any], key: str
) -> None:
    assert measured[key] == _value_for(_CELLS[key]), key


def test_the_sql_door_keeps_its_own_ntz_cast_route(tmp_path: Path) -> None:
    spark = _session("ntz_built", "true", tmp_path / "wh")
    try:
        spark.createDataFrame([(_GAP,)], "s string").createOrReplaceTempView("fx_src")
        planned = spark.sql("SELECT CAST(s AS TIMESTAMP) AS v FROM fx_src")
        assert planned.dtypes == [("v", "timestamp_ntz")]
        assert planned.selectExpr("CAST(v AS STRING)").collect()[0][0] == _GAP
    finally:
        spark.stop()
        _reset_active_session_for_tests()
