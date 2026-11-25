from __future__ import annotations

import json
import math
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from repark.spark import SparkSession
from repark.spark import functions as spark_functions
from repark.spark.session import _reset_active_session_for_tests

pytestmark = pytest.mark.xdist_group("spark_door_over_accept_syntax_1")

ORACLE_PATH = Path(__file__).with_name("spark_door_over_accept_syntax_1_spark_oracle.json")
ORACLE: list[dict[str, Any]] = json.loads(ORACLE_PATH.read_text())
STATEMENT_DOORS = {"statement", "dml"}


def normalize_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return "NaN" if math.isnan(value) else round(value, 6)
    if isinstance(value, Decimal):
        return f"Dec({value})"
    if isinstance(value, (list, tuple)):
        return [normalize_value(item) for item in value]
    if hasattr(value, "asDict"):
        return {key: normalize_value(item) for key, item in value.asDict().items()}
    return str(value)


def run_frame(build: Callable[[], Any]) -> dict[str, Any]:
    try:
        frame = build()
        fields = [
            [field.name, field.dataType.simpleString(), bool(field.nullable)]
            for field in frame.schema.fields
        ]
        rows = [[normalize_value(cell) for cell in row] for row in frame.collect()]
    except Exception as exc:
        return {"kind": "error", "text": f"{type(exc).__name__}: {exc}"}
    return {"kind": "ok", "fields": fields, "rows": rows}


def active_session(mode: str, root: Path) -> tuple[Any, Any]:
    _reset_active_session_for_tests()
    session = (
        SparkSession.builder.appName(f"spark-door-over-accept-{mode}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", mode)
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", str(root / f"warehouse-{mode}"))
        .getOrCreate()
    )
    session.sql("CREATE NAMESPACE IF NOT EXISTS ice.ns").collect()
    base = session.createDataFrame([(1, 2, "notanint", 1)], "a INT, b INT, s STRING, drop INT")
    base.createOrReplaceTempView("bt")
    return session, base


def expression_column(text: str) -> Any:
    if text.endswith(" AS v"):
        return spark_functions.expr(text[: -len(" AS v")]).alias("v")
    return spark_functions.expr(text)


def run_view(session: Any, text: str) -> dict[str, Any]:
    try:
        session.sql(f"CREATE OR REPLACE TEMP VIEW over_accept_body AS {text}").collect()
    except Exception as exc:
        return {"kind": "error", "text": f"{type(exc).__name__}: {exc}"}
    return run_frame(lambda: session.sql("SELECT * FROM over_accept_body"))


def run_statement(session: Any, record: dict[str, Any]) -> dict[str, Any]:
    for statement in record["setup"]:
        session.sql(statement).collect()
    result = run_frame(lambda: session.sql(record["text"]))
    if record["after"]:
        result["after"] = run_frame(lambda: session.sql(record["after"]))
    return result


def run_dml(session: Any, record: dict[str, Any]) -> dict[str, Any]:
    table = f"t_{record['id'].lower()}_{record['ansi']}"
    session.sql(f"DROP TABLE IF EXISTS ice.ns.{table}").collect()
    session.sql(f"CREATE TABLE ice.ns.{table} (a BIGINT, b DOUBLE) USING iceberg").collect()
    session.sql(f"INSERT INTO ice.ns.{table} VALUES (1, 10.0), (2, 20.0), (3, 30.0)").collect()
    result = run_frame(lambda: session.sql(record["text"].format(t=table)))
    result["after"] = run_frame(lambda: session.sql(f"SELECT a, b FROM ice.ns.{table} ORDER BY a"))
    return result


def run_record(session: Any, base: Any, record: dict[str, Any]) -> dict[str, Any]:
    door = record["door"]
    text = record["text"]
    if door == "sql":
        return run_frame(lambda: session.sql(text))
    if door == "view":
        return run_view(session, text)
    if door == "expr":
        return run_frame(lambda: base.select(expression_column(text)))
    if door == "selectExpr":
        return run_frame(lambda: base.selectExpr(text))
    if door == "filter":
        return run_frame(lambda: base.filter(text))
    if door == "statement":
        return run_statement(session, record)
    return run_dml(session, record)


def assert_outcome(
    expected: dict[str, Any], got: dict[str, Any] | None, record: dict[str, Any], label: str
) -> None:
    assert got is not None, f"{label}: no outcome"
    if expected["kind"] == "error":
        assert got["kind"] == "error", f"{label}: Spark refuses ({expected['class']}), got {got}"
        if record["pin_class"]:
            assert f"[{record['pin_class']}" in got["text"], f"{label}: {got['text']}"
        return
    assert got["kind"] == "ok", f"{label}: Spark answers {expected}, got {got}"
    assert got["fields"] == expected["fields"], f"{label}: {got['fields']} != {expected}"
    assert got["rows"] == expected["rows"], f"{label}: {got['rows']} != {expected}"


@pytest.fixture(scope="module")
def warehouse_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("spark_door_over_accept_syntax_1")


@pytest.mark.parametrize(
    "record",
    ORACLE,
    ids=[f"{record['id']}-{record['door']}-ansi_{record['ansi']}" for record in ORACLE],
)
def test_spark_door_cell_answers_as_spark(record: dict[str, Any], warehouse_root: Path) -> None:
    session, base = active_session(record["ansi"], warehouse_root)
    got = run_record(session, base, record)
    label = f"{record['id']} {record['door']} ansi={record['ansi']} {record['text']!r}"
    if record["door"] in STATEMENT_DOORS:
        if record["spark"]["kind"] == "ok":
            assert got["kind"] == "ok", f"{label}: Spark answers, got {got}"
        else:
            assert_outcome(record["spark"], got, record, label)
        if "spark_after" in record:
            after_record = dict(record, pin_class=None)
            assert_outcome(record["spark_after"], got.get("after"), after_record, f"{label} after")
        return
    assert_outcome(record["spark"], got, record, label)


def test_the_oracle_covers_every_door_and_both_ansi_modes() -> None:
    doors = {record["door"] for record in ORACLE}
    assert doors == {"sql", "view", "expr", "selectExpr", "filter", "statement", "dml"}
    assert {record["ansi"] for record in ORACLE} == {"true", "false"}
    assert sum(1 for record in ORACLE if record["fixed"]) > 0
