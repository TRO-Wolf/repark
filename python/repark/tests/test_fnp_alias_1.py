"""FNP-ALIAS-1 pins: PySpark alias names over existing kernels, oracle-driven."""

from __future__ import annotations

import inspect
import json
import re
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812

FIXTURE_PATH = Path(__file__).parent / "fnp_alias_1_spark_oracle.json"
FRAME = (
    "SELECT * FROM VALUES (1, 1, CAST(10 AS BIGINT), CAST(-8 AS INT), CAST(180.0 AS DOUBLE), "
    "CAST(3.14159 AS DOUBLE), 'a'), (1, 2, CAST(10 AS BIGINT), CAST(7 AS INT), "
    "CAST(90.0 AS DOUBLE), CAST(1.0 AS DOUBLE), 'b'), (1, NULL, NULL, NULL, NULL, NULL, 'a'), "
    "(2, 3, CAST(-1 AS BIGINT), CAST(2147483647 AS INT), CAST(-45.0 AS DOUBLE), "
    "CAST(-0.5 AS DOUBLE), NULL) AS t(g, v, b, i, deg, rad, s)"
)
DELIVERED_NAMES = (
    "approxCountDistinct",
    "shiftLeft",
    "shiftRight",
    "shiftRightUnsigned",
    "toDegrees",
    "toRadians",
    "degrees",
    "radians",
)
GROUPED_NAMES = frozenset({"approxCountDistinct", "approx_count_distinct"})
DEPRECATED_MESSAGES: dict[str, str] = {
    "approxCountDistinct": "Deprecated in 2.1, use approx_count_distinct instead.",
    "shiftLeft": "Deprecated in 3.2, use shiftleft instead.",
    "shiftRight": "Deprecated in 3.2, use shiftright instead.",
    "shiftRightUnsigned": "Deprecated in 3.2, use shiftrightunsigned instead.",
    "toDegrees": "Deprecated in 2.1, use degrees instead.",
    "toRadians": "Deprecated in 2.1, use radians instead.",
}


def _oracle() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text())


def _python_door_cells() -> list[dict[str, Any]]:
    return [
        cell
        for cell in _oracle()["cells"]
        if cell["door"] == "python" and cell["ansi"] and cell["name"] in DELIVERED_NAMES
    ]


def _oracle_parameter_shapes(signature: str) -> list[tuple[str, Any]]:
    body = signature.split("->")[0].strip().removeprefix("(").removesuffix(")")
    shapes: list[tuple[str, Any]] = []
    for part in filter(None, (item.strip() for item in body.split(","))):
        name, _, default = part.partition("=")
        default_value: Any = inspect.Parameter.empty
        if default.strip() == "None":
            default_value = None
        shapes.append((name.split(":")[0].strip(), default_value))
    return shapes


def _nullable_is_pinned(field_name: str) -> bool:
    return field_name != "g" and not field_name.startswith("approx_count_distinct")


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-fnp-alias-1").getOrCreate()
    yield session
    session.stop()


def test_delivered_names_present_and_exported() -> None:
    """The delivered alias names sit on the functions module and in its ``__all__``."""
    for name in DELIVERED_NAMES:
        assert callable(getattr(F, name)), name
        assert name in F.__all__, name


@pytest.mark.parametrize("name", sorted(DEPRECATED_MESSAGES))
def test_deprecated_signature_matches_the_spark_oracle(name: str) -> None:
    """Parameter names and defaults equal PySpark 4.1.2's recorded signature."""
    expected = _oracle_parameter_shapes(_oracle()["signatures"][name])
    actual = inspect.signature(getattr(F, name)).parameters
    assert list(actual) == [parameter_name for parameter_name, _ in expected]
    for parameter_name, default in expected:
        assert actual[parameter_name].default is default


@pytest.mark.parametrize("name", sorted(DEPRECATED_MESSAGES))
def test_deprecated_alias_warns_the_spark_message(name: str) -> None:
    """Calling the deprecated alias warns ``FutureWarning`` with Spark's exact message."""
    function = getattr(F, name)
    with pytest.warns(FutureWarning, match=f"^{re.escape(DEPRECATED_MESSAGES[name])}$"):
        if name.startswith("shift"):
            function("v", 1)
        else:
            function("v")


@pytest.mark.parametrize("cell", _python_door_cells(), ids=lambda cell: cell["expr"])
def test_python_door_cell_matches_the_spark_oracle(
    spark: ReparkSession, cell: dict[str, Any]
) -> None:
    """Column name, type and rows equal the recorded Spark cell; nullability where pinned."""
    frame = spark.sql(FRAME)
    expression = eval(cell["expr"], {"F": F, "col": F.col})
    if cell["name"] in GROUPED_NAMES:
        result = frame.groupBy("g").agg(expression).orderBy("g")
    else:
        result = frame.select(expression)
    actual = [
        (field.name, field.dataType.simpleString(), field.nullable)
        for field in result.schema.fields
    ]
    expected = [(column["name"], column["type"], column["nullable"]) for column in cell["columns"]]
    assert len(actual) == len(expected)
    for actual_field, expected_field in zip(actual, expected, strict=True):
        actual_name, actual_type, actual_nullable = actual_field
        name, expected_type, expected_nullable = expected_field
        assert actual_name == name
        assert actual_type == expected_type
        if _nullable_is_pinned(actual_name):
            assert actual_nullable == expected_nullable
    assert [list(row) for row in result.collect()] == cell["rows"]


def test_sql_door_degrees_radians_values_match_the_spark_oracle(spark: ReparkSession) -> None:
    """The SQL door answers Spark's degrees/radians values and types; names are run 15c's work."""
    cells = [
        cell
        for cell in _oracle()["cells"]
        if cell["door"] == "sql" and cell["name"] == "degrees" and cell["ansi"]
    ]
    assert len(cells) == 1
    cell = cells[0]
    result = spark.sql(cell["expr"].replace("FRAME", f"({FRAME})"))
    assert [field.dataType.simpleString() for field in result.schema.fields] == [
        column["type"] for column in cell["columns"]
    ]
    assert [list(row) for row in result.collect()] == cell["rows"]
