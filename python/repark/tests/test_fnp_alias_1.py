"""FNP-ALIAS-1 pins: PySpark alias names over existing kernels, oracle-driven."""

from __future__ import annotations

import inspect
import json
import re
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
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
_MISSING_APPEAR = "MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION"
_RESCALED_VALUES_FOR_ONE: dict[str, float] = {
    "degrees": 57.29577951308232,
    "radians": 0.017453292519943295,
    "toDegrees": 57.29577951308232,
    "toRadians": 0.017453292519943295,
}
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


@pytest.mark.parametrize("name", ["degrees", "radians", "toDegrees", "toRadians"])
@pytest.mark.parametrize("how", ["leftsemi", "leftanti"])
def test_rescaled_right_ref_raises_after_semi_family_join(
    spark: ReparkSession, name: str, how: str
) -> None:
    """Right-parent degrees/radians after semi/anti raise the class ``F.abs`` raises."""
    left = spark.createDataFrame([(1, 1.0), (2, 2.0), (None, 3.0)], ["k", "a"])
    right = spark.createDataFrame([(1,), (9,)], ["k"])
    joined = left.join(right, on="k", how=how)
    function = getattr(F, name)
    with pytest.raises(AnalysisException, match=_MISSING_APPEAR) as excinfo:
        joined.select(function(right["k"]))
    assert 'Resolved attribute(s) "k"' in str(excinfo.value)
    with pytest.raises(AnalysisException, match=_MISSING_APPEAR):
        joined.filter(function(right["k"]) > 0)


@pytest.mark.parametrize("name", ["degrees", "radians", "toDegrees", "toRadians"])
def test_rescaled_left_ref_still_resolves_after_semi_family_join(
    spark: ReparkSession, name: str
) -> None:
    """The origin thread must not refuse a left-parent column after a semi/anti join."""
    left = spark.createDataFrame([(1, 1.0), (2, 2.0), (None, 3.0)], ["k", "a"])
    right = spark.createDataFrame([(1,), (9,)], ["k"])
    joined = left.join(right, on="k", how="leftsemi")
    function = getattr(F, name)
    table = joined.select(function(left["k"]).alias("d")).to_arrow()
    assert table.column("d").to_pylist() == [_RESCALED_VALUES_FOR_ONE[name]]


def test_inner_join_on_degrees_both_sides_resolves_and_matches(spark: ReparkSession) -> None:
    """An ON clause using degrees on both sides binds each side and returns the matching row."""
    left = spark.createDataFrame([(1, 1.0), (2, 2.0)], ["k", "x"])
    right = spark.createDataFrame([(10, 1.0), (20, 0.5)], ["k", "x"])
    joined = left.join(right, F.degrees(left["x"]) == F.degrees(right["x"]))
    table = joined.select(left["k"].alias("lk"), right["k"].alias("rk")).to_arrow()
    assert table.to_pydict() == {"lk": [1], "rk": [10]}
