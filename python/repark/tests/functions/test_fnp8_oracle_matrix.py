"""Recorded and live Spark contracts for the FNP-8 public entry points."""

from __future__ import annotations

import base64
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import _live_parity as lp
import pyarrow as pa
import pytest
from pydantic import BaseModel, Field, TypeAdapter
from test_fnp_8_sql_door import FRAME_SQL, MATRIX

from repark import ReparkSession
from repark.errors import PySparkException
from repark.spark import functions as spark_functions
from repark.spark.column import Column


class OracleCell(BaseModel):
    """One measured Spark expression, configuration, value, and Arrow schema."""

    group: str
    name: str
    ansi: str
    expression: str
    rows: list[dict[str, Any]]
    schema_base64: str


class ReparkDisposition(BaseModel):
    """The exact RePark schema or named refusal recorded for an oracle cell."""

    schema_base64: str | None = None
    error: str | None = None
    error_type: str | None = None
    rows: list[dict[str, Any]] | None = None


class ErrorOracleCell(BaseModel):
    """One measured SQL success or error from the pinned Spark oracle."""

    ansi: str
    spark_sql: str
    rows: list[dict[str, Any]] | None = None
    schema_text: str | None = Field(default=None, alias="schema")
    error: str | None = None
    message: str | None = None


class ErrorOracleEvidence(BaseModel):
    """The live SQL error measurement collection."""

    results: list[ErrorOracleCell]


def _load_oracle() -> list[OracleCell]:
    """Read the immutable live Spark measurements."""
    return TypeAdapter(list[OracleCell]).validate_json(
        Path(__file__).with_name("fnp8_spark_oracle.json").read_text()
    )


def _cell_id(cell: OracleCell) -> str:
    """Identify a measured expression and ANSI setting."""
    return f"{cell.group}-{cell.name}-ansi_{cell.ansi}"


def _read_schema(encoded: str) -> pa.Schema:
    """Decode the complete recorded Arrow schema."""
    return pa.ipc.read_schema(pa.BufferReader(base64.b64decode(encoded)))


def _assert_table(table: pa.Table, cell: OracleCell, encoded_schema: str) -> None:
    """Compare public Arrow values and the complete measured schema."""
    assert json.loads(json.dumps(table.to_pylist())) == cell.rows
    assert table.schema.equals(_read_schema(encoded_schema)), str(table.schema)


def _string_column() -> Column:
    """Build the nullable string lambda case."""
    return spark_functions.transform(
        spark_functions.array(spark_functions.lit("a"), spark_functions.lit(None).cast("string")),
        lambda element: spark_functions.upper(element),
    )


def _struct_column() -> Column:
    """Build the struct-field lambda case."""
    return spark_functions.transform(
        spark_functions.array(
            spark_functions.struct(spark_functions.lit(1).alias("n")),
            spark_functions.struct(spark_functions.lit(2).alias("n")),
        ),
        lambda element: element.getField("n") + 1,
    )


def _nested_array_column() -> Column:
    """Build the nested array lambda case."""
    return spark_functions.transform(
        spark_functions.array(
            spark_functions.array(spark_functions.lit(1), spark_functions.lit(2)),
            spark_functions.array(spark_functions.lit(3)),
        ),
        lambda element: spark_functions.size(element),
    )


def _nested_map_column() -> Column:
    """Build the nested map lambda case."""
    return spark_functions.transform(
        spark_functions.array(
            spark_functions.create_map(spark_functions.lit("a"), spark_functions.lit(1)),
            spark_functions.create_map(spark_functions.lit("b"), spark_functions.lit(2)),
        ),
        lambda element: spark_functions.element_at(element, spark_functions.lit("a")),
    )


BINDING_COLUMNS: dict[str, Callable[[], Column]] = {
    "string": _string_column,
    "struct": _struct_column,
    "nested_array": _nested_array_column,
    "nested_map": _nested_map_column,
}
ORACLE = _load_oracle()
CELLS = [
    (cell, door)
    for cell in ORACLE
    for door in (
        ("sql", "expr", "column", "sql_columns")
        if cell.group == "forms"
        else (("sql", "expr", "column") if cell.name in BINDING_COLUMNS else ("sql", "expr"))
    )
]


def _column_query(cell: OracleCell) -> str:
    """Build the SQL case over a subquery with named input columns."""
    expression = next(expression for name, expression, _ in MATRIX if name == cell.name)
    return f"SELECT {expression} AS r FROM ({FRAME_SQL}) AS t"


def _repark_table(session: ReparkSession, cell: OracleCell, door: str) -> pa.Table:
    """Execute one expression through the selected public entry point."""
    if door == "sql_columns":
        return session.sql(_column_query(cell)).toArrow()
    if door == "sql":
        return session.sql(f"SELECT {cell.expression} AS r").toArrow()
    if door == "expr":
        return session.range(1).select(spark_functions.expr(cell.expression).alias("r")).toArrow()
    if cell.group == "forms":
        builder = next(builder for name, _, builder in MATRIX if name == cell.name)
        return session.sql(FRAME_SQL).select(builder("a", "b", "m1", "m2").alias("r")).toArrow()
    return session.range(1).select(BINDING_COLUMNS[cell.name]().alias("r")).toArrow()


@pytest.mark.parametrize("cell,door", CELLS, ids=[f"{_cell_id(c)}-{d}" for c, d in CELLS])
def test_fnp8_recorded_parity_and_named_divergences(cell: OracleCell, door: str) -> None:
    """Pin Spark values and the measured schema or explicit refusal for each RePark door."""
    dispositions = TypeAdapter(dict[str, ReparkDisposition]).validate_json(
        Path(__file__).with_name("fnp8_repark_dispositions.json").read_text()
    )
    disposition = dispositions[f"{_cell_id(cell)}-{door}"]
    session = ReparkSession.builder.config("spark.sql.ansi.enabled", cell.ansi).getOrCreate()
    try:
        if disposition.error is not None:
            with pytest.raises(PySparkException) as caught:
                _repark_table(session, cell, door)
            assert str(caught.value) == disposition.error
            assert type(caught.value).__name__ == disposition.error_type
        else:
            assert disposition.schema_base64 is not None
            _assert_table(_repark_table(session, cell, door), cell, disposition.schema_base64)
    finally:
        session.stop()


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
@pytest.mark.parametrize("cell", ORACLE, ids=_cell_id)
def test_live_fnp8_recorded_oracle(spark_engine: lp.Engine, cell: OracleCell) -> None:
    """Remeasure every recorded value and complete Arrow schema on the pinned live oracle."""
    with lp.spark_session_conf(spark_engine, (("spark.sql.ansi.enabled", cell.ansi),)):
        queries = [f"SELECT {cell.expression} AS r"]
        if cell.group == "forms":
            queries.append(_column_query(cell))
        for query in queries:
            table = spark_engine.arrow_of(spark_engine.session.sql(query))
            _assert_table(table, cell, cell.schema_base64)


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_fnp8_sql_error_oracle(spark_engine: lp.Engine) -> None:
    """Remeasure arity, accumulator typing, and overflow outcomes under both ANSI settings."""
    from pyspark.errors import PySparkException as SparkError

    evidence = ErrorOracleEvidence.model_validate_json(
        Path(__file__).with_name("fnp8_error_oracle.json").read_text()
    )
    for cell in evidence.results:
        with lp.spark_session_conf(spark_engine, (("spark.sql.ansi.enabled", cell.ansi),)):
            if cell.error is not None:
                with pytest.raises((SparkError, ValueError)) as caught:
                    spark_engine.arrow_of(spark_engine.session.sql(cell.spark_sql))
                assert type(caught.value).__name__ == cell.error
                assert str(caught.value).splitlines()[0] == cell.message
            else:
                table = spark_engine.arrow_of(spark_engine.session.sql(cell.spark_sql))
                assert json.loads(json.dumps(table.to_pylist())) == cell.rows
                assert str(table.schema) == cell.schema_text


@pytest.mark.parametrize("name", ("transform", "aggregate"))
def test_fnp8_preserves_unaliased_projection_names(name: str) -> None:
    """Preserve public column names recorded before the preparation rule was installed."""
    array = spark_functions.array(
        spark_functions.lit(1), spark_functions.lit(2), spark_functions.lit(3)
    )
    if name == "transform":
        query = "transform(array(1,2,3), (x,i) -> x+i)"
        column = spark_functions.transform(array, lambda element, index: element + index)
        sql_name = "transform(array(Int64(1),Int64(2),Int64(3)),(x, i) -> x + i)"
        column_name = "transform(array(1, 2, 3), x, y -> (x + y))"
    else:
        query = "aggregate(array(1,2,3), 0, (acc,x) -> acc+x)"
        column = spark_functions.aggregate(
            array, spark_functions.lit(0), lambda accumulator, element: accumulator + element
        )
        sql_name = "aggregate(array(Int64(1),Int64(2),Int64(3)),Int64(0),(acc, x) -> acc + x)"
        column_name = "aggregate(array(1, 2, 3), 0, x, y -> (x + y))"
    session = ReparkSession.builder.getOrCreate()
    try:
        assert session.sql(f"SELECT {query}").toArrow().column_names == [sql_name]
        assert session.range(1).select(column).toArrow().column_names == [column_name]
        assert session.range(1).select(spark_functions.expr(query)).toArrow().column_names == [
            query
        ]
    finally:
        session.stop()


ERROR_ORACLE = ErrorOracleEvidence.model_validate_json(
    Path(__file__).with_name("fnp8_error_oracle.json").read_text()
).results
ERROR_CELLS = [
    (index, cell, door) for index, cell in enumerate(ERROR_ORACLE) for door in ("sql", "expr")
]


def _error_repark_table(session: ReparkSession, cell: ErrorOracleCell, door: str) -> pa.Table:
    """Execute the measured SQL expression through the selected public text entry point."""
    if door == "sql":
        return session.sql(cell.spark_sql).toArrow()
    expression = cell.spark_sql.removeprefix("SELECT ").removesuffix(" AS r")
    return session.range(1).select(spark_functions.expr(expression).alias("r")).toArrow()


@pytest.mark.parametrize(
    "index,cell,door", ERROR_CELLS, ids=[f"error-{index}-{door}" for index, _, door in ERROR_CELLS]
)
def test_fnp8_sql_text_error_dispositions(index: int, cell: ErrorOracleCell, door: str) -> None:
    """Pin exact SQL and F.expr errors or results against each live-measured expression."""
    dispositions = TypeAdapter(dict[str, ReparkDisposition]).validate_json(
        Path(__file__).with_name("fnp8_repark_errors.json").read_text()
    )
    disposition = dispositions[f"{index}-{door}"]
    session = ReparkSession.builder.config("spark.sql.ansi.enabled", cell.ansi).getOrCreate()
    try:
        if disposition.error is not None:
            with pytest.raises(PySparkException) as caught:
                _error_repark_table(session, cell, door)
            assert type(caught.value).__name__ == disposition.error_type
            assert str(caught.value) == disposition.error
        else:
            table = _error_repark_table(session, cell, door)
            assert json.loads(json.dumps(table.to_pylist())) == disposition.rows
            assert disposition.schema_base64 is not None
            assert table.schema.equals(_read_schema(disposition.schema_base64))
            if cell.rows is not None:
                assert disposition.rows == cell.rows
    finally:
        session.stop()


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
@pytest.mark.parametrize("ansi", ("true", "false"))
def test_live_fnp8_empty_map_collect_diagnostic(spark_engine: lp.Engine, ansi: str) -> None:
    """Keep Spark's collect-only empty map result separate from its Arrow export failure."""
    with lp.spark_session_conf(spark_engine, (("spark.sql.ansi.enabled", ansi),)):
        frame = spark_engine.session.sql("SELECT transform_keys(map(), (k, v) -> k) AS r")
        assert [row.asDict() for row in frame.collect()] == [{"r": {}}]
        with pytest.raises(ValueError, match="A null type field may not be non-nullable"):
            spark_engine.arrow_of(frame)
