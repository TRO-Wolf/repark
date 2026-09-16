"""Oracle Q12 (PySpark 4.1.2) pins for the four door-converged P1 names.

Oracle fixtures (the spec — read first):
`/tmp/oc-worker/qc-oracle/fixtures-batch12.json` (cells Q12-0…Q12-55, temp view `t`
with `a = [1, 2]`, `an = [3, NULL]`, a NULL `array<int>` column, `s = 'a,b,,c'`,
`n = 5`, `sa = ['x', 'y']`) and `/tmp/oc-worker/pc-oracle/fixtures-batch7.json`
(N7-16 concat, N7-17 sequence, N7-18 split, N7-22 reverse).

pins: door-converge-2/C-001, C-002, C-003, C-004, C-005

Every value cell asserts the Spark VALUE, the Arrow TYPE and the outer NULLABILITY on
the SQL door, plus the element `containsNull`. The facade legs (C-005) repeat the
representative shapes through `F.concat` / `F.reverse` / `F.sequence` over a
`createDataFrame` with the same columns.

Respellings (same shape, parse-level only): `1L`/`2L` become `CAST(1 AS BIGINT)` /
`CAST(2 AS BIGINT)` (FNP-4B is not on main); Q12-4 spells the decimal bound as
`CAST(1.5 AS DECIMAL(2, 1))` because a bare `1.5` parses as DOUBLE on this door while
Spark reads a `decimal(2,1)` literal — the pinned claim is the widening rule to
`decimal(11,1)`; Q12-18 spells the null element as `CAST(NULL AS INT)` because the
bare `array(3, NULL)` constructor widens to BIGINT on this door (round-2
literal-widths scope — the as-written form reverses correctly, only wider).

Recorded-divergence legs (all `ARRAY-LITERAL-CONTAINSNULL-1`, owned by
DOOR-CONVERGE-2 round 2): `array(...)` literals declare a nullable element on this
door, so literal-input cells whose fixture says `containsNull=false` pin
`nullable=True` here — Q12-0, Q12-3, Q12-4, Q12-8 (outer and inner), Q12-9, Q12-16,
Q12-17, Q12-20 (element `Null` ≈ Spark `void`), Q12-23. Column-input cells,
`sequence` cells and `split` cells assert the fixture exactly: the view columns are
built nullable-element, and both kernels construct `containsNull=false` always.

Error mappings: `concat`/`sequence` refusals surface `AnalysisException` carrying the
Spark `DATATYPE_MISMATCH` class; `sequence` step errors surface the base
`PySparkException` carrying Spark's `requirement failed: Illegal sequence
boundaries: …` text. The oracle class (`IllegalArgumentException`, G-2 Q3) stays
OPEN: execution errors cross Arrow IPC and remap in `dataframe/export_errors.py`
(run 16b's fence), which no Rust route can reach — P2 hand-off to run 16b.

`F.split` still raises `UnsupportedOperationException` in `functions_expr.py`
(outside this lane's fence): the P2 hand-off test below pins that refusal loudly
for run 16a, and the split facade sub-cell stays OPEN.
"""

from __future__ import annotations

import datetime as datetime_module
from decimal import Decimal

import pyarrow as pa
import pytest

from repark.errors import AnalysisException, PySparkException, UnsupportedOperationException
from repark.spark import ReparkSession
from repark.spark import functions as F  # noqa: N812
from repark.spark.dataframe import DataFrame


@pytest.fixture
def spark() -> ReparkSession:
    """Per-test local session (no AWS)."""
    session = ReparkSession.builder.appName("pytest-door-converge-2").getOrCreate()
    yield session
    session.stop()


def _frame(spark: ReparkSession) -> DataFrame:
    """Build the Q12 view frame and register temp view `t`."""
    frame = spark.createDataFrame(
        [([1, 2], [3, None], None, "a,b,,c", 5, ["x", "y"])],
        "a array<int>, an array<int>, allnull array<int>, s string, n int, sa array<string>",
    )
    frame.createOrReplaceTempView("t")
    return frame


def _sql_cell(spark: ReparkSession, sql: str) -> tuple[pa.DataType, bool, list]:
    """Return (arrow type, nullable, values) of the first column of a spark.sql answer."""
    table = spark.sql(sql).to_arrow()
    field = table.schema[0]
    return field.type, field.nullable, table.column(0).to_pylist()


def _int_list(contains_null: bool) -> pa.DataType:
    """Array<int> result type with the engine's `element` field name."""
    return pa.list_(pa.field("element", pa.int32(), nullable=contains_null))


def _check_cell(
    spark: ReparkSession, sql: str, value: list, arrow_type: pa.DataType, nullable: bool
) -> None:
    """Assert value, Arrow type and nullability of one SQL-door oracle cell."""
    actual_type, actual_nullable, actual_value = _sql_cell(spark, sql)
    assert actual_value == value
    assert actual_type == arrow_type
    assert actual_nullable == nullable


_SQL_VALUES: dict[str, tuple[str, list, pa.DataType, bool]] = {
    "Q12-0": (
        "SELECT concat(array(1), array(2))",
        [[1, 2]],
        _int_list(True),
        False,
    ),
    "Q12-1": (
        "SELECT concat(a, an) FROM t",
        [[1, 2, 3, None]],
        pa.list_(pa.field("element", pa.int32(), nullable=True)),
        True,
    ),
    "Q12-2": (
        "SELECT concat(a, allnull) FROM t",
        [None],
        pa.list_(pa.field("element", pa.int32(), nullable=True)),
        True,
    ),
    "Q12-3": (
        "SELECT concat(array(1), array(CAST(2 AS BIGINT)))",
        [[1, 2]],
        pa.list_(pa.field("element", pa.int64(), nullable=True)),
        False,
    ),
    "Q12-4": (
        "SELECT concat(array(1), array(CAST(1.5 AS DECIMAL(2, 1))))",
        [[Decimal("1.0"), Decimal("1.5")]],
        pa.list_(pa.field("element", pa.decimal128(11, 1), nullable=True)),
        False,
    ),
    "Q12-5": (
        "SELECT concat(array('a'), sa) FROM t",
        [["a", "x", "y"]],
        pa.list_(pa.field("element", pa.string(), nullable=True)),
        True,
    ),
    "Q12-6": (
        "SELECT concat(a) FROM t",
        [[1, 2]],
        pa.list_(pa.field("element", pa.int32(), nullable=True)),
        True,
    ),
    "Q12-7": (
        "SELECT concat(array(1), array(NULL))",
        [[1, None]],
        pa.list_(pa.field("element", pa.int32(), nullable=True)),
        False,
    ),
    "Q12-8": (
        "SELECT concat(array(array(1, 2)), array(array(3)))",
        [[[1, 2], [3]]],
        pa.list_(
            pa.field(
                "element",
                pa.list_(pa.field("element", pa.int32(), nullable=True)),
                nullable=True,
            )
        ),
        False,
    ),
    "Q12-9": (
        "SELECT concat(array(1), array())",
        [[1]],
        _int_list(True),
        False,
    ),
    "Q12-11": ("SELECT concat('a', 'b')", ["ab"], pa.string(), False),
    "Q12-12": ("SELECT concat('a', NULL)", [None], pa.string(), True),
    "Q12-13": (
        "SELECT concat(CAST(X'41' AS BINARY), CAST(X'42' AS BINARY))",
        [b"AB"],
        pa.binary(),
        False,
    ),
    "Q12-14": ("SELECT concat(1, 2)", ["12"], pa.string(), False),
    "Q12-15": ("SELECT concat()", [""], pa.string(), False),
    "Q12-16": (
        "SELECT array(1) || array(2)",
        [[1, 2]],
        pa.list_(pa.field("element", pa.int32(), nullable=True)),
        False,
    ),
    "Q12-17": (
        "SELECT reverse(array(1, 2, 3))",
        [[3, 2, 1]],
        _int_list(True),
        False,
    ),
    "Q12-18": (
        "SELECT reverse(an) FROM t",
        [[None, 3]],
        pa.list_(pa.field("item", pa.int32(), nullable=True)),
        True,
    ),
    "Q12-19": (
        "SELECT reverse(allnull) FROM t",
        [None],
        pa.list_(pa.field("item", pa.int32(), nullable=True)),
        True,
    ),
    "Q12-20": (
        "SELECT reverse(array())",
        [[]],
        pa.list_(pa.field("element", pa.null(), nullable=True)),
        False,
    ),
    "Q12-21": ("SELECT reverse('abc')", ["cba"], pa.string(), False),
    "Q12-22": (
        "SELECT reverse(sa) FROM t",
        [["y", "x"]],
        pa.list_(pa.field("item", pa.string(), nullable=True)),
        True,
    ),
    "Q12-23": (
        "SELECT reverse(array(array(1, 2), array(3)))",
        [[[3], [1, 2]]],
        pa.list_(
            pa.field(
                "element",
                pa.list_(pa.field("element", pa.int32(), nullable=True)),
                nullable=True,
            )
        ),
        False,
    ),
    "Q12-24": ("SELECT reverse(NULL)", [None], pa.string(), True),
    "Q12-25": (
        "SELECT reverse(CAST(NULL AS STRING))",
        [None],
        pa.string(),
        True,
    ),
    "Q12-26": (
        "SELECT sequence(1, 3)",
        [[1, 2, 3]],
        pa.list_(pa.field("element", pa.int32(), nullable=False)),
        False,
    ),
    "Q12-27": (
        "SELECT sequence(3, 1)",
        [[3, 2, 1]],
        pa.list_(pa.field("element", pa.int32(), nullable=False)),
        False,
    ),
    "Q12-28": (
        "SELECT sequence(1, 10, 3)",
        [[1, 4, 7, 10]],
        pa.list_(pa.field("element", pa.int32(), nullable=False)),
        False,
    ),
    "Q12-29": (
        "SELECT sequence(CAST(1 AS BIGINT), CAST(3 AS BIGINT))",
        [[1, 2, 3]],
        pa.list_(pa.field("element", pa.int64(), nullable=False)),
        False,
    ),
    "Q12-30": (
        "SELECT sequence(1, n) FROM t",
        [[1, 2, 3, 4, 5]],
        pa.list_(pa.field("element", pa.int32(), nullable=False)),
        True,
    ),
    "Q12-33": (
        "SELECT sequence(CAST(1 AS TINYINT), CAST(3 AS TINYINT))",
        [[1, 2, 3]],
        pa.list_(pa.field("element", pa.int8(), nullable=False)),
        False,
    ),
    "Q12-34": (
        "SELECT sequence(DATE'2024-01-01', DATE'2024-01-03')",
        [
            [
                datetime_module.date(2024, 1, 1),
                datetime_module.date(2024, 1, 2),
                datetime_module.date(2024, 1, 3),
            ]
        ],
        pa.list_(pa.field("element", pa.date32(), nullable=False)),
        False,
    ),
    "Q12-35": (
        "SELECT sequence(DATE'2024-01-01', DATE'2024-03-01', INTERVAL 1 MONTH)",
        [
            [
                datetime_module.date(2024, 1, 1),
                datetime_module.date(2024, 2, 1),
                datetime_module.date(2024, 3, 1),
            ]
        ],
        pa.list_(pa.field("element", pa.date32(), nullable=False)),
        False,
    ),
    "Q12-36": (
        "SELECT sequence(TIMESTAMP'2024-01-01 00:00:00', TIMESTAMP'2024-01-01 02:00:00', "
        "INTERVAL 1 HOUR)",
        [
            [
                datetime_module.datetime(2024, 1, 1, 0, 0, tzinfo=datetime_module.UTC),
                datetime_module.datetime(2024, 1, 1, 1, 0, tzinfo=datetime_module.UTC),
                datetime_module.datetime(2024, 1, 1, 2, 0, tzinfo=datetime_module.UTC),
            ]
        ],
        pa.list_(
            pa.field("element", pa.timestamp("us", tz="UTC"), nullable=False),
        ),
        False,
    ),
    "Q12-37": (
        "SELECT sequence(1, NULL)",
        [None],
        pa.list_(pa.field("element", pa.int32(), nullable=False)),
        True,
    ),
    "Q12-38": (
        "SELECT sequence(1, CAST(NULL AS INT))",
        [None],
        pa.list_(pa.field("element", pa.int32(), nullable=False)),
        True,
    ),
    "Q12-39": (
        "SELECT sequence(1, 3, NULL)",
        [None],
        pa.list_(pa.field("element", pa.int32(), nullable=False)),
        True,
    ),
    "Q12-41": (
        "SELECT split(s, ',') FROM t",
        [["a", "b", "", "c"]],
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        True,
    ),
    "Q12-42": (
        "SELECT split(s, ',', 5) FROM t",
        [["a", "b", "", "c"]],
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        True,
    ),
    "Q12-43": (
        "SELECT split(s, ',', 2) FROM t",
        [["a", "b,,c"]],
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        True,
    ),
    "Q12-44": (
        "SELECT split(s, ',', -1) FROM t",
        [["a", "b", "", "c"]],
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        True,
    ),
    "Q12-45": (
        "SELECT split(s, ',', 0) FROM t",
        [["a", "b", "", "c"]],
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        True,
    ),
    "Q12-46": (
        "SELECT split('a1b22c', '[0-9]+')",
        [["a", "b", "c"]],
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        False,
    ),
    "Q12-47": (
        "SELECT split('abc', '')",
        [["a", "b", "c"]],
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        False,
    ),
    "Q12-48": (
        "SELECT split('', ',')",
        [[""]],
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        False,
    ),
    "Q12-49": (
        "SELECT split(NULL, ',')",
        [None],
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        True,
    ),
    "Q12-50": (
        "SELECT split('a,b', NULL)",
        [None],
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        True,
    ),
    "Q12-51": (
        "SELECT split('a.b', '.')",
        [["", "", "", ""]],
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        False,
    ),
    "Q12-52": (
        "SELECT split('a|b', '\\\\|')",
        [["a", "b"]],
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        False,
    ),
    "Q12-53": (
        "SELECT split('a,b', ',', NULL)",
        [None],
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        True,
    ),
    "Q12-54": (
        "SELECT split('aXbxc', '(?i)x')",
        [["a", "b", "c"]],
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        False,
    ),
    "Q12-55": (
        "SELECT split(123, '2')",
        [["1", "3"]],
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        False,
    ),
}

_SQL_ERRORS: dict[str, tuple[str, type, str]] = {
    "Q12-10": (
        "SELECT concat(array(1), 'x')",
        AnalysisException,
        "DATATYPE_MISMATCH.DATA_DIFF_TYPES",
    ),
    "Q12-31": (
        "SELECT sequence(1, 3, 0)",
        PySparkException,
        "requirement failed: Illegal sequence boundaries: 1 to 3 by 0",
    ),
    "Q12-32": (
        "SELECT sequence(1, 3, -1)",
        PySparkException,
        "requirement failed: Illegal sequence boundaries: 1 to 3 by -1",
    ),
    "Q12-40": (
        "SELECT sequence(1.5, 3)",
        AnalysisException,
        "DATATYPE_MISMATCH.SEQUENCE_WRONG_INPUT_TYPES",
    ),
}


@pytest.mark.parametrize("cell_id", sorted(_SQL_VALUES), ids=sorted(_SQL_VALUES))
def test_sql_cell(spark: ReparkSession, cell_id: str) -> None:
    """pins: door-converge-2/C-001, C-002, C-003, C-004 — one SQL-door oracle cell."""
    _frame(spark)
    sql, value, arrow_type, nullable = _SQL_VALUES[cell_id]
    _check_cell(spark, sql, value, arrow_type, nullable)


@pytest.mark.parametrize("cell_id", sorted(_SQL_ERRORS), ids=sorted(_SQL_ERRORS))
def test_sql_error_cell(spark: ReparkSession, cell_id: str) -> None:
    """pins: door-converge-2/C-001, C-003 — one SQL-door oracle refusal cell."""
    _frame(spark)
    sql, error_type, match = _SQL_ERRORS[cell_id]
    with pytest.raises(error_type, match=match):
        spark.sql(sql).to_arrow()


def test_facade_concat_arrays(spark: ReparkSession) -> None:
    """pins: door-converge-2/C-005 — F.concat over array columns."""
    frame = _frame(spark)
    table = frame.select(F.concat(F.col("a"), F.col("an")).alias("v")).to_arrow()
    assert table.column(0).to_pylist() == [[1, 2, 3, None]]
    assert table.schema[0].type == pa.list_(pa.field("element", pa.int32(), nullable=True))
    assert table.schema[0].nullable is True


def test_facade_reverse_arrays(spark: ReparkSession) -> None:
    """pins: door-converge-2/C-005 — F.reverse over an array column."""
    frame = _frame(spark)
    table = frame.select(F.reverse("an").alias("v")).to_arrow()
    assert table.column(0).to_pylist() == [[None, 3]]
    assert table.schema[0].type == pa.list_(pa.field("item", pa.int32(), nullable=True))
    assert table.schema[0].nullable is True


def test_facade_reverse_string(spark: ReparkSession) -> None:
    """pins: door-converge-2/C-005 — F.reverse over a string column."""
    frame = _frame(spark)
    table = frame.select(F.reverse("s").alias("v")).to_arrow()
    assert table.column(0).to_pylist() == ["c,,b,a"]
    assert table.schema[0].type == pa.string()


def test_facade_sequence_column_stop(spark: ReparkSession) -> None:
    """pins: door-converge-2/C-005 — F.sequence with a column stop."""
    frame = _frame(spark)
    table = frame.select(F.sequence(F.lit(1), "n").alias("v")).to_arrow()
    assert table.column(0).to_pylist() == [[1, 2, 3, 4, 5]]
    assert table.schema[0].type == pa.list_(pa.field("element", pa.int32(), nullable=False))
    assert table.schema[0].nullable is True


def test_facade_sequence_literals(spark: ReparkSession) -> None:
    """pins: door-converge-2/C-005 — F.sequence over literals."""
    frame = _frame(spark)
    table = frame.select(F.sequence(1, 3).alias("v")).to_arrow()
    assert table.column(0).to_pylist() == [[1, 2, 3]]
    assert table.schema[0].type == pa.list_(pa.field("element", pa.int32(), nullable=False))


def test_facade_sequence_illegal_step_text(spark: ReparkSession) -> None:
    """pins: door-converge-2/C-005 — F.sequence illegal step keeps Spark's text.

    The exact `requirement failed: …` text survives the Arrow export on the facade
    door. The exception class stays OPEN (base `PySparkException` today, oracle
    `IllegalArgumentException`) until run 16b maps the marker in
    `dataframe/export_errors.py`.
    """
    frame = _frame(spark)
    with pytest.raises(
        PySparkException,
        match="requirement failed: Illegal sequence boundaries: 1 to 3 by 0",
    ):
        frame.select(F.sequence(1, 3, 0).alias("v")).to_arrow()


def test_facade_split_refusal_handoff_16a(spark: ReparkSession) -> None:
    """pins: door-converge-2/C-005 — F.split still refuses; P2 hand-off to run 16a.

    The Python `F.split` raises before reaching the converged Rust kernel, so the
    split facade sub-cell stays OPEN until run 16a wires it. This pin guards the
    refusal (loud, typed) so a silent change fails visibly.
    """
    _frame(spark)
    with pytest.raises(UnsupportedOperationException):
        F.split("s", ",")


def _date_list() -> pa.DataType:
    """Array<date> result type with non-nullable elements."""
    return pa.list_(pa.field("element", pa.date32(), nullable=False))


def _q15_frame(spark: ReparkSession) -> DataFrame:
    """Build the Q15 view frame and register temp view `q15`."""
    frame = spark.createDataFrame(
        [(10000001, "é🎉x")],
        "n int, e string",
    )
    frame.createOrReplaceTempView("q15")
    return frame


_Q15_VALUES: dict[str, tuple[str, list, pa.DataType, bool]] = {
    "Q15-0": (
        "SELECT sequence(DATE'2024-01-31', DATE'2024-03-31', INTERVAL 1 MONTH)",
        [
            [
                datetime_module.date(2024, 1, 31),
                datetime_module.date(2024, 2, 29),
                datetime_module.date(2024, 3, 31),
            ]
        ],
        _date_list(),
        False,
    ),
    "Q15-1": (
        "SELECT sequence(DATE'2018-01-31', DATE'2018-03-31', INTERVAL 1 MONTH)",
        [
            [
                datetime_module.date(2018, 1, 31),
                datetime_module.date(2018, 2, 28),
                datetime_module.date(2018, 3, 31),
            ]
        ],
        _date_list(),
        False,
    ),
    "Q15-2": (
        "SELECT sequence(DATE'2024-01-31', DATE'2024-05-31', INTERVAL 1 MONTH)",
        [
            [
                datetime_module.date(2024, 1, 31),
                datetime_module.date(2024, 2, 29),
                datetime_module.date(2024, 3, 31),
                datetime_module.date(2024, 4, 30),
                datetime_module.date(2024, 5, 31),
            ]
        ],
        _date_list(),
        False,
    ),
    "Q15-3": (
        "SELECT sequence(TIMESTAMP'2024-01-31 10:00:00', TIMESTAMP'2024-03-31 10:00:00', "
        "INTERVAL 1 MONTH)",
        [
            [
                datetime_module.datetime(2024, 1, 31, 10, 0, tzinfo=datetime_module.UTC),
                datetime_module.datetime(2024, 2, 29, 10, 0, tzinfo=datetime_module.UTC),
                datetime_module.datetime(2024, 3, 31, 10, 0, tzinfo=datetime_module.UTC),
            ]
        ],
        pa.list_(
            pa.field("element", pa.timestamp("us", tz="UTC"), nullable=False),
        ),
        False,
    ),
    "Q15-4": (
        "SELECT sequence(DATE'2024-03-31', DATE'2024-01-31', INTERVAL -1 MONTH)",
        [
            [
                datetime_module.date(2024, 3, 31),
                datetime_module.date(2024, 2, 29),
                datetime_module.date(2024, 1, 31),
            ]
        ],
        _date_list(),
        False,
    ),
    "Q15-5": (
        "SELECT concat('A', CAST(X'42' AS BINARY))",
        ["AB"],
        pa.string(),
        False,
    ),
    "Q15-6": (
        "SELECT concat('A', CAST(X'42' AS BINARY), 'C')",
        ["ABC"],
        pa.string(),
        False,
    ),
    "Q15-7": (
        "SELECT concat(CAST(X'42' AS BINARY), 'A')",
        ["BA"],
        pa.string(),
        False,
    ),
    "Q15-8": (
        "SELECT concat(CAST(X'41' AS BINARY), 1)",
        ["A1"],
        pa.string(),
        False,
    ),
    "Q15-9": (
        "SELECT split('a.b.c', '\\\\Q.\\\\E')",
        [["a", "b", "c"]],
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        False,
    ),
    "Q15-14": (
        "SELECT split('a1b2c', '\\\\d')",
        [["a", "b", "c"]],
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        False,
    ),
    "Q15-15": (
        "SELECT split('aXbXc', 'X', 2)",
        [["a", "bXc"]],
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        False,
    ),
    "Q15-17": (
        "SELECT split('a🎉b', '')",
        [["a", "🎉", "b"]],
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        False,
    ),
    "Q15-16": (
        "SELECT split(e, '') FROM q15",
        [["é", "🎉", "x"]],
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        True,
    ),
    "Q15-10": (
        "SELECT split('a,b,c', '(?=,)')",
        [["a", ",b", ",c"]],
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        False,
    ),
    "Q15-11": (
        "SELECT split('aa-bb', '(a|b)\\\\1')",
        [["", "-", ""]],
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        False,
    ),
    "Q15-12": (
        "SELECT split('aaa', 'a++a')",
        [["aaa"]],
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        False,
    ),
}

_Q15_ERRORS: dict[str, tuple[str, type, str]] = {
    "Q15-13": (
        "SELECT split('ab', '[')",
        PySparkException,
        "nclosed character class",
    ),
    "Q15-18": (
        "SELECT size(sequence(1, n)) FROM q15",
        PySparkException,
        "requested 10000001 elements",
    ),
    "Q15-19": (
        "SELECT size(sequence(1, 10000001))",
        AnalysisException,
        "requested 10000001 elements",
    ),
}


@pytest.mark.parametrize("cell_id", sorted(_Q15_VALUES), ids=sorted(_Q15_VALUES))
def test_q15_cell(spark: ReparkSession, cell_id: str) -> None:
    """One round-3 oracle cell.

    pins: door-converge-2/C-007, C-008, C-009
    pins: java-regex-features-1/C-008
    """
    _frame(spark)
    _q15_frame(spark)
    sql, value, arrow_type, nullable = _Q15_VALUES[cell_id]
    _check_cell(spark, sql, value, arrow_type, nullable)


@pytest.mark.parametrize("cell_id", sorted(_Q15_ERRORS), ids=sorted(_Q15_ERRORS))
def test_q15_error_cell(spark: ReparkSession, cell_id: str) -> None:
    """pins: door-converge-2/C-007, C-008, C-009 — one round-3 oracle refusal cell."""
    _frame(spark)
    _q15_frame(spark)
    sql, error_type, match = _Q15_ERRORS[cell_id]
    with pytest.raises(error_type, match=match):
        spark.sql(sql).to_arrow()


def test_facade_sequence_month_step(spark: ReparkSession) -> None:
    """pins: door-converge-2/C-007 — F.sequence counts month steps from the start."""
    frame = spark.createDataFrame(
        [(datetime_module.date(2024, 1, 31), datetime_module.date(2024, 3, 31))],
        "d1 date, d2 date",
    )
    table = frame.select(F.sequence("d1", "d2", F.expr("INTERVAL 1 MONTH")).alias("v")).to_arrow()
    assert table.column(0).to_pylist() == [
        [
            datetime_module.date(2024, 1, 31),
            datetime_module.date(2024, 2, 29),
            datetime_module.date(2024, 3, 31),
        ]
    ]


def test_facade_concat_string_binary(spark: ReparkSession) -> None:
    """pins: door-converge-2/C-008 — F.concat over STRING+BINARY answers STRING."""
    frame = spark.createDataFrame([("A", b"B")], "s string, b binary")
    table = frame.select(F.concat(F.col("s"), F.col("b")).alias("v")).to_arrow()
    assert table.column(0).to_pylist() == ["AB"]
    assert table.schema[0].type == pa.string()


def test_facade_sequence_column_cap(spark: ReparkSession) -> None:
    """pins: door-converge-2/C-009 — F.sequence over a huge column stop refuses."""
    frame = _q15_frame(spark)
    with pytest.raises(PySparkException, match="requested 10000001 elements"):
        frame.select(F.sequence(F.lit(1), "n").alias("v")).to_arrow()


def test_split_limit_early_stop_matches_full_walk(spark: ReparkSession) -> None:
    """pins: door-converge-2/C-009 — limit=2 truncates the walk, never the answer."""
    table = spark.sql("SELECT split(repeat('a,', 5000), ',', 2) AS v").to_arrow()
    [[first, rest]] = table.column(0).to_pylist()
    assert first == "a"
    assert rest == "a," * 4999


def test_reverse_nested_column_keeps_inner(spark: ReparkSession) -> None:
    """pins: door-converge-2/C-007 — reverse over nested arrays flips outer only."""
    frame = spark.createDataFrame([([[1, 2], [3]],)], "nested array<array<int>>")
    expected_type = pa.list_(
        pa.field(
            "item",
            pa.list_(pa.field("item", pa.int32(), nullable=True)),
            nullable=True,
        )
    )
    for table in (
        frame.selectExpr("reverse(nested) AS v").to_arrow(),
        frame.select(F.reverse("nested").alias("v")).to_arrow(),
    ):
        assert table.column(0).to_pylist() == [[[3], [1, 2]]]
        assert table.schema[0].type == expected_type
        assert table.schema[0].nullable is True


def test_split_per_row_pattern_column(spark: ReparkSession) -> None:
    """pins: door-converge-2/C-009 — a pattern column resolves per row."""
    frame = spark.createDataFrame(
        [("a,b", ","), ("a;b", ";"), ("a,b", ";")],
        "s string, p string",
    )
    table = frame.selectExpr("split(s, p) AS v").to_arrow()
    assert table.column(0).to_pylist() == [["a", "b"], ["a", "b"], ["a,b"]]
