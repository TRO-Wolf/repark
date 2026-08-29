"""Facade tests for the set-operation + dedup family (Group E: E3 union, E4 distinct).

`union`/`unionAll`/`unionByName` and `distinct`/`dropDuplicates`, pinned to **real PySpark 4.1.2**
(run locally under Java 17). Union semantics — by-position vs by-name resolution, type coercion,
allowMissingColumns, UNION-ALL (no dedup) — and dedup determinism were all recorded from the live
oracle. The int+double type-coercion **parity** pin builds its inputs with `createDataFrame` so both
engines infer types AND nullability the same way (Python `int`→bigint, `float`→double, both
nullable) — an inline SQL literal would NOT (see below). One **disclosed divergence** is pinned
separately: after U2 (`parse_float_as_decimal=true`) repark types
`union(VALUES (1), VALUES (2.5))` as ``decimal128(21, 1)`` nullable (Int64 promoted to
DECIMAL(20,0) union DECIMAL(2,1)), where Spark lands ``decimal128(11, 1)`` non-null
(INT promoted to DECIMAL(10,0) union DECIMAL(2,1)).
That leftover is campaign DEC-8 / U3 **set-op widening** (Spark `forType(INT)=(10,0)`, not
`fromLiteral` digits) — see `test_union_inline_decimal_literal_diverges_from_spark`.
`dropDuplicates(subset)` is row-nondeterministic in Spark, so its fixtures pin a deterministic
survivor (the surviving key set, or identical non-key values), never an accident (docs/testing.md
row-order discipline).
"""

from __future__ import annotations

from decimal import Decimal

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark_parity import FrameMismatchError, assert_frames_equal


@pytest.fixture
def spark() -> ReparkSession:
    """A default session (PySpark ``SparkSession.builder.getOrCreate()``)."""
    return ReparkSession.builder.appName("pytest-union-distinct").getOrCreate()


def _by(rows: list[dict[str, object]], key: str) -> list[dict[str, object]]:
    """Sort collected rows by a key for order-insensitive comparison."""
    return sorted(rows, key=lambda row: (row[key] is None, row[key]))


# E3 — union by position


def test_union_is_union_all_by_position(spark: ReparkSession) -> None:
    a = spark.sql("SELECT * FROM (VALUES (1, 'a'), (1, 'a')) AS t(id, name)")
    b = spark.sql("SELECT * FROM (VALUES (2, 'b')) AS t(id, name)")
    unioned = a.union(b)
    # UNION ALL — the duplicate (1,'a') rows are BOTH kept (Spark union does not dedupe).
    assert unioned.to_arrow().num_rows == 3
    assert _by(unioned.to_arrow().to_pylist(), "id") == [
        {"id": 1, "name": "a"},
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
    ]


def test_union_all_alias(spark: ReparkSession) -> None:
    from repark import DataFrame

    assert DataFrame.unionAll is DataFrame.union
    a = spark.sql("SELECT * FROM (VALUES (1)) AS t(v)")
    b = spark.sql("SELECT * FROM (VALUES (2)) AS t(v)")
    assert _by(a.unionAll(b).to_arrow().to_pylist(), "v") == [{"v": 1}, {"v": 2}]


def test_union_by_position_keeps_left_names(spark: ReparkSession) -> None:
    # Positional union ignores the right side's column names and keeps the LEFT's (Spark).
    a = spark.sql("SELECT * FROM (VALUES (1, 'a')) AS t(id, name)")
    other = spark.sql("SELECT * FROM (VALUES (2, 'b')) AS t(xid, xname)")
    unioned = a.union(other)
    assert unioned.columns == ["id", "name"], "left column names win"
    assert _by(unioned.to_arrow().to_pylist(), "id") == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
    ]


def test_parity_union_type_coercion(spark: ReparkSession) -> None:
    """union of integer + double -> double — a GENUINE Spark-parity pin, not repark-vs-repark.

    Inputs are built with ``createDataFrame`` so both engines infer types from the Python values the
    SAME way (``int`` -> bigint/int64, ``float`` -> double, both ``nullable=True``); the union then
    widens the integer side to double. Verified against live PySpark 4.1.2
    (``JAVA_HOME=/usr/lib/jvm/zulu-17-amd64``): Spark and repark BOTH yield
    ``[('v', 'double', nullable=True)]`` with values ``1.0`` / ``2.5``. An inline SQL literal
    2.5 does NOT type identically (Spark parses it as DECIMAL(2,1)), so it cannot back this
    parity claim; that divergence is pinned in
    ``test_union_inline_decimal_literal_diverges_from_spark``.
    """
    ints = spark.createDataFrame([(1,)], ["v"])
    doubles = spark.createDataFrame([(2.5,)], ["v"])
    result = ints.union(doubles)
    golden = pa.table(
        [pa.array([1.0, 2.5], pa.float64())],
        schema=pa.schema([pa.field("v", pa.float64(), nullable=True)]),
    )
    assert_frames_equal(result.to_arrow(), golden)


def test_union_inline_decimal_literal_diverges_from_spark(spark: ReparkSession) -> None:
    """DISCLOSED DIVERGENCE (TY-3): U3 does not move this row to Spark's (11,1).

    ``VALUES (2.5)`` is DECIMAL(2,1); ``VALUES (1)`` is Int64 -> ``DECIMAL(20,0) union
    DECIMAL(2,1)`` -> ``decimal128(21, 1)`` **nullable**. Spark 4.1.2 treats the int as INT ->
    ``DECIMAL(10,0) union DECIMAL(2,1)`` -> ``decimal128(11, 1)`` **non-null**.

    UNION set-op widening uses Spark ``forType(INT) = DECIMAL(10,0)``, not integer-literal
    ``fromLiteral`` digits (which apply to ``+ - *`` only); applying fromLiteral here would
    yield ``DECIMAL(1,0) union DECIMAL(2,1)`` -> ``(3,1)``, neither today's ``(21,1)`` nor
    Spark's ``(11,1)``. The honest hook is DataFusion ``TypeCoercion`` / ``coerce_union``
    (Int64 → DECIMAL(20,0)); a UNION-only ``forType(INT)`` rewrite cannot tell
    ``VALUES (1)`` from a BIGINT column. Still DECLARED.
    """
    ints = spark.sql("SELECT * FROM (VALUES (1)) AS t(v)")
    dec = spark.sql("SELECT * FROM (VALUES (2.5)) AS t(v)")
    result = ints.union(dec).to_arrow()

    # repark's ACTUAL output after U2 — still not Spark (21,1) nullable vs (11,1) non-null.
    repark_out = pa.table(
        [pa.array([Decimal("1.0"), Decimal("2.5")], pa.decimal128(21, 1))],
        schema=pa.schema([pa.field("v", pa.decimal128(21, 1), nullable=True)]),
    )
    assert_frames_equal(result, repark_out)

    # The real Spark golden (recorded from PySpark 4.1.2). Load-bearing: if U3 converges on
    # DECIMAL(11,1) non-null, this guard flips RED and the disclosure must be revisited.
    spark_golden = pa.table(
        [pa.array([Decimal("1.0"), Decimal("2.5")], pa.decimal128(11, 1))],
        schema=pa.schema([pa.field("v", pa.decimal128(11, 1), nullable=False)]),
    )
    with pytest.raises(FrameMismatchError):
        assert_frames_equal(result, spark_golden)


def test_union_column_count_mismatch_raises(spark: ReparkSession) -> None:
    # Spark raises on a positional union with a different number of columns.
    a = spark.sql("SELECT * FROM (VALUES (1, 'a')) AS t(id, name)")
    narrow = spark.sql("SELECT * FROM (VALUES (2)) AS t(id)")
    with pytest.raises(AnalysisException):
        a.union(narrow).to_arrow()


# E3 — unionByName


def test_union_by_name_resolves_by_name_not_position(spark: ReparkSession) -> None:
    a = spark.sql("SELECT * FROM (VALUES (1, 'a')) AS t(id, name)")
    reordered = spark.sql("SELECT * FROM (VALUES ('b', 2)) AS t(name, id)")
    result = a.unionByName(reordered)
    assert result.columns == ["id", "name"]
    assert _by(result.to_arrow().to_pylist(), "id") == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
    ]


def test_union_by_name_missing_columns_raises_by_default(spark: ReparkSession) -> None:
    # Default allowMissingColumns=False: a column present on only one side is an AnalysisException
    # (Spark's NUM_COLUMNS_MISMATCH class).
    a = spark.sql("SELECT * FROM (VALUES (1, 'a')) AS t(id, name)")
    wide = spark.sql("SELECT * FROM (VALUES (9, 'z', 99)) AS t(id, name, extra)")
    with pytest.raises(AnalysisException):
        a.unionByName(wide)


def test_parity_union_by_name_allow_missing_fills_null(spark: ReparkSession) -> None:
    # allowMissingColumns=True: the extra column is filled with NULL on the side that lacks it.
    # R4 (S2): inputs are built with ``createDataFrame`` (NOT inline SQL ``VALUES``) so both engines
    # infer int64 / nullable=True identically — a GENUINE parity pin. Re-recorded from
    # PySpark 4.1.2 (``JAVA_HOME=/usr/lib/jvm/zulu-17-amd64``): schema
    # ``[(id,bigint,True),(name,string,True),(extra,bigint,True)]``, rows
    # ``[{id:1,name:a,extra:None},{id:9,name:z,extra:99}]``.
    a = spark.createDataFrame([(1, "a")], ["id", "name"])
    wide = spark.createDataFrame([(9, "z", 99)], ["id", "name", "extra"])
    result = a.unionByName(wide, allowMissingColumns=True)
    golden = pa.table(
        [
            pa.array([1, 9], pa.int64()),
            pa.array(["a", "z"], pa.string()),
            pa.array([None, 99], pa.int64()),
        ],
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=True),
                pa.field("name", pa.string(), nullable=True),
                pa.field("extra", pa.int64(), nullable=True),
            ]
        ),
    )
    assert_frames_equal(result.to_arrow(), golden)


# E4 — distinct / dropDuplicates


def test_distinct_dedups_full_rows(spark: ReparkSession) -> None:
    source = spark.sql("SELECT * FROM (VALUES (1, 'a'), (1, 'a'), (1, 'b'), (2, 'a')) AS t(k, v)")
    result = source.distinct()
    assert result.to_arrow().num_rows == 3
    assert sorted(result.to_arrow().to_pylist(), key=lambda r: (r["k"], r["v"])) == [
        {"k": 1, "v": "a"},
        {"k": 1, "v": "b"},
        {"k": 2, "v": "a"},
    ]


def test_drop_duplicates_no_subset_equals_distinct(spark: ReparkSession) -> None:
    from repark import DataFrame

    assert DataFrame.dropDuplicates is DataFrame.drop_duplicates
    source = spark.sql("SELECT * FROM (VALUES (1, 'a'), (1, 'a'), (2, 'a')) AS t(k, v)")
    assert source.dropDuplicates().to_arrow().num_rows == 2


def test_drop_duplicates_subset_keeps_one_per_key(spark: ReparkSession) -> None:
    # dropDuplicates([k]) keeps one row per key. Which row survives is unspecified in Spark when the
    # non-key values differ, so the DETERMINISTIC pins are: (1) the row COUNT, and (2) the surviving
    # KEY SET — never an accidental non-key value.
    source = spark.sql("SELECT * FROM (VALUES (1, 'a'), (1, 'b'), (2, 'c'), (2, 'd')) AS t(k, v)")
    result = source.dropDuplicates(["k"])
    rows = result.to_arrow().to_pylist()
    assert len(rows) == 2, "one survivor per key"
    assert sorted(row["k"] for row in rows) == [1, 2], "the surviving key set is deterministic"


def test_parity_drop_duplicates_subset_deterministic_survivor(spark: ReparkSession) -> None:
    # When every row sharing a key has identical non-key values, the survivor's full row IS
    # deterministic — so this can be a strict frame-equal golden.
    # R4 (S2): inputs built with ``createDataFrame`` so both engines infer int64 / nullable=True
    # identically — a GENUINE parity pin. Re-recorded from PySpark 4.1.2: schema
    # ``[(k,bigint,True),(v,string,True)]``, rows ``[{k:1,v:a},{k:2,v:b}]``.
    source = spark.createDataFrame([(1, "a"), (1, "a"), (2, "b")], ["k", "v"])
    result = source.dropDuplicates(["k"])
    golden = pa.table(
        [pa.array([1, 2], pa.int64()), pa.array(["a", "b"], pa.string())],
        schema=pa.schema(
            [
                pa.field("k", pa.int64(), nullable=True),
                pa.field("v", pa.string(), nullable=True),
            ]
        ),
    )
    assert_frames_equal(result.to_arrow(), golden)


# R5 — int UNION string: a DISCLOSED divergence (repark coerces to string; ANSI Spark 4 raises)


def test_union_int_string_coerces_to_string_diverges_from_ansi_spark(spark: ReparkSession) -> None:
    """DISCLOSED DIVERGENCE (R5, S2 — not a parity pin): repark silently coerces an int/string union
    to **string** (lossless), where **ANSI Spark 4** coerces to **bigint** and raises
    ``CAST_INVALID_INPUT`` at RUNTIME on the non-numeric string.

    Recorded from live PySpark 4.1.2 (ANSI on by default, Java 17):
    ``union(int, string)`` has schema ``bigint``, then ``.collect()`` raises
    ``NumberFormatException`` / errorClass ``CAST_INVALID_INPUT`` casting ``'x'`` → ``BIGINT``.
    repark lowers the union through DataFusion, which picks ``Utf8`` (string) as the common type, so
    it yields ``['1', 'x']`` string with NO error. We pin repark's ACTUAL output and record that
    Spark RAISES — the gap is documented, not silently encoded as "parity" (docs/testing.md
    divergence-class discipline; task/lessons.md). This guard is load-bearing: if a
    future ANSI-cast change made repark raise here too (converging), ``.to_arrow()`` below would
    raise and the test flips RED, forcing the disclosure to be revisited.
    """
    ints = spark.createDataFrame([(1,)], ["v"])
    strs = spark.createDataFrame([("x",)], ["v"])
    result = ints.union(strs).to_arrow()  # does NOT raise in repark (Spark would, at collect)
    # repark's ACTUAL (divergent) behavior — pinned on value AND Arrow type.
    assert pa.types.is_string(result.schema.field("v").type), "repark coerces int/string -> string"
    assert result.num_rows == 2, "repark returns rows where ANSI Spark 4 raises the cast error"
    # the int 1 was coerced losslessly to '1' — a value Spark never yields (it errors first).
    assert sorted(result.column("v").to_pylist()) == ["1", "x"]


# R6 — dropDuplicates(subset) arg forms: list/tuple accepted, a bare str rejected (no char-iter)


def test_drop_duplicates_subset_accepts_list_and_tuple(spark: ReparkSession) -> None:
    # R6: a list and a tuple subset behave identically (PySpark). Both keep one row per key.
    source = spark.createDataFrame([(1, "a"), (1, "b"), (2, "c")], ["k", "v"])
    by_list = sorted(row["k"] for row in source.dropDuplicates(["k"]).to_arrow().to_pylist())
    by_tuple = sorted(row["k"] for row in source.dropDuplicates(("k",)).to_arrow().to_pylist())
    assert by_list == by_tuple == [1, 2]


def test_drop_duplicates_subset_rejects_bare_str(spark: ReparkSession) -> None:
    """R6: ``dropDuplicates("key")`` raises a PySpark-shaped ``TypeError`` — a bare str is NOT a
    subset (PySpark ``NOT_LIST_OR_TUPLE``), so a multi-char column name is never char-iterated into
    ``['k','e','y']``. Oracle (PySpark 4.1.2): ``PySparkTypeError`` "should be a list or tuple".
    """
    source = spark.createDataFrame([(1, "a"), (1, "b")], ["key", "v"])
    with pytest.raises(TypeError, match="NOT_LIST_OR_TUPLE"):
        source.dropDuplicates("key")


def test_drop_duplicates_subset_wrong_type_raises(spark: ReparkSession) -> None:
    # R6: a non-list/tuple/str subset raises the PySpark-shaped TypeError.
    source = spark.createDataFrame([(1, "a")], ["k", "v"])
    with pytest.raises(TypeError, match="NOT_LIST_OR_TUPLE"):
        source.dropDuplicates(5)
