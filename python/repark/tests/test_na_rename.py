"""Facade tests for renames + the missing-data family (Group E: E5, plus E7/E8 na edges).

`withColumnRenamed` and `df.na` / `fillna` / `dropna`, pinned to **real PySpark 4.1.2** (run locally
under Java 17). Fill/drop value forms (scalar, dict), subset, `how`, and `thresh` were recorded from
the live oracle.

One deliberately-disclosed divergence: a **scalar numeric** ``fillna`` makes the filled column
non-nullable in repark (it lowers to ``coalesce(col, lit)``), whereas Spark 4.1.2 inconsistently
leaves an integral column nullable while making a filled double non-nullable — an accident we do not
pin (docs/testing.md "do not pin an accident"). So the scalar-numeric case is pinned on VALUE and
Arrow TYPE only; the dict-fill and string-fill cases (where Spark's nullability is consistent and
matches repark) are pinned as full frame-equal goldens.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark_parity import assert_frames_equal


@pytest.fixture
def spark() -> ReparkSession:
    """A default session (PySpark ``SparkSession.builder.getOrCreate()``)."""
    return ReparkSession.builder.appName("pytest-na-rename").getOrCreate()


def _na_frame(spark: ReparkSession) -> object:
    """The shared na fixture: (i, s, d) with an all-null middle row and a trailing null d.

    Built via ``createDataFrame`` so the string column is Arrow ``string`` (not ``string_view``) and
    every column is nullable — matching Spark's ``createDataFrame`` schema on all three axes.
    """
    return spark.createDataFrame(
        [(1, "a", 1.0), (None, None, None), (3, "c", None)], ["i", "s", "d"]
    )


# ==================================================================================================
# E5 — withColumnRenamed
# ==================================================================================================


def test_with_column_renamed_present_and_camelcase_alias(spark: ReparkSession) -> None:
    from repark import DataFrame

    assert DataFrame.withColumnRenamed is DataFrame.with_column_renamed
    df = spark.sql("SELECT * FROM (VALUES (1, 'a')) AS t(id, name)")
    renamed = df.withColumnRenamed("name", "label")
    assert renamed.columns == ["id", "label"]
    assert renamed.to_arrow().to_pylist() == [{"id": 1, "label": "a"}]


def test_with_column_renamed_missing_is_silent_noop(spark: ReparkSession) -> None:
    # Renaming a column that does not exist is a silent no-op (Spark semantics).
    df = spark.sql("SELECT * FROM (VALUES (1, 'a')) AS t(id, name)")
    result = df.withColumnRenamed("nonexistent", "label")
    assert result.columns == ["id", "name"], "absent old name → unchanged"


# ==================================================================================================
# E5/E7 — fillna
# ==================================================================================================


def test_parity_fillna_dict_fills_named_columns(spark: ReparkSession) -> None:
    # Dict fill: each named column filled with its value. Filling every column makes all three
    # non-nullable — and Spark agrees here, so this is a full frame-equal golden.
    result = _na_frame(spark).fillna({"i": -1, "s": "X", "d": -9.0})
    golden = pa.table(
        [
            pa.array([1, -1, 3], pa.int64()),
            pa.array(["a", "X", "c"], pa.string()),
            pa.array([1.0, -9.0, -9.0], pa.float64()),
        ],
        schema=pa.schema(
            [
                pa.field("i", pa.int64(), nullable=False),
                pa.field("s", pa.string(), nullable=False),
                pa.field("d", pa.float64(), nullable=False),
            ]
        ),
    )
    assert_frames_equal(result.to_arrow(), golden)


def test_fillna_dict_with_nonmatching_column_raises(spark: ReparkSession) -> None:
    # THE mandatory edge: a dict key that is not a column raises AnalysisException (Spark's
    # UNRESOLVED_COLUMN), it is NOT silently ignored.
    with pytest.raises(AnalysisException):
        _na_frame(spark).fillna({"nonexistent": 5})


def test_fillna_scalar_numeric_value_and_type(spark: ReparkSession) -> None:
    # Scalar numeric fill touches numeric columns only (i, d) — NOT the string s (Spark fills the
    # matching numeric family). Pinned on VALUE and Arrow TYPE; the filled column's nullability is a
    # disclosed divergence (see module docstring), so it is not asserted.
    filled = _na_frame(spark).fillna(0)
    table = filled.to_arrow()
    assert pa.types.is_int64(table.schema.field("i").type)
    assert pa.types.is_string(table.schema.field("s").type)
    assert pa.types.is_float64(table.schema.field("d").type)
    assert table.to_pylist() == [
        {"i": 1, "s": "a", "d": 1.0},
        {"i": 0, "s": None, "d": 0.0},  # s (string) is NOT filled by a numeric value
        {"i": 3, "s": "c", "d": 0.0},
    ]


def test_parity_na_fill_string_only_touches_string_columns(spark: ReparkSession) -> None:
    # A string fill touches only the string column; i/d stay NULL and nullable. Spark's nullability
    # matches repark here (s non-nullable after fill), so this is a full frame-equal golden.
    result = _na_frame(spark).na.fill("Z")
    golden = pa.table(
        [
            pa.array([1, None, 3], pa.int64()),
            pa.array(["a", "Z", "c"], pa.string()),
            pa.array([1.0, None, None], pa.float64()),
        ],
        schema=pa.schema(
            [
                pa.field("i", pa.int64(), nullable=True),
                pa.field("s", pa.string(), nullable=False),
                pa.field("d", pa.float64(), nullable=True),
            ]
        ),
    )
    assert_frames_equal(result.to_arrow(), golden)


def test_fillna_scalar_with_subset(spark: ReparkSession) -> None:
    # subset restricts the scalar fill to the named columns (still type-matched).
    filled = _na_frame(spark).fillna(0, subset=["i"])
    assert filled.to_arrow().to_pylist() == [
        {"i": 1, "s": "a", "d": 1.0},
        {"i": 0, "s": None, "d": None},  # only i filled; d untouched despite being numeric
        {"i": 3, "s": "c", "d": None},
    ]


def test_fillna_type_families_are_separate(spark: ReparkSession) -> None:
    # bool value fills only boolean columns; a numeric value fills only numeric columns (Spark).
    mixed = spark.createDataFrame([(1, True), (None, None)], ["n", "b"])
    filled_bool = mixed.fillna(True)
    assert filled_bool.to_arrow().to_pylist() == [
        {"n": 1, "b": True},
        {"n": None, "b": True},  # n (numeric) NOT filled by a bool
    ]
    filled_num = mixed.fillna(5)
    assert filled_num.to_arrow().to_pylist() == [
        {"n": 1, "b": True},
        {"n": 5, "b": None},  # b (bool) NOT filled by a numeric
    ]


def test_fillna_is_df_na_fill_alias(spark: ReparkSession) -> None:
    df = _na_frame(spark)
    assert df.fillna({"i": -1}).to_arrow().equals(df.na.fill({"i": -1}).to_arrow())


# ==================================================================================================
# R2 (S1) — fillna(float) into an integer column keeps INTEGER dtype and fills the TRUNCATED value
# ==================================================================================================


def test_parity_fillna_float_into_int_truncates_and_keeps_int(spark: ReparkSession) -> None:
    """R2: a float ``fillna`` into an integer column keeps the column INTEGER and fills the
    **truncated** value — Spark casts the fill value to the column type. The old lowering
    (``coalesce(col, lit(2.5))``) let DataFusion widen the whole column to double with ``2.5``.

    Recorded from live PySpark 4.1.2 (Java 17): ``fillna(2.5)`` into a bigint column → ``[1, 2, 3]``
    still ``bigint``; truncation is toward zero and does NOT round
    (``2.9`` → ``2``, ``-2.5`` → ``-2``). Pinned on value AND Arrow type — the filled-column
    nullability is the disclosed Spark-inconsistency divergence (module docstring), not asserted.
    """
    base = spark.createDataFrame([(1,), (None,), (3,)], ["i"])
    assert pa.types.is_int64(base.to_arrow().schema.field("i").type), "input is bigint"
    for value, expected in [(2.5, [1, 2, 3]), (-2.5, [1, -2, 3]), (2.9, [1, 2, 3])]:
        table = base.fillna(value).to_arrow()
        assert pa.types.is_int64(table.schema.field("i").type), f"fillna({value}) stays bigint"
        assert table.column("i").to_pylist() == expected, (
            f"fillna({value}) fills the truncated value"
        )


def test_parity_fillna_dict_float_into_int_truncates(spark: ReparkSession) -> None:
    # R2 dict form: fillna({i: 2.5}) into a bigint column → [1, 2, 3] still bigint (oracle).
    base = spark.createDataFrame([(1,), (None,), (3,)], ["i"])
    table = base.fillna({"i": 2.5}).to_arrow()
    assert pa.types.is_int64(table.schema.field("i").type), "dict float→int stays bigint"
    assert table.column("i").to_pylist() == [1, 2, 3]


def test_parity_fillna_float_into_double_is_unchanged(spark: ReparkSession) -> None:
    # R2 control: a float fill into a double column is UNCHANGED — value 2.5 kept, still double
    # (so the truncation branch is scoped to integer columns, not all numerics).
    base = spark.createDataFrame([(1.0,), (None,)], ["d"])
    table = base.fillna(2.5).to_arrow()
    assert pa.types.is_float64(table.schema.field("d").type)
    assert table.column("d").to_pylist() == [1.0, 2.5]


def test_parity_fillna_float_into_int32_preserves_int32(spark: ReparkSession) -> None:
    # R2 width preservation: fillna(2.5) into an int32 column stays int32 (not widened to int64 or
    # double). The int32 branch is LIVE — this input names an output the int64 path would not
    # produce (a dead branch would be a defect, docs/testing.md).
    base = spark.sql("SELECT CAST(v AS INT) AS i FROM (VALUES (1), (NULL), (3)) AS t(v)")
    assert pa.types.is_int32(base.to_arrow().schema.field("i").type), "input is int32"
    table = base.fillna(2.5).to_arrow()
    assert pa.types.is_int32(table.schema.field("i").type), "int32 stays int32"
    assert table.column("i").to_pylist() == [1, 2, 3]


def test_parity_fillna_mixed_truncates_int_keeps_double(spark: ReparkSession) -> None:
    # R2 combined (oracle rows from PySpark 4.1.2): fillna(2.5) over (i:bigint, d:double, s:string)
    # fills i with 2 (bigint), d with 2.5 (double), and leaves s (string) untouched.
    mixed = spark.createDataFrame([(1, 1.0, "a"), (None, None, None)], ["i", "d", "s"])
    table = mixed.fillna(2.5).to_arrow()
    assert pa.types.is_int64(table.schema.field("i").type)
    assert pa.types.is_float64(table.schema.field("d").type)
    assert pa.types.is_string(table.schema.field("s").type)
    assert table.to_pylist() == [
        {"i": 1, "d": 1.0, "s": "a"},
        {"i": 2, "d": 2.5, "s": None},
    ]


# ==================================================================================================
# R6 (S3) — fillna / dropna subset accept a str (wrapped, not char-iterated), tuple, or list
# ==================================================================================================


def test_fillna_subset_accepts_str_and_tuple(spark: ReparkSession) -> None:
    # R6: a bare-str subset is wrapped to [str] (PySpark), NOT iterated character-by-character; a
    # tuple behaves like a list. All three forms fill the same single column.
    by_str = _na_frame(spark).fillna(0, subset="i").to_arrow().to_pylist()
    by_tuple = _na_frame(spark).fillna(0, subset=("i",)).to_arrow().to_pylist()
    by_list = _na_frame(spark).fillna(0, subset=["i"]).to_arrow().to_pylist()
    assert by_str == by_tuple == by_list
    assert by_str == [
        {"i": 1, "s": "a", "d": 1.0},
        {"i": 0, "s": None, "d": None},
        {"i": 3, "s": "c", "d": None},
    ]


def test_dropna_subset_accepts_str_and_tuple(spark: ReparkSession) -> None:
    # R6: dropna subset accepts a bare str (and a tuple), like PySpark.
    by_str = _na_frame(spark).dropna(subset="i").to_arrow().to_pylist()
    by_tuple = _na_frame(spark).dropna(subset=("i",)).to_arrow().to_pylist()
    by_list = _na_frame(spark).dropna(subset=["i"]).to_arrow().to_pylist()
    assert by_str == by_tuple == by_list
    assert by_str == [
        {"i": 1, "s": "a", "d": 1.0},
        {"i": 3, "s": "c", "d": None},
    ]


def test_fillna_subset_wrong_type_raises_pyspark_shaped_typeerror(spark: ReparkSession) -> None:
    # R6: a non-str/list/tuple subset raises a PySpark-shaped TypeError. Oracle message classes:
    # NOT_LIST_OR_TUPLE (fillna) vs NOT_LIST_OR_STR_OR_TUPLE (dropna).
    with pytest.raises(TypeError, match="NOT_LIST_OR_TUPLE"):
        _na_frame(spark).fillna(0, subset=5)
    with pytest.raises(TypeError, match="NOT_LIST_OR_STR_OR_TUPLE"):
        _na_frame(spark).dropna(subset=5)


# ==================================================================================================
# E5/E7 — dropna
# ==================================================================================================


def test_parity_dropna_any_drops_rows_with_any_null(spark: ReparkSession) -> None:
    # Default how='any': drop a row with ANY null → only the fully-populated row survives.
    result = _na_frame(spark).dropna()
    golden = pa.table(
        [
            pa.array([1], pa.int64()),
            pa.array(["a"], pa.string()),
            pa.array([1.0], pa.float64()),
        ],
        schema=pa.schema(
            [
                pa.field("i", pa.int64(), nullable=True),
                pa.field("s", pa.string(), nullable=True),
                pa.field("d", pa.float64(), nullable=True),
            ]
        ),
    )
    assert_frames_equal(result.to_arrow(), golden)


def test_dropna_all_drops_only_fully_null_rows(spark: ReparkSession) -> None:
    # how='all': drop a row only when EVERY column is null → the all-null middle row goes, the
    # trailing (3,'c',None) row stays.
    result = _na_frame(spark).dropna(how="all")
    assert result.to_arrow().to_pylist() == [
        {"i": 1, "s": "a", "d": 1.0},
        {"i": 3, "s": "c", "d": None},
    ]


def test_dropna_thresh_keeps_rows_meeting_the_nonnull_floor(spark: ReparkSession) -> None:
    # thresh=2 keeps rows with ≥2 non-null values (overriding how): row1 (3 non-null) and row3
    # (2 non-null) survive; the all-null row (0) is dropped.
    result = _na_frame(spark).dropna(thresh=2)
    assert result.to_arrow().to_pylist() == [
        {"i": 1, "s": "a", "d": 1.0},
        {"i": 3, "s": "c", "d": None},
    ]
    # thresh=3 requires all three non-null → only row1.
    assert _na_frame(spark).dropna(thresh=3).to_arrow().num_rows == 1


def test_dropna_subset_considers_only_named_columns(spark: ReparkSession) -> None:
    # subset=['i']: drop rows whose i is null → the all-null middle row goes; row3 (i=3) stays even
    # though its d is null.
    result = _na_frame(spark).dropna(subset=["i"])
    assert result.to_arrow().to_pylist() == [
        {"i": 1, "s": "a", "d": 1.0},
        {"i": 3, "s": "c", "d": None},
    ]


def test_dropna_how_all_with_subset(spark: ReparkSession) -> None:
    # na.drop(how='all', subset=['i','s']): drop a row only when BOTH i and s are null.
    result = _na_frame(spark).na.drop(how="all", subset=["i", "s"])
    assert result.to_arrow().to_pylist() == [
        {"i": 1, "s": "a", "d": 1.0},
        {"i": 3, "s": "c", "d": None},
    ]


def test_dropna_rejects_bad_how(spark: ReparkSession) -> None:
    with pytest.raises(ValueError, match="how must be"):
        _na_frame(spark).dropna(how="some")
