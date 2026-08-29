"""Facade tests for the DataFrame aggregation family (Group E: E1, E2, E7).

``groupBy``/``agg`` + the aggregate functions in :mod:`repark.functions`, pinned to real
PySpark 4.1.2 (local JVM 17, ``JAVA_HOME=/usr/lib/jvm/zulu-17-amd64``): every golden —
output column name, Arrow type, nullability, values — was executed on real Spark, not
recalled. Parity cases compare through ``repark_parity.assert_frames_equal`` (value AND
type on the Arrow path).
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812 — PySpark idiom: `import ...functions as F`
from repark.errors import AnalysisException
from repark_parity import assert_frames_equal


@pytest.fixture
def spark() -> ReparkSession:
    """A default session (PySpark ``SparkSession.builder.getOrCreate()``)."""
    return ReparkSession.builder.appName("pytest-group-agg").getOrCreate()


@pytest.fixture
def single_partition() -> ReparkSession:
    """A single-partition session so ``first``/``last`` scan input order deterministically."""
    return (
        ReparkSession.builder.appName("pytest-group-agg-1p")
        .config("repark.target.partitions", 1)
        .getOrCreate()
    )


def _sig(table: pa.Table) -> list[tuple[str, str, bool]]:
    """The `(name, arrow-type, nullable)` schema signature — the full parity surface."""
    return [(field.name, str(field.type), field.nullable) for field in table.schema]


def _by(rows: list[dict[str, object]], key: str) -> list[dict[str, object]]:
    """Sort collected rows by a key so an unordered result set compares deterministically."""
    return sorted(rows, key=lambda row: (row[key] is None, row[key]))


# E2 — aggregate functions exist, shadow the builtins, and return Columns


def test_aggregate_functions_are_columns_and_shadow_builtins() -> None:
    from repark import Column

    # PySpark deliberately shadows the builtins sum/min/max/count — F.sum must be the aggregate.
    for name in ("sum", "count", "avg", "mean", "min", "max", "first", "last"):
        assert callable(getattr(F, name)), f"F.{name} exists"
    assert F.mean is F.avg, "mean aliases avg (PySpark)"
    assert F.countDistinct is F.count_distinct, "countDistinct/count_distinct alias identity"
    assert isinstance(F.sum("x"), Column)
    assert isinstance(F.count("*"), Column)


# E1/E2 — output column naming matches PySpark exactly (oracle-verified)


def test_aggregate_output_names_match_pyspark(spark: ReparkSession) -> None:
    # Every name here was read off real PySpark 4.1.2 — count is `count`, count(*) is `count(1)`,
    # mean's column is `avg(x)`, countDistinct is `count(DISTINCT x)`.
    df = spark.sql("SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(g, x)")
    assert df.groupBy("g").agg(F.sum("x")).columns == ["g", "sum(x)"]
    assert df.groupBy("g").agg(F.count("*")).columns == ["g", "count(1)"]
    assert df.groupBy("g").agg(F.count("x")).columns == ["g", "count(x)"]
    assert df.groupBy("g").agg(F.avg("x")).columns == ["g", "avg(x)"]
    assert df.groupBy("g").agg(F.mean("x")).columns == ["g", "avg(x)"]
    assert df.groupBy("g").agg(F.min("x"), F.max("x")).columns == ["g", "min(x)", "max(x)"]
    assert df.groupBy("g").agg(F.countDistinct("x")).columns == ["g", "count(DISTINCT x)"]
    assert df.groupBy("g").count().columns == ["g", "count"]
    assert df.groupBy("g").sum("x").columns == ["g", "sum(x)"]


# R3 — zero-arg GroupedData shortcuts aggregate ALL numeric columns (incl. the grouping key)


def test_parity_groupby_sum_no_args_aggregates_all_numeric_incl_key(spark: ReparkSession) -> None:
    """R3 (S2): no-arg ``sum()`` aggregates EVERY numeric column, key included, in schema
    order (recorded from live PySpark 4.1.2)."""
    source = spark.createDataFrame([(1, 10, 100), (1, 20, 200), (2, 30, 300)], ["g", "x", "y"])
    result = source.groupBy("g").sum()
    assert result.columns == ["g", "sum(g)", "sum(x)", "sum(y)"], "sum(g) included; schema order"
    golden = pa.table(
        [
            pa.array([1, 2], pa.int64()),
            pa.array([2, 2], pa.int64()),
            pa.array([30, 30], pa.int64()),
            pa.array([300, 300], pa.int64()),
        ],
        schema=pa.schema(
            [
                pa.field("g", pa.int64(), nullable=True),
                pa.field("sum(g)", pa.int64(), nullable=True),
                pa.field("sum(x)", pa.int64(), nullable=True),
                pa.field("sum(y)", pa.int64(), nullable=True),
            ]
        ),
    )
    ordered = pa.Table.from_pylist(
        _by(result.to_arrow().to_pylist(), "g"), schema=result.to_arrow().schema
    )
    assert_frames_equal(ordered, golden)


def test_parity_groupby_min_no_args_aggregates_all_numeric_incl_key(spark: ReparkSession) -> None:
    """R3 (S2): no-arg ``min()`` → ``[g, min(g), min(x), min(y)]``, numeric-only (a string
    column is excluded), key included. Oracle: live PySpark 4.1.2, all ``bigint``."""
    source = spark.createDataFrame([(1, 10, 100), (1, 20, 200), (2, 30, 300)], ["g", "x", "y"])
    result = source.groupBy("g").min()
    assert result.columns == ["g", "min(g)", "min(x)", "min(y)"]
    golden = pa.table(
        [
            pa.array([1, 2], pa.int64()),
            pa.array([1, 2], pa.int64()),
            pa.array([10, 30], pa.int64()),
            pa.array([100, 300], pa.int64()),
        ],
        schema=pa.schema(
            [
                pa.field("g", pa.int64(), nullable=True),
                pa.field("min(g)", pa.int64(), nullable=True),
                pa.field("min(x)", pa.int64(), nullable=True),
                pa.field("min(y)", pa.int64(), nullable=True),
            ]
        ),
    )
    ordered = pa.Table.from_pylist(
        _by(result.to_arrow().to_pylist(), "g"), schema=result.to_arrow().schema
    )
    assert_frames_equal(ordered, golden)


def test_groupby_no_arg_shortcut_names_match_pyspark(spark: ReparkSession) -> None:
    # R3: avg/mean/max no-arg naming (oracle-verified): avg/mean both emit avg(<col>), max emits
    # max(<col>), each over all numeric columns including the key.
    source = spark.createDataFrame([(1, 10), (2, 20)], ["g", "x"])
    assert source.groupBy("g").avg().columns == ["g", "avg(g)", "avg(x)"]
    assert source.groupBy("g").mean().columns == ["g", "avg(g)", "avg(x)"]
    assert source.groupBy("g").max().columns == ["g", "max(g)", "max(x)"]


def test_groupby_no_arg_shortcut_excludes_non_numeric_columns(spark: ReparkSession) -> None:
    # R3: a string column is NOT aggregated by the numeric shortcuts (oracle: sum/min/max skip it).
    source = spark.createDataFrame([(1, 10, "a"), (2, 30, "c")], ["g", "x", "s"])
    assert source.groupBy("g").sum().columns == ["g", "sum(g)", "sum(x)"], "string s excluded"
    assert source.groupBy("g").min().columns == ["g", "min(g)", "min(x)"]
    assert source.groupBy("g").max().columns == ["g", "max(g)", "max(x)"]


def test_groupby_accepts_str_and_column_and_groupby_alias(spark: ReparkSession) -> None:
    df = spark.sql("SELECT * FROM (VALUES (1, 10), (1, 20), (2, 30)) AS t(g, x)")
    by_str = df.groupBy("g").agg(F.sum("x"))
    by_col = df.groupby(F.col("g")).agg(F.sum("x"))  # groupby alias + Column arg
    assert _by(by_str.to_arrow().to_pylist(), "g") == [
        {"g": 1, "sum(x)": 30},
        {"g": 2, "sum(x)": 30},
    ]
    assert _by(by_col.to_arrow().to_pylist(), "g") == _by(by_str.to_arrow().to_pylist(), "g")


def test_alias_overrides_the_default_aggregate_name(spark: ReparkSession) -> None:
    df = spark.sql("SELECT * FROM (VALUES (1, 10)) AS t(g, x)")
    out = df.groupBy("g").agg(F.sum("x").alias("total"))
    assert out.columns == ["g", "total"], "an explicit .alias overrides the sum(x) default"


# E7 — NULL-skipping, count(*) vs count(col), int→long widening (the mandatory edge pins)


def test_parity_groupby_sum_skips_nulls(spark: ReparkSession) -> None:
    # sum skips NULLs; the group column leads; both columns nullable, sum is bigint (Spark parity).
    source = spark.createDataFrame([(1, 10), (1, None), (2, 30), (2, 40)], ["g", "x"])
    result = source.groupBy("g").agg(F.sum("x"))
    golden = pa.table(
        [pa.array([1, 2], pa.int64()), pa.array([10, 70], pa.int64())],
        schema=pa.schema(
            [
                pa.field("g", pa.int64(), nullable=True),
                pa.field("sum(x)", pa.int64(), nullable=True),
            ]
        ),
    )
    assert_frames_equal(result.to_arrow(), golden)


def test_parity_count_star_counts_rows_count_col_skips_nulls(spark: ReparkSession) -> None:
    # THE count(*) vs count(col) NULL divergence: count(1) counts every row (incl. the NULL x row);
    # count(x) skips it. Both are non-nullable bigint (Spark). Group 1 has a NULL x.
    source = spark.createDataFrame([(1, 10), (1, None), (2, 30)], ["g", "x"])
    result = source.groupBy("g").agg(F.count("*"), F.count("x"))
    golden = pa.table(
        [
            pa.array([1, 2], pa.int64()),
            pa.array([2, 1], pa.int64()),
            pa.array([1, 1], pa.int64()),
        ],
        schema=pa.schema(
            [
                pa.field("g", pa.int64(), nullable=True),
                pa.field("count(1)", pa.int64(), nullable=False),
                pa.field("count(x)", pa.int64(), nullable=False),
            ]
        ),
    )
    assert_frames_equal(result.to_arrow(), golden)


def test_parity_sum_of_integers_widens_to_long(spark: ReparkSession) -> None:
    # Spark widens sum(IntegerType) → LongType; the sum must not overflow int32.
    source = spark.sql(
        "SELECT g, CAST(x AS INT) AS x FROM (VALUES (1, 2147483647), (1, 1)) AS t(g, x)"
    )
    assert str(source.to_arrow().schema.field("x").type) == "int32", "input is int32"
    result = source.groupBy("g").agg(F.sum("x"))
    column = result.to_arrow().column("sum(x)")
    assert pa.types.is_int64(column.type), "sum(int32) widens to int64 (Spark LongType)"
    assert column.to_pylist() == [2147483648], "no int32 overflow"


def test_parity_avg_is_double(spark: ReparkSession) -> None:
    source = spark.createDataFrame([(1, 10), (1, 20), (2, 30)], ["g", "x"])
    result = source.groupBy("g").agg(F.avg("x"))
    golden = pa.table(
        [pa.array([1, 2], pa.int64()), pa.array([15.0, 30.0], pa.float64())],
        schema=pa.schema(
            [
                pa.field("g", pa.int64(), nullable=True),
                pa.field("avg(x)", pa.float64(), nullable=True),
            ]
        ),
    )
    assert_frames_equal(result.to_arrow(), golden)


def test_parity_min_max_preserve_type(spark: ReparkSession) -> None:
    source = spark.createDataFrame([(1, 10), (1, 40), (2, 30)], ["g", "x"])
    result = source.groupBy("g").agg(F.min("x"), F.max("x"))
    golden = pa.table(
        [
            pa.array([1, 2], pa.int64()),
            pa.array([10, 30], pa.int64()),
            pa.array([40, 30], pa.int64()),
        ],
        schema=pa.schema(
            [
                pa.field("g", pa.int64(), nullable=True),
                pa.field("min(x)", pa.int64(), nullable=True),
                pa.field("max(x)", pa.int64(), nullable=True),
            ]
        ),
    )
    assert_frames_equal(result.to_arrow(), golden)


def test_parity_count_distinct(spark: ReparkSession) -> None:
    # count(DISTINCT x): distinct non-null values per group; non-nullable bigint (Spark).
    source = spark.createDataFrame([(1, 5), (1, 5), (1, 7), (2, 9)], ["g", "x"])
    result = source.groupBy("g").agg(F.countDistinct("x"))
    golden = pa.table(
        [pa.array([1, 2], pa.int64()), pa.array([2, 1], pa.int64())],
        schema=pa.schema(
            [
                pa.field("g", pa.int64(), nullable=True),
                pa.field("count(DISTINCT x)", pa.int64(), nullable=False),
            ]
        ),
    )
    assert_frames_equal(result.to_arrow(), golden)


# E1 — GroupedData shortcuts + dict form


def test_groupby_shortcuts(spark: ReparkSession) -> None:
    source = spark.createDataFrame([(1, 10, 1.5), (1, 20, 2.5), (2, 30, 4.0)], ["g", "x", "y"])
    assert _by(source.groupBy("g").count().to_arrow().to_pylist(), "g") == [
        {"g": 1, "count": 2},
        {"g": 2, "count": 1},
    ]
    assert _by(source.groupBy("g").sum("x", "y").to_arrow().to_pylist(), "g") == [
        {"g": 1, "sum(x)": 30, "sum(y)": 4.0},
        {"g": 2, "sum(x)": 30, "sum(y)": 4.0},
    ]
    assert _by(source.groupBy("g").min("x").to_arrow().to_pylist(), "g") == [
        {"g": 1, "min(x)": 10},
        {"g": 2, "min(x)": 30},
    ]
    assert _by(source.groupBy("g").max("x").to_arrow().to_pylist(), "g") == [
        {"g": 1, "max(x)": 20},
        {"g": 2, "max(x)": 30},
    ]


def test_agg_dict_form_matches_pyspark(spark: ReparkSession) -> None:
    # `.agg({"x": "sum", "y": "max"})` → columns sum(x), max(y) (oracle-verified naming + order).
    source = spark.createDataFrame([(1, 10, 1.5), (1, 20, 2.5), (2, 30, 4.5)], ["g", "x", "y"])
    result = source.groupBy("g").agg({"x": "sum", "y": "max"})
    assert result.columns == ["g", "sum(x)", "max(y)"]
    assert _by(result.to_arrow().to_pylist(), "g") == [
        {"g": 1, "sum(x)": 30, "max(y)": 2.5},
        {"g": 2, "sum(x)": 30, "max(y)": 4.5},
    ]


def test_agg_dict_rejects_unknown_function(spark: ReparkSession) -> None:
    source = spark.createDataFrame([(1, 10)], ["g", "x"])
    with pytest.raises(ValueError, match="unsupported aggregate function"):
        source.groupBy("g").agg({"x": "median"})


# E7 — first/last with ignorenulls (deterministic under a single partition)


def test_parity_first_last_ignorenulls(single_partition: ReparkSession) -> None:
    # One group [NULL, 20, 30] under one partition (input order preserved, like Spark's
    # coalesce(1) oracle run): first(ignorenulls=False) is the NULL; first(ignorenulls=True)
    # skips to 20; last is 30 either way. Names stay `first(v)` / `last(v)` (Spark 4.1.2).
    source = single_partition.createDataFrame([(1, None), (1, 20), (1, 30)], ["g", "v"])
    grouped = source.groupBy("g")

    first_keep = grouped.agg(F.first("v"))
    assert first_keep.columns == ["g", "first(v)"]
    assert first_keep.to_arrow().to_pylist() == [{"g": 1, "first(v)": None}]

    first_skip = grouped.agg(F.first("v", ignorenulls=True))
    assert first_skip.to_arrow().to_pylist() == [{"g": 1, "first(v)": 20}]

    last_skip = grouped.agg(F.last("v", ignorenulls=True))
    assert last_skip.to_arrow().to_pylist() == [{"g": 1, "last(v)": 30}]


def test_first_ignorenulls_is_order_independent_for_a_unique_nonnull(spark: ReparkSession) -> None:
    # Order-independent determinism: with a single non-NULL in the group,
    # first/last(ignorenulls=True) is that value under ANY partitioning — the robust "skips" pin.
    source = spark.createDataFrame([(1, None), (1, 42), (1, None)], ["g", "v"])
    assert source.groupBy("g").agg(F.first("v", ignorenulls=True)).to_arrow().to_pylist() == [
        {"g": 1, "first(v)": 42}
    ]
    assert source.groupBy("g").agg(F.last("v", ignorenulls=True)).to_arrow().to_pylist() == [
        {"g": 1, "last(v)": 42}
    ]


# E7 — empty group vs empty-DataFrame global aggregate


def test_empty_dataframe_grouped_is_zero_rows_global_is_one_null_row(spark: ReparkSession) -> None:
    # Spark: a grouped aggregate over an empty input is ZERO rows; a global (no-group) aggregate
    # over the same empty input is ONE row of NULLs.
    empty = spark.sql("SELECT g, x FROM (VALUES (1, 1)) AS t(g, x) WHERE 1 = 0")
    assert empty.groupBy("g").count().to_arrow().num_rows == 0, "grouped over empty → 0 rows"
    global_agg = empty.agg(F.sum("x"))
    assert global_agg.to_arrow().to_pylist() == [{"sum(x)": None}], "global over empty → 1 NULL row"


def test_df_agg_is_global_single_row(spark: ReparkSession) -> None:
    source = spark.createDataFrame([(1, 10), (2, 30), (2, 40)], ["g", "x"])
    result = source.agg(F.sum("x"))
    assert result.columns == ["sum(x)"]
    assert result.to_arrow().to_pylist() == [{"sum(x)": 80}]


# E8 — an aggregate over an unresolvable column surfaces AnalysisException


def test_aggregate_unresolvable_column_raises_analysis_exception(spark: ReparkSession) -> None:
    source = spark.createDataFrame([(1, 10)], ["g", "x"])
    with pytest.raises(AnalysisException):
        source.groupBy("g").agg(F.sum("no_such_column")).to_arrow()


# GROUP J — collect_list / collect_set / multi-col countDistinct
#
# Oracle: live PySpark 4.1.2. collect_list/collect_set order is nondeterministic in Spark —
# every value pin compares SORTED contents (or single-element groups).


def _sorted_list_field(rows: list[dict[str, object]], group_key: str, list_key: str) -> dict:
    """Map group → sorted list contents (order-nondeterministic aggregates)."""
    return {
        row[group_key]: sorted(row[list_key] or [])  # type: ignore[arg-type, return-value]
        for row in rows
    }


def test_collect_list_set_exist_and_name(spark: ReparkSession) -> None:
    """J1 surface: snake_case only (no camelCase aliases — live PySpark 4.1.2 inspect)."""
    from repark import Column

    assert callable(F.collect_list) and callable(F.collect_set)
    assert not hasattr(F, "collectList")
    assert not hasattr(F, "collectSet")
    assert isinstance(F.collect_list("x"), Column)
    assert isinstance(F.collect_set("x"), Column)
    df = spark.createDataFrame([(1, 10)], ["g", "x"])
    assert df.groupBy("g").agg(F.collect_list("x")).columns == ["g", "collect_list(x)"]
    assert df.groupBy("g").agg(F.collect_set("x")).columns == ["g", "collect_set(x)"]


def test_parity_collect_list_excludes_nulls_and_keeps_dupes(spark: ReparkSession) -> None:
    """J1: ``collect_list`` drops NULLs and keeps duplicates (Spark 4.1.2)."""
    source = spark.createDataFrame(
        [(1, 10), (1, 10), (1, 20), (1, None), (2, 30), (2, None)],
        ["g", "x"],
    )
    result = source.groupBy("g").agg(F.collect_list("x"))
    assert result.columns == ["g", "collect_list(x)"]
    table = result.to_arrow()
    field = table.schema.field("collect_list(x)")
    assert pa.types.is_list(field.type), f"expected list, got {field.type}"
    # Element value type is the input int64 (DataFusion names the child field ``item``).
    assert field.type.value_type == pa.int64()
    by_group = _sorted_list_field(table.to_pylist(), "g", "collect_list(x)")
    assert by_group == {1: [10, 10, 20], 2: [30]}


def test_parity_collect_set_dedups_and_excludes_nulls(spark: ReparkSession) -> None:
    """J1: ``collect_set`` is distinct + NULL-excluding (Spark 4.1.2)."""
    source = spark.createDataFrame(
        [(1, 10), (1, 10), (1, 20), (1, None), (2, 30), (2, 30)],
        ["g", "x"],
    )
    result = source.groupBy("g").agg(F.collect_set("x"))
    assert result.columns == ["g", "collect_set(x)"]
    table = result.to_arrow()
    assert pa.types.is_list(table.schema.field("collect_set(x)").type)
    by_group = _sorted_list_field(table.to_pylist(), "g", "collect_set(x)")
    assert by_group == {1: [10, 20], 2: [30]}


def test_collect_empty_group_is_empty_array_not_null(spark: ReparkSession) -> None:
    """J1: empty group / only-NULL group → ``[]``, not NULL (Spark 4.1.2).

    The SQL-typed NULL column keeps the list value type ``int64`` (not the all-None
    inference trap).
    """
    only_nulls = spark.sql(
        "SELECT * FROM (VALUES (1, CAST(NULL AS BIGINT)), (1, CAST(NULL AS BIGINT))) AS t(g, x)"
    )
    result = only_nulls.groupBy("g").agg(F.collect_list("x"), F.collect_set("x"))
    rows = result.to_arrow().to_pylist()
    assert rows == [{"g": 1, "collect_list(x)": [], "collect_set(x)": []}]
    list_field = result.to_arrow().schema.field("collect_list(x)")
    assert pa.types.is_list(list_field.type)
    assert list_field.type.value_type == pa.int64()

    empty = spark.sql(
        "SELECT g, x FROM (VALUES (CAST(1 AS BIGINT), CAST(1 AS BIGINT))) AS t(g, x) WHERE 1 = 0"
    )
    global_empty = empty.agg(F.collect_list("x"), F.collect_set("x")).to_arrow().to_pylist()
    assert global_empty == [{"collect_list(x)": [], "collect_set(x)": []}]


def test_parity_count_distinct_multi_column_two_and_three(spark: ReparkSession) -> None:
    """J2: multi-col ``countDistinct`` — 2-col and 3-col names, LongType, values.

    Oracle (Spark 4.1.2): ``count(DISTINCT x, y)`` naming (space after commas), Arrow
    ``int64 not null``. The 3-col arm uses a within-group-varying third column: packing
    only the first two args would yield 2 there, so the pin has span semantics.
    """
    source = spark.createDataFrame(
        [
            (1, 10, "a"),
            (1, 10, "b"),
            (1, 20, "a"),
            (1, None, "c"),
            (2, 30, "x"),
            (2, 30, "x"),
            (2, None, None),
        ],
        ["g", "x", "y"],
    )
    two = source.groupBy("g").agg(F.countDistinct("x", "y"))
    assert two.columns == ["g", "count(DISTINCT x, y)"]
    two_table = two.to_arrow()
    two_field = two_table.schema.field("count(DISTINCT x, y)")
    assert two_field.type == pa.int64()
    assert two_field.nullable is False
    assert _by(two_table.to_pylist(), "g") == [
        {"g": 1, "count(DISTINCT x, y)": 3},
        {"g": 2, "count(DISTINCT x, y)": 1},
    ]

    # Third column must vary *inside* a group — ``countDistinct(x, y, g)`` after groupBy(g) is
    # span-hollow (g is constant per group ⇒ 3-col count ≡ 2-col count).
    three_source = spark.createDataFrame(
        [
            (1, 10, "a", 1),
            (1, 10, "a", 2),  # same (x,y) as row above; z differentiates
            (1, 20, "a", 1),
            (1, None, "c", 1),  # any-NULL excluded
            (2, 30, "x", 1),
            (2, 30, "x", 1),
            (2, None, None, 1),
        ],
        ["g", "x", "y", "z"],
    )
    three = three_source.groupBy("g").agg(F.countDistinct("x", "y", "z"))
    assert three.columns == ["g", "count(DISTINCT x, y, z)"]
    three_table = three.to_arrow()
    three_field = three_table.schema.field("count(DISTINCT x, y, z)")
    assert three_field.type == pa.int64()
    assert three_field.nullable is False
    # g=1: (10,a,1), (10,a,2), (20,a,1) → 3; first-two-only would be (10,a), (20,a) → 2.
    assert _by(three_table.to_pylist(), "g") == [
        {"g": 1, "count(DISTINCT x, y, z)": 3},
        {"g": 2, "count(DISTINCT x, y, z)": 1},
    ]


def test_parity_count_distinct_multi_excludes_any_null_row(spark: ReparkSession) -> None:
    """J2: a row is excluded from multi-col countDistinct when **any** column is NULL."""
    source = spark.createDataFrame(
        [(1, None), (1, 1), (1, 1), (None, 2), (2, 2)],
        ["a", "b"],
    )
    result = source.agg(F.countDistinct("a", "b"))
    assert result.columns == ["count(DISTINCT a, b)"]
    table = result.to_arrow()
    assert table.schema.field(0).type == pa.int64()
    assert table.schema.field(0).nullable is False
    assert table.to_pylist() == [{"count(DISTINCT a, b)": 2}]


def test_collect_and_count_distinct_work_in_agg_and_global(spark: ReparkSession) -> None:
    """J3: expr form + dict form inside ``groupBy().agg`` and global ``df.agg``."""
    source = spark.createDataFrame(
        [(1, 10), (1, 10), (1, 20), (1, None), (2, 30)],
        ["g", "x"],
    )
    expr_form = source.groupBy("g").agg(
        F.collect_list("x"), F.collect_set("x"), F.countDistinct("x")
    )
    assert expr_form.columns == ["g", "collect_list(x)", "collect_set(x)", "count(DISTINCT x)"]
    rows = expr_form.to_arrow().to_pylist()
    assert _sorted_list_field(rows, "g", "collect_list(x)") == {1: [10, 10, 20], 2: [30]}
    assert _sorted_list_field(rows, "g", "collect_set(x)") == {1: [10, 20], 2: [30]}

    dict_list = source.groupBy("g").agg({"x": "collect_list"})
    assert dict_list.columns == ["g", "collect_list(x)"]
    # Value pin (not just name): dict "collect_list" must keep dupes the same as the expr form.
    assert _sorted_list_field(dict_list.to_arrow().to_pylist(), "g", "collect_list(x)") == {
        1: [10, 10, 20],
        2: [30],
    }
    dict_set = source.groupBy("g").agg({"x": "collect_set"})
    assert dict_set.columns == ["g", "collect_set(x)"]
    assert _sorted_list_field(dict_set.to_arrow().to_pylist(), "g", "collect_set(x)") == {
        1: [10, 20],
        2: [30],
    }

    # Multi-key dict form (octo r2): both reducers must bind — name + sorted values.
    multi_source = spark.createDataFrame(
        [(1, 10, 100), (1, 10, 200), (1, 20, 100)],
        ["g", "x", "y"],
    )
    dict_multi = multi_source.groupBy("g").agg({"x": "collect_list", "y": "collect_set"})
    assert dict_multi.columns == ["g", "collect_list(x)", "collect_set(y)"]
    multi_rows = dict_multi.to_arrow().to_pylist()
    assert _sorted_list_field(multi_rows, "g", "collect_list(x)") == {1: [10, 10, 20]}
    assert _sorted_list_field(multi_rows, "g", "collect_set(y)") == {1: [100, 200]}

    global_row = (
        source.agg(F.collect_list("x"), F.collect_set("x"), F.countDistinct("x"))
        .to_arrow()
        .to_pylist()[0]
    )
    assert sorted(global_row["collect_list(x)"]) == [10, 10, 20, 30]
    assert sorted(global_row["collect_set(x)"]) == [10, 20, 30]
    assert global_row["count(DISTINCT x)"] == 3


def test_parity_collect_list_set_string_empty_vs_null(spark: ReparkSession) -> None:
    """J1 (octo r1): string collect keeps empty string, drops NULL (Spark 4.1.2)."""
    source = spark.createDataFrame(
        [(1, ""), (1, None), (1, ""), (1, "x")],
        ["g", "s"],
    )
    result = source.groupBy("g").agg(F.collect_list("s"), F.collect_set("s"))
    assert result.columns == ["g", "collect_list(s)", "collect_set(s)"]
    table = result.to_arrow()
    field = table.schema.field("collect_list(s)")
    assert pa.types.is_list(field.type)
    # Value type is a string family (utf8 or string_view depending on DF/Arrow path).
    value_type = field.type.value_type
    assert pa.types.is_string(value_type) or pa.types.is_string_view(value_type), value_type
    rows = table.to_pylist()
    assert _sorted_list_field(rows, "g", "collect_list(s)") == {1: ["", "", "x"]}
    assert _sorted_list_field(rows, "g", "collect_set(s)") == {1: ["", "x"]}


def test_parity_count_distinct_multi_empty_frame_is_zero(spark: ReparkSession) -> None:
    """J2 (octo r1): empty frame multi-col countDistinct → 0 (Spark 4.1.2), not NULL."""
    empty = spark.sql(
        "SELECT a, b FROM (VALUES (CAST(1 AS BIGINT), CAST(1 AS BIGINT))) AS t(a, b) WHERE 1 = 0"
    )
    result = empty.agg(F.countDistinct("a", "b"))
    table = result.to_arrow()
    assert result.columns == ["count(DISTINCT a, b)"]
    assert table.schema.field(0).type == pa.int64()
    assert table.schema.field(0).nullable is False
    assert table.to_pylist() == [{"count(DISTINCT a, b)": 0}]


def test_cross_engine_collect_and_multi_count_distinct_vs_pyspark() -> None:
    """Cross-engine e2e: groupBy + collect_list + collect_set + 2-col countDistinct vs live Spark.

    Requires pyspark + a Java-17 JVM; skips cleanly when pyspark is absent, no usable JVM
    is found, or the Spark gateway fails to launch. List contents compare **sorted**
    (order-nondeterministic).
    """
    pytest.importorskip("pyspark")
    import os
    from pathlib import Path

    # Prefer the recorded oracle JVM: Spark 4.1.2 requires class-file 61 (Java 17+); a wrong
    # ambient JAVA_HOME must not hard-fail the routine suite when pyspark is installed.
    jvm_home = Path("/usr/lib/jvm/zulu-17-amd64")
    if jvm_home.is_dir():
        os.environ["JAVA_HOME"] = str(jvm_home)
    elif not os.environ.get("JAVA_HOME"):
        pytest.skip("no JVM for live PySpark oracle")
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as spark_functions

    rows = [
        (1, 10, "a"),
        (1, 10, "b"),
        (1, 20, "a"),
        (1, None, "c"),
        (2, 30, "x"),
        (2, 30, "x"),
        (2, None, None),
    ]
    columns = ["g", "x", "y"]

    # --- repark ---
    repark = ReparkSession.builder.appName("j-cross-repark").getOrCreate()
    repark_df = repark.createDataFrame(rows, columns)
    repark_out = (
        repark_df.groupBy("g")
        .agg(F.collect_list("x"), F.collect_set("x"), F.countDistinct("x", "y"))
        .to_arrow()
    )
    repark_rows = _by(repark_out.to_pylist(), "g")

    # --- live Spark (skip, never hard-fail, when the gateway cannot start) ---
    try:
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("j-cross-spark")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate()
        )
    except Exception as err:
        # Any gateway/JVM failure is a skip, not a product RED (routine suite stays green).
        pytest.skip(f"live PySpark oracle unavailable: {err}")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        spark_df = spark.createDataFrame(rows, columns)
        spark_out = (
            spark_df.groupBy("g")
            .agg(
                spark_functions.collect_list("x"),
                spark_functions.collect_set("x"),
                spark_functions.countDistinct("x", "y"),
            )
            .orderBy("g")
        )
        spark_rows = [row.asDict() for row in spark_out.collect()]
        spark_arrow = spark_out.toArrow()
    finally:
        spark.stop()

    assert repark_out.column_names == spark_arrow.column_names
    # countDistinct LongType non-null on both
    assert repark_out.schema.field("count(DISTINCT x, y)").type == pa.int64()
    assert repark_out.schema.field("count(DISTINCT x, y)").nullable is False
    assert str(spark_arrow.schema.field("count(DISTINCT x, y)").type) == "int64"
    assert spark_arrow.schema.field("count(DISTINCT x, y)").nullable is False

    # Absolute oracle scalars (not only mutual repark==Spark equality).
    expected_counts = {1: 3, 2: 1}
    for repark_row, spark_row in zip(repark_rows, spark_rows, strict=True):
        assert repark_row["g"] == spark_row["g"]
        repark_list = sorted(repark_row["collect_list(x)"] or [])
        spark_list = sorted(spark_row["collect_list(x)"] or [])
        assert repark_list == spark_list
        repark_set = sorted(repark_row["collect_set(x)"] or [])
        spark_set = sorted(spark_row["collect_set(x)"] or [])
        assert repark_set == spark_set
        assert repark_row["count(DISTINCT x, y)"] == spark_row["count(DISTINCT x, y)"]
        assert repark_row["count(DISTINCT x, y)"] == expected_counts[repark_row["g"]]


def test_mutation_collect_set_is_not_list_routing(spark: ReparkSession) -> None:
    """Mutation proof 1: collect_set must dedup — a list-routing swap would keep duplicates."""
    source = spark.createDataFrame([(1, 10), (1, 10), (1, 20)], ["g", "x"])
    collected = source.groupBy("g").agg(F.collect_set("x")).to_arrow().to_pylist()
    assert _sorted_list_field(collected, "g", "collect_set(x)") == {1: [10, 20]}
    # Explicit: length must be 2, not 3 (the list-routing failure mode).
    assert len(collected[0]["collect_set(x)"]) == 2


def test_mutation_multi_count_distinct_is_not_first_col_only(spark: ReparkSession) -> None:
    """Mutation proof 2: multi-col countDistinct must see every column — first-col-only yields 2."""
    source = spark.createDataFrame([(1, "a"), (1, "b"), (2, "a")], ["x", "y"])
    result = source.agg(F.countDistinct("x", "y")).to_arrow().to_pylist()
    assert result == [{"count(DISTINCT x, y)": 3}]


def test_mutation_multi_count_distinct_null_if_any_pack(spark: ReparkSession) -> None:
    """Mutation proof 3: the pack must null the whole row when ANY column is NULL.

    A bare ``struct(a,b)`` without the CASE null-if-any wrapper counts null-field structs
    as distinct keys (4) and goes RED.
    """
    source = spark.createDataFrame(
        [(1, None), (1, 1), (1, 1), (None, 2), (2, 2)],
        ["a", "b"],
    )
    result = source.agg(F.countDistinct("a", "b")).to_arrow().to_pylist()
    assert result == [{"count(DISTINCT a, b)": 2}]


def test_mutation_multi_count_distinct_third_col_matters(spark: ReparkSession) -> None:
    """Mutation proof 4: 3-col pack must see the third column (octo r1 / C1-Q-001)."""
    source = spark.createDataFrame(
        [(1, "a", 1), (1, "a", 2), (1, "a", 1)],
        ["x", "y", "z"],
    )
    result = source.agg(F.countDistinct("x", "y", "z")).to_arrow().to_pylist()
    assert result == [{"count(DISTINCT x, y, z)": 2}]
    # Explicit failure-mode contrast: first-two-only would be 1.
    first_two = source.agg(F.countDistinct("x", "y")).to_arrow().to_pylist()
    assert first_two == [{"count(DISTINCT x, y)": 1}]


def test_collect_set_signed_zero_preserves_distinct_bits(spark: ReparkSession) -> None:
    """J1 float edge (octo r2 + r4): DF DISTINCT keeps IEEE ``-0.0`` distinct from ``+0.0``.

    Live PySpark 4.1.2 normalizes ``-0.0`` to ``+0`` (set cardinality 1, multi-col count 1);
    repark preserves the sign bit. Standing pin documents the accepted divergence, not a
    silent defect.
    """
    import math

    # U2: SQL `-0.0` parses as DECIMAL 0 (no sign bit). IEEE signed-zero must enter
    # as a Python float via createDataFrame.
    source = spark.createDataFrame(
        [(1, -0.0), (1, 0.0), (1, 0.0)],
        ["g", "x"],
    )
    result = source.groupBy("g").agg(F.collect_list("x"), F.collect_set("x"))
    collected = result.to_arrow().to_pylist()
    assert len(collected) == 1
    list_vals = collected[0]["collect_list(x)"]
    set_vals = collected[0]["collect_set(x)"]
    assert len(list_vals) == 3
    # Set keeps both signed zeros under DF DISTINCT (Spark collapses to one +0).
    assert len(set_vals) == 2
    signs = sorted(math.copysign(1.0, value) for value in set_vals)
    assert signs == [-1.0, 1.0]

    # Octo r4: multi-col pack inherits the same IEEE divergence vs Spark (oracle count = 1).
    multi = spark.createDataFrame(
        [(-0.0, 0.0), (0.0, 0.0), (-0.0, -0.0)],
        ["a", "b"],
    )
    multi_count = multi.agg(F.countDistinct("a", "b")).to_arrow().to_pylist()
    assert multi_count == [{"count(DISTINCT a, b)": 3}]


def test_dict_agg_collect_function_names_are_case_insensitive(spark: ReparkSession) -> None:
    """J3 (octo r4): dict reducer names match Spark case-insensitively (snake_case out)."""
    source = spark.createDataFrame(
        [(1, 10), (1, 10), (1, 20), (1, None)],
        ["g", "x"],
    )
    list_out = source.groupBy("g").agg({"x": "COLLECT_LIST"})
    assert list_out.columns == ["g", "collect_list(x)"]
    assert _sorted_list_field(list_out.to_arrow().to_pylist(), "g", "collect_list(x)") == {
        1: [10, 10, 20],
    }
    set_out = source.groupBy("g").agg({"x": "Collect_Set"})
    assert set_out.columns == ["g", "collect_set(x)"]
    assert _sorted_list_field(set_out.to_arrow().to_pylist(), "g", "collect_set(x)") == {
        1: [10, 20],
    }
    # CamelCase still fail-loud (Spark UNRESOLVED_ROUTINE — both engines reject).
    with pytest.raises(ValueError, match="collectList"):
        source.groupBy("g").agg({"x": "collectList"})


def test_parity_count_distinct_multi_empty_string_vs_null(spark: ReparkSession) -> None:
    """J2 (octo r4): empty string is a real multi-cd key; NULL still excludes the row."""
    source = spark.createDataFrame(
        [("", "a"), ("", "a"), (None, "a"), ("x", ""), ("x", None), ("x", "")],
        ["a", "b"],
    )
    result = source.agg(F.countDistinct("a", "b"))
    assert result.columns == ["count(DISTINCT a, b)"]
    table = result.to_arrow()
    assert table.schema.field(0).type == pa.int64()
    assert table.schema.field(0).nullable is False
    assert table.to_pylist() == [{"count(DISTINCT a, b)": 2}]
