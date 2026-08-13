"""R-PIVOT: GroupedData.pivot two-phase conditional aggregation."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812
from repark.errors import AnalysisException


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pivot").getOrCreate()
    try:
        yield session
    finally:
        session.stop()


@pytest.fixture
def frame(spark: ReparkSession) -> object:
    return spark.createDataFrame(
        [
            ("a", 1, 10),
            ("a", 2, 20),
            ("b", 1, 30),
            ("b", 2, None),
            (None, 1, 40),
        ],
        ["g", "p", "x"],
    )


def test_pivot_values_list_single_agg(frame: object) -> None:
    out = frame.groupBy("g").pivot("p", [1, 2]).sum("x").orderBy("g").to_arrow().to_pylist()
    # Spark: g, 1, 2
    assert out[0]["g"] is None and out[0]["1"] == 40
    a_row = next(row for row in out if row["g"] == "a")
    assert a_row["1"] == 10 and a_row["2"] == 20
    b_row = next(row for row in out if row["g"] == "b")
    assert b_row["1"] == 30 and b_row["2"] is None


def test_pivot_values_list_column_order(frame: object) -> None:
    """Values-list form preserves caller order (octo C3-Q-005) — not set-membership only."""
    single_cols = frame.groupBy("g").pivot("p", [2, 1]).sum("x").columns
    assert single_cols == ["g", "2", "1"]
    multi_cols = frame.groupBy("g").pivot("p", [2, 1]).agg(F.sum("x"), F.count("x")).columns
    # value-major then aggregate order (Spark live 4.1.2)
    assert multi_cols == ["g", "2_sum(x)", "2_count(x)", "1_sum(x)", "1_count(x)"]


def test_pivot_inferred_matches_values_list(frame: object) -> None:
    listed = frame.groupBy("g").pivot("p", [1, 2]).sum("x").orderBy("g").to_arrow().to_pylist()
    inferred = frame.groupBy("g").pivot("p").sum("x").orderBy("g").to_arrow().to_pylist()

    # Same multiset of rows (column order of pivot values may follow sort).
    def key(row: dict) -> tuple:
        g = row["g"]
        return (g is not None, g)

    assert sorted(listed, key=key) == sorted(inferred, key=key)


def test_pivot_multi_agg_column_names(frame: object) -> None:
    cols = frame.groupBy("g").pivot("p", [1, 2]).agg(F.sum("x"), F.count("x")).columns
    assert cols[0] == "g"
    # Order pin (C3-Q-005): value-major then aggregate list
    assert cols == ["g", "1_sum(x)", "1_count(x)", "2_sum(x)", "2_count(x)"]


def test_pivot_multi_agg_values(frame: object) -> None:
    """Pin multi-agg *values* (not only names) — mutation-proof vs builder/CASE bugs."""
    out = (
        frame.groupBy("g")
        .pivot("p", [1, 2])
        .agg(F.sum("x"), F.count("x"))
        .orderBy("g")
        .to_arrow()
        .to_pylist()
    )
    a_row = next(row for row in out if row["g"] == "a")
    assert a_row["1_sum(x)"] == 10 and a_row["2_sum(x)"] == 20
    assert a_row["1_count(x)"] == 1 and a_row["2_count(x)"] == 1
    b_row = next(row for row in out if row["g"] == "b")
    assert b_row["1_sum(x)"] == 30 and b_row["2_sum(x)"] is None
    # count(x) skips null x on (b,2,None)
    assert b_row["1_count(x)"] == 1 and b_row["2_count(x)"] == 0
    none_row = next(row for row in out if row["g"] is None)
    assert none_row["1_sum(x)"] == 40 and none_row["1_count(x)"] == 1
    assert none_row["2_sum(x)"] is None and none_row["2_count(x)"] == 0


def test_pivot_null_value_column_name(frame: object) -> None:
    cols = frame.groupBy("g").pivot("p", [None, 1]).sum("x").columns
    assert "null" in cols
    assert "1" in cols


def test_pivot_null_value_aggregates(spark: ReparkSession) -> None:
    """Null pivot *condition* (IS NULL), not only the output column name (octo C1-Q-002)."""
    frame = spark.createDataFrame(
        [
            ("a", None, 10),
            ("a", 1, 20),
            ("b", None, 5),
            ("b", 1, None),
        ],
        ["g", "p", "x"],
    )
    out = frame.groupBy("g").pivot("p", [None, 1]).sum("x").orderBy("g").to_arrow().to_pylist()
    a_row = next(row for row in out if row["g"] == "a")
    assert a_row["null"] == 10 and a_row["1"] == 20
    b_row = next(row for row in out if row["g"] == "b")
    assert b_row["null"] == 5 and b_row["1"] is None


def test_pivot_count_shortcut_values(frame: object) -> None:
    """GroupedData.count() uses bare agg_name ``count`` — must rebuild under pivot (C1-Q-001)."""
    out = frame.groupBy("g").pivot("p", [1, 2]).count().orderBy("g").to_arrow().to_pylist()
    a_row = next(row for row in out if row["g"] == "a")
    assert a_row["1"] == 1 and a_row["2"] == 1
    b_row = next(row for row in out if row["g"] == "b")
    assert b_row["1"] == 1 and b_row["2"] == 1  # counts the null-x row too
    none_row = next(row for row in out if row["g"] is None)
    assert none_row["1"] == 1 and none_row["2"] == 0


def test_pivot_agg_with_explicit_alias(frame: object) -> None:
    """Explicit .alias() clears _agg_name; recover kind + use alias as multi suffix (C1-L-003)."""
    single = (
        frame.groupBy("g")
        .pivot("p", [1, 2])
        .agg(F.sum("x").alias("total"))
        .orderBy("g")
        .to_arrow()
        .to_pylist()
    )
    a_row = next(row for row in single if row["g"] == "a")
    assert a_row["1"] == 10 and a_row["2"] == 20

    multi = (
        frame.groupBy("g")
        .pivot("p", [1, 2])
        .agg(F.sum("x").alias("total"), F.count("x").alias("n"))
        .orderBy("g")
        .to_arrow()
        .to_pylist()
    )
    multi_plan = frame.groupBy("g").pivot("p", [1, 2])
    multi_gd = multi_plan.agg(F.sum("x").alias("total"), F.count("x").alias("n"))
    cols = multi_gd.columns
    assert "1_total" in cols and "2_total" in cols
    assert "1_n" in cols and "2_n" in cols
    a_multi = next(row for row in multi if row["g"] == "a")
    assert a_multi["1_total"] == 10 and a_multi["2_total"] == 20
    assert a_multi["1_n"] == 1 and a_multi["2_n"] == 1


def test_pivot_max_values_overflow(spark: ReparkSession) -> None:
    spark.stop()
    session = (
        ReparkSession.builder.appName("pivot-cap")
        .config("spark.sql.pivotMaxValues", "2")
        .getOrCreate()
    )
    try:
        big = session.createDataFrame([(i % 3, i) for i in range(10)], ["g", "p"])
        with pytest.raises(AnalysisException, match="pivotMaxValues"):
            big.groupBy("g").pivot("p").count().collect()
    finally:
        session.stop()


def test_pivot_values_list_ignores_pivot_max_values(spark: ReparkSession) -> None:
    """Values-list form is not subject to ``pivotMaxValues`` (octo C8-Q-001).

    Cap overflow tests only exercise inferred form. A mutation that raises when
    ``len(explicit_values) > max`` on the ``_pivot_values_explicit`` branch stays
    green without this pin. Spark applies the conf only when values are discovered.
    """
    spark.stop()
    session = (
        ReparkSession.builder.appName("pivot-values-list-cap")
        .config("spark.sql.pivotMaxValues", "2")
        .getOrCreate()
    )
    try:
        frame = session.createDataFrame(
            [(0, 1, 10), (0, 2, 20), (0, 3, 30), (1, 1, 40)],
            ["g", "p", "x"],
        )
        # Explicit list longer than cap → must succeed with all three value columns.
        listed = frame.groupBy("g").pivot("p", [1, 2, 3]).sum("x")
        assert "1" in listed.columns and "2" in listed.columns and "3" in listed.columns
        out = listed.orderBy("g").to_arrow().to_pylist()
        zero = next(row for row in out if row["g"] == 0)
        assert zero["1"] == 10 and zero["2"] == 20 and zero["3"] == 30
        one = next(row for row in out if row["g"] == 1)
        assert one["1"] == 40 and one.get("2") in (0, None) and one.get("3") in (0, None)

        # Paired pin: inferred 3-distinct still overflows under the same cap.
        with pytest.raises(AnalysisException, match="pivotMaxValues"):
            frame.groupBy("g").pivot("p").sum("x").collect()
    finally:
        session.stop()


def test_pivot_max_values_equality_boundary(spark: ReparkSession) -> None:
    """Inferred form succeeds at exactly ``pivotMaxValues`` distincts (octo C5-Q-002).

    Overflow is ``len > max`` (Spark), not ``len >= max``. A mutation to ``>=`` stays green
    without this success pin — overflow-only tests never exercise the equality edge.
    """
    spark.stop()
    session = (
        ReparkSession.builder.appName("pivot-cap-eq")
        .config("spark.sql.pivotMaxValues", "2")
        .getOrCreate()
    )
    try:
        # Exactly 2 distinct pivot values with cap 2 → must succeed (not overflow).
        exact = session.createDataFrame([(0, 1), (0, 2), (1, 1)], ["g", "p"])
        out = exact.groupBy("g").pivot("p").count().orderBy("g").to_arrow().to_pylist()
        zero = next(row for row in out if row["g"] == 0)
        assert zero["1"] == 1 and zero["2"] == 1
        one = next(row for row in out if row["g"] == 1)
        assert one["1"] == 1 and one.get("2") in (0, None)

        # 3 distinct with cap 2 → still overflow (paired pin; > not silently disabled).
        over = session.createDataFrame([(0, 0), (0, 1), (0, 2)], ["g", "p"])
        with pytest.raises(AnalysisException, match="pivotMaxValues"):
            over.groupBy("g").pivot("p").count().collect()
    finally:
        session.stop()


def test_pivot_inferred_distinct_before_limit(spark: ReparkSession) -> None:
    """``distinct()`` before ``limit(max+1)`` is load-bearing (octo C3-Q-002).

    Without distinct, ``limit(max+1)`` on a long run of one pivot value yields
    ``len(rows) > max`` and false-positive overflow even when distinct cardinality is 1.
    """
    spark.stop()
    session = (
        ReparkSession.builder.appName("pivot-distinct")
        .config("spark.sql.pivotMaxValues", "2")
        .getOrCreate()
    )
    try:
        # 10 duplicate rows of the only pivot value — cardinality 1 ≤ cap 2.
        mono = session.createDataFrame([(0, 7)] * 10, ["g", "p"])
        out = mono.groupBy("g").pivot("p").count().to_arrow().to_pylist()
        assert len(out) == 1 and out[0]["7"] == 10

        # Still overflow when true distinct cardinality exceeds cap (order: many 0s first).
        wide = session.createDataFrame([(0, 0)] * 50 + [(0, 1), (0, 2)], ["g", "p"])
        with pytest.raises(AnalysisException, match="pivotMaxValues"):
            wide.groupBy("g").pivot("p").count().collect()
    finally:
        session.stop()


def test_pivot_inferred_under_cap_count(spark: ReparkSession) -> None:
    """Success-path inferred count (overflow test alone never exercised builder count)."""
    frame = spark.createDataFrame([(0, 1), (0, 2), (1, 1)], ["g", "p"])
    out = frame.groupBy("g").pivot("p").count().orderBy("g").to_arrow().to_pylist()
    zero = next(row for row in out if row["g"] == 0)
    assert zero["1"] == 1 and zero["2"] == 1
    one = next(row for row in out if row["g"] == 1)
    assert one["1"] == 1 and one.get("2") in (0, None)


def test_pivot_cube_rollup_refused(frame: object) -> None:
    """CUBE/ROLLUP + pivot is not a safe SQL surface (octo C1-SEC-001)."""
    with pytest.raises(AnalysisException, match="cube/rollup"):
        frame.cube("g").pivot("p", [1, 2])
    with pytest.raises(AnalysisException, match="cube/rollup"):
        frame.rollup("g").pivot("p", [1])


def test_pivot_count_distinct_refused(frame: object) -> None:
    """countDistinct must not silently rebuild as non-distinct count (octo C1-L-005)."""
    with pytest.raises(AnalysisException, match="countDistinct"):
        frame.groupBy("g").pivot("p", [1, 2]).agg(F.countDistinct("x")).collect()
    # Aliased form still refuses (recovery keeps ``count(DISTINCT x)``).
    with pytest.raises(AnalysisException, match="countDistinct"):
        frame.groupBy("g").pivot("p", [1, 2]).agg(F.countDistinct("x").alias("n")).collect()


def test_pivot_count_measure_named_distinct_id(spark: ReparkSession) -> None:
    """``F.count(\"distinct_id\")`` is non-null count, not false countDistinct (octo C7-L-001).

    ``startswith(\"count(distinct\")`` matches ``count(distinct_id)`` / ``count(distinct)``
    and raised AnalysisException. True countDistinct is ``count(DISTINCT x)`` (space after
    DISTINCT). Pin Arrow values so a silent refuse→zero or wrong error stays red.
    """
    frame = spark.createDataFrame(
        [
            ("a", 1, 10),
            ("a", 1, None),
            ("a", 2, 20),
            ("b", 1, 30),
        ],
        ["g", "p", "distinct_id"],
    )
    out = (
        frame.groupBy("g")
        .pivot("p", [1, 2])
        .agg(F.count("distinct_id"))
        .orderBy("g")
        .to_arrow()
        .to_pylist()
    )
    a_row = next(row for row in out if row["g"] == "a")
    # Non-null count of measure: p=1 has one non-null (10), p=2 has one (20).
    assert a_row["1"] == 1 and a_row["2"] == 1
    b_row = next(row for row in out if row["g"] == "b")
    assert b_row["1"] == 1 and b_row["2"] == 0

    # Column literally named ``distinct`` (agg name ``count(distinct)``) must also work.
    distinct_col = spark.createDataFrame(
        [("a", 1, 7), ("a", 1, None), ("a", 2, 8)],
        ["g", "p", "distinct"],
    )
    d_out = (
        distinct_col.groupBy("g")
        .pivot("p", [1, 2])
        .agg(F.count("distinct"))
        .to_arrow()
        .to_pylist()[0]
    )
    assert d_out["1"] == 1 and d_out["2"] == 1


def test_w5_disclosure_removed(frame: object) -> None:
    # Former loud UnsupportedOperationException is gone — success is the done-signal.
    out = frame.groupBy("g").pivot("p", [1]).sum("x")
    assert out is not None
    assert "1" in out.columns


def test_pivot_avg_min_max_values(spark: ReparkSession, frame: object) -> None:
    """Arrow-path value pins for avg/min/max — mutation-proof vs sum swap (octo C2-Q-003)."""
    avg_out = frame.groupBy("g").pivot("p", [1, 2]).avg("x").orderBy("g").to_arrow().to_pylist()
    a_avg = next(row for row in avg_out if row["g"] == "a")
    assert a_avg["1"] == 10.0 and a_avg["2"] == 20.0
    b_avg = next(row for row in avg_out if row["g"] == "b")
    assert b_avg["1"] == 30.0 and b_avg["2"] is None
    # Type identity: avg is float; sum would keep int for these fixtures.
    assert isinstance(a_avg["1"], float)

    min_out = frame.groupBy("g").pivot("p", [1, 2]).min("x").orderBy("g").to_arrow().to_pylist()
    a_min = next(row for row in min_out if row["g"] == "a")
    assert a_min["1"] == 10 and a_min["2"] == 20

    # Multi-row groups so min ≠ max ≠ sum (function-identity mutation pin).
    multi = spark.createDataFrame(
        [("a", 1, 5), ("a", 1, 15), ("a", 2, 3), ("a", 2, 9)],
        ["g", "p", "x"],
    )
    min_multi = multi.groupBy("g").pivot("p", [1, 2]).min("x").to_arrow().to_pylist()[0]
    max_multi = multi.groupBy("g").pivot("p", [1, 2]).max("x").to_arrow().to_pylist()[0]
    sum_multi = multi.groupBy("g").pivot("p", [1, 2]).sum("x").to_arrow().to_pylist()[0]
    assert min_multi["1"] == 5 and max_multi["1"] == 15 and sum_multi["1"] == 20
    assert min_multi["2"] == 3 and max_multi["2"] == 9 and sum_multi["2"] == 12
    assert min_multi["1"] != max_multi["1"]
    assert max_multi["1"] != sum_multi["1"]


def test_pivot_non_simple_agg_input_refused(frame: object) -> None:
    """Compound / lit / CAST pivot aggregates fail loud — no F.col fail-open (octo C2-Q-001)."""
    with pytest.raises(AnalysisException, match="simple column-name"):
        frame.groupBy("g").pivot("p", [1]).agg(F.sum(F.col("x") + 1)).collect()
    with pytest.raises(AnalysisException, match="simple column-name"):
        frame.groupBy("g").pivot("p", [1]).agg(F.sum(F.lit(1))).collect()
    with pytest.raises(AnalysisException, match="simple column-name"):
        frame.groupBy("g").pivot("p", [1]).agg(F.sum(F.col("x").cast("double"))).collect()
    # count(cast)/count(abs) must refuse too — not silent row-count (octo C6-Q-001).
    with pytest.raises(AnalysisException, match="simple column-name"):
        frame.groupBy("g").pivot("p", [1]).agg(F.count(F.col("x").cast("double"))).collect()
    with pytest.raises(AnalysisException, match="simple column-name"):
        frame.groupBy("g").pivot("p", [1]).agg(F.count(F.abs(F.col("x")))).collect()


def test_pivot_first_last_ignorenulls(spark: ReparkSession) -> None:
    """CASE injects NULLs; pivot rebuild forces ignorenulls=True like Spark (C2-L-001/C2-Q-002).

    Order-sensitive first/last pins require a single target partition (octo C3-Q-003) —
    multi-partition shuffles do not preserve input row order.
    """
    spark.stop()
    session = (
        ReparkSession.builder.appName("pivot-first-last")
        .config("repark.target.partitions", 1)
        .getOrCreate()
    )
    try:
        # Non-matching pivot row first so default first(ignoreNulls=false) would see NULL.
        frame = session.createDataFrame(
            [
                ("a", 2, 99),
                ("a", 1, 10),
                ("a", 1, None),
                ("a", 2, 88),
            ],
            ["g", "p", "x"],
        )
        first_out = (
            frame.groupBy("g").pivot("p", [1, 2]).agg(F.first("x")).to_arrow().to_pylist()[0]
        )
        # Matching value 10 must win over CASE-injected leading NULLs (not None).
        assert first_out["1"] == 10
        assert first_out["2"] == 99

        # Explicit ignorenulls=True must not be dropped (same rebuild path).
        first_true = (
            frame.groupBy("g")
            .pivot("p", [1, 2])
            .agg(F.first("x", ignorenulls=True))
            .to_arrow()
            .to_pylist()[0]
        )
        assert first_true["1"] == 10 and first_true["2"] == 99

        last_out = frame.groupBy("g").pivot("p", [1, 2]).agg(F.last("x")).to_arrow().to_pylist()[0]
        # last with ignoreNulls skips trailing NULL on p=1 (row order: 10 then None).
        assert last_out["1"] == 10
        assert last_out["2"] == 88

        # Explicit .alias clears _agg_name; recovery sees first_value/last_value (octo C7-Q-001).
        # partitions=1 still required for order-sensitive first/last values.
        first_alias = (
            frame.groupBy("g")
            .pivot("p", [1, 2])
            .agg(F.first("x").alias("head"))
            .to_arrow()
            .to_pylist()[0]
        )
        assert first_alias["1"] == 10 and first_alias["2"] == 99
        last_alias = (
            frame.groupBy("g")
            .pivot("p", [1, 2])
            .agg(F.last("x").alias("tail"))
            .to_arrow()
            .to_pylist()[0]
        )
        assert last_alias["1"] == 10 and last_alias["2"] == 88
        # Multi-agg: alias is the multi suffix; first_value recovery must not raise.
        multi_alias = (
            frame.groupBy("g")
            .pivot("p", [1])
            .agg(F.first("x").alias("head"), F.last("x").alias("tail"))
        )
        assert "1_head" in multi_alias.columns and "1_tail" in multi_alias.columns
        multi_row = multi_alias.to_arrow().to_pylist()[0]
        assert multi_row["1_head"] == 10 and multi_row["1_tail"] == 10
    finally:
        session.stop()


def test_pivot_repeated_refused(frame: object) -> None:
    """Second .pivot() is REPEATED_CLAUSE — not silent overwrite (octo C3-Q-001)."""
    from repark.errors import UnsupportedOperationException

    grouped = frame.groupBy("g").pivot("p", [1])
    with pytest.raises(UnsupportedOperationException, match="REPEATED_CLAUSE"):
        grouped.pivot("p", [2])


def test_pivot_bool_column_names(spark: ReparkSession) -> None:
    """Boolean pivot values name columns ``true``/``false`` (Spark Cast-to-string; C3-Q-004)."""
    frame = spark.createDataFrame([(True, 1), (False, 2), (True, 3)], ["p", "x"])
    cols = frame.groupBy().pivot("p", [True, False]).sum("x").columns
    assert cols == ["true", "false"]
    out = frame.groupBy().pivot("p", [True, False]).sum("x").to_arrow().to_pylist()[0]
    assert out["true"] == 4 and out["false"] == 2
    # Must not use Python str(True) spellings.
    assert "True" not in cols and "False" not in cols


def test_pivot_values_cast_to_pivot_type(spark: ReparkSession) -> None:
    """Values-list literals Cast to pivot column type before equality (octo C3-L-001)."""
    # String pivot column, integer values list — Spark matches via Cast.
    frame = spark.createDataFrame([("1", 10), ("2", 20), ("1", 5)], ["p", "x"])
    out = frame.groupBy().pivot("p", [1, 2]).sum("x").to_arrow().to_pylist()[0]
    assert out["1"] == 15 and out["2"] == 20


def test_pivot_nan_value_matches(spark: ReparkSession) -> None:
    """NaN pivot keys match via isnan (not IEEE == alone) — octo C3-L-003.

    SQL-sourced NaN (createDataFrame normalizes float NaN → null on some paths).
    """
    frame = spark.sql(
        "SELECT CAST('NaN' AS DOUBLE) AS p, CAST(10.0 AS DOUBLE) AS x "
        "UNION ALL SELECT CAST(1.0 AS DOUBLE), CAST(20.0 AS DOUBLE)"
    )
    out = frame.groupBy().pivot("p", [float("nan"), 1.0]).sum("x").to_arrow().to_pylist()[0]
    # Column name follows str(nan) → "nan"; value must aggregate the NaN-keyed row.
    assert out.get("nan") == 10.0
    assert out.get("1.0") == 20.0


def test_pivot_bigint_values_list_outside_int32(spark: ReparkSession) -> None:
    """BIGINT pivot keys outside int32 cast to ``long`` not ``int`` (octo C4-Q-001 / C4-L-002).

    ``frame.schema`` collapses logical long → IntegerType → cast(\"int\")/Int32, which drops
    keys like ``3_000_000_000``. Must use ``logical_schema_fields`` type_key (same as fillna).
    Mutation-proof: reverts fail equality / sum miss on the Arrow path.
    """
    big = 3_000_000_000  # > 2**31 - 1
    frame = spark.createDataFrame(
        [
            ("a", big, 10),
            ("a", 1, 20),
            ("b", big, 30),
        ],
        ["g", "p", "x"],
    )
    # Confirm pivot column is logical long (int64) — the bug only fires when schema maps long→int.
    type_keys = {name: key for name, key, _ in frame._inner.logical_schema_fields()}
    assert type_keys.get("p") == "long", type_keys
    schema_p = next(field for field in frame.schema.fields if field.name == "p")
    assert schema_p.dataType.simpleString() == "bigint"  # X2: Int64 → LongType

    out = frame.groupBy("g").pivot("p", [big, 1]).sum("x").orderBy("g").to_arrow().to_pylist()
    a_row = next(row for row in out if row["g"] == "a")
    b_row = next(row for row in out if row["g"] == "b")
    col = str(big)
    assert a_row[col] == 10 and a_row["1"] == 20
    assert b_row[col] == 30 and b_row["1"] is None

    # Inferred form must also match (same cast path for discovered values when re-compared
    # is not needed; pin values-list is the regression surface for lit cast width).
    inferred = frame.groupBy("g").pivot("p").sum("x").orderBy("g").to_arrow().to_pylist()
    listed = frame.groupBy("g").pivot("p", [1, big]).sum("x").orderBy("g").to_arrow().to_pylist()
    assert inferred == listed


def test_pivot_count_digit_named_measure(spark: ReparkSession) -> None:
    """``F.count(\"10\")`` counts non-null measure values — not row-count (octo C4-L-001).

    ``startswith(\"count(1\")`` wrongly treated ``count(10)`` as ``count(1)`` → lit(1) and
    counted every pivot-matching row, masking nulls on the digit-named column.
    """
    frame = spark.createDataFrame(
        [
            ("a", 1, 100),
            ("a", 1, None),  # null measure — must not count
            ("a", 2, 200),
        ],
        ["g", "p", "10"],
    )
    out = frame.groupBy("g").pivot("p", [1, 2]).agg(F.count("10")).to_arrow().to_pylist()[0]
    # Non-null counts: p=1 → 1 (not 2 rows); p=2 → 1.
    assert out["1"] == 1, out
    assert out["2"] == 1, out
    # Bare row-count still works (control: would be 2 for p=1).
    row_count = frame.groupBy("g").pivot("p", [1, 2]).count().to_arrow().to_pylist()[0]
    assert row_count["1"] == 2 and row_count["2"] == 1


def test_pivot_count_column_named_one(spark: ReparkSession) -> None:
    """``F.count(\"1\")`` non-null-counts column ``\"1\"`` (octo C5-L-001 / C5-Q-001).

    Spark default name for ``count(\"*\")`` is also ``count(1)`` — name-only rebuild must not
    collapse measure column ``\"1\"`` into ``lit(1)``. Null measure under a pivot key must not
    be counted. Controls: bare ``.count()`` / ``F.count(\"*\")`` still count every row.
    """
    frame = spark.createDataFrame(
        [
            ("a", 1, 100),
            ("a", 1, None),  # null on measure col \"1\" — must not count
            ("a", 2, 200),
        ],
        ["g", "p", "1"],
    )
    out = frame.groupBy("g").pivot("p", [1, 2]).agg(F.count("1")).to_arrow().to_pylist()[0]
    assert out["1"] == 1, out  # not 2 (row count would over-count the null)
    assert out["2"] == 1, out

    # Same intent via F.col / .alias recovery path (single-agg → value col names only).
    via_col = (
        frame.groupBy("g")
        .pivot("p", [1, 2])
        .agg(F.count(F.col("1")).alias("n"))
        .to_arrow()
        .to_pylist()[0]
    )
    assert via_col["1"] == 1 and via_col["2"] == 1, via_col
    # Multi-agg surfaces the alias suffix and still non-null-counts column \"1\".
    multi = (
        frame.groupBy("g")
        .pivot("p", [1, 2])
        .agg(F.count(F.col("1")).alias("n"), F.sum("1").alias("s"))
        .to_arrow()
        .to_pylist()[0]
    )
    assert multi["1_n"] == 1 and multi["2_n"] == 1, multi
    assert multi["1_s"] == 100 and multi["2_s"] == 200, multi

    row_count = frame.groupBy("g").pivot("p", [1, 2]).count().to_arrow().to_pylist()[0]
    assert row_count["1"] == 2 and row_count["2"] == 1
    star = frame.groupBy("g").pivot("p", [1, 2]).agg(F.count("*")).to_arrow().to_pylist()[0]
    assert star["1"] == 2 and star["2"] == 1
    lit_count = (
        frame.groupBy("g").pivot("p", [1, 2]).agg(F.count(F.lit(1))).to_arrow().to_pylist()[0]
    )
    assert lit_count["1"] == 2 and lit_count["2"] == 1


def test_pivot_count_cast_abs_refused_not_row_count(spark: ReparkSession) -> None:
    """``count(cast/abs)`` must not silently row-count null measures (octo C6-Q-001).

    The former ``Ident(...)`` allowlist treated CAST/abs/coalesce as ``lit(1)`` under the
    pivot condition, so a null measure row was counted. ``F.sum(cast)`` already refused —
    count must match. Mutation-proof: restoring the broad regex would either over-count
    (if the raise is removed) or this raise pin fails if the path is deleted.
    """
    frame = spark.createDataFrame(
        [
            ("a", 1, 100),
            ("a", 1, None),  # null measure — row-count would be 2; non-null count is 1
            ("a", 2, 200),
        ],
        ["g", "p", "x"],
    )
    with pytest.raises(AnalysisException, match="simple column-name"):
        frame.groupBy("g").pivot("p", [1, 2]).agg(F.count(F.col("x").cast("double"))).collect()
    with pytest.raises(AnalysisException, match="simple column-name"):
        frame.groupBy("g").pivot("p", [1, 2]).agg(F.count(F.abs(F.col("x")))).collect()
    with pytest.raises(AnalysisException, match="simple column-name"):
        frame.groupBy("g").pivot("p", [1, 2]).agg(
            F.count(F.coalesce(F.col("x"), F.lit(0)))
        ).collect()
    # Control: simple count still non-null-counts; bare count still row-counts.
    non_null = frame.groupBy("g").pivot("p", [1, 2]).agg(F.count("x")).to_arrow().to_pylist()[0]
    assert non_null["1"] == 1 and non_null["2"] == 1, non_null
    rows = frame.groupBy("g").pivot("p", [1, 2]).count().to_arrow().to_pylist()[0]
    assert rows["1"] == 2 and rows["2"] == 1, rows


def test_pivot_sum_lit_digit_named_measure_refused(spark: ReparkSession) -> None:
    """``F.sum(F.lit(1))`` must not bind measure column ``\"1\"`` (octo C6-L-001).

    Recovered name ``sum(1)`` matches ``F.sum(\"1\")``; only native ``sum(Int64(1))``
    marks the lit. Without disambiguation, pivot yields measure sums (100/200) instead of
    refusing the lit (or counting rows 2/1). Same class for avg/min/max/first/last.
    """
    frame = spark.createDataFrame(
        [
            ("a", 1, 100),
            ("a", 1, None),
            ("a", 2, 200),
        ],
        ["g", "p", "1"],
    )
    for builder in (F.sum, F.avg, F.min, F.max):
        with pytest.raises(AnalysisException, match="simple column-name"):
            frame.groupBy("g").pivot("p", [1, 2]).agg(builder(F.lit(1))).collect()
    with pytest.raises(AnalysisException, match="simple column-name"):
        frame.groupBy("g").pivot("p", [1, 2]).agg(F.first(F.lit(1))).collect()
    with pytest.raises(AnalysisException, match="simple column-name"):
        frame.groupBy("g").pivot("p", [1, 2]).agg(F.last(F.lit(1))).collect()
    # Control: real column sum still works; count(lit(1)) still row-counts.
    summed = frame.groupBy("g").pivot("p", [1, 2]).agg(F.sum("1")).to_arrow().to_pylist()[0]
    assert summed["1"] == 100 and summed["2"] == 200, summed
    lit_count = (
        frame.groupBy("g").pivot("p", [1, 2]).agg(F.count(F.lit(1))).to_arrow().to_pylist()[0]
    )
    assert lit_count["1"] == 2 and lit_count["2"] == 1, lit_count
