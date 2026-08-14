"""Group H — select/projection display naming vs live PySpark 4.1.2.

Non-aggregate projections must not leak DataFusion's ``t.x + Int64(1)`` text.
Names are applied at the ``DataFrame.select`` boundary from the facade
``_projection_name`` / ``_stable_name`` slots (cast-of-attribute keeps the child
name; compound cast uses ``CAST(...)``; explicit ``.alias`` always wins).
"""

from __future__ import annotations

import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812 — PySpark idiom: `import ...functions as F`


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("test-select-naming").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def frame(spark: ReparkSession) -> object:
    return spark.createDataFrame([(1, 2.0, "a", True)], schema=["x", "y", "s", "b"])


def test_projection_naming_matrix_matches_pyspark(frame: object) -> None:
    """Full recorded matrix: each label pins ``.columns`` against live PySpark 4.1.2."""
    df = frame
    expected = {
        "simple": ["x"],
        "x+1": ["(x + 1)"],
        "2*x": ["(x * 2)"],
        "x/2": ["(x / 2)"],
        "cast_double": ["x"],
        "cast_nested": ["CAST((x + 1) AS DOUBLE)"],
        "cast_into_binary": ["(CAST(x AS DOUBLE) + 1)"],
        "-x": ["negative(x)"],
        "x>0": ["(x > 0)"],
        "isNull": ["(x IS NULL)"],
        "when": ["CASE WHEN (x > 0) THEN 1 ELSE 0 END"],
        "coalesce": ["coalesce(x, 0)"],
        "concat": ["concat(s, z)"],
        "lit_int": ["1"],
        "lit_str": ["s"],
        "lit_float": ["2.0"],
        "abs": ["abs((x - 2))"],
        "nested_arith": ["((x + 1) * 2)"],
        "and": ["((x > 0) AND b)"],
        "or": ["((x > 0) OR b)"],
        "not": ["(NOT (x > 0))"],
        "alias_wins": ["z"],
        "(-x)+1": ["(negative(x) + 1)"],
        "x-y": ["(x - y)"],
        "eq": ["(x = 1)"],
        "neq": ["(NOT (x = 1))"],
        "cast_alias": ["y"],
        # Cheap S2 pins (octo r2 C1-Q-008): probe-clean vs live Spark 4.1.2.
        "le": ["(x <= 1)"],
        "ge": ["(x >= 1)"],
        "isNotNull": ["(x IS NOT NULL)"],
    }
    got = {
        "simple": df.select("x").columns,
        "x+1": df.select(df.x + 1).columns,
        "2*x": df.select(2 * df.x).columns,
        "x/2": df.select(df.x / 2).columns,
        "cast_double": df.select(df.x.cast("double")).columns,
        "cast_nested": df.select((df.x + 1).cast("double")).columns,
        "cast_into_binary": df.select(df.x.cast("double") + 1).columns,
        "-x": df.select(-df.x).columns,
        "x>0": df.select(df.x > 0).columns,
        "isNull": df.select(df.x.isNull()).columns,
        "when": df.select(F.when(df.x > 0, 1).otherwise(0)).columns,
        "coalesce": df.select(F.coalesce(df.x, F.lit(0))).columns,
        "concat": df.select(F.concat(df.s, F.lit("z"))).columns,
        "lit_int": df.select(F.lit(1)).columns,
        "lit_str": df.select(F.lit("s")).columns,
        "lit_float": df.select(F.lit(2.0)).columns,
        "abs": df.select(F.abs(df.x - 2)).columns,
        "nested_arith": df.select((df.x + 1) * 2).columns,
        "and": df.select((df.x > 0) & df.b).columns,
        "or": df.select((df.x > 0) | df.b).columns,
        "not": df.select(~(df.x > 0)).columns,
        "alias_wins": df.select((df.x + 1).alias("z")).columns,
        "(-x)+1": df.select((-df.x) + 1).columns,
        "x-y": df.select(df.x - df.y).columns,
        "eq": df.select(df.x == 1).columns,
        "neq": df.select(df.x != 1).columns,
        "cast_alias": df.select(df.x.alias("y").cast("double")).columns,
        "le": df.select(df.x <= 1).columns,
        "ge": df.select(df.x >= 1).columns,
        "isNotNull": df.select(df.x.isNotNull()).columns,
    }
    assert got == expected


def test_with_column_explicit_name_unaffected(frame: object) -> None:
    """``withColumn`` names the new column; source cols unchanged (not projection text)."""
    out = frame.withColumn("z", frame.x + 1)
    assert out.columns == ["x", "y", "s", "b", "z"]
    assert out.collect()[0]["z"] == 2


def test_date_function_projection_names(spark: ReparkSession) -> None:
    """Spot-check date functions applied plainly in select (live names)."""
    df = spark.createDataFrame([("2024-01-15",)], schema=["raw"]).withColumn(
        "d", F.col("raw").cast("date")
    )
    assert df.select(F.year("d")).columns == ["year(d)"]
    assert df.select(F.month("d")).columns == ["month(d)"]
    assert df.select(F.dayofmonth("d")).columns == ["dayofmonth(d)"]
    assert df.select(F.dayofyear("d")).columns == ["dayofyear(d)"]
    assert df.select(F.quarter("d")).columns == ["quarter(d)"]
    assert df.select(F.weekofyear("d")).columns == ["weekofyear(d)"]
    assert df.select(F.dayofweek("d")).columns == ["dayofweek(d)"]
    assert df.select(F.add_months("d", 1)).columns == ["add_months(d, 1)"]
    assert df.select(F.date_add("d", 1)).columns == ["date_add(d, 1)"]
    assert df.select(F.last_day("d")).columns == ["last_day(d)"]
    assert df.select(F.date_format("d", "yyyy-MM")).columns == ["date_format(d, yyyy-MM)"]
    assert df.select(F.trunc("d", "month")).columns == ["trunc(d, month)"]
    assert df.select(F.date_trunc("month", "d")).columns == ["date_trunc(month, d)"]


def test_projection_values_and_arrow_types_unchanged(frame: object) -> None:
    """Naming must not change values or Arrow types (collect + to_arrow)."""
    out = frame.select(
        frame.x + 1,
        frame.x.cast("double"),
        (-frame.x) + 1,
        F.when(frame.x > 0, 1).otherwise(0),
        F.coalesce(frame.x, F.lit(0)),
        frame.x % 2,
    )
    assert out.columns == [
        "(x + 1)",
        "x",
        "(negative(x) + 1)",
        "CASE WHEN (x > 0) THEN 1 ELSE 0 END",
        "coalesce(x, 0)",
        "(x % 2)",
    ]
    table = out.to_arrow()
    assert table.to_pydict() == {
        "(x + 1)": [2],
        "x": [1.0],
        "(negative(x) + 1)": [0],
        "CASE WHEN (x > 0) THEN 1 ELSE 0 END": [1],
        "coalesce(x, 0)": [1],
        "(x % 2)": [1],
    }
    # cast → float64; arithmetic on int stays int64-ish via engine
    assert str(table.schema.field("(x + 1)").type) in {"int64", "int32"}
    assert "float" in str(table.schema.field("x").type) or "double" in str(
        table.schema.field("x").type
    )


def test_mutation_drop_select_alias_application(
    frame: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mutation proof: if select stops applying for_select, compound names regress."""
    from repark.spark.column import Column
    from repark.spark.dataframe import DataFrame

    original = DataFrame.select

    def broken_select(self: DataFrame, *cols: Column | str) -> DataFrame:
        if len(cols) == 1 and isinstance(cols[0], str) and cols[0] == "*":
            return original(self, *cols)
        natives = [self._column_of(item)._inner for item in cols]
        return self._spawn(self._inner.select(natives))

    monkeypatch.setattr(DataFrame, "select", broken_select)
    leaked = frame.select(frame.x + 1).columns[0]  # type: ignore[attr-defined]
    # Positive engine-leak signal only (no catch-all inequality — Critic-1 Q-003).
    assert "Int64" in leaked or leaked.startswith("t.")


def test_mutation_cast_stable_name_loss(frame: object) -> None:
    """Mutation proof: cast-of-attribute without stable_name would emit CAST(...)."""
    # Construct a cast as if child were non-stable: force projection to CAST text.
    cast_col = frame.x.cast("double")
    assert cast_col._stable_name is True
    assert frame.select(cast_col).columns == ["x"]
    # If stable_name were cleared, projection_name would be the CAST form.
    unstable = type(cast_col)(
        cast_col._inner,
        spark_display=cast_col._spark_display,
        projection_name=cast_col._spark_display,
        stable_name=False,
    )
    assert frame.select(unstable).columns == ["CAST(x AS DOUBLE)"]


def test_agg_names_still_use_spark_display_not_projection_cast(frame: object) -> None:
    """Aggregate embed still uses CAST(...) for cast args (Group F), not bare child name."""
    out = frame.groupBy().agg(F.sum(frame.x.cast("double")))  # type: ignore[attr-defined]
    assert out.columns == ["sum(CAST(x AS DOUBLE))"]


def test_string_lit_unquoted_in_agg_and_select(frame: object) -> None:
    """String lit display matches Spark (no quotes) in select and agg embed."""
    assert frame.select(F.lit("z")).columns == ["z"]
    assert frame.groupBy().agg(F.first(F.lit("z"))).columns == ["first(z)"]
    assert frame.select(F.concat(frame.s, F.lit("z"))).columns == ["concat(s, z)"]


def test_select_duplicate_projection_names_multi_name_map(frame: object) -> None:
    """H2: Spark-legal duplicate *display* names via unique engine aliases + identity map.

    Live PySpark 4.1.2 allows ``select(x, x.cast("double"))`` → two ``x`` columns and
    ``select(lit("s"), s)`` → two ``s``. H1 covered origin-qualified dups; H2 extends the
    multi-name map to non-origin facade projections (cast / lit display clash).
    """
    from repark.errors import AnalysisException

    cast_out = frame.select(frame.x, frame.x.cast("double"))  # type: ignore[attr-defined]
    assert cast_out.columns == ["x", "x"]
    # Values: first int, second double — positional collect (display names collide).
    assert cast_out.collect()[0][0] == 1
    assert cast_out.collect()[0][1] == 1.0
    with pytest.raises(AnalysisException, match=r"AMBIGUOUS_REFERENCE"):
        _ = cast_out["x"]

    lit_out = frame.select(F.lit("s"), frame.s)  # type: ignore[attr-defined]
    assert lit_out.columns == ["s", "s"]
    row = lit_out.collect()[0]
    assert row[0] == "s"
    assert row[1] == "a"

    # Explicit .alias still wins (Spark and repark).
    out = frame.select(frame.x, frame.x.cast("double").alias("x_double"))  # type: ignore[attr-defined]
    assert out.columns == ["x", "x_double"]


def test_bare_select_keeps_requested_spelling(spark: ReparkSession) -> None:
    """Bare ``select("X")`` / ``F.col("X")`` keep requested spelling (live Spark 4.1.2).

    Under ``spark.sql.caseSensitive=false``, Spark resolves case-insensitively but the
    output name is the **requested** spelling (``X``), not the schema field ``x``.
    Getitem already matched; ``for_select`` must alias bare refs too (octo C3-L-001).
    ``select("X", "x")`` is two distinct names (both values from the same column) —
    must not collapse into a DataFusion unique-name engine error (C3-L-002).
    """
    df = spark.createDataFrame([(1,)], schema=["x"])
    assert df.select("X").columns == ["X"]
    assert df.select(F.col("X")).columns == ["X"]
    assert df.select(df["X"]).columns == ["X"]
    table = df.select("X", "x").to_arrow()
    assert table.column_names == ["X", "x"]
    assert table.to_pydict() == {"X": [1], "x": [1]}


def test_requested_spelling_projection_is_reselectable(spark: ReparkSession) -> None:
    """After ``select("X")``, the frame remains usable by name (live Spark 4.1.2).

    r1 aliased bare refs to the requested spelling so first-hop ``.columns`` matched Spark,
    but DataFusion folds unquoted idents to lowercase — so a second ``select("X")`` /
    ``filter`` looked for ``t.x`` against field ``"X"`` and failed (octo r3 C3-L-007).
    Quoted schema bind + select rebind of bare ``F.col`` must keep the chain green.
    """
    df = spark.createDataFrame([(1,)], schema=["x"])
    for first in (
        df.select("X"),
        df.select(F.col("X")),
        df.select(df["X"]),
    ):
        assert first.columns == ["X"]
        assert first.collect()[0][0] == 1
        second = first.select("X")
        assert second.columns == ["X"]
        assert second.to_arrow().to_pydict() == {"X": [1]}
        assert first.select(F.col("X")).to_arrow().to_pydict() == {"X": [1]}
        assert first.select(first["X"]).to_arrow().to_pydict() == {"X": [1]}
        # CI reselect with opposite spelling (Spark keeps the *requested* output name).
        assert first.select("x").columns == ["x"]
        assert first.select("x").to_arrow().to_pydict() == {"x": [1]}
    # Mixed-case user alias (pre-existing fold class; same bind fixes DataFrame paths).
    total = df.select(df.x.alias("Total"))
    assert total.columns == ["Total"]
    assert total.select("Total").to_arrow().to_pydict() == {"Total": [1]}
    assert total.select(total["Total"]).to_arrow().to_pydict() == {"Total": [1]}


def test_select_star_expands_among_other_projections(frame: object) -> None:
    """``select("*", expr)`` expands the star (live PySpark 4.1.2)."""
    out = frame.select("*", frame.x + 1)  # type: ignore[attr-defined]
    assert out.columns == ["x", "y", "s", "b", "(x + 1)"]
    assert out.to_arrow().to_pydict()["(x + 1)"] == [2]


def test_getitem_ci_composition_is_named_expression_not_alias(
    spark: ReparkSession,
) -> None:
    """CI ``df["X"]`` is NamedExpression ``X``, not ``x AS X`` (live Spark 4.1.2).

    G1 used ``col(canonical).alias(item)`` for bare select spelling; that polluted
    ``spark_display`` so compounds leaked Alias text (octo r2 C3-L-005). H2 wrap-display
    also collapses true user ``.alias("z")`` inside outer expressions to the projection
    name (``(z + 1)`` / ``round(v, 2)``); aggregate *arguments* still embed ``x AS y``.
    """
    df = spark.createDataFrame([(1,)], schema=["x"])
    assert repr(df["X"]) == "Column<'X'>"
    assert repr(F.col("X")) == "Column<'X'>"
    assert df.select(df["X"] + 1).columns == ["(X + 1)"]
    assert df.select(F.col("X") + 1).columns == ["(X + 1)"]
    assert df.select(F.abs(df["X"])).columns == ["abs(X)"]
    assert df.select(F.coalesce(df["X"], F.lit(0))).columns == ["coalesce(X, 0)"]
    assert df.select(F.when(df["X"] > 0, 1).otherwise(0)).columns == [
        "CASE WHEN (X > 0) THEN 1 ELSE 0 END"
    ]
    assert df.groupBy().agg(F.sum(df["X"])).columns == ["sum(X)"]  # type: ignore[attr-defined]
    # Values still resolve from schema column ``x``.
    assert df.select(df["X"] + 1).to_arrow().to_pydict() == {"(X + 1)": [2]}
    # H2: wrapped user alias collapses to the projection name (not ``x AS z`` leak).
    assert df.select(df.x.alias("z") + 1).columns == ["(z + 1)"]
    assert df.groupBy().agg(F.sum(df.x.alias("y"))).columns == ["sum(x AS y)"]  # type: ignore[attr-defined]


def test_expr_and_current_timestamp_projection_names(spark: ReparkSession) -> None:
    """``F.expr`` infix paren + ``current_timestamp()`` display (live Spark 4.1.2)."""
    df = spark.createDataFrame([(1,)], schema=["x"])
    assert df.select(F.expr("1 + 1")).columns == ["(1 + 1)"]
    assert df.select(F.current_timestamp()).columns == ["current_timestamp()"]


def test_requested_spelling_residual_name_sinks(spark: ReparkSession) -> None:
    """After ``select("X")``, filter/fillna/rename/dd/string-agg still work (live Spark 4.1.2).

    r3 fixed select reselect via quoted bind; residual sinks still folded unquoted idents
    (octo r4 C3-L-008). ``withColumnRenamed`` was a **silent no-op** on case-preserved fields.
    """
    df = spark.createDataFrame([(1, None), (1, 2.0)], schema=["x", "y"]).select("X", "y")
    assert df.columns == ["X", "y"]

    # filter / where SQL (CI form + exact)
    assert df.filter("X > 0").to_arrow().to_pydict()["X"] == [1, 1]
    assert df.filter("x > 0").count() == 2
    assert df.where("X > 0 AND y IS NULL").to_arrow().to_pydict() == {"X": [1], "y": [None]}
    assert df.filter(df["X"] > 0).count() == 2

    # fillna / na.drop
    filled = df.fillna(0.0)
    assert filled.to_arrow().to_pydict() == {"X": [1, 1], "y": [0.0, 2.0]}
    assert df.na.drop().to_arrow().to_pydict() == {"X": [1], "y": [2.0]}

    # dropDuplicates subset (schema column order preserved)
    deduped = df.dropDuplicates(["X"])
    assert deduped.columns == ["X", "y"]
    assert deduped.count() == 1
    assert deduped.collect()[0]["X"] == 1

    # withColumnRenamed (was silent no-op); CI old name too
    renamed = df.withColumnRenamed("X", "Y")
    assert renamed.columns == ["Y", "y"]
    assert renamed.to_arrow().to_pydict()["Y"] == [1, 1]
    assert df.withColumnRenamed("x", "Z").columns == ["Z", "y"]
    assert df.withColumnsRenamed({"X": "Y"}).columns == ["Y", "y"]

    # string aggregates + shortcuts + dict form
    only_x = spark.createDataFrame([(1,), (2,)], schema=["x"]).select("X")
    assert only_x.groupBy().agg(F.sum("X")).to_arrow().to_pydict() == {"sum(X)": [3]}
    assert only_x.groupBy().sum("X").to_arrow().to_pydict() == {"sum(X)": [3]}
    assert only_x.groupBy().agg({"X": "sum"}).to_arrow().to_pydict() == {"sum(X)": [3]}
    grouped = only_x.groupBy("X").count().to_arrow().to_pydict()
    assert sorted(grouped["X"]) == [1, 2]
    assert grouped["count"] == [1, 1]
