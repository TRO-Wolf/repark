"""Facade polish unit — aggregate display naming + ``spark.read`` expansion.

Goldens for **column names** and **values** were recorded from live PySpark 4.1.2
(``JAVA_HOME=/usr/lib/jvm/zulu-17-amd64``, ANSI on, ``SPARK_LOCAL_IP=127.0.0.1``) via the
session matrix in ``{SCRATCH}/b-oracle-naming-matrix.txt`` / the Actor report. Routine tests
are JVM-free and pin those recorded names; they do not re-invoke Spark.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812 — PySpark idiom: `import ...functions as F`
from repark.errors import AnalysisException
from repark.session import DataFrameReader


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-facade-polish").getOrCreate()
    yield session
    session.stop()


def _source(spark: ReparkSession) -> object:
    # createDataFrame (not VALUES SQL) so engine typing matches the oracle recording path.
    return spark.createDataFrame([(1, 10), (1, 20), (2, 30)], ["g", "x"])


# ---- Scope A: aggregate display naming (live-recorded PySpark 4.1.2 matrix) ---------------------


def test_agg_naming_matrix_matches_live_pyspark(spark: ReparkSession) -> None:
    """Full name matrix recorded live: simple / compound / cast / nested / alias / dict / shortcut.

    Live recipe (verbatim intent)::

        df = spark.createDataFrame([(1,10),(1,20),(2,30)], "g INT, x INT")
        df.groupBy("g").agg(F.sum(F.col("x") + 1)).columns  # → ['g', 'sum((x + 1))']
        …
    """
    df = _source(spark)
    assert df.groupBy("g").agg(F.sum("x")).columns == ["g", "sum(x)"]
    assert df.groupBy("g").agg(F.sum(F.col("x"))).columns == ["g", "sum(x)"]
    assert df.groupBy("g").agg(F.sum(F.col("x") + 1)).columns == ["g", "sum((x + 1))"]
    assert df.groupBy("g").agg(F.sum(F.col("x") * 2)).columns == ["g", "sum((x * 2))"]
    assert df.groupBy("g").agg(F.sum(F.col("x").cast("double"))).columns == [
        "g",
        "sum(CAST(x AS DOUBLE))",
    ]
    assert df.groupBy("g").agg(F.sum(F.abs(F.col("x")))).columns == ["g", "sum(abs(x))"]
    assert df.groupBy("g").agg(F.sum(F.abs(F.col("x") + 1))).columns == [
        "g",
        "sum(abs((x + 1)))",
    ]
    assert df.groupBy("g").agg(F.sum(F.col("x").alias("y"))).columns == ["g", "sum(x AS y)"]
    assert df.groupBy("g").agg(F.sum("x").alias("total")).columns == ["g", "total"]
    assert df.groupBy("g").agg({"x": "sum"}).columns == ["g", "sum(x)"]
    assert df.groupBy("g").sum("x").columns == ["g", "sum(x)"]
    assert df.groupBy("g").agg(F.count("*")).columns == ["g", "count(1)"]
    assert df.groupBy("g").agg(F.min("x"), F.max("x")).columns == ["g", "min(x)", "max(x)"]
    # Reflected operators (2026-07-21 review recording, live 4.1.2): PySpark commutes
    # reflected + and * — but NOT reflected - and / — and keeps the double point on literals.
    assert df.groupBy("g").agg(F.sum(2 + F.col("x"))).columns == ["g", "sum((x + 2))"]
    assert df.groupBy("g").agg(F.sum(2 * F.col("x"))).columns == ["g", "sum((x * 2))"]
    assert df.groupBy("g").agg(F.sum(100 - F.col("x"))).columns == ["g", "sum((100 - x))"]
    assert df.groupBy("g").agg(F.sum(100 / F.col("x"))).columns == ["g", "sum((100 / x))"]
    assert df.groupBy("g").agg(F.sum(F.col("x") + 2.0)).columns == ["g", "sum((x + 2.0))"]


def test_agg_compound_and_nested_values_match_oracle(spark: ReparkSession) -> None:
    """Value pins for compound and nested abs (recorded live: groups 1→32/30, 2→31/30)."""
    df = _source(spark)
    compound = df.groupBy("g").agg(F.sum(F.col("x") + 1).alias("s")).orderBy("g").to_arrow()
    expected_compound = pa.table(
        {"g": [1, 2], "s": [32, 31]},
        schema=pa.schema(
            [
                pa.field("g", pa.int64(), nullable=True),
                pa.field("s", pa.int64(), nullable=True),
            ]
        ),
    )
    assert compound.column_names == expected_compound.column_names
    assert compound.to_pydict() == expected_compound.to_pydict()

    nested = df.groupBy("g").agg(F.sum(F.abs(F.col("x"))).alias("s")).orderBy("g").to_arrow()
    assert nested.to_pydict() == {"g": [1, 2], "s": [30, 30]}


def test_abs_values_include_negatives_null_and_zero(spark: ReparkSession) -> None:
    """Octo C1-Q-001 / C1-L-003: abs true-branch must run.

    Positives-only fixtures stay green if abs is the identity — so pin negatives too.
    """
    df = spark.createDataFrame(
        [(1, -10), (1, 5), (1, None), (2, 0), (2, -3)],
        ["g", "x"],
    )
    # sum(abs(x)): g=1 → 10+5 = 15 (NULL skipped); g=2 → 0+3 = 3. Identity abs → -5 and -3.
    got = (
        df.groupBy("g").agg(F.sum(F.abs(F.col("x"))).alias("s")).orderBy("g").to_arrow().to_pydict()
    )
    assert got == {"g": [1, 2], "s": [15, 3]}
    assert df.groupBy("g").agg(F.sum(F.abs(F.col("x")))).columns == ["g", "sum(abs(x))"]


def test_user_alias_always_wins_over_default_agg_name(spark: ReparkSession) -> None:
    df = _source(spark)
    assert df.groupBy("g").agg(F.sum(F.col("x") + 1).alias("total")).columns == ["g", "total"]


def test_agg_comparison_and_logical_display_names_match_live_pyspark(
    spark: ReparkSession,
) -> None:
    """CCC Q-001 fix: comparison/logical ops track ``_spark_display`` (live PySpark 4.1.2).

    Recorded names::

        sum(CAST((x > 0) AS INT))
        sum(CAST((x = 10) AS INT))
        sum(CAST((NOT (x = 10)) AS INT))
        sum(CAST(((x > 0) AND (x < 20)) AS INT))
        sum(CAST((NOT (x > 0)) AS INT))
        sum(CAST((x IS NULL) AS INT))
    """
    df = _source(spark)
    assert df.groupBy("g").agg(F.sum((F.col("x") > 0).cast("int"))).columns == [
        "g",
        "sum(CAST((x > 0) AS INT))",
    ]
    assert df.groupBy("g").agg(F.sum((F.col("x") == 10).cast("int"))).columns == [
        "g",
        "sum(CAST((x = 10) AS INT))",
    ]
    assert df.groupBy("g").agg(F.sum((F.col("x") != 10).cast("int"))).columns == [
        "g",
        "sum(CAST((NOT (x = 10)) AS INT))",
    ]
    assert df.groupBy("g").agg(
        F.sum(((F.col("x") > 0) & (F.col("x") < 20)).cast("int"))
    ).columns == ["g", "sum(CAST(((x > 0) AND (x < 20)) AS INT))"]
    assert df.groupBy("g").agg(F.sum((~(F.col("x") > 0)).cast("int"))).columns == [
        "g",
        "sum(CAST((NOT (x > 0)) AS INT))",
    ]
    assert df.groupBy("g").agg(F.sum(F.col("x").isNull().cast("int"))).columns == [
        "g",
        "sum(CAST((x IS NULL) AS INT))",
    ]
    # Octo C1-Q-003: OR and IS NOT NULL display tracking.
    assert df.groupBy("g").agg(
        F.sum(((F.col("x") > 0) | (F.col("x") < 0)).cast("int"))
    ).columns == ["g", "sum(CAST(((x > 0) OR (x < 0)) AS INT))"]
    assert df.groupBy("g").agg(F.sum(F.col("x").isNotNull().cast("int"))).columns == [
        "g",
        "sum(CAST((x IS NOT NULL) AS INT))",
    ]
    # Octo C1-L-004: coalesce / when track spark_display (no native Int64 leak).
    assert df.groupBy("g").agg(F.sum(F.coalesce(F.col("x"), F.lit(1)))).columns == [
        "g",
        "sum(coalesce(x, 1))",
    ]
    assert df.groupBy("g").agg(F.sum(F.when(F.col("x") > 0, F.col("x")).otherwise(0))).columns == [
        "g",
        "sum(CASE WHEN (x > 0) THEN x ELSE 0 END)",
    ]
    # Multi-arm CASE display (octo C2-Q-004).
    assert df.groupBy("g").agg(
        F.sum(F.when(F.col("x") > 20, 1).when(F.col("x") > 0, 2).otherwise(0))
    ).columns == [
        "g",
        "sum(CASE WHEN (x > 20) THEN 1 WHEN (x > 0) THEN 2 ELSE 0 END)",
    ]
    # Values for the cast-bool sum pin (group g=1: 10,20 both >0 → 2; g=2: 30 → 1).
    vals = (
        df.groupBy("g")
        .agg(F.sum((F.col("x") > 0).cast("int")).alias("s"))
        .orderBy("g")
        .to_arrow()
        .to_pydict()
    )
    assert vals == {"g": [1, 2], "s": [2, 1]}


# ---- Scope B: spark.read expansion -------------------------------------------------------------


def test_read_returns_fresh_dataframe_reader(spark: ReparkSession) -> None:
    r1 = spark.read
    r2 = spark.read
    assert isinstance(r1, DataFrameReader)
    assert r1 is not r2
    assert r1.format("parquet") is r1


def test_read_parquet_and_format_load_parquet_equivalent(
    spark: ReparkSession, tmp_path: Path
) -> None:
    import pyarrow.parquet as pq

    path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"id": [1, 2], "name": ["a", "b"]}), path)
    via_parquet = spark.read.parquet(str(path)).orderBy("id").to_arrow().to_pydict()
    via_format = spark.read.format("parquet").load(str(path)).orderBy("id").to_arrow().to_pydict()
    assert via_parquet == via_format
    assert via_parquet["id"] == [1, 2]


def test_read_table_matches_session_table(spark: ReparkSession, tmp_path: Path) -> None:
    spark.register_memory_catalog("glue_catalog", tmp_path)
    spark.sql("CREATE NAMESPACE glue_catalog.db")
    spark.sql("CREATE TABLE glue_catalog.db.t USING iceberg AS SELECT 1 AS id, 'a' AS name")
    via_read = spark.read.table("glue_catalog.db.t").orderBy("id").to_arrow().to_pydict()
    via_session = spark.table("glue_catalog.db.t").orderBy("id").to_arrow().to_pydict()
    via_format = (
        spark.read.format("iceberg").load("glue_catalog.db.t").orderBy("id").to_arrow().to_pydict()
    )
    assert via_read == via_session == via_format
    assert via_read == {"id": [1], "name": ["a"]}


def test_read_unknown_format_raises_analysis_exception(spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException, match=r"DATA_SOURCE_NOT_FOUND|no_such_fmt"):
        spark.read.format("no_such_fmt").load("/tmp")


def test_read_option_unknown_keys_tolerated(spark: ReparkSession, tmp_path: Path) -> None:
    import pyarrow.parquet as pq

    path = tmp_path / "opt.parquet"
    pq.write_table(pa.table({"id": [1]}), path)
    # Unknown option must not fail the chain (live PySpark behavior).
    result = (
        spark.read.option("nonsense_key", "v")
        .options(another="x")
        .format("parquet")
        .load(str(path))
        .to_arrow()
        .to_pydict()
    )
    assert result == {"id": [1]}


def test_read_option_path_applied_when_load_path_omitted(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """Octo C1-SAF-001: option('path') is honored when load() has no path arg."""
    import pyarrow.parquet as pq

    path = tmp_path / "via_opt.parquet"
    pq.write_table(pa.table({"id": [7]}), path)
    result = spark.read.format("parquet").option("path", str(path)).load().to_arrow().to_pydict()
    assert result == {"id": [7]}


def test_read_semantic_option_unsupported_fails_loud(spark: ReparkSession, tmp_path: Path) -> None:
    """Octo C1-SAF-001 / C1-L-002: pathGlobFilter must not silently widen the read set."""
    import pyarrow.parquet as pq

    path = tmp_path / "sem.parquet"
    pq.write_table(pa.table({"id": [1]}), path)
    with pytest.raises(AnalysisException, match=r"pathGlobFilter|not supported"):
        spark.read.format("parquet").option("pathGlobFilter", "*.ok.parquet").load(str(path))


def test_read_table_rejects_sql_fragments(spark: ReparkSession) -> None:
    """Octo C1-SEC-001 / C1-L-001: table() is identifier-only, not a FROM-clause sink."""
    hostile = "t UNION ALL SELECT 1"
    with pytest.raises(AnalysisException, match=r"invalid table identifier|SQL fragments"):
        spark.table(hostile)
    with pytest.raises(AnalysisException, match=r"invalid table identifier|SQL fragments"):
        spark.read.table("a JOIN b ON true")
    with pytest.raises(AnalysisException, match=r"invalid table identifier|SQL fragments"):
        spark.read.format("iceberg").load("(SELECT 1) AS s")


def test_sql_table_ref_accepts_quoted_segments_with_dots() -> None:
    """Octo C2-L-001 / C2-Q-002: quote-aware multipart parsing (dots inside quotes)."""
    from repark.session import _sql_table_ref

    assert _sql_table_ref('catalog."db.with.dot".t') == '"catalog"."db.with.dot"."t"'
    assert _sql_table_ref('"a.b"') == '"a.b"'
    assert _sql_table_ref("cat.db.`my-table`") == '"cat"."db"."my-table"'
    assert _sql_table_ref('cat.db."order"') == '"cat"."db"."order"'


def test_read_semantic_option_rejected_on_parquet_and_iceberg_snapshot(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """Octo C2-SAF-001/002: semantic gate on parquet(); I1 time-travel options no longer denylisted.

    ``snapshot-id`` on format('iceberg') is supported (I1) — unknown table/snapshot still fails
    analysis (not the old denylist message). Incremental ``start-snapshot-id`` stays loud.
    """
    import pyarrow.parquet as pq

    path = tmp_path / "sem2.parquet"
    pq.write_table(pa.table({"id": [1]}), path)
    with pytest.raises(AnalysisException, match=r"pathGlobFilter|not supported"):
        spark.read.option("pathGlobFilter", "*.ok.parquet").parquet(str(path))
    # snapshot-id is no longer denylisted; missing catalog/table fails analysis (not denylist).
    with pytest.raises(AnalysisException, match=r"catalog|not registered|table|snapshot"):
        spark.read.format("iceberg").option("snapshot-id", "1").load("glue_catalog.db.t")
    with pytest.raises(AnalysisException, match=r"start-snapshot-id|incremental"):
        spark.read.format("iceberg").option("start-snapshot-id", "1").load("glue_catalog.db.t")


def test_read_option_path_case_insensitive_last_wins(spark: ReparkSession, tmp_path: Path) -> None:
    """Octo C2-L-002: path/PATH last-write-wins (Spark case-insensitive map)."""
    import pyarrow.parquet as pq

    path_a = tmp_path / "a.parquet"
    path_b = tmp_path / "b.parquet"
    pq.write_table(pa.table({"id": [1]}), path_a)
    pq.write_table(pa.table({"id": [2]}), path_b)
    result = (
        spark.read.format("parquet")
        .option("path", str(path_a))
        .option("PATH", str(path_b))
        .load()
        .to_arrow()
        .to_pydict()
    )
    assert result == {"id": [2]}


def test_when_after_otherwise_raises(spark: ReparkSession) -> None:
    """Octo C2-L-003: cannot re-chain .when after .otherwise (would drop ELSE)."""
    closed = F.when(F.col("x") > 0, 1).otherwise(0)
    with pytest.raises(TypeError, match="otherwise"):
        closed.when(F.col("x") == 0, 2)


def test_coalesce_requires_at_least_one_argument() -> None:
    """Octo C2-L-005: empty coalesce matches Spark fail-loud."""
    with pytest.raises(AnalysisException, match="coalesce"):
        F.coalesce()


def test_concat_requires_at_least_one_argument() -> None:
    """Octo C3-L-002: empty concat matches coalesce fail-loud."""
    with pytest.raises(AnalysisException, match="concat"):
        F.concat()


def test_multi_arm_case_values(spark: ReparkSession) -> None:
    """Octo C3-L-001: multi-arm when values (not only display)."""
    df = spark.createDataFrame([(1, -1), (1, 0), (1, 5), (1, 25)], ["g", "x"])
    got = (
        df.groupBy("g")
        .agg(F.sum(F.when(F.col("x") > 20, 1).when(F.col("x") > 0, 2).otherwise(0)).alias("s"))
        .to_arrow()
        .to_pydict()
    )
    # arms: 25→1, 5→2, 0→0, -1→0 → sum = 3
    assert got == {"g": [1], "s": [3]}


@pytest.mark.parametrize(
    "key",
    [
        "datetimeRebaseMode",
        "int96RebaseModeInRead",
        "start-snapshot-id",
        "end-snapshot-id",
    ],
)
def test_denylist_semantic_keys_fail_loud(spark: ReparkSession, key: str) -> None:
    """Octo C3-Q-001: residual denylist pinned (I1 removed the four time-travel options)."""
    with pytest.raises(AnalysisException, match=r"not supported|incremental"):
        spark.read.format("parquet").option(key, "x").load("/tmp/does_not_matter.parquet")


@pytest.mark.parametrize("key", ["branch", "tag", "as-of-timestamp", "snapshot-id"])
def test_time_travel_options_rejected_on_parquet(spark: ReparkSession, key: str) -> None:
    """I1: time-travel options are Iceberg-only — parquet path fails loud naming Iceberg."""
    with pytest.raises(AnalysisException, match=r"iceberg|time travel"):
        spark.read.format("parquet").option(key, "1").load("/tmp/does_not_matter.parquet")


def test_read_load_missing_format_and_path_fail(spark: ReparkSession) -> None:
    """Octo C1-Q-007: empty format / missing parquet path fail with AnalysisException."""
    with pytest.raises(AnalysisException, match=r"DATA_SOURCE_NOT_FOUND|empty"):
        spark.read.load("/tmp/x.parquet")
    with pytest.raises(AnalysisException, match="path"):
        spark.read.format("parquet").load()


def test_read_format_case_insensitive_and_load_arg_beats_option_path(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """Octo C7/C5: format case fold; option(path)+load(arg) uses the load argument."""
    import pyarrow.parquet as pq

    path_a = tmp_path / "case_a.parquet"
    path_b = tmp_path / "case_b.parquet"
    pq.write_table(pa.table({"id": [1]}), path_a)
    pq.write_table(pa.table({"id": [2]}), path_b)
    via_case = spark.read.format("PARQUET").load(str(path_b)).to_arrow().to_pydict()
    assert via_case == {"id": [2]}
    # load(path) wins over option("path") — Spark load(path) ≡ option then load.
    via_arg = (
        spark.read.format("parquet")
        .option("path", str(path_a))
        .load(str(path_b))
        .to_arrow()
        .to_pydict()
    )
    assert via_arg == {"id": [2]}


def test_read_iceberg_option_path_load(spark: ReparkSession, tmp_path: Path) -> None:
    """Octo C5-B-001: format(iceberg).option(path, id).load() without load arg."""
    spark.register_memory_catalog("glue_catalog", tmp_path)
    spark.sql("CREATE NAMESPACE glue_catalog.db")
    spark.sql("CREATE TABLE glue_catalog.db.topt USING iceberg AS SELECT 9 AS id")
    got = (
        spark.read.format("iceberg")
        .option("path", "glue_catalog.db.topt")
        .load()
        .to_arrow()
        .to_pydict()
    )
    assert got == {"id": [9]}


def test_read_schema_stores_for_csv_json(spark: ReparkSession, tmp_path: Path) -> None:
    """R1: DataFrameReader.schema chains and applies on csv/json (was C1-Q-007 unsupported)."""
    from repark.types import IntegerType, StructField, StructType

    path = tmp_path / "schema.csv"
    path.write_text("1\n2\n", encoding="utf-8")
    schema = StructType([StructField("id", IntegerType(), True)])
    reader = spark.read.schema(schema)
    assert reader is not None
    rows = reader.csv(str(path), header=False).to_arrow().to_pylist()
    assert rows == [{"id": 1}, {"id": 2}]
