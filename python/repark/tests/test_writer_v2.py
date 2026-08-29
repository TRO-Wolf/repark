"""Facade tests for DataFrameWriterV2 (writeTo), path parquet, sortWithinPartitions,
partition transforms, and F.weekday.

Routes only over existing CTAS / CREATE OR REPLACE / INSERT / COPY TO paths — no new commit
machinery. Live-PySpark oracle notes are embedded in the test docstrings.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812 — PySpark idiom
from repark import types as T  # noqa: N812 — PySpark idiom
from repark.errors import AnalysisException, UnsupportedOperationException

CATALOG = "glue_catalog"
NS = "writer_v2_ns"
TABLE = f"{CATALOG}.{NS}.tbl"


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """A session with an in-memory Iceberg catalog + namespace (local, AWS-free)."""
    session = ReparkSession.builder.appName("pytest-writer-v2").getOrCreate()
    session.register_memory_catalog(CATALOG, tmp_path)
    session.sql(f"CREATE NAMESPACE {CATALOG}.{NS}")
    return session


def _read(spark: ReparkSession, table: str = TABLE) -> list[dict[str, object]]:
    return spark.sql(f"SELECT * FROM {table} ORDER BY 1").to_arrow().to_pylist()


def _source(spark: ReparkSession, rows: str, cols: str = "id, name") -> object:
    return spark.sql(f"SELECT * FROM (VALUES {rows}) AS t({cols})")


# F.weekday (Monday=0)
def test_weekday_monday_is_zero(spark: ReparkSession) -> None:
    """weekday Monday=0 / Sunday=6 (live PySpark 4.1.2)."""
    # 2024-01-08 = Monday, 2024-01-07 = Sunday, 2024-03-15 = Friday.
    # createDataFrame does not accept Python dates yet — string + cast (spine pattern).
    frame = spark.createDataFrame(
        [("2024-01-08",), ("2024-01-07",), ("2024-03-15",)],
        ["calendar_date_str"],
    ).select(F.col("calendar_date_str").cast(T.DateType()).alias("d"))
    table = frame.select(F.weekday("d").alias("w"), F.dayofweek("d").alias("dw")).to_arrow()
    assert table.column("w").to_pylist() == [0, 6, 4]
    # dayofweek is 1=Sunday..7=Saturday — different indexing (parity pin).
    assert table.column("dw").to_pylist() == [2, 1, 6]
    assert pa.types.is_integer(table.schema.field("w").type)


# sortWithinPartitions
def test_sort_within_partitions_orders_like_order_by(spark: ReparkSession) -> None:
    """Single-node repark = one partition → sortWithinPartitions ≡ orderBy (value + order)."""
    frame = spark.sql("SELECT * FROM (VALUES (3,'c'),(1,'a'),(2,'b')) AS t(id, name)")
    via_swp = frame.sortWithinPartitions("id").to_arrow().to_pylist()
    via_ob = frame.orderBy("id").to_arrow().to_pylist()
    assert (
        via_swp
        == via_ob
        == [
            {"id": 1, "name": "a"},
            {"id": 2, "name": "b"},
            {"id": 3, "name": "c"},
        ]
    )


def test_sort_within_partitions_multi_column(spark: ReparkSession) -> None:
    frame = spark.sql(
        "SELECT * FROM (VALUES (2,2,'b'),(1,9,'a'),(1,1,'c'),(2,1,'d')) AS t(g, n, name)"
    )
    got = frame.sortWithinPartitions("g", "n").to_arrow().to_pylist()
    assert got == [
        {"g": 1, "n": 1, "name": "c"},
        {"g": 1, "n": 9, "name": "a"},
        {"g": 2, "n": 1, "name": "d"},
        {"g": 2, "n": 2, "name": "b"},
    ]


# F.years/months/days/hours (partitionedBy only) + transform gate
def test_years_outside_partitioned_by_raises(spark: ReparkSession) -> None:
    """Oracle: years() outside partitionedBy raises PARTITION_TRANSFORM…_NOT_IN_PARTITIONED_BY."""
    frame = spark.sql("SELECT DATE '2024-03-15' AS d")
    with pytest.raises(
        AnalysisException, match="PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY"
    ):
        frame.select(F.years("d").alias("y")).collect()
    # Compound ops must keep the marker sticky — never silent-empty on the dummy null literal.
    with pytest.raises(
        AnalysisException, match="PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY"
    ):
        frame.filter(F.years("d").isNotNull()).collect()
    with pytest.raises(
        AnalysisException, match="PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY"
    ):
        frame.select((F.years("d") + 1).alias("y")).collect()
    # groupBy / join must not evaluate the dummy null literal.
    with pytest.raises(
        AnalysisException, match="PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY"
    ):
        frame.groupBy(F.years("d")).count().collect()
    other = spark.sql("SELECT DATE '2024-03-15' AS d2")
    with pytest.raises(
        AnalysisException, match="PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY"
    ):
        frame.join(other, F.years("d") == F.years(F.col("d2"))).collect()
    # F.* wrappers must carry the marker — never silent NULL / lit fallback.
    with pytest.raises(
        AnalysisException, match="PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY"
    ):
        frame.select(F.abs(F.years("d")).alias("y")).collect()
    with pytest.raises(
        AnalysisException, match="PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY"
    ):
        frame.select(F.coalesce(F.years("d"), F.lit(1)).alias("y")).collect()
    # Date-fn wrappers must carry the marker too — never year(NULL) silent.
    with pytest.raises(
        AnalysisException, match="PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY"
    ):
        frame.select(F.year(F.years("d")).alias("y")).collect()
    # Window.partitionBy must not evaluate the dummy null.
    from repark import Window

    with pytest.raises(
        AnalysisException, match="PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY"
    ):
        frame.select(
            F.row_number().over(Window.partitionBy(F.years("d")).orderBy("d")).alias("rn")
        ).collect()


def test_bucket_partitioned_by_round_trips_e2e(spark: ReparkSession) -> None:
    """``writeTo(t).partitionedBy(F.bucket(4, "id")).create()`` round-trips end-to-end.

    Facade → CTAS ``PARTITIONED BY (bucket(4, "id"))`` → the computed-mode fork splitter; every
    row reads back value AND type.
    """
    table = f"{CATALOG}.{NS}.bucket_e2e"
    frame = spark.sql(
        "SELECT * FROM (VALUES (1,'a'),(2,'b'),(3,'c'),(55,'d'),(89,'e')) AS t(id, name)"
    )
    frame.writeTo(table).partitionedBy(F.bucket(4, "id")).create()
    assert spark.catalog.tableExists(table), "the transform-partitioned create must land a table"
    rows = spark.sql(f"SELECT id, name FROM {table} ORDER BY id").to_arrow().to_pylist()
    assert rows == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
        {"id": 3, "name": "c"},
        {"id": 55, "name": "d"},
        {"id": 89, "name": "e"},
    ]


def test_years_partitioned_by_round_trips_e2e(spark: ReparkSession) -> None:
    """A ``years(event_date)``-partitioned create works end-to-end and rows round-trip."""
    table = f"{CATALOG}.{NS}.years_e2e"
    frame = spark.sql(
        "SELECT * FROM (VALUES (DATE '2024-03-15', 1),(DATE '2025-06-01', 2)) AS t(event_date, id)"
    )
    frame.writeTo(table).partitionedBy(F.years(F.col("event_date"))).create()
    assert spark.catalog.tableExists(table)
    assert spark.sql(f"SELECT id FROM {table} ORDER BY id").to_arrow().to_pylist() == [
        {"id": 1},
        {"id": 2},
    ]


def test_partition_transform_quotes_identity_arg() -> None:
    """Transforms embed a double-quoted identity arg, not raw text.

    If quoting regresses, hostile fragments re-enter PARTITIONED BY unescaped.
    """
    assert F.years("event_date")._partition_transform == 'years("event_date")'
    assert F.months(F.col("ts"))._partition_transform == 'months("ts")'
    assert F.days("d")._partition_transform == 'days("d")'
    assert F.hours("h")._partition_transform == 'hours("h")'
    assert F.bucket(4, "id")._partition_transform == 'bucket(4, "id")'
    # Quote-escape: embedded " becomes "" inside the identifier.
    assert F.years('x"; DROP')._partition_transform == 'years("x""; DROP")'
    assert F.bucket(8, 'x"; DROP')._partition_transform == 'bucket(8, "x""; DROP")'


# DataFrameWriterV2
def test_write_to_create_and_create_on_existing(spark: ReparkSession) -> None:
    """create() works; create() on existing → AnalysisException (TABLE_OR_VIEW_ALREADY_EXISTS)."""
    _source(spark, "(1,'a')").writeTo(TABLE).create()
    assert _read(spark) == [{"id": 1, "name": "a"}]
    with pytest.raises(AnalysisException, match=r"ALREADY_EXISTS|already exists"):
        _source(spark, "(2,'b')").writeTo(TABLE).create()


def test_write_to_create_or_replace_vs_create_discriminator(spark: ReparkSession) -> None:
    """createOrReplace replaces; create on existing errors."""
    _source(spark, "(1,'a')").writeTo(TABLE).createOrReplace()
    _source(spark, "(9,'z')").writeTo(TABLE).createOrReplace()
    assert _read(spark) == [{"id": 9, "name": "z"}]
    with pytest.raises(AnalysisException, match=r"ALREADY_EXISTS|already exists"):
        _source(spark, "(1,'a')").writeTo(TABLE).create()


def test_write_to_replace_missing_and_existing(spark: ReparkSession) -> None:
    missing = f"{CATALOG}.{NS}.rep_missing"
    with pytest.raises(AnalysisException, match=r"NOT_FOUND|does not exist"):
        _source(spark, "(1,'a')").writeTo(missing).replace()
    _source(spark, "(1,'a')").writeTo(TABLE).create()
    _source(spark, "(2,'b')").writeTo(TABLE).replace()
    assert _read(spark) == [{"id": 2, "name": "b"}]


def test_write_to_append_case_insensitive_by_name(spark: ReparkSession) -> None:
    """writeTo.append by-name is case-insensitive."""
    table = "glue_catalog.writer_v2_ns.byname_ci"
    spark.createDataFrame([(1, 10)], ["a", "b"]).writeTo(table).create()
    spark.createDataFrame([(20, 2)], ["B", "A"]).writeTo(table).append()
    rows = spark.sql(f"SELECT a, b FROM {table} ORDER BY a").to_arrow().to_pylist()
    assert rows == [{"a": 1, "b": 10}, {"a": 2, "b": 20}]


def test_write_to_append_resolves_by_name(spark: ReparkSession) -> None:
    """V2 append is by-name (a reordered frame does not transpose)."""
    table = f"{CATALOG}.{NS}.byname"
    spark.createDataFrame([(1, 10)], ["a", "b"]).writeTo(table).create()
    spark.createDataFrame([(20, 2)], ["b", "a"]).writeTo(table).append()
    got = spark.sql(f"SELECT a, b FROM {table} ORDER BY a").to_arrow()
    assert got.to_pylist() == [{"a": 1, "b": 10}, {"a": 2, "b": 20}]
    assert got.schema.field("a").type == pa.int64()
    assert got.schema.field("b").type == pa.int64()


def test_write_to_append_vs_v1_insert_into_discriminator(spark: ReparkSession) -> None:
    """Same reordered frame: V2 append by-name vs V1 insertInto positional."""
    t_v2 = f"{CATALOG}.{NS}.v2name"
    t_v1 = f"{CATALOG}.{NS}.v1pos"
    spark.createDataFrame([(1, 10)], ["a", "b"]).writeTo(t_v2).create()
    spark.createDataFrame([(1, 10)], ["a", "b"]).write.saveAsTable(t_v1)
    reordered = spark.createDataFrame([(20, 2)], ["b", "a"])
    reordered.writeTo(t_v2).append()
    reordered.write.insertInto(t_v1)
    v2_row = spark.sql(f"SELECT a, b FROM {t_v2} ORDER BY a").to_arrow().to_pylist()[1]
    v1_row = spark.sql(f"SELECT a, b FROM {t_v1} ORDER BY a").to_arrow().to_pylist()[1]
    assert v2_row == {"a": 2, "b": 20}, "writeTo.append is BY NAME"
    assert v1_row == {"a": 20, "b": 2}, "insertInto is POSITIONAL"
    assert v2_row != v1_row


def test_write_to_overwrite_partitions_refuses_loud_and_leaves_data(
    spark: ReparkSession,
) -> None:
    """overwritePartitions raises UnsupportedOperationException; target untouched.

    Spark Iceberg semantics are DYNAMIC partition overwrite (source partitions only; empty
    source = no-op), but the engine has only static whole-table INSERT OVERWRITE — honoring the
    call silently replaced ALL rows. Refuse loud until the fork's ReplacePartitions is wired;
    both spellings gate and the target's rows survive the raise bit-for-bit.
    """
    from repark.errors import UnsupportedOperationException

    table = f"{CATALOG}.{NS}.ow_gate"
    (
        spark.sql("SELECT * FROM (VALUES (1,'a'),(2,'b')) AS t(id, cat)")
        .writeTo(table)
        .partitionedBy("cat")
        .create()
    )
    src = spark.sql("SELECT * FROM (VALUES (9,'a')) AS t(id, cat)")
    with pytest.raises(UnsupportedOperationException, match="dynamic partition overwrite"):
        src.writeTo(table).overwritePartitions()
    empty = spark.sql("SELECT * FROM (VALUES (1,'a')) AS t(id, cat) WHERE false")
    with pytest.raises(UnsupportedOperationException, match="dynamic partition overwrite"):
        empty.writeTo(table).overwrite_partitions()
    got = spark.sql(f"SELECT id, cat FROM {table} ORDER BY id").to_arrow().to_pylist()
    assert got == [{"id": 1, "cat": "a"}, {"id": 2, "cat": "b"}], "target must be untouched"


def test_save_as_table_empty_overwrite_wipes_all(spark: ReparkSession) -> None:
    """V1 mode('overwrite').saveAsTable empty source must wipe."""
    table = f"{CATALOG}.{NS}.v1_ow_empty"
    spark.sql("SELECT 1 AS id, 'a' AS n").write.mode("overwrite").saveAsTable(table)
    spark.sql("SELECT 1 AS id, 'a' AS n WHERE false").write.mode("overwrite").saveAsTable(table)
    assert spark.sql(f"SELECT * FROM {table}").to_arrow().to_pylist() == []


def test_insert_into_empty_overwrite_wipes_all(spark: ReparkSession) -> None:
    """insertInto(overwrite=True) empty source must wipe via INSERT OVERWRITE SQL.

    Guards a special-case to bare DELETE (skipping engine schema validate) or a fork empty
    short-circuit no-op regression.
    """
    table = f"{CATALOG}.{NS}.v1_ow_insert_into_empty"
    spark.sql("SELECT 1 AS id, 'a' AS n").write.mode("overwrite").saveAsTable(table)
    empty = spark.sql("SELECT 1 AS id, 'a' AS n WHERE false")
    empty.write.insertInto(table, overwrite=True)
    assert spark.sql(f"SELECT * FROM {table}").to_arrow().to_pylist() == []


def test_write_to_overwrite_condition_loud_reject(spark: ReparkSession) -> None:
    _source(spark, "(1,'a')").writeTo(TABLE).create()
    with pytest.raises(UnsupportedOperationException, match="overwrite\\(condition\\)"):
        _source(spark, "(1,'aa')").writeTo(TABLE).overwrite(F.col("id") == 1)


def test_write_to_using_rejects_non_iceberg(spark: ReparkSession) -> None:
    with pytest.raises(ValueError, match="iceberg"):
        _source(spark, "(1,'a')").writeTo(TABLE).using("parquet")


def test_write_to_table_property_and_identity_partitioned_by(spark: ReparkSession) -> None:
    table = f"{CATALOG}.{NS}.props_part"
    (
        spark.sql("SELECT * FROM (VALUES (1,'a'),(2,'b')) AS t(id, cat)")
        .writeTo(table)
        .tableProperty("write.format.default", "parquet")
        .tableProperty("format-version", "2")
        .partitionedBy(F.col("cat"))
        .create()
    )
    rows = spark.sql(f"SELECT id, cat FROM {table} ORDER BY id").to_arrow().to_pylist()
    assert rows == [{"id": 1, "cat": "a"}, {"id": 2, "cat": "b"}]
    only_a = spark.sql(f"SELECT id FROM {table} WHERE cat = 'a'").to_arrow().to_pylist()
    assert only_a == [{"id": 1}]


def test_write_to_partitioned_by_string_identity(spark: ReparkSession) -> None:
    table = f"{CATALOG}.{NS}.str_part"
    spark.sql("SELECT 1 AS id, 'x' AS p").writeTo(table).partitionedBy("p").create()
    assert spark.sql(f"SELECT id, p FROM {table}").to_arrow().to_pylist() == [{"id": 1, "p": "x"}]


def test_write_to_option_warns_once(spark: ReparkSession) -> None:
    """option/options emit a process-once UserWarning (ignored storage options)."""
    import warnings

    from repark.spark.dataframe import _reset_writer_v2_option_warnings_for_tests

    _reset_writer_v2_option_warnings_for_tests()
    writer = _source(spark, "(1,'a')").writeTo(TABLE)
    with pytest.warns(UserWarning, match="option/options are accepted"):
        writer.option("compression", "zstd")
    # Second call in the same process must not warn again.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        writer.options(compression="snappy")
        assert caught == [], f"second option/options must not re-warn, got {caught}"


# path parquet / format.save via COPY TO
def test_write_parquet_path_round_trip(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "out_parquet"
    frame = _source(spark, "(1,'a'),(2,'b')")
    frame.write.mode("overwrite").parquet(str(path))
    assert path.is_dir(), "COPY TO writes a directory"
    parquet_files = list(path.rglob("*.parquet"))
    assert parquet_files, "repark COPY TO emits at least one *.parquet file"
    # Shape disclosure: no Spark _SUCCESS marker required.
    got = spark.read.parquet(str(path)).orderBy("id").to_arrow().to_pylist()
    assert got == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


def test_write_format_parquet_save_equivalent(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "via_format"
    _source(spark, "(3,'c')").write.mode("overwrite").format("parquet").save(str(path))
    got = spark.read.parquet(str(path)).to_arrow().to_pylist()
    assert got == [{"id": 3, "name": "c"}]


def test_write_parquet_error_mode_on_existing(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "exists"
    _source(spark, "(1,'a')").write.mode("overwrite").parquet(str(path))
    with pytest.raises(AnalysisException, match=r"PATH_ALREADY_EXISTS|already exists"):
        _source(spark, "(2,'b')").write.mode("error").parquet(str(path))
    # default mode is error-like
    with pytest.raises(AnalysisException, match=r"PATH_ALREADY_EXISTS|already exists"):
        _source(spark, "(2,'b')").write.parquet(str(path))


def test_write_parquet_path_braces_and_view_placeholder_literal(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """User path text must not be reinterpreted by writer SQL binding.

    Brace segments and the substring ``{view}`` are literal path components — never
    ``str.format`` keys / placeholders.
    """
    braced = tmp_path / "{foo}" / "out"
    _source(spark, "(1,'a')").write.mode("overwrite").parquet(str(braced))
    assert braced.is_dir()
    assert (spark.read.parquet(str(braced)).to_arrow().to_pylist()) == [{"id": 1, "name": "a"}]

    with_view_token = tmp_path / "out{view}x"
    _source(spark, "(2,'b')").write.mode("overwrite").format("parquet").save(str(with_view_token))
    assert with_view_token.is_dir(), "path must keep the literal {view} segment"
    assert spark.read.parquet(str(with_view_token)).to_arrow().to_pylist() == [
        {"id": 2, "name": "b"}
    ]


def test_write_parquet_overwrite_preserves_data_until_success(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """Overwrite stages then swaps — prior path remains until COPY succeeds.

    On-disk parquet is the source of truth for the second write: DataFusion may cache a
    directory listing for ``read.parquet`` on a reused path within one session.
    """
    import pyarrow.parquet as pq

    path = tmp_path / "atomic_out"
    _source(spark, "(1,'a')").write.mode("overwrite").parquet(str(path))
    before_files = list(path.rglob("*.parquet"))
    assert before_files
    assert pq.read_table(before_files[0]).to_pylist() == [{"id": 1, "name": "a"}]
    # Successful overwrite replaces contents; staging sibling must not linger.
    _source(spark, "(9,'z')").write.mode("overwrite").parquet(str(path))
    after_files = list(path.rglob("*.parquet"))
    assert after_files, "overwrite must leave parquet under the destination path"
    after_rows = [row for file in after_files for row in pq.read_table(file).to_pylist()]
    assert after_rows == [{"id": 9, "name": "z"}]
    staging_left = list(tmp_path.glob("repark-staging-*"))
    assert staging_left == [], f"staging paths must be swapped away, found {staging_left}"


def test_write_parquet_empty_overwrite_does_not_destroy_prior(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """Empty-frame overwrite must not rmtree-then-fail.

    DataFusion COPY on an empty SELECT can succeed without creating the staging path; the writer
    must materialize staging before touching the destination so prior data is either fully
    replaced or left intact — never deleted into a missing rename.
    """
    import pyarrow.parquet as pq

    path = tmp_path / "empty_ow"
    _source(spark, "(1,'a')").write.mode("overwrite").parquet(str(path))
    assert pq.read_table(next(path.rglob("*.parquet"))).to_pylist() == [{"id": 1, "name": "a"}]
    empty = spark.sql("SELECT * FROM (VALUES (1,'a')) AS t(id, name) WHERE 1=0")
    empty.write.mode("overwrite").parquet(str(path))
    assert path.exists(), "empty overwrite must leave a destination path"
    # Empty success → no residual part files required; prior non-empty parts must not linger.
    leftover_rows = [
        row for file in path.rglob("*.parquet") for row in pq.read_table(file).to_pylist()
    ]
    assert leftover_rows == [], f"prior rows must not survive empty overwrite: {leftover_rows}"
    staging_left = list(tmp_path.glob("repark-staging-*"))
    assert staging_left == [], f"staging paths must be swapped away, found {staging_left}"


def test_write_parquet_empty_first_write_succeeds(spark: ReparkSession, tmp_path: Path) -> None:
    """First path write of an empty frame must not raise FileNotFoundError on rename."""
    path = tmp_path / "empty_first"
    spark.sql("SELECT 1 AS id WHERE false").write.mode("overwrite").parquet(str(path))
    assert path.exists() and path.is_dir()


def test_write_parquet_overwrite_same_session_read_sees_new_rows(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """Same-session read.parquet after path overwrite must not return stale rows.

    DataFusion's list-files cache keys directory listings by path and stage-swap reuses the
    destination path, so session RuntimeEnv disables that cache for the public write→read path.
    """
    path = tmp_path / "rw_cache"
    spark.sql("SELECT 1 AS id").write.mode("overwrite").parquet(str(path))
    assert spark.read.parquet(str(path)).to_arrow().to_pylist() == [{"id": 1}]
    spark.sql("SELECT 9 AS id").write.mode("overwrite").parquet(str(path))
    assert spark.read.parquet(str(path)).to_arrow().to_pylist() == [{"id": 9}]
    # Empty overwrite: disk empty + same-session read must not resurrect prior rows.
    spark.sql("SELECT 1 AS id WHERE false").write.mode("overwrite").parquet(str(path))
    assert spark.read.parquet(str(path)).to_arrow().to_pylist() == []


def test_write_to_table_property_view_placeholder_literal(spark: ReparkSession) -> None:
    """TBLPROPERTIES values containing ``{view}`` must not be rewritten."""
    table = f"{CATALOG}.{NS}.prop_view_token"
    (
        _source(spark, "(1,'a')")
        .writeTo(table)
        .tableProperty("repark.test.comment", "{view}")
        .create()
    )
    assert _read(spark, table) == [{"id": 1, "name": "a"}]
    # The critical pin: create succeeds without format KeyError or silent SQL rewrite
    # (the value is embedded via _sql_string_literal, not str.format).


def test_write_save_requires_path(spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException, match="path"):
        _source(spark, "(1,'a')").write.format("parquet").save()


def test_write_csv_json_round_trip(spark: ReparkSession, tmp_path: Path) -> None:
    """csv/json path writes round-trip."""
    csv_path = tmp_path / "c"
    json_path = tmp_path / "j"
    _source(spark, "(1,'a')").write.mode("overwrite").csv(str(csv_path), header=True)
    _source(spark, "(1,'a')").write.mode("overwrite").json(str(json_path))
    assert spark.read.csv(str(csv_path), header=True, inferSchema=True).to_arrow().to_pylist() == [
        {"id": 1, "name": "a"}
    ]
    assert spark.read.json(str(json_path)).to_arrow().to_pylist() == [{"id": 1, "name": "a"}]


def test_write_orc_loud(spark: ReparkSession, tmp_path: Path) -> None:
    """Unsupported path format stays DATA_SOURCE_NOT_FOUND-shaped."""
    with pytest.raises(AnalysisException, match="DATA_SOURCE_NOT_FOUND") as raised:
        _source(spark, "(1,'a')").write.format("orc").save(str(tmp_path / "o"))
    assert "orc" in str(raised.value).lower()


# ==================================================================================================
# Mini-dogfood — production writeTo snippet shape (memory catalog)
# ==================================================================================================


def test_mini_dogfood_write_to_production_snippet(spark: ReparkSession) -> None:
    """Reproduce the production writeTo / sortWithinPartitions / createOrReplace shape.

    overwritePartitions is refused loud (dynamic partition overwrite unavailable), exactly what
    the production write_roll_schedule would hit on repark today.
    """
    fq_table = f"{CATALOG}.{NS}.spliced_futures_1m"
    schedule_table = f"{CATALOG}.{NS}.schedule"

    # No TIMESTAMP literals (DataFusion → timestamp_ns; Iceberg v2 rejects ns): a sortable
    # string clock column matches the production dual-key sort shape.
    spliced = spark.sql(
        "SELECT * FROM (VALUES "
        "(DATE '2024-03-15', '2024-03-15 10:00:00', 1),"
        "(DATE '2024-03-15', '2024-03-15 09:00:00', 2),"
        "(DATE '2024-03-16', '2024-03-16 08:00:00', 3)"
        ") AS t(event_date, event_timestamp_utc, id)"
    )
    # years_transform gated — identity partition on event_date instead.
    writer = (
        spliced.sortWithinPartitions("event_date", "event_timestamp_utc")
        .writeTo(fq_table)
        .partitionedBy(F.col("event_date"))
    )
    for key, value in {
        "write.format.default": "parquet",
        "format-version": "2",
    }.items():
        writer = writer.tableProperty(key, value)
    writer.createOrReplace()

    rows = spark.sql(f"SELECT id FROM {fq_table} ORDER BY id").to_arrow().to_pylist()
    assert rows == [{"id": 1}, {"id": 2}, {"id": 3}]

    schedule_df = spark.sql("SELECT * FROM (VALUES (10, 'a'),(20, 'b')) AS t(id, cat)")
    schedule_df.writeTo(schedule_table).partitionedBy(F.col("cat"), F.col("id")).create()
    # The production job fails here, visibly, not silently.
    with pytest.raises(UnsupportedOperationException, match="dynamic partition overwrite"):
        spark.sql("SELECT * FROM (VALUES (99, 'a')) AS t(id, cat)").writeTo(
            schedule_table
        ).overwritePartitions()
    assert spark.sql(
        f"SELECT id, cat FROM {schedule_table} ORDER BY id"
    ).to_arrow().to_pylist() == [
        {"id": 10, "cat": "a"},
        {"id": 20, "cat": "b"},
    ]

    spliced.writeTo(f"{CATALOG}.{NS}.years_prod").partitionedBy(
        F.years(F.col("event_date"))
    ).createOrReplace()
    assert spark.sql(
        f"SELECT id FROM {CATALOG}.{NS}.years_prod ORDER BY id"
    ).to_arrow().to_pylist() == [{"id": 1}, {"id": 2}, {"id": 3}]
