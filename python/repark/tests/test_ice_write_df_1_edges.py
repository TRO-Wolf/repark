"""U7 PR1 round 2 — Spark's answers for the writer shapes the first review found unpinned.

Every expected value is the committed ``ice_write_df_1_spark_oracle.json`` ``measured``
entry of the same name (Spark 4.1.2 + Iceberg 1.11.0): the missing bucket column, the
``output-spec-id`` on replacing, dynamic and empty writes, the path relation text, the
saveAsTable layout check and the ``CLUSTERED BY`` scan.

pins: u7-write-df/C-004, C-005, C-006, C-008, C-009, C-011, C-013, C-015, C-016, C-017
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from test_ice_write_df_1 import (
    _MEASURED,
    _T,
    _assert_error,
    _assert_state,
    _frame,
    _operations,
    _partitioning,
    _rows,
    _seed,
    _specs,
    _summaries,
    spark,
)

from repark import ReparkSession
from repark.errors import AnalysisException, IllegalArgumentException, ParseException

__all__ = ["spark"]


@pytest.mark.parametrize(
    ("cell", "mode", "part"),
    [
        ("bucketBy_new_missing_col", "error", []),
        ("bucketBy_new_missing_col_overwrite", "overwrite", []),
        ("bucketBy_new_missing_col_partby", "error", ["cat"]),
    ],
)
def test_a_missing_bucket_column_on_a_new_table_is_legacy_3060(
    spark: ReparkSession, cell: str, mode: str, part: list[str]
) -> None:
    """Spark's CTAS and RTAS cannot find the column: the frame's printSchema tree, no table.

    pins: u7-write-df/C-015
    """
    writer = _frame(spark).write.format("iceberg").mode(mode).partitionBy(*part)
    with pytest.raises(AnalysisException) as raised:
        writer.bucketBy(4, "nope").saveAsTable(_T)
    _assert_error(raised.value, cell)
    assert raised.value.getMessageParameters() == {  # type: ignore[attr-defined]
        "i": "nope",
        "schema": str(raised.value).split(":\n", 1)[1],
    }
    assert not spark.catalog.tableExists(_T)


@pytest.mark.parametrize(
    ("cell", "schema", "column"),
    [
        ("bucketBy_new_nested_col", "id BIGINT, s STRUCT<a: INT, b: STRING>", "s.a"),
        ("bucketBy_new_nested_missing", "id BIGINT, s STRUCT<a: INT, b: STRING>", "s.zz"),
        ("bucketBy_new_dotted_missing", "id BIGINT, data STRING, cat STRING", "a.b"),
    ],
)
def test_a_dotted_bucket_column_is_backticked_in_legacy_3060(
    spark: ReparkSession, cell: str, schema: str, column: str
) -> None:
    """A bucket column naming a nested field is not found, and a dotted name is backticked.

    pins: u7-write-df/C-015
    """
    rows = [(1, (1, "q"))] if "STRUCT" in schema else [(7, "g", "x"), (8, "h", "w")]
    frame = spark.createDataFrame(rows, schema)
    with pytest.raises(AnalysisException) as raised:
        frame.write.format("iceberg").bucketBy(4, column).saveAsTable(_T)
    _assert_error(raised.value, cell)
    assert raised.value.getMessageParameters() == {  # type: ignore[attr-defined]
        "i": f"`{column}`",
        "schema": str(raised.value).split(":\n", 1)[1],
    }
    assert not spark.catalog.tableExists(_T)


@pytest.mark.parametrize(
    ("cell", "mode", "error_type"),
    [
        ("bucket_unbucketed_missing_append", "append", IllegalArgumentException),
        ("bucket_unbucketed_missing_overwrite", "overwrite", AnalysisException),
        ("bucket_unbucketed_missing_error", "error", AnalysisException),
        ("bucket_unbucketed_missing_ignore", "ignore", AnalysisException),
    ],
)
def test_a_missing_bucket_column_on_an_existing_table(
    spark: ReparkSession, cell: str, mode: str, error_type: type[Exception]
) -> None:
    """Append answers the layout mismatch; every other mode is Spark's 3060, before existence.

    pins: u7-write-df/C-015
    """
    _seed(spark)
    with pytest.raises(error_type) as raised:
        _frame(spark).write.format("iceberg").bucketBy(4, "nope").mode(mode).saveAsTable(_T)
    _assert_error(raised.value, cell)
    _assert_state(spark, cell)


def test_bucket_count_precedes_the_missing_column(spark: ReparkSession) -> None:
    """``bucketBy(0, ...)`` answers INVALID_BUCKET_COUNT first, missing column or not.

    pins: u7-write-df/C-015
    """
    for cell, column in [("bucketBy_new_zero", "id"), ("bucketBy_new_missing_col_zero", "nope")]:
        with pytest.raises(AnalysisException) as raised:
            _frame(spark).write.format("iceberg").bucketBy(0, column).saveAsTable(_T)
        _assert_error(raised.value, cell)
    assert not spark.catalog.tableExists(_T)


def test_error_mode_and_bucketed_save_answer_spark_text(spark: ReparkSession) -> None:
    """saveAsTable error mode says Spark's already-exists text; bucketed ``save()`` refuses.

    pins: u7-write-df/C-004, C-009
    """
    _seed(spark)
    for cell, writer in [
        ("sat_error_existing", _frame(spark).write.format("iceberg")),
        ("bucketBy_existing_default", _frame(spark).write.format("iceberg").bucketBy(4, "id")),
    ]:
        with pytest.raises(AnalysisException) as raised:
            writer.saveAsTable(_T)
        _assert_error(raised.value, cell)
    with pytest.raises(AnalysisException) as bucketed:
        _frame(spark).write.format("iceberg").bucketBy(4, "id").mode("append").save(_T)
    _assert_error(bucketed.value, "bucketBy_save_name")
    _assert_state(spark, "bucketBy_existing_default")


def test_bucketed_overwrite_of_two_columns_leaves_the_table(spark: ReparkSession) -> None:
    """A multi-column bucketed RTAS refuses with Spark's text before replacing anything.

    pins: u7-write-df/C-008
    """
    _seed(spark)
    with pytest.raises(IllegalArgumentException) as raised:
        _frame(spark).write.format("iceberg").bucketBy(4, "id", "data").mode(
            "overwrite"
        ).saveAsTable(_T)
    _assert_error(raised.value, "bucket_existing_overwrite_two")
    _assert_state(spark, "bucket_existing_overwrite_two")


def test_layout_mismatch_renders_every_table_transform(spark: ReparkSession) -> None:
    """The table side lists days and truncate transforms in Spark's words.

    pins: u7-write-df/C-009
    """
    spark.sql(
        f"CREATE TABLE {_T} (id BIGINT, data STRING, cat STRING, ts TIMESTAMP) USING iceberg "
        "PARTITIONED BY (cat, days(ts), truncate(3, data))"
    )
    frame = spark.createDataFrame(
        [(7, "g", "x", None)], "id BIGINT, data STRING, cat STRING, ts TIMESTAMP"
    )
    with pytest.raises(IllegalArgumentException) as raised:
        frame.write.format("iceberg").partitionBy("cat").bucketBy(4, "id").mode(
            "append"
        ).saveAsTable(_T)
    _assert_error(raised.value, "bucket_existing_partitioned_append")
    assert _rows(spark) == []


def test_layout_mismatch_renders_time_transforms(spark: ReparkSession) -> None:
    """The table side names years, months and hours as Spark does.

    pins: u7-write-df/C-009
    """
    columns = "id BIGINT, y DATE, m TIMESTAMP, h TIMESTAMP"
    spark.sql(
        f"CREATE TABLE {_T} ({columns}) USING iceberg "
        "PARTITIONED BY (years(y), months(m), hours(h))"
    )
    frame = spark.createDataFrame([(1, None, None, None)], columns)
    with pytest.raises(IllegalArgumentException) as raised:
        frame.write.format("iceberg").bucketBy(4, "id").mode("append").saveAsTable(_T)
    _assert_error(raised.value, "bucket_existing_time_append")
    _assert_state(spark, "bucket_existing_time_append")


@pytest.mark.parametrize(
    ("cell", "part"),
    [
        ("sat_partby_match_append", "PARTITIONED BY (cat)"),
        ("sat_nopartby_append_partitioned", "PARTITIONED BY (cat)"),
    ],
)
def test_save_as_table_append_checks_a_partition_by_layout(
    spark: ReparkSession, cell: str, part: str
) -> None:
    """A non-bucketed saveAsTable append whose partitionBy matches, or is absent, appends.

    pins: u7-write-df/C-005
    """
    _seed(spark, part=part)
    writer = _frame(spark).write.format("iceberg").mode("append")
    columns = [] if cell == "sat_nopartby_append_partitioned" else ["cat"]
    writer.partitionBy(*columns).saveAsTable(_T)
    _assert_state(spark, cell)


def test_save_as_table_append_refuses_a_different_partition_by(spark: ReparkSession) -> None:
    """A partitionBy the unpartitioned table lacks refuses with Spark's requirement text.

    pins: u7-write-df/C-005
    """
    _seed(spark)
    with pytest.raises(IllegalArgumentException) as raised:
        _frame(spark).write.format("iceberg").mode("append").partitionBy("cat").saveAsTable(_T)
    _assert_error(raised.value, "sat_partby_mismatch_append")
    _assert_state(spark, "sat_partby_mismatch_append")


def test_dynamic_overwrite_replaces_partitions_of_the_staged_spec(spark: ReparkSession) -> None:
    """Spec 0 partitioned by cat, current unpartitioned: only partitions x and w are replaced.

    pins: u7-write-df/C-016
    """
    _seed(spark, part="PARTITIONED BY (cat)")
    spark.sql(f"ALTER TABLE {_T} DROP PARTITION FIELD cat")
    _frame(spark).writeTo(_T).option("output-spec-id", "0").overwritePartitions()
    _assert_state(spark, "dyn_overwrite_old_part_spec0")
    deleted = _summaries(spark)[1]
    assert (deleted["deleted-records"], deleted["total-records"]) == ("2", "3")


@pytest.mark.parametrize(
    ("cell", "part", "alter", "options"),
    [
        (
            "dyn_overwrite_old_part_nooption",
            "PARTITIONED BY (cat)",
            "DROP PARTITION FIELD cat",
            {},
        ),
        ("dyn_overwrite_new_part_spec0", "", "ADD PARTITION FIELD cat", {"output-spec-id": "0"}),
    ],
)
def test_dynamic_overwrite_of_an_unpartitioned_staged_spec_replaces_everything(
    spark: ReparkSession, cell: str, part: str, alter: str, options: dict[str, str]
) -> None:
    """An unpartitioned staged spec, current or chosen, replaces every row, as Spark does.

    pins: u7-write-df/C-016
    """
    _seed(spark, part=part)
    spark.sql(f"ALTER TABLE {_T} {alter}")
    _frame(spark).writeTo(_T).options(**options).overwritePartitions()
    _assert_state(spark, cell)


@pytest.mark.parametrize(
    ("cell", "write"),
    [
        (
            "bucketed_rtas_spec0",
            lambda df: (
                df.write.format("iceberg")
                .bucketBy(4, "id")
                .mode("overwrite")
                .option("output-spec-id", "0")
                .saveAsTable(_T)
            ),
        ),
        (
            "createOrReplace_part_spec0",
            lambda df: (
                df.writeTo(_T)
                .option("output-spec-id", "0")
                .partitionedBy(df["cat"])
                .createOrReplace()
            ),
        ),
        (
            "replace_part_spec1",
            lambda df: (
                df.writeTo(_T).option("output-spec-id", "1").partitionedBy(df["cat"]).replace()
            ),
        ),
    ],
)
def test_output_spec_id_on_a_replacing_write_resolves_in_the_replacement(
    spark: ReparkSession, cell: str, write: Callable[[Any], None]
) -> None:
    """RTAS and replace resolve the id against the replacement metadata and stage under it.

    pins: u7-write-df/C-016
    """
    _seed(spark)
    write(_frame(spark))
    _assert_state(spark, cell)


def test_output_spec_id_on_a_create_and_an_unknown_replacement_spec(spark: ReparkSession) -> None:
    """A CTAS stages under its own spec 0; an id the replacement lacks refuses, table kept.

    pins: u7-write-df/C-016
    """
    _frame(spark).write.format("iceberg").partitionBy("cat").option(
        "output-spec-id", "0"
    ).saveAsTable(_T)
    _assert_state(spark, "ctas_new_partby_spec0")
    _seed(spark)
    frame = _frame(spark)
    with pytest.raises(IllegalArgumentException) as raised:
        frame.writeTo(_T).option("output-spec-id", "5").partitionedBy(frame["cat"]).replace()
    _assert_error(raised.value, "replace_part_spec5")
    _assert_state(spark, "replace_part_spec5")


@pytest.mark.parametrize(
    ("cell", "part", "write"),
    [
        (
            "empty_overwritePartitions_spec7",
            ["cat"],
            lambda df: df.writeTo(_T).option("output-spec-id", "7").overwritePartitions(),
        ),
        (
            "empty_append_spec7",
            [],
            lambda df: df.writeTo(_T).option("output-spec-id", "7").append(),
        ),
        (
            "empty_saveAsTable_append_spec7",
            [],
            lambda df: (
                df.write.format("iceberg")
                .option("output-spec-id", "7")
                .mode("append")
                .saveAsTable(_T)
            ),
        ),
        (
            "empty_insertInto_spec7",
            [],
            lambda df: df.write.option("output-spec-id", "7").insertInto(_T),
        ),
        (
            "empty_overwrite_saveAsTable_spec7",
            [],
            lambda df: (
                df.write.format("iceberg")
                .option("output-spec-id", "7")
                .mode("overwrite")
                .saveAsTable(_T)
            ),
        ),
    ],
)
def test_an_unknown_output_spec_id_refuses_a_write_that_stages_nothing(
    spark: ReparkSession, cell: str, part: list[str], write: Callable[[Any], None]
) -> None:
    """An empty frame still validates the id against the table, as Spark does.

    pins: u7-write-df/C-011
    """
    _seed(spark)
    for field in part:
        spark.sql(f"ALTER TABLE {_T} ADD PARTITION FIELD {field}")
    with pytest.raises(IllegalArgumentException) as raised:
        write(_frame(spark).limit(0))
    _assert_error(raised.value, cell)
    _assert_state(spark, cell)


@pytest.mark.parametrize(
    ("cell", "target", "mode"),
    [
        ("save_file_uri", "file://{root}/a/b", "append"),
        ("save_trailing_slash", "{root}/a/b/", "append"),
        ("save_double_slash", "/tmp//x//y", "overwrite"),
        ("save_single_segment_slash", "/x", "append"),
        ("save_relative", "a/b", "append"),
    ],
)
def test_save_path_relations_split_at_the_last_slash(
    spark: ReparkSession, tmp_path: Path, cell: str, target: str, mode: str
) -> None:
    """URI, trailing-slash, doubled-slash and relative paths name Spark's path relation.

    pins: u7-write-df/C-006
    """
    with pytest.raises(AnalysisException) as raised:
        _frame(spark).write.format("iceberg").mode(mode).save(target.format(root=tmp_path))
    _assert_error(raised.value, cell, root=tmp_path)
    assert not (tmp_path / "a").exists()


@pytest.mark.parametrize(
    ("cell", "statements"),
    [
        (
            "clustered_colname",
            [
                "CREATE TABLE {t} (clustered BIGINT, data STRING) USING iceberg "
                "CLUSTERED BY (clustered) INTO 4 BUCKETS",
                "INSERT INTO {t} VALUES (1, 'a'), (2, 'b')",
            ],
        ),
        (
            "ctas_clustered_colname",
            [
                "CREATE TABLE {t} USING iceberg CLUSTERED BY (clustered) INTO 4 BUCKETS "
                "AS SELECT id AS clustered, data FROM v"
            ],
        ),
    ],
)
def test_a_column_named_clustered_keeps_the_bucket_clause(
    spark: ReparkSession, cell: str, statements: list[str]
) -> None:
    """The rewrite tries every ``CLUSTERED`` word, so a column of that name still buckets.

    pins: u7-write-df/C-017
    """
    _frame(spark).createOrReplaceTempView("v")
    for statement in statements:
        spark.sql(statement.format(t=_T))
    expected = _MEASURED[cell]
    assert [list(row) for row in spark.sql(f"SELECT * FROM {_T} ORDER BY 1").collect()] == (
        expected["rows"]
    )
    assert _operations(spark) == expected["operations"]
    assert _specs(spark) == expected["specs"]
    assert _partitioning(spark) == expected["partitioning"]


def test_sorted_by_ordering_in_a_clustered_clause(spark: ReparkSession) -> None:
    """``ASC`` refuses like an unqualified sort; ``DESC`` is a ParseException (residue: text).

    pins: u7-write-df/C-017
    """
    base = "CREATE TABLE {t} (id BIGINT, x STRING) USING iceberg CLUSTERED BY (id) SORTED BY "
    with pytest.raises(IllegalArgumentException) as raised:
        spark.sql((base + "(x ASC) INTO 4 BUCKETS").format(t=_T))
    _assert_error(raised.value, "clustered_sorted_asc")
    for cell, sql in [
        ("clustered_sorted_desc", base + "(x DESC) INTO 4 BUCKETS"),
        (
            "clustered_self_sorted_desc",
            "CREATE TABLE {t} (id BIGINT) USING iceberg CLUSTERED BY (id) SORTED BY (id DESC) "
            "INTO 4 BUCKETS",
        ),
    ]:
        with pytest.raises(ParseException):
            spark.sql(sql.format(t=_T))
        assert _MEASURED[cell]["error"]["type"] == "ParseException"
    assert not spark.catalog.tableExists(_T)


def test_a_mixed_partition_spec_describes_as_part_rows(spark: ReparkSession) -> None:
    """``PARTITIONED BY (cat, bucket(4, id))`` lists both fields as ``Part N`` rows.

    pins: u7-write-df/C-013
    """
    spark.sql(
        f"CREATE TABLE {_T} (id BIGINT, data STRING, cat STRING) USING iceberg "
        "PARTITIONED BY (cat, bucket(4, id))"
    )
    _assert_state(spark, "create_part_bucket_mixed")
