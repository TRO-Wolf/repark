"""WO TBLPROPS-1 battery — ``SHOW TBLPROPERTIES`` on an Iceberg table.

Fresh, post-insert, post-``SET TBLPROPERTIES``, v1 and partitioned tables answer
Spark's exact row lists; keyed lookups hit built-ins and miss loudly with
Spark's text; two-part and one-part names resolve after ``USE``; a temporary
view answers the empty frame; a missing table keeps the ``TABLE_OR_VIEW_NOT_FOUND``
refusal. Every expected list below is Spark 4.1.2 + Iceberg 1.11, measured
2026-09-26 (``tblprops-spark-2026-09-26.json``).

pins: tblprops-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException


@pytest.fixture()
def spark(tmp_path: Path) -> ReparkSession:
    """Memory catalog ``sc`` with namespace ``ns`` for the tblproperties pins."""
    session = ReparkSession.builder.appName("pytest-tblprops-1").getOrCreate()
    session.register_memory_catalog("sc", tmp_path)
    session.sql("CREATE NAMESPACE sc.ns")
    return session


def _rows(frame: Any) -> list[list[Any]]:
    """Collect a frame to plain nested lists for golden comparison."""
    return [list(row) for row in frame.collect()]


def _fresh_rows(snapshot: str) -> list[list[str]]:
    """Spark's rows for the fresh ``k=v`` table with the given snapshot cell."""
    return [
        ["current-snapshot-id", snapshot],
        ["format", "iceberg/parquet"],
        ["format-version", "2"],
        ["k", "v"],
        ["write.parquet.compression-codec", "zstd"],
    ]


def test_fresh_table_answers_spark_rows(spark: ReparkSession) -> None:
    """D-SHOW-TBLPROPERTIES — a fresh table answers Spark's five rows. pins: tblprops-1/C-001."""
    spark.sql("CREATE TABLE sc.ns.t (id BIGINT) USING iceberg TBLPROPERTIES ('k'='v')")
    frame = spark.sql("SHOW TBLPROPERTIES sc.ns.t")
    assert [(f.name, f.dataType.simpleString()) for f in frame.schema.fields] == [
        ("key", "string"),
        ("value", "string"),
    ]
    assert [f.nullable for f in frame.schema.fields] == [False, False]
    assert _rows(frame) == _fresh_rows("none")


def test_insert_moves_snapshot_cell_to_the_decimal_id(spark: ReparkSession) -> None:
    """D-SHOW-TBLPROPERTIES — after insert the snapshot cell is the id. pins: tblprops-1/C-002."""
    spark.sql("CREATE TABLE sc.ns.t (id BIGINT) USING iceberg TBLPROPERTIES ('k'='v')")
    spark.sql("INSERT INTO sc.ns.t VALUES (1)")
    shown = _rows(spark.sql("SHOW TBLPROPERTIES sc.ns.t"))
    assert shown[0][0] == "current-snapshot-id"
    assert shown[0][1] != "none"
    assert shown == _fresh_rows(shown[0][1])


def test_set_tblproperties_updates_format_and_adds_rows(spark: ReparkSession) -> None:
    """D-SHOW-TBLPROPERTIES — SET adds rows and retargets format. pins: tblprops-1/C-003."""
    spark.sql("CREATE TABLE sc.ns.t (id BIGINT) USING iceberg TBLPROPERTIES ('k'='v')")
    spark.sql("INSERT INTO sc.ns.t VALUES (1)")
    snapshot = _rows(spark.sql("SHOW TBLPROPERTIES sc.ns.t"))[0][1]
    spark.sql("ALTER TABLE sc.ns.t SET TBLPROPERTIES ('k2'='v2', 'write.format.default'='orc')")
    assert _rows(spark.sql("SHOW TBLPROPERTIES sc.ns.t")) == [
        ["current-snapshot-id", snapshot],
        ["format", "iceberg/orc"],
        ["format-version", "2"],
        ["k", "v"],
        ["k2", "v2"],
        ["write.format.default", "orc"],
        ["write.parquet.compression-codec", "zstd"],
    ]


def test_v1_table_reports_format_version_once(spark: ReparkSession) -> None:
    """D-SHOW-TBLPROPERTIES — a v1 table reports version 1 once. pins: tblprops-1/C-004."""
    spark.sql("CREATE TABLE sc.ns.v1 (id INT) USING iceberg TBLPROPERTIES ('format-version'='1')")
    assert _rows(spark.sql("SHOW TBLPROPERTIES sc.ns.v1")) == [
        ["current-snapshot-id", "none"],
        ["format", "iceberg/parquet"],
        ["format-version", "1"],
        ["write.parquet.compression-codec", "zstd"],
    ]


def test_partitioned_table_hides_comment(spark: ReparkSession) -> None:
    """D-SHOW-TBLPROPERTIES — comment is stored but never a row. pins: tblprops-1/C-005."""
    spark.sql(
        "CREATE TABLE sc.ns.p (id INT, c STRING) USING iceberg PARTITIONED BY (c) "
        "TBLPROPERTIES ('write.parquet.compression-codec'='zstd', 'comment'='hello')"
    )
    assert _rows(spark.sql("SHOW TBLPROPERTIES sc.ns.p")) == [
        ["current-snapshot-id", "none"],
        ["format", "iceberg/parquet"],
        ["format-version", "2"],
        ["write.parquet.compression-codec", "zstd"],
    ]
    assert _rows(spark.sql("SHOW TBLPROPERTIES sc.ns.p ('comment')")) == [
        ["comment", "Table sc.ns.p does not have property: comment"]
    ]


def test_owner_is_never_a_row(spark: ReparkSession) -> None:
    """D-SHOW-TBLPROPERTIES — RePark's owner stamp is never a row. pins: tblprops-1/C-005."""
    spark.sql("CREATE TABLE sc.ns.t (id BIGINT) USING iceberg TBLPROPERTIES ('k'='v')")
    assert _rows(spark.sql("SHOW TBLPROPERTIES sc.ns.t")) == _fresh_rows("none")


def test_stored_compression_codec_wins_over_the_default(spark: ReparkSession) -> None:
    """D-SHOW-TBLPROPERTIES — a stored codec beats the zstd default. pins: tblprops-1/C-011."""
    spark.sql("CREATE TABLE sc.ns.t (id BIGINT) USING iceberg TBLPROPERTIES ('k'='v')")
    spark.sql("ALTER TABLE sc.ns.t SET TBLPROPERTIES ('write.parquet.compression-codec'='snappy')")
    assert _rows(spark.sql("SHOW TBLPROPERTIES sc.ns.t")) == [
        ["current-snapshot-id", "none"],
        ["format", "iceberg/parquet"],
        ["format-version", "2"],
        ["k", "v"],
        ["write.parquet.compression-codec", "snappy"],
    ]


def test_unset_compression_codec_falls_back_to_the_default(spark: ReparkSession) -> None:
    """D-SHOW-TBLPROPERTIES — after UNSET the zstd default returns. pins: tblprops-1/C-011."""
    spark.sql("CREATE TABLE sc.ns.t (id BIGINT) USING iceberg TBLPROPERTIES ('k'='v')")
    spark.sql("ALTER TABLE sc.ns.t UNSET TBLPROPERTIES ('write.parquet.compression-codec')")
    assert _rows(spark.sql("SHOW TBLPROPERTIES sc.ns.t")) == _fresh_rows("none")


def test_keyed_lookup_hits_value_and_builtins(spark: ReparkSession) -> None:
    """D-SHOW-TBLPROPERTIES-KEY — keyed hits answer one row. pins: tblprops-1/C-006."""
    spark.sql("CREATE TABLE sc.ns.t (id BIGINT) USING iceberg TBLPROPERTIES ('k'='v')")
    assert _rows(spark.sql("SHOW TBLPROPERTIES sc.ns.t ('k')")) == [["k", "v"]]
    assert _rows(spark.sql("SHOW TBLPROPERTIES sc.ns.t ('format-version')")) == [
        ["format-version", "2"]
    ]
    assert _rows(spark.sql("SHOW TBLPROPERTIES sc.ns.t ('current-snapshot-id')")) == [
        ["current-snapshot-id", "none"]
    ]


def test_keyed_lookup_misses_loudly_and_case_sensitively(spark: ReparkSession) -> None:
    """D-SHOW-TBLPROPERTIES-KEY — misses carry Spark's text. pins: tblprops-1/C-006."""
    spark.sql("CREATE TABLE sc.ns.t (id BIGINT) USING iceberg TBLPROPERTIES ('k'='v')")
    assert _rows(spark.sql("SHOW TBLPROPERTIES sc.ns.t ('nope')")) == [
        ["nope", "Table sc.ns.t does not have property: nope"]
    ]
    assert _rows(spark.sql("SHOW TBLPROPERTIES sc.ns.t ('K')")) == [
        ["K", "Table sc.ns.t does not have property: K"]
    ]


def test_two_part_and_one_part_names_answer_after_use(spark: ReparkSession) -> None:
    """D-SHOW-TBLPROPERTIES — shorter names resolve after USE. pins: tblprops-1/C-007."""
    spark.sql("CREATE TABLE sc.ns.t (id BIGINT) USING iceberg TBLPROPERTIES ('k'='v')")
    spark.sql("INSERT INTO sc.ns.t VALUES (1)")
    spark.sql("ALTER TABLE sc.ns.t SET TBLPROPERTIES ('k2'='v2', 'write.format.default'='orc')")
    expected = _rows(spark.sql("SHOW TBLPROPERTIES sc.ns.t"))
    spark.sql("USE sc.ns")
    assert _rows(spark.sql("SHOW TBLPROPERTIES ns.t")) == expected
    assert _rows(spark.sql("SHOW TBLPROPERTIES t")) == expected


def test_temp_view_answers_the_empty_frame(spark: ReparkSession) -> None:
    """D-SHOW-TBLPROPERTIES — a temporary view answers no rows. pins: tblprops-1/C-008."""
    spark.sql("CREATE TEMPORARY VIEW tv AS SELECT 1 AS id")
    frame = spark.sql("SHOW TBLPROPERTIES tv")
    assert [(f.name, f.dataType.simpleString()) for f in frame.schema.fields] == [
        ("key", "string"),
        ("value", "string"),
    ]
    assert _rows(frame) == []


def test_missing_table_keeps_the_not_found_refusal(spark: ReparkSession) -> None:
    """D-SHOW-TBLPROPERTIES — a missing table refuses not-found. pins: tblprops-1/C-009."""
    spark.sql("CREATE TABLE sc.ns.t (id BIGINT) USING iceberg TBLPROPERTIES ('k'='v')")
    for name in ["nope", "T"]:
        with pytest.raises(AnalysisException) as caught:
            spark.sql(f"SHOW TBLPROPERTIES sc.ns.{name}").collect()
        assert caught.value.getCondition() == "TABLE_OR_VIEW_NOT_FOUND"
        assert caught.value.getSqlState() == "42P01"
        assert f"`sc`.`ns`.`{name}`" in str(caught.value)
        assert "information_schema" not in str(caught.value)


def test_fallthrough_text_is_unreachable(spark: ReparkSession) -> None:
    """Every SHOW TBLPROPERTIES arm answers without the DataFusion text. pins: tblprops-1/C-010."""
    spark.sql("CREATE TABLE sc.ns.t (id BIGINT) USING iceberg TBLPROPERTIES ('k'='v')")
    spark.sql("CREATE VIEW sc.ns.v AS SELECT 1 AS id")
    spark.sql("CREATE TEMPORARY VIEW tv AS SELECT 1 AS id")
    spark.sql("SHOW TBLPROPERTIES sc.ns.t").collect()
    spark.sql("SHOW TBLPROPERTIES sc.ns.t ('k')").collect()
    spark.sql("SHOW TBLPROPERTIES sc.ns.v").collect()
    spark.sql("SHOW TBLPROPERTIES tv").collect()
    with pytest.raises(AnalysisException) as caught:
        spark.sql("SHOW TBLPROPERTIES sc.ns.nope").collect()
    assert "information_schema" not in str(caught.value)
