"""DF-METADATA-COL-1 — `DataFrame.metadataColumn` and the hidden `_metadata` pins.

Every pin is driven by a live PySpark 4.1.2 cell (ANSI on, UTC) from
``facade_df_metadata_col_oracle.json`` (34 run-18b cells plus the 14 run-16b
``metadata_*`` cells); the ``metadata_csv`` skip marker carries no pin.

pins: df-metadata-col-1/M-1, M-2, M-3, M-4, M-5
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkTypeError
from repark.spark import functions as F  # noqa: N812

_ORACLE = json.loads((Path(__file__).parent / "facade_df_metadata_col_oracle.json").read_text())


def _cell(name: str) -> dict:
    """Return the recorded PySpark 4.1.2 oracle cell."""
    return _ORACLE[name]


def _result(name: str):
    """Return the recorded success payload of a cell."""
    return _cell(name)["result"]


def _error(name: str) -> dict:
    """Return the recorded error payload of a cell."""
    return _cell(name)["error"]


@pytest.fixture
def spark() -> ReparkSession:
    """Fresh session per test."""
    session = ReparkSession.builder.appName("pytest-df-metadata-col-1").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def root(spark: ReparkSession, tmp_path: Path) -> str:
    """The probe's five file-source datasets, written through repark like the probe."""
    base = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], "i int, s string")
    place = str(tmp_path)
    base.coalesce(1).write.parquet(f"{place}/p")
    base.coalesce(1).write.option("header", "true").csv(f"{place}/c")
    base.coalesce(1).write.json(f"{place}/j")
    base.select("s").coalesce(1).write.text(f"{place}/t")
    base.coalesce(1).write.partitionBy("s").parquet(f"{place}/pp")
    return place


def _parquet(spark: ReparkSession, root: str):
    """Read back the single-file parquet dataset."""
    return spark.read.parquet(f"{root}/p")


def _assert_condition(error: BaseException, name: str, fragments: list[str] | None = None) -> None:
    """Assert a Spark error class, condition, and stable message fragments."""
    recorded = _error(name)
    assert type(error).__name__ == recorded["raises"]
    assert error.getCondition() == recorded["condition"]
    for fragment in fragments or []:
        assert fragment in str(error)


def test_schema_parquet_shape(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-1 — cell `schema_parquet`."""
    frame = _parquet(spark, root)
    assert repr(frame.select(frame.metadataColumn("_metadata")).schema.simpleString()) == (
        _result("schema_parquet")
    )


def test_schema_partitioned_parquet_shape(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-1 — cell `schema_parquet_partitioned`."""
    frame = spark.read.parquet(f"{root}/pp")
    assert repr(frame.select(frame.metadataColumn("_metadata")).schema.simpleString()) == (
        _result("schema_parquet_partitioned")
    )


def test_schema_csv_shape(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-1 — cell `schema_csv` (no `row_index`)."""
    frame = spark.read.option("header", "true").schema("i int, s string").csv(f"{root}/c")
    assert repr(frame.select(frame.metadataColumn("_metadata")).schema.simpleString()) == (
        _result("schema_csv")
    )


def test_schema_json_shape(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-1 — cell `schema_json` (no `row_index`)."""
    frame = spark.read.schema("i int, s string").json(f"{root}/j")
    assert repr(frame.select(frame.metadataColumn("_metadata")).schema.simpleString()) == (
        _result("schema_json")
    )


def test_schema_text_shape(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-1 — cell `schema_text` (no `row_index`)."""
    frame = spark.read.text(f"{root}/t")
    assert repr(frame.select(frame.metadataColumn("_metadata")).schema.simpleString()) == (
        _result("schema_text")
    )


def test_json_parquet_carries_file_source_metadata(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-1 — cell `json_parquet`."""
    assert repr(_parquet(spark, root).select("_metadata").schema.json()) == (
        _result("json_parquet")
    )


def test_json_partitioned_carries_file_source_metadata(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-1 — cell `json_parquet_partitioned`."""
    frame = spark.read.parquet(f"{root}/pp")
    assert repr(frame.select("_metadata").schema.json()) == (_result("json_parquet_partitioned"))


def test_json_csv_carries_file_source_metadata(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-1 — cell `json_csv`."""
    frame = spark.read.option("header", "true").schema("i int, s string").csv(f"{root}/c")
    assert repr(frame.select("_metadata").schema.json()) == _result("json_csv")


def test_json_json_carries_file_source_metadata(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-1 — cell `json_json`."""
    frame = spark.read.schema("i int, s string").json(f"{root}/j")
    assert repr(frame.select("_metadata").schema.json()) == _result("json_json")


def test_json_text_carries_file_source_metadata(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-1 — cell `json_text`."""
    frame = spark.read.text(f"{root}/t")
    assert repr(frame.select("_metadata").schema.json()) == _result("json_text")


def test_json_full_select_matches_earlier_cell(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-1 — cell `metadata_fields_full`."""
    assert _parquet(spark, root).select("_metadata").schema.json() == (
        _result("metadata_fields_full")
    )


def test_modification_time_select_type(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-1, M-2 — cell `mtime_type`."""
    frame = _parquet(spark, root).select(F.col("_metadata.file_modification_time"))
    assert repr(frame.schema.simpleString()) == _result("mtime_type")
    moment = frame.collect()[0][0]
    assert moment is not None


def test_row_index_select_schema(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-1, M-4 — cell `metadata_row_index`."""
    frame = _parquet(spark, root)
    scoped = frame.select(frame.metadataColumn("_metadata").getField("row_index"))
    assert scoped.schema.simpleString() == _result("metadata_row_index")


def test_string_select_names_field(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-2 — cell `metadata_string_select`."""
    assert _parquet(spark, root).select("_metadata.file_path").schema.simpleString() == (
        _result("metadata_string_select")
    )


def test_select_star_hides_metadata(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-2 — cell `metadata_select_star_hidden`."""
    assert _parquet(spark, root).select("*").columns == _result("metadata_select_star_hidden")


def test_star_plus_fields_names(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-2 — cell `names_parquet`."""
    frame = _parquet(spark, root).select("*", "_metadata.file_name", "_metadata.file_size")
    assert repr(frame.columns) == _result("names_parquet")


@pytest.mark.xfail(strict=True, reason="BACKLOG IO-PARQUET-PARTITION-DISCOVERY-1 (R-18b-10)")
def test_star_plus_fields_names_partitioned(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-2 — cell `names_parquet_partitioned`."""
    frame = spark.read.parquet(f"{root}/pp").select(
        "*", "_metadata.file_name", "_metadata.file_size"
    )
    assert repr(frame.columns) == _result("names_parquet_partitioned")


def test_star_plus_fields_names_csv(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-2 — cell `names_csv`."""
    frame = (
        spark.read.option("header", "true")
        .schema("i int, s string")
        .csv(f"{root}/c")
        .select("*", "_metadata.file_name", "_metadata.file_size")
    )
    assert repr(frame.columns) == _result("names_csv")


def test_star_plus_fields_names_json(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-2 — cell `names_json`."""
    frame = (
        spark.read.schema("i int, s string")
        .json(f"{root}/j")
        .select("*", "_metadata.file_name", "_metadata.file_size")
    )
    assert repr(frame.columns) == _result("names_json")


def test_star_plus_fields_names_text(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-2 — cell `names_text`."""
    frame = spark.read.text(f"{root}/t").select("*", "_metadata.file_name", "_metadata.file_size")
    assert repr(frame.columns) == _result("names_text")


def test_metadata_survives_dropping_select(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-2, M-4 — cell `metadata_col_after_select`."""
    frame = _parquet(spark, root)
    selected = frame.select("i").select(frame.metadataColumn("_metadata"))
    assert repr(selected.schema.simpleString()) == (_result("metadata_col_after_select"))


def test_named_field_survives_dropping_select(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-2 — cell `metadata_col_after_select_named`."""
    assert repr(_parquet(spark, root).select("i").select("_metadata.file_name").columns) == (
        _result("metadata_col_after_select_named")
    )


def test_metadata_survives_filter(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-3, M-4 — cell `metadata_after_filter`."""
    frame = _parquet(spark, root)
    scoped = frame.filter("i > 0").select(frame.metadataColumn("_metadata").getField("file_size"))
    assert scoped.schema.simpleString() == _result("metadata_after_filter")


def test_parquet_values(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-3 — cell `values_parquet` (R-18b-9 residue)."""
    frame = (
        _parquet(spark, root)
        .select("i", "_metadata.file_path", "_metadata.file_block_start", "_metadata.row_index")
        .withColumn("size_positive", F.col("_metadata.file_size") > 0)
        .withColumn("fname_ok", F.col("_metadata.file_name").endswith(".parquet"))
    )
    assert frame.columns == _result("values_parquet")["columns"]
    assert frame.schema.simpleString() == _result("values_parquet")["simpleString"]
    assert [field.nullable for field in frame.schema.fields[:4]] == _result("values_parquet")[
        "nullable"
    ][:4]
    rows = sorted(tuple(row) for row in frame.collect())
    assert [row[0] for row in rows] == [1, 2, 3]
    assert {row[2] for row in rows} == {0}
    assert [row[3] for row in rows] == [0, 1, 2]
    assert {row[4] for row in rows} == {True}
    assert {row[5] for row in rows} == {True}
    paths = {row[1] for row in rows}
    assert len(paths) == 1
    (path,) = paths
    assert path.startswith("file:")
    assert path.endswith(".parquet")
    assert Path(path[len("file:") :]).is_file()


def test_csv_values(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-3 — cell `values_csv` (R-18b-9 residue)."""
    frame = (
        spark.read.option("header", "true")
        .schema("i int, s string")
        .csv(f"{root}/c")
        .select("i", "_metadata.file_block_start", "_metadata.file_block_length")
        .withColumn("path_prefix", F.substring(F.col("_metadata.file_path"), 1, 6))
    )
    assert frame.columns == _result("values_csv")["columns"]
    assert frame.schema.simpleString() == _result("values_csv")["simpleString"]
    assert [field.nullable for field in frame.schema.fields[:3]] == _result("values_csv")[
        "nullable"
    ][:3]
    rows = sorted(tuple(row) for row in frame.collect())
    assert [row[0] for row in rows] == [1, 2, 3]
    assert {row[1] for row in rows} == {0}
    assert {row[3] for row in rows} == {"file:/"}
    lengths = {row[2] for row in rows}
    assert len(lengths) == 1


def test_row_index_values(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-3, M-4 — cell `row_index_parquet`."""
    frame = _parquet(spark, root)
    scoped = frame.select("i", frame.metadataColumn("_metadata").getField("row_index").alias("ri"))
    assert scoped.columns == _result("row_index_parquet")["columns"]
    assert [field.nullable for field in scoped.schema.fields] == _result("row_index_parquet")[
        "nullable"
    ]
    assert sorted(tuple(row) for row in scoped.collect()) == [(1, 0), (2, 1), (3, 2)]


def test_parquet_field_values(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-3, M-4 — cell `metadata_parquet_field`."""
    frame = _parquet(spark, root)
    scoped = frame.select(frame.metadataColumn("_metadata").getField("file_name"))
    assert scoped.schema.simpleString() == _result("metadata_parquet_field")["schema"]
    rows = scoped.collect()
    assert len(rows) == 3
    assert all(name.endswith(".parquet") for (name,) in (tuple(row) for row in rows))


def test_metadata_column_repr(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-4 — cell `metadata_col_repr`."""
    assert repr(_parquet(spark, root).metadataColumn("_metadata")) == _result("metadata_col_repr")


def test_metadata_column_rejects_int(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-4 — cell `metadata_arg_type`."""
    recorded = _error("metadata_arg_type")
    with pytest.raises(PySparkTypeError) as caught:
        _parquet(spark, root).metadataColumn(1)
    assert caught.value.getCondition() == recorded["condition"]
    assert str(caught.value) == recorded["message"]
    assert caught.value.getMessageParameters() == recorded["params"]


def test_metadata_column_rejects_none(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-4 — cell `metadata_arg_none`."""
    recorded = _error("metadata_arg_none")
    with pytest.raises(PySparkTypeError) as caught:
        _parquet(spark, root).metadataColumn(None)
    assert caught.value.getCondition() == recorded["condition"]
    assert str(caught.value) == recorded["message"]


def test_metadata_column_on_local_data(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-4 — cell `metadata_on_local`."""
    frame = spark.createDataFrame([(1, "a")], "i int, s string")
    with pytest.raises(AnalysisException) as caught:
        frame.select(frame.metadataColumn("_metadata")).schema.simpleString()
    _assert_condition(caught.value, "metadata_on_local", ["`_metadata` cannot be resolved."])


def test_metadata_column_on_range(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-4 — cell `metadata_on_range`."""
    frame = spark.range(3)
    with pytest.raises(AnalysisException) as caught:
        frame.select(frame.metadataColumn("_metadata")).schema.simpleString()
    _assert_condition(caught.value, "metadata_on_range", ["`_metadata` cannot be resolved."])


def test_metadata_column_on_sql_values(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-4 — cell `metadata_on_sql_values`."""
    frame = spark.sql("SELECT 1 AS a")
    with pytest.raises(AnalysisException) as caught:
        frame.select(frame.metadataColumn("_metadata")).schema.simpleString()
    _assert_condition(caught.value, "metadata_on_sql_values", ["`_metadata` cannot be resolved."])


def test_metadata_column_unknown_name(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-4 — cell `metadata_missing`."""
    frame = _parquet(spark, root)
    with pytest.raises(AnalysisException) as caught:
        frame.select(frame.metadataColumn("nope")).schema.simpleString()
    _assert_condition(
        caught.value, "metadata_missing", ["`nope` cannot be resolved", "[`_metadata`]"]
    )


def test_metadata_column_unknown_name_repr(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-4 — cell `metadata_missing_lazy`."""
    frame = _parquet(spark, root)
    with pytest.raises(AnalysisException) as caught:
        repr(frame.metadataColumn("nope"))
    _assert_condition(
        caught.value, "metadata_missing_lazy", ["`nope` cannot be resolved", "[`_metadata`]"]
    )


def test_metadata_column_regular_name(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-4 — cell `metadata_regular_col`."""
    frame = _parquet(spark, root)
    with pytest.raises(AnalysisException) as caught:
        frame.select(frame.metadataColumn("i")).schema.simpleString()
    _assert_condition(
        caught.value, "metadata_regular_col", ["`i` cannot be resolved", "[`_metadata`]"]
    )


def test_metadata_column_field_name_select(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-4 — cell `metadata_bad_name_select`."""
    frame = _parquet(spark, root)
    with pytest.raises(AnalysisException) as caught:
        frame.select(frame.metadataColumn("file_path")).schema.simpleString()
    _assert_condition(caught.value, "metadata_bad_name_select", ["`file_path` cannot be resolved"])


def test_metadata_column_field_name_repr(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-4 — cell `metadata_repr_name`."""
    frame = _parquet(spark, root)
    with pytest.raises(AnalysisException) as caught:
        repr(frame.metadataColumn("file_path"))
    _assert_condition(caught.value, "metadata_repr_name", ["`file_path` cannot be resolved"])


def test_metadata_column_after_join(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-4 — cell `metadata_col_after_join`."""
    frame = _parquet(spark, root)
    other = spark.read.option("header", "true").schema("i int, s string").csv(f"{root}/c")
    with pytest.raises(AnalysisException) as caught:
        frame.join(other, "i").select(
            frame.metadataColumn("_metadata").getField("file_name")
        ).schema.simpleString()
    _assert_condition(
        caught.value,
        "metadata_col_after_join",
        ['Resolved attribute(s) "_metadata" missing from'],
    )


def test_metadata_column_after_agg(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-4 — cell `metadata_col_after_agg`."""
    frame = _parquet(spark, root)
    with pytest.raises(AnalysisException) as caught:
        frame.groupBy("s").count().select(frame.metadataColumn("_metadata")).schema.simpleString()
    _assert_condition(
        caught.value,
        "metadata_col_after_agg",
        ['Resolved attribute(s) "_metadata" missing from'],
    )


def test_user_column_shadows_hidden(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-4 — cell `metadata_user_column_shadow`."""
    spark.createDataFrame([(1, "x")], "i int, _metadata string").write.parquet(f"{root}/shadow")
    scoped = spark.read.parquet(f"{root}/shadow").select("_metadata")
    assert scoped.columns == _result("metadata_user_column_shadow")["columns"]
    assert scoped.schema.simpleString() == _result("metadata_user_column_shadow")["simpleString"]
    assert [tuple(row) for row in scoped.collect()] == [("x",)]


def test_metadata_column_refused_under_shadow(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-4 — cell `metadata_user_column_shadow_mc`."""
    spark.createDataFrame([(1, "x")], "i int, _metadata string").write.parquet(f"{root}/shadow")
    frame = spark.read.parquet(f"{root}/shadow")
    with pytest.raises(AnalysisException) as caught:
        frame.select(frame.metadataColumn("_metadata")).schema.simpleString()
    _assert_condition(
        caught.value,
        "metadata_user_column_shadow_mc",
        ['Resolved attribute(s) "__metadata" missing from'],
    )


def test_sql_star_hides_metadata(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-2, M-5 — cell `sql_star_hidden`."""
    _parquet(spark, root).createOrReplaceTempView("pv")
    assert repr(spark.sql("SELECT * FROM pv").columns) == _result("sql_star_hidden")


def test_sql_view_metadata_unresolved(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-5 — cell `sql_view_metadata` (today's door answer)."""
    _parquet(spark, root).createOrReplaceTempView("pv")
    with pytest.raises(AnalysisException) as caught:
        spark.sql("SELECT i, _metadata.file_name LIKE '%.parquet' AS ok FROM pv").collect()
    assert "_metadata" in str(caught.value)
    assert caught.value.getCondition() is None


def test_sql_values_metadata_unresolved(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-5 — cell `sql_local_metadata` (today's door answer)."""
    with pytest.raises(AnalysisException) as caught:
        spark.sql("SELECT _metadata FROM VALUES (1) AS v(a)").collect()
    assert "_metadata" in str(caught.value)
    assert caught.value.getCondition() is None


@pytest.mark.xfail(strict=True, reason="BACKLOG SQL-METADATA-COL-1: no parquet path-table door")
def test_sql_path_metadata(spark: ReparkSession, root: str) -> None:
    """pins: df-metadata-col-1/M-5 — cell `sql_path_metadata`."""
    rows = spark.sql(f"SELECT i, _metadata.row_index AS ri FROM parquet.`{root}/p`").collect()
    assert sorted(tuple(row) for row in rows) == [(1, 0), (2, 1), (3, 2)]
