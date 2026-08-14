"""R1 — CSV/JSON read + write facade pins (Arrow path: value AND type).

Charter: ``briefs/2026-08-09-grok-r20-grouph-census5-slate.md`` TRACK 8 / R1.
Ledger: ``task/r1-read-formats-ledger.md``.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark.session import _reset_active_session_for_tests
from repark.spark.types import IntegerType, StringType, StructField, StructType


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """Isolated session (no AWS)."""
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("pytest-r1-read-formats").getOrCreate()
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _sorted_rows(frame: object) -> list[dict[str, object]]:
    """Collect Arrow rows ordered by first column for stable comparison."""
    table = frame.to_arrow()  # type: ignore[attr-defined]
    rows = table.to_pylist()
    if not rows:
        return rows
    key0 = next(iter(rows[0]))
    return sorted(rows, key=lambda row: (row[key0] is None, row[key0]))


# ==================================================================================================
# Readers — csv / json / format.load
# ==================================================================================================


def test_read_csv_header_and_values(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "t.csv"
    path.write_text("id,name\n1,a\n2,b\n", encoding="utf-8")
    frame = spark.read.csv(str(path), header=True, inferSchema=True)
    rows = _sorted_rows(frame)
    assert rows == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    table = frame.orderBy("id").to_arrow()
    assert table.schema.field("id").type in (pa.int64(), pa.int32())
    assert pa.types.is_string(table.schema.field("name").type) or pa.types.is_large_string(
        table.schema.field("name").type
    )


def test_read_csv_infer_schema_false_all_string(spark: ReparkSession, tmp_path: Path) -> None:
    """Spark default inferSchema=false → all string; repark matches when inferSchema=false."""
    path = tmp_path / "s.csv"
    path.write_text("id,name\n1,a\n", encoding="utf-8")
    frame = spark.read.option("header", "true").option("inferSchema", "false").csv(str(path))
    table = frame.to_arrow()
    assert pa.types.is_string(table.schema.field("id").type) or pa.types.is_large_string(
        table.schema.field("id").type
    )
    assert table.to_pylist() == [{"id": "1", "name": "a"}]


def test_read_csv_schema_positional_no_header(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "n.csv"
    path.write_text("1,a\n2,b\n", encoding="utf-8")
    schema = StructType(
        [StructField("id", IntegerType(), True), StructField("name", StringType(), True)]
    )
    frame = spark.read.schema(schema).csv(str(path), header=False)
    assert _sorted_rows(frame) == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    table = frame.orderBy("id").to_arrow()
    assert table.schema.field("id").type in (pa.int32(), pa.int64())


def test_read_csv_null_value(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "nulls.csv"
    path.write_text("id,name\n1,NA\n2,b\n", encoding="utf-8")
    frame = spark.read.csv(str(path), header=True, nullValue="NA", inferSchema=True)
    rows = _sorted_rows(frame)
    assert rows[0]["name"] is None
    assert rows[1] == {"id": 2, "name": "b"}


def test_read_csv_sep(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "semi.csv"
    path.write_text("id;name\n1;a\n", encoding="utf-8")
    frame = spark.read.csv(str(path), header=True, sep=";", inferSchema=True)
    assert frame.to_arrow().to_pylist() == [{"id": 1, "name": "a"}]


def test_read_csv_format_load_equivalent(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "fmt.csv"
    path.write_text("id,name\n3,c\n", encoding="utf-8")
    via_csv = spark.read.csv(str(path), header=True, inferSchema=True).to_arrow().to_pylist()
    via_fmt = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(str(path))
        .to_arrow()
        .to_pylist()
    )
    assert via_csv == via_fmt == [{"id": 3, "name": "c"}]


def test_read_json_ndjson_flat(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "t.json"
    path.write_text('{"id":1,"name":"a"}\n{"id":2,"name":"b"}\n', encoding="utf-8")
    frame = spark.read.json(str(path))
    rows = _sorted_rows(frame)
    assert rows == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    table = frame.orderBy("id").to_arrow()
    assert table.schema.field("id").type in (pa.int64(), pa.int32())


def test_read_json_nested_struct(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "nested.json"
    path.write_text('{"id":1,"meta":{"k":"v"}}\n', encoding="utf-8")
    frame = spark.read.json(str(path))
    table = frame.to_arrow()
    rows = table.to_pylist()
    assert rows == [{"id": 1, "meta": {"k": "v"}}]
    assert pa.types.is_struct(table.schema.field("meta").type)


def test_read_json_schema_projects(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "proj.json"
    path.write_text('{"id":1,"name":"a","extra":9}\n', encoding="utf-8")
    schema = StructType(
        [StructField("id", IntegerType(), True), StructField("name", StringType(), True)]
    )
    frame = spark.read.schema(schema).json(str(path))
    assert frame.columns == ["id", "name"]
    assert frame.to_arrow().to_pylist() == [{"id": 1, "name": "a"}]


def test_read_json_multiline(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "multi.json"
    path.write_text('[\n  {"id": 1, "name": "a"},\n  {"id": 2, "name": "b"}\n]\n', encoding="utf-8")
    frame = spark.read.json(str(path), multiLine=True)
    assert _sorted_rows(frame) == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


def test_read_unsupported_parse_option_loud(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "x.csv"
    path.write_text("a\n1\n", encoding="utf-8")
    with pytest.raises(AnalysisException, match=r"dateFormat|not supported"):
        spark.read.option("dateFormat", "yyyy").csv(str(path), header=True)


def test_read_mode_failfast_loud(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "m.csv"
    path.write_text("a\n1\n", encoding="utf-8")
    with pytest.raises(AnalysisException, match=r"FAILFAST|not supported"):
        spark.read.csv(str(path), header=True, mode="FAILFAST")


def test_load_orc_still_data_source_not_found(spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException, match="DATA_SOURCE_NOT_FOUND"):
        spark.read.format("orc").load("/tmp/does-not-matter")


# ==================================================================================================
# Writers + round-trips (Arrow value AND type)
# ==================================================================================================


def test_write_csv_round_trip(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "out_csv"
    source = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    source.write.mode("overwrite").csv(str(path), header=True)
    loaded = spark.read.csv(str(path), header=True, inferSchema=True)
    assert _sorted_rows(loaded) == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    table = loaded.orderBy("id").to_arrow()
    assert table.schema.field("id").type in (pa.int64(), pa.int32())
    assert pa.types.is_string(table.schema.field("name").type) or pa.types.is_large_string(
        table.schema.field("name").type
    )


def test_write_json_round_trip_flat(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "out_json"
    source = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    source.write.mode("overwrite").json(str(path))
    loaded = spark.read.json(str(path))
    assert _sorted_rows(loaded) == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    table = loaded.orderBy("id").to_arrow()
    assert table.schema.field("id").type in (pa.int64(), pa.int32())


def test_write_json_round_trip_nested(spark: ReparkSession, tmp_path: Path) -> None:
    """Nested struct survives JSON write→read on the Arrow path."""
    path = tmp_path / "nested_out"
    # Build nested via SQL so types are engine-native.
    source = spark.sql(
        "SELECT 1 AS id, named_struct('k', 'v') AS meta UNION ALL SELECT 2, named_struct('k', 'w')"
    )
    source.write.mode("overwrite").json(str(path))
    loaded = spark.read.json(str(path))
    rows = _sorted_rows(loaded)
    assert rows == [{"id": 1, "meta": {"k": "v"}}, {"id": 2, "meta": {"k": "w"}}]
    table = loaded.orderBy("id").to_arrow()
    assert pa.types.is_struct(table.schema.field("meta").type)


def test_write_format_json_save_builder(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "builder_json"
    spark.createDataFrame([(9, "z")], ["id", "name"]).write.mode("overwrite").format("json").save(
        str(path)
    )
    assert spark.read.format("json").load(str(path)).to_arrow().to_pylist() == [
        {"id": 9, "name": "z"}
    ]


def test_write_csv_null_value_round_trip(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "null_csv"
    source = spark.createDataFrame([(1, None), (2, "b")], ["id", "name"])
    source.write.mode("overwrite").option("header", "true").option("nullValue", "NA").csv(str(path))
    # Raw file should contain the null token.
    text = "\n".join(p.read_text(encoding="utf-8") for p in path.rglob("*.csv"))
    assert "NA" in text
    loaded = spark.read.csv(str(path), header=True, nullValue="NA", inferSchema=True)
    rows = _sorted_rows(loaded)
    assert rows[0]["name"] is None
    assert rows[1]["name"] == "b"


def test_write_csv_error_mode_on_existing(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "exists_csv"
    spark.createDataFrame([(1, "a")], ["id", "name"]).write.mode("overwrite").csv(str(path))
    with pytest.raises(AnalysisException, match=r"PATH_ALREADY_EXISTS|already exists"):
        spark.createDataFrame([(2, "b")], ["id", "name"]).write.mode("error").csv(str(path))


def test_write_orc_still_data_source_not_found(spark: ReparkSession, tmp_path: Path) -> None:
    with pytest.raises(AnalysisException, match="DATA_SOURCE_NOT_FOUND") as raised:
        spark.createDataFrame([(1,)], ["id"]).write.format("orc").save(str(tmp_path / "o"))
    assert "orc" in str(raised.value).lower()


def test_write_empty_csv_overwrite_preserves_path(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "empty_csv"
    spark.createDataFrame([(1, "a")], ["id", "name"]).write.mode("overwrite").csv(
        str(path), header=True
    )
    empty = spark.createDataFrame([], schema="id INT, name STRING")
    empty.write.mode("overwrite").csv(str(path), header=True)
    assert path.exists()
    # Header-only / schema-carrying part — read back zero data rows with infer off.
    loaded = spark.read.csv(str(path), header=True, inferSchema=False)
    assert loaded.count() == 0


# ==================================================================================================
# Critic-octo pins (R1-C1 … C5)
# ==================================================================================================


def test_read_csv_null_value_numeric_column(spark: ReparkSession, tmp_path: Path) -> None:
    """nullValue must null a numeric-inferred column (not fail parse) — R1-C1-001."""
    path = tmp_path / "num_null.csv"
    path.write_text("id,val\n1,NA\n2,3\n", encoding="utf-8")
    frame = spark.read.csv(str(path), header=True, nullValue="NA", inferSchema=True)
    rows = _sorted_rows(frame)
    assert rows == [{"id": 1, "val": None}, {"id": 2, "val": 3}]
    table = frame.orderBy("id").to_arrow()
    assert table.schema.field("id").type in (pa.int64(), pa.int32())
    assert table.schema.field("val").type in (pa.int64(), pa.int32())


def test_read_csv_default_no_header_c_names(spark: ReparkSession, tmp_path: Path) -> None:
    """Spark default inferSchema=false still renames generic cols to _cN — R1-C1-002."""
    path = tmp_path / "noh.csv"
    path.write_text("1,a\n2,b\n", encoding="utf-8")
    frame = spark.read.csv(str(path))
    assert frame.columns == ["_c0", "_c1"]
    table = frame.to_arrow()
    assert table.to_pylist() == [{"_c0": "1", "_c1": "a"}, {"_c0": "2", "_c1": "b"}]
    assert pa.types.is_string(table.schema.field("_c0").type) or pa.types.is_large_string(
        table.schema.field("_c0").type
    )


def test_write_empty_csv_honors_sep(spark: ReparkSession, tmp_path: Path) -> None:
    """Empty CSV materialize must use sep/delimiter (R1-C1-003)."""
    path = tmp_path / "empty_sep"
    empty = spark.createDataFrame([], schema="id INT, name STRING")
    empty.write.mode("overwrite").option("sep", ";").csv(str(path), header=True)
    texts = [part.read_text(encoding="utf-8") for part in path.rglob("*.csv")]
    assert texts, "expected schema-carrying part file"
    assert any(text.startswith("id;name") for text in texts)


def test_write_read_csv_gzip_round_trip(spark: ReparkSession, tmp_path: Path) -> None:
    """compression=gzip write+read directory round-trip (R1-C1-004)."""
    path = tmp_path / "gz_out"
    spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"]).write.mode("overwrite").option(
        "compression", "gzip"
    ).csv(str(path), header=True)
    assert list(path.rglob("*.csv.gz")) or list(path.rglob("*.gz"))
    loaded = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("compression", "gzip")
        .csv(str(path))
    )
    assert _sorted_rows(loaded) == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


def test_read_json_multiline_single_object_loud(spark: ReparkSession, tmp_path: Path) -> None:
    """multiLine single object must not silent-empty (R1-C1-005)."""
    path = tmp_path / "pretty.json"
    path.write_text('{\n  "id": 1,\n  "name": "a"\n}\n', encoding="utf-8")
    with pytest.raises(AnalysisException, match=r"multiLine|JSON array|empty schema"):
        spark.read.json(str(path), multiLine=True).to_arrow()


def test_read_csv_infer_schema_invalid_bool_loud(spark: ReparkSession, tmp_path: Path) -> None:
    """Invalid boolean option spellings fail loud (R1-C1-006)."""
    path = tmp_path / "b.csv"
    path.write_text("id\n1\n", encoding="utf-8")
    with pytest.raises(AnalysisException, match=r"boolean|inferSchema"):
        spark.read.option("header", "true").option("inferSchema", "maybe").csv(str(path))


def test_write_csv_partition_by_wires_hive_dirs(spark: ReparkSession, tmp_path: Path) -> None:
    """partitionBy path csv wires hive-style dirs (R1-C2-001 → R2 wire; no silent ignore)."""
    path = tmp_path / "pb"
    spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"]).write.mode("overwrite").partitionBy(
        "id"
    ).csv(str(path), header=True)
    # Hive-style layout; partition col omitted from data files (Spark shape).
    assert (path / "id=1").is_dir()
    assert (path / "id=2").is_dir()
    texts = [part.read_text(encoding="utf-8") for part in path.rglob("*.csv")]
    assert texts
    assert all("name" in text.splitlines()[0] for text in texts if text.strip())
    assert all("id" not in text.splitlines()[0].split(",") for text in texts if text.strip())


def test_read_json_schema_null_fills_missing(spark: ReparkSession, tmp_path: Path) -> None:
    """User schema fields absent from JSON become null columns (R1-C2-002)."""
    path = tmp_path / "miss.json"
    path.write_text('{"id":1}\n', encoding="utf-8")
    schema = StructType(
        [StructField("id", IntegerType(), True), StructField("name", StringType(), True)]
    )
    frame = spark.read.schema(schema).json(str(path))
    assert frame.columns == ["id", "name"]
    assert frame.to_arrow().to_pylist() == [{"id": 1, "name": None}]


def test_read_csv_semantic_options_loud_on_shorthand(spark: ReparkSession, tmp_path: Path) -> None:
    """pathGlobFilter / ignoreCorruptFiles / mergeSchema fail on .csv() too (R1-C3-001)."""
    path = tmp_path / "s.csv"
    path.write_text("a\n1\n", encoding="utf-8")
    for key, value in (
        ("pathGlobFilter", "*.csv"),
        ("ignoreCorruptFiles", "true"),
        ("mergeSchema", "true"),
    ):
        with pytest.raises(AnalysisException, match=r"not supported|silently change"):
            spark.read.option(key, value).csv(str(path), header=True)


def test_read_json_multiline_empty_array_ok(spark: ReparkSession, tmp_path: Path) -> None:
    """Empty JSON array under multiLine is zero rows, not a mismatch error (R1-C5-001)."""
    path = tmp_path / "empty_arr.json"
    path.write_text("[]\n", encoding="utf-8")
    frame = spark.read.json(str(path), multiLine=True)
    assert frame.count() == 0
