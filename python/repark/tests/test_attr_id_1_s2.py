from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession, _native
from repark.spark import functions

_ATTR_KEY = b"repark.attr"


@pytest.fixture
def spark() -> ReparkSession:
    return ReparkSession.builder.appName("pytest-attr-id-1-s2").getOrCreate()


@pytest.fixture
def ice_spark(tmp_path: Path) -> tuple[ReparkSession, Path]:
    session = ReparkSession.builder.appName("pytest-attr-id-1-s2-ice").getOrCreate()
    warehouse = tmp_path / "wh"
    session.register_memory_catalog("sc", str(warehouse))
    session.sql("CREATE NAMESPACE sc.u7")
    return session, warehouse


def _frame(spark: ReparkSession) -> Any:
    return spark.createDataFrame([(1, "a"), (2, "b")], "id BIGINT, data STRING")


def _ids(frame: Any) -> list[str | None]:
    return list(_native.attribute_ids(frame._inner))


def _assert_footer_clean(part: Path) -> None:
    import pyarrow.parquet as parquet

    schema = parquet.read_schema(part)
    for field in schema:
        assert (field.metadata or {}).get(_ATTR_KEY) is None
    file_metadata = parquet.ParquetFile(part).metadata.metadata
    assert not file_metadata or _ATTR_KEY not in file_metadata


def test_every_spawned_frame_carries_an_id_on_every_output_field(
    spark: ReparkSession, tmp_path: Path
) -> None:
    base = _frame(spark)
    assert _ids(base) == [base["id"]._attr_id, base["data"]._attr_id]
    assert all(id is not None for id in _ids(base))
    assert _ids(base)[0] != _ids(base)[1]
    via_sql = spark.sql("SELECT id, data FROM (SELECT 1 AS id, 'a' AS data)")
    assert all(id is not None for id in _ids(via_sql))
    assert all(id is not None for id in _ids(base.select("id", "data")))
    assert all(id is not None for id in _ids(base.filter("id > 0")))
    assert all(id is not None for id in _ids(base.withColumn("k", functions.lit(1))))
    other = _frame(spark).withColumnRenamed("data", "other_data")
    assert all(id is not None for id in _ids(base.join(other, on="id")))
    out = tmp_path / "stamped.parquet"
    base.write.parquet(str(out))
    reread = spark.read.parquet(str(out))
    assert all(id is not None for id in _ids(reread))


def test_bind_sites_set_the_column_attribute_id(spark: ReparkSession) -> None:
    frame = _frame(spark)
    held = _ids(frame)
    assert frame["id"]._attr_id == held[0]
    assert frame["data"]._attr_id == held[1]
    assert frame.id._attr_id == held[0]
    assert frame._column_of("data")._attr_id == held[1]
    assert functions.col("id")._attr_id is None


def test_written_parquet_footer_carries_no_attribute_key(
    spark: ReparkSession, tmp_path: Path
) -> None:
    out = tmp_path / "clean.parquet"
    _frame(spark).write.parquet(str(out))
    parts = sorted(out.rglob("*.parquet"))
    assert parts
    for part in parts:
        _assert_footer_clean(part)
    assert [list(row) for row in spark.read.parquet(str(out)).collect()] == [
        [1, "a"],
        [2, "b"],
    ]


def test_written_csv_and_json_bytes_carry_no_attribute_key(
    spark: ReparkSession, tmp_path: Path
) -> None:
    csv_out = tmp_path / "clean.csv"
    _frame(spark).write.csv(str(csv_out))
    csv_parts = [part for part in sorted(csv_out.rglob("*")) if part.is_file()]
    assert csv_parts
    for part in csv_parts:
        assert _ATTR_KEY not in part.read_bytes()
    json_out = tmp_path / "clean.json"
    _frame(spark).write.json(str(json_out))
    json_parts = [part for part in sorted(json_out.rglob("*")) if part.is_file()]
    assert json_parts
    for part in json_parts:
        assert _ATTR_KEY not in part.read_bytes()


def test_iceberg_stored_schema_and_data_files_carry_no_attribute_key(
    ice_spark: tuple[ReparkSession, Path],
) -> None:
    spark, warehouse = ice_spark
    spark.sql("DROP TABLE IF EXISTS sc.u7.t")
    _frame(spark).write.saveAsTable("sc.u7.t")
    assert [list(row) for row in spark.sql("SELECT * FROM sc.u7.t ORDER BY id").collect()] == [
        [1, "a"],
        [2, "b"],
    ]
    metadata_files = sorted(warehouse.rglob("*.metadata.json"))
    assert metadata_files
    for metadata_file in metadata_files:
        assert "repark.attr" not in metadata_file.read_text(encoding="utf-8")
    parts = sorted(warehouse.rglob("*.parquet"))
    assert parts
    for part in parts:
        _assert_footer_clean(part)


def test_merge_data_files_carry_no_attribute_key(
    ice_spark: tuple[ReparkSession, Path],
) -> None:
    spark, warehouse = ice_spark
    spark.sql("DROP TABLE IF EXISTS sc.u7.m")
    spark.sql("CREATE TABLE sc.u7.m (id BIGINT, data STRING) USING iceberg")
    spark.sql("INSERT INTO sc.u7.m VALUES (1, 'a'), (2, 'b')")
    source = spark.createDataFrame([(2, "B"), (3, "c")], "id BIGINT, data STRING")
    (
        source.mergeInto("sc.u7.m", "id")
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .merge()
    )
    assert [list(row) for row in spark.sql("SELECT * FROM sc.u7.m ORDER BY id").collect()] == [
        [1, "a"],
        [2, "B"],
        [3, "c"],
    ]
    parts = sorted(warehouse.rglob("*.parquet"))
    assert parts
    for part in parts:
        _assert_footer_clean(part)


def test_arrow_exports_carry_no_attribute_key(
    spark: ReparkSession, capsys: pytest.CaptureFixture[str]
) -> None:
    frame = _frame(spark)
    assert _ATTR_KEY not in str(frame.to_arrow().schema.metadata)
    for field in frame.to_arrow().schema:
        assert (field.metadata or {}).get(_ATTR_KEY) is None
    for field in frame.toArrow().schema:
        assert (field.metadata or {}).get(_ATTR_KEY) is None
    for batch in frame.to_arrow_batches():
        for field in batch.schema:
            assert (field.metadata or {}).get(_ATTR_KEY) is None
    for struct_field in frame.schema.fields:
        assert "repark.attr" not in (struct_field.metadata or {})
    frame.printSchema()
    printed = capsys.readouterr().out
    assert "repark.attr" not in printed
    assert "repark.attr" not in repr(frame)
    assert [list(row) for row in frame.collect()] == [[1, "a"], [2, "b"]]


def test_using_self_join_re_mints_colliding_non_key_ids(spark: ReparkSession) -> None:
    frame = _frame(spark)
    held = _ids(frame)
    joined = frame.join(frame, on="id")
    after = _native.attribute_ids(joined._plan())
    assert len(after) == 3
    assert after[0] == held[0]
    assert after[1] == held[1]
    assert after[2] is not None and after[2] not in held


def test_cache_keeps_frames_bindable(spark: ReparkSession) -> None:
    frame = _frame(spark).cache()
    assert frame.count() == 2
    assert frame["id"]._attr_id is not None
    assert all(id is not None for id in _ids(frame))


def test_statement_frames_bind_without_an_attribute_id(spark: ReparkSession) -> None:
    explained = spark.sql("EXPLAIN SELECT 1 AS id")
    assert explained["plan"]._attr_id is None
    assert explained.collect()
