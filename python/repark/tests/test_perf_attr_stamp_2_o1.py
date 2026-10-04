from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession, _native
from repark.spark import column_fields, functions

_ATTR_KEY = b"repark.attr"
_ROOT = Path(__file__).resolve().parents[3]
_ID_READERS = {
    "crates/repark-core/src/orc_footer.rs",
    "crates/repark-core/src/session/df_guards/attr_id.rs",
    "crates/repark-core/src/session/df_guards/attr_lineage.rs",
    "crates/repark-core/src/session/df_guards/sort_names.rs",
}


@pytest.fixture
def spark() -> ReparkSession:
    return ReparkSession.builder.appName("pytest-perf-attr-stamp-2-o1").getOrCreate()


def _ids(frame: Any) -> list[str | None]:
    return list(_native.attribute_ids(frame._inner))


def _rows(frame: Any) -> list[tuple[Any, ...]]:
    return sorted(tuple(row) for row in frame.collect())


def _assert_clean(schema: Any) -> None:
    assert _ATTR_KEY not in (schema.metadata or {})
    for field in schema:
        assert (field.metadata or {}).get(_ATTR_KEY) is None


def test_executing_doors_leave_the_stamped_handle_bindable(
    spark: ReparkSession, capsys: pytest.CaptureFixture[str]
) -> None:
    d = spark.createDataFrame([(1, 10), (2, 20)], ["id", "v"])
    e = spark.createDataFrame([(1, 100), (3, 300)], ["id", "w"])
    cases = [
        (d, [(10,), (20,)]),
        (d.groupBy("id").agg(functions.sum("v").alias("v")), [(10,), (20,)]),
        (d.join(e, d["id"] == e["id"]).select(d["id"], d["v"], e["w"]), [(10,)]),
    ]
    for frame, expected in cases:
        held = _ids(frame)
        assert held
        assert all(attr_id is not None for attr_id in held)
        frame.collect()
        assert frame.count() == len(expected)
        frame.show()
        frame.explain()
        frame.toPandas()
        assert frame.columns
        assert frame.schema.fields
        assert _ids(frame) == held
        assert _rows(frame.select(frame["v"])) == expected
    capsys.readouterr()


def test_the_native_export_schema_is_clean_without_a_facade_strip(spark: ReparkSession) -> None:
    assert not hasattr(column_fields, "_strip_attribute_id_metadata")
    d = spark.createDataFrame([(1, 10), (2, 20)], ["id", "v"])
    e = spark.createDataFrame([(1, 100), (3, 300)], ["id", "w"])
    joined = d.join(e, d["id"] == e["id"]).select(d["v"], e["w"])
    for frame in (d, joined, d.groupBy("id").agg(functions.max("v").alias("m"))):
        assert any(attr_id is not None for attr_id in _ids(frame))
        _assert_clean(pa.table(frame).schema)
        _assert_clean(pa.table(frame._inner).schema)
        reader = pa.RecordBatchReader.from_stream(frame._inner)
        _assert_clean(reader.schema)
        for batch in reader:
            _assert_clean(batch.schema)
        for batch in frame.to_arrow_batches():
            _assert_clean(batch.schema)
    assert _rows(joined) == [(10, 100)]


def test_temp_views_and_sql_frames_keep_the_source_ids(spark: ReparkSession) -> None:
    d = spark.createDataFrame([(1, 10), (2, 20)], ["id", "v"])
    d.createOrReplaceTempView("o1_tv")
    source = _ids(d)
    assert _ids(spark.table("o1_tv")) == source
    assert _ids(spark.table("o1_tv")) == source
    assert _ids(spark.sql("SELECT * FROM o1_tv")) == source
    assert _ids(spark.sql("SELECT v FROM o1_tv")) == [source[1]]
    assert _rows(spark.table("o1_tv").select(d["v"])) == [(10,), (20,)]
    assert _rows(spark.sql("SELECT * FROM o1_tv").select(d["v"])) == [(10,), (20,)]
    d2 = d.withColumn("w", functions.lit(1))
    d2.createOrReplaceTempView("o1_tv2")
    assert _ids(spark.table("o1_tv2")) == _ids(d2)
    assert _rows(spark.table("o1_tv2").select(d["v"], d2["w"])) == [(10, 1), (20, 1)]
    d.createOrReplaceTempView("o1_tv")
    assert _ids(spark.table("o1_tv")) == source
    assert _rows(spark.table("o1_tv").select(d["v"])) == [(10,), (20,)]
    e = spark.createDataFrame([(1, 10), (2, 20)], ["id", "v"])
    e.createOrReplaceTempView("o1_tv")
    assert _ids(spark.table("o1_tv")) == _ids(e)
    assert _rows(spark.table("o1_tv").select(e["v"])) == [(10,), (20,)]
    f = d.filter(d["id"] > 1)
    f.createOrReplaceTempView("o1_tf")
    assert _ids(spark.table("o1_tf")) == _ids(f)
    assert _rows(spark.table("o1_tf").select(d["v"])) == [(20,)]


def test_condition_and_cross_joins_over_frames_and_views_answer(spark: ReparkSession) -> None:
    a = spark.createDataFrame([(1, 10), (2, 20)], ["id", "v"])
    b = spark.createDataFrame([(1, 100), (3, 300)], ["id", "w"])
    assert _rows(a.join(b, a["id"] == b["id"])) == [(1, 10, 1, 100)]
    assert _rows(a.join(b, a["id"] == b["id"]).select(a["v"], b["w"])) == [(10, 100)]
    assert _rows(a.crossJoin(b)) == [
        (1, 10, 1, 100),
        (1, 10, 3, 300),
        (2, 20, 1, 100),
        (2, 20, 3, 300),
    ]
    a.createOrReplaceTempView("o1_ja")
    t = spark.table("o1_ja")
    assert _rows(a.join(t, a["id"] == t["id"])) == [(1, 10, 1, 10), (2, 20, 2, 20)]
    t1 = spark.table("o1_ja")
    t2 = spark.table("o1_ja")
    assert _rows(t1.join(t2, t1["id"] == t2["id"])) == [(1, 10, 1, 10), (2, 20, 2, 20)]
    q = spark.sql("SELECT * FROM o1_ja")
    c = spark.createDataFrame([(1, 10), (2, 20)], ["id", "v"])
    assert _rows(q.join(c, q["id"] == c["id"]).select(q["v"])) == [(10,), (20,)]


def test_no_new_reader_of_the_attribute_key() -> None:
    pattern = re.compile(r"repark\.attr|ATTR_KEY")
    sources = list((_ROOT / "crates").glob("*/src/**/*.rs"))
    sources += list((_ROOT / "python" / "repark" / "src").rglob("*.py"))
    readers = set()
    for path in sources:
        relative = path.relative_to(_ROOT).as_posix()
        if "/tests/" in relative or relative.endswith(("/tests.rs", "_tests.rs")):
            continue
        if pattern.search(path.read_text(encoding="utf-8")):
            readers.add(relative)
    assert readers == _ID_READERS
