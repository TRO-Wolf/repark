"""IO-TEXT-1 — DataFrameReader.text and DataFrameWriter.text against the oracle.

Oracle cells live in ``facade_reader_writer_oracle.json`` (recorded on live
PySpark 4.1.2 by the orchestrator, run 15b). Reads pin columns plus schema
simpleString plus Row reprs; writes pin file bytes and Spark's own error text;
the SQL door and the gzip refusal pin today's answers.

pins: io-text-1/C-001, C-002, C-003
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark.session import _reset_active_session_for_tests

CELLS: dict[str, Any] = json.loads(
    Path(__file__).with_name("facade_reader_writer_oracle.json").read_text()
)["cells"]


def _cell(name: str) -> dict[str, Any]:
    return CELLS[name]


def _frame_pin(frame: Any, cell: str) -> None:
    expected = _cell(cell)["result"]
    assert frame.columns == expected["columns"]
    assert frame.schema.simpleString() == expected["schema"]
    assert [repr(row) for row in frame.collect()] == expected["rows"]


@pytest.fixture
def spark() -> Any:
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("test-io-text-1").getOrCreate()
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _kv(spark: ReparkSession) -> Any:
    return spark.createDataFrame([("x", 1, 2), ("y", 3, 4)], "key string, a int, b int")


def test_text_read_file(spark: ReparkSession, tmp_path: Path) -> None:
    """cell io/text_read_file — universal newlines, empty line kept. pins: io-text-1/C-001"""
    target = tmp_path / "t6.txt"
    target.write_text("x\n\ny\r\nz", encoding="utf-8")
    _frame_pin(spark.read.text(str(target)), "text_read_file")


def test_text_linesep(spark: ReparkSession, tmp_path: Path) -> None:
    """cell io/text_linesep — custom separator only. pins: io-text-1/C-001"""
    target = tmp_path / "t5.txt"
    target.write_text("a;b;c", encoding="utf-8")
    _frame_pin(spark.read.text(str(target), lineSep=";"), "text_linesep")


def test_text_wholetext(spark: ReparkSession, tmp_path: Path) -> None:
    """cell io/text_wholetext — one row per file. pins: io-text-1/C-001"""
    out = tmp_path / "txt4"
    spark.createDataFrame([("a",), ("b",)], "value string").coalesce(1).write.text(str(out))
    _frame_pin(spark.read.text(str(out), wholetext=True), "text_wholetext")


def test_text_read_list_matches_oracle_count(spark: ReparkSession, tmp_path: Path) -> None:
    """cell io/text_read_dir_glob — a path list unions files. pins: io-text-1/C-001"""
    first = tmp_path / "t6.txt"
    first.write_text("x\n\ny", encoding="utf-8")
    second = tmp_path / "t5.txt"
    second.write_text("a;b;c", encoding="utf-8")
    assert (
        spark.read.text([str(first), str(second)]).count()
        == _cell("text_read_dir_glob")["result"]["value"]
    )


def test_text_read_directory(spark: ReparkSession, tmp_path: Path) -> None:
    """A directory reads every file inside it. pins: io-text-1/C-001"""
    root = tmp_path / "dir"
    root.mkdir()
    (root / "a.txt").write_text("x\n", encoding="utf-8")
    (root / "b.txt").write_text("y\n", encoding="utf-8")
    frame = spark.read.text(str(root)).orderBy("value")
    assert [repr(row) for row in frame.collect()] == ["Row(value='x')", "Row(value='y')"]


def test_text_read_schema(spark: ReparkSession, tmp_path: Path) -> None:
    """cell io/text_read_schema — struct<value:string>. pins: io-text-1/C-001"""
    target = tmp_path / "t6.txt"
    target.write_text("x\n", encoding="utf-8")
    assert (
        spark.read.text(str(target)).schema.simpleString()
        == _cell("text_read_schema")["result"]["value"]
    )


def test_format_text_load(spark: ReparkSession, tmp_path: Path) -> None:
    """format(text).load answers the shorthand read. pins: io-text-1/C-001"""
    target = tmp_path / "t5.txt"
    target.write_text("a;b;c", encoding="utf-8")
    frame = spark.read.format("text").option("lineSep", ";").load(str(target)).orderBy("value")
    assert [repr(row) for row in frame.collect()] == [
        "Row(value='a')",
        "Row(value='b')",
        "Row(value='c')",
    ]


def test_text_engine_options_refuse_loud(spark: ReparkSession, tmp_path: Path) -> None:
    """pathGlobFilter and friends fail loud, never silent. pins: io-text-1/C-001"""
    target = tmp_path / "t.txt"
    target.write_text("a\n", encoding="utf-8")
    for key, value in (
        ("pathGlobFilter", "*.txt"),
        ("recursiveFileLookup", "true"),
        ("modifiedBefore", "2026-09-14"),
        ("modifiedAfter", "2026-09-01"),
    ):
        with pytest.raises(AnalysisException):
            spark.read.option(key, value).format("text").load(str(target))


def test_text_roundtrip(spark: ReparkSession, tmp_path: Path) -> None:
    """cell io/text_roundtrip — write then read, null reads back empty. pins: io-text-1/C-002"""
    out = tmp_path / "txt1"
    spark.createDataFrame([("a",), ("b c",), (None,)], "value string").write.text(str(out))
    _frame_pin(spark.read.text(str(out)).orderBy("value"), "text_roundtrip")


def test_text_multi_col_error(spark: ReparkSession, tmp_path: Path) -> None:
    """cell io/text_multi_col — Spark's error text. pins: io-text-1/C-002"""
    expected = _cell("text_multi_col")["error"]
    with pytest.raises(AnalysisException) as raised:
        _kv(spark).write.text(str(tmp_path / "txt2"))
    assert type(raised.value).__name__ == expected["raises"]
    assert str(raised.value) == expected["message"]


def test_text_non_string_error(spark: ReparkSession, tmp_path: Path) -> None:
    """cell io/text_non_string — Spark's error text. pins: io-text-1/C-002"""
    expected = _cell("text_non_string")["error"]
    with pytest.raises(AnalysisException) as raised:
        spark.range(2).write.text(str(tmp_path / "txt3"))
    assert type(raised.value).__name__ == expected["raises"]
    assert str(raised.value) == expected["message"]


def test_text_write_null_row_file(spark: ReparkSession, tmp_path: Path) -> None:
    """cell io/text_write_null_row_file — null writes an empty line. pins: io-text-1/C-002"""
    out = tmp_path / "txt11"
    spark.createDataFrame([("a",), (None,)], "value string").coalesce(1).write.text(str(out))
    parts = sorted(out.glob("part-*.txt"))
    assert len(parts) == 1
    assert (
        repr(parts[0].read_text(encoding="utf-8"))
        == _cell("text_write_null_row_file")["result"]["value"]
    )


def test_text_write_mode_error(spark: ReparkSession, tmp_path: Path) -> None:
    """cell io/text_write_mode_error — fresh path writes None. pins: io-text-1/C-002"""
    out = tmp_path / "txt1"
    assert spark.createDataFrame([("a",)], "value string").write.text(str(out)) is None
    assert _cell("text_write_mode_error")["result"]["value"] is None
    with pytest.raises(AnalysisException, match="PATH_ALREADY_EXISTS"):
        spark.createDataFrame([("a",)], "value string").write.text(str(out))


def test_text_write_linesep(spark: ReparkSession, tmp_path: Path) -> None:
    """lineSep joins written rows. pins: io-text-1/C-002"""
    out = tmp_path / "sep"
    spark.createDataFrame([("a",), ("b",)], "value string").coalesce(1).write.text(
        str(out), lineSep=";"
    )
    parts = sorted(out.glob("part-*.txt"))
    assert len(parts) == 1
    assert parts[0].read_text(encoding="utf-8") == "a;b;"


def test_format_text_save(spark: ReparkSession, tmp_path: Path) -> None:
    """format(text).save answers the shorthand write. pins: io-text-1/C-002"""
    out = tmp_path / "saved"
    spark.createDataFrame([("a",)], "value string").write.format("text").save(str(out))
    assert [path.name for path in sorted(out.glob("part-*.txt"))] != []
    assert [repr(row) for row in spark.read.text(str(out)).collect()] == ["Row(value='a')"]


def test_text_write_append_overwrite(spark: ReparkSession, tmp_path: Path) -> None:
    """Save modes follow the writer path handling. pins: io-text-1/C-002"""
    out = tmp_path / "modes"
    spark.createDataFrame([("a",)], "value string").write.text(str(out))
    spark.createDataFrame([("b",)], "value string").write.mode("append").text(str(out))
    assert sorted(path.read_text(encoding="utf-8") for path in out.glob("part-*.txt")) == [
        "a\n",
        "b\n",
    ]
    spark.createDataFrame([("c",)], "value string").write.mode("overwrite").text(str(out))
    assert [path.read_text(encoding="utf-8") for path in out.glob("part-*.txt")] == ["c\n"]
    spark.createDataFrame([("z",)], "value string").write.mode("ignore").text(str(out))
    assert [path.read_text(encoding="utf-8") for path in out.glob("part-*.txt")] == ["c\n"]


def test_text_user_schema(spark: ReparkSession, tmp_path: Path) -> None:
    """A single string schema renames; anything wider refuses. pins: io-text-1/C-001"""
    target = tmp_path / "t.txt"
    target.write_text("x\n", encoding="utf-8")
    renamed = spark.read.schema("content string").text(str(target))
    assert [repr(row) for row in renamed.collect()] == ["Row(content='x')"]
    with pytest.raises(AnalysisException, match="single string field"):
        spark.read.schema("a int, b string").text(str(target)).collect()


def test_text_compression_gzip_refused(spark: ReparkSession, tmp_path: Path) -> None:
    """gzip stays refused — no compressor without a new dependency. pins: io-text-1/C-003"""
    with pytest.raises(AnalysisException, match="compression"):
        spark.createDataFrame([("a",)], "value string").write.text(
            str(tmp_path / "txt7"), compression="gzip"
        )
    plain = tmp_path / "plain"
    spark.createDataFrame([("a",)], "value string").write.text(str(plain), compression="none")
    assert [path.suffix for path in sorted(plain.glob("part-*"))] == [".txt"]


def test_text_partitionby_refused(spark: ReparkSession, tmp_path: Path) -> None:
    """Partitioned text layout is a future seed, never silent. pins: io-text-1/C-003"""
    with pytest.raises(AnalysisException, match="partitionBy"):
        spark.createDataFrame([("a",)], "value string").write.partitionBy("value").text(
            str(tmp_path / "parted")
        )


def test_text_glob_and_remote_refused(spark: ReparkSession, tmp_path: Path) -> None:
    """Globs and remote paths fail loud on the local-only scan. pins: io-text-1/C-003"""
    with pytest.raises(AnalysisException, match="glob"):
        spark.read.text(str(tmp_path / "*.txt")).collect()
    with pytest.raises(AnalysisException, match="not supported"):
        spark.read.text("s3://bucket/data.txt").collect()


def test_text_sql_door_pins_today_refusal(spark: ReparkSession, tmp_path: Path) -> None:
    """The SQL planner owns text-dot-path; today it refuses. pins: io-text-1/C-003"""
    target = tmp_path / "t.txt"
    target.write_text("a\n", encoding="utf-8")
    with pytest.raises(AnalysisException) as raised:
        spark.sql(f"SELECT * FROM text.`{target}`").collect()
    assert "not found" in str(raised.value)
