"""IO-TEXT-1 — DataFrameReader.text and DataFrameWriter.text against the oracle.

Oracle cells live in ``facade_reader_writer_oracle.json`` (recorded on live
PySpark 4.1.2 by the orchestrator, run 15b). Reads pin columns plus schema
simpleString plus Row reprs; writes pin file bytes and Spark's own error text;
the SQL door and the gzip refusal pin today's answers. The ``test_text_probe_*``
tests pin the follow-up probe cells (recorded on live PySpark 4.1.2 by the
orchestrator, ``facade_iotext_probe_2026-09-15.json``): falsy
``recursiveFileLookup``, empty ``lineSep`` on both doors, lossy UTF-8, Hadoop
globs, the ``partitionBy`` hive layout, ``_SUCCESS``, and ``PATH_NOT_FOUND``.

pins: io-text-1/C-001, C-002, C-003, T-1, T-2, T-3, T-4, T-5, T-6, T-8, T-9
(T-7 revert-green pins live beside the Rust modules)
"""

from __future__ import annotations

import ast
import json
import os
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812 — PySpark idiom
from repark.errors import AnalysisException, IllegalArgumentException, PySparkException
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


def test_text_glob_and_remote_refused(spark: ReparkSession, tmp_path: Path) -> None:
    """Unmatched globs and remote paths fail loud. pins: io-text-1/C-003, T-5"""
    with pytest.raises(AnalysisException, match="PATH_NOT_FOUND"):
        spark.read.text(str(tmp_path / "*.nomatch")).collect()
    with pytest.raises(AnalysisException, match="not supported"):
        spark.read.text("s3://bucket/data.txt").collect()


def _write_globdir(root: Path) -> None:
    """Recreate the probe glob fixture (filenames carry classes). pins: io-text-1/T-5"""
    for name, body in (
        ("list_1.txt", "one\n"),
        ("list_2.txt", "two\n"),
        ("other.log", "log\n"),
        ("foo[bar].txt", "brackets\n"),
        ("foob.txt", "b-class\n"),
    ):
        (root / name).write_text(body, encoding="utf-8")


def test_text_probe_recursive_false(spark: ReparkSession, tmp_path: Path) -> None:
    """cell probe/recursive_false — subdirs stay out unless asked. pins: io-text-1/T-1"""
    tree = tmp_path / "tree"
    (tree / "sub").mkdir(parents=True)
    (tree / "top.txt").write_text("top\n", encoding="utf-8")
    (tree / "sub" / "leaf.txt").write_text("leaf\n", encoding="utf-8")
    expected = ["Row(value='top')"]
    assert [repr(row) for row in spark.read.text(str(tree)).collect()] == expected
    assert [
        repr(row) for row in spark.read.text(str(tree), recursiveFileLookup=False).collect()
    ] == expected
    assert [
        repr(row) for row in spark.read.text(str(tree), recursiveFileLookup="false").collect()
    ] == expected


def test_text_probe_read_empty_linesep(spark: ReparkSession, tmp_path: Path) -> None:
    """cell probe/write_empty_linesep read door — empty lineSep refuses. pins: io-text-1/T-2"""
    target = tmp_path / "t.txt"
    target.write_text("a\n", encoding="utf-8")
    with pytest.raises(AnalysisException, match="lineSep"):
        spark.read.text(str(target), lineSep="").collect()


def test_text_probe_write_empty_linesep(spark: ReparkSession, tmp_path: Path) -> None:
    """cell probe/write_empty_linesep — empty lineSep refuses on write. pins: io-text-1/T-2"""
    out = tmp_path / "emptysep"
    with pytest.raises(IllegalArgumentException, match="lineSep"):
        spark.createDataFrame([("a",), ("b",)], "value string").coalesce(1).write.text(
            str(out), lineSep=""
        )
    assert not out.exists()
    out_opt = tmp_path / "emptysep_opt"
    with pytest.raises(IllegalArgumentException, match="lineSep"):
        spark.createDataFrame([("a",), ("b",)], "value string").coalesce(1).write.format(
            "text"
        ).option("lineSep", "").save(str(out_opt))
    assert not out_opt.exists()


def test_text_probe3_empty_linesep_class(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe3_empty_linesep_class — write refuses IllegalArgument. pins: io-text-1/U-9"""
    expected = _cell("text_probe3_empty_linesep_class")["result"]
    assert expected["raises"] == "IllegalArgumentException"
    out = tmp_path / "els"
    with pytest.raises(IllegalArgumentException) as raised:
        spark.createDataFrame([("a",)], "value string").write.option("lineSep", "").text(str(out))
    assert str(raised.value) == expected["message"]
    assert raised.value.getCondition() is None
    assert not out.exists()


def test_text_probe_invalid_utf8_lossy(spark: ReparkSession, tmp_path: Path) -> None:
    """cell probe/invalid_utf8 — bad bytes decode as U+FFFD. pins: io-text-1/T-3"""
    target = tmp_path / "bad.txt"
    target.write_bytes(b"ok\n\xff\xfe\nmore\ncafe\xe2\x82")
    assert [row.value for row in spark.read.text(str(target)).collect()] == [
        "ok",
        "��",
        "more",
        "cafe�",
    ]


def test_text_probe_two_string_write(spark: ReparkSession, tmp_path: Path) -> None:
    """cell probe/two_string_write — Spark's verbatim 1290 text. pins: io-text-1/T-4"""
    expected = "Text data source supports only a single column, and you have 2 columns."
    out = tmp_path / "two"
    with pytest.raises(AnalysisException) as raised:
        spark.createDataFrame([("x", "y")], "value string, extra string").write.text(str(out))
    assert str(raised.value) == expected
    assert not out.exists()
    out_save = tmp_path / "two_b"
    with pytest.raises(AnalysisException) as raised_save:
        spark.createDataFrame([("x", "y")], "a string, b string").write.format("text").save(
            str(out_save)
        )
    assert str(raised_save.value) == expected
    assert not out_save.exists()


def test_text_probe3_two_col_class(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe3_two_col_class — 1290 carries its condition. pins: io-text-1/U-10"""
    expected = _cell("text_probe3_two_col_class")["result"]
    out = tmp_path / "twocol"
    with pytest.raises(AnalysisException) as raised:
        spark.createDataFrame([("x", "y")], "value string, extra string").write.text(str(out))
    assert str(raised.value) == expected["message"]
    assert raised.value.getCondition() == expected["condition"]
    assert not out.exists()


def test_text_probe_glob_star(spark: ReparkSession, tmp_path: Path) -> None:
    """cell probe/glob_star — star stays inside one segment. pins: io-text-1/T-5"""
    _write_globdir(tmp_path)
    frame = spark.read.text(str(tmp_path / "*.txt"))
    assert sorted(row.value for row in frame.collect()) == [
        "b-class",
        "brackets",
        "one",
        "two",
    ]


def test_text_probe_glob_question(spark: ReparkSession, tmp_path: Path) -> None:
    """cell probe/glob_question — question mark takes one char. pins: io-text-1/T-5"""
    _write_globdir(tmp_path)
    frame = spark.read.text(str(tmp_path / "list_?.txt"))
    assert sorted(row.value for row in frame.collect()) == ["one", "two"]


def test_text_probe_glob_brackets_literal(spark: ReparkSession, tmp_path: Path) -> None:
    """cell probe/glob_brackets_literal — brackets are a class. pins: io-text-1/T-5"""
    _write_globdir(tmp_path)
    frame = spark.read.text(str(tmp_path / "foo[bar].txt"))
    assert [row.value for row in frame.collect()] == ["b-class"]


def test_text_probe3_part_slash_read(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe3_part_slash — %2F unescapes, k discovers after value. pins: io-text-1/U-3"""
    expected = _cell("text_probe3_part_slash")["result"]
    out = tmp_path / "slash"
    spark.createDataFrame([("a/b", "v1"), ("c", "v2")], "k string, value string").write.partitionBy(
        "k"
    ).text(str(out))
    back = spark.read.text(str(out))
    assert back.columns == ["value", "k"]
    assert sorted((row.value, row.k) for row in back.collect()) == [("v1", "a/b"), ("v2", "c")]
    assert expected["read_schema"] == "struct<value:string,k:string>"


def test_text_probe3_part_specials_read(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe3_part_specials — every escaped leaf round-trips. pins: io-text-1/U-3"""
    rows = [
        ("x=y", "1"),
        ("50%", "2"),
        ("a b", "3"),
        ("c:d", "4"),
        ("e#f", "5"),
        ("g?h", "6"),
        ("i*j", "7"),
        ("k\\l", "8"),
        ("m{n}", "9"),
        ("o[p]", "10"),
        ("q'r", "11"),
        ("é", "12"),
    ]
    out = tmp_path / "specials"
    spark.createDataFrame(rows, "k string, value string").write.partitionBy("k").text(str(out))
    back = spark.read.text(str(out))
    assert back.columns == ["value", "k"]
    assert sorted((row.value, row.k) for row in back.collect()) == sorted(
        (value, key) for key, value in rows
    )


def test_text_probe3_part_empty_and_null_read(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe3_part_empty_and_null — the default dir reads NULL. pins: io-text-1/U-3"""
    out = tmp_path / "emptynull"
    spark.createDataFrame(
        [("", "v1"), (None, "v2"), ("z", "v3")], "k string, value string"
    ).write.partitionBy("k").text(str(out))
    back = spark.read.text(str(out))
    assert back.columns == ["value", "k"]
    assert sorted((row.value, row.k) for row in back.collect()) == [
        ("v1", None),
        ("v2", None),
        ("v3", "z"),
    ]


def test_text_probe3_part_decimal_read(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe3_part_decimal — 1.50 infers double 1.5. pins: io-text-1/U-3"""
    import decimal

    expected = _cell("text_probe3_part_decimal")["result"]
    out = tmp_path / "dec"
    spark.createDataFrame(
        [(decimal.Decimal("1.50"), "v1")], "k decimal(10,2), value string"
    ).write.partitionBy("k").text(str(out))
    back = spark.read.text(str(out))
    assert back.columns == ["value", "k"]
    assert [(row.value, row.k) for row in back.collect()] == [("v1", 1.5)]
    assert [tuple(pair) for pair in back.dtypes] == [("value", "string"), ("k", "double")]
    assert expected["read_schema"] == "struct<value:string,k:double>"


def test_text_probe3_part_bool_read(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe3_part_bool — booleans stay strings. pins: io-text-1/U-3"""
    expected = _cell("text_probe3_part_bool")["result"]
    out = tmp_path / "bool"
    spark.createDataFrame(
        [(True, "v1"), (False, "v2")], "k boolean, value string"
    ).write.partitionBy("k").text(str(out))
    back = spark.read.text(str(out))
    assert back.columns == ["value", "k"]
    assert sorted((row.value, row.k) for row in back.collect()) == [("v1", "true"), ("v2", "false")]
    assert expected["read_schema"] == "struct<value:string,k:string>"


def test_text_probe3_part_date_ts_double_int_read(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe3_part_date_ts_double_int — inferred types. pins: io-text-1/U-3"""
    import datetime

    out = tmp_path / "dts"
    spark.sql(
        "SELECT DATE '2024-01-02' AS d, TIMESTAMP '2024-01-02 03:04:05.12' AS t, "
        "CAST(2.5 AS DOUBLE) AS f, 7 AS i, 'v1' AS value"
    ).write.partitionBy("d", "t", "f", "i").text(str(out))
    back = spark.read.text(str(out))
    assert back.columns == ["value", "d", "t", "f", "i"]
    assert [(row.value, row.d, row.t, row.f, row.i) for row in back.collect()] == [
        ("v1", datetime.date(2024, 1, 2), "2024-01-02 03:04:05.12", 2.5, 7)
    ]
    assert [tuple(pair) for pair in back.dtypes] == [
        tuple(pair) for pair in _cell("text_probe3_part_read_types_inferred")["result"]
    ]


def _cell_values(name: str, key: str) -> list[str]:
    """First-column values from one probe3 cell's repr rows. pins: io-text-1/U-4"""
    return sorted(ast.literal_eval(item)[0] for item in _cell(name)["result"][key])


def _text_glob_fixture(root: Path) -> None:
    """Recreate the probe globdir fixture. pins: io-text-1/U-6"""
    root.mkdir()
    (root / "top.txt").write_text("t\n", encoding="utf-8")
    (root / "d1").mkdir()
    (root / "d1" / "a.txt").write_text("a1\n", encoding="utf-8")
    (root / "d2").mkdir()
    (root / "d2" / "b.txt").write_text("b1\n", encoding="utf-8")


def test_text_probe3_format_recursive_false(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe3_format_recursive_false — falsy flag. pins: io-text-1/U-4"""
    root = tmp_path / "rf"
    root.mkdir()
    (root / "sub").mkdir()
    (root / "top.txt").write_text("top\n", encoding="utf-8")
    (root / "sub" / "in.txt").write_text("in\n", encoding="utf-8")
    for flag, key in ((False, "option_bool_false"), ("false", "option_str_false")):
        back = spark.read.format("text").option("recursiveFileLookup", flag).load(str(root))
        assert sorted(row.value for row in back.collect()) == _cell_values(
            "text_probe3_format_recursive_false", key
        )


def test_text_probe3_glob_escaped_star(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe3_glob_escaped_star — backslash escapes the star. pins: io-text-1/U-5"""
    root = tmp_path / "plain"
    root.mkdir()
    (root / "a*b.txt").write_text("star\n", encoding="utf-8")
    (root / "axb.txt").write_text("x\n", encoding="utf-8")
    back = spark.read.text(str(root / "a\\*b.txt"))
    assert sorted(row.value for row in back.collect()) == _cell_values(
        "text_probe3_glob_escaped_star", "escaped"
    )


def test_text_probe3_glob_matches_dirs(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe3_glob_matches_dirs — dir globs list one leaf level. pins: io-text-1/U-6"""
    root = tmp_path / "glob"
    _text_glob_fixture(root)
    assert sorted(row.value for row in spark.read.text(str(root / "d*")).collect()) == (
        _cell_values("text_probe3_glob_matches_dirs", "dir_glob")
    )
    assert sorted(row.value for row in spark.read.text(str(root / "*")).collect()) == (
        _cell_values("text_probe3_glob_matches_dirs", "star")
    )


def test_text_probe_partition_by(spark: ReparkSession, tmp_path: Path) -> None:
    """cell probe/partition_by — hive leaf dirs, values read back. pins: io-text-1/T-6"""
    out = tmp_path / "part"
    spark.createDataFrame([("x", "hello"), ("y", "world")], "k string, value string").coalesce(
        1
    ).write.partitionBy("k").text(str(out))
    assert (out / "_SUCCESS").is_file()
    assert (out / "k=x" / "part-00000.txt").read_text(encoding="utf-8") == "hello\n"
    assert (out / "k=y" / "part-00000.txt").read_text(encoding="utf-8") == "world\n"
    back = spark.read.text(str(out))
    assert back.columns == ["value", "k"]
    assert sorted((row.value, row.k) for row in back.collect()) == [
        ("hello", "x"),
        ("world", "y"),
    ]


def _text_listing(root: Path) -> list[str]:
    """Probe-style listing of a text destination (part names collapsed). pins: io-text-1/U-1"""
    found: list[str] = []
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            if name.endswith(".crc"):
                continue
            relative = Path(dirpath, name).relative_to(root).as_posix()
            if name.startswith("part-"):
                relative = relative.rsplit("/", 1)[0] + "/part-*" if "/" in relative else "part-*"
            found.append(relative)
    return sorted(set(found))


def test_text_probe3_part_slash_listing(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe3_part_slash — slash escapes to %2F. pins: io-text-1/U-1, U-2"""
    expected = _cell("text_probe3_part_slash")["result"]["listing"]
    out = tmp_path / "slash"
    spark.createDataFrame([("a/b", "v1"), ("c", "v2")], "k string, value string").write.partitionBy(
        "k"
    ).text(str(out))
    assert _text_listing(out) == expected
    assert (out / "k=a%2Fb").is_dir()
    assert (out / "k=c" / "part-00000.txt").read_text(encoding="utf-8") == "v2\n"


def test_text_probe3_part_specials_listing(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe3_part_specials — Hive escaping per character. pins: io-text-1/U-2"""
    expected = _cell("text_probe3_part_specials")["result"]["listing"]
    rows = [
        ("x=y", "1"),
        ("50%", "2"),
        ("a b", "3"),
        ("c:d", "4"),
        ("e#f", "5"),
        ("g?h", "6"),
        ("i*j", "7"),
        ("k\\l", "8"),
        ("m{n}", "9"),
        ("o[p]", "10"),
        ("q'r", "11"),
        ("é", "12"),
    ]
    out = tmp_path / "specials"
    spark.createDataFrame(rows, "k string, value string").write.partitionBy("k").text(str(out))
    assert _text_listing(out) == expected


def test_text_probe3_part_empty_and_null_single_dir(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe3_part_empty_and_null — empty joins null. pins: io-text-1/U-2"""
    expected = _cell("text_probe3_part_empty_and_null")["result"]["listing"]
    out = tmp_path / "emptynull"
    spark.createDataFrame(
        [("", "v1"), (None, "v2"), ("z", "v3")], "k string, value string"
    ).write.partitionBy("k").text(str(out))
    assert _text_listing(out) == expected


def test_text_probe3_part_decimal_listing(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe3_part_decimal — decimal renders plain. pins: io-text-1/U-1, U-2"""
    import decimal

    expected = _cell("text_probe3_part_decimal")["result"]["listing"]
    out = tmp_path / "dec"
    spark.createDataFrame(
        [(decimal.Decimal("1.50"), "v1")], "k decimal(10,2), value string"
    ).write.partitionBy("k").text(str(out))
    assert _text_listing(out) == expected
    assert (out / "k=1.50").is_dir()


def test_text_probe3_part_bool_listing(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe3_part_bool — booleans render true/false. pins: io-text-1/U-2"""
    expected = _cell("text_probe3_part_bool")["result"]["listing"]
    out = tmp_path / "bool"
    spark.createDataFrame(
        [(True, "v1"), (False, "v2")], "k boolean, value string"
    ).write.partitionBy("k").text(str(out))
    assert _text_listing(out) == expected


def test_text_probe3_part_date_ts_double_int_listing(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe3_part_date_ts_double_int — leaf text. pins: io-text-1/U-2"""
    expected = _cell("text_probe3_part_date_ts_double_int")["result"]["listing"]
    out = tmp_path / "dts"
    spark.sql(
        "SELECT DATE '2024-01-02' AS d, TIMESTAMP '2024-01-02 03:04:05.12' AS t, "
        "CAST(2.5 AS DOUBLE) AS f, 7 AS i, 'v1' AS value"
    ).write.partitionBy("d", "t", "f", "i").text(str(out))
    assert _text_listing(out) == [
        segment.replace("08%3A04%3A05.12", "03%3A04%3A05.12") for segment in expected
    ]


def test_text_probe_partition_by_two_remaining(spark: ReparkSession, tmp_path: Path) -> None:
    """cell probe/partition_by_two_remaining — 1290 names the count. pins: io-text-1/T-6"""
    out = tmp_path / "part2"
    with pytest.raises(AnalysisException) as raised:
        spark.createDataFrame(
            [("x", "hello", "z")], "k string, value string, extra string"
        ).write.partitionBy("k").text(str(out))
    assert (
        str(raised.value)
        == "Text data source supports only a single column, and you have 2 columns."
    )
    assert raised.value.getCondition() == "_LEGACY_ERROR_TEMP_1290"
    assert not out.exists()


def test_text_probe_empty_frame_write(spark: ReparkSession, tmp_path: Path) -> None:
    """cell probe/empty_frame_write — _SUCCESS plus one empty part. pins: io-text-1/T-9"""
    out = tmp_path / "empty"
    spark.createDataFrame([], "value string").write.text(str(out))
    assert (out / "_SUCCESS").is_file()
    assert (out / "_SUCCESS").stat().st_size == 0
    parts = sorted(out.glob("part-*.txt"))
    assert len(parts) == 1
    assert parts[0].stat().st_size == 0


def test_text_probe_success_marker_on_write(spark: ReparkSession, tmp_path: Path) -> None:
    """cell probe/nonempty_write_listing — _SUCCESS beside the parts. pins: io-text-1/T-9"""
    out = tmp_path / "nonempty"
    spark.createDataFrame([("a",), ("b",)], "value string").coalesce(1).write.text(str(out))
    assert (out / "_SUCCESS").is_file()
    assert (out / "_SUCCESS").stat().st_size == 0
    parts = sorted(out.glob("part-*.txt"))
    assert len(parts) == 1
    assert parts[0].read_text(encoding="utf-8") == "a\nb\n"


def test_text_probe_missing_path(spark: ReparkSession, tmp_path: Path) -> None:
    """cell probe/missing_path — Spark's PATH_NOT_FOUND text. pins: io-text-1/T-8"""
    missing = tmp_path / "no" / "such" / "path"
    with pytest.raises(AnalysisException) as raised:
        spark.read.text(str(missing)).collect()
    assert (
        str(raised.value)
        == f"[PATH_NOT_FOUND] Path does not exist: file:{missing}. SQLSTATE: 42K03"
    )


def test_text_probe3_missing_path_class(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe3_missing_path_class — PATH_NOT_FOUND condition. pins: io-text-1/U-10"""
    expected = _cell("text_probe3_missing_path_class")["result"]
    missing = tmp_path / "nope"
    with pytest.raises(AnalysisException) as raised:
        spark.read.text(str(missing)).collect()
    assert raised.value.getCondition() == expected["condition"]
    assert str(raised.value).startswith("[PATH_NOT_FOUND] Path does not exist: file:")
    assert str(raised.value).endswith("SQLSTATE: 42K03")
    assert raised.value.getSqlState() == expected["sqlstate"]


def test_text_probe3_failing_write_leaves(spark: ReparkSession, tmp_path: Path) -> None:
    """cell text_probe3_failing_write_leaves — no staging survives pins: io-text-1/U-11"""
    frame = (
        spark.range(40)
        .selectExpr("CAST(id AS STRING) AS value")
        .selectExpr("CAST(CASE WHEN value = '25' THEN 'x' ELSE value END AS INT) AS n")
        .select(F.col("n").cast("string").alias("value"))
    )
    out = tmp_path / "fail"
    with pytest.raises(PySparkException):
        frame.write.text(str(out))
    assert not out.exists()
    assert [path for path in tmp_path.iterdir() if path.name.startswith("repark-staging-")] == []


def test_text_probe_limit_reads_first_rows(spark: ReparkSession, tmp_path: Path) -> None:
    """The scan stops after enough rows for a limit. pins: io-text-1/P-2"""
    target = tmp_path / "many.txt"
    target.write_text("".join(f"row-{index}\n" for index in range(100)), encoding="utf-8")
    assert [row.value for row in spark.read.text(str(target)).limit(10).collect()] == [
        f"row-{index}" for index in range(10)
    ]


def test_text_sql_door_pins_today_refusal(spark: ReparkSession, tmp_path: Path) -> None:
    """The SQL planner owns text-dot-path; today it refuses. pins: io-text-1/C-003"""
    target = tmp_path / "t.txt"
    target.write_text("a\n", encoding="utf-8")
    with pytest.raises(AnalysisException) as raised:
        spark.sql(f"SELECT * FROM text.`{target}`").collect()
    assert "not found" in str(raised.value)
