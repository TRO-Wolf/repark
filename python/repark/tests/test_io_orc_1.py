"""IO-ORC-1 — the read-only ORC scan against the oracle.

Oracle cells live in ``facade_orc_oracle.json`` (recorded on live PySpark 4.1.2
classic by the orchestrator, run 16b, 2026-09-15) over the Spark-written
fixtures under ``fixtures/orc/`` (``.crc`` files not copied). Reads pin columns
plus schema simpleString plus Row reprs; error cells pin Spark's own class,
message, params, and SQLSTATE.

pins: io-orc-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

CELLS: dict[str, Any] = json.loads(Path(__file__).with_name("facade_orc_oracle.json").read_text())

FIXTURES = str(Path(__file__).with_name("fixtures") / "orc")


def _cell(name: str) -> dict[str, Any]:
    return CELLS[name]


def _resolve(value: Any) -> Any:
    if isinstance(value, str):
        return value.replace("{FIX}", FIXTURES)
    if isinstance(value, list):
        return [_resolve(item) for item in value]
    if isinstance(value, dict):
        return {key: _resolve(item) for key, item in value.items()}
    return value


def _value_text(value: Any, dtype: Any) -> str:
    from repark.spark.types import StructType

    if value is None:
        return "None"
    if isinstance(dtype, StructType):
        fields = dtype.fields
        items = list(value.values()) if isinstance(value, dict) else list(value)
        inner = ", ".join(
            f"{field.name}={_value_text(item, field.dataType)}"
            for field, item in zip(fields, items, strict=True)
        )
        return f"Row({inner})"
    return repr(value)


def _row_text(row: Any, fields: Any) -> str:
    parts = [_value_text(item, field.dataType) for field, item in zip(fields, row, strict=True)]
    if len(parts) == 1:
        return f"({parts[0]},)"
    return "(" + ", ".join(parts) + ")"


def _frame_pin(frame: Any, cell: str) -> None:
    result = _resolve(_cell(cell)["result"])
    expected = result.get("read", result)
    assert frame.columns == expected["columns"]
    assert frame.schema.simpleString() == expected["schema"]
    fields = frame.schema.fields
    assert [_row_text(row, fields) for row in frame.collect()] == expected["rows"]


@pytest.fixture
def spark() -> Any:
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("test-io-orc-1").getOrCreate()
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _use_probe_zone(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")


def test_orc_typed_read(spark: ReparkSession) -> None:
    """cell orc_typed — every ORC type maps to Spark's names. pins: io-orc-1/C-002"""
    _use_probe_zone(spark)
    try:
        _frame_pin(spark.read.orc(f"{FIXTURES}/typed").orderBy("i"), "orc_typed")
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")


def test_orc_typed_format_load(spark: ReparkSession) -> None:
    """cell orc_typed_format_load — format spelling answers the shorthand. pins: io-orc-1/C-002"""
    _use_probe_zone(spark)
    try:
        _frame_pin(
            spark.read.format("orc").load(f"{FIXTURES}/typed").orderBy("i"), "orc_typed_format_load"
        )
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")


@pytest.mark.parametrize("codec", ["none", "uncompressed", "snappy", "zlib", "lzo", "lz4", "zstd"])
def test_orc_codecs_read(spark: ReparkSession, codec: str) -> None:
    """cells orc_codec_* — every Spark-written codec reads back. pins: io-orc-1/C-003"""
    _frame_pin(spark.read.orc(f"{FIXTURES}/codec_{codec}"), f"orc_codec_{codec}")


def _assert_analysis(
    raised: Any,
    *,
    condition: str | None,
    message: str,
    params: dict[str, str] | None,
    sqlstate: str | None,
) -> None:
    assert raised.value.getCondition() == condition
    assert str(raised.value) == message
    assert raised.value.getMessageParameters() == params
    assert raised.value.getSqlState() == sqlstate


def _assert_unable_to_infer_schema(raised: Any) -> None:
    _assert_analysis(
        raised,
        condition="UNABLE_TO_INFER_SCHEMA",
        message=(
            "[UNABLE_TO_INFER_SCHEMA] Unable to infer schema for ORC. "
            "It must be specified manually. SQLSTATE: 42KD9"
        ),
        params={"format": "ORC"},
        sqlstate="42KD9",
    )


def test_orc_read_single_file(spark: ReparkSession) -> None:
    """A single file path reads. pins: io-orc-1/C-004"""
    found = sorted(str(item) for item in Path(f"{FIXTURES}/typed").glob("*.orc"))
    _use_probe_zone(spark)
    try:
        _frame_pin(spark.read.orc(found[0]).orderBy("i"), "orc_typed")
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")


def test_orc_hidden_files_skipped(spark: ReparkSession, tmp_path: Path) -> None:
    """Hidden `_`/`.` files in a directory never reach the scan. pins: io-orc-1/C-004"""
    import shutil

    target = tmp_path / "typed"
    shutil.copytree(f"{FIXTURES}/typed", target)
    (target / "_skip.orc").write_text("hello", encoding="utf-8")
    (target / ".hidden").write_text("hello", encoding="utf-8")
    _use_probe_zone(spark)
    try:
        _frame_pin(spark.read.orc(str(target)).orderBy("i"), "orc_typed")
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")


def test_orc_reader_list_arg(spark: ReparkSession) -> None:
    """cell orc_reader_list_arg — a path list unions. pins: io-orc-1/C-004"""
    _frame_pin(
        spark.read.orc([f"{FIXTURES}/m1", f"{FIXTURES}/m1"]).orderBy("id"),
        "orc_reader_list_arg",
    )


def test_orc_path_glob(spark: ReparkSession) -> None:
    """cell orc_path_glob — a `*` glob unions matches. pins: io-orc-1/C-004"""
    _frame_pin(spark.read.orc(f"{FIXTURES}/codec_s*"), "orc_path_glob")


def test_orc_missing_path(spark: ReparkSession, tmp_path: Path) -> None:
    """cell orc_missing_path — Spark's PATH_NOT_FOUND file: form. pins: io-orc-1/C-004"""
    from repark.errors import AnalysisException

    missing = tmp_path / "does_not_exist"
    with pytest.raises(AnalysisException) as raised:
        spark.read.orc(str(missing))
    _assert_analysis(
        raised,
        condition="PATH_NOT_FOUND",
        message=f"[PATH_NOT_FOUND] Path does not exist: file:{missing}. SQLSTATE: 42K03",
        params={"path": f"file:{missing}"},
        sqlstate="42K03",
    )


def test_orc_empty_dir(spark: ReparkSession, tmp_path: Path) -> None:
    """cell orc_empty_dir — no files means UNABLE_TO_INFER_SCHEMA. pins: io-orc-1/C-004"""
    from repark.errors import AnalysisException

    empty = tmp_path / "empty_dir"
    empty.mkdir()
    with pytest.raises(AnalysisException) as raised:
        spark.read.orc(str(empty))
    _assert_unable_to_infer_schema(raised)


def test_orc_glob_filter_nomatch(spark: ReparkSession) -> None:
    """cell orc_glob_filter_nomatch — a filter leaving no file. pins: io-orc-1/C-004"""
    from repark.errors import AnalysisException

    with pytest.raises(AnalysisException) as raised:
        spark.read.option("pathGlobFilter", "*.parquet").orc(f"{FIXTURES}/m1")
    _assert_unable_to_infer_schema(raised)


def test_orc_modified_before_past(spark: ReparkSession) -> None:
    """cell orc_modified_before_past — a past bound leaves no file. pins: io-orc-1/C-004"""
    from repark.errors import AnalysisException

    with pytest.raises(AnalysisException) as raised:
        spark.read.option("modifiedBefore", "2000-01-01T00:00:00").orc(f"{FIXTURES}/m1")
    _assert_unable_to_infer_schema(raised)


def test_orc_nested_no_recursive(spark: ReparkSession) -> None:
    """cell orc_nested_no_recursive — plain dirs do not recurse. pins: io-orc-1/C-004"""
    from repark.errors import AnalysisException

    with pytest.raises(AnalysisException) as raised:
        spark.read.orc(f"{FIXTURES}/nested")
    _assert_unable_to_infer_schema(raised)


def test_orc_not_orc_file(spark: ReparkSession) -> None:
    """cell orc_not_orc_file — FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER. pins: io-orc-1/C-004"""
    from repark.errors import AnalysisException

    bad = f"{FIXTURES}/notorc/x.orc"
    with pytest.raises(AnalysisException) as raised:
        spark.read.orc(f"{FIXTURES}/notorc")
    assert raised.value.getCondition() == "FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER"
    assert str(raised.value).startswith(
        "[FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER] Encountered error while reading file "
        f"file:{bad}. Could not read footer."
    )
    assert str(raised.value).endswith("SQLSTATE: KD001")
    assert raised.value.getSqlState() == "KD001"


def test_orc_ignore_corrupt(spark: ReparkSession) -> None:
    """cell orc_ignore_corrupt — only-corrupt under the flag. pins: io-orc-1/C-004"""
    from repark.errors import AnalysisException

    with pytest.raises(AnalysisException) as raised:
        spark.read.option("ignoreCorruptFiles", "true").orc(f"{FIXTURES}/notorc")
    _assert_unable_to_infer_schema(raised)


def test_orc_zero_rows(spark: ReparkSession) -> None:
    """cell orc_zero_rows — a zero-row file keeps its schema. pins: io-orc-1/C-004"""
    frame = spark.read.orc(f"{FIXTURES}/zero_rows")
    assert frame.columns == ["id"]
    assert frame.schema.simpleString() == "struct<id:bigint>"
    assert frame.count() == 0


def test_orc_two_paths_bind_merge_schema(spark: ReparkSession) -> None:
    """cell orc_two_paths — a second positional is mergeSchema. pins: io-orc-1/C-005"""
    from repark.errors import IllegalArgumentException

    second = f"{FIXTURES}/m2"
    with pytest.raises(IllegalArgumentException) as raised:
        spark.read.orc(f"{FIXTURES}/m1", second)
    assert str(raised.value) == f'For input string: "{second}"'


def test_orc_merge_schema_kw(spark: ReparkSession) -> None:
    """cell orc_merge_schema_kw — keyword after two positionals. pins: io-orc-1/C-005"""
    with pytest.raises(TypeError, match="multiple values for argument 'mergeSchema'"):
        spark.read.orc(f"{FIXTURES}/m1", f"{FIXTURES}/m2", mergeSchema=True)


def test_orc_reader_signature_bad_arg(spark: ReparkSession) -> None:
    """cell orc_reader_signature_bad_arg — NOT_STR_OR_LIST. pins: io-orc-1/C-005"""
    from repark.errors import PySparkTypeError

    with pytest.raises(PySparkTypeError) as raised:
        spark.read.orc(123)
    assert raised.value.getCondition() == "NOT_STR_OR_LIST"
    assert (
        str(raised.value) == "[NOT_STR_OR_LIST] Argument `path` should be a str or list, got int."
    )


def test_orc_merge_schema_off_dir(spark: ReparkSession) -> None:
    """cell orc_merge_schema_off_dir — recorded with m* over m1/ and m2/ only. pins: io-orc-1/C-006"""
    _frame_pin(spark.read.orc(f"{FIXTURES}/m?").orderBy("id"), "orc_merge_schema_off_dir")


def test_orc_glob_non_orc_match_refuses(spark: ReparkSession) -> None:
    """m* over the fixture dir refuses naming the map.md match. pins: io-orc-1/C-006"""
    from repark.errors import AnalysisException

    with pytest.raises(AnalysisException) as raised:
        spark.read.orc(f"{FIXTURES}/m*")
    assert raised.value.getCondition() == "FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER"
    assert "map.md" in str(raised.value)


def test_orc_list_matches_glob_without_merge(spark: ReparkSession) -> None:
    """A differing-schema list answers one scan like the glob. pins: io-orc-1/C-006"""
    listed = spark.read.orc([f"{FIXTURES}/m1", f"{FIXTURES}/m2"]).orderBy("id")
    globbed = spark.read.orc(f"{FIXTURES}/m?").orderBy("id")
    assert listed.columns == globbed.columns == ["id"]
    assert listed.schema.simpleString() == globbed.schema.simpleString()
    assert [repr(row) for row in listed.collect()] == [repr(row) for row in globbed.collect()]


def test_orc_merge_schema_on_unions(spark: ReparkSession) -> None:
    """mergeSchema=true unions columns by name, missing cells NULL. pins: io-orc-1/C-006"""
    frame = spark.read.option("mergeSchema", "true").orc([f"{FIXTURES}/m1", f"{FIXTURES}/m2"])
    assert frame.columns == ["id", "extra"]
    assert frame.schema.simpleString() == "struct<id:bigint,extra:string>"
    fields = frame.schema.fields
    assert sorted(_row_text(row, fields) for row in frame.collect()) == [
        "(0, '0')",
        "(0, None)",
        "(1, '1')",
        "(1, None)",
    ]


def test_orc_glob_filter(spark: ReparkSession) -> None:
    """cell orc_glob_filter — pathGlobFilter keeps matches. pins: io-orc-1/C-006"""
    _frame_pin(
        spark.read.option("pathGlobFilter", "*.orc").orc(f"{FIXTURES}/m1"),
        "orc_glob_filter",
    )


def test_orc_nested_recursive(spark: ReparkSession) -> None:
    """cell orc_nested_recursive — recursiveFileLookup descends. pins: io-orc-1/C-006"""
    _frame_pin(
        spark.read.option("recursiveFileLookup", "true").orc(f"{FIXTURES}/nested"),
        "orc_nested_recursive",
    )


def test_orc_modified_after_past(spark: ReparkSession) -> None:
    """cell orc_modified_after_past — a past bound keeps new files. pins: io-orc-1/C-006"""
    _frame_pin(
        spark.read.option("modifiedAfter", "2000-01-01T00:00:00").orc(f"{FIXTURES}/m1"),
        "orc_modified_after_past",
    )


def test_orc_unknown_options_ignored(spark: ReparkSession) -> None:
    """Unknown reader options are ignored like Spark. pins: io-orc-1/C-006"""
    frame = (
        spark.read.option("bogusOption", "zzz").option("orc.stripe.size", "1").orc(f"{FIXTURES}/m1")
    )
    assert frame.count() == 2


def test_orc_partitioned_read(spark: ReparkSession) -> None:
    """cell orc_partitioned_read — k=v dirs append partition columns. pins: io-orc-1/C-007"""
    _frame_pin(spark.read.orc(f"{FIXTURES}/part").orderBy("a"), "orc_partitioned_read")


def test_orc_partition_leaf(spark: ReparkSession) -> None:
    """cell orc_partition_leaf — a leaf read adds no column. pins: io-orc-1/C-007"""
    _frame_pin(spark.read.orc(f"{FIXTURES}/part/k=x"), "orc_partition_leaf")


def test_orc_partition_basepath(spark: ReparkSession) -> None:
    """cell orc_partition_basepath — basePath restores partitions. pins: io-orc-1/C-006"""
    _frame_pin(
        spark.read.option("basePath", f"{FIXTURES}/part").orc(f"{FIXTURES}/part/k=x"),
        "orc_partition_basepath",
    )


def test_orc_user_schema_subset(spark: ReparkSession) -> None:
    """cell orc_user_schema_subset — names resolve, order follows. pins: io-orc-1/C-007"""
    _frame_pin(
        spark.read.schema("extra string").orc(f"{FIXTURES}/m2").orderBy("extra"),
        "orc_user_schema_subset",
    )


def test_orc_user_schema_missing_col(spark: ReparkSession) -> None:
    """cell orc_user_schema_missing_col — a missing column reads NULL. pins: io-orc-1/C-007"""
    _frame_pin(
        spark.read.schema("id bigint, nope int").orc(f"{FIXTURES}/m2"),
        "orc_user_schema_missing_col",
    )


def test_orc_user_schema_wrong_type(spark: ReparkSession) -> None:
    """cell orc_user_schema_wrong_type — int reads as its text form. pins: io-orc-1/C-007"""
    _frame_pin(
        spark.read.schema("id string").orc(f"{FIXTURES}/m2"),
        "orc_user_schema_wrong_type",
    )


@pytest.mark.parametrize("wanted", ["map<string,int>", "struct<x:int>", "char(3)"])
def test_orc_user_schema_unapplied_type_refuses(spark: ReparkSession, wanted: str) -> None:
    """An unparsed user-schema type refuses with field, type, file type. pins: io-orc-1/C-007"""
    from repark.errors import AnalysisException

    with pytest.raises(AnalysisException) as raised:
        spark.read.schema(f"s {wanted}").orc(f"{FIXTURES}/typed")
    assert str(raised.value) == (
        f"orc read cannot apply schema type `{wanted}` to column `s` of type Utf8"
    )


def test_orc_select_one(spark: ReparkSession) -> None:
    """cell orc_select_one — nested field plus array project. pins: io-orc-1/C-008"""
    frame = spark.read.orc(f"{FIXTURES}/typed")
    _frame_pin(frame.select(frame.st.y.alias("y"), "arr").orderBy("y"), "orc_select_one")


def test_orc_filter_pushdown(spark: ReparkSession) -> None:
    """cell orc_filter_pushdown — filter answers before select. pins: io-orc-1/C-008"""
    _frame_pin(
        spark.read.orc(f"{FIXTURES}/typed").filter("i = 2").select("i", "s"),
        "orc_filter_pushdown",
    )


def test_orc_case_insensitive_col(spark: ReparkSession) -> None:
    """cell orc_case_insensitive_col — names match case-insensitively. pins: io-orc-1/C-008"""
    _frame_pin(
        spark.read.orc(f"{FIXTURES}/typed").select("I", "S").orderBy("I"),
        "orc_case_insensitive_col",
    )


def test_orc_count(spark: ReparkSession) -> None:
    """cell orc_count — count over the scan. pins: io-orc-1/C-008"""
    assert spark.read.orc(f"{FIXTURES}/typed").count() == _cell("orc_count")["result"]


def test_orc_ts_session_tz(spark: ReparkSession) -> None:
    """cell orc_ts_session_tz — instant shifts, ntz does not. pins: io-orc-1/C-008"""
    from repark.spark import functions as F  # noqa: N812 — PySpark idiom

    spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    try:
        frame = (
            spark.read.orc(f"{FIXTURES}/typed")
            .select("i", "ts", "tsntz", F.col("ts").cast("string").alias("ts_s"))
            .orderBy("i")
        )
        _frame_pin(frame, "orc_ts_session_tz")
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")


def test_orc_ts_utc_render(spark: ReparkSession) -> None:
    """display orc_ts_utc_session — the instant renders 03:04:05 in UTC. pins: io-orc-1/C-008"""
    frame = spark.read.orc(f"{FIXTURES}/typed").select("i", "ts").orderBy("i")
    rows = frame.collect()
    assert repr(rows[0][1]) == "datetime.datetime(2024, 1, 2, 3, 4, 5, 123456)"
    assert rows[1][1] is None


def test_orc_sql_door_refusal(spark: ReparkSession) -> None:
    """cell orc_sql_door_missing — the path table is the planner's. pins: io-orc-1/C-009"""
    from repark.errors import AnalysisException

    with pytest.raises(AnalysisException) as raised:
        spark.sql(f"SELECT * FROM orc.`{FIXTURES}/nope`")
    assert "datafusion.orc." in str(raised.value)


def test_orc_sql_create_using_refusal(spark: ReparkSession) -> None:
    """cell orc_sql_create_using — USING orc LOCATION is the planner's. pins: io-orc-1/C-009"""
    from repark.errors import PySparkException

    with pytest.raises(PySparkException) as raised:
        spark.sql(f"CREATE TABLE orc_t USING orc LOCATION '{FIXTURES}/m1'")
    assert "LOCATION is not supported" in str(raised.value)


def test_orc_write_still_declared(spark: ReparkSession, tmp_path: Path) -> None:
    """The write side keeps #610's NOT_IMPLEMENTED refusal. pins: io-orc-1/C-002"""
    from repark.errors import PySparkNotImplementedError

    frame = spark.createDataFrame([(1,)], ["id"])
    with pytest.raises(PySparkNotImplementedError) as raised:
        frame.write.orc(str(tmp_path / "o"))
    assert raised.value.getCondition() == "NOT_IMPLEMENTED"
    with pytest.raises(PySparkNotImplementedError) as refused:
        frame.write.format("orc").save(str(tmp_path / "o2"))
    assert refused.value.getCondition() == "NOT_IMPLEMENTED"
