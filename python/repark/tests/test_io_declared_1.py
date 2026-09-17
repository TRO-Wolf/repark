"""io-declared-1 pins: the orc / xml / jdbc declared IO refusals and ``na.replace``.

pins: io-declared-1/C-001, C-002, C-003, C-004
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import (
    AnalysisException,
    PySparkNotImplementedError,
    PySparkTypeError,
    PySparkValueError,
)

_ORACLE: dict[str, Any] = json.loads(
    Path(__file__).with_name("facade_reader_writer_oracle.json").read_text(encoding="utf-8")
)["cells"]


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    """A per-test facade session."""
    session = ReparkSession.builder.appName("pytest-io-declared-1").getOrCreate()
    yield session
    session.stop()


def _kv_frame(spark: ReparkSession) -> Any:
    """The (key, a, b) frame the oracle orc/xml writer cells ran against."""
    return spark.createDataFrame([("x", 1, 2), ("y", 3, 4)], "key string, a int, b int")


def _xy_frame(spark: ReparkSession) -> Any:
    """The (x, y) frame the oracle na.replace cells ran against."""
    return spark.createDataFrame([(1, "a"), (2, "b")], "x int, y string")


def _xy_null_frame(spark: ReparkSession) -> Any:
    """The (x, y) frame with a NULL x the na_replace_basic cell ran against."""
    return spark.createDataFrame([(1, "a"), (2, "b"), (None, "a")], "x int, y string")


def _assert_frame_cell(frame: Any, cell_id: str) -> None:
    """Compare a frame against one recorded DataFrame oracle cell."""
    cell = _ORACLE[cell_id]["result"]
    assert cell["kind"] == "DataFrame"
    assert frame.columns == cell["columns"]
    assert frame.schema.simpleString() == cell["schema"]
    assert [repr(row) for row in frame.collect()] == cell["rows"]


def _assert_error_cell(error: BaseException, cell_id: str) -> None:
    """Compare a raised error against one recorded error oracle cell."""
    cell = _ORACLE[cell_id]["error"]
    assert type(error).__name__ == cell["raises"]
    if cell["condition"] is not None:
        assert error.getCondition() == cell["condition"]
    if cell["params"] is not None:
        assert error.getMessageParameters() == cell["params"]
    recorded_lines = cell["message"].splitlines()
    actual_lines = str(error).splitlines()
    assert actual_lines[0] == recorded_lines[0]
    if len(recorded_lines) == 1:
        assert str(error) == cell["message"]


def _assert_not_implemented(error: BaseException, feature: str) -> None:
    """Assert one declared NOT_IMPLEMENTED refusal shape."""
    assert type(error).__name__ == "PySparkNotImplementedError"
    assert error.getCondition() == "NOT_IMPLEMENTED"
    assert error.getMessageParameters() == {"feature": feature}
    assert str(error) == f"[NOT_IMPLEMENTED] {feature} is not implemented."


def test_reader_orc_takes_spark_signature(spark: ReparkSession) -> None:
    """DataFrameReader.orc takes Spark's signature and reaches the scan.

    IO-ORC-1 (2026-09-16): the read side is a real scan now; a missing path answers
    PATH_NOT_FOUND. The full read pins live in ``test_io_orc_1.py``.
    """
    with pytest.raises(AnalysisException) as raised:
        spark.read.orc(
            "/tmp/does-not-matter.orc",
            mergeSchema=True,
            pathGlobFilter="*.orc",
            recursiveFileLookup=True,
            modifiedBefore="2026-01-01",
            modifiedAfter="2020-01-01",
        )
    assert raised.value.getCondition() == "PATH_NOT_FOUND"


def test_reader_format_orc_loads_at_load(spark: ReparkSession) -> None:
    """format('orc').load reaches the scan at load, not at format.

    IO-ORC-1 (2026-09-16): the read side is a real scan now; a missing path answers
    PATH_NOT_FOUND. The full read pins live in ``test_io_orc_1.py``.
    """
    reader = spark.read.format("orc")
    with pytest.raises(AnalysisException) as raised:
        reader.load("/tmp/does-not-matter.orc")
    assert raised.value.getCondition() == "PATH_NOT_FOUND"


def test_reader_xml_without_row_tag_refuses(spark: ReparkSession) -> None:
    """spark.read.xml without rowTag raises Spark's XML_ROW_TAG_MISSING exactly.

    pins: io-declared-1/C-002
    """
    with pytest.raises(AnalysisException) as raised:
        spark.read.xml("/tmp/does-not-matter.xml")
    _assert_error_cell(raised.value, "xml_read_default_rowtag")
    assert raised.value.getSqlState() == "42KDF"


def test_reader_xml_with_row_tag_refuses(spark: ReparkSession) -> None:
    """spark.read.xml with rowTag refuses NOT_IMPLEMENTED xml at the call.

    Replaces the read arm of oracle cell ``xml_roundtrip`` (registry IO-XML-1).
    """
    with pytest.raises(PySparkNotImplementedError) as raised:
        spark.read.xml("/tmp/does-not-matter.xml", rowTag="row", schema=None)
    _assert_not_implemented(raised.value, "xml")


def test_reader_format_xml_row_tag_from_options(spark: ReparkSession) -> None:
    """format('xml') reads the rowTag check from the option map at load.

    pins: io-declared-1/C-002
    """
    reader = spark.read.format("xml")
    with pytest.raises(AnalysisException) as missing:
        reader.load("/tmp/does-not-matter.xml")
    _assert_error_cell(missing.value, "xml_read_default_rowtag")
    with pytest.raises(PySparkNotImplementedError) as raised:
        spark.read.format("xml").option("rowTag", "row").load("/tmp/does-not-matter.xml")
    _assert_not_implemented(raised.value, "xml")


def test_writer_orc_refuses_at_the_call(spark: ReparkSession) -> None:
    """DataFrameWriter.orc refuses NOT_IMPLEMENTED orc at the call.

    Replaces oracle cells ``orc_roundtrip`` / ``orc_nested_types`` /
    ``orc_compression_default`` (registry IO-ORC-1).
    """
    with pytest.raises(PySparkNotImplementedError) as raised:
        _kv_frame(spark).write.orc("/tmp/does-not-matter.orc")
    _assert_not_implemented(raised.value, "orc")
    with pytest.raises(PySparkNotImplementedError) as raised:
        _kv_frame(spark).write.orc("/tmp/does-not-matter.orc", mode="overwrite", compression="zstd")
    _assert_not_implemented(raised.value, "orc")


def test_writer_format_orc_refuses_at_save(spark: ReparkSession) -> None:
    """format('orc').save refuses NOT_IMPLEMENTED orc at save, not at format.

    pins: io-declared-1/C-001
    """
    writer = _kv_frame(spark).write.format("orc").mode("overwrite")
    with pytest.raises(PySparkNotImplementedError) as raised:
        writer.save("/tmp/does-not-matter.orc")
    _assert_not_implemented(raised.value, "orc")


def test_writer_xml_without_row_tag_refuses(spark: ReparkSession) -> None:
    """DataFrameWriter.xml without rowTag raises Spark's XML_ROW_TAG_MISSING exactly.

    pins: io-declared-1/C-002
    """
    with pytest.raises(AnalysisException) as raised:
        _kv_frame(spark).write.xml("/tmp/does-not-matter.xml")
    _assert_error_cell(raised.value, "xml_no_rowtag_write")
    assert raised.value.getSqlState() == "42KDF"


def test_writer_xml_with_row_tag_refuses(spark: ReparkSession) -> None:
    """DataFrameWriter.xml with rowTag refuses NOT_IMPLEMENTED xml at the call.

    Replaces the write arm of oracle cells ``xml_roundtrip`` /
    ``xml_write_default_ok`` (registry IO-XML-1).
    """
    with pytest.raises(PySparkNotImplementedError) as raised:
        _kv_frame(spark).write.xml("/tmp/does-not-matter.xml", rowTag="row")
    _assert_not_implemented(raised.value, "xml")
    with pytest.raises(PySparkNotImplementedError) as raised:
        _kv_frame(spark).write.option("rowTag", "row").xml("/tmp/does-not-matter.xml")
    _assert_not_implemented(raised.value, "xml")


def test_writer_jdbc_bad_mode_is_invalid_save_mode(spark: ReparkSession) -> None:
    """A bad mode on DataFrameWriter.jdbc raises Spark's INVALID_SAVE_MODE first.

    pins: io-declared-1/C-003
    """
    with pytest.raises(AnalysisException) as raised:
        _kv_frame(spark).write.jdbc("jdbc:postgresql://127.0.0.1:1/x", "t", mode="bogus")
    _assert_error_cell(raised.value, "writer_jdbc_mode_bad")
    assert raised.value.getSqlState() == "42000"


def test_writer_jdbc_mixed_case_modes_are_valid_then_refuse(spark: ReparkSession) -> None:
    """Spark's mode(String) lowercases before matching; mixed-case valid modes refuse.

    L-001: ``Append`` / ``OVERWRITE`` / ``ErrorIfExists`` / ``ERROR`` / ``Ignore`` /
    ``DEFAULT`` are valid Spark save modes, so under R-2/R-3 they reach
    ``NOT_IMPLEMENTED`` ``{"feature": "jdbc"}`` instead of ``INVALID_SAVE_MODE``.

    pins: io-declared-1/C-008
    """
    for mode in ("Append", "OVERWRITE", "ErrorIfExists", "ERROR", "Ignore", "DEFAULT"):
        with pytest.raises(PySparkNotImplementedError) as raised:
            _kv_frame(spark).write.jdbc("jdbc:postgresql://127.0.0.1:1/x", "t", mode=mode)
        _assert_not_implemented(raised.value, "jdbc")


def test_writer_jdbc_invalid_modes_keep_the_caller_spelling(spark: ReparkSession) -> None:
    """Invalid modes raise INVALID_SAVE_MODE naming the caller's original spelling.

    L-001: ``bogus``, the empty string, and a padded valid word stay invalid after
    lowercasing, and the message and ``mode`` parameter show them verbatim.

    pins: io-declared-1/C-008
    """
    for mode in ("bogus", "", "append "):
        with pytest.raises(AnalysisException) as raised:
            _kv_frame(spark).write.jdbc("jdbc:postgresql://127.0.0.1:1/x", "t", mode=mode)
        assert type(raised.value).__name__ == "AnalysisException"
        assert raised.value.getCondition() == "INVALID_SAVE_MODE"
        assert raised.value.getMessageParameters() == {"mode": f'"{mode}"'}
        assert str(raised.value) == (
            f'[INVALID_SAVE_MODE] The specified save mode "{mode}" is invalid. '
            'Valid save modes include "append", "overwrite", "ignore", "error", '
            '"errorifexists", and "default". SQLSTATE: 42000'
        )
        assert raised.value.getSqlState() == "42000"


def test_writer_jdbc_refuses_after_the_mode_check(spark: ReparkSession) -> None:
    """DataFrameWriter.jdbc refuses NOT_IMPLEMENTED jdbc once the mode is valid.

    Replaces oracle cell ``jdbc_write_no_driver`` (registry IO-JDBC-1).
    """
    with pytest.raises(PySparkNotImplementedError) as raised:
        _kv_frame(spark).write.jdbc("jdbc:postgresql://127.0.0.1:1/x", "t")
    _assert_not_implemented(raised.value, "jdbc")
    with pytest.raises(PySparkNotImplementedError) as raised:
        _kv_frame(spark).write.jdbc(
            "jdbc:postgresql://127.0.0.1:1/x", "t", mode="append", properties={"user": "u"}
        )
    _assert_not_implemented(raised.value, "jdbc")


def test_reader_jdbc_non_postgres_urls_refuse_at_the_call(spark: ReparkSession) -> None:
    """Non-PostgreSQL driver URLs refuse NOT_IMPLEMENTED jdbc at the call.

    R-3: PostgreSQL URLs read through the native connector (pinned in
    test_pg_jdbc_options.py); every other driver keeps the declared refusal.
    Replaces oracle cell ``jdbc_read_no_driver`` (registry IO-JDBC-1).
    """
    with pytest.raises(PySparkNotImplementedError) as raised:
        spark.read.jdbc("jdbc:mysql://127.0.0.1:1/x", "t")
    _assert_not_implemented(raised.value, "jdbc")


def test_reader_jdbc_non_postgres_props_refuse_at_the_call(spark: ReparkSession) -> None:
    """A non-PostgreSQL URL with Spark's camelCase partition signature refuses too.

    Replaces oracle cell ``reader_jdbc_props`` (registry IO-JDBC-1).
    """
    with pytest.raises(PySparkNotImplementedError) as raised:
        spark.read.jdbc(
            "jdbc:sqlserver://127.0.0.1:1/x",
            "t",
            column="a",
            lowerBound=1,
            upperBound=10,
            numPartitions=2,
        )
    _assert_not_implemented(raised.value, "jdbc")


def test_na_replace_basic(spark: ReparkSession) -> None:
    """na.replace scalar form answers oracle cell ``na_replace_basic``.

    pins: io-declared-1/C-004
    """
    _assert_frame_cell(_xy_null_frame(spark).na.replace("a", "z"), "na_replace_basic")


def test_na_replace_subset(spark: ReparkSession) -> None:
    """na.replace with a subset answers oracle cell ``na_replace_subset``.

    pins: io-declared-1/C-004
    """
    _assert_frame_cell(_xy_frame(spark).na.replace(1, 5, "x"), "na_replace_subset")


def test_na_replace_list(spark: ReparkSession) -> None:
    """na.replace list pair answers oracle cell ``na_replace_list``.

    pins: io-declared-1/C-004
    """
    _assert_frame_cell(_xy_frame(spark).na.replace([1, 2], [3, 4]), "na_replace_list")


def test_na_replace_none_value(spark: ReparkSession) -> None:
    """na.replace with an explicit None value answers cell ``na_replace_none_value``.

    pins: io-declared-1/C-004
    """
    _assert_frame_cell(_xy_frame(spark).na.replace("a", None), "na_replace_none_value")


def test_na_replace_dict_mixed_types_raise(spark: ReparkSession) -> None:
    """na.replace with a mixed-type dict raises Spark's MIXED_TYPE_REPLACEMENT.

    pins: io-declared-1/C-004
    """
    with pytest.raises(PySparkValueError) as raised:
        _xy_frame(spark).na.replace({1: 10, "b": "B"})
    _assert_error_cell(raised.value, "na_replace_dict")


def test_na_replace_novalue_nondict_raise(spark: ReparkSession) -> None:
    """na.replace with a non-dict to_replace and no value raises ARGUMENT_REQUIRED.

    pins: io-declared-1/C-004
    """
    with pytest.raises(PySparkTypeError) as raised:
        spark.createDataFrame([(1, "a")], "x int, y string").na.replace("a")
    _assert_error_cell(raised.value, "na_replace_novalue_nondict")


def test_na_replace_identity(spark: ReparkSession) -> None:
    """na.replace answers exactly DataFrame.replace (oracle cell ``na_replace_identity``).

    pins: io-declared-1/C-004
    """
    frame = _xy_frame(spark)
    through_na = frame.na.replace("a", "z").collect()
    through_frame = frame.replace("a", "z").collect()
    assert (through_na == through_frame) == _ORACLE["na_replace_identity"]["result"]["value"]
