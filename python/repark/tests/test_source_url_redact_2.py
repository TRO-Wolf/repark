from __future__ import annotations

import traceback
from pathlib import Path

import pytest

from repark.errors import AnalysisException, IllegalArgumentException, PySparkException
from repark.spark import SparkSession
from repark.spark._secrets import scrub_exception

MARK = "S3cr3tPw"

SHUFFLE = "spark.sql.shuffle.partitions"
MAX_ARRAY = "repark.sql.maxArrayElements"
DF_BATCH = "datafusion.execution.batch_size"
WRITE_FILES = "repark.write.max-concurrent-files"

KNOBS = [SHUFFLE, MAX_ARRAY, DF_BATCH, WRITE_FILES]

SHAPES = ["jdbc", "dsn", "odbc"]

DOORS = ["builder", "toml", "confset", "sqlset"]

GUARD_KNOBS = [
    SHUFFLE,
    "repark.memory.limit.gb",
    "spark.sql.execution.arrow.maxRecordsPerBatch",
]

USERINFO = "u" + ":pw@"


def _guard_value(kind: str) -> str:
    if kind == "backslash":
        return f"Driver=x;Uid=u;Pwd={MARK}\\9;"
    return f"Driver=x;Uid=u;Pwd={MARK};" + "Database=" + "d" * 300 + ";"


EXPECTED: dict[tuple[str, str], type[BaseException] | None] = {
    ("builder", SHUFFLE): IllegalArgumentException,
    ("builder", MAX_ARRAY): AnalysisException,
    ("builder", DF_BATCH): IllegalArgumentException,
    ("builder", WRITE_FILES): IllegalArgumentException,
    ("toml", SHUFFLE): IllegalArgumentException,
    ("toml", MAX_ARRAY): AnalysisException,
    ("toml", DF_BATCH): IllegalArgumentException,
    ("toml", WRITE_FILES): IllegalArgumentException,
    ("confset", SHUFFLE): None,
    ("confset", MAX_ARRAY): None,
    ("confset", DF_BATCH): IllegalArgumentException,
    ("confset", WRITE_FILES): None,
    ("sqlset", SHUFFLE): IllegalArgumentException,
    ("sqlset", MAX_ARRAY): None,
    ("sqlset", DF_BATCH): PySparkException,
    ("sqlset", WRITE_FILES): None,
}


def _shape_value(shape: str, token: str) -> str:
    if shape == "jdbc":
        return f"jdbc:postgresql://h/db?user=u&password={token}"
    if shape == "dsn":
        return f"host=h user=u password={token}"
    return f"Driver=x;Server=h;Uid=u;Pwd={token};"


def _attempt_builder(key: str, value: str) -> None:
    session = SparkSession.builder.config(key, value).getOrCreate()
    try:
        session.sql("SELECT 1").collect()
    finally:
        session.stop()


def _attempt_toml(key: str, value: str, tmp_path: Path) -> None:
    path = tmp_path / "repark.toml"
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    path.write_text(f'[default.conf]\n"{key}" = "{escaped}"\n', encoding="utf-8")
    session = SparkSession.builder.configFile(str(path)).getOrCreate()
    try:
        session.sql("SELECT 1").collect()
    finally:
        session.stop()


def _attempt_conf_set(key: str, value: str) -> None:
    session = SparkSession.builder.getOrCreate()
    try:
        session.conf.set(key, value)
        session.sql("SELECT array(1) AS a").collect()
    finally:
        session.stop()


def _attempt_sql_set(key: str, value: str) -> None:
    session = SparkSession.builder.getOrCreate()
    try:
        session.sql(f"SET {key} = '{value}'").collect()
        session.sql("SELECT array(1) AS a").collect()
    finally:
        session.stop()


@pytest.mark.parametrize("shape", SHAPES)
@pytest.mark.parametrize("knob", KNOBS)
@pytest.mark.parametrize("door", DOORS)
def test_config_value_never_echoes_a_credential(
    door: str, knob: str, shape: str, tmp_path: Path
) -> None:
    token = f"{MARK}{DOORS.index(door)}{KNOBS.index(knob)}{SHAPES.index(shape)}"
    value = _shape_value(shape, token)
    expected = EXPECTED[(door, knob)]
    try:
        if door == "builder":
            _attempt_builder(knob, value)
        elif door == "toml":
            _attempt_toml(knob, value, tmp_path)
        elif door == "confset":
            _attempt_conf_set(knob, value)
        else:
            _attempt_sql_set(knob, value)
    except BaseException as error:
        assert expected is not None
        assert type(error) is expected
        rendered = "".join(traceback.format_exception(error))
        assert MARK not in rendered
        assert "***" in rendered
    else:
        assert expected is None


@pytest.mark.parametrize("knob", GUARD_KNOBS)
@pytest.mark.parametrize("kind", ["backslash", "long"])
def test_credential_shaped_integer_refusal_cuts_the_chain(knob: str, kind: str) -> None:
    try:
        SparkSession.builder.config(knob, _guard_value(kind)).getOrCreate()
    except BaseException as error:
        assert type(error) is IllegalArgumentException
        assert error.__cause__ is None
        rendered = "".join(traceback.format_exception(error))
        assert MARK not in rendered
        assert "***" in rendered
    else:
        pytest.fail(f"{knob} accepted a non-integer")


@pytest.mark.parametrize("scheme", ["file", "hdfs"])
def test_orc_message_parameters_mask_url_userinfo(scheme: str) -> None:
    host = "localhost/tmp/nope/x" if scheme == "file" else "h/x"
    url = scheme + "://" + USERINFO + host
    session = SparkSession.builder.getOrCreate()
    try:
        session.read.orc(url).collect()
    except BaseException as error:
        assert type(error) is AnalysisException
        params = error.getMessageParameters()
        assert set(params) == {"path"}
        assert USERINFO not in repr(params)
    else:
        pytest.fail(f"orc read of a {scheme} userinfo path unexpectedly succeeded")
    finally:
        session.stop()


def test_rest_catalog_uri_chain_carries_no_userinfo() -> None:
    uri = "http://" + USERINFO + "127.0.0.1:9/"
    session = (
        SparkSession.builder.config("spark.sql.catalog.c", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.c.type", "rest")
        .config("spark.sql.catalog.c.uri", uri)
        .getOrCreate()
    )
    try:
        session.sql("SHOW NAMESPACES IN c").collect()
    except BaseException as error:
        rendered = "".join(traceback.format_exception(error))
        assert USERINFO not in rendered
    else:
        pytest.fail("rest catalog read unexpectedly succeeded")
    finally:
        session.stop()


def test_scrub_exception_masks_cause_chain_args() -> None:
    cause = ValueError("http://" + USERINFO + "127.0.0.1:9/refused")
    context = RuntimeError("during catalog read")
    context.__cause__ = cause
    outer = AnalysisException("listNamespaces failed")
    outer.__context__ = context
    scrub_exception(outer)
    assert USERINFO not in repr(cause.args)
    assert USERINFO not in "".join(traceback.format_exception(outer))


def test_reader_path_chain_carries_no_userinfo() -> None:
    url = "file://" + USERINFO + "localhost/tmp/nope/x"
    session = SparkSession.builder.getOrCreate()
    try:
        session.read.parquet(url).collect()
    except BaseException as error:
        rendered = "".join(traceback.format_exception(error))
        assert USERINFO not in rendered
    else:
        pytest.fail("parquet read unexpectedly succeeded")
    finally:
        session.stop()


def test_writer_option_value_never_echoes(tmp_path: Path) -> None:
    value = f"host=h user=u password={MARK}W4"
    session = SparkSession.builder.getOrCreate()
    try:
        session.range(2).write.option("compression", value).parquet(str(tmp_path / "w"))
    except BaseException as error:
        rendered = "".join(traceback.format_exception(error))
        assert MARK not in rendered
        assert "***" in rendered
    else:
        pytest.fail("writer option refusal unexpectedly succeeded")
    finally:
        session.stop()
