from __future__ import annotations

import traceback
from pathlib import Path

import pytest

from repark.errors import AnalysisException, IllegalArgumentException, PySparkException
from repark.spark import SparkSession

MARK = "S3cr3tPw"

JDBC = f"jdbc:postgresql://h/db?user=u&password={MARK}"
DSN = f"host=h user=u password={MARK}"
ODBC = f"Driver=x;Server=h;Uid=u;Pwd={MARK};"

SHAPES = [JDBC, DSN, ODBC]

SHUFFLE = "spark.sql.shuffle.partitions"
MAX_ARRAY = "repark.sql.maxArrayElements"
DF_BATCH = "datafusion.execution.batch_size"
WRITE_FILES = "repark.write.max-concurrent-files"

KNOBS = [SHUFFLE, MAX_ARRAY, DF_BATCH, WRITE_FILES]

DOORS = ["builder", "toml", "confset", "sqlset"]

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
    expected = EXPECTED[(door, knob)]
    try:
        if door == "builder":
            _attempt_builder(knob, shape)
        elif door == "toml":
            _attempt_toml(knob, shape, tmp_path)
        elif door == "confset":
            _attempt_conf_set(knob, shape)
        else:
            _attempt_sql_set(knob, shape)
    except BaseException as error:
        assert expected is not None
        assert type(error) is expected
        assert MARK not in "".join(traceback.format_exception(error))
    else:
        assert expected is None
