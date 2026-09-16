"""FNP-GEN-1 remediation pins: critic cells for corrupt records, timestamps and decimals."""

from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark import functions as F  # noqa: N812

FIXTURE_PATH = Path(__file__).parent / "fnp_gen_1_s34_critic_spark_oracle.json"
TIME_ZONE_KEY = "spark.sql.session.timeZone"
NEW_YORK_ZONE = "America/New_York"
TIMESTAMP_CSV = (
    "2024-01-01 10:00:00,2024-01-01T10:00:00.123,2024-01-01,"
    "2024-01-01 10:00:00+02:00,2024/01/01 10:00"
)
TIMESTAMP_SCHEMA = "t1 TIMESTAMP, t2 TIMESTAMP, t3 TIMESTAMP, t4 TIMESTAMP, t5 TIMESTAMP"
CORRUPT_SCHEMA = "a INT, b STRING, c DOUBLE, _corrupt_record STRING"
CORRUPT_OPTIONS = {"columnNameOfCorruptRecord": "_corrupt_record"}
MIDDLE_SCHEMA = "a INT, _corrupt_record STRING, b STRING"
MIDDLE_BAD_SCHEMA = "a INT, _corrupt_record INT"
DECIMAL_SCHEMA = "d DECIMAL(5,2), e DECIMAL(10,2), f DECIMAL(5,2), g DECIMAL(5,2)"
LADDER_CSV = (
    "2024-01-01 10:00:00.1,2024-01-01T10:00:00Z,2024-01-01T10:00:00,1.0E10,"
    "123456789012345678901234,2024-01-01 10:00,1.5f,01/02/2024"
)
LADDER_DDL = (
    "STRUCT<_c0: TIMESTAMP, _c1: TIMESTAMP, _c2: TIMESTAMP, _c3: DOUBLE, "
    "_c4: DECIMAL(24,0), _c5: TIMESTAMP, _c6: DOUBLE, _c7: STRING>"
)


def _oracle() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text())


def _cells(name: str, door: str, fragment: str, zone: str) -> list[dict[str, Any]]:
    cells = [
        cell
        for cell in _oracle()["cells"]
        if cell["name"] == name
        and cell["door"] == door
        and fragment in cell["expr"]
        and cell["session_time_zone"] == zone
    ]
    assert len(cells) == 2
    assert {cell["ansi"] for cell in cells} == {True, False}
    return cells


def _want_rows(cells: list[dict[str, Any]]) -> list[Any]:
    assert cells[0]["rows"] == cells[1]["rows"]
    return list(cells[0]["rows"])


def _ordered_rows(result: Any) -> list[list[Any]]:
    names = [field.name for field in result.schema.fields]
    return [[row[name] for name in names] for row in result.to_arrow().to_pylist()]


def _single_struct(result: Any) -> dict[str, Any]:
    rows = result.to_arrow().to_pylist()
    assert len(rows) == 1
    value = next(iter(rows[0].values()))
    assert isinstance(value, dict)
    return value


def _single_value(result: Any) -> Any:
    rows = result.to_arrow().to_pylist()
    assert len(rows) == 1
    return next(iter(rows[0].values()))


def _epoch(value: Any, zone: str) -> Any:
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=ZoneInfo(zone))
        return int(value.timestamp())
    return value


def _wall(value: Any, zone: str) -> Any:
    if not isinstance(value, datetime.datetime):
        return value
    if value.tzinfo is not None:
        value = value.astimezone(ZoneInfo(zone))
    text = value.strftime("%Y-%m-%d %H:%M:%S")
    if value.microsecond:
        text += "." + f"{value.microsecond:06d}".rstrip("0")
    return text


def _epochs_and_walls(struct: dict[str, Any], zone: str) -> list[Any]:
    return [
        item
        for index in range(1, 6)
        for item in (
            _epoch(struct[f"t{index}"], zone),
            _wall(struct[f"t{index}"], zone),
        )
    ]


def _render_struct(struct: dict[str, Any]) -> str:
    return "{" + ", ".join("null" if value is None else str(value) for value in struct.values()) + "}"


def session_range_select(session: ReparkSession, column: Any) -> Any:
    """Select one column over a one-row frame."""
    return session.range(1).select(column.alias("s"))


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-fnp-gen-1-s34-critic").getOrCreate()
    yield session
    session.stop()


def _timestamp_struct(session: ReparkSession) -> dict[str, Any]:
    return _single_struct(
        session.range(1).select(F.from_csv(F.lit(TIMESTAMP_CSV), TIMESTAMP_SCHEMA).alias("s"))
    )


def test_python_door_from_csv_default_timestamps_utc(spark: ReparkSession) -> None:
    """Default patterns parse space, T-fraction, date-only and offset stamps in UTC."""
    spark.conf.set(TIME_ZONE_KEY, "UTC")
    cells = _cells("from_csv", "python", "each field cast long and string", "UTC")
    assert [_epochs_and_walls(_timestamp_struct(spark), "UTC")] == _want_rows(cells)


def test_python_door_from_csv_default_timestamps_new_york(spark: ReparkSession) -> None:
    """Naive stamps shift with the session zone while the offset stamp stays fixed."""
    spark.conf.set(TIME_ZONE_KEY, NEW_YORK_ZONE)
    cells = _cells("from_csv", "python", "each field cast long and string", NEW_YORK_ZONE)
    assert [_epochs_and_walls(_timestamp_struct(spark), NEW_YORK_ZONE)] == _want_rows(cells)


def test_python_door_from_csv_timestamp_format_option(spark: ReparkSession) -> None:
    """A ``timestampFormat`` parses the slash stamp in both session zones."""
    for zone in ("UTC", NEW_YORK_ZONE):
        spark.conf.set(TIME_ZONE_KEY, zone)
        cells = _cells("from_csv", "python", "timestampFormat", zone)
        struct = _single_struct(
            session_range_select(
                spark,
                F.from_csv(
                    F.lit("2024/01/01 10:00"),
                    "t TIMESTAMP",
                    {"timestampFormat": "yyyy/MM/dd HH:mm"},
                ),
            )
        )
        assert [[_epoch(struct["t"], zone), _wall(struct["t"], zone)]] == _want_rows(cells)


def test_python_door_from_csv_timestamp_ntz_stays_wall_clock(spark: ReparkSession) -> None:
    """``TIMESTAMP_NTZ`` answers the wall clock in both session zones."""
    for zone in ("UTC", NEW_YORK_ZONE):
        spark.conf.set(TIME_ZONE_KEY, zone)
        cells = _cells("from_csv", "python", "TIMESTAMP_NTZ", zone)
        struct = _single_struct(
            session_range_select(spark, F.from_csv(F.lit("2024-01-01 10:00:00"), "t TIMESTAMP_NTZ"))
        )
        assert [[_wall(struct["t"], zone)]] == _want_rows(cells)


def test_sql_door_from_csv_default_timestamp_value(spark: ReparkSession) -> None:
    """The SQL door parses the default space stamp in both session zones."""
    for zone in ("UTC", NEW_YORK_ZONE):
        spark.conf.set(TIME_ZONE_KEY, zone)
        cells = _cells("from_csv", "sql", "CAST(from_csv", zone)
        result = spark.sql("SELECT from_csv('2024-01-01 10:00:00', 't TIMESTAMP')")
        assert [_epoch(_single_struct(result)["t"], zone)] == cells[0]["rows"][0]


@pytest.mark.xfail(
    strict=True,
    reason="FNP-GEN-1 remediation: struct-field access on a call result needs the "
    "DataFusion SQL dot-access seam, owned by run 18c (R-18a-5).",
)
def test_sql_door_from_csv_dot_access_cells(spark: ReparkSession) -> None:
    """The exact critic SQL exprs answer the epoch once the dot-access seam lands."""
    cells = [
        cell
        for cell in _oracle()["cells"]
        if cell["name"] == "from_csv"
        and cell["door"] == "sql"
        and "CAST(from_csv" in cell["expr"]
    ]
    assert len(cells) == 4
    for cell in cells:
        spark.conf.set(TIME_ZONE_KEY, str(cell["session_time_zone"]))
        result = spark.sql(str(cell["expr"]))
        assert _ordered_rows(result) == cell["rows"]


def test_python_door_from_csv_extra_token_marks_corrupt(spark: ReparkSession) -> None:
    """An extra token marks the record corrupt and keeps the parsed fields."""
    cells = _cells("from_csv", "python", "c DOUBLE, _corrupt_record STRING", "UTC")
    result = spark.range(1).select(
        F.from_csv(F.lit("1,abc,2.5,EXTRA"), CORRUPT_SCHEMA, CORRUPT_OPTIONS)
    )
    assert _single_struct(result) == cells[0]["rows"][0][0]["row"]


def test_python_door_from_csv_extra_token_without_corrupt_column(
    spark: ReparkSession,
) -> None:
    """Without a corrupt column the extra token drops and the fields parse."""
    cells = _cells("from_csv", "python", "c DOUBLE')", "UTC")
    result = spark.range(1).select(
        F.from_csv(F.lit("1,abc,2.5,EXTRA"), "a INT, b STRING, c DOUBLE")
    )
    assert _single_struct(result) == cells[0]["rows"][0][0]["row"]


def test_python_door_from_csv_corrupt_column_middle_wellformed(
    spark: ReparkSession,
) -> None:
    """A well-formed row leaves a middle corrupt column NULL."""
    cells = _cells("from_csv", "python", "from_csv(lit('1,x')", "UTC")
    result = spark.range(1).select(F.from_csv(F.lit("1,x"), MIDDLE_SCHEMA, CORRUPT_OPTIONS))
    assert _single_struct(result) == cells[0]["rows"][0][0]["row"]


def test_python_door_from_csv_corrupt_column_middle_malformed(
    spark: ReparkSession,
) -> None:
    """A malformed row writes the whole record into a middle corrupt column."""
    cells = _cells("from_csv", "python", "from_csv(lit('q,x,y')", "UTC")
    result = spark.range(1).select(F.from_csv(F.lit("q,x,y"), MIDDLE_SCHEMA, CORRUPT_OPTIONS))
    assert _single_struct(result) == cells[0]["rows"][0][0]["row"]


def test_python_door_from_csv_non_string_corrupt_column_refuses(
    spark: ReparkSession,
) -> None:
    """A non-STRING corrupt column raises ``INVALID_CORRUPT_RECORD_TYPE`` at analysis."""
    cells = _cells("from_csv", "python", "_corrupt_record INT", "UTC")
    assert all(cell["error_condition"] == "INVALID_CORRUPT_RECORD_TYPE" for cell in cells)
    assert all(cell["error_type"] == "AnalysisException" for cell in cells)
    with pytest.raises(AnalysisException, match="INVALID_CORRUPT_RECORD_TYPE"):
        spark.range(1).select(
            F.from_csv(F.lit("q"), MIDDLE_BAD_SCHEMA, CORRUPT_OPTIONS)
        ).collect()
    with pytest.raises(AnalysisException, match="INVALID_CORRUPT_RECORD_TYPE"):
        spark.range(1).select(
            F.from_csv(F.lit("1"), MIDDLE_BAD_SCHEMA, CORRUPT_OPTIONS)
        ).collect()


def test_sql_door_from_csv_non_string_corrupt_column_refuses(
    spark: ReparkSession,
) -> None:
    """The SQL door refuses a non-STRING corrupt column at analysis."""
    with pytest.raises(AnalysisException, match="INVALID_CORRUPT_RECORD_TYPE"):
        spark.sql(
            "SELECT from_csv('q', 'a INT, _corrupt_record INT', "
            "map('columnNameOfCorruptRecord', '_corrupt_record'))"
        ).collect()


def test_python_door_from_csv_decimal_rounds_half_up(spark: ReparkSession) -> None:
    """Decimals round HALF_UP, take scientific notation and NULL on overflow."""
    cells = _cells("from_csv", "python", "DECIMAL(5,2)", "UTC")
    result = spark.range(1).select(
        F.from_csv(F.lit("1.239,1e2,123456.7,-0.005"), DECIMAL_SCHEMA)
    )
    assert _render_struct(_single_struct(result)) == cells[0]["rows"][0][0]


def test_python_door_from_csv_empty_frame(spark: ReparkSession) -> None:
    """A zero-row input answers an empty frame with the struct type."""
    cells = _cells("from_csv", "python", "spark.range(0)", "UTC")
    assert all(cell["rows"] == [] for cell in cells)
    result = spark.range(0).select(
        F.from_csv(F.concat(F.lit("1"), F.col("id").cast("string")), "a INT")
    )
    assert result.to_arrow().num_rows == 0
    fields = result.schema.fields
    assert len(fields) == 1
    assert fields[0].dataType.simpleString() == "struct<a:int>"


def test_python_door_schema_of_csv_inference_ladder(spark: ReparkSession) -> None:
    """Fractional and ``Z`` stamps, big integers, ``1.5f`` and slash dates infer the ladder."""
    cells = _cells("schema_of_csv", "python", "2024-01-01 10:00:00.1", "UTC")
    assert cells[0]["rows"][0][0] == LADDER_DDL
    result = spark.range(1).select(F.schema_of_csv(LADDER_CSV)).limit(1)
    assert _single_value(result) == LADDER_DDL


def test_python_door_json_tuple_trailing_content(spark: ReparkSession) -> None:
    """Trailing bytes after a document still answer the first value."""
    cells = _cells("json_tuple", "python", "trailing", "UTC")
    frame = spark.createDataFrame(
        [('{"a":1} trailing',), ('{"a":1}{"b":2}',), ('  {"a":2}  ',)], "js string"
    )
    assert _ordered_rows(frame.select(F.json_tuple("js", "a"))) == cells[0]["rows"]
