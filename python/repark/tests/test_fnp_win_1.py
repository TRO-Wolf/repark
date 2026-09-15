"""FNP-WIN-1 pins for window, window_time, session_window over the live oracle.

pins: fnp-win-1/C-001, C-002, C-003, C-004, C-005, C-007,
    C-009, C-010, C-011, C-012, C-013, C-014, C-015
"""

from __future__ import annotations

import datetime
import inspect
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom
from repark.spark.column import Column

_ORACLE_PATH = Path(__file__).with_name("fnp_win_1_spark_oracle.json")
_ORACLE: dict[str, Any] | None = None


def _oracle() -> dict[str, Any]:
    """Load the copied live-PySpark-4.1.2 oracle fixture."""
    global _ORACLE
    if _ORACLE is None:
        _ORACLE = json.loads(_ORACLE_PATH.read_text())
    assert _ORACLE is not None
    return _ORACLE


def _cells(name: str, door: str, key: str) -> list[dict[str, Any]]:
    """Return oracle cells for one name, door, and expression fragment."""
    cells = [
        cell
        for cell in _oracle()["cells"]
        if cell.get("name") == name and cell["door"] == door and key in cell["expr"]
    ]
    assert cells, f"oracle has no {name}/{door} cell matching {key!r}"
    return cells


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("fnp-win-1").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def frame(spark: ReparkSession) -> Any:
    rows = [
        (1, datetime.datetime(2024, 1, 1, 10, 7, 30), "k1"),
        (2, datetime.datetime(2024, 1, 1, 10, 12, 0), "k1"),
        (3, datetime.datetime(2024, 1, 1, 10, 31, 0), "k2"),
    ]
    built = spark.createDataFrame(rows, ["id", "ts", "key"])
    built.createOrReplaceTempView("fnp_win_1_frame")
    return built


def _set_ansi(spark: ReparkSession, enabled: bool) -> None:
    spark.conf.set("spark.sql.ansi.enabled", "true" if enabled else "false")


def _string_column(table: pa.Table, name: str) -> list[Any]:
    return table.column(name).to_pylist()


def test_window_signature_matches_pyspark() -> None:
    """pins: fnp-win-1/C-001"""
    assert callable(F.window)
    parameters = list(inspect.signature(F.window).parameters.values())
    assert [parameter.name for parameter in parameters] == [
        "timeColumn",
        "windowDuration",
        "slideDuration",
        "startTime",
    ]
    assert parameters[2].default is None
    assert parameters[3].default is None


def test_window_time_signature_matches_pyspark() -> None:
    """pins: fnp-win-1/C-001"""
    assert callable(F.window_time)
    parameters = list(inspect.signature(F.window_time).parameters.values())
    assert [parameter.name for parameter in parameters] == ["windowColumn"]


def test_session_window_signature_matches_pyspark() -> None:
    """pins: fnp-win-1/C-001"""
    assert callable(F.session_window)
    parameters = list(inspect.signature(F.session_window).parameters.values())
    assert [parameter.name for parameter in parameters] == ["timeColumn", "gapDuration"]
    assert parameters[1].default is inspect.Parameter.empty


def test_window_tumbling_groupby(spark: ReparkSession, frame: Any) -> None:
    """pins: fnp-win-1/C-002"""
    cells = _cells("window", "python", "groupBy(window(ts,'10 minutes')).count()")
    assert len(cells) == 2
    for cell in cells:
        _set_ansi(spark, bool(cell["ansi"]))
        table = (
            frame.groupBy(F.window("ts", "10 minutes"))
            .count()
            .select(
                F.col("window").getField("start").cast("string").alias("s"),
                F.col("window").getField("end").cast("string").alias("e"),
                "count",
            )
            .orderBy("s")
            .to_arrow()
        )
        assert table.schema.field("s").type == pa.string()
        assert table.schema.field("e").type == pa.string()
        assert table.schema.field("count").type == pa.int64()
        assert list(
            zip(
                _string_column(table, "s"),
                _string_column(table, "e"),
                table.column("count").to_pylist(),
                strict=True,
            )
        ) == [(row[0], row[1], row[2]) for row in cell["rows"]]


def test_window_tumbling_schema(spark: ReparkSession, frame: Any) -> None:
    """pins: fnp-win-1/C-002"""
    cells = _cells("window", "python", "window(ts,'10 minutes') schema")
    assert len(cells) == 2
    for cell in cells:
        _set_ansi(spark, bool(cell["ansi"]))
        table = frame.groupBy(F.window("ts", "10 minutes")).count().limit(0).to_arrow()
        assert table.num_rows == 0
        assert table.schema.names == [column["name"] for column in cell["columns"]]
        window_field = table.schema.field("window")
        assert pa.types.is_struct(window_field.type)
        assert window_field.type.names == ["start", "end"]
        assert all(
            pa.types.is_timestamp(window_field.type.field(name).type) for name in ("start", "end")
        )
        assert window_field.nullable == cell["columns"][0]["nullable"]
        assert table.schema.field("count").type == pa.int64()


def test_window_sliding_groupby(spark: ReparkSession, frame: Any) -> None:
    """pins: fnp-win-1/C-002"""
    cells = _cells("window", "python", "sliding window(ts,'10 minutes','5 minutes')")
    assert len(cells) == 2
    for cell in cells:
        _set_ansi(spark, bool(cell["ansi"]))
        table = (
            frame.groupBy(F.window("ts", "10 minutes", "5 minutes"))
            .agg(F.sum("id").alias("s"))
            .select(F.col("window").getField("start").cast("string").alias("s0"), "s")
            .orderBy("s0")
            .to_arrow()
        )
        assert [
            (row[0], row[1])
            for row in zip(_string_column(table, "s0"), table.column("s").to_pylist(), strict=True)
        ] == [(row[0], row[1]) for row in cell["rows"]]


def test_window_starttime_groupby(spark: ReparkSession, frame: Any) -> None:
    """pins: fnp-win-1/C-002"""
    cells = _cells("window", "python", "window with startTime")
    assert len(cells) == 2
    for cell in cells:
        _set_ansi(spark, bool(cell["ansi"]))
        table = (
            frame.groupBy(F.window(F.col("ts"), "10 minutes", startTime="2 minutes"))
            .count()
            .select(F.col("window").getField("start").cast("string").alias("s"), "count")
            .orderBy("s")
            .to_arrow()
        )
        assert list(
            zip(_string_column(table, "s"), table.column("count").to_pylist(), strict=True)
        ) == [(row[0], row[1]) for row in cell["rows"]]


def test_window_plain_select(spark: ReparkSession, frame: Any) -> None:
    """pins: fnp-win-1/C-002"""
    cells = _cells("window", "python", "window select not in groupBy")
    assert len(cells) == 2
    for cell in cells:
        _set_ansi(spark, bool(cell["ansi"]))
        table = (
            frame.select(F.window("ts", "10 minutes").alias("w"))
            .select(F.col("w").getField("start").cast("string"))
            .to_arrow()
        )
        assert table.schema.field(0).type == pa.string()
        assert [[value] for value in _string_column(table, table.schema.names[0])] == cell["rows"]


def test_window_bad_duration_raises(spark: ReparkSession, frame: Any) -> None:
    """pins: fnp-win-1/C-005"""
    cells = _cells("window", "python", "window bad duration")
    assert len(cells) == 2
    assert all(cell.get("error_condition") == "CANNOT_PARSE_INTERVAL" for cell in cells)
    for cell in cells:
        _set_ansi(spark, bool(cell["ansi"]))
        with pytest.raises(AnalysisException, match=r"\[CANNOT_PARSE_INTERVAL\]"):
            frame.groupBy(F.window("ts", "10 parsecs")).count().to_arrow()


def test_window_sql_tumbling_groupby(spark: ReparkSession, frame: Any) -> None:
    """pins: fnp-win-1/C-002"""
    cells = _cells("window", "sql", "GROUP BY window(ts, '10 minutes')")
    assert len(cells) == 2
    for cell in cells:
        _set_ansi(spark, bool(cell["ansi"]))
        table = spark.sql(
            "SELECT CAST(window.start AS STRING) s, CAST(window.end AS STRING) e, count(*) c "
            "FROM fnp_win_1_frame GROUP BY window(ts, '10 minutes') ORDER BY s"
        ).to_arrow()
        assert table.schema.names == ["s", "e", "c"]
        assert list(
            zip(
                _string_column(table, "s"),
                _string_column(table, "e"),
                table.column("c").to_pylist(),
                strict=True,
            )
        ) == [(row[0], row[1], row[2]) for row in cell["rows"]]


def test_window_sql_sliding_schema(spark: ReparkSession, frame: Any) -> None:
    """pins: fnp-win-1/C-002"""
    cells = _cells("window", "sql", "SELECT window(ts, '10 minutes', '5 minutes')")
    assert len(cells) == 2
    for cell in cells:
        _set_ansi(spark, bool(cell["ansi"]))
        table = spark.sql(
            "SELECT window(ts, '10 minutes', '5 minutes') FROM fnp_win_1_frame LIMIT 0"
        ).to_arrow()
        assert table.num_rows == 0
        assert table.schema.names == [column["name"] for column in cell["columns"]]
        window_field = table.schema.field("window")
        assert pa.types.is_struct(window_field.type)
        assert window_field.type.names == ["start", "end"]


def test_window_time_values(spark: ReparkSession, frame: Any) -> None:
    """pins: fnp-win-1/C-003"""
    cells = _cells("window_time", "python", "window_time after window")
    assert len(cells) == 2
    for cell in cells:
        _set_ansi(spark, bool(cell["ansi"]))
        table = (
            frame.groupBy(F.window("ts", "10 minutes"))
            .agg(F.count("*").alias("c"))
            .select(F.window_time("window").cast("string").alias("wt"), "c")
            .orderBy("wt")
            .to_arrow()
        )
        assert list(
            zip(_string_column(table, "wt"), table.column("c").to_pylist(), strict=True)
        ) == [(row[0], row[1]) for row in cell["rows"]]


def test_window_time_schema(spark: ReparkSession, frame: Any) -> None:
    """pins: fnp-win-1/C-003"""
    cells = _cells("window_time", "python", "window_time schema")
    assert len(cells) == 2
    for cell in cells:
        _set_ansi(spark, bool(cell["ansi"]))
        table = (
            frame.groupBy(F.window("ts", "10 minutes"))
            .agg(F.count("*").alias("c"))
            .select(F.window_time("window").alias("wt"))
            .limit(0)
            .to_arrow()
        )
        assert table.num_rows == 0
        assert table.schema.names == ["wt"]
        assert pa.types.is_timestamp(table.schema.field("wt").type)


def test_window_time_sql_missing_aggregation(spark: ReparkSession, frame: Any) -> None:
    """pins: fnp-win-1/C-005"""
    cells = _cells("window_time", "sql", "window_time(window)")
    assert len(cells) == 2
    assert all(cell.get("error_condition") == "MISSING_AGGREGATION" for cell in cells)
    for cell in cells:
        _set_ansi(spark, bool(cell["ansi"]))
        with pytest.raises(AnalysisException, match=r"\[MISSING_AGGREGATION\]"):
            spark.sql(
                "SELECT CAST(window_time(window) AS STRING) wt, count(*) FROM fnp_win_1_frame "
                "GROUP BY window(ts, '10 minutes') ORDER BY wt"
            ).to_arrow()


def test_session_window_static_gaps(spark: ReparkSession, frame: Any) -> None:
    """pins: fnp-win-1/C-004, C-012"""
    cells = _cells("session_window", "python", "session_window(ts,'5 minutes')")
    assert len(cells) == 2
    for cell in cells:
        _set_ansi(spark, bool(cell["ansi"]))
        table = (
            frame.groupBy("key", F.session_window("ts", "5 minutes"))
            .count()
            .select(
                "key",
                F.col("session_window").getField("start").cast("string").alias("s"),
                F.col("session_window").getField("end").cast("string").alias("e"),
                "count",
            )
            .orderBy("key", "s")
            .to_arrow()
        )
        assert list(
            zip(
                _string_column(table, "key"),
                _string_column(table, "s"),
                _string_column(table, "e"),
                table.column("count").to_pylist(),
                strict=True,
            )
        ) == [(row[0], row[1], row[2], row[3]) for row in cell["rows"]]
    wide = _cells("session_window", "python", "session_window(ts,'30 minutes')")
    assert len(wide) == 2
    for cell in wide:
        _set_ansi(spark, bool(cell["ansi"]))
        table = (
            frame.groupBy(F.session_window("ts", "30 minutes"))
            .count()
            .select(
                F.col("session_window").getField("start").cast("string").alias("s"),
                F.col("session_window").getField("end").cast("string").alias("e"),
                "count",
            )
            .orderBy("s")
            .to_arrow()
        )
        assert list(
            zip(
                _string_column(table, "s"),
                _string_column(table, "e"),
                table.column("count").to_pylist(),
                strict=True,
            )
        ) == [(row[0], row[1], row[2]) for row in cell["rows"]]


def test_session_window_schema(spark: ReparkSession, frame: Any) -> None:
    """pins: fnp-win-1/C-004"""
    cells = _cells("session_window", "python", "session_window schema")
    assert len(cells) == 2
    for cell in cells:
        _set_ansi(spark, bool(cell["ansi"]))
        table = frame.groupBy(F.session_window("ts", "5 minutes")).count().limit(0).to_arrow()
        assert table.num_rows == 0
        assert table.schema.names == [column["name"] for column in cell["columns"]]
        window_field = table.schema.field("session_window")
        assert pa.types.is_struct(window_field.type)
        assert window_field.type.names == ["start", "end"]


def test_session_window_dynamic_gap(spark: ReparkSession, frame: Any) -> None:
    """pins: fnp-win-1/C-004"""
    cells = _cells("session_window", "python", "session_window dynamic gap")
    assert len(cells) == 2
    for cell in cells:
        _set_ansi(spark, bool(cell["ansi"]))
        gap: Column = F.when(F.col("id") == 1, "20 minutes").otherwise("1 minute")
        table = (
            frame.groupBy(F.session_window("ts", gap))
            .count()
            .select(
                F.col("session_window").getField("start").cast("string").alias("s"),
                F.col("session_window").getField("end").cast("string").alias("e"),
                "count",
            )
            .orderBy("s")
            .to_arrow()
        )
        assert list(
            zip(
                _string_column(table, "s"),
                _string_column(table, "e"),
                table.column("count").to_pylist(),
                strict=True,
            )
        ) == [(row[0], row[1], row[2]) for row in cell["rows"]]


def _zoned_session(zone: str) -> ReparkSession:
    """Build a throwaway session fixed to one time zone."""
    return (
        ReparkSession.builder.appName("fnp-win-1-residual")
        .config("spark.sql.session.timeZone", zone)
        .getOrCreate()
    )


def _rcell(cell_id: str, door: str) -> dict[str, Any]:
    """Fetch one residual oracle cell by id and door."""
    matches = [
        cell
        for cell in _oracle()["cells"]
        if cell.get("id") == cell_id and cell.get("door") == door
    ]
    assert matches, f"oracle has no residual cell {cell_id!r} for {door!r}"
    return matches[0]


def _resid_datetimes(values: list[str]) -> list[datetime.datetime]:
    """Naive wall-clock datetimes matching the recorder's TIMESTAMP literals."""
    return [
        datetime.datetime(
            *[int(part) for part in value.replace("-", " ").replace(":", " ").split()]
        )
        for value in values
    ]


def _resid_make(session: ReparkSession, kind: str) -> None:
    """Build the recorder's residual frame for one kind as temp view wr."""
    base = ["2026-09-15 10:07:30", "2026-09-15 10:12:00", "2026-09-15 10:31:00"]
    dst = [
        "2026-03-08 01:10:00",
        "2026-03-08 01:50:00",
        "2026-03-08 03:10:00",
        "2026-11-01 01:10:00",
        "2026-11-01 01:50:00",
    ]
    if kind in ("ntz-base", "ntz-dst"):
        values = base if kind == "ntz-base" else dst
        rows = [(index, stamp) for index, stamp in enumerate(_resid_datetimes(values))]
        session.createDataFrame(rows, "id int, ts timestamp_ntz").createOrReplaceTempView("wr")
    elif kind == "dst":
        rows = [(index, stamp) for index, stamp in enumerate(_resid_datetimes(dst))]
        session.createDataFrame(rows, ["id", "ts"]).createOrReplaceTempView("wr")
    elif kind == "pre-epoch":
        rows = [
            (0, datetime.datetime(1969, 12, 31, 23, 55, 0)),
            (1, datetime.datetime(1969, 12, 31, 23, 59, 59)),
        ]
        session.createDataFrame(rows, ["id", "ts"]).createOrReplaceTempView("wr")
    elif kind == "null":
        rows = [(1, datetime.datetime(2026, 9, 15, 10, 0, 0)), (2, None)]
        session.createDataFrame(rows, ["id", "ts"]).createOrReplaceTempView("wr")
    elif kind == "date":
        rows = [(datetime.date(2026, 9, 15),), (datetime.date(2026, 9, 16),)]
        session.createDataFrame(rows, ["ts"]).createOrReplaceTempView("wr")
    else:
        rows = [(index, stamp) for index, stamp in enumerate(_resid_datetimes(base))]
        session.createDataFrame(rows, ["id", "ts"]).createOrReplaceTempView("wr")


def _resid_window_python(
    session: ReparkSession,
    duration: str,
    slide: str | None = None,
    start: str | None = None,
) -> pa.Table:
    """Run the recorder's python-door window query on temp view wr."""
    if slide is None and start is None:
        window = F.window("ts", duration)
    else:
        window = F.window("ts", duration, slide, start)
    return (
        session.table("wr")
        .groupBy(window)
        .count()
        .select(
            F.col("window").getField("start").cast("string").alias("start"),
            F.col("window").getField("end").cast("string").alias("end"),
            "count",
        )
        .orderBy("start")
        .to_arrow()
    )


def _resid_window_sql(session: ReparkSession, args: str) -> pa.Table:
    """Run the recorder's sql-door window query on temp view wr."""
    return session.sql(
        "SELECT CAST(window.start AS STRING) AS start, "
        "CAST(window.end AS STRING) AS end, count(*) AS count "
        f"FROM wr GROUP BY window(ts, {args}) ORDER BY start"
    ).to_arrow()


def _resid_session_python(session: ReparkSession, gap: str) -> pa.Table:
    """Run the recorder's python-door session query on temp view wr."""
    return (
        session.table("wr")
        .groupBy(F.session_window("ts", gap))
        .count()
        .select(
            F.col("session_window").getField("start").cast("string").alias("start"),
            F.col("session_window").getField("end").cast("string").alias("end"),
            "count",
        )
        .orderBy("start")
        .to_arrow()
    )


def _assert_resid_rows(cell: dict[str, Any], table: pa.Table) -> None:
    """Table rows equal the oracle cell rows, stringified like the recorder."""
    names = table.schema.names
    assert [column[0] for column in cell["schema"]] == names
    strings = [[str(value) for value in _string_column(table, name)] for name in names]
    assert list(zip(*strings, strict=True)) == [tuple(row) for row in cell["rows"]]


def test_resid_zone_tumble() -> None:
    """pins: fnp-win-1/C-009"""
    for zone, tag in (("UTC", "utc"), ("America/New_York", "ny")):
        session = _zoned_session(zone)
        _resid_make(session, "base")
        for span, duration, args in (
            ("1h", "1 hour", "'1 hour'"),
            ("1d", "1 day", "'1 day'"),
        ):
            for door, table in (
                ("python", _resid_window_python(session, duration)),
                ("sql", _resid_window_sql(session, args)),
            ):
                _assert_resid_rows(_rcell(f"R-zone-tumble-{span}-{tag}", door), table)
        session.stop()


def test_resid_dst_sliding() -> None:
    """pins: fnp-win-1/C-010"""
    for zone, tag in (("UTC", "utc"), ("America/New_York", "ny")):
        session = _zoned_session(zone)
        _resid_make(session, "dst")
        for door, table in (
            ("python", _resid_window_python(session, "1 hour", "30 minutes")),
            ("sql", _resid_window_sql(session, "'1 hour', '30 minutes'")),
        ):
            _assert_resid_rows(_rcell(f"R-dst-slide-1h-30m-{tag}", door), table)
        session.stop()
    session = _zoned_session("UTC")
    _resid_make(session, "dst")
    _assert_resid_rows(
        _rcell("R-dst-tumble-1d-utc", "python"), _resid_window_python(session, "1 day")
    )
    session.stop()
    session = _zoned_session("America/New_York")
    _resid_make(session, "dst")
    _assert_resid_rows(
        _rcell("R-dst-tumble-1d-ny", "python"), _resid_window_python(session, "1 day")
    )
    session.stop()


def test_resid_ntz_tumble() -> None:
    """pins: fnp-win-1/C-012"""
    for zone, tag in (("UTC", "utc"), ("America/New_York", "ny")):
        session = _zoned_session(zone)
        _resid_make(session, "ntz-base")
        for door, table in (
            ("python", _resid_window_python(session, "10 minutes")),
            ("sql", _resid_window_sql(session, "'10 minutes'")),
        ):
            _assert_resid_rows(_rcell(f"R-ntz-tumble-10m-{tag}", door), table)
        session.stop()
    session = _zoned_session("UTC")
    _resid_make(session, "ntz-dst")
    _assert_resid_rows(
        _rcell("R-ntz-dst-slide-utc", "python"),
        _resid_window_python(session, "1 hour", "30 minutes"),
    )
    session.stop()
    session = _zoned_session("America/New_York")
    _resid_make(session, "ntz-dst")
    _assert_resid_rows(
        _rcell("R-ntz-dst-slide-ny", "python"),
        _resid_window_python(session, "1 hour", "30 minutes"),
    )
    session.stop()


def _assert_ntz_struct(table: pa.Table, name: str) -> None:
    """The grouping column is a non-null struct of non-null tz-naive micros."""
    field = table.schema.field(name)
    assert field.type == pa.struct(
        [
            pa.field("start", pa.timestamp("us"), nullable=False),
            pa.field("end", pa.timestamp("us"), nullable=False),
        ]
    )
    assert not field.nullable


def _resid_struct_rows(table: pa.Table, name: str, count: str) -> list[tuple[str, str]]:
    """Raw grouping structs rendered like the recorder's Row strings."""
    structs = table.column(name).to_pylist()
    counts = [str(value) for value in table.column(count).to_pylist()]
    return sorted(
        (f"Row(start={row['start']!r}, end={row['end']!r})", number)
        for row, number in zip(structs, counts, strict=True)
    )


def test_resid_ntz_schemas() -> None:
    """pins: fnp-win-1/C-012"""
    session = _zoned_session("UTC")
    _resid_make(session, "ntz-base")
    window_table = session.table("wr").groupBy(F.window("ts", "10 minutes")).count().to_arrow()
    _assert_ntz_struct(window_table, "window")
    assert _resid_struct_rows(window_table, "window", "count") == sorted(
        tuple(row) for row in _rcell("R-window-ntz-schema", "python")["rows"]
    )
    session_table = (
        session.table("wr").groupBy(F.session_window("ts", "5 minutes")).count().to_arrow()
    )
    _assert_ntz_struct(session_table, "session_window")
    assert _resid_struct_rows(session_table, "session_window", "count") == sorted(
        tuple(row) for row in _rcell("R-session-ntz-5m-schema", "python")["rows"]
    )
    session.stop()


def test_resid_month_refusal() -> None:
    """pins: fnp-win-1/C-011"""
    session = _zoned_session("UTC")
    _resid_make(session, "base")
    cases = [
        ("R-month-window", "python", lambda: _resid_window_python(session, "1 month")),
        ("R-month-window", "sql", lambda: _resid_window_sql(session, "'1 month'")),
        (
            "R-month-slide",
            "python",
            lambda: _resid_window_python(session, "10 minutes", "1 month"),
        ),
        ("R-year-window", "sql", lambda: _resid_window_sql(session, "'1 year'")),
    ]
    for cell_id, door, build in cases:
        cell = _rcell(cell_id, door)
        with pytest.raises(AnalysisException) as excinfo:
            build()
        message = str(excinfo.value)
        assert cell["condition"] in message
        assert cell["message"] in message
    session.stop()


def test_resid_week_window() -> None:
    """pins: fnp-win-1/C-002"""
    session = _zoned_session("UTC")
    _resid_make(session, "base")
    _assert_resid_rows(_rcell("R-week-window", "python"), _resid_window_python(session, "1 week"))
    session.stop()


def test_resid_date_input() -> None:
    """pins: fnp-win-1/C-002"""
    session = _zoned_session("UTC")
    _resid_make(session, "date")
    for door, table in (
        ("python", _resid_window_python(session, "1 day")),
        ("sql", _resid_window_sql(session, "'1 day'")),
    ):
        _assert_resid_rows(_rcell("R-date-input", door), table.select(["start", "count"]))
    session.stop()


def test_resid_session_values() -> None:
    """pins: fnp-win-1/C-004"""
    session = _zoned_session("UTC")
    _resid_make(session, "ntz-base")
    _assert_resid_rows(
        _rcell("R-session-ntz-5m", "python"), _resid_session_python(session, "5 minutes")
    )
    session.stop()
    session = _zoned_session("America/New_York")
    _resid_make(session, "dst")
    _assert_resid_rows(
        _rcell("R-session-dst-ny-40m", "python"),
        _resid_session_python(session, "40 minutes"),
    )
    session.stop()


def test_resid_session_empty_gaps() -> None:
    """pins: fnp-win-1/C-013"""
    session = _zoned_session("UTC")
    _resid_make(session, "base")
    for cell_id, gap in (("R-session-zero-gap", "0 seconds"), ("R-session-neg-gap", "-5 minutes")):
        cell = _rcell(cell_id, "python")
        table = _resid_session_python(session, gap)
        assert [column[0] for column in cell["schema"]] == table.schema.names
        assert table.num_rows == 0
    session.stop()


def test_resid_session_month_diverges() -> None:
    """pins: fnp-win-1/C-015"""
    session = _zoned_session("UTC")
    _resid_make(session, "base")
    with pytest.raises(AnalysisException) as excinfo:
        _resid_session_python(session, "1 month")
    assert "months or years" in str(excinfo.value)
    session.stop()


def test_resid_null_timestamps() -> None:
    """pins: fnp-win-1/C-002, C-004"""
    session = _zoned_session("UTC")
    _resid_make(session, "null")
    session_table = _resid_session_python(session, "5 minutes")
    _assert_resid_rows(
        _rcell("R-session-null-ts", "python"), session_table.select(["start", "count"])
    )
    null_table = (
        session.table("wr")
        .groupBy(F.window("ts", "10 minutes"))
        .count()
        .select(F.col("window").getField("start").cast("string").alias("start"), "count")
        .orderBy("start")
        .to_arrow()
    )
    _assert_resid_rows(_rcell("R-window-null-ts", "python"), null_table)
    session.stop()


def test_resid_pre_epoch_slide() -> None:
    """pins: fnp-win-1/C-002"""
    session = _zoned_session("UTC")
    _resid_make(session, "pre-epoch")
    _assert_resid_rows(
        _rcell("R-pre-epoch", "python"),
        _resid_window_python(session, "10 minutes", "3 minutes"),
    )
    session.stop()
    session = _zoned_session("UTC")
    _resid_make(session, "base")
    _assert_resid_rows(
        _rcell("R-slide-not-divide", "python"),
        _resid_window_python(session, "10 minutes", "3 minutes"),
    )
    session.stop()


def test_resid_start_gt_slide() -> None:
    """pins: fnp-win-1/C-014"""
    session = _zoned_session("UTC")
    _resid_make(session, "base")
    cell = _rcell("R-start-gt-slide", "python")
    with pytest.raises(AnalysisException) as excinfo:
        _resid_window_python(session, "10 minutes", "5 minutes", "7 minutes")
    message = str(excinfo.value)
    assert cell["condition"] in message
    assert "420000000L) must be < the `slide_duration`(300000000L)" in message
    session.stop()


def test_session_window_sql_groupby(spark: ReparkSession, frame: Any) -> None:
    """pins: fnp-win-1/C-004"""
    cells = _cells("session_window", "sql", "session_window(ts, '5 minutes')")
    assert len(cells) == 2
    for cell in cells:
        _set_ansi(spark, bool(cell["ansi"]))
        table = spark.sql(
            "SELECT key, CAST(session_window.start AS STRING) s, "
            "CAST(session_window.end AS STRING) e, count(*) FROM fnp_win_1_frame "
            "GROUP BY key, session_window(ts, '5 minutes') ORDER BY key, s"
        ).to_arrow()
        assert list(
            zip(
                _string_column(table, "key"),
                _string_column(table, "s"),
                _string_column(table, "e"),
                table.column(table.schema.names[3]).to_pylist(),
                strict=True,
            )
        ) == [(row[0], row[1], row[2], row[3]) for row in cell["rows"]]


def _crit_make(session: ReparkSession, kind: str) -> None:
    """Build the run-16a critic frame for one kind as temp view crit."""
    if kind == "eq":
        rows: list[Any] = [
            (1, datetime.datetime(2024, 1, 1, 10, 0, 0), "k"),
            (2, datetime.datetime(2024, 1, 1, 10, 5, 0), "k"),
        ]
        session.createDataFrame(rows, ["id", "ts", "key"]).createOrReplaceTempView("crit")
    elif kind == "near":
        rows = [
            (1, datetime.datetime(2024, 1, 1, 10, 0, 0), "k"),
            (2, datetime.datetime(2024, 1, 1, 10, 4, 59), "k"),
        ]
        session.createDataFrame(rows, ["id", "ts", "key"]).createOrReplaceTempView("crit")
    elif kind == "nul":
        rows = [(1, datetime.datetime(2024, 1, 1, 10, 0, 0)), (2, None)]
        session.createDataFrame(rows, ["id", "ts"]).createOrReplaceTempView("crit")
    elif kind == "dates":
        rows = [
            (1, datetime.date(2026, 9, 15), "k"),
            (2, datetime.date(2026, 9, 16), "k"),
            (3, datetime.date(2026, 9, 20), "k"),
        ]
        session.createDataFrame(rows, "id int, ts date, key string").createOrReplaceTempView("crit")
    else:
        rows = [
            (1, datetime.datetime(2026, 9, 15, 10, 7, 30), "k1"),
            (2, datetime.datetime(2026, 9, 15, 10, 12, 0), "k1"),
            (3, datetime.datetime(2026, 9, 15, 10, 31, 0), "k2"),
        ]
        session.createDataFrame(rows, ["id", "ts", "key"]).createOrReplaceTempView("crit")


def _assert_crit_rows(cell: dict[str, Any], table: pa.Table) -> None:
    """Table rows equal the critic cell rows, stringified like the recorder."""
    names = table.schema.names
    assert [column[0] for column in cell["schema"]] == names
    strings = [[str(value) for value in _string_column(table, name)] for name in names]
    assert list(zip(*strings, strict=True)) == [tuple(row) for row in cell["rows"]]


def _crit_session_keyed(session: ReparkSession, gap: Any, count: str) -> pa.Table:
    """Run the critic keyed session query with one count alias."""
    return (
        session.table("crit")
        .groupBy("key", F.session_window("ts", gap))
        .count()
        .select(
            "key",
            F.col("session_window").getField("start").cast("string").alias("s"),
            F.col("session_window").getField("end").cast("string").alias("e"),
            F.col("count").alias(count),
        )
        .orderBy("s")
        .to_arrow()
    )


def test_crit_exact_gap_session_merges() -> None:
    """pins: fnp-win-1/C-004"""
    session = _zoned_session("UTC")
    _crit_make(session, "eq")
    for door, table in (
        ("python", _crit_session_keyed(session, "5 minutes", "count")),
        (
            "sql",
            session.sql(
                "SELECT key, CAST(session_window.start AS STRING) s, "
                "CAST(session_window.end AS STRING) e, count(*) c FROM crit "
                "GROUP BY key, session_window(ts, '5 minutes') ORDER BY s"
            ).to_arrow(),
        ),
    ):
        _assert_crit_rows(_rcell("C-L001-exact-gap", door), table)
    session.stop()


def test_crit_near_gap_session_merges() -> None:
    """pins: fnp-win-1/C-004"""
    session = _zoned_session("UTC")
    _crit_make(session, "near")
    _assert_crit_rows(
        _rcell("C-L001-near-gap", "python"), _crit_session_keyed(session, "5 minutes", "count")
    )
    session.stop()


def test_crit_null_select_drops_row() -> None:
    """pins: fnp-win-1/C-002"""
    session = _zoned_session("UTC")
    _crit_make(session, "nul")
    for door, table in (
        (
            "python",
            session.table("crit")
            .select(F.window("ts", "10 minutes").getField("start").cast("string").alias("s"), "id")
            .orderBy("id")
            .to_arrow(),
        ),
        (
            "sql",
            session.sql(
                "SELECT CAST(w.start AS STRING) s, id FROM "
                "(SELECT window(ts, '10 minutes') w, id FROM crit) ORDER BY id"
            ).to_arrow(),
        ),
    ):
        _assert_crit_rows(_rcell("C-L002-select-null-tumble", door), table)
    for door, table in (
        (
            "python",
            session.table("crit")
            .select(
                F.window("ts", "10 minutes", "5 minutes")
                .getField("start")
                .cast("string")
                .alias("s"),
                "id",
            )
            .orderBy("id", "s")
            .to_arrow(),
        ),
        (
            "sql",
            session.sql(
                "SELECT CAST(w.start AS STRING) s, id FROM "
                "(SELECT window(ts, '10 minutes', '5 minutes') w, id FROM crit) "
                "ORDER BY id, s"
            ).to_arrow(),
        ),
    ):
        _assert_crit_rows(_rcell("C-L002-select-null-slide", door), table)
    session.stop()


def test_crit_sliding_values() -> None:
    """pins: fnp-win-1/C-002"""
    session = _zoned_session("UTC")
    _crit_make(session, "base")
    table = (
        session.table("crit")
        .select(
            F.window("ts", "10 minutes", "5 minutes").getField("start").cast("string").alias("s"),
            F.window("ts", "10 minutes", "5 minutes").getField("end").cast("string").alias("e"),
            "id",
        )
        .orderBy("id", "s")
        .to_arrow()
    )
    _assert_crit_rows(_rcell("C-L002-select-slide-values", "python"), table)
    grouped = session.sql(
        "SELECT CAST(window.start AS STRING) s, CAST(window.end AS STRING) e, count(*) c "
        "FROM crit GROUP BY window(ts, '10 minutes', '5 minutes') ORDER BY s"
    ).to_arrow()
    _assert_crit_rows(_rcell("C-L009-sql-group-slide-values", "sql"), grouped)
    session.stop()


def test_crit_start_negative_and_zero() -> None:
    """pins: fnp-win-1/C-002"""
    session = _zoned_session("UTC")
    _crit_make(session, "base")
    table = (
        session.table("crit")
        .groupBy(F.window("ts", "10 minutes", "5 minutes", "-2 minutes"))
        .count()
        .select(
            F.col("window").getField("start").cast("string").alias("s"),
            F.col("window").getField("end").cast("string").alias("e"),
            "count",
        )
        .orderBy("s")
        .to_arrow()
    )
    _assert_crit_rows(_rcell("C-L003-start-negative", "python"), table)
    grouped = session.sql(
        "SELECT CAST(window.start AS STRING) s, count(*) c FROM crit "
        "GROUP BY window(ts, '10 minutes', '5 minutes', '-2 minutes') ORDER BY s"
    ).to_arrow()
    _assert_crit_rows(_rcell("C-L003-start-negative", "sql"), grouped)
    zero = (
        session.table("crit")
        .groupBy(F.window("ts", "10 minutes", "10 minutes", "0 seconds"))
        .count()
        .select(
            F.col("window").getField("start").cast("string").alias("s"),
            F.col("window").getField("end").cast("string").alias("e"),
            "count",
        )
        .orderBy("s")
        .to_arrow()
    )
    _assert_crit_rows(_rcell("C-L003-start-zero", "python"), zero)
    zero_grouped = session.sql(
        "SELECT CAST(window.start AS STRING) s, count(*) c FROM crit "
        "GROUP BY window(ts, '10 minutes', '10 minutes', '0 seconds') ORDER BY s"
    ).to_arrow()
    _assert_crit_rows(_rcell("C-L003-start-zero", "sql"), zero_grouped)
    session.stop()


def test_crit_start_abs_ge_slide_refuses() -> None:
    """pins: fnp-win-1/C-005"""
    session = _zoned_session("UTC")
    _crit_make(session, "base")
    cell = _rcell("C-L003-start-neg-abs-ge-slide", "python")
    assert cell["condition"] == "DATATYPE_MISMATCH.PARAMETER_CONSTRAINT_VIOLATION"
    with pytest.raises(AnalysisException) as excinfo:
        session.table("crit").groupBy(
            F.window("ts", "10 minutes", "5 minutes", "-5 minutes")
        ).count().to_arrow()
    message = str(excinfo.value)
    assert cell["condition"] in message
    assert "300000000L) must be < the `slide_duration`(300000000L)" in message
    session.stop()


def test_crit_dynamic_gap_drops_and_month_answers() -> None:
    """pins: fnp-win-1/C-004"""
    session = _zoned_session("UTC")
    _crit_make(session, "base")
    session.table("crit").withColumn("key", F.lit("k")).createOrReplaceTempView("crit")
    null_when: Any = F.when(F.col("id") == 2, F.lit(None)).otherwise(F.lit("30 minutes"))
    _assert_crit_rows(
        _rcell("C-L004-dyn-null-when", "python"),
        _crit_session_keyed(session, null_when, "count"),
    )
    null_sql = session.sql(
        "SELECT CAST(session_window.start AS STRING) s, "
        "CAST(session_window.end AS STRING) e, count(*) c FROM crit "
        "GROUP BY session_window(ts, CASE WHEN id = 2 THEN NULL ELSE '30 minutes' END) "
        "ORDER BY s"
    ).to_arrow()
    _assert_crit_rows(_rcell("C-L004-dyn-null-when", "sql"), null_sql)
    null_cast = _crit_session_keyed(session, F.lit(None).cast("string"), "count")
    assert null_cast.schema.names == [
        column[0] for column in _rcell("C-L004-dyn-null-cast", "python")["schema"]
    ]
    assert null_cast.num_rows == 0
    zero_gap: Any = F.when(F.col("id") == 1, F.lit("0 seconds")).otherwise(F.lit("30 minutes"))
    _assert_crit_rows(
        _rcell("C-L004-dyn-zero", "python"), _crit_session_keyed(session, zero_gap, "count")
    )
    zero_sql = session.sql(
        "SELECT CAST(session_window.start AS STRING) s, "
        "CAST(session_window.end AS STRING) e, count(*) c FROM crit "
        "GROUP BY session_window(ts, CASE WHEN id = 1 THEN '0 seconds' ELSE '30 minutes' END) "
        "ORDER BY s"
    ).to_arrow()
    _assert_crit_rows(_rcell("C-L004-dyn-zero", "sql"), zero_sql)
    neg_gap: Any = F.when(F.col("id") == 1, F.lit("-1 minute")).otherwise(F.lit("30 minutes"))
    _assert_crit_rows(
        _rcell("C-L004-dyn-negative", "python"),
        _crit_session_keyed(session, neg_gap, "count"),
    )
    month_gap: Any = F.when(F.col("id") == 1, F.lit("1 month")).otherwise(F.lit("30 minutes"))
    _assert_crit_rows(
        _rcell("C-L004-dyn-month", "python"),
        _crit_session_keyed(session, month_gap, "count"),
    )
    session.stop()


def test_crit_window_lt_slide_refuses() -> None:
    """pins: fnp-win-1/C-005"""
    session = _zoned_session("UTC")
    _crit_make(session, "base")
    cell = _rcell("C-L005-window-lt-slide", "python")
    assert cell["condition"] == "DATATYPE_MISMATCH.PARAMETER_CONSTRAINT_VIOLATION"
    with pytest.raises(AnalysisException) as excinfo:
        session.table("crit").groupBy(F.window("ts", "5 minutes", "10 minutes")).count().to_arrow()
    message = str(excinfo.value)
    assert cell["condition"] in message
    assert "600000000L) must be <= the `window_duration`(300000000L)" in message
    select_cell = _rcell("C-L005-window-lt-slide-select", "python")
    with pytest.raises((AnalysisException, PySparkException)) as excinfo:
        session.table("crit").select(F.window("ts", "5 minutes", "10 minutes")).to_arrow()
    assert select_cell["condition"] in str(excinfo.value)
    sql_cell = _rcell("C-L005-window-lt-slide", "sql")
    assert sql_cell["condition"] == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
    with pytest.raises(AnalysisException) as excinfo:
        session.sql(
            "SELECT CAST(window.start AS STRING) s, count(*) c FROM crit "
            "GROUP BY window(ts, '5 minutes', '10 minutes') ORDER BY s"
        ).to_arrow()
    assert "PARAMETER_CONSTRAINT_VIOLATION" in str(excinfo.value)
    session.stop()


def test_crit_nested_sql_grouping() -> None:
    """pins: fnp-win-1/C-002, C-004"""
    session = _zoned_session("UTC")
    _crit_make(session, "base")
    nested = session.sql(
        "SELECT * FROM (SELECT CAST(window.start AS STRING) s, count(*) c FROM crit "
        "GROUP BY window(ts, '10 minutes')) t ORDER BY s"
    ).to_arrow()
    _assert_crit_rows(_rcell("C-L006-nested", "sql"), nested)
    cte = session.sql(
        "WITH w AS (SELECT CAST(window.start AS STRING) s, count(*) c FROM crit "
        "GROUP BY window(ts, '10 minutes')) SELECT * FROM w ORDER BY s"
    ).to_arrow()
    _assert_crit_rows(_rcell("C-L006-cte", "sql"), cte)
    union = session.sql(
        "SELECT CAST(window.start AS STRING) s, count(*) c FROM crit "
        "GROUP BY window(ts, '10 minutes') UNION ALL "
        "SELECT CAST(window.start AS STRING) s, count(*) c FROM crit "
        "GROUP BY window(ts, '30 minutes') ORDER BY s"
    ).to_arrow()
    _assert_crit_rows(_rcell("C-L006-union", "sql"), union)
    nested_session = session.sql(
        "SELECT * FROM (SELECT key, CAST(session_window.start AS STRING) s, count(*) c "
        "FROM crit GROUP BY key, session_window(ts, '5 minutes')) t ORDER BY s"
    ).to_arrow()
    _assert_crit_rows(_rcell("C-L006-nested-session", "sql"), nested_session)
    session.stop()


def test_crit_session_date() -> None:
    """pins: fnp-win-1/C-004"""
    session = _zoned_session("UTC")
    _crit_make(session, "dates")
    table = (
        session.table("crit")
        .groupBy("key", F.session_window("ts", "1 day"))
        .count()
        .select(
            "key",
            F.col("session_window").getField("start").cast("string").alias("s"),
            F.col("session_window").getField("end").cast("string").alias("e"),
            "count",
        )
        .orderBy("s")
        .to_arrow()
    )
    _assert_crit_rows(_rcell("C-L007-session-date", "python"), table)
    grouped = session.sql(
        "SELECT key, CAST(session_window.start AS STRING) s, "
        "CAST(session_window.end AS STRING) e, count(*) c FROM crit "
        "GROUP BY key, session_window(ts, '1 day') ORDER BY s"
    ).to_arrow()
    _assert_crit_rows(_rcell("C-L007-session-date", "sql"), grouped)
    schema_cell = _rcell("C-L007-session-date-schema", "python")
    raw = session.table("crit").groupBy("key", F.session_window("ts", "1 day")).count().to_arrow()
    assert [column[0] for column in schema_cell["schema"][:2]] == ["key", "session_window"]
    assert raw.schema.field("session_window").type.names == ["start", "end"]
    assert all(
        pa.types.is_timestamp(raw.schema.field("session_window").type.field(name).type)
        for name in ("start", "end")
    )
    session.stop()


def test_crit_window_time_plain_struct_refuses() -> None:
    """pins: fnp-win-1/C-003"""
    session = _zoned_session("UTC")
    builds = (
        (
            "python",
            lambda: (
                session.sql(
                    "SELECT named_struct('start', TIMESTAMP '2024-01-01 10:00:00', "
                    "'end', TIMESTAMP '2024-01-01 10:10:00') AS w"
                )
                .select(F.window_time("w"))
                .to_arrow()
            ),
        ),
        (
            "sql",
            lambda: session.sql(
                "SELECT window_time(named_struct('start', TIMESTAMP '2024-01-01 10:00:00', "
                "'end', TIMESTAMP '2024-01-01 10:10:00'))"
            ).to_arrow(),
        ),
    )
    for door, build in builds:
        cell = _rcell("C-L008-window-time-plain-struct", door)
        assert cell["condition"] == "_LEGACY_ERROR_TEMP_3101"
        with pytest.raises(AnalysisException) as excinfo:
            build()
        message = str(excinfo.value)
        assert cell["condition"] in message
        assert "The input is not a correct window column:" in message
        assert "window_time(" in message
    session.stop()


def test_crit_window_time_grouped_answers() -> None:
    """pins: fnp-win-1/C-003"""
    session = _zoned_session("UTC")
    _crit_make(session, "base")
    table = (
        session.table("crit")
        .groupBy(F.window("ts", "10 minutes"))
        .agg(F.count("*").alias("c"))
        .select(F.window_time("window").cast("string").alias("wt"))
        .orderBy("wt")
        .to_arrow()
    )
    _assert_crit_rows(_rcell("C-L008-window-time-grouped", "python"), table)
    session.stop()


def test_crit_window_time_table_struct_answers() -> None:
    """pins: fnp-win-1/C-003"""
    session = _zoned_session("UTC")
    session.createDataFrame(
        [
            (
                {
                    "start": datetime.datetime(2024, 1, 1, 10, 0, 0),
                    "end": datetime.datetime(2024, 1, 1, 10, 10, 0),
                },
            )
        ],
        "w struct<start:timestamp,end:timestamp>",
    ).createOrReplaceTempView("crit_struct")
    answered = (
        session.table("crit_struct")
        .select(F.window_time("w").cast("string").alias("wt"))
        .to_arrow()
    )
    assert _string_column(answered, "wt") == ["2024-01-01 10:09:59.999999"]
    session.stop()
