from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from _record_date_part_session_zone_1 import (
    ANSI_MODES,
    EXTRACT_FIELDS,
    FIXTURE,
    INT_FIELDS,
    ROWS_TS,
    SEAT_FIELDS,
    ZONES,
    measure_all,
    seed_iceberg_table,
    ts_literal,
    write_multi_rg_parquet,
)

import repark
from repark import ReparkSession
from repark.spark import functions as spark_functions
from repark.spark.session import _reset_active_session_for_tests

_ORACLE: dict[str, Any] = json.loads(FIXTURE.read_text(encoding="utf-8"))
_EXPECTED: dict[str, Any] = _ORACLE["cells"]
_KEYS: list[str] = sorted(_EXPECTED)

pytestmark = pytest.mark.xdist_group("test_date_part_session_zone_1")


def _new_session(app: str, warehouse: Path) -> Any:
    _reset_active_session_for_tests()
    return (
        ReparkSession.builder.appName(app)
        .config("spark.sql.catalog.dp1", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.dp1.type", "hadoop")
        .config("spark.sql.catalog.dp1.warehouse", str(warehouse))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


@pytest.fixture(scope="module")
def measured(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, Any]]:
    root = tmp_path_factory.mktemp("date-part-session-zone-1")
    pq_path = root / "events.parquet"
    write_multi_rg_parquet(pq_path)
    warehouse = root / "wh"
    warehouse.mkdir()
    session = _new_session("date-part-session-zone-1", warehouse)
    try:
        session.sql("CREATE NAMESPACE IF NOT EXISTS dp1.db").collect()
        seed_iceberg_table(session, "dp1.db.events")
        found = measure_all(
            session,
            spark_functions,
            pq_path,
            "dp1.db.events",
            "dp1.db",
            warehouse,
            lambda: _new_session("date-part-session-zone-1-cold", warehouse),
            allow_errors=True,
        )
    finally:
        session.stop()
        _reset_active_session_for_tests()
    yield found


def test_the_oracle_fixture_is_complete() -> None:
    assert _ORACLE["spark"] == "4.1.2"
    assert len(_ORACLE["rows"]) == len(ROWS_TS)
    assert len(_KEYS) == len(ZONES) * len(ANSI_MODES) * 25


@pytest.mark.parametrize("key", _KEYS)
def test_date_part_and_extract_match_spark_in_session_zone(
    measured: dict[str, Any], key: str
) -> None:
    assert measured[key] == _EXPECTED[key], (key, measured[key], _EXPECTED[key])


def test_arrow_path_returns_int32_for_every_calendar_field(tmp_path: Path) -> None:
    pq_path = tmp_path / "types.parquet"
    write_multi_rg_parquet(pq_path)
    session = _new_session("date-part-arrow-types", tmp_path / "wh")
    try:
        session.conf.set("spark.sql.session.timeZone", "America/New_York")
        session.read.parquet(str(pq_path)).createOrReplaceTempView("dp_types")
        dp_sql = ", ".join(f"date_part('{f}', ts) AS dp_{f}" for f in INT_FIELDS)
        ex_sql = ", ".join(f"extract({f} FROM ts) AS ex_{f.lower()}" for f in EXTRACT_FIELDS)
        table = session.sql(
            f"SELECT {dp_sql}, {ex_sql}, date_part('second', ts) AS dp_sec, "
            "extract(SECOND FROM ts) AS ex_sec FROM dp_types ORDER BY id"
        ).to_arrow()
        for field in table.schema:
            assert pa.types.is_int32(field.type), (field.name, field.type)
        assert table.column("dp_hour").to_pylist()[2] == 21
        assert table.column("ex_hour").to_pylist()[2] == 21
        assert table.column("dp_year").to_pylist()[2] == 2023
        assert table.column("ex_year").to_pylist()[2] == 2023
        assert table.column("dp_dayofweek").to_pylist()[2] == 1
        assert table.column("ex_dayofweek").to_pylist()[2] == 1
        assert table.column("ex_dow").to_pylist()[2] == 1
        assert table.column("dp_hour").to_pylist()[17] is None
        assert table.column("ex_hour").to_pylist()[17] is None
    finally:
        session.stop()
        _reset_active_session_for_tests()


def test_typed_facade_returns_int32_in_session_zone(tmp_path: Path) -> None:
    pq_path = tmp_path / "typed.parquet"
    write_multi_rg_parquet(pq_path)
    session = _new_session("date-part-typed-arrow", tmp_path / "wh")
    try:
        session.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")
        frame = session.read.parquet(str(pq_path)).orderBy("id")
        table = frame.select(
            *(
                spark_functions.date_part(spark_functions.lit(f), spark_functions.col("ts")).alias(
                    f"dp_{f}"
                )
                for f in SEAT_FIELDS
            ),
            *(
                spark_functions.extract(spark_functions.lit(f), spark_functions.col("ts")).alias(
                    f"ex_{f}"
                )
                for f in SEAT_FIELDS
            ),
        ).to_arrow()
        for field in table.schema:
            assert pa.types.is_int32(field.type), (field.name, field.type)
        assert table.column("dp_hour").to_pylist()[12] == 0
        assert table.column("dp_minute").to_pylist()[12] == 15
        assert table.column("dp_day").to_pylist()[12] == 2
        assert table.column("ex_hour").to_pylist()[12] == 0
        assert table.column("ex_minute").to_pylist()[12] == 15
    finally:
        session.stop()
        _reset_active_session_for_tests()


def test_null_inputs_and_invalid_fields(tmp_path: Path) -> None:
    session = _new_session("date-part-nulls", tmp_path / "wh")
    try:
        session.conf.set("spark.sql.session.timeZone", "America/New_York")
        table = session.sql(
            "SELECT date_part('hour', NULL) AS a, extract(HOUR FROM NULL) AS b, "
            "date_part('hour', CAST(NULL AS TIMESTAMP)) AS c, "
            "date_part('hour', CAST(NULL AS DATE)) AS d"
        ).to_arrow()
        for field in table.schema:
            assert pa.types.is_int32(field.type), (field.name, field.type)
            assert table.column(field.name).to_pylist() == [None]
        lit_expr = ts_literal(ROWS_TS[2][1])
        with pytest.raises(Exception, match="not supported"):
            session.sql(f"SELECT date_part(' year ', {lit_expr})").collect()
        with pytest.raises(Exception, match="not supported"):
            session.sql(f"SELECT date_part('bogus', {lit_expr})").collect()
    finally:
        session.stop()
        _reset_active_session_for_tests()


def test_native_repark_sql_door_is_unchanged() -> None:
    src = (
        "SELECT 0 AS id, CAST('2024-01-01T02:00:00Z' AS TIMESTAMPTZ) AS ts "
        "UNION ALL SELECT 1 AS id, CAST('2024-06-15T12:00:00Z' AS TIMESTAMPTZ) AS ts"
    )
    table = repark.sql(
        "SELECT date_part('year', ts) AS dp_y, date_part('hour', ts) AS dp_h, "
        "date_part('dow', ts) AS dp_dow, extract(HOUR FROM ts) AS ex_h, "
        f"extract(DOW FROM ts) AS ex_dow FROM ({src}) t ORDER BY id"
    ).to_arrow()
    assert table.column("dp_y").to_pylist() == [2024, 2024]
    assert table.column("dp_h").to_pylist() == [2, 12]
    assert table.column("dp_dow").to_pylist() == [1, 6]
    assert table.column("ex_h").to_pylist() == [2, 12]
    assert table.column("ex_dow").to_pylist() == [1, 6]
