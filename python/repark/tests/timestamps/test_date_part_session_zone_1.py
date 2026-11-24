from __future__ import annotations

import contextlib
import datetime as dt
import io
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
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
        with pytest.raises(Exception, match="INVALID_EXTRACT_FIELD"):
            session.sql(f"SELECT date_part(' year ', {lit_expr})").collect()
        with pytest.raises(Exception, match="INVALID_EXTRACT_FIELD"):
            session.sql(f"SELECT date_part('bogus', {lit_expr})").collect()
    finally:
        session.stop()
        _reset_active_session_for_tests()


def test_a1_interval_inputs_across_typed_facade_and_sql(tmp_path: Path) -> None:
    session = _new_session("date-part-a1-interval", tmp_path / "wh")
    try:
        session.conf.set("spark.sql.session.timeZone", "America/New_York")
        session.conf.set("spark.sql.ansi.enabled", "true")
        cases = [
            ("INTERVAL '2' YEAR", "year", 2),
            ("INTERVAL '2' YEAR", "y", 2),
            ("INTERVAL '2' YEAR", "yr", 2),
            ("INTERVAL '2' YEAR", "years", 2),
            ("INTERVAL '14' MONTH", "month", 2),
            ("INTERVAL '14' MONTH", "mon", 2),
            ("INTERVAL '5' DAY", "day", 5),
            ("INTERVAL '5' DAY", "d", 5),
            ("INTERVAL '3' HOUR", "hour", 3),
            ("INTERVAL '3' HOUR", "h", 3),
            ("INTERVAL '3' HOUR", "hrs", 3),
            ("INTERVAL '45' MINUTE", "minute", 45),
            ("INTERVAL '45' MINUTE", "mins", 45),
            ("INTERVAL '12' SECOND", "second", 12),
            ("INTERVAL '12' SECOND", "secs", 12),
        ]
        for iv_sql, field, expected in cases:
            frame = session.sql(f"SELECT {iv_sql} AS iv")
            typed = frame.select(
                spark_functions.date_part(
                    spark_functions.lit(field), spark_functions.col("iv")
                ).alias("a"),
                spark_functions.datepart(
                    spark_functions.lit(field), spark_functions.col("iv")
                ).alias("b"),
                spark_functions.extract(
                    spark_functions.lit(field), spark_functions.col("iv")
                ).alias("c"),
            ).to_arrow()
            for col_name in ("a", "b", "c"):
                assert pa.types.is_int32(typed.schema.field(col_name).type), (
                    iv_sql,
                    field,
                    col_name,
                )
                assert typed.column(col_name).to_pylist() == [expected], (iv_sql, field, col_name)
        for extract_sql, expected in [
            ("SELECT extract(YEAR FROM INTERVAL '2' YEAR) AS v", 2),
            ("SELECT extract(MONTH FROM INTERVAL '14' MONTH) AS v", 2),
            ("SELECT extract(DAY FROM INTERVAL '5' DAY) AS v", 5),
            ("SELECT extract(HOUR FROM INTERVAL '3' HOUR) AS v", 3),
            ("SELECT extract(MINUTE FROM INTERVAL '45' MINUTE) AS v", 45),
            ("SELECT extract(SECOND FROM INTERVAL '12' SECOND) AS v", 12),
        ]:
            table = session.sql(extract_sql).to_arrow()
            assert pa.types.is_int32(table.schema.field("v").type)
            assert table.column("v").to_pylist() == [expected]
        for refused_sql in (
            "SELECT date_part('day', INTERVAL '2' YEAR)",
            "SELECT date_part('year', INTERVAL '14' MONTH)",
            "SELECT date_part('year', INTERVAL '5' DAY)",
            "SELECT date_part('year', INTERVAL '2' YEAR)",
        ):
            with pytest.raises(Exception, match="MonthDayNano"):
                session.sql(refused_sql).collect()
        for bad_field in ("quarter", "week", "dow", "doy", "epoch", "millisecond", "nanosecond"):
            with pytest.raises(Exception, match="INVALID_EXTRACT_FIELD"):
                session.sql("SELECT INTERVAL '5' DAY AS iv").select(
                    spark_functions.date_part(
                        spark_functions.lit(bad_field), spark_functions.col("iv")
                    )
                ).collect()
            with pytest.raises(Exception, match="INVALID_EXTRACT_FIELD"):
                session.sql(f"SELECT date_part('{bad_field}', INTERVAL '5' DAY)").collect()
    finally:
        session.stop()
        _reset_active_session_for_tests()


def test_d1_1_f_expr_literal_and_column_extract_match_spark(tmp_path: Path) -> None:
    session = _new_session("date-part-d1-1-fexpr", tmp_path / "wh")
    try:
        session.conf.set("spark.sql.session.timeZone", "America/New_York")
        for ansi in ("true", "false"):
            session.conf.set("spark.sql.ansi.enabled", ansi)
            frame = session.sql(
                "SELECT '2024-01-01T02:00:00Z' AS stz, '2024-03-10 02:30:00' AS sgap, "
                "'2024-01-01 99:99:99' AS sbadt, '02:30:45' AS stime, "
                "TIMESTAMP '2024-01-01 02:00:00Z' AS ts"
            )
            cases = [
                ("extract(day FROM '2024-01-01T02:00:00Z')", "extract(day FROM stz)", 1),
                ("extract(year FROM '2024-01-01T02:00:00Z')", "extract(year FROM stz)", 2024),
                ("extract(hour FROM '2024-01-01T02:00:00Z')", "extract(hour FROM stz)", 21),
                ("extract(hour FROM '2024-03-10 02:30:00')", "extract(hour FROM sgap)", 3),
                ("extract(year FROM '2024-01-01 99:99:99')", "extract(year FROM sbadt)", 2024),
                ("extract(hour FROM '02:30:45')", "extract(hour FROM stime)", 2),
                (
                    "extract(DAY FROM TIMESTAMP '2024-01-01 02:00:00Z')",
                    "extract(DAY FROM ts)",
                    31,
                ),
            ]
            for lit_expr, col_expr, expected in cases:
                table = frame.select(
                    spark_functions.expr(lit_expr).alias("lit_v"),
                    spark_functions.expr(col_expr).alias("col_v"),
                ).to_arrow()
                assert pa.types.is_int32(table.schema.field("lit_v").type), (ansi, lit_expr)
                assert pa.types.is_int32(table.schema.field("col_v").type), (ansi, col_expr)
                assert table.column("lit_v").to_pylist() == [expected], (ansi, lit_expr)
                assert table.column("col_v").to_pylist() == [expected], (ansi, col_expr)
    finally:
        session.stop()
        _reset_active_session_for_tests()


def test_a1_string_inputs_across_zones_and_ansi_modes(tmp_path: Path) -> None:
    session = _new_session("date-part-a1-string", tmp_path / "wh")
    try:
        expected_by_zone = {
            "UTC": [
                ("2024-01-01", "year", 2024),
                ("2024-01-01", "hour", 0),
                ("2020-02-29", "day", 29),
                ("2024-01-01 02:30:45", "hour", 2),
                ("2024-01-01T02:30:45Z", "hour", 2),
                ("2024-01-01T02:30:45Z", "day", 1),
                ("2024-01-01T02:30:45Z", "year", 2024),
                ("2024-01-01 02:30:45+05:30", "hour", 21),
                ("2024-01-01 02:30:45-05:00", "hour", 7),
                ("2024-03-10 02:30:00", "hour", 2),
            ],
            "America/New_York": [
                ("2024-01-01", "year", 2024),
                ("2024-01-01", "hour", 0),
                ("2020-02-29", "day", 29),
                ("2024-01-01 02:30:45", "hour", 2),
                ("2024-01-01T02:30:45Z", "hour", 21),
                ("2024-01-01T02:30:45Z", "day", 1),
                ("2024-01-01T02:30:45Z", "year", 2024),
                ("2024-01-01 02:30:45+00:00", "hour", 21),
                ("2024-01-01 02:30:45+05:30", "hour", 16),
                ("2024-01-01 02:30:45-05:00", "hour", 2),
                ("2024-03-10 02:30:00", "hour", 3),
            ],
            "Asia/Kolkata": [
                ("2024-01-01", "year", 2024),
                ("2024-01-01 02:30:45", "hour", 2),
                ("2024-01-01T02:30:45Z", "hour", 8),
                ("2024-01-01T02:30:45Z", "minute", 0),
                ("2024-01-01T02:30:45Z", "day", 1),
                ("2024-01-01 02:30:45+05:30", "hour", 2),
                ("2024-01-01 02:30:45-05:00", "hour", 13),
            ],
        }
        for zone, cases in expected_by_zone.items():
            session.conf.set("spark.sql.session.timeZone", zone)
            for ansi in ("false", "true"):
                session.conf.set("spark.sql.ansi.enabled", ansi)
                for text, field, expected in cases:
                    frame = session.sql(f"SELECT '{text}' AS s")
                    typed = frame.select(
                        spark_functions.date_part(
                            spark_functions.lit(field), spark_functions.col("s")
                        ).alias("a"),
                        spark_functions.datepart(
                            spark_functions.lit(field), spark_functions.col("s")
                        ).alias("b"),
                        spark_functions.extract(
                            spark_functions.lit(field), spark_functions.col("s")
                        ).alias("c"),
                    ).to_arrow()
                    for col_name in ("a", "b", "c"):
                        assert pa.types.is_int32(typed.schema.field(col_name).type)
                        assert typed.column(col_name).to_pylist() == [expected], (
                            zone,
                            ansi,
                            text,
                            field,
                            col_name,
                        )
                    sql_table = session.sql(
                        f"SELECT date_part('{field}', '{text}') AS dp, "
                        f"extract({field} FROM '{text}') AS ex"
                    ).to_arrow()
                    assert sql_table.column("dp").to_pylist() == [expected], (
                        zone,
                        ansi,
                        text,
                        field,
                    )
                    assert sql_table.column("ex").to_pylist() == [expected], (
                        zone,
                        ansi,
                        text,
                        field,
                    )
        session.conf.set("spark.sql.session.timeZone", "America/New_York")
        session.conf.set("spark.sql.ansi.enabled", "false")
        bad_frame = session.sql("SELECT 'not-a-date' AS s UNION ALL SELECT '' AS s")
        off_table = bad_frame.select(
            spark_functions.date_part(spark_functions.lit("year"), spark_functions.col("s")).alias(
                "y"
            ),
            spark_functions.date_part(spark_functions.lit("hour"), spark_functions.col("s")).alias(
                "h"
            ),
        ).to_arrow()
        assert off_table.column("y").to_pylist() == [None, None]
        assert off_table.column("h").to_pylist() == [None, None]
        null_table = (
            session.sql("SELECT CAST(NULL AS STRING) AS s")
            .select(
                spark_functions.date_part(
                    spark_functions.lit("hour"), spark_functions.col("s")
                ).alias("h")
            )
            .to_arrow()
        )
        assert null_table.column("h").to_pylist() == [None]
        session.conf.set("spark.sql.ansi.enabled", "true")
        bad_frame_ansi = session.sql("SELECT 'not-a-date' AS s UNION ALL SELECT '' AS s")
        for field in ("year", "hour"):
            with pytest.raises(Exception, match="CAST_INVALID_INPUT"):
                bad_frame_ansi.select(
                    spark_functions.date_part(spark_functions.lit(field), spark_functions.col("s"))
                ).collect()
    finally:
        session.stop()
        _reset_active_session_for_tests()


def test_a2_non_spark_fields_refuse_with_invalid_extract_field(tmp_path: Path) -> None:
    session = _new_session("date-part-a2-fields", tmp_path / "wh")
    try:
        session.conf.set("spark.sql.session.timeZone", "America/New_York")
        session.conf.set("spark.sql.ansi.enabled", "true")
        lit_expr = ts_literal(ROWS_TS[2][1])
        df_ts = session.sql(f"SELECT {lit_expr} AS ts, DATE '2024-01-01' AS d, '2024-01-01' AS s")
        df_ts.createOrReplaceTempView("a2_t")
        df_ts.limit(0).createOrReplaceTempView("a2_empty")
        rejected = (
            "dayofyear",
            "isoweek",
            "quarters",
            "epoch",
            "nanosecond",
            "nanoseconds",
            "ns",
            "year_iso",
            "isoyear",
            "isodow",
            "millisecond",
            "milliseconds",
            "ms",
            "microsecond",
            "microseconds",
            "us",
            " year ",
            "'year'",
            "",
            "bogus",
        )
        for field in rejected:
            esc = field.replace("'", "''")
            for sql in (
                f"SELECT date_part('{esc}', ts) FROM a2_t",
                f"SELECT date_part('{esc}', ts) FROM a2_empty",
                f"SELECT date_part('{esc}', NULL)",
                f"SELECT date_part('{esc}', d) FROM a2_t",
                f"SELECT date_part('{esc}', s) FROM a2_t",
            ):
                with pytest.raises(Exception, match="INVALID_EXTRACT_FIELD"):
                    session.sql(sql).collect()
            with pytest.raises(Exception, match="INVALID_EXTRACT_FIELD"):
                df_ts.select(
                    spark_functions.date_part(spark_functions.lit(field), spark_functions.col("ts"))
                ).collect()
        for extract_sql in (
            "SELECT extract(ISOYEAR FROM ts) FROM a2_t",
            "SELECT extract(ISODOW FROM ts) FROM a2_t",
            "SELECT extract(EPOCH FROM ts) FROM a2_t",
            "SELECT extract(MILLISECOND FROM NULL)",
            "SELECT extract(MICROSECOND FROM NULL)",
        ):
            with pytest.raises(Exception, match="INVALID_EXTRACT_FIELD"):
                session.sql(extract_sql).collect()
    finally:
        session.stop()
        _reset_active_session_for_tests()


def test_a3_non_foldable_field_refuses_before_optimizer(tmp_path: Path) -> None:
    session = _new_session("date-part-a3-foldable", tmp_path / "wh")
    try:
        session.conf.set("spark.sql.session.timeZone", "America/New_York")
        session.conf.set("spark.sql.ansi.enabled", "true")
        lit_expr = ts_literal(ROWS_TS[2][1])
        pq_path = tmp_path / "fold.parquet"
        session.sql(f"SELECT 'hour' AS f, {lit_expr} AS ts").write.parquet(str(pq_path))
        frame = session.read.parquet(str(pq_path))
        frame.createOrReplaceTempView("a3_pq")
        for sql in (
            f"SELECT date_part(f, ts) FROM (SELECT 'hour' AS f, {lit_expr} AS ts) s",
            "SELECT date_part(f, ts) FROM a3_pq",
            "SELECT date_part(concat(f, ''), ts) FROM a3_pq",
            "SELECT date_part(CASE WHEN f = 'hour' THEN 'hour' ELSE 'hour' END, ts) FROM a3_pq",
            "SELECT date_part(CASE WHEN rand() >= 0 THEN 'hour' ELSE 'day' END, ts) FROM a3_pq",
        ):
            with pytest.raises(Exception, match="NON_FOLDABLE_ARGUMENT"):
                session.sql(sql).collect()
        with pytest.raises(Exception, match="NON_FOLDABLE_ARGUMENT"):
            frame.select(
                spark_functions.date_part(spark_functions.col("f"), spark_functions.col("ts"))
            ).collect()
        foldable = session.sql(
            "SELECT date_part(lower('HOUR'), ts) AS a, "
            "date_part(concat('ho', 'ur'), ts) AS b, "
            "date_part(CASE WHEN 1 = 1 THEN 'hour' ELSE 'day' END, ts) AS c FROM a3_pq"
        ).to_arrow()
        assert foldable.column("a").to_pylist() == [21]
        assert foldable.column("b").to_pylist() == [21]
        assert foldable.column("c").to_pylist() == [21]
        typed_fold = frame.select(
            spark_functions.date_part(
                spark_functions.lower(spark_functions.lit("HOUR")), spark_functions.col("ts")
            ).alias("h")
        ).to_arrow()
        assert typed_fold.column("h").to_pylist() == [21]
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


def _capture_explain(frame: Any) -> str:
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        frame.explain(extended=True)
    return buffer.getvalue()


def test_d1_part3_f_expr_zoneless_timestamp_defers_to_session_zone(tmp_path: Path) -> None:
    session = _new_session("date-part-d1-p3-fexpr-zone", tmp_path / "wh")
    lit_ts = "TIMESTAMP '2024-01-01 02:00:00'"
    expected_epoch = {
        "America/New_York": 1704092400,
        "Asia/Kolkata": 1704054600,
        "UTC": 1704074400,
    }
    expected_z_hour = {"America/New_York": 21, "Asia/Kolkata": 7, "UTC": 2}
    expected_gap_hour = {"America/New_York": 3, "Asia/Kolkata": 2, "UTC": 2}
    expected_overlap_epoch = {
        "America/New_York": 1730611800,
        "Asia/Kolkata": 1730577600,
        "UTC": 1730597400,
    }
    try:
        for zone in ("America/New_York", "Asia/Kolkata", "UTC"):
            session.conf.set("spark.sql.session.timeZone", zone)
            for ansi in ("false", "true"):
                session.conf.set("spark.sql.ansi.enabled", ansi)
                frame = session.sql("SELECT 1 AS id")
                projected = frame.select(
                    spark_functions.expr(lit_ts).alias("ts"),
                    spark_functions.expr(f"hour({lit_ts})").alias("hr"),
                    spark_functions.expr(f"day({lit_ts})").alias("dy"),
                    spark_functions.expr(f"extract(HOUR FROM {lit_ts})").alias("ex_hr"),
                    spark_functions.expr(f"extract(DAY FROM {lit_ts})").alias("ex_dy"),
                    spark_functions.expr(f"date_part('day', {lit_ts})").alias("dp_dy"),
                    spark_functions.expr(f"date_format({lit_ts}, 'yyyy-MM-dd HH:mm')").alias("fmt"),
                    spark_functions.expr(f"CAST({lit_ts} AS STRING)").alias("as_str"),
                    spark_functions.expr(f"unix_timestamp({lit_ts})").alias("ep"),
                    spark_functions.expr(
                        "extract(DAY FROM CAST('2024-01-01 02:00:00' AS TIMESTAMP))"
                    ).alias("cast_dy"),
                    spark_functions.expr(f"extract(HOUR FROM {lit_ts} + INTERVAL 1 HOUR)").alias(
                        "plus1_hr"
                    ),
                    spark_functions.expr(
                        "extract(HOUR FROM TRY_CAST('2024-01-01 02:00:00' AS TIMESTAMP))"
                    ).alias("try_hr"),
                    spark_functions.extract(
                        spark_functions.lit("hour"), spark_functions.expr(lit_ts)
                    ).alias("fn_ex"),
                    spark_functions.date_part(
                        spark_functions.lit("hour"), spark_functions.expr(lit_ts)
                    ).alias("fn_dp"),
                    spark_functions.datepart(
                        spark_functions.lit("hour"), spark_functions.expr(lit_ts)
                    ).alias("fn_datepart"),
                    spark_functions.expr(
                        f"TRY_CAST('2024-01-01 02:00:00' AS TIMESTAMP) = {lit_ts}"
                    ).alias("eq_try"),
                    spark_functions.expr(f"{lit_ts} = '2024-01-01 02:00:00'").alias("eq_str"),
                    spark_functions.expr(
                        f"{lit_ts} BETWEEN TIMESTAMP '2024-01-01 01:00:00' "
                        "AND TIMESTAMP '2024-01-01 03:00:00'"
                    ).alias("btw"),
                    spark_functions.expr(
                        f"{lit_ts} IN (TIMESTAMP '2024-01-01 01:00:00', "
                        "TIMESTAMP '2024-01-01 02:00:00')"
                    ).alias("in_list"),
                    spark_functions.expr(f"{lit_ts} = TIMESTAMP_NTZ '2024-01-01 02:00:00'").alias(
                        "eq_ntz"
                    ),
                    spark_functions.expr(
                        "extract(HOUR FROM TIMESTAMP '2024-01-01 02:00:00Z')"
                    ).alias("z_hr"),
                    spark_functions.expr(
                        "extract(HOUR FROM TIMESTAMP '2024-01-01 02:00:00+00:00')"
                    ).alias("off_hr"),
                    spark_functions.expr(
                        "extract(HOUR FROM TIMESTAMP '2024-03-10 02:30:00')"
                    ).alias("gap_hr"),
                    spark_functions.expr("unix_timestamp(TIMESTAMP '2024-11-03 01:30:00')").alias(
                        "ov_ep"
                    ),
                    spark_functions.expr(
                        "extract(DAY FROM TIMESTAMP_NTZ '2024-01-01 02:00:00')"
                    ).alias("ntz_dy"),
                    spark_functions.expr("TRY_CAST('bad' AS TIMESTAMP)").alias("try_bad"),
                )
                arrow = projected.to_arrow()
                assert arrow.column("hr").to_pylist() == [2], (zone, ansi)
                assert arrow.column("dy").to_pylist() == [1], (zone, ansi)
                assert arrow.column("ex_hr").to_pylist() == [2], (zone, ansi)
                assert arrow.column("ex_dy").to_pylist() == [1], (zone, ansi)
                assert arrow.column("dp_dy").to_pylist() == [1], (zone, ansi)
                assert arrow.column("fmt").to_pylist() == ["2024-01-01 02:00"], (zone, ansi)
                assert arrow.column("as_str").to_pylist() == ["2024-01-01 02:00:00"], (zone, ansi)
                assert arrow.column("ep").to_pylist() == [expected_epoch[zone]], (zone, ansi)
                assert arrow.column("cast_dy").to_pylist() == [1], (zone, ansi)
                assert arrow.column("plus1_hr").to_pylist() == [3], (zone, ansi)
                assert arrow.column("try_hr").to_pylist() == [2], (zone, ansi)
                assert arrow.column("fn_ex").to_pylist() == [2], (zone, ansi)
                assert arrow.column("fn_dp").to_pylist() == [2], (zone, ansi)
                assert arrow.column("fn_datepart").to_pylist() == [2], (zone, ansi)
                assert arrow.column("eq_try").to_pylist() == [True], (zone, ansi)
                assert arrow.column("eq_str").to_pylist() == [True], (zone, ansi)
                assert arrow.column("btw").to_pylist() == [True], (zone, ansi)
                assert arrow.column("in_list").to_pylist() == [True], (zone, ansi)
                assert arrow.column("eq_ntz").to_pylist() == [True], (zone, ansi)
                assert arrow.column("z_hr").to_pylist() == [expected_z_hour[zone]], (zone, ansi)
                assert arrow.column("off_hr").to_pylist() == [expected_z_hour[zone]], (zone, ansi)
                assert arrow.column("gap_hr").to_pylist() == [expected_gap_hour[zone]], (zone, ansi)
                assert arrow.column("ov_ep").to_pylist() == [expected_overlap_epoch[zone]], (
                    zone,
                    ansi,
                )
                assert arrow.column("ntz_dy").to_pylist() == [1], (zone, ansi)
                assert arrow.column("try_bad").to_pylist() == [None], (zone, ansi)
                unaliased = frame.select(
                    spark_functions.expr(lit_ts),
                    spark_functions.expr(f"extract(HOUR FROM {lit_ts})"),
                )
                assert unaliased.columns == [lit_ts, f"extract(HOUR FROM {lit_ts})"]
                assert unaliased.dtypes == [
                    (lit_ts, "timestamp"),
                    (f"extract(HOUR FROM {lit_ts})", "int"),
                ]
                if ansi == "true":
                    with pytest.raises(Exception, match="CAST_INVALID_INPUT"):
                        frame.select(spark_functions.expr("TIMESTAMP 'bad'")).collect()
                else:
                    bad = frame.select(
                        spark_functions.expr("TIMESTAMP 'bad'").alias("b")
                    ).to_arrow()
                    assert bad.column("b").to_pylist() == [None]
    finally:
        session.stop()
        _reset_active_session_for_tests()


def test_d1_part3_f_expr_frame_doors_pruning_and_zone_lifecycle(tmp_path: Path) -> None:
    lit_ts = "TIMESTAMP '2024-01-01 02:00:00'"
    pre_session_ts = spark_functions.expr(lit_ts)
    pre_session_hr = spark_functions.expr(f"extract(HOUR FROM {lit_ts})")
    pq_path = tmp_path / "scan.parquet"
    schema = pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("ts", pa.timestamp("us", tz="UTC"), nullable=True),
        ]
    )
    with pq.ParquetWriter(str(pq_path), schema) as writer:
        writer.write_table(
            pa.Table.from_pydict(
                {
                    "id": [1, 2],
                    "ts": [
                        dt.datetime(2023, 12, 31, 20, 0, tzinfo=dt.UTC),
                        dt.datetime(2024, 1, 1, 2, 0, tzinfo=dt.UTC),
                    ],
                },
                schema=schema,
            )
        )
        writer.write_table(
            pa.Table.from_pydict(
                {
                    "id": [3, 4],
                    "ts": [
                        dt.datetime(2024, 1, 1, 7, 0, tzinfo=dt.UTC),
                        dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.UTC),
                    ],
                },
                schema=schema,
            )
        )
    warehouse = tmp_path / "wh"
    session = _new_session("date-part-d1-p3-doors", warehouse)
    try:
        session.conf.set("spark.sql.session.timeZone", "America/New_York")
        session.conf.set("spark.sql.ansi.enabled", "true")
        session.sql("CREATE NAMESPACE IF NOT EXISTS dp1.p3")
        session.sql("CREATE TABLE dp1.p3.scan (id INT, ts TIMESTAMP) USING iceberg")
        session.sql(
            "INSERT INTO dp1.p3.scan VALUES "
            "(1, TIMESTAMP '2023-12-31 20:00:00Z'), (2, TIMESTAMP '2024-01-01 02:00:00Z')"
        )
        session.sql(
            "INSERT INTO dp1.p3.scan VALUES "
            "(3, TIMESTAMP '2024-01-01 07:00:00Z'), (4, TIMESTAMP '2024-01-01 12:00:00Z')"
        )
        base = session.sql("SELECT 1 AS id")
        pq_filtered = session.read.parquet(str(pq_path)).filter(
            spark_functions.col("ts") >= spark_functions.expr(lit_ts)
        )
        ice_filtered = session.table("dp1.p3.scan").filter(
            spark_functions.col("ts") >= spark_functions.expr(lit_ts)
        )
        assert pq_filtered.orderBy("id").to_arrow().column("id").to_pylist() == [3, 4]
        assert ice_filtered.orderBy("id").to_arrow().column("id").to_pylist() == [3, 4]
        pq_plan = _capture_explain(pq_filtered)
        ice_plan = _capture_explain(ice_filtered)
        assert "1704092400000000" in pq_plan and "pruning_predicate" in pq_plan
        assert "ts >= 2024-01-01 07:00:00 UTC" in ice_plan and "N=1" in ice_plan
        assert base.withColumn("t", spark_functions.expr(lit_ts)).select(
            spark_functions.expr("extract(HOUR FROM t)").alias("h")
        ).to_arrow().column("h").to_pylist() == [2]
        assert base.selectExpr(f"extract(HOUR FROM {lit_ts}) AS h").to_arrow().column(
            "h"
        ).to_pylist() == [2]
        assert session.read.parquet(str(pq_path)).alias("a").join(
            base.alias("b"), spark_functions.col("a.ts") == spark_functions.expr(lit_ts)
        ).select("a.id").to_arrow().column("id").to_pylist() == [3]
        assert session.read.parquet(str(pq_path)).groupBy(
            spark_functions.expr(f"extract(HOUR FROM {lit_ts})").alias("h")
        ).count().to_arrow().column("h").to_pylist() == [2]
        assert session.read.parquet(str(pq_path)).agg(
            spark_functions.max(spark_functions.expr(lit_ts)).alias("mx")
        ).to_arrow().column("mx").cast(pa.int64()).to_pylist() == [1704092400000000]
        base.select(
            spark_functions.col("id"), spark_functions.expr(lit_ts).alias("t")
        ).createOrReplaceTempView("dp1_p3_view")
        assert session.sql("SELECT extract(HOUR FROM t) AS h FROM dp1_p3_view").to_arrow().column(
            "h"
        ).to_pylist() == [2]
        before_set_ts = spark_functions.expr(lit_ts)
        before_set_hr = spark_functions.expr(f"extract(HOUR FROM {lit_ts})")
        ny_lc = base.select(
            pre_session_ts.alias("pre_t"),
            pre_session_hr.alias("pre_h"),
            before_set_ts.alias("set_t"),
            before_set_hr.alias("set_h"),
        ).to_arrow()
        assert ny_lc.column("pre_t").cast(pa.int64()).to_pylist() == [1704092400000000]
        assert ny_lc.column("pre_h").to_pylist() == [2]
        assert ny_lc.column("set_t").cast(pa.int64()).to_pylist() == [1704092400000000]
        assert ny_lc.column("set_h").to_pylist() == [2]
        base.select(
            spark_functions.col("id"),
            spark_functions.expr(lit_ts).alias("t"),
            spark_functions.expr(f"extract(HOUR FROM {lit_ts})").alias("h"),
        ).writeTo("dp1.p3.written").create()
        session.conf.set("spark.sql.session.timeZone", "Asia/Tokyo")
        tokyo_lc = (
            session.sql("SELECT 1 AS id")
            .select(
                pre_session_ts.alias("pre_t"),
                pre_session_hr.alias("pre_h"),
                before_set_ts.alias("set_t"),
                before_set_hr.alias("set_h"),
            )
            .to_arrow()
        )
        assert tokyo_lc.column("pre_t").cast(pa.int64()).to_pylist() == [1704042000000000]
        assert tokyo_lc.column("pre_h").to_pylist() == [2]
        assert tokyo_lc.column("set_t").cast(pa.int64()).to_pylist() == [1704042000000000]
        assert tokyo_lc.column("set_h").to_pylist() == [2]
    finally:
        session.stop()
        _reset_active_session_for_tests()
    cold = _new_session("date-part-d1-p3-cold", warehouse)
    try:
        readback = cold.read.format("iceberg").load(str(warehouse / "p3" / "written")).to_arrow()
        assert readback.column("t").cast(pa.int64()).to_pylist() == [1704092400000000]
        assert readback.column("h").to_pylist() == [2]
    finally:
        cold.stop()
        _reset_active_session_for_tests()
