from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from repark import ReparkSession
from repark.spark import functions as sf
from repark.spark.session import _reset_active_session_for_tests

pytestmark = pytest.mark.xdist_group("test_fexpr_timestamp_ntz_1")

T = "TIMESTAMP '2024-01-01 02:00:00'"
ZONES = ("America/New_York", "Asia/Kolkata", "UTC")
ANSI_MODES = (True, False)
DOORS = ("select", "withColumn", "selectExpr", "sql")
FEXPR_DOORS = ("select", "withColumn")

NTZ_ARROW = pa.timestamp("us")
LTZ_ARROW = pa.timestamp("us", tz="UTC")


def _new_session(
    app: str,
    zone: str,
    ansi: bool,
    timestamp_type: str = "TIMESTAMP_LTZ",
    warehouse: Path | None = None,
) -> ReparkSession:
    _reset_active_session_for_tests()
    builder = (
        ReparkSession.builder.appName(app)
        .config("spark.sql.session.timeZone", zone)
        .config("spark.sql.ansi.enabled", "true" if ansi else "false")
        .config("spark.sql.timestampType", timestamp_type)
    )
    if warehouse is not None:
        builder = (
            builder.config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.ice.type", "hadoop")
            .config("spark.sql.catalog.ice.warehouse", str(warehouse))
        )
    return builder.getOrCreate()


@pytest.fixture(autouse=True)
def _clean_active_session() -> Iterator[None]:
    _reset_active_session_for_tests()
    yield
    _reset_active_session_for_tests()


def _eval_door(spark: ReparkSession, door: str, expr_sql: str, base: Any | None = None) -> Any:
    if door == "select":
        frame = spark.range(1) if base is None else base
        return frame.select(sf.expr(expr_sql).alias("v"))
    if door == "withColumn":
        frame = spark.range(1) if base is None else base
        return frame.withColumn("v", sf.expr(expr_sql)).select("v")
    if door == "selectExpr":
        frame = spark.range(1) if base is None else base
        return frame.selectExpr(f"{expr_sql} AS v")
    if base is None:
        return spark.sql(f"SELECT {expr_sql} AS v")
    base.createOrReplaceTempView("_fexpr_ntz_pin_src")
    return spark.sql(f"SELECT {expr_sql} AS v FROM _fexpr_ntz_pin_src")


def _assert_ntz_arrow(df: Any, expected: dt.datetime | None) -> None:
    assert df.dtypes == [("v", "timestamp_ntz")]
    table = df.to_arrow()
    assert table.schema.field("v").type == NTZ_ARROW
    assert table.column("v")[0].as_py() == expected
    rows = df.collect()
    assert rows[0]["v"] == expected


def _assert_ltz_arrow(df: Any, expected_utc: dt.datetime | None) -> None:
    assert df.dtypes == [("v", "timestamp")]
    table = df.to_arrow()
    assert table.schema.field("v").type == LTZ_ARROW
    actual = table.column("v")[0].as_py()
    if expected_utc is None:
        assert actual is None
    else:
        assert actual == expected_utc


NTZ_CAST_SCALAR_CASES: tuple[tuple[str, str, dict[str, dt.datetime | None]], ...] = (
    (
        "cast_t_ntz",
        f"CAST({T} AS TIMESTAMP_NTZ)",
        {
            "America/New_York": dt.datetime(2024, 1, 1, 2, 0, 0),
            "Asia/Kolkata": dt.datetime(2024, 1, 1, 2, 0, 0),
            "UTC": dt.datetime(2024, 1, 1, 2, 0, 0),
        },
    ),
    (
        "colon_t_ntz",
        f"{T}::TIMESTAMP_NTZ",
        {
            "America/New_York": dt.datetime(2024, 1, 1, 2, 0, 0),
            "Asia/Kolkata": dt.datetime(2024, 1, 1, 2, 0, 0),
            "UTC": dt.datetime(2024, 1, 1, 2, 0, 0),
        },
    ),
    (
        "try_cast_t_ntz",
        f"TRY_CAST({T} AS TIMESTAMP_NTZ)",
        {
            "America/New_York": dt.datetime(2024, 1, 1, 2, 0, 0),
            "Asia/Kolkata": dt.datetime(2024, 1, 1, 2, 0, 0),
            "UTC": dt.datetime(2024, 1, 1, 2, 0, 0),
        },
    ),
    (
        "cast_tz_ntz",
        "CAST(TIMESTAMP '2024-01-01 02:00:00Z' AS TIMESTAMP_NTZ)",
        {
            "America/New_York": dt.datetime(2023, 12, 31, 21, 0, 0),
            "Asia/Kolkata": dt.datetime(2024, 1, 1, 7, 30, 0),
            "UTC": dt.datetime(2024, 1, 1, 2, 0, 0),
        },
    ),
    (
        "cast_str_ntz",
        "CAST('2024-01-01 02:00:00' AS TIMESTAMP_NTZ)",
        {
            "America/New_York": dt.datetime(2024, 1, 1, 2, 0, 0),
            "Asia/Kolkata": dt.datetime(2024, 1, 1, 2, 0, 0),
            "UTC": dt.datetime(2024, 1, 1, 2, 0, 0),
        },
    ),
    (
        "cast_str_off_ntz",
        "CAST('2024-01-01 02:00:00+05:30' AS TIMESTAMP_NTZ)",
        {
            "America/New_York": dt.datetime(2024, 1, 1, 2, 0, 0),
            "Asia/Kolkata": dt.datetime(2024, 1, 1, 2, 0, 0),
            "UTC": dt.datetime(2024, 1, 1, 2, 0, 0),
        },
    ),
    (
        "try_cast_str_ntz",
        "TRY_CAST('2024-01-01 02:00:00' AS TIMESTAMP_NTZ)",
        {
            "America/New_York": dt.datetime(2024, 1, 1, 2, 0, 0),
            "Asia/Kolkata": dt.datetime(2024, 1, 1, 2, 0, 0),
            "UTC": dt.datetime(2024, 1, 1, 2, 0, 0),
        },
    ),
    (
        "try_cast_bad_str_ntz",
        "TRY_CAST('bad' AS TIMESTAMP_NTZ)",
        {"America/New_York": None, "Asia/Kolkata": None, "UTC": None},
    ),
    (
        "cast_date_ntz",
        "CAST(DATE '2024-01-01' AS TIMESTAMP_NTZ)",
        {
            "America/New_York": dt.datetime(2024, 1, 1, 0, 0, 0),
            "Asia/Kolkata": dt.datetime(2024, 1, 1, 0, 0, 0),
            "UTC": dt.datetime(2024, 1, 1, 0, 0, 0),
        },
    ),
    (
        "cast_ntz_plus_1h",
        f"CAST({T} AS TIMESTAMP_NTZ) + INTERVAL 1 HOUR",
        {
            "America/New_York": dt.datetime(2024, 1, 1, 3, 0, 0),
            "Asia/Kolkata": dt.datetime(2024, 1, 1, 3, 0, 0),
            "UTC": dt.datetime(2024, 1, 1, 3, 0, 0),
        },
    ),
)


@pytest.mark.parametrize("zone", ZONES)
@pytest.mark.parametrize("ansi", ANSI_MODES, ids=["ansi_on", "ansi_off"])
@pytest.mark.parametrize("door", DOORS)
def test_ntz_cast_scalars_across_doors_zones_and_ansi(zone: str, ansi: bool, door: str) -> None:
    spark = _new_session("fexpr-ntz-scalars", zone, ansi)
    try:
        for case_id, expr_sql, expected_by_zone in NTZ_CAST_SCALAR_CASES:
            df = _eval_door(spark, door, expr_sql)
            _assert_ntz_arrow(df, expected_by_zone[zone])
            assert case_id
        with pytest.raises(Exception, match="CAST_WITHOUT_SUGGESTION"):
            _eval_door(spark, door, "CAST(0 AS TIMESTAMP_NTZ)").to_arrow()
        if ansi:
            with pytest.raises(Exception, match="CAST_INVALID_INPUT"):
                _eval_door(spark, door, "CAST('bad' AS TIMESTAMP_NTZ)").to_arrow()
        else:
            bad_df = _eval_door(spark, door, "CAST('bad' AS TIMESTAMP_NTZ)")
            _assert_ntz_arrow(bad_df, None)
    finally:
        spark.stop()


@pytest.mark.parametrize("zone", ZONES)
@pytest.mark.parametrize("ansi", ANSI_MODES, ids=["ansi_on", "ansi_off"])
@pytest.mark.parametrize("door", DOORS)
def test_ntz_cast_columns_across_doors_zones_and_ansi(zone: str, ansi: bool, door: str) -> None:
    spark = _new_session("fexpr-ntz-cols", zone, ansi)
    try:
        base = spark.sql(f"SELECT {T} AS ts, '2024-01-01 02:00:00' AS s, DATE '2024-01-01' AS d")
        cases = (
            ("CAST(ts AS TIMESTAMP_NTZ)", dt.datetime(2024, 1, 1, 2, 0, 0)),
            ("TRY_CAST(ts AS TIMESTAMP_NTZ)", dt.datetime(2024, 1, 1, 2, 0, 0)),
            ("CAST(s AS TIMESTAMP_NTZ)", dt.datetime(2024, 1, 1, 2, 0, 0)),
            ("CAST(d AS TIMESTAMP_NTZ)", dt.datetime(2024, 1, 1, 0, 0, 0)),
        )
        for expr_sql, expected in cases:
            df = _eval_door(spark, door, expr_sql, base=base)
            _assert_ntz_arrow(df, expected)
    finally:
        spark.stop()


TTYPE_NTZ_CASES: tuple[tuple[str, str, dict[str, dt.datetime | None]], ...] = (
    (
        "ttype_ntz_lit",
        T,
        {
            "America/New_York": dt.datetime(2024, 1, 1, 2, 0, 0),
            "Asia/Kolkata": dt.datetime(2024, 1, 1, 2, 0, 0),
            "UTC": dt.datetime(2024, 1, 1, 2, 0, 0),
        },
    ),
    (
        "ttype_ntz_lit_gap",
        "TIMESTAMP '2024-03-10 02:30:00'",
        {
            "America/New_York": dt.datetime(2024, 3, 10, 2, 30, 0),
            "Asia/Kolkata": dt.datetime(2024, 3, 10, 2, 30, 0),
            "UTC": dt.datetime(2024, 3, 10, 2, 30, 0),
        },
    ),
    (
        "ttype_ntz_cast_str",
        "CAST('2024-01-01 02:00:00' AS TIMESTAMP)",
        {
            "America/New_York": dt.datetime(2024, 1, 1, 2, 0, 0),
            "Asia/Kolkata": dt.datetime(2024, 1, 1, 2, 0, 0),
            "UTC": dt.datetime(2024, 1, 1, 2, 0, 0),
        },
    ),
    (
        "ttype_ntz_cast_str_z",
        "CAST('2024-01-01 02:00:00Z' AS TIMESTAMP)",
        {
            "America/New_York": dt.datetime(2024, 1, 1, 2, 0, 0),
            "Asia/Kolkata": dt.datetime(2024, 1, 1, 2, 0, 0),
            "UTC": dt.datetime(2024, 1, 1, 2, 0, 0),
        },
    ),
    (
        "ttype_ntz_try_cast_str",
        "TRY_CAST('2024-01-01 02:00:00' AS TIMESTAMP)",
        {
            "America/New_York": dt.datetime(2024, 1, 1, 2, 0, 0),
            "Asia/Kolkata": dt.datetime(2024, 1, 1, 2, 0, 0),
            "UTC": dt.datetime(2024, 1, 1, 2, 0, 0),
        },
    ),
    (
        "ttype_ntz_try_cast_str_z",
        "TRY_CAST('2024-01-01 02:00:00Z' AS TIMESTAMP)",
        {
            "America/New_York": dt.datetime(2024, 1, 1, 2, 0, 0),
            "Asia/Kolkata": dt.datetime(2024, 1, 1, 2, 0, 0),
            "UTC": dt.datetime(2024, 1, 1, 2, 0, 0),
        },
    ),
    (
        "ttype_ntz_try_cast_bad",
        "TRY_CAST('bad' AS TIMESTAMP)",
        {"America/New_York": None, "Asia/Kolkata": None, "UTC": None},
    ),
    (
        "ttype_ntz_cast_date",
        "CAST(DATE '2024-01-01' AS TIMESTAMP)",
        {
            "America/New_York": dt.datetime(2024, 1, 1, 0, 0, 0),
            "Asia/Kolkata": dt.datetime(2024, 1, 1, 0, 0, 0),
            "UTC": dt.datetime(2024, 1, 1, 0, 0, 0),
        },
    ),
)


@pytest.mark.parametrize("zone", ZONES)
@pytest.mark.parametrize("ansi", ANSI_MODES, ids=["ansi_on", "ansi_off"])
@pytest.mark.parametrize("door", DOORS)
def test_timestamp_type_ntz_bare_expressions_across_doors(zone: str, ansi: bool, door: str) -> None:
    spark = _new_session("fexpr-ttype-ntz", zone, ansi, timestamp_type="TIMESTAMP_NTZ")
    try:
        for _case_id, expr_sql, expected_by_zone in TTYPE_NTZ_CASES:
            df = _eval_door(spark, door, expr_sql)
            _assert_ntz_arrow(df, expected_by_zone[zone])
    finally:
        spark.stop()


@pytest.mark.parametrize("zone", ZONES)
@pytest.mark.parametrize("ansi", ANSI_MODES, ids=["ansi_on", "ansi_off"])
@pytest.mark.parametrize("door", FEXPR_DOORS)
def test_timestamp_type_ntz_preserves_zoned_literals_and_explicit_ltz(
    zone: str, ansi: bool, door: str
) -> None:
    spark = _new_session("fexpr-ttype-ntz-ltz", zone, ansi, timestamp_type="TIMESTAMP_NTZ")
    try:
        expected_wall_instants = {
            "America/New_York": dt.datetime(2024, 1, 1, 7, 0, 0, tzinfo=dt.UTC),
            "Asia/Kolkata": dt.datetime(2023, 12, 31, 20, 30, 0, tzinfo=dt.UTC),
            "UTC": dt.datetime(2024, 1, 1, 2, 0, 0, tzinfo=dt.UTC),
        }
        expected_z = dt.datetime(2024, 1, 1, 2, 0, 0, tzinfo=dt.UTC)
        expected_off = dt.datetime(2023, 12, 31, 20, 30, 0, tzinfo=dt.UTC)
        _assert_ltz_arrow(_eval_door(spark, door, "TIMESTAMP '2024-01-01 02:00:00Z'"), expected_z)
        _assert_ltz_arrow(
            _eval_door(spark, door, "TIMESTAMP '2024-01-01 02:00:00+05:30'"), expected_off
        )
        _assert_ltz_arrow(
            _eval_door(spark, door, "CAST('2024-01-01 02:00:00' AS TIMESTAMP_LTZ)"),
            expected_wall_instants[zone],
        )
        _assert_ltz_arrow(
            _eval_door(spark, door, f"CAST({T} AS TIMESTAMP_LTZ)"),
            expected_wall_instants[zone],
        )
        _assert_ltz_arrow(
            _eval_door(spark, door, "TIMESTAMP_LTZ '2024-01-01 02:00:00'"),
            expected_wall_instants[zone],
        )
        base = spark.sql("SELECT '2024-01-01 02:00:00' AS s")
        _assert_ltz_arrow(
            _eval_door(spark, door, "CAST(s AS TIMESTAMP_LTZ)", base=base),
            expected_wall_instants[zone],
        )
        cast_z_walls = {
            "America/New_York": dt.datetime(2023, 12, 31, 21, 0, 0),
            "Asia/Kolkata": dt.datetime(2024, 1, 1, 7, 30, 0),
            "UTC": dt.datetime(2024, 1, 1, 2, 0, 0),
        }
        _assert_ntz_arrow(
            _eval_door(spark, door, "CAST(TIMESTAMP '2024-01-01 02:00:00Z' AS TIMESTAMP)"),
            cast_z_walls[zone],
        )
    finally:
        spark.stop()


@pytest.mark.parametrize("ansi", ANSI_MODES, ids=["ansi_on", "ansi_off"])
def test_zone_change_preserves_ntz_wall_and_unix_timestamp(ansi: bool) -> None:
    shapes = (
        ("cast_t_ntz", f"CAST({T} AS TIMESTAMP_NTZ)", "TIMESTAMP_LTZ"),
        ("cast_str_ntz", "CAST('2024-01-01 02:00:00' AS TIMESTAMP_NTZ)", "TIMESTAMP_LTZ"),
        ("ttype_ntz_lit", T, "TIMESTAMP_NTZ"),
        ("to_timestamp_ntz", "to_timestamp_ntz('2024-01-01 02:00:00')", "TIMESTAMP_LTZ"),
        ("make_timestamp_ntz", "make_timestamp_ntz(2024, 1, 1, 2, 0, 0)", "TIMESTAMP_LTZ"),
    )
    for _name, expr_sql, ttype in shapes:
        spark = _new_session("fexpr-ntz-zchg", "America/New_York", ansi, timestamp_type=ttype)
        try:
            col_expr = sf.expr(expr_sql)
            base = spark.sql("SELECT TIMESTAMP_NTZ '2024-01-01 02:00:00' AS ntz")
            df = base.withColumn("v", col_expr)
            for target_zone in ZONES:
                spark.conf.set("spark.sql.session.timeZone", target_zone)
                res = df.select(
                    sf.col("v"),
                    sf.col("v").cast("string").alias("s"),
                    sf.hour("v").alias("h"),
                    sf.date_format("v", "yyyy-MM-dd HH:mm").alias("fmt"),
                    sf.unix_timestamp("v").alias("u"),
                    (sf.col("v") == sf.col("ntz")).alias("eq"),
                )
                assert res.dtypes[0] == ("v", "timestamp_ntz")
                table = res.to_arrow()
                assert table.schema.field("v").type == NTZ_ARROW
                assert table.column("v")[0].as_py() == dt.datetime(2024, 1, 1, 2, 0, 0)
                assert table.column("s")[0].as_py() == "2024-01-01 02:00:00"
                assert table.column("h")[0].as_py() == 2
                assert table.column("fmt")[0].as_py() == "2024-01-01 02:00"
                assert table.column("u")[0].as_py() == 1704074400
                assert table.column("eq")[0].as_py() is True
        finally:
            spark.stop()


@pytest.mark.parametrize("zone", ZONES)
@pytest.mark.parametrize("ansi", ANSI_MODES, ids=["ansi_on", "ansi_off"])
def test_mixing_with_real_ntz_across_union_coalesce_case_join_and_filter(
    zone: str, ansi: bool
) -> None:
    shapes = (
        ("cast_t_ntz", f"CAST({T} AS TIMESTAMP_NTZ)", "TIMESTAMP_LTZ"),
        ("cast_str_ntz", "CAST('2024-01-01 02:00:00' AS TIMESTAMP_NTZ)", "TIMESTAMP_LTZ"),
        ("ttype_ntz_lit", T, "TIMESTAMP_NTZ"),
        ("to_timestamp_ntz", "to_timestamp_ntz('2024-01-01 02:00:00')", "TIMESTAMP_LTZ"),
        ("make_timestamp_ntz", "make_timestamp_ntz(2024, 1, 1, 2, 0, 0)", "TIMESTAMP_LTZ"),
    )
    expected = dt.datetime(2024, 1, 1, 2, 0, 0)
    for _name, expr_sql, ttype in shapes:
        spark = _new_session("fexpr-ntz-mix", zone, ansi, timestamp_type=ttype)
        try:
            df = spark.sql(
                "SELECT TIMESTAMP_NTZ '2024-01-01 02:00:00' AS ntz, "
                "TIMESTAMP_NTZ '2024-01-01 03:00:00' AS ntz_later, "
                "CAST(NULL AS TIMESTAMP_NTZ) AS ntz_null"
            )
            col_expr = sf.expr(expr_sql)
            u_df = df.select(col_expr.alias("v")).union(df.select(sf.col("ntz").alias("v")))
            u_arrow = u_df.to_arrow()
            assert u_df.dtypes == [("v", "timestamp_ntz")]
            assert u_arrow.schema.field("v").type == NTZ_ARROW
            assert u_arrow.column("v").to_pylist() == [expected, expected]

            for mixed_df in (
                df.select(sf.coalesce(col_expr, sf.col("ntz")).alias("v")),
                df.select(sf.coalesce(sf.col("ntz"), col_expr).alias("v")),
                df.select(sf.coalesce(sf.col("ntz_null"), col_expr).alias("v")),
                df.select(sf.when(sf.lit(True), col_expr).otherwise(sf.col("ntz")).alias("v")),
            ):
                _assert_ntz_arrow(mixed_df, expected)

            cmp_table = df.select(
                (col_expr == sf.col("ntz")).alias("eq"),
                sf.expr(f"({expr_sql}) <=> ntz").alias("eq_ns"),
                (col_expr < sf.col("ntz_later")).alias("lt"),
            ).to_arrow()
            assert cmp_table.column("eq")[0].as_py() is True
            assert cmp_table.column("eq_ns")[0].as_py() is True
            assert cmp_table.column("lt")[0].as_py() is True

            assert df.filter(sf.col("ntz") == col_expr).count() == 1
            assert df.filter(sf.expr(f"ntz = ({expr_sql})")).count() == 1
            assert df.filter(f"ntz = ({expr_sql})").count() == 1

            right = spark.range(1).select(col_expr.alias("v"))
            joined = df.select("ntz").join(right, df["ntz"] == right["v"], "inner")
            assert joined.dtypes == [("ntz", "timestamp_ntz"), ("v", "timestamp_ntz")]
            assert joined.count() == 1
        finally:
            spark.stop()


@pytest.mark.parametrize("ansi", ANSI_MODES, ids=["ansi_on", "ansi_off"])
def test_iceberg_and_parquet_writes_store_exact_ntz_and_ltz_values(
    tmp_path: Path, ansi: bool
) -> None:
    expected_wall = dt.datetime(2024, 1, 1, 2, 0, 0)
    expected_wall_micros = 1704074400000000
    expected_ltz_utc = dt.datetime(2024, 1, 1, 7, 0, 0, tzinfo=dt.UTC)
    shapes = (
        ("cast_t_ntz", f"CAST({T} AS TIMESTAMP_NTZ)", "TIMESTAMP_LTZ"),
        ("cast_str_ntz", "CAST('2024-01-01 02:00:00' AS TIMESTAMP_NTZ)", "TIMESTAMP_LTZ"),
        ("ttype_ntz_lit", T, "TIMESTAMP_NTZ"),
        ("to_timestamp_ntz", "to_timestamp_ntz('2024-01-01 02:00:00')", "TIMESTAMP_LTZ"),
        ("make_timestamp_ntz", "make_timestamp_ntz(2024, 1, 1, 2, 0, 0)", "TIMESTAMP_LTZ"),
    )
    for name, expr_sql, ttype in shapes:
        wh = tmp_path / f"wh_{name}_{ansi}"
        wh.mkdir()
        pq_dir = tmp_path / f"pq_{name}_{ansi}"
        spark = _new_session(
            f"fexpr-write-{name}", "America/New_York", ansi, timestamp_type=ttype, warehouse=wh
        )
        try:
            spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.db").collect()
            tbl_ntz = f"ice.db.t_ntz_{name}"
            tbl_ltz = f"ice.db.t_ltz_{name}"
            spark.sql(f"CREATE TABLE {tbl_ntz} (v TIMESTAMP_NTZ) USING iceberg").collect()
            spark.sql(f"CREATE TABLE {tbl_ltz} (v TIMESTAMP_LTZ) USING iceberg").collect()

            df = spark.range(1).select(sf.expr(expr_sql).alias("v"))
            df.writeTo(tbl_ntz).append()
            df.writeTo(tbl_ltz).append()
            df.write.mode("overwrite").parquet(str(pq_dir))

            raw_pq = pq.read_table(pq_dir)
            assert raw_pq.schema.field("v").type == NTZ_ARROW
            assert raw_pq.column("v").cast(pa.int64()).to_pylist() == [expected_wall_micros]

            for read_zone in ("America/New_York", "UTC"):
                spark.conf.set("spark.sql.session.timeZone", read_zone)
                _assert_ntz_arrow(spark.table(tbl_ntz), expected_wall)
                _assert_ntz_arrow(spark.read.parquet(str(pq_dir)), expected_wall)
                _assert_ltz_arrow(spark.table(tbl_ltz), expected_ltz_utc)
        finally:
            spark.stop()

    wh_sql = tmp_path / f"wh_sql_{ansi}"
    wh_sql.mkdir()
    spark = _new_session(
        "fexpr-write-sql-ltz",
        "America/New_York",
        ansi,
        timestamp_type="TIMESTAMP_NTZ",
        warehouse=wh_sql,
    )
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.db").collect()
        spark.sql("CREATE TABLE ice.db.t_sql_ltz (v TIMESTAMP_LTZ) USING iceberg").collect()
        spark.sql(f"INSERT INTO ice.db.t_sql_ltz SELECT {T} AS v").collect()
        _assert_ltz_arrow(spark.table("ice.db.t_sql_ltz"), expected_ltz_utc)
    finally:
        spark.stop()


@pytest.mark.parametrize("zone", ZONES)
@pytest.mark.parametrize("ansi", ANSI_MODES, ids=["ansi_on", "ansi_off"])
def test_neighbours_do_not_move(zone: str, ansi: bool) -> None:
    spark = _new_session("fexpr-ntz-neighbours", zone, ansi, timestamp_type="TIMESTAMP_LTZ")
    try:
        expected_zoneless = {
            "America/New_York": dt.datetime(2024, 1, 1, 7, 0, 0, tzinfo=dt.UTC),
            "Asia/Kolkata": dt.datetime(2023, 12, 31, 20, 30, 0, tzinfo=dt.UTC),
            "UTC": dt.datetime(2024, 1, 1, 2, 0, 0, tzinfo=dt.UTC),
        }[zone]
        expected_z = dt.datetime(2024, 1, 1, 2, 0, 0, tzinfo=dt.UTC)
        expected_off = dt.datetime(2023, 12, 31, 20, 30, 0, tzinfo=dt.UTC)
        expected_plus_1h = expected_zoneless + dt.timedelta(hours=1)
        for expr_sql, expected in (
            (T, expected_zoneless),
            ("TIMESTAMP '2024-01-01 02:00:00Z'", expected_z),
            ("TIMESTAMP '2024-01-01 02:00:00+05:30'", expected_off),
            ("CAST('2024-01-01 02:00:00' AS TIMESTAMP_LTZ)", expected_zoneless),
            ("CAST('2024-01-01 02:00:00Z' AS TIMESTAMP_LTZ)", expected_z),
            ("CAST(TIMESTAMP_NTZ '2024-01-01 02:00:00' AS TIMESTAMP_LTZ)", expected_zoneless),
            ("TRY_CAST('2024-01-01 02:00:00' AS TIMESTAMP_LTZ)", expected_zoneless),
            (f"{T} + INTERVAL 1 HOUR", expected_plus_1h),
        ):
            _assert_ltz_arrow(spark.range(1).select(sf.expr(expr_sql).alias("v")), expected)

        d_table = spark.range(1).select(sf.expr("DATE '2024-01-01'").alias("v")).to_arrow()
        assert d_table.schema.field("v").type == pa.date32()
        assert d_table.column("v")[0].as_py() == dt.date(2024, 1, 1)

        d2_expr = sf.expr("DATE '2024-01-01' + INTERVAL 1 DAY").alias("v")
        d2_table = spark.range(1).select(d2_expr).to_arrow()
        assert d2_table.column("v")[0].as_py() == dt.date(2024, 1, 2)

        _assert_ntz_arrow(
            spark.range(1).select(
                sf.expr("TIMESTAMP_NTZ '2024-01-01 02:00:00' + INTERVAL 1 HOUR").alias("v")
            ),
            dt.datetime(2024, 1, 1, 3, 0, 0),
        )
    finally:
        spark.stop()


def _explain_text(df: Any, extended: bool = False) -> str:
    import contextlib
    import io

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(extended=extended)
    return buf.getvalue()


@pytest.mark.parametrize("pushdown", [True, False], ids=["pd_on", "pd_off"])
def test_delta1_fa1_mixed_ntz_ltz_filter_parquet_and_iceberg(
    tmp_path: Path, pushdown: bool
) -> None:
    wh = tmp_path / f"wh_fa1_{pushdown}"
    wh.mkdir()
    pq_dir = tmp_path / f"pq_fa1_{pushdown}"
    _reset_active_session_for_tests()
    spark = (
        ReparkSession.builder.appName(f"fa1-{pushdown}")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.timestampType", "TIMESTAMP_NTZ")
        .config("datafusion.execution.parquet.pushdown_filters", str(pushdown).lower())
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", str(wh))
        .getOrCreate()
    )
    try:
        p1 = pq_dir / "part-1.parquet"
        p2 = pq_dir / "part-2.parquet"
        pq_dir.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table(
                {
                    "id": pa.array([1], type=pa.int32()),
                    "ts": pa.array([dt.datetime(2024, 1, 1, 2, 0, 0)], type=NTZ_ARROW),
                }
            ),
            p1,
        )
        pq.write_table(
            pa.table(
                {
                    "id": pa.array([2], type=pa.int32()),
                    "ts": pa.array([dt.datetime(2024, 1, 1, 7, 0, 0)], type=NTZ_ARROW),
                }
            ),
            p2,
        )
        pq_df = spark.read.parquet(str(pq_dir))
        for pred in (
            "ts = CAST('2024-01-01 02:00:00' AS TIMESTAMP_LTZ)",
            "ts = TIMESTAMP_LTZ '2024-01-01 02:00:00'",
        ):
            assert [r[0] for r in pq_df.filter(sf.expr(pred)).select("id").collect()] == [1]
            assert [r[0] for r in pq_df.filter(pred).select("id").collect()] == [1]

        spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.db").collect()
        for part_name, part_expr in (("days", "days(ts)"), ("hours", "hours(ts)")):
            tbl = f"ice.db.fa1_{part_name}"
            spark.sql(
                f"CREATE TABLE {tbl} (id INT, ts TIMESTAMP_NTZ) USING iceberg "
                f"PARTITIONED BY ({part_expr})"
            ).collect()
            spark.sql(
                f"INSERT INTO {tbl} VALUES "
                "(1, TIMESTAMP_NTZ '2024-01-01 23:00:00'), "
                "(2, TIMESTAMP_NTZ '2024-01-02 04:00:00')"
            ).collect()
            tdf = spark.table(tbl)
            for pred in (
                "ts = CAST('2024-01-01 23:00:00' AS TIMESTAMP_LTZ)",
                "ts = TIMESTAMP_LTZ '2024-01-01 23:00:00'",
            ):
                assert [r[0] for r in tdf.filter(sf.expr(pred)).select("id").collect()] == [1]
                assert [r[0] for r in tdf.filter(pred).select("id").collect()] == [1]
    finally:
        spark.stop()


@pytest.mark.parametrize("ansi", ANSI_MODES, ids=["ansi_on", "ansi_off"])
def test_delta1_fa2_explicit_ltz_casts_on_sql_and_select_expr(ansi: bool) -> None:
    spark = _new_session("fa2-ltz", "America/New_York", ansi, timestamp_type="TIMESTAMP_NTZ")
    try:
        src = spark.sql("SELECT '2024-01-01 02:00:00Z' AS sz, 'bad' AS sb")
        src.createOrReplaceTempView("fa2_src")
        want_ny = dt.datetime(2024, 1, 1, 7, 0, 0, tzinfo=dt.UTC)
        want_z = dt.datetime(2024, 1, 1, 2, 0, 0, tzinfo=dt.UTC)
        for expr_sql, want in (
            ("CAST('2024-01-01 02:00:00' AS TIMESTAMP_LTZ)", want_ny),
            ("CAST('2024-01-01 02:00:00Z' AS TIMESTAMP_LTZ)", want_z),
            ("TRY_CAST('2024-01-01 02:00:00Z' AS TIMESTAMP_LTZ)", want_z),
            ("TRY_CAST(sz AS TIMESTAMP_LTZ)", want_z),
        ):
            _assert_ltz_arrow(src.selectExpr(f"{expr_sql} AS v"), want)
            _assert_ltz_arrow(spark.sql(f"SELECT {expr_sql} AS v FROM fa2_src"), want)
        for expr_sql in (
            "TRY_CAST('bad' AS TIMESTAMP_LTZ)",
            "TRY_CAST(sb AS TIMESTAMP_LTZ)",
            "TRY_CAST(NULL AS TIMESTAMP_LTZ)",
        ):
            for df in (
                src.selectExpr(f"{expr_sql} AS v"),
                spark.sql(f"SELECT {expr_sql} AS v FROM fa2_src"),
            ):
                table = df.to_arrow()
                assert table.schema.field("v").type == LTZ_ARROW
                assert table.column("v")[0].as_py() is None
    finally:
        spark.stop()


def test_delta1_fa3_merge_update_and_insert_values_into_ltz(tmp_path: Path) -> None:
    wh = tmp_path / "wh_fa3"
    wh.mkdir()
    spark = _new_session(
        "fa3-dml", "America/New_York", True, timestamp_type="TIMESTAMP_NTZ", warehouse=wh
    )
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.db").collect()
        spark.sql("CREATE TABLE ice.db.fa3_ltz (id INT, v TIMESTAMP_LTZ) USING iceberg").collect()
        spark.sql("SELECT 1 AS id").select(
            sf.col("id"), sf.expr("TIMESTAMP_LTZ '2020-01-01 00:00:00'").alias("v")
        ).writeTo("ice.db.fa3_ltz").append()
        msrc = (
            spark.sql("SELECT 1 AS id")
            .select(
                sf.col("id"),
                sf.expr("CAST(TIMESTAMP '2024-01-01 02:00:00.123456' AS TIMESTAMP_NTZ)").alias("v"),
            )
            .union(
                spark.sql("SELECT 2 AS id").select(
                    sf.col("id"),
                    sf.expr("CAST(TIMESTAMP '2024-01-01 02:00:00.123456' AS TIMESTAMP_NTZ)").alias(
                        "v"
                    ),
                )
            )
        )
        msrc.createOrReplaceTempView("fa3_src")
        spark.sql(
            "MERGE INTO ice.db.fa3_ltz t USING fa3_src s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET t.v = s.v "
            "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
        ).collect()
        want = dt.datetime(2024, 1, 1, 7, 0, 0, 123456, tzinfo=dt.UTC)
        merged = spark.sql("SELECT id, v FROM ice.db.fa3_ltz ORDER BY id").to_arrow()
        assert merged.schema.field("v").type == LTZ_ARROW
        assert merged.column("v").to_pylist() == [want, want]

        spark.sql(
            "UPDATE ice.db.fa3_ltz SET v = TIMESTAMP_NTZ '2024-01-01 02:00:00' WHERE id = 1"
        ).collect()
        spark.sql(
            "INSERT INTO ice.db.fa3_ltz VALUES "
            "(3, TIMESTAMP_NTZ '2024-01-01 02:00:00'), "
            "(4, TIMESTAMP '2024-01-01 02:00:00')"
        ).collect()
        want_whole = dt.datetime(2024, 1, 1, 7, 0, 0, tzinfo=dt.UTC)
        after = spark.sql("SELECT id, v FROM ice.db.fa3_ltz ORDER BY id").to_arrow()
        assert after.column("v").to_pylist() == [want_whole, want, want_whole, want_whole]
    finally:
        spark.stop()


def test_delta1_fa4_dst_gap_wall_clock_never_shifts_for_ntz(tmp_path: Path) -> None:
    pq_dir = tmp_path / "pq_fa4"
    spark = _new_session("fa4", "America/New_York", True, timestamp_type="TIMESTAMP_NTZ")
    try:
        gap_wall = dt.datetime(2024, 3, 10, 2, 30, 0, 654321)
        for expr_sql in (
            "CAST('2024-03-10 02:30:00.654321' AS TIMESTAMP_NTZ)",
            "TRY_CAST('2024-03-10 02:30:00.654321' AS TIMESTAMP)",
            "CAST('2024-03-10 02:30:00.654321' AS TIMESTAMP)",
        ):
            _assert_ntz_arrow(spark.range(1).select(sf.expr(expr_sql).alias("v")), gap_wall)
        spark.range(1).select(
            sf.expr("CAST('2024-03-10 02:30:00.654321' AS TIMESTAMP_NTZ)").alias("v")
        ).write.mode("overwrite").parquet(str(pq_dir))
        _assert_ntz_arrow(spark.read.parquet(str(pq_dir)), gap_wall)
    finally:
        spark.stop()


def test_delta1_fa5_offset_string_writes_under_ntz(tmp_path: Path) -> None:
    wh = tmp_path / "wh_fa5"
    wh.mkdir()
    spark = _new_session(
        "fa5", "America/New_York", True, timestamp_type="TIMESTAMP_NTZ", warehouse=wh
    )
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.db").collect()
        spark.sql("CREATE TABLE ice.db.fa5_ltz (id INT, v TIMESTAMP_LTZ) USING iceberg").collect()
        spark.sql("CREATE TABLE ice.db.fa5_ntz (v TIMESTAMP_NTZ) USING iceberg").collect()
        spark.sql(
            "INSERT INTO ice.db.fa5_ltz SELECT 1, CAST('2024-01-01 02:00:00+05:30' AS TIMESTAMP)"
        ).collect()
        _assert_ltz_arrow(
            spark.sql("SELECT v FROM ice.db.fa5_ltz"),
            dt.datetime(2024, 1, 1, 7, 0, 0, tzinfo=dt.UTC),
        )
        spark.range(1).select(
            sf.expr("CAST('2024-01-01 02:00:00+05:30' AS TIMESTAMP)").alias("v")
        ).writeTo("ice.db.fa5_ntz").append()
        _assert_ntz_arrow(spark.table("ice.db.fa5_ntz"), dt.datetime(2024, 1, 1, 2, 0, 0))
    finally:
        spark.stop()


def test_delta1_fa6_named_zone_and_seconds_offset_literals() -> None:
    spark = _new_session("fa6", "America/New_York", True, timestamp_type="TIMESTAMP_NTZ")
    try:
        for expr_sql, expected_utc in (
            (
                "TIMESTAMP '2024-01-01 02:00:00 America/New_York'",
                dt.datetime(2024, 1, 1, 7, 0, 0, tzinfo=dt.UTC),
            ),
            (
                "TIMESTAMP '2024-01-01 02:00:00 UTC'",
                dt.datetime(2024, 1, 1, 2, 0, 0, tzinfo=dt.UTC),
            ),
            (
                "TIMESTAMP '2024-01-01 02:00:00+05:30:00'",
                dt.datetime(2023, 12, 31, 20, 30, 0, tzinfo=dt.UTC),
            ),
            (
                "TIMESTAMP '2024-01-01 02:00:00+053000'",
                dt.datetime(2023, 12, 31, 20, 30, 0, tzinfo=dt.UTC),
            ),
            (
                "TIMESTAMP '2024-01-01 02:00:00 GMT+02:00'",
                dt.datetime(2024, 1, 1, 0, 0, 0, tzinfo=dt.UTC),
            ),
        ):
            _assert_ltz_arrow(spark.range(1).select(sf.expr(expr_sql).alias("v")), expected_utc)
    finally:
        spark.stop()


def test_delta1_fa7_union_bad_string_raises_cast_invalid_input_under_ntz() -> None:
    spark = _new_session("fa7", "America/New_York", True, timestamp_type="TIMESTAMP_NTZ")
    try:
        base = spark.sql("SELECT 1 AS id")
        u = base.select(sf.expr("CAST('bad' AS TIMESTAMP)").alias("v")).union(
            base.select(sf.expr(T).alias("v"))
        )
        with pytest.raises(Exception, match="CAST_INVALID_INPUT"):
            u.collect()
    finally:
        spark.stop()


@pytest.mark.parametrize("zone", ["America/New_York", "UTC"])
@pytest.mark.parametrize("ttype", ["TIMESTAMP_LTZ", "TIMESTAMP_NTZ"])
def test_delta1_fa8_date_trunc_over_ntz_returns_ltz_all_units(zone: str, ttype: str) -> None:
    spark = _new_session("fa8-dt", zone, True, timestamp_type=ttype)
    try:
        expected_ny = {
            "year": dt.datetime(2024, 1, 1, 5, 0, 0, tzinfo=dt.UTC),
            "yyyy": dt.datetime(2024, 1, 1, 5, 0, 0, tzinfo=dt.UTC),
            "yy": dt.datetime(2024, 1, 1, 5, 0, 0, tzinfo=dt.UTC),
            "quarter": dt.datetime(2024, 1, 1, 5, 0, 0, tzinfo=dt.UTC),
            "month": dt.datetime(2024, 3, 1, 5, 0, 0, tzinfo=dt.UTC),
            "mon": dt.datetime(2024, 3, 1, 5, 0, 0, tzinfo=dt.UTC),
            "mm": dt.datetime(2024, 3, 1, 5, 0, 0, tzinfo=dt.UTC),
            "week": dt.datetime(2024, 3, 4, 5, 0, 0, tzinfo=dt.UTC),
            "day": dt.datetime(2024, 3, 10, 5, 0, 0, tzinfo=dt.UTC),
            "dd": dt.datetime(2024, 3, 10, 5, 0, 0, tzinfo=dt.UTC),
            "hour": dt.datetime(2024, 3, 10, 7, 0, 0, tzinfo=dt.UTC),
            "minute": dt.datetime(2024, 3, 10, 7, 30, 0, tzinfo=dt.UTC),
            "second": dt.datetime(2024, 3, 10, 7, 30, 45, tzinfo=dt.UTC),
            "millisecond": dt.datetime(2024, 3, 10, 7, 30, 45, 123000, tzinfo=dt.UTC),
            "microsecond": dt.datetime(2024, 3, 10, 7, 30, 45, 123456, tzinfo=dt.UTC),
        }
        expected_utc = {
            "year": dt.datetime(2024, 1, 1, 0, 0, 0, tzinfo=dt.UTC),
            "yyyy": dt.datetime(2024, 1, 1, 0, 0, 0, tzinfo=dt.UTC),
            "yy": dt.datetime(2024, 1, 1, 0, 0, 0, tzinfo=dt.UTC),
            "quarter": dt.datetime(2024, 1, 1, 0, 0, 0, tzinfo=dt.UTC),
            "month": dt.datetime(2024, 3, 1, 0, 0, 0, tzinfo=dt.UTC),
            "mon": dt.datetime(2024, 3, 1, 0, 0, 0, tzinfo=dt.UTC),
            "mm": dt.datetime(2024, 3, 1, 0, 0, 0, tzinfo=dt.UTC),
            "week": dt.datetime(2024, 3, 4, 0, 0, 0, tzinfo=dt.UTC),
            "day": dt.datetime(2024, 3, 10, 0, 0, 0, tzinfo=dt.UTC),
            "dd": dt.datetime(2024, 3, 10, 0, 0, 0, tzinfo=dt.UTC),
            "hour": dt.datetime(2024, 3, 10, 2, 0, 0, tzinfo=dt.UTC),
            "minute": dt.datetime(2024, 3, 10, 2, 30, 0, tzinfo=dt.UTC),
            "second": dt.datetime(2024, 3, 10, 2, 30, 45, tzinfo=dt.UTC),
            "millisecond": dt.datetime(2024, 3, 10, 2, 30, 45, 123000, tzinfo=dt.UTC),
            "microsecond": dt.datetime(2024, 3, 10, 2, 30, 45, 123456, tzinfo=dt.UTC),
        }
        want_map = expected_ny if zone == "America/New_York" else expected_utc
        for unit, want in want_map.items():
            expr_sql = f"date_trunc('{unit}', TIMESTAMP_NTZ '2024-03-10 02:30:45.123456')"
            _assert_ltz_arrow(spark.range(1).select(sf.expr(expr_sql).alias("v")), want)
            _assert_ltz_arrow(spark.sql(f"SELECT {expr_sql} AS v"), want)
    finally:
        spark.stop()


@pytest.mark.parametrize("ttype", ["TIMESTAMP_LTZ", "TIMESTAMP_NTZ"])
@pytest.mark.parametrize("ansi", ANSI_MODES, ids=["ansi_on", "ansi_off"])
def test_delta1_fa9_schema_nullability_and_ctas(tmp_path: Path, ttype: str, ansi: bool) -> None:
    wh = tmp_path / f"wh_fa9_{ttype}_{ansi}"
    wh.mkdir()
    spark = _new_session("fa9", "America/New_York", ansi, timestamp_type=ttype, warehouse=wh)
    try:
        base = spark.sql("SELECT 1 AS id")
        assert (
            base.select(sf.expr("CAST('2024-01-01 02:00:00' AS TIMESTAMP)").alias("v"))
            .schema[0]
            .nullable
            is True
        )
        assert (
            base.select(sf.expr("TRY_CAST('2024-03-10 02:30:00' AS TIMESTAMP)").alias("v"))
            .schema[0]
            .nullable
            is True
        )
        assert (
            base.select(sf.expr("CAST(DATE '2024-01-01' AS TIMESTAMP_LTZ)").alias("v"))
            .schema[0]
            .nullable
            is False
        )
        assert base.select(sf.expr(f"CAST({T} AS DATE)").alias("v")).schema[0].nullable is (
            ttype == "TIMESTAMP_NTZ"
        )
        assert base.select(sf.expr(f"unix_timestamp({T})").alias("v")).schema[0].nullable is (
            not ansi
        )

        spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.db").collect()
        df_ctas = spark.range(1).select(
            sf.expr("CAST('2024-01-01 02:00:00' AS TIMESTAMP)").alias("v")
        )
        assert df_ctas.schema[0].nullable is True
        df_ctas.writeTo("ice.db.fa9_ctas").create()
        assert spark.table("ice.db.fa9_ctas").schema[0].nullable is True
    finally:
        spark.stop()


@pytest.mark.parametrize("ttype", ["TIMESTAMP_LTZ", "TIMESTAMP_NTZ"])
def test_delta1_fa10_marker_free_display_names_and_explain(ttype: str) -> None:
    spark = _new_session("fa10", "America/New_York", True, timestamp_type=ttype)
    try:
        base = spark.sql(
            "SELECT 1 AS id, '2024-01-01 02:00:00' AS s, TIMESTAMP '2024-01-01 02:00:00' AS ts"
        )
        src_s = spark.sql("SELECT '2024-01-01 02:00:00' AS s")
        assert src_s.selectExpr("TRY_CAST(s AS TIMESTAMP_LTZ)").columns == ["s"]
        assert src_s.selectExpr("TRY_CAST(s AS TIMESTAMP)").columns == ["s"]
        assert spark.sql(
            "SELECT TRY_CAST(s AS TIMESTAMP_LTZ) FROM (SELECT '2024-01-01 02:00:00' AS s)"
        ).columns == ["s"]

        markers = (
            "__repark_cast_timestamp_ltz__",
            "__repark_try_cast_timestamp_ltz__",
            "__repark_cast_timestamp_ntz__",
            "__repark_try_cast_timestamp_ntz__",
        )
        for df in (
            base.select(sf.expr("CAST(s AS TIMESTAMP_NTZ)")),
            base.select(sf.expr("CAST(ts AS TIMESTAMP_NTZ)")),
            base.select(sf.expr("TRY_CAST(ts AS TIMESTAMP_NTZ)")),
            base.select(sf.expr("CAST('2024-01-01 02:00:00' AS TIMESTAMP_NTZ)")),
            base.filter("ts = CAST('2024-01-01 02:00:00' AS TIMESTAMP_NTZ)"),
        ):
            for ext in (False, True):
                text = _explain_text(df, extended=ext)
                for marker in markers:
                    assert marker not in text, f"{marker} leaked in explain(extended={ext}): {text}"
    finally:
        spark.stop()
