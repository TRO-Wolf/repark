from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from repark import ReparkSession

CAST_INVALID = "[CAST_INVALID_INPUT]"
CANNOT_PARSE_TS = "[CANNOT_PARSE_TIMESTAMP]"

DELTA1_ACCEPT_PINS: tuple[tuple[str, str, str, str | None, str, int], ...] = (
    (
        "fa1_left_join_nomatch_ow",
        "UTC",
        "DOUBLE",
        "9.0",
        "INSERT OVERWRITE {t} SELECT CAST(r.s AS DOUBLE) FROM local.db.real_id2 l "
        "LEFT JOIN (SELECT 1 AS id, 'x' AS s) r ON l.id = r.id",
        1,
    ),
    (
        "fa1_left_join_nomatch_into",
        "UTC",
        "DOUBLE",
        "9.0",
        "INSERT INTO {t} SELECT CAST(r.s AS DOUBLE) FROM local.db.real_id2 l "
        "LEFT JOIN (SELECT 1 AS id, 'x' AS s) r ON l.id = r.id",
        2,
    ),
    (
        "fa1_left_join_nomatch_ctas",
        "UTC",
        "CTAS",
        None,
        "CREATE TABLE {t} USING iceberg AS SELECT CAST(r.s AS DOUBLE) AS x "
        "FROM local.db.real_id2 l LEFT JOIN (SELECT 1 AS id, 'x' AS s) r ON l.id = r.id",
        1,
    ),
    (
        "fa1_left_join_on_false_ts",
        "UTC",
        "TIMESTAMP",
        "TIMESTAMP '2024-01-01 00:00:00'",
        "INSERT OVERWRITE {t} SELECT CAST(r.s AS TIMESTAMP) FROM local.db.real_id2 l "
        "LEFT JOIN (SELECT 1 AS id, 'x' AS s) r ON false",
        1,
    ),
    (
        "fa1_right_join_nomatch_ts",
        "UTC",
        "TIMESTAMP",
        "TIMESTAMP '2024-01-01 00:00:00'",
        "INSERT OVERWRITE {t} SELECT CAST(l.s AS TIMESTAMP) FROM "
        "(SELECT 1 AS id, 'x' AS s) l RIGHT JOIN local.db.real_id2 r ON l.id = r.id",
        1,
    ),
    (
        "fa1_full_join_left_const_live",
        "UTC",
        "TIMESTAMP",
        "TIMESTAMP '2024-01-01 00:00:00'",
        "INSERT OVERWRITE {t} SELECT CAST(l.s AS TIMESTAMP) FROM (SELECT 1 AS id, 'x' AS s) l "
        "FULL OUTER JOIN local.db.real_id2 r ON l.id = r.id WHERE l.id IS NULL",
        1,
    ),
    (
        "fa1_left_join_real0_on_true",
        "UTC",
        "DOUBLE",
        "9.0",
        "INSERT OVERWRITE {t} SELECT CAST(r.s AS DOUBLE) FROM local.db.real_empty l "
        "LEFT JOIN (SELECT 1 AS id, 'x' AS s) r ON true",
        0,
    ),
    (
        "fa1_inner_join_on_false",
        "UTC",
        "DOUBLE",
        "9.0",
        "INSERT OVERWRITE {t} SELECT CAST(r.s AS DOUBLE) FROM local.db.real_id2 l "
        "INNER JOIN (SELECT 1 AS id, 'x' AS s) r ON false",
        0,
    ),
    (
        "fa2_merge_nomatch_src_col",
        "UTC",
        "MERGE_TS",
        None,
        "MERGE INTO {t} tgt USING (SELECT 2 AS id, 'x' AS s) src ON tgt.id = src.id "
        "WHEN MATCHED THEN UPDATE SET x = CAST(src.s AS TIMESTAMP)",
        1,
    ),
    (
        "fa2_merge_on_false_src_col",
        "UTC",
        "MERGE_TS",
        None,
        "MERGE INTO {t} tgt USING (SELECT 1 AS id, 'x' AS s) src ON false "
        "WHEN MATCHED THEN UPDATE SET x = CAST(src.s AS TIMESTAMP)",
        1,
    ),
    (
        "fa5_date_trimmed",
        "UTC",
        "DATE",
        "DATE '2024-01-01'",
        "INSERT INTO {t} SELECT CAST(s AS DATE) FROM (SELECT ' 2024-01-01 ' AS s) q WHERE 1 = 0",
        1,
    ),
    (
        "fa5_date_year_only",
        "UTC",
        "DATE",
        "DATE '2024-01-01'",
        "INSERT INTO {t} SELECT CAST(s AS DATE) FROM (SELECT '2024' AS s) q WHERE 1 = 0",
        1,
    ),
    (
        "fa5_date_year_month",
        "UTC",
        "DATE",
        "DATE '2024-01-01'",
        "INSERT INTO {t} SELECT CAST(s AS DATE) FROM (SELECT '2024-01' AS s) q WHERE 1 = 0",
        1,
    ),
    (
        "fa5_double_hex_float",
        "UTC",
        "DOUBLE",
        "1.0",
        "INSERT OVERWRITE {t} SELECT CAST(s AS DOUBLE) FROM (SELECT '0x1.0p0' AS s) q WHERE 1 = 0",
        0,
    ),
    (
        "fa5_decimal_sci",
        "UTC",
        "DECIMAL(5, 0)",
        "CAST(1 AS DECIMAL(5, 0))",
        "INSERT INTO {t} SELECT CAST(s AS DECIMAL(5, 0)) FROM (SELECT '1e4' AS s) q WHERE 1 = 0",
        1,
    ),
    *(
        (
            f"fa4_ntz_{tag}_{tz.replace('/', '_')}",
            tz,
            "TIMESTAMP_NTZ",
            "TIMESTAMP_NTZ '2024-01-01 00:00:00'",
            f"INSERT OVERWRITE {{t}} SELECT CAST(s AS TIMESTAMP_NTZ) "
            f"FROM (SELECT '{raw}' AS s) q WHERE 1 = 0",
            0,
        )
        for tz in ("UTC", "America/New_York")
        for tag, raw in (
            ("md", "2024-1-1"),
            ("trim", " 2024-01-01 00:00:00 "),
            ("ym", "2024-01"),
            ("y", "2024"),
        )
    ),
)

DELTA1_REFUSE_PINS: tuple[tuple[str, str, str, str, str], ...] = (
    (
        "fc1_to_ts_ltz_2arg",
        "TIMESTAMP",
        "TIMESTAMP '2024-01-01 00:00:00'",
        "SELECT to_timestamp_ltz(s, 'yyyy-MM-dd') FROM (SELECT 'x' AS s) q WHERE 1 = 0",
        CANNOT_PARSE_TS,
    ),
    (
        "fc1_to_ts_ntz_2arg",
        "TIMESTAMP_NTZ",
        "TIMESTAMP_NTZ '2024-01-01 00:00:00'",
        "SELECT to_timestamp_ntz(s, 'yyyy-MM-dd') FROM (SELECT 'x' AS s) q WHERE 1 = 0",
        CANNOT_PARSE_TS,
    ),
    (
        "fc2_ungrouped_agg_limit0",
        "TIMESTAMP",
        "TIMESTAMP '2024-01-01 00:00:00'",
        "SELECT CAST('x' AS TIMESTAMP) FROM (SELECT COUNT(*) AS c FROM (SELECT 1 AS a LIMIT 0)) q "
        "WHERE 1 = 0",
        CAST_INVALID,
    ),
)

DELTA1_REPLACED_VIEW_PINS: tuple[tuple[str, str, str, str, str, str | None], ...] = (
    (
        "fa3_replaced_view_live_cast_inside",
        "CREATE OR REPLACE TEMP VIEW d1_v1 AS SELECT 'x' AS s",
        "CREATE OR REPLACE TEMP VIEW d1_v2 AS SELECT CAST(s AS TIMESTAMP) AS x FROM d1_v1",
        "CREATE OR REPLACE TEMP VIEW d1_v1 AS SELECT '2024-01-01 00:00:00' AS s",
        "INSERT OVERWRITE {t} SELECT x FROM d1_v2",
        None,
    ),
    (
        "fa3_replaced_view_live_cast_outside",
        "CREATE OR REPLACE TEMP VIEW d1_va AS SELECT 'x' AS s",
        "CREATE OR REPLACE TEMP VIEW d1_vb AS SELECT s FROM d1_va",
        "CREATE OR REPLACE TEMP VIEW d1_va AS SELECT '2024-01-01 00:00:00' AS s",
        "INSERT OVERWRITE {t} SELECT CAST(s AS TIMESTAMP) FROM d1_vb",
        None,
    ),
    (
        "fa3_replaced_view_empty_now_bad",
        "CREATE OR REPLACE TEMP VIEW d1_vc AS SELECT '2024-01-01 00:00:00' AS s",
        "CREATE OR REPLACE TEMP VIEW d1_vd AS SELECT s FROM d1_vc",
        "CREATE OR REPLACE TEMP VIEW d1_vc AS SELECT 'x' AS s",
        "INSERT OVERWRITE {t} SELECT CAST(s AS TIMESTAMP) FROM d1_vd WHERE 1 = 0",
        CAST_INVALID,
    ),
)


@pytest.fixture
def delta1_session(tmp_path: Path) -> Iterator[ReparkSession]:
    session = (
        ReparkSession.builder.appName("insert-overwrite-const-cast-1-delta1")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", str(tmp_path / "wh"))
        .getOrCreate()
    )
    session.sql("CREATE NAMESPACE IF NOT EXISTS local.db").collect()
    session.sql("CREATE TABLE local.db.real_empty (s STRING) USING iceberg").collect()
    session.sql("CREATE TABLE local.db.real_id2 (id INT) USING iceberg").collect()
    session.sql("INSERT INTO local.db.real_id2 VALUES (2)").collect()
    yield session
    session.stop()


def _create_flat_table(session: ReparkSession, name: str, col_type: str, seed: str | None) -> str:
    fq = f"local.db.{name}"
    session.sql(f"DROP TABLE IF EXISTS {fq}").collect()
    session.sql(f"CREATE TABLE {fq} (x {col_type}) USING iceberg").collect()
    if seed is not None:
        session.sql(f"INSERT INTO {fq} VALUES ({seed})").collect()
    return fq


def _row_count(session: ReparkSession, fq: str) -> int:
    return int(session.sql(f"SELECT * FROM {fq}").toArrow().num_rows)


@pytest.mark.parametrize(
    ("name", "tz", "col_type", "seed", "sql_tpl", "expected_rows"),
    DELTA1_ACCEPT_PINS,
    ids=[row[0] for row in DELTA1_ACCEPT_PINS],
)
def test_delta1_spark_valid_writes_succeed(
    delta1_session: ReparkSession,
    name: str,
    tz: str,
    col_type: str,
    seed: str | None,
    sql_tpl: str,
    expected_rows: int,
) -> None:
    delta1_session.conf.set("spark.sql.ansi.enabled", "true")
    delta1_session.conf.set("spark.sql.session.timeZone", tz)
    fq = f"local.db.d1_{name.lower()}"
    if col_type == "CTAS":
        delta1_session.sql(f"DROP TABLE IF EXISTS {fq}").collect()
    elif col_type == "MERGE_TS":
        delta1_session.sql(f"DROP TABLE IF EXISTS {fq}").collect()
        delta1_session.sql(f"CREATE TABLE {fq} (id INT, x TIMESTAMP) USING iceberg").collect()
        delta1_session.sql(
            f"INSERT INTO {fq} VALUES (1, TIMESTAMP '2024-01-01 00:00:00')"
        ).collect()
    else:
        fq = _create_flat_table(delta1_session, f"d1_{name.lower()}", col_type, seed)
    delta1_session.sql(sql_tpl.format(t=fq)).collect()
    assert _row_count(delta1_session, fq) == expected_rows


@pytest.mark.parametrize(
    ("name", "col_type", "seed", "source_sql", "expected_class"),
    DELTA1_REFUSE_PINS,
    ids=[row[0] for row in DELTA1_REFUSE_PINS],
)
def test_delta1_refusal_rules(
    delta1_session: ReparkSession,
    name: str,
    col_type: str,
    seed: str,
    source_sql: str,
    expected_class: str,
) -> None:
    delta1_session.conf.set("spark.sql.ansi.enabled", "true")
    fq = _create_flat_table(delta1_session, f"d1r_{name}", col_type, seed)
    with pytest.raises(Exception) as exc_info:
        delta1_session.sql(f"INSERT OVERWRITE {fq} {source_sql}").collect()
    assert expected_class in str(exc_info.value)
    assert _row_count(delta1_session, fq) == 1


@pytest.mark.parametrize(
    ("name", "ddl1", "ddl2", "ddl3", "write_tpl", "expected_err"),
    DELTA1_REPLACED_VIEW_PINS,
    ids=[row[0] for row in DELTA1_REPLACED_VIEW_PINS],
)
def test_delta1_replaced_temp_view_live_resolution(
    delta1_session: ReparkSession,
    name: str,
    ddl1: str,
    ddl2: str,
    ddl3: str,
    write_tpl: str,
    expected_err: str | None,
) -> None:
    delta1_session.conf.set("spark.sql.ansi.enabled", "true")
    delta1_session.sql(ddl1).collect()
    delta1_session.sql(ddl2).collect()
    delta1_session.sql(ddl3).collect()
    fq = _create_flat_table(
        delta1_session,
        f"d1v_{name}",
        "TIMESTAMP",
        "TIMESTAMP '2024-01-01 00:00:00'",
    )
    if expected_err is None:
        delta1_session.sql(write_tpl.format(t=fq)).collect()
    else:
        with pytest.raises(Exception) as exc_info:
            delta1_session.sql(write_tpl.format(t=fq)).collect()
        assert expected_err in str(exc_info.value)
    assert _row_count(delta1_session, fq) == 1
