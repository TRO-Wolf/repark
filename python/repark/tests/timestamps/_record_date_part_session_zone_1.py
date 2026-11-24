from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

FIXTURE = Path(__file__).resolve().parent / "date_part_session_zone_1_spark_oracle.json"
ZONES: tuple[str, ...] = ("UTC", "America/New_York", "Asia/Kolkata")
ANSI_MODES: tuple[str, ...] = ("false", "true")

ROWS_TS: tuple[tuple[str, dt.datetime | None], ...] = (
    ("ny_0000", dt.datetime(2024, 6, 15, 4, 0, 0, tzinfo=dt.UTC)),
    ("ny_1930", dt.datetime(2024, 1, 1, 0, 30, 0, tzinfo=dt.UTC)),
    ("ny_2100", dt.datetime(2024, 1, 1, 2, 0, 0, tzinfo=dt.UTC)),
    ("ny_2345", dt.datetime(2024, 6, 1, 3, 45, 12, 500000, tzinfo=dt.UTC)),
    ("leap_day", dt.datetime(2024, 3, 1, 2, 15, 30, tzinfo=dt.UTC)),
    ("pre_1970_a", dt.datetime(1969, 12, 31, 23, 30, 0, tzinfo=dt.UTC)),
    ("pre_1970_b", dt.datetime(1970, 1, 1, 2, 0, 0, tzinfo=dt.UTC)),
    ("lmt_1880", dt.datetime(1880, 6, 15, 12, 0, 0, tzinfo=dt.UTC)),
    ("post_2038", dt.datetime(2040, 1, 1, 2, 30, 0, tzinfo=dt.UTC)),
    ("post_2099", dt.datetime(2100, 7, 1, 3, 30, 0, tzinfo=dt.UTC)),
    ("yow_2021", dt.datetime(2021, 1, 1, 2, 0, 0, tzinfo=dt.UTC)),
    ("yow_2025_edge", dt.datetime(2024, 12, 30, 2, 0, 0, tzinfo=dt.UTC)),
    ("kolkata_half", dt.datetime(2024, 1, 1, 18, 45, 0, tzinfo=dt.UTC)),
    ("dst_spring_pre", dt.datetime(2024, 3, 10, 6, 59, 59, tzinfo=dt.UTC)),
    ("dst_spring_at", dt.datetime(2024, 3, 10, 7, 0, 0, tzinfo=dt.UTC)),
    ("dst_fall_edt", dt.datetime(2024, 11, 3, 5, 30, 0, tzinfo=dt.UTC)),
    ("dst_fall_est", dt.datetime(2024, 11, 3, 6, 30, 0, tzinfo=dt.UTC)),
    ("null_ts", None),
)

INT_FIELDS: tuple[str, ...] = (
    "year",
    "yearofweek",
    "quarter",
    "month",
    "week",
    "day",
    "dayofweek",
    "dow",
    "dayofweek_iso",
    "dow_iso",
    "doy",
    "hour",
    "minute",
)

ALIAS_FIELDS: tuple[str, ...] = (
    "y",
    "yr",
    "yrs",
    "years",
    "qtr",
    "mon",
    "mons",
    "months",
    "w",
    "weeks",
    "d",
    "days",
    "h",
    "hr",
    "hrs",
    "hours",
    "m",
    "min",
    "mins",
    "minutes",
)

EXTRACT_FIELDS: tuple[str, ...] = (
    "YEAR",
    "QUARTER",
    "MONTH",
    "WEEK",
    "DAY",
    "DOW",
    "DAYOFWEEK",
    "DOY",
    "HOUR",
    "MINUTE",
)

SECOND_FIELDS: tuple[str, ...] = ("second", "s", "sec", "secs", "seconds")

SEAT_FIELDS: tuple[str, ...] = (
    "year",
    "yearofweek",
    "quarter",
    "month",
    "week",
    "day",
    "dayofweek",
    "dow",
    "dayofweek_iso",
    "dow_iso",
    "doy",
    "hour",
    "minute",
)

FN_SPELLINGS: tuple[tuple[str, str], ...] = (
    ("year", "year(ts)"),
    ("quarter", "quarter(ts)"),
    ("month", "month(ts)"),
    ("weekofyear", "weekofyear(ts)"),
    ("day", "day(ts)"),
    ("dayofmonth", "dayofmonth(ts)"),
    ("dayofweek", "dayofweek(ts)"),
    ("weekday", "weekday(ts)"),
    ("dayofyear", "dayofyear(ts)"),
    ("hour", "hour(ts)"),
    ("minute", "minute(ts)"),
    ("second", "second(ts)"),
)


def write_multi_rg_parquet(path: Path) -> None:
    schema = pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("ts", pa.timestamp("us", tz="UTC"), nullable=True),
        ]
    )
    with pq.ParquetWriter(str(path), schema) as writer:
        for start in range(0, len(ROWS_TS), 4):
            chunk = ROWS_TS[start : start + 4]
            batch = pa.Table.from_pydict(
                {
                    "id": list(range(start, start + len(chunk))),
                    "ts": [moment for _, moment in chunk],
                },
                schema=schema,
            )
            writer.write_table(batch)


def ts_literal(moment: dt.datetime | None) -> str:
    if moment is None:
        return "CAST(NULL AS TIMESTAMP)"
    return f"TIMESTAMP '{moment.strftime('%Y-%m-%d %H:%M:%S.%f')} UTC'"


def seed_iceberg_table(session: Any, table: str) -> None:
    session.sql(f"CREATE TABLE {table} (id INT, ts TIMESTAMP) USING iceberg").collect()
    for start in range(0, len(ROWS_TS), 4):
        chunk = ROWS_TS[start : start + 4]
        values = ", ".join(
            f"({start + offset}, {ts_literal(moment)})" for offset, (_, moment) in enumerate(chunk)
        )
        session.sql(f"INSERT INTO {table} VALUES {values}").collect()


def register_control_views(session: Any) -> None:
    session.sql(
        "CREATE OR REPLACE TEMP VIEW dp_ntz_src AS "
        "SELECT 0 AS id, TIMESTAMP_NTZ '2023-12-31 21:15:30.500000' AS ts "
        "UNION ALL SELECT 1 AS id, TIMESTAMP_NTZ '2024-02-29 23:45:00' AS ts "
        "UNION ALL SELECT 2 AS id, CAST(NULL AS TIMESTAMP_NTZ) AS ts"
    ).collect()
    session.sql(
        "CREATE OR REPLACE TEMP VIEW dp_date_src AS "
        "SELECT 0 AS id, DATE '2023-12-31' AS ts "
        "UNION ALL SELECT 1 AS id, DATE '2024-02-29' AS ts "
        "UNION ALL SELECT 2 AS id, CAST(NULL AS DATE) AS ts"
    ).collect()


def _collect_columns(df: Any, allow_errors: bool = False) -> dict[str, Any]:
    try:
        rows = df.collect()
    except Exception as exc:
        if not allow_errors:
            raise
        return {"error": str(exc).splitlines()[0]}
    names = df.columns
    out: dict[str, list[int | None]] = {}
    for index, name in enumerate(names):
        if name == "id":
            continue
        out[name] = [None if row[index] is None else int(row[index]) for row in rows]
    return out


def _collect_ids(df: Any, allow_errors: bool = False) -> Any:
    try:
        return [int(row[0]) for row in df.collect()]
    except Exception as exc:
        if not allow_errors:
            raise
        return {"error": str(exc).splitlines()[0]}


def _collect_pairs(df: Any, allow_errors: bool = False) -> Any:
    try:
        return [[int(row[0]), int(row[1])] for row in df.collect()]
    except Exception as exc:
        if not allow_errors:
            raise
        return {"error": str(exc).splitlines()[0]}


def _collect_tuples(df: Any, allow_errors: bool = False) -> Any:
    try:
        return [[None if v is None else int(v) for v in row] for row in df.collect()]
    except Exception as exc:
        if not allow_errors:
            raise
        return {"error": str(exc).splitlines()[0]}


def measure_all(
    session: Any,
    functions: Any,
    parquet_path: Path,
    iceberg_table: str,
    catalog_ns: str,
    warehouse: Path,
    cold_session_factory: Any,
    allow_errors: bool = False,
) -> dict[str, Any]:
    results: dict[str, Any] = {}
    register_control_views(session)
    pq_df = session.read.parquet(str(parquet_path))
    pq_df.createOrReplaceTempView("dp_pq_src")

    for zone in ZONES:
        for ansi in ANSI_MODES:
            session.conf.set("spark.sql.session.timeZone", zone)
            session.conf.set("spark.sql.ansi.enabled", ansi)
            key_prefix = f"{zone}|{ansi}"

            dp_cols = ", ".join(
                f"date_part('{field}', ts) AS dp_{field}, datepart('{field}', ts) AS dpa_{field}"
                for field in (*INT_FIELDS, *ALIAS_FIELDS)
            )
            results[f"{key_prefix}|sql_dp_pq"] = _collect_columns(
                session.sql(f"SELECT id, {dp_cols} FROM dp_pq_src ORDER BY id"), allow_errors
            )
            results[f"{key_prefix}|sql_dp_ice"] = _collect_columns(
                session.sql(f"SELECT id, {dp_cols} FROM {iceberg_table} ORDER BY id"), allow_errors
            )

            ex_cols = ", ".join(
                f"extract({field} FROM ts) AS ex_{field.lower()}" for field in EXTRACT_FIELDS
            )
            results[f"{key_prefix}|sql_ex_pq"] = _collect_columns(
                session.sql(f"SELECT id, {ex_cols} FROM dp_pq_src ORDER BY id"), allow_errors
            )
            results[f"{key_prefix}|sql_ex_ice"] = _collect_columns(
                session.sql(f"SELECT id, {ex_cols} FROM {iceberg_table} ORDER BY id"), allow_errors
            )

            sec_cols = ", ".join(
                f"date_part('{field}', ts) AS dp_{field}" for field in SECOND_FIELDS
            )
            results[f"{key_prefix}|sql_sec_int"] = _collect_columns(
                session.sql(
                    f"SELECT id, {sec_cols}, extract(SECOND FROM ts) AS ex_second "
                    "FROM dp_pq_src ORDER BY id"
                ),
                allow_errors,
            )

            base_df = session.sql("SELECT id, ts FROM dp_pq_src ORDER BY id")
            se_exprs = ["id"]
            fe_cols = [functions.col("id")]
            typed_cols = [functions.col("id")]
            for field in SEAT_FIELDS:
                se_exprs.append(f"date_part('{field}', ts) AS dp_{field}")
                se_exprs.append(f"datepart('{field}', ts) AS dpa_{field}")
                fe_cols.append(functions.expr(f"date_part('{field}', ts)").alias(f"dp_{field}"))
                fe_cols.append(functions.expr(f"datepart('{field}', ts)").alias(f"dpa_{field}"))
                typed_cols.append(
                    functions.date_part(functions.lit(field), functions.col("ts")).alias(
                        f"dp_{field}"
                    )
                )
                typed_cols.append(
                    functions.datepart(functions.lit(field), functions.col("ts")).alias(
                        f"dpa_{field}"
                    )
                )
                typed_cols.append(
                    functions.extract(functions.lit(field), functions.col("ts")).alias(
                        f"ex_{field}"
                    )
                )
            for field in EXTRACT_FIELDS:
                se_exprs.append(f"extract({field} FROM ts) AS ex_{field.lower()}")
                fe_cols.append(
                    functions.expr(f"extract({field} FROM ts)").alias(f"ex_{field.lower()}")
                )
            results[f"{key_prefix}|selectExpr"] = _collect_columns(
                base_df.selectExpr(*se_exprs).orderBy("id"), allow_errors
            )
            results[f"{key_prefix}|F.expr"] = _collect_columns(
                base_df.select(*fe_cols).orderBy("id"), allow_errors
            )
            results[f"{key_prefix}|typed"] = _collect_columns(
                base_df.select(*typed_cols).orderBy("id"), allow_errors
            )

            view_cols = ", ".join(
                [
                    *(f"date_part('{f}', ts) AS dp_{f}" for f in SEAT_FIELDS),
                    *(f"extract({f} FROM ts) AS ex_{f.lower()}" for f in EXTRACT_FIELDS),
                ]
            )
            session.sql(
                f"CREATE OR REPLACE TEMP VIEW dp_view_body AS SELECT id, {view_cols} FROM dp_pq_src"
            ).collect()
            results[f"{key_prefix}|view"] = _collect_columns(
                session.sql("SELECT * FROM dp_view_body ORDER BY id"), allow_errors
            )

            lit_expr = ts_literal(ROWS_TS[2][1])
            lit_cols = ", ".join(
                [
                    *(f"date_part('{f}', {lit_expr}) AS dp_{f}" for f in SEAT_FIELDS),
                    *(f"extract({f} FROM {lit_expr}) AS ex_{f.lower()}" for f in EXTRACT_FIELDS),
                ]
            )
            results[f"{key_prefix}|literal"] = _collect_columns(
                session.sql(f"SELECT {lit_cols}"), allow_errors
            )

            results[f"{key_prefix}|where_pq_dp_h21"] = _collect_ids(
                session.sql(
                    "SELECT id FROM dp_pq_src WHERE date_part('hour', ts) = 21 ORDER BY id"
                ),
                allow_errors,
            )
            results[f"{key_prefix}|where_pq_ex_h21"] = _collect_ids(
                session.sql(
                    "SELECT id FROM dp_pq_src WHERE extract(HOUR FROM ts) = 21 ORDER BY id"
                ),
                allow_errors,
            )
            results[f"{key_prefix}|where_pq_dp_y2023"] = _collect_ids(
                session.sql(
                    "SELECT id FROM dp_pq_src WHERE date_part('year', ts) = 2023 ORDER BY id"
                ),
                allow_errors,
            )
            results[f"{key_prefix}|where_pq_ex_y2023"] = _collect_ids(
                session.sql(
                    "SELECT id FROM dp_pq_src WHERE extract(YEAR FROM ts) = 2023 ORDER BY id"
                ),
                allow_errors,
            )
            results[f"{key_prefix}|where_ice_dp_h21"] = _collect_ids(
                session.sql(
                    f"SELECT id FROM {iceberg_table} WHERE date_part('hour', ts) = 21 ORDER BY id"
                ),
                allow_errors,
            )
            results[f"{key_prefix}|where_ice_ex_h21"] = _collect_ids(
                session.sql(
                    f"SELECT id FROM {iceberg_table} WHERE extract(HOUR FROM ts) = 21 ORDER BY id"
                ),
                allow_errors,
            )
            results[f"{key_prefix}|where_ice_dp_y2023"] = _collect_ids(
                session.sql(
                    f"SELECT id FROM {iceberg_table} WHERE date_part('year', ts) = 2023 ORDER BY id"
                ),
                allow_errors,
            )
            results[f"{key_prefix}|where_ice_ex_y2023"] = _collect_ids(
                session.sql(
                    f"SELECT id FROM {iceberg_table} WHERE extract(YEAR FROM ts) = 2023 ORDER BY id"
                ),
                allow_errors,
            )
            results[f"{key_prefix}|groupby_day"] = _collect_pairs(
                session.sql(
                    f"SELECT date_part('day', ts) AS d, COUNT(*) AS c "
                    f"FROM {iceberg_table} WHERE ts IS NOT NULL "
                    "GROUP BY date_part('day', ts) ORDER BY d"
                ),
                allow_errors,
            )
            results[f"{key_prefix}|join_day"] = _collect_ids(
                session.sql(
                    f"SELECT a.id FROM {iceberg_table} a "
                    "JOIN (SELECT 31 AS k UNION ALL SELECT 29 AS k) b "
                    "ON date_part('day', a.ts) = b.k ORDER BY a.id"
                ),
                allow_errors,
            )

            fn_cols = ", ".join(f"{expr} AS fn_{name}" for name, expr in FN_SPELLINGS)
            results[f"{key_prefix}|fn_spellings"] = _collect_columns(
                session.sql(f"SELECT id, {fn_cols} FROM dp_pq_src ORDER BY id"), allow_errors
            )

            ctrl_cols = ", ".join(
                [
                    *(f"date_part('{f}', ts) AS dp_{f}" for f in INT_FIELDS),
                    *(f"extract({f} FROM ts) AS ex_{f.lower()}" for f in EXTRACT_FIELDS),
                ]
            )
            results[f"{key_prefix}|ctrl_ntz"] = _collect_columns(
                session.sql(f"SELECT id, {ctrl_cols} FROM dp_ntz_src ORDER BY id"), allow_errors
            )
            results[f"{key_prefix}|ctrl_date"] = _collect_columns(
                session.sql(f"SELECT id, {ctrl_cols} FROM dp_date_src ORDER BY id"), allow_errors
            )

            ztag = zone.replace("/", "_").lower()
            atag = "a1" if ansi == "true" else "a0"
            ctas_tbl = f"{catalog_ns}.ctas_{ztag}_{atag}"
            ins_tbl = f"{catalog_ns}.ins_{ztag}_{atag}"
            session.sql(
                f"CREATE TABLE {ctas_tbl} USING iceberg AS "
                "SELECT id, date_part('year', ts) AS y, date_part('month', ts) AS mo, "
                "date_part('day', ts) AS d, date_part('dayofweek', ts) AS dow, "
                "date_part('hour', ts) AS h, date_part('minute', ts) AS mi, "
                f"extract(HOUR FROM ts) AS eh, extract(DOW FROM ts) AS edow FROM {iceberg_table}"
            ).collect()
            session.sql(
                f"CREATE TABLE {ins_tbl} "
                "(id INT, y INT, mo INT, d INT, dow INT, h INT, mi INT, eh INT, edow INT) "
                "USING iceberg"
            ).collect()
            session.sql(
                f"INSERT INTO {ins_tbl} "
                "SELECT id, date_part('year', ts), date_part('month', ts), "
                "date_part('day', ts), date_part('dayofweek', ts), "
                "date_part('hour', ts), date_part('minute', ts), "
                f"extract(HOUR FROM ts), extract(DOW FROM ts) FROM {iceberg_table}"
            ).collect()

    cold = cold_session_factory()
    try:
        cold.conf.set("spark.sql.session.timeZone", "UTC")
        ns_dir = warehouse / catalog_ns.split(".")[-1]
        for zone in ZONES:
            for ansi in ANSI_MODES:
                ztag = zone.replace("/", "_").lower()
                atag = "a1" if ansi == "true" else "a0"
                key_prefix = f"{zone}|{ansi}"
                cold.read.format("iceberg").load(
                    str(ns_dir / f"ctas_{ztag}_{atag}")
                ).createOrReplaceTempView("dp_cold_ctas")
                cold.read.format("iceberg").load(
                    str(ns_dir / f"ins_{ztag}_{atag}")
                ).createOrReplaceTempView("dp_cold_ins")
                results[f"{key_prefix}|ctas_cold"] = _collect_tuples(
                    cold.sql(
                        "SELECT id, y, mo, d, dow, h, mi, eh, edow FROM dp_cold_ctas ORDER BY id"
                    )
                )
                results[f"{key_prefix}|ins_cold"] = _collect_tuples(
                    cold.sql(
                        "SELECT id, y, mo, d, dow, h, mi, eh, edow FROM dp_cold_ins ORDER BY id"
                    )
                )
    finally:
        cold.stop()

    return results


def _spark_session(warehouse: Path) -> Any:
    os.environ["TZ"] = "UTC"
    time.tzset()
    from pyspark.sql import SparkSession

    jar = Path.home() / ".ivy2/jars/org.apache.iceberg_iceberg-spark-runtime-4.1_2.13-1.11.0.jar"
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("record-date-part-session-zone-1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "1")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.dp1", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.dp1.type", "hadoop")
        .config("spark.sql.catalog.dp1.warehouse", str(warehouse))
    )
    if jar.is_file():
        builder = builder.config("spark.jars", str(jar))
    else:
        builder = builder.config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0",
        )
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("OFF")
    return session


def record_spark() -> dict[str, Any]:
    from pyspark.sql import functions as spark_functions

    with tempfile.TemporaryDirectory(prefix="dp-record-") as tmp:
        root = Path(tmp)
        pq_path = root / "events.parquet"
        write_multi_rg_parquet(pq_path)
        warehouse = root / "wh"
        warehouse.mkdir()
        session = _spark_session(warehouse)
        version = session.version
        try:
            session.conf.set("spark.sql.session.timeZone", "UTC")
            session.sql("CREATE NAMESPACE IF NOT EXISTS dp1.db").collect()
            seed_iceberg_table(session, "dp1.db.events")
            cells = measure_all(
                session,
                spark_functions,
                pq_path,
                "dp1.db.events",
                "dp1.db",
                warehouse,
                lambda: _spark_session(warehouse),
            )
        finally:
            session.stop()
    return {
        "spark": version,
        "recorder": "python/repark/tests/timestamps/_record_date_part_session_zone_1.py",
        "rows": [label for label, _ in ROWS_TS],
        "cells": cells,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--out", type=Path, default=FIXTURE)
    args = parser.parse_args()
    document = record_spark()
    if args.check:
        committed = json.loads(args.out.read_text(encoding="utf-8"))
        if document != committed:
            print("date-part-session-zone-1 oracle drift", file=sys.stderr)
            return 1
        print(f"date-part-session-zone-1 oracle matches ({len(document['cells'])} cells)")
        return 0
    args.out.write_text(json.dumps(document, indent=1) + "\n", encoding="utf-8")
    print(f"wrote {len(document['cells'])} cells to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
