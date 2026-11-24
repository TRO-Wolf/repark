from __future__ import annotations

import argparse
import json
import math
import re
import shutil
import sys
import tempfile
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

FIXTURE = Path(__file__).resolve().parent / "ansi_off_narrowing_overflow_1_spark_oracle.json"
ANSI_MODES: tuple[str, ...] = ("false", "true")
SEATS: tuple[str, ...] = (
    "sql-literal",
    "sql-column",
    "dataframe",
    "expr",
    "selectexpr",
    "view",
    "try-cast",
)
_CONDITION = re.compile(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]")
_STATE = re.compile(r"SQLSTATE: ([0-9A-Z]{5})")

SCALAR_SPECS: tuple[tuple[str, str, str, tuple[str | None, ...]], ...] = (
    (
        "bigint_to_int",
        "BIGINT",
        "INT",
        (
            "-9223372036854775808",
            "-2147483649",
            "-2147483648",
            "0",
            "1",
            "2147483647",
            "2147483648",
            "4294967297",
            "9223372036854775807",
            None,
        ),
    ),
    (
        "bigint_to_smallint",
        "BIGINT",
        "SMALLINT",
        (
            "-9223372036854775808",
            "-32769",
            "-32768",
            "0",
            "32767",
            "32768",
            "65537",
            "9223372036854775807",
            None,
        ),
    ),
    (
        "bigint_to_tinyint",
        "BIGINT",
        "TINYINT",
        (
            "-9223372036854775808",
            "-129",
            "-128",
            "0",
            "127",
            "128",
            "300",
            "9223372036854775807",
            None,
        ),
    ),
    (
        "int_to_smallint",
        "INT",
        "SMALLINT",
        ("-2147483648", "-32769", "-32768", "0", "32767", "32768", "65537", "2147483647", None),
    ),
    (
        "int_to_tinyint",
        "INT",
        "TINYINT",
        ("-2147483648", "-129", "-128", "0", "127", "128", "300", "2147483647", None),
    ),
    (
        "smallint_to_tinyint",
        "SMALLINT",
        "TINYINT",
        ("-32768", "-129", "-128", "0", "127", "128", "300", "32767", None),
    ),
    (
        "dec10_2_to_tinyint",
        "DECIMAL(10,2)",
        "TINYINT",
        ("-129.00", "-128.99", "-128.00", "-0.99", "0.00", "127.99", "128.00", "300.99", None),
    ),
    (
        "dec10_2_to_smallint",
        "DECIMAL(10,2)",
        "SMALLINT",
        ("-32769.00", "-32768.99", "0.00", "32767.99", "32768.00", "99999999.99", None),
    ),
    (
        "dec38_0_to_int",
        "DECIMAL(38,0)",
        "INT",
        (
            "-2147483649",
            "-2147483648",
            "0",
            "2147483647",
            "2147483648",
            "4294967297",
            "99999999999999999999",
            None,
        ),
    ),
    (
        "dec38_0_to_bigint",
        "DECIMAL(38,0)",
        "BIGINT",
        (
            "-9223372036854775809",
            "-9223372036854775808",
            "0",
            "9223372036854775807",
            "9223372036854775808",
            None,
        ),
    ),
    (
        "dec38_2_to_bigint",
        "DECIMAL(38,2)",
        "BIGINT",
        (
            "-9223372036854775809.00",
            "-9223372036854775808.99",
            "9223372036854775807.99",
            "9223372036854775808.00",
            None,
        ),
    ),
    (
        "dec10_2_to_dec3_2",
        "DECIMAL(10,2)",
        "DECIMAL(3,2)",
        ("-10.00", "-9.99", "0.00", "9.99", "10.00", "123.45", "99999999.99", None),
    ),
    (
        "dec10_3_to_dec3_2",
        "DECIMAL(10,3)",
        "DECIMAL(3,2)",
        ("-10.000", "-9.995", "-9.994", "0.000", "9.994", "9.995", "10.000", None),
    ),
    (
        "float_to_tinyint",
        "FLOAT",
        "TINYINT",
        (
            "-1e10",
            "-129.0",
            "-128.9",
            "-1.9",
            "-0.0",
            "0.0",
            "1.9",
            "127.9",
            "128.0",
            "1e10",
            "NaN",
            "Infinity",
            "-Infinity",
            None,
        ),
    ),
    (
        "float_to_smallint",
        "FLOAT",
        "SMALLINT",
        (
            "-1e10",
            "-32769.0",
            "-32768.0",
            "-0.0",
            "32767.0",
            "32768.0",
            "1e10",
            "NaN",
            "Infinity",
            "-Infinity",
            None,
        ),
    ),
    (
        "float_to_int",
        "FLOAT",
        "INT",
        (
            "-1e20",
            "-2147483648.0",
            "-1.5",
            "-0.0",
            "1.5",
            "2147483648.0",
            "1e10",
            "NaN",
            "Infinity",
            "-Infinity",
            None,
        ),
    ),
    (
        "float_to_bigint",
        "FLOAT",
        "BIGINT",
        (
            "-1e38",
            "-9223372036854775808.0",
            "-0.0",
            "9223371487098961920.0",
            "9223372036854775808.0",
            "1e38",
            "NaN",
            "Infinity",
            "-Infinity",
            None,
        ),
    ),
    (
        "double_to_tinyint",
        "DOUBLE",
        "TINYINT",
        (
            "-1e20",
            "-129.0",
            "-128.9",
            "-1.9",
            "-0.0",
            "0.0",
            "1.9",
            "127.9",
            "128.0",
            "1e10",
            "9007199254740992.0",
            "NaN",
            "Infinity",
            "-Infinity",
            None,
        ),
    ),
    (
        "double_to_smallint",
        "DOUBLE",
        "SMALLINT",
        (
            "-1e20",
            "-32769.0",
            "-32768.9",
            "-0.0",
            "32767.9",
            "32768.0",
            "9007199254740992.0",
            "NaN",
            "Infinity",
            "-Infinity",
            None,
        ),
    ),
    (
        "double_to_int",
        "DOUBLE",
        "INT",
        (
            "-1e20",
            "-2147483649.0",
            "-2147483648.9",
            "-1.5",
            "-0.0",
            "1.5",
            "2147483647.9",
            "2147483648.0",
            "9007199254740992.0",
            "NaN",
            "Infinity",
            "-Infinity",
            None,
        ),
    ),
    (
        "double_to_bigint",
        "DOUBLE",
        "BIGINT",
        (
            "-1e100",
            "-9223372036854777856.0",
            "-9223372036854775808.0",
            "-1.5",
            "-0.0",
            "1.5",
            "9223372036854774784.0",
            "9223372036854775808.0",
            "9223372036854777856.0",
            "1e100",
            "NaN",
            "Infinity",
            "-Infinity",
            None,
        ),
    ),
)

CARD_COMPARISON_SPECS: tuple[tuple[str, str, str, tuple[str | None, ...], tuple[str, ...]], ...] = (
    (
        "bigint_int",
        "BIGINT",
        "INT",
        ("1", "2147483648", "4294967297", "-2147483649", None),
        (
            "CAST(c AS INT) = 1",
            "CAST(c AS INT) = 5",
            "1 = CAST(c AS INT)",
            "CAST(c AS INT) != 1",
            "CAST(c AS INT) <=> 1",
            "CAST(c AS INT) > 0",
            "CAST(c AS INT) IN (1, 5)",
            "CAST(c AS INT) BETWEEN 0 AND 1",
        ),
    ),
    (
        "int_smallint",
        "INT",
        "SMALLINT",
        ("1", "32768", "65537", "-32769", None),
        (
            "CAST(c AS SMALLINT) = 1",
            "CAST(c AS SMALLINT) = 5",
            "CAST(c AS SMALLINT) <=> 1",
            "CAST(c AS SMALLINT) > 0",
        ),
    ),
    (
        "dec_tinyint_overflow",
        "DECIMAL(10,2)",
        "TINYINT",
        ("1.00", "127.99", "128.00", "257.50", "-129.00", None),
        (
            "CAST(c AS TINYINT) = 1",
            "CAST(c AS TINYINT) = 5",
            "CAST(c AS TINYINT) > 127",
            "CAST(c AS TINYINT) < -128",
            "CAST(c AS TINYINT) <=> -128",
        ),
    ),
    (
        "dec38_int_overflow",
        "DECIMAL(38,0)",
        "INT",
        ("1", "2147483648", "4294967297", None),
        (
            "CAST(c AS INT) = 1",
            "CAST(c AS INT) = 5",
            "CAST(c AS INT) <=> 1",
        ),
    ),
    (
        "dec_prec_overflow",
        "DECIMAL(10,2)",
        "DECIMAL(3,2)",
        ("1.25", "9.99", "10.00", "123.45", None),
        (
            "CAST(c AS DECIMAL(3,2)) = 1.25",
            "CAST(c AS DECIMAL(3,2)) = 5.00",
            "CAST(c AS DECIMAL(3,2)) IS NULL",
            "CAST(c AS DECIMAL(3,2)) <=> 1.25",
            "CAST(c AS DECIMAL(3,2)) IN (1.25, 9.99)",
            "CAST(c AS DECIMAL(3,2)) BETWEEN 1.00 AND 2.00",
        ),
    ),
    (
        "dec_round_overflow",
        "DECIMAL(10,3)",
        "DECIMAL(3,2)",
        ("1.250", "9.994", "9.995", "-9.995", None),
        (
            "CAST(c AS DECIMAL(3,2)) = 9.99",
            "CAST(c AS DECIMAL(3,2)) IS NULL",
            "CAST(c AS DECIMAL(3,2)) <=> 9.99",
        ),
    ),
)

PARQUET_BARRIER_QUERIES: tuple[tuple[str, str, str], ...] = (
    ("tiny_max_1_300", "tiny_pair", "SELECT max(CAST(x AS TINYINT)) AS v FROM tiny_pair"),
    ("tiny_min_1_300", "tiny_pair", "SELECT min(CAST(x AS TINYINT)) AS v FROM tiny_pair"),
    ("int_rg_max", "int_rg", "SELECT max(CAST(b AS TINYINT)) AS v FROM int_rg"),
    ("int_rg_min", "int_rg", "SELECT min(CAST(b AS TINYINT)) AS v FROM int_rg"),
    ("int_rg_eq", "int_rg", "SELECT id FROM int_rg WHERE CAST(b AS INT) = 1 ORDER BY id"),
    ("int_rg_gt", "int_rg", "SELECT id FROM int_rg WHERE CAST(b AS TINYINT) > 40 ORDER BY id"),
    (
        "int_rg_in",
        "int_rg",
        "SELECT id FROM int_rg WHERE CAST(b AS TINYINT) IN (1, 44) ORDER BY id",
    ),
    ("dec_rg_max", "dec_rg", "SELECT max(CAST(d AS DECIMAL(3,2))) AS v FROM dec_rg"),
    ("dec_rg_min", "dec_rg", "SELECT min(CAST(d AS DECIMAL(3,2))) AS v FROM dec_rg"),
    (
        "dec_rg_null",
        "dec_rg",
        "SELECT id FROM dec_rg WHERE CAST(d AS DECIMAL(3,2)) IS NULL ORDER BY id",
    ),
    ("dec_rg_eq", "dec_rg", "SELECT id FROM dec_rg WHERE CAST(d AS TINYINT) = -128 ORDER BY id"),
    ("flt_rg_max", "flt_rg", "SELECT max(CAST(f AS TINYINT)) AS v FROM flt_rg"),
    ("flt_rg_min", "flt_rg", "SELECT min(CAST(f AS TINYINT)) AS v FROM flt_rg"),
    ("flt_rg_eq", "flt_rg", "SELECT id FROM flt_rg WHERE CAST(f AS TINYINT) = 0 ORDER BY id"),
    ("flt_rg_gt", "flt_rg", "SELECT id FROM flt_rg WHERE CAST(f AS TINYINT) > 0 ORDER BY id"),
    (
        "flt_rg_in",
        "flt_rg",
        "SELECT id FROM flt_rg WHERE CAST(f AS TINYINT) IN (-1, 0) ORDER BY id",
    ),
)

REACH_QUERIES: tuple[tuple[str, str], ...] = (
    (
        "implicit_in",
        "SELECT x FROM (VALUES (CAST(1 AS INT)), (CAST(2 AS INT))) AS t(x) "
        "WHERE x IN (CAST(1 AS BIGINT), CAST(4294967297 AS BIGINT)) ORDER BY x",
    ),
    (
        "implicit_case",
        "SELECT CASE WHEN flag THEN CAST(1 AS INT) ELSE CAST(4294967297 AS BIGINT) END AS v "
        "FROM (VALUES (true), (false)) AS t(flag) ORDER BY v",
    ),
    (
        "implicit_coalesce",
        "SELECT coalesce(CAST(NULL AS INT), CAST(4294967297 AS BIGINT)) AS v",
    ),
    (
        "implicit_cmp_mixed",
        "SELECT x FROM (VALUES (CAST(1 AS INT))) AS t(x) WHERE x = CAST(4294967297 AS BIGINT)",
    ),
    (
        "subquery_outer_wrap",
        "SELECT r FROM (SELECT CAST(b AS INT) AS r "
        "FROM (VALUES (CAST(4294967297 AS BIGINT))) AS t(b)) AS u",
    ),
    (
        "subquery_outer_null",
        "SELECT r FROM (SELECT CAST(d AS DECIMAL(3,2)) AS r FROM "
        "(VALUES (CAST(10.00 AS DECIMAL(10,2))), (CAST(1.25 AS DECIMAL(10,2)))) AS t(d)) "
        "AS u WHERE r IS NULL",
    ),
    (
        "group_by_cast",
        "SELECT CAST(b AS INT) AS k, count(*) AS cnt FROM "
        "(VALUES (CAST(1 AS BIGINT)), (CAST(4294967297 AS BIGINT))) AS t(b) "
        "GROUP BY CAST(b AS INT)",
    ),
    (
        "join_on_cast",
        "SELECT l.id, r.b FROM (VALUES (CAST(1 AS INT))) AS l(id) "
        "JOIN (VALUES (CAST(4294967297 AS BIGINT))) AS r(b) ON l.id = CAST(r.b AS INT)",
    ),
    (
        "union_cast",
        "SELECT CAST(b AS INT) AS k FROM (VALUES (CAST(4294967297 AS BIGINT))) AS t(b) "
        "UNION SELECT CAST(1 AS INT) AS k",
    ),
    (
        "intersect_cast",
        "SELECT CAST(b AS INT) AS k FROM (VALUES (CAST(4294967297 AS BIGINT))) AS t(b) "
        "INTERSECT SELECT CAST(1 AS INT) AS k",
    ),
)

WRITE_CASES: tuple[str, ...] = (
    "ctas",
    "insert_select",
    "insert_overwrite",
    "update_set",
    "merge_set",
    "writeto_append",
)


def sql_literal(raw: str | None, source_type: str) -> str:
    if raw is None:
        return f"CAST(NULL AS {source_type})"
    if source_type in ("FLOAT", "DOUBLE") or source_type.startswith("DECIMAL"):
        return f"CAST('{raw}' AS {source_type})"
    return f"CAST({raw} AS {source_type})"


def norm_val(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return value
    return value


def extract_error(error: BaseException) -> dict[str, Any]:
    text = str(error)
    first = text.splitlines()[0]
    condition_match = _CONDITION.search(text)
    state_match = _STATE.search(text)
    return {
        "error": condition_match.group(1) if condition_match else f"NONE:{type(error).__name__}",
        "sqlstate": state_match.group(1) if state_match else None,
        "message": first,
    }


def collect_scalar(frame: Any) -> dict[str, Any]:
    field = frame.schema.fields[0]
    rows = frame.collect()
    return {
        "value": norm_val(rows[0][0]),
        "type": field.dataType.simpleString(),
        "nullable": bool(field.nullable),
    }


def collect_table(frame: Any) -> dict[str, Any]:
    rows = [[norm_val(cell) for cell in row] for row in frame.collect()]
    types = [field.dataType.simpleString() for field in frame.schema.fields]
    return {"rows": rows, "types": types}


def scalar_key(kind: str, seat: str, raw: str | None) -> str:
    return f"{kind}|{seat}|{'NULL' if raw is None else raw}"


def run_scalar(
    session: Any,
    functions: Any,
    source_type: str,
    target_type: str,
    seat: str,
    raw: str | None,
) -> dict[str, Any]:
    lit_sql = sql_literal(raw, source_type)
    try:
        if seat == "sql-literal":
            frame = session.sql(f"SELECT CAST({lit_sql} AS {target_type}) AS r")
        elif seat == "sql-column":
            frame = session.sql(
                f"SELECT CAST(x AS {target_type}) AS r FROM (SELECT {lit_sql} AS x) AS t"
            )
        elif seat == "dataframe":
            frame = session.sql(f"SELECT {lit_sql} AS x").select(
                functions.col("x").cast(target_type).alias("r")
            )
        elif seat == "expr":
            frame = session.sql(f"SELECT {lit_sql} AS x").select(
                functions.expr(f"CAST(x AS {target_type})").alias("r")
            )
        elif seat == "selectexpr":
            frame = session.sql(f"SELECT {lit_sql} AS x").selectExpr(
                f"CAST(x AS {target_type}) AS r"
            )
        elif seat == "view":
            session.sql(
                f"CREATE OR REPLACE TEMP VIEW _narrow_v AS "
                f"SELECT CAST(x AS {target_type}) AS r FROM (SELECT {lit_sql} AS x) AS t"
            ).collect()
            frame = session.sql("SELECT r FROM _narrow_v")
        elif seat == "try-cast":
            frame = session.sql(
                f"SELECT TRY_CAST(x AS {target_type}) AS r FROM (SELECT {lit_sql} AS x) AS t"
            )
        else:
            raise ValueError(f"unknown seat {seat}")
        return collect_scalar(frame)
    except Exception as error:
        return extract_error(error)


def write_barrier_parquets(root: Path) -> dict[str, Path]:
    root.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}

    tiny_path = root / "tiny_pair.parquet"
    tiny_schema = pa.schema([pa.field("x", pa.int64())])
    with pq.ParquetWriter(tiny_path, tiny_schema) as writer:
        writer.write_table(pa.table({"x": pa.array([1], type=pa.int64())}))
        writer.write_table(pa.table({"x": pa.array([300], type=pa.int64())}))
    paths["tiny_pair"] = tiny_path

    int_path = root / "int_rg.parquet"
    int_schema = pa.schema([pa.field("id", pa.int32()), pa.field("b", pa.int64())])
    with pq.ParquetWriter(int_path, int_schema) as writer:
        writer.write_table(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int32()),
                    "b": pa.array([1, 2], type=pa.int64()),
                }
            )
        )
        writer.write_table(
            pa.table(
                {
                    "id": pa.array([3, 4], type=pa.int32()),
                    "b": pa.array([300, 4294967297], type=pa.int64()),
                }
            )
        )
    paths["int_rg"] = int_path

    dec_path = root / "dec_rg.parquet"
    dec_type = pa.decimal128(10, 2)
    dec_schema = pa.schema([pa.field("id", pa.int32()), pa.field("d", dec_type)])
    with pq.ParquetWriter(dec_path, dec_schema) as writer:
        writer.write_table(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int32()),
                    "d": pa.array([Decimal("1.00"), Decimal("2.00")], type=dec_type),
                }
            )
        )
        writer.write_table(
            pa.table(
                {
                    "id": pa.array([3, 4], type=pa.int32()),
                    "d": pa.array([Decimal("10.00"), Decimal("128.00")], type=dec_type),
                }
            )
        )
    paths["dec_rg"] = dec_path

    flt_path = root / "flt_rg.parquet"
    flt_schema = pa.schema([pa.field("id", pa.int32()), pa.field("f", pa.float32())])
    with pq.ParquetWriter(flt_path, flt_schema) as writer:
        writer.write_table(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int32()),
                    "f": pa.array([1.0, 2.0], type=pa.float32()),
                }
            )
        )
        writer.write_table(
            pa.table(
                {
                    "id": pa.array([3, 4], type=pa.int32()),
                    "f": pa.array([1e10, float("nan")], type=pa.float32()),
                }
            )
        )
    paths["flt_rg"] = flt_path

    return paths


def run_card_comparisons(session: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name, src_type, _dst_type, vals, preds in CARD_COMPARISON_SPECS:
        rows_sql = ", ".join(
            f"({index}, {sql_literal(val, src_type)})" for index, val in enumerate(vals, start=1)
        )
        session.sql(
            f"CREATE OR REPLACE TEMP VIEW _cmp_{name} AS "
            f"SELECT * FROM VALUES {rows_sql} AS t(id, c)"
        ).collect()
        for pred in preds:
            key = f"{name}|{pred}"
            try:
                frame = session.sql(f"SELECT id FROM _cmp_{name} WHERE {pred} ORDER BY id")
                out[key] = collect_table(frame)
            except Exception as error:
                out[key] = extract_error(error)
    return out


def run_parquet_barrier(session: Any, parquet_paths: dict[str, Path]) -> dict[str, Any]:
    for name, path in parquet_paths.items():
        session.read.parquet(str(path)).createOrReplaceTempView(name)
    out: dict[str, Any] = {}
    for key, _table, sql in PARQUET_BARRIER_QUERIES:
        try:
            out[key] = collect_table(session.sql(sql))
        except Exception as error:
            out[key] = extract_error(error)
    return out


def run_reach_queries(session: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, sql in REACH_QUERIES:
        try:
            out[key] = collect_table(session.sql(sql))
        except Exception as error:
            out[key] = extract_error(error)
    return out


def run_write_case(
    session: Any,
    functions: Any,
    catalog: str,
    case: str,
    ansi: str,
    cold_read: Any,
) -> dict[str, Any]:
    table = f"{catalog}.ns.w_{case}_{ansi}"
    session.sql(f"DROP TABLE IF EXISTS {table}").collect()
    src_select = (
        "SELECT 1 AS id, "
        "CAST(CAST(4294967297 AS BIGINT) AS INT) AS bi, "
        "CAST(CAST(1e10 AS FLOAT) AS TINYINT) AS ft, "
        "CAST(CAST(10.00 AS DECIMAL(10,2)) AS DECIMAL(3,2)) AS dd"
    )
    try:
        if case == "ctas":
            session.sql(f"CREATE TABLE {table} USING iceberg AS {src_select}").collect()
        elif case == "insert_select":
            session.sql(
                f"CREATE TABLE {table} (id INT, bi INT, ft TINYINT, dd DECIMAL(3,2)) USING iceberg"
            ).collect()
            session.sql(f"INSERT INTO {table} {src_select}").collect()
        elif case == "insert_overwrite":
            session.sql(
                f"CREATE TABLE {table} (id INT, bi INT, ft TINYINT, dd DECIMAL(3,2)) USING iceberg"
            ).collect()
            session.sql(
                f"INSERT INTO {table} VALUES (9, 9, CAST(9 AS TINYINT), CAST(1.00 AS DECIMAL(3,2)))"
            ).collect()
            session.sql(f"INSERT OVERWRITE {table} {src_select}").collect()
        elif case == "update_set":
            session.sql(
                f"CREATE TABLE {table} (id INT, bi INT, ft TINYINT, dd DECIMAL(3,2)) USING iceberg"
            ).collect()
            session.sql(
                f"INSERT INTO {table} VALUES (1, 0, CAST(0 AS TINYINT), CAST(1.00 AS DECIMAL(3,2)))"
            ).collect()
            session.sql(
                f"UPDATE {table} SET "
                "bi = CAST(CAST(4294967297 AS BIGINT) AS INT), "
                "ft = CAST(CAST(1e10 AS FLOAT) AS TINYINT), "
                "dd = CAST(CAST(10.00 AS DECIMAL(10,2)) AS DECIMAL(3,2)) "
                "WHERE id = 1"
            ).collect()
        elif case == "merge_set":
            session.sql(
                f"CREATE TABLE {table} (id INT, bi INT, ft TINYINT, dd DECIMAL(3,2)) USING iceberg"
            ).collect()
            session.sql(
                f"INSERT INTO {table} VALUES (1, 0, CAST(0 AS TINYINT), CAST(1.00 AS DECIMAL(3,2)))"
            ).collect()
            session.sql(
                f"MERGE INTO {table} t USING "
                "(SELECT 1 AS id, CAST(4294967297 AS BIGINT) AS b, CAST(1e10 AS FLOAT) AS f, "
                "CAST(10.00 AS DECIMAL(10,2)) AS d) s ON t.id = s.id "
                "WHEN MATCHED THEN UPDATE SET "
                "t.bi = CAST(s.b AS INT), "
                "t.ft = CAST(s.f AS TINYINT), "
                "t.dd = CAST(s.d AS DECIMAL(3,2))"
            ).collect()
        elif case == "writeto_append":
            session.sql(
                f"CREATE TABLE {table} (id INT, bi INT, ft TINYINT, dd DECIMAL(3,2)) USING iceberg"
            ).collect()
            frame = session.sql(
                "SELECT 1 AS id, CAST(4294967297 AS BIGINT) AS b, CAST(1e10 AS FLOAT) AS f, "
                "CAST(10.00 AS DECIMAL(10,2)) AS d"
            ).select(
                functions.col("id"),
                functions.col("b").cast("int").alias("bi"),
                functions.col("f").cast("tinyint").alias("ft"),
                functions.col("d").cast("decimal(3,2)").alias("dd"),
            )
            frame.writeTo(table).append()
        else:
            raise ValueError(f"unknown write case {case}")
        return cold_read(table)
    except Exception as error:
        return extract_error(error)


def _open_spark(warehouse: Path, runtime_jar: Path) -> Any:
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("record-ansi-off-narrowing-overflow-1")
        .config("spark.jars", str(runtime_jar))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(warehouse / "spark-warehouse"))
        .config("spark.sql.catalog.narrow", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.narrow.type", "hadoop")
        .config("spark.sql.catalog.narrow.warehouse", str(warehouse / "narrow"))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def record(scratch: Path, runtime_jar: Path) -> dict[str, Any]:
    from pyspark.sql import functions

    if scratch.exists():
        shutil.rmtree(scratch)
    scratch.mkdir(parents=True)
    parquet_paths = write_barrier_parquets(scratch / "pq")
    warehouse = scratch / "wh"
    spark = _open_spark(warehouse, runtime_jar)
    try:
        version = spark.version
        spark.sql("CREATE NAMESPACE IF NOT EXISTS narrow.ns").collect()
        modes: dict[str, Any] = {}
        for ansi in ANSI_MODES:
            spark.conf.set("spark.sql.ansi.enabled", ansi)
            scalars: dict[str, Any] = {}
            for kind, src_type, dst_type, vals in SCALAR_SPECS:
                for seat in SEATS:
                    for raw in vals:
                        scalars[scalar_key(kind, seat, raw)] = run_scalar(
                            spark, functions, src_type, dst_type, seat, raw
                        )
            comparisons = run_card_comparisons(spark)
            barrier = run_parquet_barrier(spark, parquet_paths)
            reach = run_reach_queries(spark)
            writes: dict[str, Any] = {}
            for case in WRITE_CASES:
                writes[case] = run_write_case(
                    spark,
                    functions,
                    "narrow",
                    case,
                    ansi,
                    lambda tbl: collect_table(
                        spark.sql(f"SELECT id, bi, ft, dd FROM {tbl} ORDER BY id")
                    ),
                )
            modes[ansi] = {
                "scalars": scalars,
                "card_comparisons": comparisons,
                "parquet_barrier": barrier,
                "reach": reach,
                "writes": writes,
            }
        return {"spark": version, "modes": modes}
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scratch", type=Path, default=None)
    parser.add_argument("--runtime-jar", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=FIXTURE)
    parsed = parser.parse_args(argv)
    scratch = parsed.scratch or Path(tempfile.mkdtemp(prefix="record-narrow-"))
    payload = record(scratch, parsed.runtime_jar)
    parsed.out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"WROTE {parsed.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
