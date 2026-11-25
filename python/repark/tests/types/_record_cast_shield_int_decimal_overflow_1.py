from __future__ import annotations

import argparse
import json
import os
import re
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

ROWS: list[tuple[Any, ...]] = [
    (1, 1, 1, 1, 1, "1.00", "1", 1.0),
    (2, 99, 9999, 99999, 123456, "12345.67", "123456", 123456.0),
    (
        3,
        -128,
        -32768,
        -2147483648,
        -9223372036854775808,
        "-99999999.99",
        "-123456",
        -123456.0,
    ),
    (4, 127, 32767, 2147483647, 9223372036854775807, "99999999.99", "abc", 1e30),
    (5, None, None, None, None, None, None, None),
    (6, -12, -1234, -123456, -123456, "-1.50", "-1.5", -1.5),
    (7, 12, 123, 12345, 12345, "123.45", "12345", 12345.0),
]
FIT_IDS = (1, 5, 7)
COLUMNS = ("id", "ti", "si", "ii", "bi", "dd", "ss", "db")
SOURCE_TYPES = {"ti": "TINYINT", "si": "SMALLINT", "ii": "INT", "bi": "BIGINT"}
TARGETS: dict[str, list[tuple[int, int]]] = {
    "ti": [(2, 0), (3, 0), (3, 1), (4, 1)],
    "si": [(4, 0), (5, 0), (5, 1), (6, 1)],
    "ii": [(9, 0), (10, 0), (10, 1), (11, 1)],
    "bi": [(5, 0), (18, 0), (19, 0), (20, 2), (21, 2)],
}
FABLE_ROWS: list[tuple[Any, ...]] = [
    (1, 1, True, 1),
    (2, 123456, None, None),
    (3, -200000, None, None),
    (4, 999999, None, None),
    (5, None, True, 1),
    (6, 123456, None, None),
    (7, 12345, True, 1),
    (8, 1, False, 1),
]
FABLE_CELLS: dict[str, str] = json.loads(
    (
        Path(__file__).resolve().parent / "cast_shield_int_decimal_overflow_1_fable_cells.json"
    ).read_text(encoding="utf-8")
)
CAST = "CAST(bi AS DECIMAL(5,0))"
PLACEMENTS: dict[str, tuple[str, bool]] = {
    "where_eq": (f"SELECT id FROM {{t}} WHERE {CAST} = 1 ORDER BY id", True),
    "where_lt": (f"SELECT id FROM {{t}} WHERE {CAST} < 1 ORDER BY id", True),
    "where_gt": (f"SELECT id FROM {{t}} WHERE {CAST} > 1 ORDER BY id", True),
    "where_ne": (f"SELECT id FROM {{t}} WHERE {CAST} <> 1 ORDER BY id", True),
    "where_in": (f"SELECT id FROM {{t}} WHERE {CAST} IN (1, 12345) ORDER BY id", True),
    "where_not_in": (f"SELECT id FROM {{t}} WHERE {CAST} NOT IN (1, 12345) ORDER BY id", True),
    "where_between": (
        f"SELECT id FROM {{t}} WHERE {CAST} BETWEEN 0 AND 99999 ORDER BY id",
        True,
    ),
    "where_is_null": (f"SELECT id FROM {{t}} WHERE {CAST} IS NULL ORDER BY id", True),
    "where_is_not_null": (f"SELECT id FROM {{t}} WHERE {CAST} IS NOT NULL ORDER BY id", True),
    "where_case": (
        f"SELECT id FROM {{t}} WHERE CASE WHEN {CAST} = 1 THEN true ELSE false END ORDER BY id",
        True,
    ),
    "select_case": (
        f"SELECT id, CASE WHEN {CAST} > 0 THEN 'p' ELSE 'n' END AS r FROM {{t}} ORDER BY id",
        True,
    ),
    "join_key": (
        "SELECT a.id FROM {t} a JOIN keys k ON CAST(a.bi AS DECIMAL(5,0)) = k.d ORDER BY a.id",
        True,
    ),
    "group_by": (f"SELECT {CAST} AS g, count(*) AS c FROM {{t}} GROUP BY 1", False),
    "order_by": (f"SELECT id FROM {{t}} ORDER BY {CAST} NULLS FIRST, id", True),
    "sum": (f"SELECT sum({CAST}) AS s FROM {{t}}", False),
    "avg": (f"SELECT avg({CAST}) AS a FROM {{t}}", False),
    "projection": (f"SELECT id, {CAST} AS r FROM {{t}} ORDER BY id", True),
    "having": (
        f"SELECT id FROM {{t}} GROUP BY id, bi HAVING {CAST} = 1 ORDER BY id",
        True,
    ),
    "having_sum": (
        f"SELECT id FROM {{t}} GROUP BY id HAVING sum({CAST}) > 0 ORDER BY id",
        True,
    ),
}
DOOR_FILTERS: dict[str, tuple[str, str]] = {
    "eq": (f"{CAST} = 1", "eq"),
    "lt": (f"{CAST} < 1", "lt"),
    "is_null": (f"{CAST} IS NULL", "is_null"),
}
NEIGHBOURS: dict[str, str] = {
    "dec_to_dec": "CAST(dd AS DECIMAL(5,1))",
    "str_to_dec": "CAST(ss AS DECIMAL(5,0))",
    "dbl_to_dec": "CAST(db AS DECIMAL(5,0))",
    "big_to_int": "CAST(bi AS INT)",
    "int_to_small": "CAST(ii AS SMALLINT)",
    "try_dec_to_dec": "TRY_CAST(dd AS DECIMAL(5,1))",
    "try_str_to_dec": "TRY_CAST(ss AS DECIMAL(5,0))",
    "try_dbl_to_dec": "TRY_CAST(db AS DECIMAL(5,0))",
    "try_big_to_int": "TRY_CAST(bi AS INT)",
    "try_int_to_small": "TRY_CAST(ii AS SMALLINT)",
    "try_big_to_dec": "TRY_CAST(bi AS DECIMAL(5,0))",
    "try_tiny_to_dec": "TRY_CAST(ti AS DECIMAL(3,1))",
}
PRUNE_STATEMENTS: dict[str, str] = {
    "cast_eq": f"SELECT id FROM {{t}} WHERE {CAST} = 1 ORDER BY id",
    "cast_lt": f"SELECT id FROM {{t}} WHERE {CAST} < 1 ORDER BY id",
    "cast_in": f"SELECT id FROM {{t}} WHERE {CAST} IN (1, 2) ORDER BY id",
    "lossless_eq": "SELECT id FROM {t} WHERE CAST(bi AS DECIMAL(20,0)) = 1 ORDER BY id",
    "plain_eq": "SELECT id FROM {t} WHERE bi = 1 ORDER BY id",
    "plain_gt": "SELECT id FROM {t} WHERE bi > 100000 ORDER BY id",
    "int_lossless": "SELECT id FROM {t} WHERE CAST(ii AS DECIMAL(10,0)) = 1 ORDER BY id",
    "int_narrow": "SELECT id FROM {t} WHERE CAST(ii AS DECIMAL(5,0)) = 1 ORDER BY id",
    "conj_eq": f"SELECT id FROM {{t}} WHERE id IN (1, 2, 7, 8) AND {CAST} = 1 ORDER BY id",
    "conj_or": f"SELECT id FROM {{t}} WHERE id NOT IN (1, 2, 7, 8) OR {CAST} = 1 ORDER BY id",
    "conj_cast_first": f"SELECT id FROM {{t}} WHERE {CAST} = 1 AND id IN (1, 2, 7, 8) ORDER BY id",
}
PUSHDOWN_KEY = "datafusion.execution.parquet.pushdown_filters"
PRUNE_ROWS = [
    (1, 1, 1),
    (2, 2, 2),
    (3, 123456, 123456),
    (4, 200000, 200000),
    (5, -123456, -123456),
    (6, -200000, -200000),
    (7, 12345, 12345),
    (8, 99999, 99999),
]
WRITE_SELECT = "SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t"
WRITE_SOURCES = {"full": "", "fit": " WHERE id IN (1, 5, 7)", "empty": " WHERE id < 0"}
ERROR_CLASS = re.compile(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)?)\]")
METRIC = re.compile(
    r"(output_rows|row_groups_pruned_statistics|row_groups_matched_statistics|"
    r"files_ranges_pruned_statistics|pushdown_rows_pruned|file_groups)=([^,\]]+)"
)


def norm(value: Any) -> Any:
    if isinstance(value, Decimal):
        return f"Dec({value})"
    if isinstance(value, float):
        return repr(value)
    return value


def error_of(exc: BaseException) -> dict[str, Any]:
    text = str(exc)
    found = ERROR_CLASS.search(text)
    first = text.strip().splitlines()[0][:300] if text.strip() else type(exc).__name__
    return {"kind": "error", "class": found.group(1) if found else "OTHER", "first": first}


def collect(make: Callable[[], Any], ordered: bool) -> dict[str, Any]:
    try:
        frame = make()
        rows = [[norm(cell) for cell in row] for row in frame.collect()]
        types = [dtype for _, dtype in frame.dtypes]
        nullable = [bool(field.nullable) for field in frame.schema.fields]
    except Exception as exc:
        return error_of(exc)
    if not ordered:
        rows = sorted(rows, key=repr)
    return {"kind": "ok", "rows": rows, "types": types, "nullable": nullable}


def native_collect(session: Any, sql: str, ordered: bool) -> dict[str, Any]:
    try:
        table = pa.table(session.sql(sql))
        rows = [[norm(cell) for cell in row.values()] for row in table.to_pylist()]
        types = [str(field.type) for field in table.schema]
    except Exception as exc:
        return error_of(exc)
    if not ordered:
        rows = sorted(rows, key=repr)
    return {"kind": "ok", "rows": rows, "types": types}


def arrow_rows() -> pa.Table:
    columns = list(zip(*ROWS, strict=True))
    return pa.table(
        {
            "id": pa.array(columns[0], pa.int32()),
            "ti": pa.array(columns[1], pa.int8()),
            "si": pa.array(columns[2], pa.int16()),
            "ii": pa.array(columns[3], pa.int32()),
            "bi": pa.array(columns[4], pa.int64()),
            "dd": pa.array(
                [None if v is None else Decimal(v) for v in columns[5]], pa.decimal128(10, 2)
            ),
            "ss": pa.array(columns[6], pa.string()),
            "db": pa.array(columns[7], pa.float64()),
        }
    )


def write_parquet(root: Path) -> Path:
    path = root / "prune.parquet"
    if not path.exists():
        root.mkdir(parents=True, exist_ok=True)
        ids, bis, iis = zip(*PRUNE_ROWS, strict=True)
        table = pa.table(
            {
                "id": pa.array(ids, pa.int32()),
                "bi": pa.array(bis, pa.int64()),
                "ii": pa.array(iis, pa.int32()),
            }
        )
        pq.write_table(table, str(path), row_group_size=2)
    return path


def spark_session(warehouse: Path, source: Path, ansi: str) -> Any:
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName(f"castshield-{ansi}")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", str(warehouse))
        .config("spark.sql.catalog.src", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.src.type", "hadoop")
        .config("spark.sql.catalog.src.warehouse", str(source))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.jars", os.environ["SPARK_ICEBERG_JAR"])
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    return session


def repark_session(warehouse: Path, source: Path, ansi: str) -> Any:
    from repark import ReparkSession
    from repark.spark.session import _reset_active_session_for_tests

    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName(f"castshield-{ansi}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", str(warehouse))
        .config("spark.sql.catalog.src.type", "hadoop")
        .config("spark.sql.catalog.src.warehouse", str(source))
        .getOrCreate()
    )
    session.conf.set("spark.sql.ansi.enabled", ansi)
    return session


def functions_for(engine: str) -> Any:
    if engine == "spark":
        from pyspark.sql import functions
    else:
        from repark.spark import functions
    return functions


def schema_ddl() -> str:
    return (
        "id INT, ti TINYINT, si SMALLINT, ii INT, bi BIGINT, dd DECIMAL(10,2), ss STRING, db DOUBLE"
    )


def make_frames(spark: Any, root: Path) -> None:
    data = [
        tuple(Decimal(v) if i == 5 and v is not None else v for i, v in enumerate(row))
        for row in ROWS
    ]
    frame = spark.createDataFrame(data, schema_ddl())
    frame.createOrReplaceTempView("t")
    rows_path = root / "rows.parquet"
    if not rows_path.exists():
        pq.write_table(arrow_rows(), str(rows_path))
    spark.read.parquet(str(rows_path)).createOrReplaceTempView("tp")
    spark.sql(f"SELECT * FROM t WHERE id IN {FIT_IDS}").createOrReplaceTempView("t_fit")
    spark.sql(
        "SELECT * FROM VALUES (CAST(1 AS DECIMAL(5,0))), (CAST(12345 AS DECIMAL(5,0))) AS k(d)"
    ).createOrReplaceTempView("keys")


def literal_text(value: Any, sql_type: str) -> str:
    if value is None:
        return f"CAST(NULL AS {sql_type})"
    return f"CAST({value} AS {sql_type})"


def kernel_cells(spark: Any, cells: dict[str, Any]) -> None:
    for column, targets in TARGETS.items():
        index = COLUMNS.index(column)
        sql_type = SOURCE_TYPES[column]
        for precision, scale in targets:
            target = f"DECIMAL({precision},{scale})"
            for row in ROWS:
                value = row[index]
                tag = f"{column}->{precision},{scale}/id{row[0]}"
                cells[f"kernel/lit/{tag}"] = collect(
                    lambda v=value, s=sql_type, g=target: spark.sql(
                        f"SELECT CAST({literal_text(v, s)} AS {g}) AS r"
                    ),
                    True,
                )
                cells[f"kernel/col/{tag}"] = collect(
                    lambda c=column, g=target, i=row[0]: spark.sql(
                        f"SELECT CAST({c} AS {g}) AS r FROM t WHERE id = {i}"
                    ),
                    True,
                )


def placement_cells(spark: Any, cells: dict[str, Any]) -> None:
    for view in ("t", "t_fit"):
        for name, (template, ordered) in PLACEMENTS.items():
            query = template.format(t=view)
            cells[f"place/{view}/{name}"] = collect(lambda q=query: spark.sql(q), ordered)
    cells["card/filter"] = collect(
        lambda: spark.sql(
            "SELECT b FROM (SELECT CAST(123456 AS BIGINT) AS b) WHERE CAST(b AS DECIMAL(5,0)) = 1"
        ),
        True,
    )
    cells["card/projection"] = collect(
        lambda: spark.sql(
            "SELECT CAST(b AS DECIMAL(5,0)) FROM (SELECT CAST(123456 AS BIGINT) AS b)"
        ),
        True,
    )
    cells["place/t/tiny_scale_eq"] = collect(
        lambda: spark.sql("SELECT id FROM t WHERE CAST(ti AS DECIMAL(3,1)) = 1 ORDER BY id"),
        True,
    )
    cells["place/t/int_digit_lt"] = collect(
        lambda: spark.sql("SELECT id FROM t WHERE CAST(ii AS DECIMAL(9,0)) < 0 ORDER BY id"),
        True,
    )
    cells["place/t/small_digit_in"] = collect(
        lambda: spark.sql(
            "SELECT id FROM t WHERE CAST(si AS DECIMAL(4,0)) IN (1, 123) ORDER BY id"
        ),
        True,
    )


def door_cells(spark: Any, engine: str, cells: dict[str, Any]) -> None:
    functions = functions_for(engine)
    for view in ("t", "t_fit"):
        frame = spark.table(view)
        for name, (predicate, _) in DOOR_FILTERS.items():
            base = f"door/{view}/{name}"
            cells[f"{base}/sql"] = collect(
                lambda p=predicate, v=view: spark.sql(f"SELECT id FROM {v} WHERE {p} ORDER BY id"),
                True,
            )
            cells[f"{base}/expr"] = collect(
                lambda p=predicate, f=frame: f.filter(functions.expr(p)).select("id").orderBy("id"),
                True,
            )
            cells[f"{base}/filter_str"] = collect(
                lambda p=predicate, f=frame: f.filter(p).select("id").orderBy("id"), True
            )
            fit = " AND id IN (1, 5, 7)" if view == "t_fit" else ""
            cells[f"{base}/view"] = collect(
                lambda p=predicate, n=name, x=fit: (
                    spark.sql(
                        f"CREATE OR REPLACE TEMP VIEW v_{n} AS SELECT id FROM tp WHERE {p}{x}"
                    ).collect(),
                    spark.sql(f"SELECT id FROM v_{n} ORDER BY id"),
                )[1],
                True,
            )
            cast_column = functions.col("bi").cast("decimal(5,0)")
            if name == "eq":
                condition = cast_column == 1
            elif name == "lt":
                condition = cast_column < 1
            else:
                condition = cast_column.isNull()
            cells[f"{base}/col_cast"] = collect(
                lambda c=condition, f=frame: f.filter(c).select("id").orderBy("id"), True
            )
        base = f"door/{view}/projection"
        cells[f"{base}/sql"] = collect(
            lambda v=view: spark.sql(f"SELECT id, {CAST} AS r FROM {v} ORDER BY id"), True
        )
        cells[f"{base}/expr"] = collect(
            lambda f=frame: f.select("id", functions.expr(CAST).alias("r")).orderBy("id"), True
        )
        cells[f"{base}/select_expr"] = collect(
            lambda f=frame: f.selectExpr("id", f"{CAST} AS r").orderBy("id"), True
        )
        cells[f"{base}/col_cast"] = collect(
            lambda f=frame: f.select(
                "id", functions.col("bi").cast("decimal(5,0)").alias("r")
            ).orderBy("id"),
            True,
        )
        fit = " WHERE id IN (1, 5, 7)" if view == "t_fit" else ""
        cells[f"{base}/view"] = collect(
            lambda x=fit: (
                spark.sql(
                    f"CREATE OR REPLACE TEMP VIEW v_proj AS SELECT id, {CAST} AS r FROM tp{x}"
                ).collect(),
                spark.sql("SELECT * FROM v_proj ORDER BY id"),
            )[1],
            True,
        )


def neighbour_cells(spark: Any, cells: dict[str, Any]) -> None:
    for name, expr in NEIGHBOURS.items():
        for row in ROWS:
            cells[f"neighbour/{name}/id{row[0]}"] = collect(
                lambda e=expr, i=row[0]: spark.sql(f"SELECT {e} AS r FROM t WHERE id = {i}"), True
            )
        cells[f"neighbour/{name}/where_eq"] = collect(
            lambda e=expr: spark.sql(f"SELECT id FROM t WHERE {e} = 1 ORDER BY id"), True
        )
        cells[f"neighbour/{name}/where_lt"] = collect(
            lambda e=expr: spark.sql(f"SELECT id FROM t WHERE {e} < 1 ORDER BY id"), True
        )


CONJUNCTION_CASTS: dict[str, str] = {
    "big_to_dec": "CAST(bi AS DECIMAL(5,0))",
    "tiny_to_dec": "CAST(ti AS DECIMAL(3,1))",
    "big_to_int": "CAST(bi AS INT)",
    "int_to_small": "CAST(ii AS SMALLINT)",
    "dec_to_dec": "CAST(dd AS DECIMAL(5,1))",
    "str_to_dec": "CAST(ss AS DECIMAL(5,0))",
    "dbl_to_dec": "CAST(db AS DECIMAL(5,0))",
}


def conjunction_cells(spark: Any, cells: dict[str, Any]) -> None:
    for name, expr in CONJUNCTION_CASTS.items():
        cells[f"conj/{name}/view_eq"] = collect(
            lambda e=expr: spark.sql(f"SELECT id FROM t_fit WHERE {e} = 1 ORDER BY id"), True
        )
        cells[f"conj/{name}/and_eq"] = collect(
            lambda e=expr: spark.sql(
                f"SELECT id FROM t WHERE id IN (1, 5, 7) AND {e} = 1 ORDER BY id"
            ),
            True,
        )
        cells[f"conj/{name}/and_one_row"] = collect(
            lambda e=expr: spark.sql(f"SELECT id FROM t WHERE id = 1 AND {e} = 1 ORDER BY id"),
            True,
        )
    cells["null/notnull_source"] = collect(
        lambda: spark.sql(
            f"SELECT {CAST.replace('bi', 'nb')} AS r FROM "
            "(SELECT coalesce(bi, 0) AS nb FROM t WHERE id IN (1, 7)) ORDER BY r"
        ),
        True,
    )
    cells["null/lossless_notnull_source"] = collect(
        lambda: spark.sql(
            "SELECT CAST(nb AS DECIMAL(20,0)) AS r FROM "
            "(SELECT coalesce(bi, 0) AS nb FROM t WHERE id IN (1, 7)) ORDER BY r"
        ),
        True,
    )


def explain_text(spark: Any, sql: str, engine: str, analyze: bool) -> str:
    if engine == "spark":
        rows = spark.sql(f"EXPLAIN FORMATTED {sql}").collect()
        return "\n".join(str(row[0]) for row in rows)
    keyword = "EXPLAIN ANALYZE" if analyze else "EXPLAIN"
    rows = spark.sql(f"{keyword} {sql}").collect()
    return "\n".join(f"{row[0]}:\n{row[1]}" for row in rows)


def scan_metrics(text: str) -> list[str]:
    found = []
    for line in text.splitlines():
        if "DataSourceExec" in line or "IcebergScan" in line or "Scan" in line:
            found.append(
                " ".join(f"{name}={value}" for name, value in METRIC.findall(line))
                or line.strip()[:200]
            )
    return found


def prune_cells(spark: Any, engine: str, root: Path, out: dict[str, Any]) -> None:
    spark.read.parquet(str(write_parquet(root / "pq"))).createOrReplaceTempView("pq")
    spark.read.format("iceberg").load(str(root / "src_wh" / "cs" / "part")).createOrReplaceTempView(
        "ice_part"
    )
    for source in ("pq", "ice_part"):
        for name, template in PRUNE_STATEMENTS.items():
            sql = template.format(t=source)
            key = f"prune/{source}/{name}"
            out["cells"][key] = collect(lambda q=sql: spark.sql(q), True)
            record: dict[str, Any] = {}
            for analyze in (False, True):
                label = "analyze" if analyze else "explain"
                try:
                    text = explain_text(spark, sql, engine, analyze)
                except Exception as exc:
                    text = f"ERROR {error_of(exc)['first']}"
                record[label] = text
                record[f"{label}_scan"] = scan_metrics(text)
            out["explain"][key] = record


def pushdown_cells(spark: Any, engine: str, out: dict[str, Any]) -> None:
    accepted: dict[str, Any] = {"kind": "ok", "rows": [["n/a"]]}
    if engine == "repark":
        try:
            spark.conf.set(PUSHDOWN_KEY, "true")
            accepted = {"kind": "ok", "rows": [[str(spark.conf.get(PUSHDOWN_KEY))]]}
        except Exception as exc:
            accepted = error_of(exc)
    out["cells"]["pushdown/accepted"] = accepted
    for name in ("conj_eq", "conj_or", "conj_cast_first", "plain_eq", "lossless_eq"):
        sql = PRUNE_STATEMENTS[name].format(t="pq")
        key = f"pushdown/pq/{name}"
        out["cells"][key] = collect(lambda q=sql: spark.sql(q), True)
        try:
            text = explain_text(spark, sql, engine, True)
        except Exception as exc:
            text = f"ERROR {error_of(exc)['first']}"
        out["explain"][key] = {"analyze": text, "analyze_scan": scan_metrics(text)}
    if engine == "repark" and accepted.get("kind") == "ok":
        spark.conf.set(PUSHDOWN_KEY, "false")


def setup_source(source: Path, ansi: str) -> None:
    spark = spark_session(source / "unused", source, ansi)
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS src.cs")
        spark.sql("DROP TABLE IF EXISTS src.cs.part")
        spark.sql(
            "CREATE TABLE src.cs.part (id INT, bi BIGINT, ii INT) USING iceberg PARTITIONED BY (bi)"
        )
        for chunk in (PRUNE_ROWS[:2], PRUNE_ROWS[2:4], PRUNE_ROWS[4:6], PRUNE_ROWS[6:]):
            values = ", ".join(f"({i}, {b}, {n})" for i, b, n in chunk)
            spark.sql(f"INSERT INTO src.cs.part VALUES {values}")
        files = spark.sql("SELECT file_path, record_count FROM src.cs.part.files").collect()
        print(json.dumps({"files": [[str(r[0]), int(r[1])] for r in files]}))
    finally:
        spark.stop()


def write_cells(spark: Any, out: dict[str, Any]) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.cs")
    writes = out["writes"]
    for label, where in WRITE_SOURCES.items():
        select = WRITE_SELECT + where
        statements = {
            f"insert_into/{label}": [
                f"CREATE TABLE ice.cs.ins_{label} (id INT, d DECIMAL(5,0)) USING iceberg",
                f"INSERT INTO ice.cs.ins_{label} VALUES (100, 7)",
                f"INSERT INTO ice.cs.ins_{label} {select}",
            ],
            f"insert_overwrite/{label}": [
                f"CREATE TABLE ice.cs.ow_{label} (id INT, d DECIMAL(5,0)) USING iceberg",
                f"INSERT INTO ice.cs.ow_{label} VALUES (100, 7)",
                f"INSERT OVERWRITE ice.cs.ow_{label} {select}",
            ],
            f"ctas/{label}": [f"CREATE TABLE ice.cs.ctas_{label} USING iceberg AS {select}"],
            f"update/{label}": [
                f"CREATE TABLE ice.cs.upd_{label} (id INT, bi BIGINT, d DECIMAL(5,0)) "
                "USING iceberg",
                f"INSERT INTO ice.cs.upd_{label} SELECT id, bi, CAST(7 AS DECIMAL(5,0)) FROM t",
                f"UPDATE ice.cs.upd_{label} SET d = CAST(bi AS DECIMAL(5,0))" + where,
            ],
            f"merge/{label}": [
                f"CREATE TABLE ice.cs.mrg_{label} (id INT, bi BIGINT, d DECIMAL(5,0)) "
                "USING iceberg",
                f"INSERT INTO ice.cs.mrg_{label} SELECT id, bi, CAST(7 AS DECIMAL(5,0)) FROM t "
                "WHERE id <= 3",
                f"MERGE INTO ice.cs.mrg_{label} m USING (SELECT * FROM t{where}) s "
                "ON m.id = s.id WHEN MATCHED THEN UPDATE SET d = CAST(s.bi AS DECIMAL(5,0)) "
                "WHEN NOT MATCHED THEN INSERT (id, bi, d) "
                "VALUES (s.id, s.bi, CAST(s.bi AS DECIMAL(5,0)))",
            ],
        }
        for key, steps in statements.items():
            results = []
            for statement in steps:
                try:
                    spark.sql(statement).collect()
                    results.append({"kind": "ok"})
                except Exception as exc:
                    results.append(error_of(exc))
            writes[key] = results


def readback(warehouse: Path, ansi: str, out_path: Path) -> None:
    spark = spark_session(warehouse, warehouse, ansi)
    out: dict[str, Any] = {}
    try:
        for row in spark.sql("SHOW TABLES IN ice.cs").collect():
            name = row["tableName"]
            out[name] = collect(lambda n=name: spark.sql(f"SELECT * FROM ice.cs.{n}"), False)
    except Exception as exc:
        out["_error"] = error_of(exc)
    finally:
        spark.stop()
    out_path.write_text(json.dumps(out, indent=1, sort_keys=True), encoding="utf-8")


def run_native(root: Path) -> dict[str, Any]:
    import repark
    from repark._native import PyReparkSession

    session = PyReparkSession.native()
    session.register_arrow_stream_as_temp_view("t", arrow_rows())
    session.create_or_replace_temp_view("pq", session.read_parquet(str(write_parquet(root / "pq"))))
    cells: dict[str, Any] = {}
    for name, (template, ordered) in PLACEMENTS.items():
        if name == "join_key":
            continue
        cells[f"native/{name}"] = native_collect(session, template.format(t="t"), ordered)
    for name, template in PRUNE_STATEMENTS.items():
        cells[f"native/pq/{name}"] = native_collect(session, template.format(t="pq"), True)
    explains = {}
    for name, template in PRUNE_STATEMENTS.items():
        try:
            table = pa.table(session.sql("EXPLAIN " + template.format(t="pq")))
            explains[name] = "\n".join(f"{r['plan_type']}:\n{r['plan']}" for r in table.to_pylist())
        except Exception as exc:
            explains[name] = f"ERROR {error_of(exc)['first']}"
    return {"native_file": repark._native.__file__, "cells": cells, "explain": explains}


def run(engine: str, ansi: str, root: Path) -> dict[str, Any]:
    warehouse = root / f"wh-{engine}-{ansi}"
    source = root / "src_wh"
    if engine == "spark":
        spark = spark_session(warehouse, source, ansi)
    else:
        spark = repark_session(warehouse, source, ansi)
    out: dict[str, Any] = {"engine": engine, "ansi": ansi, "cells": {}, "explain": {}, "writes": {}}
    if engine == "repark":
        import repark

        out["native_file"] = repark._native.__file__
        print(f"repark._native.__file__ = {repark._native.__file__}", flush=True)
    try:
        make_frames(spark, root)
        kernel_cells(spark, out["cells"])
        placement_cells(spark, out["cells"])
        door_cells(spark, engine, out["cells"])
        neighbour_cells(spark, out["cells"])
        conjunction_cells(spark, out["cells"])
        prune_cells(spark, engine, root, out)
        pushdown_cells(spark, engine, out)
        write_cells(spark, out)
    finally:
        spark.stop()
    return out


WRITE_TABLES = {
    "insert_into": "ins",
    "insert_overwrite": "ow",
    "ctas": "ctas",
    "update": "upd",
    "merge": "mrg",
}


def flatten_writes(writes: dict[str, Any], readback: dict[str, Any]) -> dict[str, Any]:
    out = {}
    for key, steps in writes.items():
        last = steps[-1]
        kind, label = key.split("/")
        state = readback.get(f"{WRITE_TABLES[kind]}_{label}")
        if last["kind"] == "error":
            out[key] = last
        else:
            out[key] = state or {"kind": "ok", "rows": ["<no readback>"]}
        if state is not None:
            out[key + "#readback"] = state
    return out


def same_answer(spark: dict[str, Any] | None, other: dict[str, Any] | None) -> bool:
    if spark is None or other is None:
        return False
    if spark["kind"] == "ok" and other["kind"] == "ok":
        return spark["rows"] == other["rows"]
    if spark["kind"] == "error" and other["kind"] == "error":
        return spark["class"] == other["class"]
    return False


def fable_answer(raw: dict[str, Any]) -> dict[str, Any]:
    if "err" in raw:
        found = ERROR_CLASS.search(raw["err"])
        return {"kind": "error", "class": found.group(1) if found else "OTHER"}
    return {"kind": "ok", "rows": raw["ok"]}


def fable_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    if text.startswith("Dec(") and text.endswith(")"):
        return text[4:-1]
    return text


def fable_result(cell: dict[str, Any]) -> dict[str, Any]:
    if cell["kind"] == "error":
        return {"kind": "error", "class": cell["class"]}
    rows = [[fable_text(value) for value in row] for row in cell["rows"]]
    return {"kind": "ok", "rows": sorted(rows, key=repr)}


def fable_cells(spark: Any, root: Path) -> dict[str, Any]:
    path = root / "fable.parquet"
    if not path.exists():
        ids, bis, nbs, gs = zip(*FABLE_ROWS, strict=True)
        table = pa.table(
            {
                "id": pa.array(ids, pa.int32()),
                "bi": pa.array(bis, pa.int64()),
                "nb": pa.array(nbs, pa.bool_()),
                "g": pa.array(gs, pa.int32()),
            }
        )
        pq.write_table(table, str(path), row_group_size=2)
    spark.createDataFrame(
        FABLE_ROWS, "id INT, bi BIGINT, nb BOOLEAN, g INT"
    ).createOrReplaceTempView("t")
    spark.read.parquet(str(path)).createOrReplaceTempView("tp")
    keys = [(i, 1 if i in (1, 5, 7, 8) else 0) for i in range(1, 9)]
    spark.createDataFrame(keys, "id INT, k INT").createOrReplaceTempView("u")
    return {key: collect(lambda q=sql: spark.sql(q), False) for key, sql in FABLE_CELLS.items()}


def repark_source(spark: Any) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS src.cs").collect()
    spark.sql(
        "CREATE TABLE src.cs.part (id INT, bi BIGINT, ii INT) USING iceberg PARTITIONED BY (bi)"
    ).collect()
    for chunk in (PRUNE_ROWS[:2], PRUNE_ROWS[2:4], PRUNE_ROWS[4:6], PRUNE_ROWS[6:]):
        values = ", ".join(f"({i}, {b}, {n})" for i, b, n in chunk)
        spark.sql(f"INSERT INTO src.cs.part VALUES {values}").collect()


def build_fixture(step0: Path, out: Path) -> None:
    payload: dict[str, Any] = {"modes": {}, "fable": {}}
    for ansi in ("true", "false"):
        run_payload = json.loads((step0 / f"spark-{ansi}.json").read_text(encoding="utf-8"))
        readback_payload = json.loads(
            (step0 / f"readback-spark-{ansi}.json").read_text(encoding="utf-8")
        )
        payload["modes"][ansi] = {
            "cells": run_payload["cells"],
            "writes": run_payload["writes"],
            "readback": readback_payload,
        }
    for name in ("fable-spark1", "fable-spark2"):
        raw = json.loads((step0 / f"{name}.json").read_text(encoding="utf-8"))
        for key, value in raw.items():
            if not key.startswith("explain"):
                payload["fable"][key] = value
    out.write_text(json.dumps(payload, indent=1, sort_keys=True), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "action", choices=["setup", "run", "native", "readback", "fable", "fixture"]
    )
    parser.add_argument("--engine", choices=["spark", "repark"], default="repark")
    parser.add_argument("--ansi", choices=["true", "false"], default="true")
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--warehouse", type=Path)
    args = parser.parse_args()
    args.root.mkdir(parents=True, exist_ok=True)
    if args.action == "setup":
        setup_source(args.root / "src_wh", args.ansi)
        return
    if args.action == "readback":
        readback(args.warehouse, args.ansi, args.out)
        return
    if args.action == "fixture":
        build_fixture(args.root, args.out)
        return
    if args.action == "fable":
        session = spark_session(args.root / "fable-wh", args.root / "fable-wh", "true")
        try:
            result = fable_cells(session, args.root)
        finally:
            session.stop()
        args.out.write_text(json.dumps(result, indent=1, sort_keys=True), encoding="utf-8")
        return
    payload = (
        run_native(args.root) if args.action == "native" else run(args.engine, args.ansi, args.root)
    )
    args.out.write_text(json.dumps(payload, indent=1, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":
    main()
