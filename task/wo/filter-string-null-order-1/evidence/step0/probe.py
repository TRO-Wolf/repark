"""FILTER-STRING-NULL-ORDER-1 step 0: NULL placement of an ORDER BY inside SQL text a frame plans.

Usage: probe.py <spark|repark> <out.jsonl>

Every predicate cell runs on the string ``filter`` / ``where`` doors of Spark-door frames, on the
controls (``filter(F.expr)``, ``spark.sql`` WHERE, ``selectExpr``) and, on RePark, on the native
door's frames and ``repark.sql()`` WHERE. The control cells hold the sort methods, the typed
``Window`` and the window forms of each door. Each line is one cell on one door.
"""

from __future__ import annotations

import json
import math
import os
import sys
from pathlib import Path
from typing import Any

ROWS = (
    "(1, 10, 'b', CAST(1.5 AS DOUBLE)), "
    "(2, CAST(NULL AS INT), 'e', CAST(2.5 AS DOUBLE)), "
    "(3, 30, CAST(NULL AS STRING), CAST(3.5 AS DOUBLE)), "
    "(4, 20, 'a', CAST(NULL AS DOUBLE)), "
    "(5, 40, 'd', CAST(0.5 AS DOUBLE))"
)
COLUMNS = "t(id, ki, ks, kd)"
SOURCE = f"SELECT * FROM VALUES {ROWS} AS {COLUMNS}"
NATIVE_SOURCE = f"WITH w AS (SELECT * FROM (VALUES {ROWS}) AS {COLUMNS}) "
PY_ROWS = [
    (1, 10, "b", 1.5),
    (2, None, "e", 2.5),
    (3, 30, None, 3.5),
    (4, 20, "a", None),
    (5, 40, "d", 0.5),
]
SCHEMA = "id INT, ki INT, ks STRING, kd DOUBLE"

NULL_INT = "CAST(NULL AS INT)"
NULL_STR = "CAST(NULL AS STRING)"
NULL_DBL = "CAST(NULL AS DOUBLE)"

PREDICATES = [
    (
        "int_desc_eq",
        f"ki = (SELECT a FROM VALUES ({NULL_INT}), (10), (20) AS q(a) ORDER BY a DESC LIMIT 1)",
    ),
    (
        "int_asc_is_null",
        f"(SELECT a FROM VALUES ({NULL_INT}), (10) AS q(a) ORDER BY a LIMIT 1) IS NULL",
    ),
    (
        "int_asc_kw_is_null",
        f"(SELECT a FROM VALUES ({NULL_INT}), (10) AS q(a) ORDER BY a ASC LIMIT 1) IS NULL",
    ),
    (
        "int_desc_gt",
        f"ki > (SELECT a FROM VALUES ({NULL_INT}), (15) AS q(a) ORDER BY a DESC LIMIT 1)",
    ),
    (
        "int_desc_nulls_first",
        f"ki = (SELECT a FROM VALUES ({NULL_INT}), (10), (20)"
        " AS q(a) ORDER BY a DESC NULLS FIRST LIMIT 1)",
    ),
    (
        "int_asc_nulls_last",
        f"ki = (SELECT a FROM VALUES ({NULL_INT}), (10), (20)"
        " AS q(a) ORDER BY a ASC NULLS LAST LIMIT 1)",
    ),
    (
        "int_desc_nulls_last",
        f"ki = (SELECT a FROM VALUES ({NULL_INT}), (10), (20)"
        " AS q(a) ORDER BY a DESC NULLS LAST LIMIT 1)",
    ),
    (
        "int_asc_nulls_first",
        f"(SELECT a FROM VALUES ({NULL_INT}), (10) AS q(a) ORDER BY a NULLS FIRST LIMIT 1) IS NULL",
    ),
    (
        "int_in_asc",
        f"ki IN (SELECT a FROM VALUES ({NULL_INT}), (10), (20) AS q(a) ORDER BY a LIMIT 2)",
    ),
    (
        "int_in_desc",
        f"ki IN (SELECT a FROM VALUES ({NULL_INT}), (10), (20) AS q(a) ORDER BY a DESC LIMIT 2)",
    ),
    (
        "int_not_in_asc",
        f"ki NOT IN (SELECT a FROM VALUES ({NULL_INT}), (10), (20) AS q(a) ORDER BY a LIMIT 2)",
    ),
    (
        "int_exists",
        f"EXISTS (SELECT 1 FROM (SELECT a FROM VALUES ({NULL_INT}), (1)"
        " AS q(a) ORDER BY a LIMIT 1) z WHERE z.a IS NULL)",
    ),
    (
        "int_not_exists",
        f"NOT EXISTS (SELECT 1 FROM (SELECT a FROM VALUES ({NULL_INT}), (1)"
        " AS q(a) ORDER BY a DESC LIMIT 1) z WHERE z.a IS NULL)",
    ),
    (
        "int_multi_key",
        f"(SELECT b FROM VALUES (1, 'x'), (1, {NULL_STR}), ({NULL_INT}, 'y')"
        " AS q(a, b) ORDER BY a DESC, b LIMIT 1) IS NULL",
    ),
    (
        "int_multi_key_explicit",
        f"(SELECT b FROM VALUES (1, 'x'), (1, {NULL_STR}), ({NULL_INT}, 'y') AS q(a, b) "
        "ORDER BY a DESC NULLS FIRST, b NULLS LAST LIMIT 1) = 'y'",
    ),
    (
        "int_window_in_subquery",
        f"(SELECT max(f) FROM (SELECT first_value(a) OVER (ORDER BY a DESC) AS f"
        f" FROM VALUES (2), ({NULL_INT}) AS q(a))) = 2",
    ),
    (
        "int_row_number_in_subquery",
        f"ki = (SELECT a FROM (SELECT a, row_number() OVER (ORDER BY a) AS rn"
        f" FROM VALUES ({NULL_INT}), (10) AS q(a)) z WHERE rn = 2)",
    ),
    (
        "int_window_explicit",
        f"ki = (SELECT a FROM (SELECT a, row_number() OVER (ORDER BY a NULLS LAST) AS rn"
        f" FROM VALUES ({NULL_INT}), (10) AS q(a)) z WHERE rn = 1)",
    ),
    (
        "int_count_derived",
        f"(SELECT count(a) FROM (SELECT a FROM VALUES (1), ({NULL_INT}), (3)"
        " AS q(a) ORDER BY a DESC LIMIT 2)) = 2",
    ),
    (
        "string_desc_eq",
        f"ks = (SELECT s FROM VALUES ('a'), ('b'), ({NULL_STR}) AS q(s) ORDER BY s DESC LIMIT 1)",
    ),
    (
        "string_asc_is_null",
        f"(SELECT s FROM VALUES ('a'), ({NULL_STR}) AS q(s) ORDER BY s LIMIT 1) IS NULL",
    ),
    (
        "string_in_asc",
        f"ks IN (SELECT s FROM VALUES ({NULL_STR}), ('a'), ('e') AS q(s) ORDER BY s LIMIT 2)",
    ),
    (
        "string_desc_nulls_first",
        f"ks = (SELECT s FROM VALUES ('a'), ('b'), ({NULL_STR})"
        " AS q(s) ORDER BY s DESC NULLS FIRST LIMIT 1)",
    ),
    (
        "double_desc_nan",
        f"isnan((SELECT a FROM VALUES (CAST(1.5 AS DOUBLE)), (CAST('NaN' AS DOUBLE)), ({NULL_DBL})"
        " AS q(a) ORDER BY a DESC LIMIT 1))",
    ),
    (
        "double_desc_eq",
        f"kd = (SELECT a FROM VALUES (CAST(2.5 AS DOUBLE)), ({NULL_DBL})"
        " AS q(a) ORDER BY a DESC LIMIT 1)",
    ),
    (
        "double_asc_is_null",
        f"(SELECT a FROM VALUES (CAST(2.5 AS DOUBLE)), ({NULL_DBL})"
        " AS q(a) ORDER BY a LIMIT 1) IS NULL",
    ),
    (
        "double_in_asc",
        f"kd IN (SELECT a FROM VALUES ({NULL_DBL}), (CAST(0.5 AS DOUBLE)), (CAST(1.5 AS DOUBLE))"
        " AS q(a) ORDER BY a LIMIT 2)",
    ),
    (
        "double_asc_nulls_last",
        f"kd = (SELECT a FROM VALUES ({NULL_DBL}), (CAST(0.5 AS DOUBLE)), (CAST(1.5 AS DOUBLE))"
        " AS q(a) ORDER BY a ASC NULLS LAST LIMIT 1)",
    ),
]

SCALARS = [
    (
        "join_int_desc",
        "ki",
        f"(SELECT a FROM VALUES ({NULL_INT}), (10), (20) AS q(a) ORDER BY a DESC LIMIT 1)",
    ),
    (
        "join_string_desc",
        "ks",
        f"(SELECT s FROM VALUES ('a'), ('b'), ({NULL_STR}) AS q(s) ORDER BY s DESC LIMIT 1)",
    ),
    (
        "join_double_desc",
        "kd",
        f"(SELECT a FROM VALUES (CAST(2.5 AS DOUBLE)), ({NULL_DBL})"
        " AS q(a) ORDER BY a DESC LIMIT 1)",
    ),
]


def plain(value: Any) -> Any:
    """Encode one answer value as JSON keeps it."""
    if isinstance(value, float) and math.isnan(value):
        return "NaN"
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    return value


def rows(frame: Any) -> list[list[Any]]:
    """Collect a frame as plain lists."""
    return [[plain(cell) for cell in row] for row in frame.collect()]


def err(error: Exception) -> str:
    """Render a refusal as one short line."""
    text = str(error).strip().splitlines()[0] if str(error).strip() else ""
    return "ERR:" + type(error).__name__ + ":" + text[:200]


def session(engine: str) -> tuple[Any, Any, Any, Any, dict[str, Any]]:
    """Build the engine's session and return it with its functions, Window and header."""
    if engine == "spark":
        from pyspark.sql import SparkSession, Window, functions

        spark = (
            SparkSession.builder.master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        return spark, functions, Window, None, {"engine": "spark", "version": spark.version}
    import repark
    import repark._native as native
    from repark import ReparkSession, Window
    from repark.spark import functions

    spark = (
        ReparkSession.builder.appName("filter-null-order-1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    header = {
        "engine": "repark",
        "version": repark.__version__,
        "native_module": Path(native.__file__).name,
    }
    return spark, functions, Window, repark.sql, header


def predicate_doors(spark: Any, functions: Any, native_sql: Any) -> dict[str, Any]:
    """Map each door to the call that answers one predicate."""
    w = spark.table("w")
    from_sql = spark.sql(SOURCE)
    created = spark.createDataFrame(PY_ROWS, SCHEMA)
    derived = w.withColumn("z", functions.lit(1)).select("id", "ki", "ks", "kd", "z")
    doors = {
        "filter_str_table": lambda p: rows(w.filter(p).select("id").orderBy("id")),
        "where_str_table": lambda p: rows(w.where(p).select("id").orderBy("id")),
        "filter_str_sql_frame": lambda p: rows(from_sql.filter(p).select("id").orderBy("id")),
        "filter_str_created": lambda p: rows(created.filter(p).select("id").orderBy("id")),
        "filter_str_derived": lambda p: rows(derived.filter(p).select("id").orderBy("id")),
        "filter_str_case_sensitive": lambda p: case_sensitive(spark, p),
        "filter_fexpr": lambda p: rows(w.filter(functions.expr(p)).select("id").orderBy("id")),
        "sparksql_where": lambda p: rows(spark.sql(f"SELECT id FROM w WHERE {p} ORDER BY id")),
        "selectexpr": lambda p: rows(w.selectExpr("id", f"({p}) AS r").orderBy("id")),
    }
    if native_sql is not None:
        doors["native_filter_str"] = lambda p: rows(
            native_sql(f"{NATIVE_SOURCE}SELECT * FROM w").filter(p).select("id").orderBy("id")
        )
        doors["native_sql_where"] = lambda p: rows(
            native_sql(f"{NATIVE_SOURCE}SELECT id FROM w WHERE {p} ORDER BY id")
        )
    return doors


def case_sensitive(spark: Any, predicate: str) -> list[list[Any]]:
    """Answer one predicate on a frame built while ``spark.sql.caseSensitive`` is true."""
    spark.conf.set("spark.sql.caseSensitive", "true")
    try:
        return rows(spark.table("w").filter(predicate).select("id").orderBy("id"))
    finally:
        spark.conf.set("spark.sql.caseSensitive", "false")


def control_cells(
    spark: Any, functions: Any, window: Any, native_sql: Any
) -> list[tuple[str, str, Any]]:
    """Return the controls that must answer as on the base: sorts, typed Window, door windows."""
    w = spark.table("w")
    col = functions.col
    cells = [
        (
            "sort_int_desc",
            "orderBy(col('ki').desc())",
            lambda: rows(w.orderBy(col("ki").desc()).select("id")),
        ),
        ("sort_int_asc", "sort('ki')", lambda: rows(w.sort("ki").select("id"))),
        (
            "sort_string_desc",
            "orderBy(desc('ks'))",
            lambda: rows(w.orderBy(functions.desc("ks")).select("id")),
        ),
        (
            "sort_double_asc",
            "orderBy(col('kd').asc())",
            lambda: rows(w.orderBy(col("kd").asc()).select("id")),
        ),
        (
            "typed_window_double_desc",
            "row_number().over(Window.orderBy(col('kd').desc()))",
            lambda: rows(
                w.select(
                    "id", functions.row_number().over(window.orderBy(col("kd").desc())).alias("r")
                ).orderBy("id")
            ),
        ),
        (
            "typed_window_int_asc",
            "row_number().over(Window.orderBy('ki'))",
            lambda: rows(
                w.select(
                    "id", functions.row_number().over(window.orderBy("ki")).alias("r")
                ).orderBy("id")
            ),
        ),
        (
            "sparksql_window_int",
            "row_number() OVER (ORDER BY ki)",
            lambda: rows(
                spark.sql("SELECT id, row_number() OVER (ORDER BY ki) AS r FROM w ORDER BY id")
            ),
        ),
        (
            "sparksql_order_by_string_desc",
            "SELECT id FROM w ORDER BY ks DESC",
            lambda: rows(spark.sql("SELECT id FROM w ORDER BY ks DESC")),
        ),
        (
            "fexpr_window_string_desc",
            "row_number() OVER (ORDER BY ks DESC)",
            lambda: rows(
                w.select(
                    "id", functions.expr("row_number() OVER (ORDER BY ks DESC)").alias("r")
                ).orderBy("id")
            ),
        ),
        (
            "selectexpr_window_double",
            "row_number() OVER (ORDER BY kd)",
            lambda: rows(w.selectExpr("id", "row_number() OVER (ORDER BY kd) AS r").orderBy("id")),
        ),
    ]
    if native_sql is not None:
        cells += [
            (
                "native_window_int",
                "row_number() OVER (ORDER BY ki)",
                lambda: rows(
                    native_sql(
                        f"{NATIVE_SOURCE}SELECT id, row_number() OVER (ORDER BY ki) AS r"
                        " FROM w ORDER BY id"
                    )
                ),
            ),
            (
                "native_order_by_int",
                "SELECT id FROM w ORDER BY ki",
                lambda: rows(native_sql(f"{NATIVE_SOURCE}SELECT id FROM w ORDER BY ki")),
            ),
        ]
    return cells


def join_doors(spark: Any, functions: Any) -> dict[str, Any]:
    """Map each joined-frame door to the call that filters on ``<left key> = <scalar subquery>``."""
    w = spark.table("w")
    right = spark.sql(f"SELECT * FROM VALUES {ROWS} AS r(rid, rki, rks, rkd)")

    def joined() -> Any:
        return w.join(right, w["id"] == right["rid"])

    return {
        "join_filter_column": lambda key, sub: rows(
            joined().filter(w[key] == functions.expr(sub)).select("id").orderBy("id")
        ),
        "join_filter_str": lambda key, sub: rows(
            joined().filter(f"{key} = {sub}").select("id").orderBy("id")
        ),
    }


def main() -> None:
    """Run every cell on every door and write one jsonl line each."""
    engine, out_path = sys.argv[1], sys.argv[2]
    os.environ["TZ"] = "UTC"
    spark, functions, window, native_sql, header = session(engine)
    spark.sql(SOURCE).createOrReplaceTempView("w")
    with Path(out_path).open("w", encoding="utf-8") as out:
        out.write(json.dumps({"header": header}) + "\n")

        def record(kind: str, cell: str, door: str, text: str, call: Any) -> None:
            try:
                value = call()
            except Exception as error:
                value = err(error)
            line = {"kind": kind, "cell": cell, "door": door, "text": text, "value": value}
            out.write(json.dumps(line, sort_keys=True) + "\n")
            out.flush()

        for door, call in predicate_doors(spark, functions, native_sql).items():
            for cell, text in PREDICATES:
                record("predicate", cell, door, text, lambda call=call, text=text: call(text))
        for door, call in join_doors(spark, functions).items():
            for cell, key, sub in SCALARS:
                record(
                    "join",
                    cell,
                    door,
                    f"{key} = {sub}",
                    lambda call=call, key=key, sub=sub: call(key, sub),
                )
        for cell, text, call in control_cells(spark, functions, window, native_sql):
            record("control", cell, "control", text, call)


if __name__ == "__main__":
    main()
