from __future__ import annotations

import re
from collections.abc import Callable
from decimal import Context, Decimal
from typing import Any

WIDE_CONTEXT = Context(prec=60)
DECIMAL_TEXT = re.compile(r"-?\d+\.\d+")
UNSCALED = [30, 100, 251, -251, 1, 1000, None, 777, 6]
KINDS = {
    "id": "INT",
    "d10_0": "DECIMAL(10,0)",
    "d10_2": "DECIMAL(10,2)",
    "d28_2": "DECIMAL(28,2)",
    "bi": "INT",
    "i": "INT",
    "j": "INT",
    "x": "DOUBLE",
    "s": "STRING",
}
ICE_MODES = {
    "cow": "'write.update.mode'='copy-on-write', 'write.merge.mode'='copy-on-write'",
    "mor": "'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read', "
    "'format-version'='2'",
}


def text(unscaled: int | None, scale: int) -> str | None:
    if unscaled is None:
        return None
    return format(Decimal(unscaled).scaleb(-scale, context=WIDE_CONTEXT), "f")


def rows() -> list[list[str | None]]:
    out: list[list[str | None]] = []
    for index, unscaled in enumerate(UNSCALED, start=1):
        out.append(
            [
                str(index),
                text(unscaled, 0),
                text(unscaled, 2),
                text(unscaled, 2),
                "3",
                str(7 * index),
                "2",
                text(unscaled, 2),
                "10",
            ]
        )
    return out


def sql_cells() -> dict[str, str]:
    cells = {
        "div/right/d10_2/3": "d10_2 / 3",
        "div/right/d28_2/3": "d28_2 / 3",
        "div/right/d10_0/3": "d10_0 / 3",
        "div/right/d10_2/100": "d10_2 / 100",
        "div/right/d10_2/neg3": "d10_2 / -3",
        "div/right/d10_2/11": "d10_2 / 11",
        "div/left/d10_2/3": "3 / d10_2",
        "div/left/d28_2/3": "3 / d28_2",
        "div/left/d10_2/100": "100 / d10_2",
        "mod/right/d10_2/3": "d10_2 % 3",
        "mod/right/d28_2/3": "d28_2 % 3",
        "mod/left/d10_2/3": "3 % d10_2",
        "suffix/mul/3S": "d10_2 * 3S",
        "suffix/mul/3L": "d10_2 * 3L",
        "suffix/mul/neg3L": "d10_2 * -3L",
        "suffix/sub/3L": "d10_2 - 3L",
        "suffix/div/3S": "d10_2 / 3S",
        "suffix/div/3L": "d10_2 / 3L",
        "suffix/div_left/3L": "3L / d10_2",
        "suffix/mod/3L": "d10_2 % 3L",
        "call/mod": "mod(d10_2, 3)",
        "call/mod_left": "mod(3, d10_2)",
        "call/mod_3L": "mod(d10_2, 3L)",
        "call/pmod": "pmod(d10_2, 3)",
        "call/pmod_neg": "pmod(d10_2, -3)",
        "call/pmod_d28_2": "pmod(d28_2, 3)",
        "call/mod_decimal": "mod(d10_2, 0.7)",
        "call/mod_intcol": "mod(d10_2, bi)",
        "call/mod_neg": "mod(d10_2, -3)",
        "call/mod_d28_2": "mod(d28_2, 3)",
        "call/mod_zero": "mod(d10_2, 0)",
        "call/mod_zero_decimal": "mod(d10_2, CAST(0 AS DECIMAL(5,2)))",
        "call/mod_zero_column": "mod(d10_2, bi - 3)",
        "mod/zero": "d10_2 % 0",
        "guard/fold": "d10_2 / (1 + 2)",
        "guard/int_column": "d10_2 / bi",
        "guard/int_int": "i / j",
        "guard/int_literal": "i / 3",
        "guard/float": "x / 3",
        "guard/cast_bigint": "d10_2 / CAST(3 AS BIGINT)",
        "guard/cast_decimal": "d10_2 / CAST(3 AS DECIMAL(20,0))",
        "guard/cast_smallint_mul": "d10_2 * CAST(3 AS SMALLINT)",
        "guard/tinyint": "d10_2 / 3Y",
        "guard/int_mod": "mod(i, 3)",
        "guard/float_mod": "x % 3",
    }
    out = {f"sql/{key}": f"SELECT id, {expr} AS v FROM v" for key, expr in cells.items()}
    out["sql/scope/derived"] = "SELECT h / 2 AS v FROM (SELECT d10_2 / 2 AS h FROM v) q"
    out["sql/scope/cte"] = "WITH q AS (SELECT d10_2 / 2 AS h FROM v) SELECT h / 2 AS v FROM q"
    out["sql/scope/flat"] = "SELECT d10_2 / 2 / 2 AS v FROM v"
    out["sql/scope/view"] = "SELECT h / 2 AS v FROM hv"
    out["sql/avg/d10_2"] = "SELECT avg(d10_2 / 3) AS v FROM v"
    out["sql/avg/d28_2"] = "SELECT avg(d28_2 / 3) AS v FROM v"
    out["sql/where"] = "SELECT id FROM v WHERE d10_2 / 3 = 0.836667"
    out["sql/case"] = "SELECT id, CASE WHEN d10_2 / 3 > 0.8366667 THEN 'hi' ELSE 'lo' END FROM v"
    out["sql/guard/suffix_select"] = "SELECT 3L AS a, 3S AS b, -3L AS c"
    return out


def frame_cells(sf: Any) -> dict[str, Callable[[Any], Any]]:
    d = sf.col("d10_2")
    return {
        "df/div_int": lambda f: f.select("id", (d / 3).alias("v")),
        "df/div_lit": lambda f: f.select("id", (d / sf.lit(3)).alias("v")),
        "df/div_column": lambda f: f.select("id", (d / sf.col("bi")).alias("v")),
        "df/div_left": lambda f: f.select("id", (3 / d).alias("v")),
        "df/div_d28_2": lambda f: f.select("id", (sf.col("d28_2") / 3).alias("v")),
        "df/div_decimal": lambda f: f.select("id", (d / sf.col("d28_2")).alias("v")),
        "df/div_nested": lambda f: f.select("id", ((d / 3) / 2).alias("v")),
        "df/mod": lambda f: f.select("id", (d % 3).alias("v")),
        "df/pmod": lambda f: f.select("id", sf.pmod(d, sf.lit(3)).alias("v")),
        "df/expr_div": lambda f: f.select("id", sf.expr("d10_2 / 3").alias("v")),
        "df/expr_mod": lambda f: f.select("id", sf.expr("mod(d10_2, 3)").alias("v")),
        "df/expr_mod_neg": lambda f: f.select("id", sf.expr("mod(d10_2, -3)").alias("v")),
        "df/expr_mod_zero": lambda f: f.select("id", sf.expr("mod(d10_2, 0)").alias("v")),
        "df/select_expr_pmod": lambda f: f.selectExpr("id", "pmod(d10_2, 3) AS v"),
        "df/guard/int_int": lambda f: f.select("id", (sf.col("i") / sf.col("j")).alias("v")),
        "df/guard/int_literal": lambda f: f.select("id", (sf.col("i") / 3).alias("v")),
        "df/guard/float": lambda f: f.select("id", (sf.col("x") / 3).alias("v")),
        "df/guard/decimal_float": lambda f: f.select("id", (d / sf.col("x")).alias("v")),
        "df/guard/string": lambda f: f.select("id", (sf.col("s") / 2).alias("v")),
        "df/guard/fold": lambda f: f.select("id", sf.expr("d10_2 / (1 + 2)").alias("v")),
    }


def norm(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def answer_of(frame: Any) -> dict[str, list[str]]:
    rendered = ("|".join(norm(v) for v in row) for row in frame.collect())
    return {
        "rows": sorted(rendered),
        "types": [field.dataType.simpleString() for field in frame.schema.fields],
    }


def same_answer(found: Any, want: Any) -> bool:
    if found == want:
        return True
    if not (isinstance(found, dict) and isinstance(want, dict)):
        return False
    if found.get("types") != want.get("types") or "rows" not in found or "rows" not in want:
        return False
    return found["rows"] == want["rows"]


def error_text(error: Exception) -> str:
    return "ERR:" + " ".join(str(error).split())[:300]


def attempt(run: Callable[[], Any]) -> Any:
    try:
        return run()
    except Exception as error:
        return error_text(error)


def make_tables(spark: Any, root: str) -> None:
    names = list(KINDS)
    raw = spark.createDataFrame(rows(), ", ".join(f"{n} STRING" for n in names))
    typed = raw.selectExpr(*[f"CAST({n} AS {KINDS[n]}) AS {n}" for n in names])
    path = f"{root}/pq_v"
    typed.write.mode("overwrite").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("v")
    spark.sql("CREATE OR REPLACE TEMP VIEW hv AS SELECT d10_2 / 2 AS h FROM v").collect()


def stored(spark: Any, statement: str, table: str) -> dict[str, Any]:
    result = attempt(lambda: spark.sql(statement).collect())
    return {
        "statement": result if isinstance(result, str) else "ok",
        "rows": attempt(lambda: answer_of(spark.sql(f"SELECT * FROM {table}"))),
        "schema": [f.dataType.simpleString() for f in spark.table(table).schema.fields],
    }


def iceberg_cells(spark: Any, catalog: str) -> dict[str, Any]:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.ddl").collect()
    found: dict[str, Any] = {}
    for c in ("d10_2", "d28_2"):
        name = f"{catalog}.ddl.ctas_{c}"
        found[f"ice/ctas/{c}"] = attempt(
            lambda c=c, name=name: stored(
                spark,
                f"CREATE TABLE {name} USING iceberg AS SELECT id, {c} / 3 AS q, {c} / 11 AS q11 "
                "FROM v",
                name,
            )
        )
        name = f"{catalog}.ddl.insert_{c}"
        spark.sql(
            f"CREATE TABLE {name} (id INT, n DECIMAL(10,5), w DECIMAL(24,10)) USING iceberg"
        ).collect()
        found[f"ice/insert/{c}"] = attempt(
            lambda c=c, name=name: stored(
                spark, f"INSERT INTO {name} SELECT id, {c} / 11, {c} / 3 FROM v", name
            )
        )
    for mode, props in ICE_MODES.items():
        create = (
            "CREATE TABLE {n} (id INT, d DECIMAL(10,2), n DECIMAL(10,5), w DECIMAL(24,10)) "
            f"USING iceberg TBLPROPERTIES ({props})"
        )
        name = f"{catalog}.ddl.update_{mode}"
        spark.sql(create.format(n=name)).collect()
        spark.sql(f"INSERT INTO {name} SELECT id, d10_2, 0, 0 FROM v").collect()
        found[f"ice/update/{mode}"] = attempt(
            lambda name=name: stored(spark, f"UPDATE {name} SET n = d / 11, w = d / 3", name)
        )
        name = f"{catalog}.ddl.merge_{mode}"
        spark.sql(create.format(n=name)).collect()
        spark.sql(f"INSERT INTO {name} SELECT id, d10_2, 0, 0 FROM v WHERE id < 5").collect()
        found[f"ice/merge/{mode}"] = attempt(
            lambda name=name: stored(
                spark,
                f"MERGE INTO {name} USING v s ON {name}.id = s.id "
                "WHEN MATCHED THEN UPDATE SET n = s.d10_2 / 11, w = s.d10_2 / 3 "
                "WHEN NOT MATCHED THEN INSERT (id, d, n, w) "
                "VALUES (s.id, s.d10_2, s.d10_2 / 11, s.d10_2 / 3)",
                name,
            )
        )
    return found


def measure_all(spark: Any, sf: Any, catalog: str) -> dict[str, Any]:
    found: dict[str, Any] = {}
    for key, sql in sql_cells().items():
        found[key] = attempt(lambda sql=sql: answer_of(spark.sql(sql)))
    for key, build in frame_cells(sf).items():
        found[key] = attempt(lambda build=build: answer_of(build(spark.table("v"))))
    found.update(iceberg_cells(spark, catalog))
    return found


def measure_pick_false(spark: Any, sf: Any) -> dict[str, Any]:
    cells = {
        "pmp/mul": "d10_2 * 3",
        "pmp/div": "d10_2 / 3",
        "pmp/mod": "d10_2 % 3",
        "pmp/call_mod": "mod(d10_2, 3)",
        "pmp/call_mod_d28_2": "mod(d28_2, 3)",
        "pmp/call_mod_zero": "mod(d10_2, 0)",
        "pmp/suffix_mul": "d10_2 * 3L",
        "pmp/left_div": "3 / d28_2",
    }
    found = {
        key: attempt(lambda e=e: answer_of(spark.sql(f"SELECT id, {e} AS v FROM v")))
        for key, e in cells.items()
    }
    frame = spark.table("v")
    found["pmp/df_div"] = attempt(
        lambda: answer_of(frame.select("id", (sf.col("d10_2") / 3).alias("v")))
    )
    return found


PMOD_KINDS = {
    "id": "INT",
    "d10_2": "DECIMAL(10,2)",
    "d28_2": "DECIMAL(28,2)",
    "d38_10": "DECIMAL(38,10)",
    "zd": "DECIMAL(5,2)",
    "zi": "INT",
    "n": "INT",
    "i": "INT",
    "x": "DOUBLE",
}
PMOD_ROWS = [
    ["1", "7.25", "7.25", "7.25", "0", "0", "3", "7", "7.25"],
    ["2", "-7.25", "-7.25", "-7.25", "0", "0", "-3", "-7", "-7.25"],
    ["3", None, None, None, "0", "0", "3", None, None],
    ["4", "6.00", "6.00", "6.00", "0", "0", "-3", "6", "6.0"],
    ["5", "-6.00", "-6.00", "-6.00", "0", "0", "3", "-6", "-6.0"],
    [
        "6",
        "12345678.91",
        "12345678901234567890123456.78",
        "1234567890123456789012345678.1234567890",
        "0",
        "0",
        "3",
        "2147483647",
        "12345678.91",
    ],
    [
        "7",
        "-12345678.91",
        "-12345678901234567890123456.78",
        "-1234567890123456789012345678.1234567890",
        "0",
        "0",
        "-3",
        "-2147483647",
        "-12345678.91",
    ],
]
PRECISIONS = ("d10_2", "d28_2", "d38_10")
PICK_SET_KEY = "spark.sql.legacy.literal.pickMinimumPrecision"
SET_EXPRS = {
    "div": "d10_2 / 3",
    "mod": "d10_2 % 3",
    "call_mod": "mod(d10_2, 3)",
    "call_pmod": "pmod(d10_2, 3)",
    "mul": "d10_2 * 3",
    "add": "d10_2 + 3",
    "sub": "d10_2 - 3",
}


def make_pmod_table(spark: Any, root: str) -> None:
    names = list(PMOD_KINDS)
    raw = spark.createDataFrame(PMOD_ROWS, ", ".join(f"{n} STRING" for n in names))
    typed = raw.selectExpr(*[f"CAST({n} AS {PMOD_KINDS[n]}) AS {n}" for n in names])
    path = f"{root}/pq_w"
    typed.write.mode("overwrite").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("w")


def pmod_sql_cells() -> dict[str, str]:
    out: dict[str, str] = {}
    for p in PRECISIONS:
        for name, expr in (
            ("pos", f"pmod({p}, 3)"),
            ("neg_divisor", f"pmod({p}, -3)"),
            ("left_neg", f"pmod(-3, {p})"),
            ("left_pos", f"pmod(3, {p})"),
            ("zero", f"pmod({p}, 0)"),
            ("zero_decimal", f"pmod({p}, CAST(0 AS DECIMAL(5,2)))"),
            ("zero_column", f"pmod({p}, zd)"),
            ("int_column", f"pmod({p}, n)"),
        ):
            out[f"pm/sql/{p}/{name}"] = f"SELECT id, {expr} AS v FROM w"
    return out


def pmod_frame_cells(sf: Any) -> dict[str, Callable[[Any], Any]]:
    out: dict[str, Callable[[Any], Any]] = {}
    for p in PRECISIONS:
        for name, expr in (
            ("neg_divisor", f"pmod({p}, -3)"),
            ("zero", f"pmod({p}, 0)"),
            ("left_neg", f"pmod(-3, {p})"),
        ):
            out[f"pm/expr/{p}/{name}"] = lambda f, e=expr: f.select("id", sf.expr(e).alias("v"))
            out[f"pm/select_expr/{p}/{name}"] = lambda f, e=expr: f.selectExpr("id", f"{e} AS v")
    operands = {p: (sf.lit(3), sf.lit(-3), sf.lit(0), "zd") for p in PRECISIONS}
    operands["i"] = (sf.lit(3), sf.lit(-3), sf.lit(0), "zi")
    operands["x"] = (sf.lit(3.0), sf.lit(-3.0), sf.lit(0.0), "zi")
    for c, (three, minus, zero, zcol) in operands.items():
        for name, left, right in (
            ("pos", sf.col(c), three),
            ("neg_divisor", sf.col(c), minus),
            ("left_neg", minus, sf.col(c)),
            ("zero", sf.col(c), zero),
            ("zero_column", sf.col(c), sf.col(zcol)),
            ("int_column", sf.col(c), sf.col("n")),
        ):
            out[f"pm/F/{c}/{name}"] = lambda f, a=left, b=right: f.select(
                "id", sf.pmod(a, b).alias("v")
            )
    return out


def measure_pmod(spark: Any, sf: Any) -> dict[str, Any]:
    found: dict[str, Any] = {}
    for key, sql in pmod_sql_cells().items():
        found[key] = attempt(lambda sql=sql: answer_of(spark.sql(sql)))
    for key, build in pmod_frame_cells(sf).items():
        found[key] = attempt(lambda build=build: answer_of(build(spark.table("w"))))
    return found


def set_state(spark: Any, label: str, found: dict[str, Any]) -> None:
    for name, expr in SET_EXPRS.items():
        found[f"rs/{label}/{name}"] = attempt(
            lambda e=expr: answer_of(spark.sql(f"SELECT id, {e} AS v FROM v"))
        )


def measure_runtime_set(spark: Any, built_false: bool) -> dict[str, Any]:
    found: dict[str, Any] = {}
    if built_false:
        set_state(spark, "built_false", found)
        spark.sql(f"SET {PICK_SET_KEY}=true").collect()
        set_state(spark, "built_false_set_true", found)
        spark.sql("RESET").collect()
        set_state(spark, "built_false_reset", found)
        return found
    spark.sql(
        "CREATE OR REPLACE TEMP VIEW sv AS "
        "SELECT id, d10_2 * 3 AS m, d10_2 / 3 AS q, d10_2 % 3 AS r FROM v"
    ).collect()
    found["rs/refuse"] = attempt(lambda: answer_of(spark.sql(f"SET {PICK_SET_KEY}=maybe")))
    spark.sql(f"SET {PICK_SET_KEY}=false").collect()
    set_state(spark, "set_false", found)
    found["rs/set_false/view"] = attempt(lambda: answer_of(spark.sql("SELECT * FROM sv")))
    spark.sql(f"SET {PICK_SET_KEY}=true").collect()
    set_state(spark, "set_true", found)
    spark.sql(f"SET {PICK_SET_KEY}=false").collect()
    spark.sql(f"RESET {PICK_SET_KEY}").collect()
    set_state(spark, "reset", found)
    return found
