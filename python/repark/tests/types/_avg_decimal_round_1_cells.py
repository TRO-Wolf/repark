from __future__ import annotations

import re
from collections.abc import Callable
from decimal import ROUND_HALF_UP, Decimal, getcontext
from typing import Any

getcontext().prec = 120

DECIMAL_TEXT = re.compile(r"-?\d+\.\d+")
COLUMNS = [(10, 0), (10, 2), (28, 2), (34, 4), (38, 0), (38, 18)]
NORMAL = {
    1: [1, 2],
    2: [1, 1, 0],
    4: [-1, -1, 0],
    5: [1] + [0] * 31,
    7: [1, None, 2, None, 2],
    9: [251, 0, 0],
    10: [-7, 2, -1],
    13: [1, 2, 5, 5],
}
BIG = (20, 22, 24)
UNGROUPED = (2, 5, 9, 10, 20, 22, 24)
WIDE = ("c10_2", "c28_2", "c34_4", "c38_18")
EDGE = ("c10_2", "c38_18")
MAX38 = "9" * 38
S04 = [MAX38, MAX38, "-" + MAX38, "-" + MAX38, "5"]
FUNCTIONS = ("avg", "mean")


def col(p: int, s: int) -> str:
    return f"c{p}_{s}"


def pattern(g: int, p: int) -> list[int | None]:
    if g in NORMAL:
        return NORMAL[g]
    if g == 20:
        return [10**p - 1] * 2
    if g == 22:
        return [9 * 10 ** (min(p, 34) - 1)] * 2
    return [5 * 10 ** (p - 1)] * 2


def text(unscaled: int | None, s: int) -> str | None:
    return None if unscaled is None else format(Decimal(unscaled).scaleb(-s), "f")


def rows() -> tuple[list[str], list[list[str | None]]]:
    names = ["g", "id"] + [col(p, s) for p, s in COLUMNS] + ["x", "b", "i"]
    out: list[list[str | None]] = []
    for g in list(NORMAL) + list(BIG):
        width = max(len(pattern(g, p)) for p, _ in COLUMNS)
        for k in range(width):
            row: list[str | None] = [str(g), str(len(out) + 1)]
            for p, s in COLUMNS:
                values = pattern(g, p)
                row.append(text(values[k] if k < len(values) else None, s))
            base = NORMAL.get(g)
            plain = base[k] if base is not None and k < len(base) else None
            row += [None if plain is None else str(float(plain))]
            row += [None if plain is None else str(plain)] * 2
            out.append(row)
    return names, out


def types() -> dict[str, str]:
    found = {"g": "INT", "id": "INT", "x": "DOUBLE", "b": "BIGINT", "i": "INT"}
    for p, s in COLUMNS:
        found[col(p, s)] = f"DECIMAL({p},{s})"
    return found


def expected(g: int, p: int, s: int) -> str:
    values = [v for v in pattern(g, p) if v is not None]
    quotient = Decimal(sum(values)).scaleb(-s) / Decimal(len(values))
    scale = min(s + 4, 38)
    return format(quotient.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP), "f")


def having_case(p: int, s: int) -> str:
    target = f"DECIMAL({min(p + 4, 38)},{min(s + 4, 38)})"
    arms = " ".join(f"WHEN {g} THEN CAST('{expected(g, p, s)}' AS {target})" for g in NORMAL)
    return f"CASE g {arms} END"


def normal() -> str:
    return ", ".join(str(g) for g in NORMAL)


def position_sql(c: str) -> dict[str, str]:
    p, s = (int(part) for part in c[1:].split("_"))
    where = f"FROM t WHERE g IN ({normal()})"
    window = "OVER (PARTITION BY g ORDER BY id"
    return {
        "grouped": f"SELECT g, {{f}}({c}) AS m {where} GROUP BY g",
        "having": f"SELECT g {where} GROUP BY g HAVING {{f}}({c}) = {having_case(p, s)}",
        "distinct": f"SELECT g, {{f}}(DISTINCT {c}) AS m {where} GROUP BY g",
        "filter": f"SELECT g, {{f}}({c}) FILTER (WHERE {c} >= 0) AS m {where} GROUP BY g",
        "win_part": f"SELECT g, id, {{f}}({c}) OVER (PARTITION BY g) AS m {where}",
        "win_rows": f"SELECT g, id, {{f}}({c}) {window} ROWS 1 PRECEDING) AS m {where}",
        "win_rows3": (
            f"SELECT g, id, {{f}}({c}) {window} ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS m "
            f"{where}"
        ),
        "win_running": (
            f"SELECT g, id, {{f}}({c}) {window} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
            f" AS m {where}"
        ),
        "win_range": (
            f"SELECT g, id, {{f}}({c}) {window} RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) AS m "
            f"{where}"
        ),
        "grouping_sets": f"SELECT g, {{f}}({c}) AS m {where} GROUP BY GROUPING SETS ((g), ())",
        "rollup": f"SELECT g, {{f}}({c}) AS m {where} GROUP BY ROLLUP(g)",
        "cube": f"SELECT g, {{f}}({c}) AS m {where} GROUP BY CUBE(g)",
        "union_all": (
            f"SELECT 2 AS k, {{f}}({c}) AS m FROM t WHERE g = 2 UNION ALL "
            f"SELECT 9 AS k, {{f}}({c}) AS m FROM t WHERE g = 9"
        ),
        "beside": (f"SELECT g, {{f}}({c}) + max({c}) AS a, max({c}) AS mx {where} GROUP BY g"),
        "grouped_t4": f"SELECT g, {{f}}({c}) AS m FROM t4 WHERE g IN ({normal()}) GROUP BY g",
    }


EDGE_ONLY = {"having", "filter", "win_rows3", "win_running", "win_range", "grouping_sets"}
EDGE_ONLY |= {"rollup", "cube", "union_all", "beside"}

SUM_SQL = {
    "s04": "SELECT {f}(x) AS m FROM s04",
    "s04_head": "SELECT {f}(x) AS m FROM s04 WHERE k IN (0, 1, 4)",
    "s01": "SELECT {f}(x) AS m FROM s04 WHERE k IN (0, 1, 2)",
}

CONTROL_SQL = {
    "sum_div_count/c10_2": (
        f"SELECT g, sum(c10_2) / count(c10_2) AS m FROM t WHERE g IN ({normal()}) GROUP BY g"
    ),
    "sum_div_count/c38_18": (
        f"SELECT g, sum(c38_18) / count(c38_18) AS m FROM t WHERE g IN ({normal()}) GROUP BY g"
    ),
    "typed_div/c10_2": "SELECT id, c10_2 / i AS q FROM t WHERE g IN (1, 10)",
    "try_avg/c10_2": f"SELECT g, try_avg(c10_2) AS m FROM t WHERE g IN ({normal()}) GROUP BY g",
    "try_avg/c38_0_g20": "SELECT try_avg(c38_0) AS m FROM t WHERE g = 20",
}
for _f in FUNCTIONS:
    for _c in ("x", "b", "i"):
        CONTROL_SQL[f"{_f}_{_c}/grouped"] = (
            f"SELECT g, {_f}({_c}) AS m FROM t WHERE g IN ({normal()}) GROUP BY g"
        )
        CONTROL_SQL[f"{_f}_{_c}/ungrouped"] = f"SELECT {_f}({_c}) AS m FROM t WHERE g = 10"
        CONTROL_SQL[f"{_f}_{_c}/win_rows"] = (
            f"SELECT g, id, {_f}({_c}) OVER (PARTITION BY g ORDER BY id ROWS 1 PRECEDING) AS m "
            f"FROM t WHERE g IN ({normal()})"
        )


def sql_cells() -> dict[str, str]:
    cells: dict[str, str] = {}
    for f in FUNCTIONS:
        for p, s in COLUMNS:
            c = col(p, s)
            for g in UNGROUPED:
                cells[f"{f}/ungrouped/{c}/g{g}"] = f"SELECT {f}({c}) AS m FROM t WHERE g = {g}"
        for c in WIDE:
            for pos, sql in position_sql(c).items():
                if pos in EDGE_ONLY and c not in EDGE:
                    continue
                cells[f"{f}/{pos}/{c}"] = sql.format(f=f)
        for key, sql in SUM_SQL.items():
            cells[f"{f}/sum/{key}"] = sql.format(f=f)
    for c in WIDE:
        cells[f"try_avg/grouped/{c}"] = position_sql(c)["grouped"].format(f="try_avg")
    for key, sql in CONTROL_SQL.items():
        cells[f"ctl/{key}"] = sql
    return cells


def frame_cells(sf: Any, window: Any) -> dict[str, Callable[[Any], Any]]:
    cells: dict[str, Callable[[Any], Any]] = {}
    for c in EDGE:

        def base(t: Any) -> Any:
            return t.where(sf.col("g").isin(list(NORMAL)))

        cells[f"df/F.avg/{c}"] = lambda t, c=c: base(t).groupBy("g").agg(sf.avg(c).alias("m"))
        cells[f"df/F.mean/{c}"] = lambda t, c=c: base(t).groupBy("g").agg(sf.mean(c).alias("m"))
        cells[f"df/GroupedData.avg/{c}"] = lambda t, c=c: base(t).groupBy("g").avg(c)
        cells[f"df/GroupedData.mean/{c}"] = lambda t, c=c: base(t).groupBy("g").mean(c)
        cells[f"df/agg_dict_avg/{c}"] = lambda t, c=c: base(t).groupBy("g").agg({c: "avg"})
        cells[f"df/agg_dict_mean/{c}"] = lambda t, c=c: base(t).groupBy("g").agg({c: "mean"})
        cells[f"df/select_F.avg/{c}"] = lambda t, c=c: t.where("g = 9").select(sf.avg(c).alias("m"))
        cells[f"df/select_F.mean/{c}"] = lambda t, c=c: t.where("g = 9").select(
            sf.mean(c).alias("m")
        )
        cells[f"df/agg_F.expr_avg/{c}"] = lambda t, c=c: (
            base(t).groupBy("g").agg(sf.expr(f"avg({c})").alias("m"))
        )
        cells[f"df/agg_F.expr_mean/{c}"] = lambda t, c=c: (
            base(t).groupBy("g").agg(sf.expr(f"mean({c})").alias("m"))
        )
        cells[f"df/selectExpr_avg/{c}"] = lambda t, c=c: t.where("g = 9").selectExpr(
            f"avg({c}) AS m"
        )
        cells[f"df/selectExpr_mean/{c}"] = lambda t, c=c: t.where("g = 9").selectExpr(
            f"mean({c}) AS m"
        )
        cells[f"df/window_rows/{c}"] = lambda t, c=c: base(t).select(
            "g",
            "id",
            sf.avg(c).over(window.partitionBy("g").orderBy("id").rowsBetween(-2, 0)).alias("m"),
        )
        cells[f"df/window_rows_mean/{c}"] = lambda t, c=c: base(t).select(
            "g",
            "id",
            sf.mean(c).over(window.partitionBy("g").orderBy("id").rowsBetween(-2, 0)).alias("m"),
        )
        cells[f"df/repartition/{c}"] = lambda t, c=c: (
            base(t.repartition(4)).groupBy("g").agg(sf.avg(c).alias("m"))
        )
    for c in ("c10_2", "c28_2"):
        cells[f"df/describe/{c}"] = lambda t, c=c: (
            t.where("g IN (2, 9)").describe(c).where("summary IN ('count', 'mean')")
        )
        cells[f"df/summary/{c}"] = lambda t, c=c: (
            t.where("g IN (2, 9)")
            .select(c)
            .summary("count", "mean", "stddev", "min", "max")
            .where("summary IN ('count', 'mean')")
        )
    return cells


ICE_AVG = {
    "ctas_avg/c10_0": ("avg", "c10_0"),
    "ctas_avg/c10_2": ("avg", "c10_2"),
    "ctas_avg/c28_2": ("avg", "c28_2"),
    "ctas_avg/c38_18": ("avg", "c38_18"),
    "ctas_mean/c10_2": ("mean", "c10_2"),
    "ctas_mean/c28_2": ("mean", "c28_2"),
}
ICE_MODES = {
    "cow": "'write.update.mode'='copy-on-write','write.merge.mode'='copy-on-write'",
    "mor": (
        "'write.update.mode'='merge-on-read','write.merge.mode'='merge-on-read',"
        "'format-version'='2'"
    ),
}


def norm(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(norm(item) for item in value) + "]"
    return str(value)


def answer_of(frame: Any) -> dict[str, list[str]]:
    rendered = ("|".join(norm(v) for v in row) for row in frame.collect())
    return {
        "rows": sorted(rendered),
        "types": [field.dataType.simpleString() for field in frame.schema.fields],
    }


def trim_decimal(match: re.Match[str]) -> str:
    found = match.group(0).rstrip("0").rstrip(".")
    return "0" if found in ("-0", "") else found


def same_answer(found: Any, want: Any) -> bool:
    if found == want:
        return True
    if not (isinstance(found, dict) and isinstance(want, dict)):
        return False
    if found.get("types") != want.get("types") or "rows" not in found or "rows" not in want:
        return False
    trim = [sorted(DECIMAL_TEXT.sub(trim_decimal, r) for r in x["rows"]) for x in (found, want)]
    return trim[0] == trim[1]


def error_text(error: Exception) -> str:
    return "ERR:" + " ".join(str(error).split())[:300]


def attempt(run: Callable[[], Any]) -> Any:
    try:
        return run()
    except Exception as error:
        return error_text(error)


def make_table(
    spark: Any, root: str, name: str, data: tuple[list, list], kinds: dict, parts: int
) -> None:
    names, values = data
    raw = spark.createDataFrame(values, ", ".join(f"{n} STRING" for n in names))
    typed = raw.selectExpr(*[f"CAST({n} AS {kinds[n]}) AS {n}" for n in names])
    path = f"{root}/pq_{name}"
    for part in range(parts):
        typed.where(f"{names[1]} % {parts} = {part}").write.mode("append").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView(name)


def make_tables(spark: Any, root: str) -> None:
    make_table(spark, root, "t", rows(), types(), 1)
    make_table(spark, root, "t4", rows(), types(), 4)
    s04 = (["g", "k", "x"], [["1", str(k), x] for k, x in enumerate(S04)])
    make_table(spark, root, "s04", s04, {"g": "INT", "k": "INT", "x": "DECIMAL(38,0)"}, 1)


def stored(spark: Any, statement: str, read: str, table: str) -> dict[str, Any]:
    result = attempt(lambda: spark.sql(statement).collect())
    return {
        "statement": result if isinstance(result, str) else "ok",
        "rows": attempt(lambda: answer_of(spark.sql(read))),
        "schema": [f.dataType.simpleString() for f in spark.table(table).schema.fields],
    }


def iceberg_cells(spark: Any, catalog: str) -> dict[str, Any]:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.avgns").collect()
    found: dict[str, Any] = {}
    where = f"FROM t WHERE g IN ({normal()}) GROUP BY g"
    for key, (f, c) in ICE_AVG.items():
        name = f"{catalog}.avgns.{key.replace('/', '_')}"
        found[f"ice/{key}"] = attempt(
            lambda f=f, c=c, name=name: stored(
                spark,
                f"CREATE TABLE {name} USING iceberg AS SELECT g, {f}({c}) AS m {where}",
                f"SELECT g, m FROM {name}",
                name,
            )
        )
    for mode, props in ICE_MODES.items():
        for c in ("c10_2", "c10_0"):
            create = (
                "CREATE TABLE {n} (k INT, n DECIMAL(10,5), w DECIMAL(24,10)) USING iceberg "
                f"TBLPROPERTIES ({props})"
            )
            if mode == "cow":
                name = f"{catalog}.avgns.ins_{mode}_{c}"
                spark.sql(create.format(n=name)).collect()
                found[f"ice/insert_avg/{mode}/{c}"] = attempt(
                    lambda c=c, name=name: stored(
                        spark,
                        f"INSERT INTO {name} SELECT g, avg({c}), avg({c}) {where}",
                        f"SELECT k, n, w FROM {name}",
                        name,
                    )
                )
            name = f"{catalog}.avgns.merge_{mode}_{c}"
            spark.sql(create.format(n=name)).collect()
            spark.sql(f"INSERT INTO {name} VALUES (1, 0, 0), (2, 0, 0), (9, 0, 0)").collect()
            found[f"ice/merge_avg/{mode}/{c}"] = attempt(
                lambda c=c, name=name: stored(
                    spark,
                    f"MERGE INTO {name} USING (SELECT g, avg({c}) AS m {where}) s "
                    f"ON {name}.k = s.g WHEN MATCHED THEN UPDATE SET n = s.m, w = s.m "
                    "WHEN NOT MATCHED THEN INSERT (k, n, w) VALUES (s.g, s.m, s.m)",
                    f"SELECT k, n, w FROM {name}",
                    name,
                )
            )
    return found


def measure_all(spark: Any, sf: Any, window: Any, catalog: str) -> dict[str, Any]:
    found: dict[str, Any] = {}
    for key, sql in sql_cells().items():
        found[key] = attempt(lambda sql=sql: answer_of(spark.sql(sql)))
    for key, build in frame_cells(sf, window).items():
        found[key] = attempt(lambda build=build: answer_of(build(spark.table("t"))))
    found.update(iceberg_cells(spark, catalog))
    return found
