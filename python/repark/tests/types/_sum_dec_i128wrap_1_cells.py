from __future__ import annotations

import re
from collections.abc import Callable
from decimal import Context, Decimal
from pathlib import Path
from typing import Any

WIDE_CONTEXT = Context(prec=120)
DECIMAL_TEXT = re.compile(r"-?\d+\.\d+")
COLUMNS = [(10, 2), (38, 0), (38, 38)]
WIDE = ("c38_0", "c38_38")
SHAPES: dict[int, list[str]] = {
    1: ["105", "255", "-10"],
    4: ["M"],
    5: ["M", "1"],
    6: ["M", "M"],
    7: ["M", "M", "M"],
    10: ["M", "M", "-M", "-M", "5"],
    11: ["M", "1", "-1"],
    14: ["1"],
}
ICE_MODES = {
    "cow": "'format-version'='2','write.merge.mode'='copy-on-write'",
    "mor": "'format-version'='2','write.merge.mode'='merge-on-read'",
}


def col(p: int, s: int) -> str:
    return f"c{p}_{s}"


def unscaled(token: str, p: int) -> int:
    largest = 10**p - 1
    named = {"M": largest, "-M": -largest}.get(token)
    return int(token) if named is None else named


def text(value: int, s: int) -> str:
    return format(Decimal(value).scaleb(-s, context=WIDE_CONTEXT), "f")


def rows() -> tuple[list[str], list[list[str]]]:
    names = ["g", "id"] + [col(p, s) for p, s in COLUMNS] + ["b"]
    out: list[list[str]] = []
    for g, shape in SHAPES.items():
        for token in shape:
            row = [str(g), str(len(out) + 1)]
            row += [text(unscaled(token, p), s) for p, s in COLUMNS]
            row.append(str(unscaled(token, 4)))
            out.append(row)
    return names, out


def types() -> dict[str, str]:
    found = {"g": "INT", "id": "INT", "b": "BIGINT"}
    for p, s in COLUMNS:
        found[col(p, s)] = f"DECIMAL({p},{s})"
    return found


def column_sql(c: str) -> dict[str, str]:
    window = "OVER (PARTITION BY g ORDER BY id"
    return {
        "single/g5": f"SELECT sum({c}) AS m FROM t WHERE g = 5",
        "single/g7": f"SELECT sum({c}) AS m FROM t WHERE g = 7",
        "grouped/g1_5_6_7": f"SELECT g, sum({c}) AS m FROM t WHERE g IN (1, 5, 6, 7) GROUP BY g",
        "union/g4_14": (
            f"SELECT sum(x) AS m FROM (SELECT {c} AS x FROM t WHERE g = 4 "
            f"UNION ALL SELECT {c} AS x FROM t WHERE g = 14)"
        ),
        "union_partial/g5_14": (
            f"SELECT sum(x) AS m FROM (SELECT {c} AS x FROM t WHERE g = 5 "
            f"UNION ALL SELECT {c} AS x FROM t WHERE g = 14)"
        ),
        "sticky/g10": f"SELECT g, sum({c}) AS m FROM t WHERE g = 10 GROUP BY g",
        "sticky/g11": f"SELECT g, sum({c}) AS m FROM t WHERE g = 11 GROUP BY g",
        "back_ungrouped/g10": f"SELECT sum({c}) AS m FROM t WHERE g = 10",
        "win_rows/g10": f"SELECT id, sum({c}) {window} ROWS 1 PRECEDING) AS m FROM t WHERE g = 10",
        "win_running/g10": (
            f"SELECT id, sum({c}) {window} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS m "
            "FROM t WHERE g = 10"
        ),
        "win_part/g10": f"SELECT id, sum({c}) OVER (PARTITION BY g) AS m FROM t WHERE g = 10",
        "rollup_total/g1_4": f"SELECT g, sum({c}) AS m FROM t WHERE g IN (1, 4) GROUP BY ROLLUP(g)",
        "cube_total/g1_4": f"SELECT g, sum({c}) AS m FROM t WHERE g IN (1, 4) GROUP BY CUBE(g)",
        "gsets_total/g1_4": (
            f"SELECT g, sum({c}) AS m FROM t WHERE g IN (1, 4) GROUP BY GROUPING SETS ((g), ())"
        ),
        "distinct_mixed/g5": (
            f"SELECT g, sum(DISTINCT {c}) AS m, count({c}) AS n FROM t WHERE g = 5 GROUP BY g"
        ),
        "distinct_mixed_ungrouped/g5": (
            f"SELECT sum(DISTINCT {c}) AS m, count({c}) AS n FROM t WHERE g = 5"
        ),
        "having_lt/g1_7": (f"SELECT g FROM t WHERE g IN (1, 7) GROUP BY g HAVING sum({c}) < 0"),
        "try_ungrouped/g10": f"SELECT try_sum({c}) AS m FROM t WHERE g = 10",
        "try_grouped/g10": f"SELECT g, try_sum({c}) AS m FROM t WHERE g = 10 GROUP BY g",
        "try_win_part/g10": (
            f"SELECT id, try_sum({c}) OVER (PARTITION BY g) AS m FROM t WHERE g = 10"
        ),
    }


def sql_cells() -> dict[str, str]:
    cells = {f"sql/{pos}/{c}": sql for c in WIDE for pos, sql in column_sql(c).items()}
    cells["sql/grouped/all/c10_2"] = "SELECT g, sum(c10_2) AS m FROM t GROUP BY g"
    cells["sql/win_rows/all/c10_2"] = (
        "SELECT id, sum(c10_2) OVER (PARTITION BY g ORDER BY id ROWS 1 PRECEDING) AS m FROM t"
    )
    cells["sql/bigint/grouped"] = "SELECT g, sum(b) AS m, try_sum(b) AS t FROM t GROUP BY g"
    return cells


def frame_cells(sf: Any, window: Any, engine: str) -> dict[str, Callable[[Any], Any]]:
    cells: dict[str, Callable[[Any], Any]] = {}
    for c in WIDE:

        def some(t: Any, groups: tuple[int, ...]) -> Any:
            return t.where(sf.col("g").isin(list(groups)))

        cells[f"df/F.sum/g1_5_6_7/{c}"] = lambda t, c=c: (
            some(t, (1, 5, 6, 7)).groupBy("g").agg(sf.sum(c).alias("m"))
        )
        cells[f"df/GroupedData.sum/g1_7/{c}"] = lambda t, c=c: some(t, (1, 7)).groupBy("g").sum(c)
        cells[f"df/agg_dict/g10/{c}"] = lambda t, c=c: some(t, (10,)).groupBy("g").agg({c: "sum"})
        cells[f"df/F.expr/g6/{c}"] = lambda t, c=c: (
            some(t, (6,)).groupBy("g").agg(sf.expr(f"sum({c})").alias("m"))
        )
        cells[f"df/selectExpr/g5/{c}"] = lambda t, c=c: some(t, (5,)).selectExpr(f"sum({c}) AS m")
        cells[f"df/pivot/g1_7/{c}"] = lambda t, c=c: (
            some(t, (1, 7)).groupBy().pivot("g", [1, 7]).agg(sf.sum(c))
        )
        cells[f"df/call_function/g7/{c}"] = lambda t, c=c: (
            some(t, (7,)).groupBy("g").agg(sf.call_function("sum", sf.col(c)).alias("m"))
        )
        cells[f"df/rollup_total/g1_4/{c}"] = lambda t, c=c: (
            some(t, (1, 4)).rollup("g").agg(sf.sum(c).alias("m"))
        )
        cells[f"df/cube_total/g1_4/{c}"] = lambda t, c=c: (
            some(t, (1, 4)).cube("g").agg(sf.sum(c).alias("m"))
        )
        if engine == "spark":
            cells[f"df/gsets_total/g1_4/{c}"] = lambda t, c=c: (
                some(t, (1, 4)).groupingSets([["g"], []], "g").agg(sf.sum(c).alias("m"))
            )
        else:
            cells[f"df/gsets_total/g1_4/{c}"] = lambda t, c=c: (
                some(t, (1, 4)).groupingSets("g").agg(sf.sum(c).alias("m"))
            )
        cells[f"df/try_sum_ungrouped/g10/{c}"] = lambda t, c=c: some(t, (10,)).agg(
            sf.try_sum(c).alias("m")
        )
        cells[f"df/window_rows/g10/{c}"] = lambda t, c=c: some(t, (10,)).select(
            "id",
            sf.sum(c).over(window.partitionBy("g").orderBy("id").rowsBetween(-1, 0)).alias("m"),
        )
    return cells


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


def make_tables(spark: Any, root: str) -> None:
    names, values = rows()
    kinds = types()
    raw = spark.createDataFrame(values, ", ".join(f"{n} STRING" for n in names))
    typed = raw.selectExpr(*[f"CAST({n} AS {kinds[n]}) AS {n}" for n in names])
    typed.coalesce(1).write.mode("overwrite").parquet(f"{root}/pq_t")
    spark.read.parquet(f"{root}/pq_t").createOrReplaceTempView("t")


def stored(spark: Any, statement: str, table: str) -> dict[str, Any]:
    result = attempt(lambda: spark.sql(statement).collect())
    return {
        "statement": result if isinstance(result, str) else "ok",
        "rows": attempt(lambda: answer_of(spark.sql(f"SELECT g, s FROM {table}"))),
    }


def iceberg_cells(spark: Any, catalog: str) -> dict[str, Any]:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.sumns").collect()
    found: dict[str, Any] = {}
    for c in WIDE:
        s = c.split("_")[1]
        wrap = f"SELECT g, sum({c}) AS s FROM t WHERE g IN (1, 7) GROUP BY g"
        name = f"{catalog}.sumns.ctas_wrap_{c}"
        found[f"ice/ctas/wrap/{c}"] = stored(
            spark, f"CREATE TABLE {name} USING iceberg AS {wrap}", name
        )
        name = f"{catalog}.sumns.insert_wrap_{c}"
        spark.sql(f"CREATE TABLE {name} (g INT, s DECIMAL(38,{s})) USING iceberg").collect()
        found[f"ice/insert/wrap/{c}"] = stored(spark, f"INSERT INTO {name} {wrap}", name)
        past = f"SELECT g, sum({c}) AS s FROM t WHERE g IN (1, 5, 6) GROUP BY g"
        for metrics in ("none", "counts"):
            name = f"{catalog}.sumns.insert_{metrics}_{c}"
            spark.sql(
                f"CREATE TABLE {name} (g INT, s DECIMAL(38,{s})) USING iceberg "
                f"TBLPROPERTIES ('write.metadata.metrics.default'='{metrics}')"
            ).collect()
            found[f"ice/insert/metrics_{metrics}/{c}"] = stored(
                spark, f"INSERT INTO {name} {past}", name
            )
    for mode, props in ICE_MODES.items():
        name = f"{catalog}.sumns.merge_{mode}"
        spark.sql(
            f"CREATE TABLE {name} (g INT, s DECIMAL(38,0)) USING iceberg TBLPROPERTIES ({props})"
        ).collect()
        spark.sql(f"INSERT INTO {name} VALUES (1, 0), (7, 0)").collect()
        found[f"ice/merge_{mode}/wrap/c38_0"] = stored(
            spark,
            f"MERGE INTO {name} USING (SELECT g, sum(c38_0) AS m FROM t WHERE g IN (1, 7) "
            f"GROUP BY g) src ON {name}.g = src.g WHEN MATCHED THEN UPDATE SET s = src.m "
            "WHEN NOT MATCHED THEN INSERT (g, s) VALUES (src.g, src.m)",
            name,
        )
    return found


def data_values(warehouse: Path) -> list[Decimal]:
    import pyarrow.parquet as pq

    found: list[Decimal] = []
    for path in sorted(warehouse.rglob("*.parquet")):
        if "/data/" not in path.as_posix():
            continue
        table = pq.read_table(path)
        if "s" in table.column_names:
            found.extend(value for value in table.column("s").to_pylist() if value is not None)
    return found


def measure_all(spark: Any, sf: Any, window: Any, catalog: str, engine: str) -> dict[str, Any]:
    found: dict[str, Any] = {}
    for key, sql in sql_cells().items():
        found[key] = attempt(lambda sql=sql: answer_of(spark.sql(sql)))
    for key, build in frame_cells(sf, window, engine).items():
        found[key] = attempt(lambda build=build: answer_of(build(spark.table("t"))))
    found.update(iceberg_cells(spark, catalog))
    return found
