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
SPLIT_SHAPES: dict[int, list[list[str]]] = {
    21: [["M"], ["1"], ["-1"]],
    22: [["M"], ["1"]],
    23: [["M", "5"], ["-M"]],
    24: [["M", "-M"], ["M", "-M", "5"]],
    25: [["M"], ["-M"], ["5"]],
}
REPEATS = 20
MERGE_TOTAL_CLASS = ("21",)
KEPT_DIVERGENCE = {
    f"{pos}/{c}": g
    for c in WIDE
    for pos, g in (
        ("sql/sticky/g10", 10),
        ("sql/sticky/g11", 11),
        ("sql/try_grouped/g10", 10),
        ("df/agg_dict/g10", 10),
    )
}
CONDITION_CLASS = {f"sql/union/g4_14/{c}": "NUMERIC_VALUE_OUT_OF_RANGE" for c in WIDE}
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
    names, values = split_rows()
    raw = spark.createDataFrame(values, ", ".join(f"{n} STRING" for n in names))
    typed = raw.selectExpr(*[f"CAST({n} AS {kinds.get(n, 'INT')}) AS {n}" for n in names])
    for part in range(3):
        files = typed.where(f"f = {part}").coalesce(1)
        files.write.mode("append").parquet(f"{root}/pq_t4")
    spark.read.parquet(f"{root}/pq_t4").createOrReplaceTempView("t4")


def split_rows() -> tuple[list[str], list[list[str]]]:
    names = ["g", "id", "f"] + [col(p, s) for p, s in COLUMNS if p == 38]
    out: list[list[str]] = []
    for part in range(3):
        for g, files in SPLIT_SHAPES.items():
            for token in files[part] if part < len(files) else []:
                row = [str(g), str(len(out) + 100), str(part)]
                row += [text(unscaled(token, p), s) for p, s in COLUMNS if p == 38]
                out.append(row)
    return names, out


def class_of(key: str) -> str:
    if key in KEPT_DIVERGENCE:
        return "kept-divergence"
    if key in CONDITION_CLASS:
        return "condition-class"
    return "spark"


def kept_exact(key: str, types: list[str]) -> dict[str, Any]:
    g, c = KEPT_DIVERGENCE[key], key.rsplit("/", 1)[1]
    p, s = (int(x) for x in c[1:].split("_"))
    total = sum(unscaled(token, p) for token in SHAPES[g])
    return {"rows": [f"{g}|{text(total, s)}"], "types": types}


def exact_row(g: int, c: str) -> str:
    p, s = (int(x) for x in c[1:].split("_"))
    total = sum(unscaled(token, p) for files in SPLIT_SHAPES[g] for token in files)
    return f"{g}|{text(total, s)}"


def repeat_sql() -> dict[str, str]:
    found: dict[str, str] = {}
    for c in WIDE:
        for g in SPLIT_SHAPES:
            found[f"final/sum/g{g}/{c}"] = (
                f"SELECT g, sum({c}) AS m FROM t4 WHERE g = {g} GROUP BY g"
            )
            found[f"final/try_sum/g{g}/{c}"] = (
                f"SELECT g, try_sum({c}) AS m FROM t4 WHERE g = {g} GROUP BY g"
            )
        found[f"final/distinct/g10/{c}"] = (
            f"SELECT g, sum(DISTINCT {c}) AS m FROM t WHERE g = 10 GROUP BY g"
        )
    return found


def measure_repeats(spark: Any, repeats: int) -> dict[str, list[Any]]:
    found: dict[str, list[Any]] = {}
    for key, sql in repeat_sql().items():
        found[key] = [attempt(lambda sql=sql: answer_of(spark.sql(sql))) for _ in range(repeats)]
    return found


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


TARGET_PARTITIONS = (2, 4, 8)
SPREAD_COLUMNS = [(19, 0), (20, 0), (38, 0)]
SPREAD_FILES = {"a": "M", "b": "5", "c": "-M"}
SPREAD_ROWS = 300_000
SPREAD_GROUPS = 3_000


def spread_hits(k: int) -> list[bool]:
    rounds = SPREAD_ROWS // SPREAD_GROUPS
    return [
        i // SPREAD_GROUPS == ((i % SPREAD_GROUPS) * (k + 3) * 2654435761 >> 7) % rounds
        for i in range(SPREAD_ROWS)
    ]


def write_spread_tables(root: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    (root / "big").mkdir()
    (root / "t4x").mkdir()
    for k, (name, token) in enumerate(SPREAD_FILES.items()):
        hits = spread_hits(k)
        table = {"g": pa.array([i % SPREAD_GROUPS for i in range(SPREAD_ROWS)], pa.int32())}
        for p, s in SPREAD_COLUMNS:
            special, zero = text(unscaled(token, p), s), text(0, s)
            values = pa.array([special if hit else zero for hit in hits], pa.string())
            table[col(p, s)] = values.cast(pa.decimal128(p, s))
        pq.write_table(pa.table(table), root / "big" / f"{name}.parquet")
        part = [(g, x) for g, files in SPLIT_SHAPES.items() if k < len(files) for x in files[k]]
        small = {"g": pa.array([g for g, _ in part], pa.int32())}
        for p, s in SPREAD_COLUMNS:
            texts = [text(unscaled(token, p), s) for _, token in part]
            small[col(p, s)] = pa.array(texts, pa.string()).cast(pa.decimal128(p, s))
        pq.write_table(pa.table(small), root / "t4x" / f"{name}.parquet")


def spread_sql() -> dict[str, str]:
    found: dict[str, str] = {}
    for p, s in SPREAD_COLUMNS:
        c = col(p, s)
        for agg in ("sum", "try_sum"):
            found[f"big/grouped/{agg}/{c}"] = f"SELECT g, {agg}({c}) AS m FROM big GROUP BY g"
            found[f"big/ungrouped/{agg}/{c}"] = f"SELECT {agg}({c}) AS m FROM big"
            for g in SPLIT_SHAPES:
                found[f"t4x/g{g}/grouped/{agg}/{c}"] = (
                    f"SELECT g, {agg}({c}) AS m FROM t4x WHERE g = {g} GROUP BY g"
                )
                found[f"t4x/g{g}/ungrouped/{agg}/{c}"] = (
                    f"SELECT {agg}({c}) AS m FROM t4x WHERE g = {g}"
                )
    return found


def spread_exact(key: str, ansi: str) -> Any:
    table, *rest = key.split("/")
    agg, c = rest[-2], rest[-1]
    p, s = (int(x) for x in c[1:].split("_"))
    result = f"decimal({min(38, p + 10)},{s})"
    if table == "big":
        g, total = None, 5 * (SPREAD_GROUPS if rest[0] == "ungrouped" else 1)
    else:
        g = int(rest[0][1:])
        total = sum(unscaled(token, p) for files in SPLIT_SHAPES[g] for token in files)
    grouped = "grouped" in rest
    if abs(total) >= 10 ** min(38, p + 10):
        if ansi == "true" and agg == "sum":
            return "ARITHMETIC_OVERFLOW"
        value = "null"
    else:
        value = text(total, s)
    if table == "big" and grouped:
        return {"rows": [f"{i}|{value}" for i in range(SPREAD_GROUPS)], "types": ["int", result]}
    if grouped:
        return {"rows": [f"{g}|{value}"], "types": ["int", result]}
    return {"rows": [value], "types": [result]}


def register_spread_tables(spark: Any, root: Path) -> None:
    for name in ("big", "t4x"):
        spark.read.parquet(str(root / name)).createOrReplaceTempView(name)
