import json
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path

import cells
import run_cells_lib as lib

engine, ansi, wh_parent, jar, out = sys.argv[1:6]
wh = tempfile.mkdtemp(prefix=f"rev-{engine}-{ansi}-", dir=wh_parent)
s, sf, prov = lib.session(engine, ansi, wh, jar)
records: list = [dict(key="provenance", ansi=ansi, **prov)]
base = s.range(8).selectExpr(*cells.setup_exprs())
for name, cond in (("t", "id <= 6"), ("tx", "id >= 7")):
    path = str(Path(wh) / f"pq_{name}")
    base.filter(cond).write.parquet(path)
    s.read.parquet(path).createOrReplaceTempView(name)

W30 = "CAST('1.000000000000000000000000000049' AS DECIMAL(38,30))"
SQL = {
    "m1/map_concat_keys_id1": (
        "SELECT map_concat(map(a, 'x'), map(c, 'y')) AS r FROM t WHERE id = 1"
    ),
    "m1/map_concat_keys_all": (
        "SELECT id, map_concat(map(a, 'x'), map(c, 'y')) AS r FROM t WHERE id <= 5"
    ),
    "m1/map_concat_keys_apart": (
        "SELECT id, map_concat(map(a, 'x'), map(c + 5, 'y')) AS r FROM t WHERE id <= 5"
    ),
    "m1/create_map_keys_id1": "SELECT map(a, 'x', c, 'y') AS r FROM t WHERE id = 1",
    "m1/create_map_keys_apart": "SELECT id, map(a, 'x', c + 5, 'y') AS r FROM t WHERE id <= 5",
    "m1/map_keys_of_concat": (
        "SELECT id, map_keys(map_concat(map(a, 'x'), map(c + 5, 'y'))) AS r FROM t WHERE id <= 5"
    ),
    "m2/using_inner": (
        "SELECT k, li, ri FROM (SELECT id AS li, a AS k FROM t) "
        "JOIN (SELECT id AS ri, c AS k FROM t) USING (k)"
    ),
    "m2/using_left": (
        "SELECT k, li, ri FROM (SELECT id AS li, a AS k FROM t) "
        "LEFT JOIN (SELECT id AS ri, c AS k FROM t) USING (k)"
    ),
    "m2/using_full_distinct_names": (
        "SELECT k, li, ri FROM (SELECT id AS li, a AS k FROM t) "
        "FULL OUTER JOIN (SELECT id AS ri, c AS k FROM t) USING (k)"
    ),
    "m2/using_inner_dup_names": (
        "SELECT k FROM (SELECT a AS k FROM t) JOIN (SELECT c AS k FROM t) USING (k)"
    ),
    "m2/using_full_dup_names": (
        "SELECT k FROM (SELECT a AS k FROM t) FULL OUTER JOIN (SELECT c AS k FROM t) USING (k)"
    ),
    "m2/using_full_dup_names_same_type": (
        "SELECT k FROM (SELECT a AS k FROM t) FULL OUTER JOIN (SELECT a AS k FROM t) USING (k)"
    ),
    "m2/using_inner_h_i64": (
        "SELECT k, li, ri FROM (SELECT id AS li, i64 AS k FROM t) "
        "JOIN (SELECT id AS ri, h AS k FROM t) USING (k)"
    ),
    "m2/using_inner_aliased": (
        "SELECT k, li, ri FROM (SELECT id AS li, a AS k FROM t) l "
        "JOIN (SELECT id AS ri, c AS k FROM t) r USING (k)"
    ),
    "m2/using_left_aliased": (
        "SELECT k, li, ri FROM (SELECT id AS li, a AS k FROM t) l "
        "LEFT JOIN (SELECT id AS ri, c AS k FROM t) r USING (k)"
    ),
    "m2/using_full_aliased": (
        "SELECT k, li, ri FROM (SELECT id AS li, a AS k FROM t) l "
        "FULL OUTER JOIN (SELECT id AS ri, c AS k FROM t) r USING (k)"
    ),
    "m2/using_full_aliased_dup_names": (
        "SELECT k FROM (SELECT a AS k FROM t) l FULL OUTER JOIN (SELECT c AS k FROM t) r USING (k)"
    ),
    "m2/using_inner_aliased_same_type": (
        "SELECT k, li, ri FROM (SELECT id AS li, c AS k FROM t) l "
        "JOIN (SELECT id AS ri, c AS k FROM t) r USING (k)"
    ),
    "m2/using_inner_aliased_h_i64": (
        "SELECT k, li, ri FROM (SELECT id AS li, i64 AS k FROM t) l "
        "JOIN (SELECT id AS ri, h AS k FROM t) r USING (k)"
    ),
    "m1/map_concat_same_type_control": (
        "SELECT id, map_concat(map(a, 'x'), map(a + 5, 'y')) AS r FROM t WHERE id <= 5"
    ),
    "m1/map_value_null_key_control": "SELECT id, map(a, 'x') AS r FROM t WHERE id <= 5",
    "m3/lit_in_int_lit": f"SELECT id, {W30} IN (1) AS r FROM t",
    "m3/lit_in_id": f"SELECT id, {W30} IN (id) AS r FROM t",
    "m3/lit_eq_id": f"SELECT id, {W30} = id AS r FROM t",
    "m3/w30_in_1": "SELECT id, w30 IN (1) AS r FROM t",
    "m3/w30_in_1_where": "SELECT id FROM t WHERE w30 IN (1)",
    "m3/w30_in_1_2": "SELECT id FROM t WHERE w30 IN (1, 2)",
    "m3/w30_not_in_1": "SELECT id FROM t WHERE w30 NOT IN (1)",
    "m3/w30_eq_1": "SELECT id FROM t WHERE w30 = 1",
    "m3/w30_in_big1": "SELECT id FROM t WHERE w30 IN (CAST(1 AS BIGINT))",
    "m3/w30_in_i32": "SELECT id FROM t WHERE w30 IN (i32)",
    "m3/w30_in_subq_i32": "SELECT id FROM t WHERE w30 IN (SELECT i32 FROM t)",
    "m3/w36_in_1": "SELECT id FROM t WHERE w36 IN (1)",
    "m3/w36_eq_1": "SELECT id FROM t WHERE w36 = 1",
    "m3/h_in_1": "SELECT id FROM t WHERE h IN (1)",
    "m3/w30_case_when_1": "SELECT id, CASE w30 WHEN 1 THEN 'm' ELSE 'n' END AS r FROM t",
    "m3/w30_between_1_1": "SELECT id FROM t WHERE w30 BETWEEN 1 AND 1",
    "m4/intersect_distinct_ac": "SELECT a AS v FROM t INTERSECT DISTINCT SELECT c FROM t",
    "m4/except_distinct_ac": "SELECT a AS v FROM t EXCEPT DISTINCT SELECT c FROM t",
    "m4/intersect_ca": "SELECT c AS v FROM t INTERSECT SELECT a FROM t",
    "m4/except_ca": "SELECT c AS v FROM t EXCEPT SELECT a FROM t",
    "m4/union_distinct_ac": "SELECT a AS v FROM t UNION DISTINCT SELECT c FROM t",
    "m4/intersect_i64h": "SELECT i64 AS v FROM t INTERSECT SELECT h FROM t",
    "pin/struct_pos": (
        "SELECT named_struct('f', a, 'g', c) AS r FROM t WHERE id = 1 "
        "UNION ALL SELECT named_struct('f', c, 'g', a) FROM t WHERE id = 2"
    ),
    "pin/struct_cmp": "SELECT id FROM t WHERE named_struct('f', a) = named_struct('f', c)",
    "pin/max_case": "SELECT max(CASE WHEN id > 1 THEN a ELSE c END) AS r FROM t",
    "pin/min_case": "SELECT min(CASE WHEN id > 1 THEN a ELSE c END) AS r FROM t",
    "pin/case_in_agg": "SELECT sum(CASE WHEN id > 1 THEN a ELSE c END) AS r FROM t",
    "pin/sum_of_union": (
        "SELECT sum(v) AS r FROM (SELECT a AS v FROM t UNION ALL SELECT c AS v FROM t)"
    ),
    "pin/between_mix": "SELECT id FROM t WHERE CAST('1' AS DECIMAL(38,0)) BETWEEN c AND c",
    "pin/map_lit": "SELECT map('k', a, 'j', c) AS r FROM t WHERE id = 1",
    "pin/map_vals": "SELECT map_concat(map('x', a), map('y', c)) AS r FROM t WHERE id = 1",
    "pin/shield_fwd": "SELECT id FROM t WHERE c = CAST('1' AS DECIMAL(38,0))",
    "pin/shield_rev": "SELECT id FROM t WHERE CAST('1' AS DECIMAL(38,0)) = c",
    "pin/shield_a_fwd": "SELECT id FROM t WHERE a = CAST('1.25' AS DECIMAL(10,2))",
    "pin/shield_a_rev": "SELECT id FROM t WHERE CAST('1.25' AS DECIMAL(10,2)) = a",
    "pin/fold_rounded_literal": "SELECT id FROM t WHERE a = CAST(1.5 AS DECIMAL(10,2))",
}
for key, sql in SQL.items():
    records.append(
        {
            "key": f"rev/{key}",
            "group": "revision",
            "door": "sql",
            "pos": key.split("/")[0],
            "pair": key.split("/")[1],
            "sql": sql,
            "result": lib.attempt(lambda q=sql: lib.answer_of(s.sql(q))),
        }
    )


def tab() -> object:
    return s.table("t")


def by_name_missing() -> object:
    left = tab().select(sf.col("c").alias("v"))
    right = tab().select(sf.col("a").alias("v")).withColumn("extra", sf.lit(1))
    return lib.answer_of(left.unionByName(right, allowMissingColumns=True))


def df_join_on_name() -> object:
    left = tab().select(sf.col("id").alias("li"), sf.col("a").alias("k"))
    right = tab().select(sf.col("id").alias("ri"), sf.col("c").alias("k"))
    return lib.answer_of(left.join(right, "k").select("k", "li", "ri"))


def df_intersect() -> object:
    left = tab().select(sf.col("a").alias("v"))
    return lib.answer_of(left.intersect(tab().select(sf.col("c").alias("v"))))


def df_subtract() -> object:
    left = tab().select(sf.col("a").alias("v"))
    return lib.answer_of(left.subtract(tab().select(sf.col("c").alias("v"))))


def df_isin_int() -> object:
    return lib.answer_of(tab().filter(sf.col("w30").isin(1)).select("id"))


frames: dict[str, Callable[[], object]] = {
    "df_bynamemissing": by_name_missing,
    "df_join_on_name": df_join_on_name,
    "df_intersect": df_intersect,
    "df_subtract": df_subtract,
    "df_w30_isin_1": df_isin_int,
}
for key, run in frames.items():
    records.append(
        {
            "key": f"rev/df/{key}",
            "group": "revision",
            "door": "dataframe",
            "pos": "df",
            "pair": key,
            "result": lib.attempt(run),
        }
    )
with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
print(engine, ansi, len(records))
