import json
import sys
import tempfile
from pathlib import Path

import cells
import run_cells_lib as lib

engine, ansi, wh_parent, jar, out = sys.argv[1:6]
wh = tempfile.mkdtemp(prefix=f"side-{engine}-{ansi}-", dir=wh_parent)
s, sf, prov = lib.session(engine, ansi, wh, jar)
records = [dict(key="provenance", ansi=ansi, **prov)]
base = s.range(8).selectExpr(*cells.setup_exprs())
path = str(Path(wh) / "pq_t")
base.filter("id <= 6").write.parquet(path)
s.read.parquet(path).createOrReplaceTempView("t")

SQL = {
    "using_same_type": (
        "SELECT k FROM (SELECT a AS k FROM t) FULL OUTER JOIN (SELECT a AS k FROM t) USING (k)"
    ),
    "using_aliased": (
        "SELECT coalesce(l.k, r.k) AS k FROM (SELECT a AS k FROM t) l FULL OUTER JOIN "
        "(SELECT c AS k FROM t) r ON l.k = r.k"
    ),
    "nvl_same": "SELECT id, nvl(c, c) AS r FROM t",
    "nvl_c0c": "SELECT id, nvl(c, c0) AS r FROM t",
    "ifnull_c0c": "SELECT id, ifnull(c, c0) AS r FROM t",
    "nullif_c0c": "SELECT id, nullif(c0, c) AS r FROM t",
    "except_c0c": "SELECT c0 AS v FROM t EXCEPT SELECT c FROM t",
    "avg_k28": "SELECT avg(k28) AS m FROM t WHERE id IN (1, 3, 5)",
    "avg_c": "SELECT avg(c) AS m FROM t WHERE id IN (1, 2, 6)",
    "lag_literal_default": "SELECT id, lag(a, 1, 0) OVER (ORDER BY id) AS r FROM t",
    "lag_same_type_default": "SELECT id, lag(a, 1, a) OVER (ORDER BY id) AS r FROM t",
    "range_same_type": (
        "SELECT id, count(*) OVER (ORDER BY c RANGE BETWEEN CAST('0.5' AS DECIMAL(10,2)) "
        "PRECEDING AND CURRENT ROW) AS n FROM t"
    ),
    "union_nested_cd_a": (
        "SELECT v FROM (SELECT c AS v FROM t UNION ALL SELECT d FROM t) UNION ALL SELECT a FROM t"
    ),
    "union_nested_a_cd_paren": (
        "SELECT a AS v FROM t UNION ALL (SELECT c FROM t UNION ALL SELECT d FROM t)"
    ),
    "union_flat_a_c_d": "SELECT a AS v FROM t UNION ALL SELECT c FROM t UNION ALL SELECT d FROM t",
}
for key, sql in SQL.items():
    records.append(
        {
            "key": f"side/{key}",
            "sql": sql,
            "result": lib.attempt(lambda q=sql: lib.answer_of(s.sql(q))),
        }
    )


def chained() -> object:
    tab = s.table("t")
    return lib.answer_of(
        tab.select(sf.col("c").alias("v"))
        .union(tab.select(sf.col("d").alias("v")))
        .union(tab.select(sf.col("a").alias("v")))
    )


records.append({"key": "side/df_union_chain_c_d_a", "result": lib.attempt(chained)})
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns").collect()
m = "lc.ns.side_update"
s.sql(
    f"CREATE TABLE {m} (id INT, a DECIMAL(38,0), c DECIMAL(10,2), a2 DECIMAL(38,2)) USING iceberg"
).collect()
s.sql(f"INSERT INTO {m} SELECT id, a, c, CAST(NULL AS DECIMAL(38,2)) FROM t").collect()
for key, stmt in (
    (
        "update_set_coalesce_same",
        f"UPDATE {m} SET a2 = coalesce(CASE WHEN id % 2 = 0 THEN NULL ELSE a END, a)",
    ),
    ("update_set_case_ac", f"UPDATE {m} SET a2 = CASE WHEN id % 2 = 0 THEN a ELSE c END"),
    ("update_set_coalesce_ac", f"UPDATE {m} SET a2 = coalesce(c, a)"),
):
    st = lib.attempt(lambda q=stmt: s.sql(q).collect())
    records.append(
        {
            "key": f"side/{key}",
            "sql": stmt,
            "statement": st if isinstance(st, str) else "ok",
            "result": lib.attempt(lambda: lib.answer_of(s.sql(f"SELECT id, a2 FROM {m}"))),
        }
    )
key = cells.LEGACY_KEY
for label, run in (
    ("legacy_get_default", lambda: s.conf.get(key, "unset")),
    ("legacy_set_sql", lambda: s.sql(f"SET {key}=true").collect() and "ok"),
    ("legacy_get_after_set", lambda: s.conf.get(key, "unset")),
    ("legacy_reset_sql", lambda: s.sql(f"RESET {key}").collect() and "ok"),
    ("legacy_get_after_reset", lambda: s.conf.get(key, "unset")),
):
    res = lib.attempt(run)
    records.append({"key": f"side/{label}", "result": res if res not in ([], None) else "ok"})
with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
print(engine, ansi, len(records))
