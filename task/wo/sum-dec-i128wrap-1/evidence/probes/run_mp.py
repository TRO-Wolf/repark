import json
import sys
import tempfile
from pathlib import Path

import cells
import lib

engine, ansi, wh_parent, jar, out = sys.argv[1:6]
wh = tempfile.mkdtemp(prefix=f"wh-mp-{engine}-{ansi}-", dir=wh_parent)
extra = {"spark.sql.files.openCostInBytes": "134217728"}
s, sf, Window, prov = lib.session(engine, ansi, wh, jar, extra)
records: list = [dict(key="provenance", ansi=ansi, conf="mp", **prov)]
records.append(
    {
        "key": "conf/get/spark.sql.files.openCostInBytes",
        "result": lib.attempt(lambda: s.conf.get("spark.sql.files.openCostInBytes", "unset")),
    }
)


def make_table(name: str, names: list, rows: list, types: dict, parts: int) -> None:
    schema = ", ".join(f"{n} STRING" for n in names)
    raw = s.createDataFrame(rows, schema)
    typed = raw.selectExpr(*[f"CAST({n} AS {types[n]}) AS {n}" for n in names])
    path = str(Path(wh) / f"pq_{name}")
    for k in range(parts):
        typed.where(f"f = {k}").write.mode("append").parquet(path)
    s.read.parquet(path).createOrReplaceTempView(name)
    records.append({"key": f"data/{name}_files", "result": len(list(Path(path).glob("*.parquet")))})
    if engine == "spark":
        records.append(
            {
                "key": f"data/{name}_partitions",
                "result": lib.attempt(lambda: s.table(name).rdd.getNumPartitions()),
            }
        )


sn, sr = cells.s_rows()
make_table("s4", sn, sr, cells.s_types(), 4)
p2_names, p2_data = cells.p2_rows()
make_table("p2", p2_names, p2_data, cells.p2_types(), 2)
for key, fam, pos, col, g, table, sql in cells.sum_sql_cells():
    if table not in ("s4", "p2"):
        continue
    records.append(
        {
            "key": "mp/" + key,
            "family": fam,
            "door": "sql",
            "pos": "mp_" + pos,
            "col": col,
            "group": g,
            "result": lib.attempt(lambda sql=sql: lib.answer_of(s.sql(sql))),
        }
    )
with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
print(engine, ansi, len(records), flush=True)
