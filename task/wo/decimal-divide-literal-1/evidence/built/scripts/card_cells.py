import importlib
import json
import sys
import tempfile
from pathlib import Path

engine, ansi, probes, wh_parent, jar, out = sys.argv[1:7]
sys.path.insert(0, probes)
lib = importlib.import_module("lib")

ROWS = [["1", "7.25", "3"], ["2", "-7.25", "3"], ["3", None, "3"], ["4", "12345678.91", "3"]]
WRITTEN = {
    "written_cast/mul_dec20": "d10_2 * CAST(3 AS DECIMAL(20,0))",
    "written_cast/add_dec20": "d10_2 + CAST(3 AS DECIMAL(20,0))",
    "written_cast/div_dec20": "d10_2 / CAST(3 AS DECIMAL(20,0))",
    "written_cast/mul_dec10": "d10_2 * CAST(3 AS DECIMAL(10,0))",
}
PICK = {
    "runtime_set_pmp/mul": "d10_2 * 3",
    "runtime_set_pmp/div": "d10_2 / 3",
    "runtime_set_pmp/pct": "d10_2 % 3",
    "runtime_set_pmp/mod": "mod(d10_2, 3)",
}
PICK_KEY = "spark.sql.legacy.literal.pickMinimumPrecision"


def measure(s: object, cells: dict, prefix: str) -> list:
    found = []
    for key, e in cells.items():
        sql = f"SELECT id, {e} AS q FROM v"
        result = lib.attempt(lambda q=sql: lib.answer_of(s.sql(q)))
        found.append({"key": f"{prefix}{key}", "sql": sql, "result": result})
    return found


wh = tempfile.mkdtemp(prefix=f"card-{engine}-{ansi}-", dir=wh_parent)
s, sf, _, prov = lib.session(engine, ansi, wh, jar, {})
raw = s.createDataFrame(ROWS, "id STRING, d10_2 STRING, bi STRING")
typed = raw.selectExpr(
    "CAST(id AS INT) AS id", "CAST(d10_2 AS DECIMAL(10,2)) AS d10_2", "CAST(bi AS INT) AS bi"
)
typed.write.mode("overwrite").parquet(f"{wh}/pq_v")
s.read.parquet(f"{wh}/pq_v").createOrReplaceTempView("v")
records = [dict(key="provenance", ansi=ansi, **prov)]
records += measure(s, WRITTEN, "")
records += measure(s, PICK, "default/")
records.append(
    {
        "key": "setup/set",
        "result": lib.attempt(lambda: s.sql(f"SET {PICK_KEY}=false").collect() and "ok"),
    }
)
records.append({"key": "setup/read_back", "result": lib.attempt(lambda: s.conf.get(PICK_KEY))})
records += measure(s, PICK, "")
with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
print(engine, ansi, len(records), flush=True)
