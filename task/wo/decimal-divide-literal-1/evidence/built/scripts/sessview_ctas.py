import importlib
import json
import sys
import tempfile
from pathlib import Path

engine, ansi, probes, wh_parent, jar, out = sys.argv[1:7]
sys.path.insert(0, probes)
lib = importlib.import_module("lib")

ROWS = [["1", "7.25"], ["2", "-7.25"], ["3", None], ["4", "0.06"], ["5", "12345678.91"]]
SOURCES = {
    "sessview": "default.sv_h",
    "tempview": "tv_h",
}


def stored_type(table_dir: Path) -> list:
    metadata = sorted((table_dir / "metadata").glob("*.metadata.json"))[-1]
    found = json.loads(metadata.read_text())
    schema = next(s for s in found["schemas"] if s["schema-id"] == found["current-schema-id"])
    return [[field["name"], field["type"]] for field in schema["fields"]]


wh = tempfile.mkdtemp(prefix=f"ctas-{engine}-{ansi}-", dir=wh_parent)
s, sf, _, prov = lib.session(engine, ansi, wh, jar, {})
raw = s.createDataFrame(ROWS, "id STRING, d10_2 STRING")
typed = raw.selectExpr("CAST(id AS INT) AS id", "CAST(d10_2 AS DECIMAL(10,2)) AS d10_2")
typed.write.mode("overwrite").parquet(f"{wh}/pq_v")
s.read.parquet(f"{wh}/pq_v").createOrReplaceTempView("v")
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns").collect()
s.sql("CREATE TABLE lc.ns.vt USING iceberg AS SELECT * FROM v").collect()
s.sql("CREATE VIEW default.sv_h AS SELECT id, d10_2 / 2 AS h FROM lc.ns.vt").collect()
s.sql("CREATE OR REPLACE TEMPORARY VIEW tv_h AS SELECT id, d10_2 / 2 AS h FROM lc.ns.vt").collect()
records = [dict(key="provenance", ansi=ansi, **prov)]
for scope, source in SOURCES.items():
    select = f"SELECT id, h / 2 AS v FROM {source}"
    table = f"ctas_{scope}"
    records.append(
        {
            "key": f"hdiv/{scope}/select",
            "sql": select,
            "result": lib.attempt(lambda q=select: lib.answer_of(s.sql(q))),
        }
    )
    statement = f"CREATE TABLE lc.ns.{table} USING iceberg AS {select}"
    record = {"key": f"hdiv/{scope}/ctas", "sql": statement}
    record["statement"] = lib.attempt(lambda q=statement: s.sql(q).collect() and "ok") or "ok"
    record["metadata_type"] = lib.attempt(lambda t=table: stored_type(Path(wh) / "ns" / t))
    record["read_type"] = lib.attempt(
        lambda t=table: [f.dataType.simpleString() for f in s.table(f"lc.ns.{t}").schema.fields]
    )
    record["result"] = lib.attempt(lambda t=table: lib.answer_of(s.sql(f"SELECT * FROM lc.ns.{t}")))
    records.append(record)
with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
print(engine, ansi, len(records), flush=True)
