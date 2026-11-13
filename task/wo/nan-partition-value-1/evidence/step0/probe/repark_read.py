import json
import sys
from pathlib import Path

import repark
from repark import ReparkSession

warehouse = Path(sys.argv[1]).resolve()
out = Path(sys.argv[2])
records = [{"native": repark._native.__file__}]
session = (
    ReparkSession.builder.appName("nan-partition-step0-read")
    .config("spark.sql.catalog.hd", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.hd.type", "hadoop")
    .config(
        "spark.sql.catalog.hd.warehouse", str(warehouse.parent / (warehouse.name + "-repark-adopt"))
    )
    .getOrCreate()
)

session.sql("CREATE NAMESPACE IF NOT EXISTS hd.ns")


def run(cid: str, text: str) -> None:
    try:
        rows = [list(r) for r in session.sql(text).collect()]
        rec = {
            "cell": cid,
            "sql": text,
            "rows": json.loads(json.dumps(rows, default=str).replace("NaN", '"NaN"')),
        }
    except Exception as exc:
        rec = {"cell": cid, "sql": text, "error": f"{type(exc).__name__}: {str(exc)[:300]}"}
    records.append(rec)
    print(json.dumps(rec, default=str))


for table in sorted(p.name for p in (warehouse / "ns").iterdir()):
    ty = table.split("_")[1].upper()
    kind = table.split("_", 2)[2]
    name = f"hd.ns.{table}"
    latest = max(
        (warehouse / "ns" / table / "metadata").glob("v*.metadata.json"),
        key=lambda p: int(p.name[1:].split(".")[0]),
    )
    session.sql(
        f"CALL hd.system.register_table(table => 'ns.{table}', metadata_file => '{latest}')"
    ).collect()
    tag = f"{ty}|{kind}|repark-reads-spark"
    run(f"rows|{tag}", f"SELECT id, d FROM {name} ORDER BY id")
    if ty in ("DOUBLE", "FLOAT"):
        run(f"isnan|{tag}", f"SELECT id FROM {name} WHERE isnan(d) ORDER BY id")
        run(f"eqnan|{tag}", f"SELECT id FROM {name} WHERE d = CAST('NaN' AS {ty}) ORDER BY id")
        run(f"gt1|{tag}", f"SELECT id FROM {name} WHERE d > 1.0 ORDER BY id")
        run(f"isnull|{tag}", f"SELECT id FROM {name} WHERE d IS NULL ORDER BY id")
        run(f"count|{tag}", f"SELECT count(*) FROM {name}")
    if kind != "flat":
        run(f"files|{tag}", f"SELECT partition, record_count FROM {name}.files")
out.write_text("".join(json.dumps(r, default=str) + "\n" for r in records))
session.stop()
