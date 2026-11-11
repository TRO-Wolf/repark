import contextlib
import json
import shutil
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import probe_family as pf

leg = sys.argv[1]
original = pf.session


def session(leg_name: Any, warehouse: Any) -> Any:
    spark = original(leg_name, warehouse)
    return spark


if leg != "spark":
    from repark import ReparkSession

    def session(leg_name: Any, warehouse: Any) -> Any:
        return (
            ReparkSession.builder.appName("family-v3-" + leg_name)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.catalog.hd.type", "hadoop")
            .config("spark.sql.catalog.hd.warehouse", warehouse)
            .config("repark.sql.allowCreateFormatVersion3", "true")
            .getOrCreate()
        )


wh = pf.WORK / (leg + "-v3") / "wh"
shutil.rmtree(wh, ignore_errors=True)
wh.mkdir(parents=True, exist_ok=True)
spark = session(leg, str(wh))
for _name, statement in pf.view_statements(leg):
    with contextlib.suppress(Exception):
        spark.sql(statement).collect()
spark.sql("CREATE NAMESPACE IF NOT EXISTS hd.vp").collect()
cells = [c for c in pf.gen_dml_cells() if c.get("fmt", "").startswith("v3")]
with (pf.RAW / f"{leg}-v3.jsonl").open("w") as handle:
    for cell in cells:
        result = pf.run_dml(spark, cell)
        handle.write(json.dumps(result, default=str) + "\n")
        handle.flush()
spark.stop()
print(leg, "v3 cells", len(cells))
