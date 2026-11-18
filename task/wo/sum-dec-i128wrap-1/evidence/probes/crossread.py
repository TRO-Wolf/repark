import json
import sys
from pathlib import Path

import lib

ansi, jar, out = sys.argv[1:4]
warehouses = sys.argv[4:]
s, sf, Window, prov = lib.session("spark", ansi, warehouses[0], jar, {})
records: list = [dict(key="provenance", ansi=ansi, **prov)]
for n, wh in enumerate(warehouses):
    catalog = f"rc{n}"
    s.conf.set(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
    s.conf.set(f"spark.sql.catalog.{catalog}.type", "hadoop")
    s.conf.set(f"spark.sql.catalog.{catalog}.warehouse", wh)
    writer = Path(wh).name
    for table in sorted(p.name for p in (Path(wh) / "db").iterdir() if p.is_dir()):
        name = f"{catalog}.db.{table}"
        queries = {
            "all": f"SELECT g, s FROM {name}",
            "g1": f"SELECT g, s FROM {name} WHERE g = 1",
            "count": f"SELECT count(*) FROM {name}",
            "positive": f"SELECT g FROM {name} WHERE s > 0",
        }
        for label, text in queries.items():
            records.append(
                {
                    "key": f"cross/{writer}/{table}/{label}",
                    "result": lib.attempt(lambda text=text: lib.answer_of(s.sql(text))),
                }
            )
Path(out).write_text("".join(json.dumps(r) + "\n" for r in records))
print("done", len(records), flush=True)
