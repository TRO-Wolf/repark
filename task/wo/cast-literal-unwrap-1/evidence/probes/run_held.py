import json
import sys
import time
from collections.abc import Callable
from pathlib import Path

import cells

engine, ansi, out = sys.argv[1:4]
if engine == "spark":
    import pyspark
    from pyspark.sql import SparkSession as Builder

    builder = Builder.builder.master("local[1]").config("spark.ui.enabled", "false")
    prov = {"engine": "spark", "version": pyspark.__version__}
else:
    import repark._native as native
    from repark import ReparkSession as Builder

    builder = Builder.builder.appName("cast-unwrap-held")
    prov = {
        "engine": "repark",
        "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
    }
    print("native", native.__file__, flush=True)
s = (
    builder.config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", ansi)
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()
)
records = [dict(key="provenance", ansi=ansi, **prov)]
HELD_OPS = ("eq", "lt", "gt", "ns")


def answer(run: Callable[[], object]) -> object:
    try:
        return run()
    except Exception as e:
        return f"ERR:{type(e).__name__}:{' '.join(str(e).split())[:200]}"


start = time.time()
for kind in cells.KINDS:
    view = f"v_{kind.name}"
    s.sql(cells.select_sql(kind)).createOrReplaceTempView(view)
    for ci, const in enumerate(kind.consts[:2]):
        held = f"h_{kind.name}_{ci}"
        s.sql(
            f"SELECT id, c, element_at(array({const}, {const}), CAST(id % 2 + 1 AS INT)) AS k "
            f"FROM {view}"
        ).createOrReplaceTempView(held)
        for op in HELD_OPS:
            pred = f"{cells.cast_expr(kind)} {cells.OPS[op]} k"
            sql = f"SELECT id FROM {held} WHERE {pred}"
            records.append(
                {
                    "key": f"{kind.name}/view/where/c{ci}/{op}/r",
                    "kind": kind.name,
                    "sql": sql,
                    "result": answer(lambda q=sql: sorted(int(r[0]) for r in s.sql(q).collect())),
                }
            )
    print(kind.name, len(records), round(time.time() - start, 1), flush=True)
with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
print(engine, ansi, len(records), round(time.time() - start, 1))
