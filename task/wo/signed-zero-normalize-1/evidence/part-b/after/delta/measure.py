import json
import os
import struct
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

eng, master, out = sys.argv[1], sys.argv[2], sys.argv[3]
data = os.environ["DDATA"]
work = os.environ["DWORK"]
if eng == "spark":
    from pyspark.sql import SparkSession, functions

    s = (
        SparkSession.builder.master(master)
        .config("spark.ui.enabled", "false")
        .config("spark.jars", os.environ["VJAR"])
        .config("spark.sql.catalog.h", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.h.type", "hadoop")
        .config(
            "spark.sql.catalog.h.warehouse",
            f"{work}/spark-wh-{master.replace('[', '').replace(']', '')}",
        )
        .getOrCreate()
    )
else:
    import repark
    from repark import ReparkSession, functions

    print(repark._native.__file__, file=sys.stderr)
    s = ReparkSession.builder.getOrCreate()
    s.register_memory_catalog("h", f"{work}/repark-wh-{eng}")


def bits(value: Any, wide: bool = True) -> Any:
    if value is None:
        return None
    if isinstance(value, float):
        return struct.pack(">d", value).hex() if wide else struct.pack(">f", value).hex()
    return value


results = []


def rec(cell: str, rows: Any) -> None:
    results.append({"engine": eng, "master": master, "cell": cell, "rows": rows})


def rows_of(frame: Any, narrow_cols: tuple[str, ...] = ("f",)) -> list[dict[str, Any]]:
    out_rows = []
    for row in frame.collect():
        item = {}
        for key, value in row.asDict().items():
            if key in narrow_cols:
                item[key] = bits(value, wide=False)
            elif isinstance(value, float):
                item[key] = bits(value)
            else:
                item[key] = value
        out_rows.append(item)
    return sorted(out_rows, key=lambda r: json.dumps(r, sort_keys=True, default=str))


def safe(cell: str, thunk: Callable[[], Any]) -> None:
    try:
        rec(cell, thunk())
    except Exception as error:
        rec(cell, {"error": f"{type(error).__name__}: {str(error)[:200]}"})


r = s.read.parquet(f"{data}/r.parquet")
r.createOrReplaceTempView("r")
safe("dd-c", lambda: rows_of(r.dropDuplicates(["c"])))
safe("dd-c-select", lambda: rows_of(r.dropDuplicates(["c"]).select("c")))
safe("dd-f", lambda: rows_of(r.dropDuplicates(["f"])))
safe("dd-c-k", lambda: rows_of(r.dropDuplicates(["c", "k"])))
safe("dd-k-v", lambda: rows_of(r.dropDuplicates(["k", "v"])))
safe("dd-k", lambda: rows_of(r.dropDuplicates(["k"])))
safe("dd-none", lambda: rows_of(r.select("c", "f").dropDuplicates()))
safe("dd-none-all", lambda: rows_of(r.dropDuplicates()))
safe("drop_duplicates-c", lambda: rows_of(r.drop_duplicates(["c"])))
safe("distinct-c-f", lambda: rows_of(r.select("c", "f").distinct()))
safe("distinct-k", lambda: rows_of(r.select("k").distinct()))
safe("ddww-c", lambda: rows_of(r.dropDuplicatesWithinWatermark(["c"])))
safe("dd-c-then-filter", lambda: rows_of(r.dropDuplicates(["c"]).filter(functions.col("i") > 0)))


def written(name: str, frame: Any) -> list[dict[str, Any]]:
    s.sql("CREATE NAMESPACE IF NOT EXISTS h.d")
    s.sql(f"DROP TABLE IF EXISTS h.d.{name}")
    frame.writeTo(f"h.d.{name}").create()
    return rows_of(s.table(f"h.d.{name}"))


safe("w-dd-c", lambda: written("ddc", r.dropDuplicates(["c"])))
safe("w-dd-f", lambda: written("ddf", r.dropDuplicates(["f"])))
safe("w-dd-k-v", lambda: written("ddkv", r.dropDuplicates(["k", "v"])))

for path in ("cs.parquet", "cs4"):
    cs = s.read.parquet(f"{data}/{path}")
    view = "cs" if path == "cs.parquet" else "cs4"
    cs.createOrReplaceTempView(view)
    for variant, frame in (("df", cs), ("df-rep4", cs.repartition(4))):
        safe(
            f"cset-{path}-{variant}",
            lambda frame=frame: sorted(
                [row["s"], sorted(bits(v) for v in row["e"]), row["n"]]
                for row in frame.groupBy("s")
                .agg(
                    functions.collect_set("x").alias("e"),
                    functions.size(functions.collect_set("x")).alias("n"),
                )
                .collect()
            ),
        )
    safe(
        f"cset-{path}-sql",
        lambda view=view: sorted(
            [row["s"], sorted(bits(v) for v in row["e"]), row["n"]]
            for row in s.sql(
                f"SELECT s, collect_set(x) AS e, size(collect_set(x)) AS n FROM {view} GROUP BY s"
            ).collect()
        ),
    )
    safe(
        f"cset-{path}-global",
        lambda frame=cs: [
            [sorted(bits(v) for v in row["e"]), row["n"]]
            for row in frame.filter(functions.col("s") == "s6-all")
            .agg(
                functions.collect_set("x").alias("e"),
                functions.size(functions.collect_set("x")).alias("n"),
            )
            .collect()
        ],
    )

with Path(out).open("a", encoding="utf-8") as handle:
    for item in results:
        handle.write(json.dumps(item) + "\n")
print("cells", len(results))
