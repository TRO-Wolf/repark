import json
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "probes"))
import cells
import lib

engine, ansi, files, reps, conf, subset, out = sys.argv[1:8]
CONDITION = re.compile(r"\[([A-Z_]+(?:\.[A-Z_]+)?)\]")
PASS_BACK = {5, 10, 11, 12, 21, 22, 23, 24}
DISTINCT = ("distinct", "distinct_ungrouped", "distinct_all")
FABLE = {
    "fable/grouped_sum": "SELECT g, sum(c) AS m FROM fq WHERE g = 11 GROUP BY g",
    "fable/grouped_try_sum": "SELECT g, try_sum(c) AS m FROM fq WHERE g = 11 GROUP BY g",
    "fable/distinct_ungrouped": "SELECT sum(DISTINCT c) AS m FROM fq WHERE g = 11",
}


def session() -> tuple:
    if engine == "spark":
        import pyspark
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as sf

        builder = (
            SparkSession.builder.master("local[4]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
        )
        if conf == "split":
            builder = builder.config("spark.sql.files.openCostInBytes", "134217728")
        prov = {"engine": "spark", "version": pyspark.__version__, "master": "local[4]"}
    else:
        import repark._native as native
        from repark import ReparkSession as SparkSession
        from repark import functions as sf

        print("native", native.__file__, flush=True)
        builder = SparkSession.builder.appName("sum-delta1-spread")
        prov = {
            "engine": "repark",
            "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
        }
    builder = builder.config("spark.sql.session.timeZone", "UTC").config(
        "spark.sql.ansi.enabled", ansi
    )
    return builder.getOrCreate(), sf, prov


def queries(sf: object, s: object) -> dict:
    found = {}
    for key, _fam, pos, _col, g, table, sql in cells.sum_sql_cells():
        multi = table in ("s4", "p2")
        if not (multi or pos in DISTINCT):
            continue
        if subset == "passback" and g is not None and g not in PASS_BACK:
            continue
        found[("mp/" if multi else "") + key] = lambda sql=sql: s.sql(sql)
    for g in cells.DF_GROUPS:
        if subset == "passback" and g not in PASS_BACK:
            continue
        for c in cells.names_of(cells.WIDE):
            found[f"df/repartition/g{g}/{c}"] = lambda g=g, c=c: (
                s.table("s")
                .where(sf.col("g") == g)
                .repartition(4)
                .groupBy("g")
                .agg(sf.sum(c).alias("m"))
            )
    for key, sql in FABLE.items():
        found[key] = lambda sql=sql: s.sql(sql)
    return found


def answer(run: object) -> str:
    try:
        return json.dumps(lib.answer_of(run()), sort_keys=True)
    except Exception as e:
        text = " ".join(str(e).split())
        match = CONDITION.search(text)
        return "ERR:" + (match.group(1) if match else text[:120])


s, sf, prov = session()
for name in ("s", "s4", "p2", "fq"):
    s.read.parquet(str(Path(files) / name)).createOrReplaceTempView(name)
records = [dict(key="provenance", ansi=ansi, conf=conf, subset=subset, reps=int(reps), **prov)]
if engine == "spark":
    for name in ("s4", "p2", "fq"):
        parts = s.table(name).rdd.getNumPartitions()
        records.append({"key": f"data/{name}_partitions", "result": parts})
for key, run in queries(sf, s).items():
    seen: Counter = Counter()
    for _ in range(int(reps)):
        seen[answer(run)] += 1
    records.append({"key": key, "answers": dict(seen)})
with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
varied = sum(1 for r in records if len(r.get("answers", {})) > 1)
print(engine, ansi, conf, subset, len(records), "varied", varied, flush=True)
