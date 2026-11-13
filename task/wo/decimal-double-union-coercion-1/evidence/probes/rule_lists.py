import json
import sys
from pathlib import Path

import repark
import repark._native as native
from repark import ReparkSession

out = sys.argv[1]
q = "EXPLAIN VERBOSE SELECT 0.1 AS c UNION ALL SELECT CAST(1e-20 AS DOUBLE)"
result = {"native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1], "query": q}
s = ReparkSession.builder.appName("rules").getOrCreate()
for door, run in (("spark_door", s.sql), ("native_door", repark.sql)):
    rows = run(q).collect()
    analyzer, optimizer, phase, plans = [], [], "analyzer", {}
    for r in rows:
        name = r[0]
        plans[name] = r[1]
        if name == "analyzed_logical_plan":
            phase = "optimizer"
            continue
        if name.startswith("logical_plan after "):
            (analyzer if phase == "analyzer" else optimizer).append(
                name[len("logical_plan after ") :]
            )
    result[door] = {
        "analyzer": analyzer,
        "optimizer_pass_1": optimizer[: len(optimizer) // 2]
        if len(optimizer) % 2 == 0
        else optimizer,
        "initial_logical_plan": plans.get("initial_logical_plan"),
        "analyzed_logical_plan": plans.get("analyzed_logical_plan"),
        "logical_plan": plans.get("logical_plan"),
        "changed_at": [
            r[0]
            for r in rows
            if r[1] != "SAME TEXT AS ABOVE" and r[0].startswith("logical_plan after ")
        ],
    }
prep = {}
for door, run in (("spark_door", s.sql), ("native_door", repark.sql)):
    try:
        run("PREPARE dd_p AS SELECT 0.1 AS c UNION ALL SELECT CAST(1e-20 AS DOUBLE)").collect()
        f = run("EXECUTE dd_p")
        prep[door] = {
            "type": f.schema.fields[0].dataType.simpleString(),
            "values": [str(x[0]) for x in f.collect()],
        }
    except Exception as e:
        prep[door] = {"error": type(e).__name__, "head": str(e).splitlines()[0][:200]}
result["prepare_execute"] = prep
cache = {}
try:
    f = s.sql("SELECT 0.1 AS c UNION ALL SELECT CAST(1e-20 AS DOUBLE)").cache()
    f.count()
    cache["df_cache"] = {
        "type": f.schema.fields[0].dataType.simpleString(),
        "values": sorted(str(x[0]) for x in f.collect()),
    }
except Exception as e:
    cache["df_cache"] = {"error": type(e).__name__, "head": str(e).splitlines()[0][:200]}
try:
    s.sql("CACHE TABLE dd_ct AS SELECT 0.1 AS c UNION ALL SELECT CAST(1e-20 AS DOUBLE)").collect()
    f = s.sql("SELECT c FROM dd_ct")
    cache["cache_table"] = {
        "type": f.schema.fields[0].dataType.simpleString(),
        "values": sorted(str(x[0]) for x in f.collect()),
    }
except Exception as e:
    cache["cache_table"] = {"error": type(e).__name__, "head": str(e).splitlines()[0][:200]}
try:
    repark.sql(
        "CREATE VIEW dd_nv AS SELECT 0.1 AS c UNION ALL SELECT CAST(1e-20 AS DOUBLE)"
    ).collect()
    f = repark.sql("SELECT c FROM dd_nv")
    cache["native_view"] = {
        "type": f.schema.fields[0].dataType.simpleString(),
        "values": sorted(str(x[0]) for x in f.collect()),
    }
except Exception as e:
    cache["native_view"] = {"error": type(e).__name__, "head": str(e).splitlines()[0][:200]}
result["stored_plans"] = cache
Path(out).write_text(json.dumps(result, indent=1))
print(
    json.dumps(
        {k: v for k, v in result.items() if k in ("prepare_execute", "stored_plans")}, indent=1
    )
)
print(result["spark_door"]["changed_at"], result["native_door"]["changed_at"])
print(len(result["spark_door"]["analyzer"]), len(result["native_door"]["analyzer"]))
