import json
import sys
from pathlib import Path

import repark._native as native
from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

print(native.__file__, flush=True)
d, out = sys.argv[1], sys.argv[2]
QUERIES = {
    "sum/ungrouped": "SELECT sum(c) FROM t",
    "try_sum/ungrouped": "SELECT try_sum(c) FROM t",
    "sum/grouped": "SELECT g, sum(c) FROM t GROUP BY g",
    "try_sum/grouped": "SELECT g, try_sum(c) FROM t GROUP BY g",
    "avg/ungrouped": "SELECT avg(c) FROM t",
    "avg/grouped": "SELECT g, avg(c) FROM t GROUP BY g",
    "plan/ungrouped": "EXPLAIN SELECT sum(c) FROM t",
}
rows = []
for ansi in ["false", "true"]:
    for tp in [1, 2, 4, 8]:
        _reset_active_session_for_tests()
        s = (
            ReparkSession.builder.appName("op3")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", ansi)
            .getOrCreate()
        )
        s.sql(f"SET datafusion.execution.target_partitions = {tp}").collect()
        for name in ["wrap5", "neg", "fit", "wrap5_two", "neg_two", "fit_two"]:
            for p in [19, 20]:
                stem, _, two = name.partition("_")
                path = f"{d}/{stem}_p{p}_two" if two else f"{d}/{stem}_p{p}.parquet"
                s.read.parquet(path).createOrReplaceTempView("t")
                for key, q in QUERIES.items():
                    try:
                        r = [[str(v) for v in x] for x in s.sql(q).collect()]
                        if key.startswith("plan"):
                            r = " ".join(" ".join(x) for x in r)
                            r = (
                                "single"
                                if "mode=Single" in r
                                else ("partial" if "mode=Partial" in r else r[:300])
                            )
                    except Exception as e:
                        r = "ERR " + str(e).split("\n")[0][:200]
                    rows.append(
                        {"ansi": ansi, "tp": tp, "file": f"{name}_p{p}", "query": key, "answer": r}
                    )
                    print(ansi, tp, name, p, key, r, flush=True)
        s.stop()
        _reset_active_session_for_tests()
with Path(out).open("w") as fh:
    for r in rows:
        fh.write(json.dumps(r) + "\n")
