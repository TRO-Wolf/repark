import json
import sys

from pyspark.sql import SparkSession

session = SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").getOrCreate()
session.sparkContext.setLogLevel("ERROR")
session.sql(
    "CREATE OR REPLACE TEMPORARY VIEW t AS SELECT * FROM VALUES (1, CAST('0.0' AS DOUBLE)), "
    "(2, CAST('-0.0' AS DOUBLE)), (6, CAST('1.5' AS DOUBLE)) AS x(id, d)"
)
session.sql(
    "CREATE OR REPLACE TEMPORARY VIEW u AS SELECT * FROM VALUES (10, CAST('0.0' AS DOUBLE)) AS x(uid, d)"
)
queries = {
    "not-in": "SELECT id FROM t WHERE d NOT IN (SELECT d FROM u)",
    "in": "SELECT id FROM t WHERE d IN (SELECT d FROM u)",
    "not-exists": "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.d = t.d)",
    "literal-not-in": "SELECT id FROM t WHERE d NOT IN (CAST('0.0' AS DOUBLE))",
}
out = []
for naaj in ("true", "false"):
    for threshold in ("10485760", "-1"):
        session.conf.set("spark.sql.optimizeNullAwareAntiJoin", naaj)
        session.conf.set("spark.sql.autoBroadcastJoinThreshold", threshold)
        for name, query in queries.items():
            frame = session.sql(query)
            plan = frame._jdf.queryExecution().executedPlan().toString()
            joins = sorted({word for word in plan.replace("(", " ").split() if word.endswith("Join")})
            rows = sorted(r[0] for r in frame.collect())
            out.append({"naaj": naaj, "broadcast_threshold": threshold, "query": name, "ids": rows, "joins": joins})
session.stop()
with open(sys.argv[1], "w", encoding="utf-8") as handle:
    for record in out:
        handle.write(json.dumps(record) + "\n")
