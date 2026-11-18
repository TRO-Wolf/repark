import json
import sys
import tempfile
from pathlib import Path

import lib

engine, wh_parent, jar, out = sys.argv[1:5]
wh = tempfile.mkdtemp(prefix="wcs-" + engine + "-", dir=wh_parent)
s, prov = lib.build(engine, wh, jar, "true")
if engine == "spark":
    s.conf.set("spark.sql.pyspark.jvmStacktrace.enabled", "true")
found = {"provenance": prov}


def keep(key: str, run: object) -> None:
    try:
        res = run()
        found[key] = lib.answer(res) if res is not None else "OK"
    except Exception as e:
        text = " ".join(str(e).split())
        lines = [
            x.strip() for x in str(e).splitlines() if "org.apache.spark.sql.catalyst.analysis" in x
        ][:6]
        found[key] = {"error": lib.error(e)["error"], "text": text[:300], "analysis_frames": lines}


s.sql("SET spark.sql.caseSensitive=true").collect()
path = wh + "/cs2"
s.sql("SELECT CAST(1 AS INT) AS a, CAST(2 AS INT) AS A").write.mode("overwrite").parquet(path)
s.read.parquet(path).createOrReplaceTempView("cs2")
keep(
    "create/sql_view", lambda: s.sql("CREATE OR REPLACE TEMP VIEW v_view AS SELECT A AS m FROM cs2")
)
keep(
    "create/sql_path",
    lambda: s.sql(
        "CREATE OR REPLACE TEMP VIEW v_path AS SELECT A AS m FROM parquet.`" + path + "`"
    ),
)
keep(
    "create/sql_upper_only",
    lambda: s.sql("CREATE OR REPLACE TEMP VIEW v_up AS SELECT CAST(9 AS INT) AS Q"),
)
keep(
    "create/sql_upper_ref",
    lambda: s.sql("CREATE OR REPLACE TEMP VIEW v_upref AS SELECT Q AS m FROM v_up"),
)
keep(
    "create/df_path",
    lambda: s.sql("SELECT A AS m FROM parquet.`" + path + "`").createOrReplaceTempView("d_path"),
)
fresh = s.read.parquet(path).selectExpr("A AS m")
keep("read/cs/sql_view", lambda: s.sql("SELECT m FROM v_view"))
keep("read/cs/sql_path", lambda: s.sql("SELECT m FROM v_path"))
s.sql("SET spark.sql.caseSensitive=false").collect()
keep("read/ci/sql_view", lambda: s.sql("SELECT m FROM v_view"))
keep("read/ci/sql_path", lambda: s.sql("SELECT m FROM v_path"))
keep("read/ci/sql_upref", lambda: s.sql("SELECT m FROM v_upref"))
keep("read/ci/df_path", lambda: s.sql("SELECT m FROM d_path"))
keep("read/ci/held_fresh", lambda: fresh)
keep("read/ci/fixture", lambda: s.sql("SELECT * FROM cs2"))
keep("direct/ci/path", lambda: s.sql("SELECT A AS m FROM parquet.`" + path + "`"))
Path(out).write_text(json.dumps(found, indent=1, sort_keys=True) + "\n")
s.stop()
print("WROTE " + out, flush=True)
