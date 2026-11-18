import sys
import tempfile
from pathlib import Path

import cells
import lib

engine, wh_parent, jar, out = sys.argv[1:5]
wh = tempfile.mkdtemp(prefix="wx-" + engine + "-", dir=wh_parent)
s, prov = lib.build(engine, wh, jar, "true")
lines = ["provenance " + " ".join(k + "=" + str(v) for k, v in sorted(prov.items()))]


def plan_text(frame: object) -> str:
    if engine == "spark":
        return str(frame._jdf.queryExecution().toString())
    return frame._explain_text(True, None)


for name in ("fx", "tzt"):
    path = wh + "/fx-" + name
    s.sql(cells.FIXTURES[name]).write.mode("overwrite").parquet(path)
    s.read.parquet(path).createOrReplaceTempView(name)
bodies = {"div": cells.ANSI_BODIES["div"], "add_ovf": cells.ANSI_BODIES["add_ovf"]}
for b, body in bodies.items():
    s.sql("CREATE OR REPLACE TEMP VIEW sql_on_" + b + " AS " + body).collect()
    s.sql(body).createOrReplaceTempView("df_on_" + b)
held = {b: s.sql(body) for b, body in bodies.items()}
s.sql("CREATE OR REPLACE TEMP VIEW sql_utc_hour AS " + cells.TZ_BODIES["hour"]).collect()
s.sql(cells.TZ_BODIES["hour"]).createOrReplaceTempView("df_utc_hour")
s.sql("SET spark.sql.ansi.enabled=false").collect()
s.sql("SET spark.sql.session.timeZone=America/Los_Angeles").collect()
targets = [("read sql_on_" + b, "SELECT m FROM sql_on_" + b) for b in bodies]
targets += [("read df_on_" + b, "SELECT m FROM df_on_" + b) for b in bodies]
targets += [
    ("read sql_utc_hour", "SELECT m FROM sql_utc_hour"),
    ("read df_utc_hour", "SELECT m FROM df_utc_hour"),
]
targets += [("direct off " + b, body) for b, body in bodies.items()]
for title, sql in targets:
    lines.append(
        "=== "
        + title
        + " (created ANSI on and UTC; read ANSI off and America/Los_Angeles) :: "
        + sql
    )
    lines.append(str(lib.attempt(lambda sql=sql: plan_text(s.sql(sql)))))
for b, frame in held.items():
    lines.append("=== held " + b + " (created ANSI on; explained after SET off)")
    lines.append(str(lib.attempt(lambda frame=frame: plan_text(frame))))
Path(out).write_text("\n".join(lines) + "\n")
s.stop()
print("WROTE " + out, flush=True)
