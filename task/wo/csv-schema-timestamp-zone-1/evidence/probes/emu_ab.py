import json
import sys
import time
from pathlib import Path
from typing import Any

from common import session

parent, out, rows_n = sys.argv[1], sys.argv[2], int(sys.argv[3])
s, wh = session("repark", parent)
LA = "America/Los_Angeles"
SCHEMA = "n INT, ts TIMESTAMP"
FILES = {
    "good": "1,2024-01-01 03:00:00\n2,2024-01-01 12:00:00\n",
    "bad": "1,2024-01-01 03:00:00\n2147483648,not a time\n",
    "ov": "1,2024-01-01 03:00:00\n2147483648,2024-01-01 12:00:00\n",
    "ts": "1,2024-01-01 03:00:00\n2,not a time\n",
}
for key, text in FILES.items():
    Path(wh, key).mkdir(parents=True, exist_ok=True)
    Path(wh, key, "a.csv").write_text(text)


def main_reader(path: str) -> Any:
    return s.read.schema(SCHEMA).csv(path)


def a_reader(path: str) -> Any:
    return s.read_csv(path, {"header": "false", "utf8_columns": "column_2"}).selectExpr(
        "CAST(column_1 AS INT) AS n", "CAST(column_2 AS TIMESTAMP) AS ts"
    )


def b_reader(path: str) -> Any:
    return s.read_csv(path, {"header": "false"}).selectExpr(
        "CAST(column_1 AS INT) AS n", "CAST(CAST(column_2 AS STRING) AS TIMESTAMP) AS ts"
    )


READERS = {"main": main_reader, "A": a_reader, "B": b_reader}
found = {}
for zone in (LA, "UTC"):
    for ansi in ("true", "false"):
        s.sql("SET spark.sql.session.timeZone=" + zone).collect()
        s.sql("SET spark.sql.ansi.enabled=" + ansi).collect()
        for key in FILES:
            for name, build in READERS.items():
                tag = "/".join((zone[-3:], ansi, key, name))
                try:
                    frame = build(str(Path(wh, key)))
                    frame.createOrReplaceTempView("v")
                    got = s.sql("SELECT n, unix_timestamp(ts) AS u FROM v").collect()
                    found[tag] = sorted(repr(tuple(row)) for row in got)
                except Exception as error:
                    found[tag] = "RAISE " + " ".join(str(error).split())[:120]
s.sql("SET spark.sql.session.timeZone=" + LA).collect()
s.sql("SET spark.sql.ansi.enabled=true").collect()
big = Path(wh, "big")
big.mkdir(parents=True, exist_ok=True)
with (big / "a.csv").open("w") as handle:
    for index in range(rows_n):
        handle.write(f"{index},2024-01-01 {index % 24:02d}:{index % 60:02d}:00\n")
times = {name: {"count": [], "hour": []} for name in READERS}
for _ in range(3):
    for name, build in READERS.items():
        start = time.perf_counter()
        build(str(big)).count()
        times[name]["count"].append(round(time.perf_counter() - start, 3))
        start = time.perf_counter()
        build(str(big)).filter(f"n > {rows_n // 2}").selectExpr("max(hour(ts))").collect()
        times[name]["hour"].append(round(time.perf_counter() - start, 3))
found["times"] = times
for name, build in READERS.items():
    build(str(Path(wh, "good"))).createOrReplaceTempView("x")
    found["explain/" + name] = [
        " | ".join(str(cell) for cell in row) for row in s.sql("EXPLAIN SELECT * FROM x").collect()
    ]
Path(out).write_text(json.dumps(found, indent=1, sort_keys=True))
s.stop()
