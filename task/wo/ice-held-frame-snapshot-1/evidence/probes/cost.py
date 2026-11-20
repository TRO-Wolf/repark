import json
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from common import session

engine, parent, out, round_id = sys.argv[1:5]
s, wh, F = session(engine, parent, "true")
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.db").collect()
ROWS = 2_000_000
BODY = "SELECT CAST(id AS INT) AS n, CAST(id % 7 AS STRING) AS c FROM range(" + str(ROWS) + ")"
s.sql("CREATE TABLE lc.db.big USING iceberg AS " + BODY).collect()
T = "lc.db.big"
counter = [0]


def insert() -> None:
    counter[0] += 1
    s.sql("INSERT INTO " + T + " VALUES (" + str(-counter[0]) + ", 'w')").collect()


def read(frame: Any) -> int:
    return len(frame.collect()) if engine == "spark" else frame.toArrow().num_rows


def timed(body: Callable[[], Any]) -> tuple[float, Any]:
    start = time.perf_counter()
    got = body()
    return time.perf_counter() - start, got


def case_held_nowrite() -> tuple[float, Any]:
    held = s.table(T)
    return timed(lambda: read(held))


def case_held_write() -> tuple[float, Any]:
    held = s.table(T)
    insert()
    return timed(lambda: read(held))


def case_fresh_write() -> tuple[float, Any]:
    insert()
    return timed(lambda: read(s.table(T)))


def case_plan() -> tuple[float, Any]:
    wall, _ = timed(lambda: [s.table(T) for _ in range(100)])
    return wall / 100, 100


def case_held_count() -> tuple[float, Any]:
    held = s.table(T).filter("n >= 0")
    insert()
    return timed(held.count)


CASES = {
    "held_nowrite": case_held_nowrite,
    "held_write": case_held_write,
    "fresh_write": case_fresh_write,
    "plan_100": case_plan,
    "held_filter_count": case_held_count,
}
read(s.table(T))
with Path(out).open("a") as handle:
    for name, body in CASES.items():
        wall, got = body()
        record = {"engine": engine, "round": round_id, "case": name, "wall": round(wall, 5)}
        record["got"] = got
        handle.write(json.dumps(record) + "\n")
