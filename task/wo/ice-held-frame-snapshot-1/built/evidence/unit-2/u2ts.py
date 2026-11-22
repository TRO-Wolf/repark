import json
import sys
import time
from pathlib import Path
from typing import Any

from common import condition, session

engine, parent, out, ansi = sys.argv[1:5]
s, wh, F = session(engine, parent, ansi)
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.db").collect()
counter = [0]


def run(statement: str) -> None:
    s.sql(statement).collect()


def make() -> str:
    counter[0] += 1
    name = "lc.db.s" + str(counter[0])
    run("CREATE TABLE " + name + " (n INT, c STRING) USING iceberg")
    run("INSERT INTO " + name + " VALUES (1, 'a'), (2, 'b')")
    return name


EVENTS = {
    "none": lambda t: None,
    "insert": lambda t: run("INSERT INTO " + t + " VALUES (9, 'z')"),
    "alter_add": lambda t: run("ALTER TABLE " + t + " ADD COLUMN x INT"),
    "alter_drop": lambda t: run("ALTER TABLE " + t + " DROP COLUMN c"),
    "rename_col": lambda t: run("ALTER TABLE " + t + " RENAME COLUMN c TO d"),
    "set_props": lambda t: run("ALTER TABLE " + t + " SET TBLPROPERTIES ('k1' = 'v1')"),
    "refresh": lambda t: run("REFRESH TABLE " + t),
}


def stamp(frame: Any) -> Any:
    return frame.agg(F.max("ts")).collect()[0][0]


def cell(event: str, kept_cols: bool) -> list[Any]:
    t = make()
    held = s.table(t).select("n", F.current_timestamp().alias("ts"))
    held.cache()
    first = stamp(held)
    time.sleep(0.05)
    EVENTS[event](t)
    got: list[Any] = [event]
    try:
        second = stamp(held)
        time.sleep(0.05)
        third = stamp(held)
        got.extend([first == second, second == third, held.count()])
    except Exception as error:
        got.append("ERROR " + condition(error))
    s.catalog.clearCache()
    return got


with Path(out).open("w") as handle:
    for event in EVENTS:
        try:
            got: Any = cell(event, True)
        except Exception as error:
            got = "ERROR " + condition(error)
        handle.write(
            json.dumps({"cell": "T3|" + event, "ansi": ansi, "got": got}, default=str) + "\n"
        )
        handle.flush()
print("DONE", flush=True)
