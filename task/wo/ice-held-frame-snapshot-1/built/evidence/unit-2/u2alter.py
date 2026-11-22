import json
import sys
import time
from pathlib import Path
from typing import Any

from common import condition, rows, session

engine, parent, out, ansi = sys.argv[1:5]
s, wh, F = session(engine, parent, ansi)
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.db").collect()
counter = [0]


def run(statement: str) -> None:
    s.sql(statement).collect()


def make() -> str:
    counter[0] += 1
    name = "lc.db.a" + str(counter[0])
    run("CREATE TABLE " + name + " (n INT, c STRING) USING iceberg")
    run("INSERT INTO " + name + " VALUES (1, 'a'), (2, 'b')")
    return name


ALTERS = {
    "drop": "ALTER TABLE {t} DROP COLUMN c",
    "rename": "ALTER TABLE {t} RENAME COLUMN c TO d",
    "add": "ALTER TABLE {t} ADD COLUMN x INT",
    "props": "ALTER TABLE {t} SET TBLPROPERTIES ('k1' = 'v1')",
}
HOLDS = {
    "bare": lambda t: s.table(t),
    "select_all": lambda t: s.table(t).select("n", "c"),
    "select_n": lambda t: s.table(t).select("n"),
    "filter": lambda t: s.table(t).filter("n > 0"),
    "ts": lambda t: s.table(t).withColumn("ts", F.current_timestamp()),
}


def attempt(body: Any) -> Any:
    try:
        return body()
    except Exception as error:
        return "ERROR " + condition(error)


def cell(hold: str, alter: str) -> list[Any]:
    t = make()
    held = HOLDS[hold](t)
    held.cache()
    got: list[Any] = [held.count()]
    stamp = None
    if hold == "ts":
        stamp = held.agg(F.max("ts")).collect()[0][0]
    time.sleep(0.05)
    run(ALTERS[alter].format(t=t))
    got.append(attempt(lambda: held.count()))
    got.append(attempt(lambda: rows(held.select("n"))))
    got.append(attempt(lambda: held.filter("n > 0").count()))
    if stamp is not None:
        got.append(attempt(lambda: held.agg(F.max("ts")).collect()[0][0] == stamp))
    got.append(attempt(lambda: s.table(t).count()))
    got.append(attempt(lambda: held.is_cached))
    s.catalog.clearCache()
    return got


with Path(out).open("w") as handle:
    for hold in HOLDS:
        for alter in ALTERS:
            got = attempt(lambda hold=hold, alter=alter: cell(hold, alter))
            handle.write(
                json.dumps(
                    {"cell": "A|" + hold + "|" + alter, "ansi": ansi, "got": got}, default=str
                )
                + "\n"
            )
            handle.flush()
print("DONE", flush=True)
