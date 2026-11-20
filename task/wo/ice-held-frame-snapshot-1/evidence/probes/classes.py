import gzip
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ACTIONS = ["copy", "cor", "app", "ins_into", "sat", "insel", "self"]
SCHEMA = ["alter_add", "alter_drop", "alter_rename", "recreate", "wt_create"]
HOLDS = ["table", "read", "sql", "sqlf", "filter", "select", "sel_all", "agg", "join_l", "dfview"]
TEXT = {"table", "read", "sql", "sqlf"}
WRITE_TO = {"copy", "cor", "app", "self"}


def load(path: str) -> list[dict[str, Any]]:
    raw = Path(path).read_bytes()
    text = gzip.decompress(raw).decode() if path.endswith(".gz") else raw.decode()
    return [json.loads(line) for line in text.splitlines()]


def klass(got: Any) -> str:
    if isinstance(got, str):
        for needle, name in (("TABLE_ID_MISMATCH", "ID"), ("COLUMNS_MISMATCH", "COL")):
            if needle in got:
                return name
        if "VIEW_WITH_PLAN" in got:
            return "VIEW"
        return "err:" + got.replace("ERROR ", "")[:18]
    return "new:" + ",".join(got[0])


def unit(record: dict[str, Any]) -> str:
    _, hold, write, action, _ = record["key"].split("|")
    spark = record["spark"]
    if write in SCHEMA and hold in TEXT and action in WRITE_TO:
        return "1b"
    if write == "alter_add" and isinstance(spark, str) and "COLUMNS_MISMATCH" in spark:
        return "1b"
    return "1a"


records = load(sys.argv[1])
table: dict[tuple[str, str], dict[str, str]] = defaultdict(dict)
units: Counter[str] = Counter()
for record in records:
    group, hold, write, action, mode = record["key"].split("|")
    if group != "B" or mode != "true":
        continue
    if write in SCHEMA:
        table[(hold, write)][action] = klass(record["spark"])
    if record["v_main"] != "EQUAL":
        units[unit(record)] += 1
for write in SCHEMA:
    print("==", write)
    for hold in HOLDS:
        row = table.get((hold, write), {})
        print(hold.ljust(8), " | ".join(row.get(action, "-") for action in ACTIONS))
print("units per build and mode", dict(units))
