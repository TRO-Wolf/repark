import json
import sys
from pathlib import Path

HMS = ("hour", "minute", "second")
LTZ = ("a0-utc", "a0-la", "a1-utc", "a1-la")
NTZ = ("a1-utc-ntz", "a1-la-ntz")
NTZ_SHAPES = ("off", "zulu", "gap", "tonly")


def wanted(r: dict) -> bool:
    if r.get("fn") not in HMS:
        return False
    if r["cfg"] in LTZ:
        return True
    return r["cfg"] in NTZ and r["sh"] in NTZ_SHAPES


def main() -> None:
    raw, out = Path(sys.argv[1]), Path(sys.argv[2])
    cells: dict[str, dict] = {}
    for path in sorted(raw.glob("spark-*.jsonl")):
        for line in path.read_text().splitlines():
            r = json.loads(line)
            if "fn" not in r or not wanted(r):
                continue
            key = "|".join((r["cfg"], r["in"], r["sh"], r["door"], r["fn"]))
            cells[key] = {"err": r["err"]} if "err" in r else {"v": r["v"]}
    record = {
        "source": "STRING-HOUR-MINUTE-SECOND-1 step 0, Spark 4.1.2 with Iceberg 1.11.0, local[1]",
        "cells": dict(sorted(cells.items())),
    }
    out.write_text(json.dumps(record, indent=0, sort_keys=False) + "\n")
    print(len(cells))


main()
