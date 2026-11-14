from __future__ import annotations

import json
import os
import statistics
import sys
import time
from pathlib import Path

from repark import ReparkSession

ITERATIONS = 40


def folded() -> str:
    preds = []
    for i in range(200):
        if i % 2 == 0:
            preds.append(f"CAST('{i}.5' AS DOUBLE) > CAST('0.25' AS DOUBLE)")
        else:
            preds.append(f"(CAST({i} AS DOUBLE) * -1) <= CAST('{i}.0' AS DOUBLE)")
    return "SELECT count(*) FROM range(1000) WHERE " + " AND ".join(preds)


def columns() -> str:
    preds = [f"CAST(id AS DOUBLE) + 0.5 = CAST('{i}.5' AS DOUBLE)" for i in range(200)]
    return "SELECT count(*) FROM range(1000) WHERE " + " OR ".join(preds)


def median_ms(session: ReparkSession, text: str) -> tuple[float, list]:
    answer = None
    times = []
    for _ in range(ITERATIONS):
        start = time.perf_counter()
        answer = [tuple(row) for row in session.sql(text).collect()]
        times.append((time.perf_counter() - start) * 1000)
    return statistics.median(times[5:]), answer


def main() -> None:
    session = ReparkSession.builder.appName("const200").getOrCreate()
    record = {"build": sys.argv[1], "run": int(sys.argv[2]), "load": os.getloadavg()[0]}
    for name, text in (("folded200", folded()), ("column200", columns())):
        record[name], record[name + "_answer"] = median_ms(session, text)
    with Path(sys.argv[3]).open("a") as sink:
        sink.write(json.dumps(record) + "\n")


if __name__ == "__main__":
    main()
