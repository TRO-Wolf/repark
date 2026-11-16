import json
import sys
import time
from collections.abc import Callable
from pathlib import Path

import repark._native as native
from repark import ReparkSession

label, out = sys.argv[1:3]
s = ReparkSession.builder.appName("phase").getOrCreate()


def rows(n: int, cell: str, odd: str | None) -> str:
    r = [f"({i}, {cell})" for i in range(n - 1)]
    r.append(f"({n - 1}, {odd or cell})")
    return ", ".join(r)


cases = {
    "none": f"SELECT count(*) FROM VALUES {rows(10000, '0.5', None)} AS x(a, b)",
    "dbl": f"SELECT count(*) FROM VALUES {rows(10000, '0.5', 'CAST(0.25 AS DOUBLE)')} AS x(a, b)",
}


def med(f: Callable[[], object], reps: int = 10) -> float:
    f()
    ts = []
    for _ in range(reps):
        t = time.perf_counter()
        f()
        ts.append(time.perf_counter() - t)
    ts.sort()
    return ts[len(ts) // 2]


rec: dict[str, object] = {"label": label, "native": native.__file__}
for k, q in cases.items():
    rec[k + "_sql"] = med(lambda q=q: s.sql(q))
    rec[k + "_collect"] = med(lambda q=q: s.sql(q).collect())
    fr = s.sql(q)
    rec[k + "_collect_only"] = med(lambda fr=fr: fr.collect())
with Path(out).open("a") as fh:
    fh.write(json.dumps(rec) + "\n")
print(rec)
