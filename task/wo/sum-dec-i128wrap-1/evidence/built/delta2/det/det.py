import hashlib
import json
import os
import sys
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any

import _sum_dec_i128wrap_1_cells as cells

import repark._native as native
from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

TMP, OUT, STEAL, TP = sys.argv[1], Path(sys.argv[2]), sys.argv[3], int(sys.argv[4])
RUNS = 20
cells.SPREAD_COLUMNS = [(10, 2), (18, 0), (19, 0), (20, 0), (38, 0)]
BUILD = native.__file__.split("/")[-3]
CPUS = len(os.sched_getaffinity(0))
Row = tuple[str, str | None]


def queries() -> dict[str, str]:
    found = {}
    for key, sql in cells.spread_sql().items():
        found[key] = sql.replace("SELECT sum(", "SELECT 0 AS g, sum(").replace(
            "SELECT try_sum(", "SELECT 0 AS g, try_sum("
        )
    found["big/grouped/avg/c38_0"] = "SELECT g, avg(c38_0) AS m FROM big GROUP BY g"
    for g in cells.SPLIT_SHAPES:
        found[f"t4x/g{g}/grouped/avg/c38_0"] = (
            f"SELECT g, avg(c38_0) AS m FROM t4x WHERE g = {g} GROUP BY g"
        )
    return found


def expected(key: str, ansi: str) -> str | list[Row] | None:
    if "/avg/" in key:
        return None
    want = cells.spread_exact(key, ansi)
    if isinstance(want, str):
        return "ERR [" + want
    rows: list[Row] = []
    for row in want["rows"]:
        parts = row.split("|")
        g, v = (parts[0], parts[1]) if len(parts) == 2 else ("0", parts[0])
        rows.append((g, None if v == "null" else v))
    return rows


def trimmed(rows: list[Row]) -> list[Row]:
    return [
        (g, None if v is None else v.rstrip("0").rstrip(".") if "." in v else v) for g, v in rows
    ]


def label_of(value: list[Row]) -> str:
    if len(value) <= 2:
        return "rows=" + ";".join(f"{a}|{b}" for a, b in value)
    nulls = sum(1 for _, v in value if v is None)
    digest = hashlib.sha1(repr(value).encode()).hexdigest()[:8]
    return f"rows={len(value)} nulls={nulls} sha={digest}"


def answer(session: Any, sql: str) -> tuple[str, Any]:
    try:
        rows = session.sql(sql).collect()
    except Exception as error:
        return "err", " ".join(str(error).split())
    return "ok", sorted((str(r[0]), None if r[1] is None else str(r[1])) for r in rows)


def record(session: Any, ansi: str, key: str, sql: str) -> dict[str, Any]:
    seen: Counter[str] = Counter()
    exact = True
    want = expected(key, ansi)
    for _ in range(RUNS):
        kind, value = answer(session, sql)
        if kind == "ok":
            label = label_of(value)
            if want is not None and (
                isinstance(want, str) or trimmed(value) != trimmed(sorted(want))
            ):
                exact = False
        else:
            start = value.find("[")
            label = "ERR " + (value[start : start + 34] if start >= 0 else value[:34])
            if want is not None and not (
                isinstance(want, str) and value[start:].startswith(want[4:])
            ):
                exact = False
        seen[label] += 1
    return {
        "build": BUILD,
        "cpus": CPUS,
        "steal": STEAL,
        "tp": TP,
        "ansi": ansi,
        "query": key,
        "distinct": len(seen),
        "exact": exact if want is not None else None,
        "answers": dict(seen),
    }


print("native", native.__file__, "cpus", CPUS, "steal", STEAL, "tp", TP, flush=True)
root = Path(tempfile.mkdtemp(dir=TMP))
cells.write_spread_tables(root)
with OUT.open("a") as out:
    for ansi in ("false", "true"):
        _reset_active_session_for_tests()
        s = (
            ReparkSession.builder.appName("det2")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", ansi)
            .getOrCreate()
        )
        if STEAL == "off":
            s.sql("SET datafusion.execution.enable_file_stream_work_stealing = false").collect()
        if TP:
            s.sql(f"SET datafusion.execution.target_partitions = {TP}").collect()
        cells.register_spread_tables(s, root)
        for key, sql in queries().items():
            out.write(json.dumps(record(s, ansi, key, sql)) + "\n")
            out.flush()
        s.stop()
print("DONE", flush=True)
