"""Time the ICE-TSNS-COERCION-1 cost baseline on the installed module.

One set is 15 repeats. The command runs three sets and records the median of
the three set medians. Closed-gate statements use a table with no nanosecond
column. The open-gate reads select only a ``timestamp_ns`` column, with no
card spelling.
"""

from __future__ import annotations

import argparse
import json
import statistics
import tempfile
import time
from collections import Counter
from pathlib import Path
from typing import Any

from repark import ReparkSession

ROWS = 2_000
REPEATS = 15
SETS = 3
VALUE = (
    "coalesce(t, NULL) AS a, CASE WHEN id % 2 = 0 THEN t ELSE NULL END AS b, "
    "if(id > 3, n, NULL) AS c, array(t, NULL)[0] AS d, coalesce(s, 'x') AS s"
)
COLUMNS = "id INT, a TIMESTAMP, b TIMESTAMP, c TIMESTAMP_NTZ, d TIMESTAMP, s STRING"
MOR = (
    ", 'write.delete.mode' = 'merge-on-read', 'write.update.mode' = 'merge-on-read', "
    "'write.merge.mode' = 'merge-on-read'"
)
NAMES = (
    "insert",
    "merge",
    "merge_mor",
    "read_union",
    "read_wide_case",
    "read_ns_union",
    "read_ns_case",
)


def quiet_note() -> dict[str, Any]:
    """Return the load and how many python or java processes are up."""
    load = Path("/proc/loadavg").read_text(encoding="utf-8").split()[:3]
    counts: Counter[str] = Counter()
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            comm = (entry / "comm").read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if comm in {"python", "java", "cargo", "rustc"}:
            counts[comm] += 1
    return {"load": load, "processes": dict(counts)}


def timed(spark: Any, statement: str) -> float:
    """Return the wall time of ``statement`` in milliseconds."""
    began = time.perf_counter()
    spark.sql(statement).toArrow()
    return (time.perf_counter() - began) * 1_000


def create(spark: Any, table: str, mode: str = "") -> None:
    """Create one closed-gate target and seed the even source rows."""
    spark.sql(
        f"CREATE TABLE {table} ({COLUMNS}) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '3'{mode})"
    )
    spark.sql(f"INSERT INTO {table} SELECT id, {VALUE} FROM ice.ns.src WHERE id % 2 = 0")


def measure(warehouse: Path) -> dict[str, list[float]]:
    """Return 15 wall times for each named statement."""
    spark = (
        ReparkSession.builder.appName("ice-tsns-coercion-1-cost")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    spark.register_memory_catalog("ice", str(warehouse))
    spark.sql("CREATE NAMESPACE ice.ns")
    spark.sql(
        "CREATE TABLE ice.ns.src (id INT, t TIMESTAMP, n TIMESTAMP_NTZ, s STRING) USING iceberg "
        "TBLPROPERTIES ('format-version' = '3')"
    )
    spark.sql(
        "INSERT INTO ice.ns.src SELECT CAST(id AS INT), "
        "timestamp_micros(1767323045123456 + id), "
        "CAST(timestamp_micros(1767323045123456 + id) AS TIMESTAMP_NTZ), CAST(id AS STRING) "
        f"FROM range({ROWS})"
    )
    spark.sql(
        "CREATE TABLE ice.ns.nsrc (id INT, n timestamp_ns) USING iceberg "
        "TBLPROPERTIES ('format-version' = '3')"
    )
    spark.sql(
        "INSERT INTO ice.ns.nsrc SELECT CAST(id AS INT), "
        "CAST('2026-01-02 03:04:05.123456789' AS timestamp_ns) "
        f"FROM range({ROWS})"
    )
    merge = (
        "MERGE INTO {t} t USING (SELECT id, " + VALUE + " FROM ice.ns.src) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET a = s.a, b = coalesce(s.b, NULL), c = s.c "
        "WHEN NOT MATCHED THEN INSERT (id, a, b, c, d, s) VALUES (s.id, s.a, s.b, s.c, s.d, s.s)"
    )
    branch = "SELECT id, " + VALUE + " FROM ice.ns.src WHERE id > {i}"
    union = " UNION ALL ".join(branch.format(i=index) for index in range(50))
    arms = " ".join(
        f"WHEN id % 211 = {index} THEN t + INTERVAL '{index}' SECOND" for index in range(200)
    )
    wide_case = f"SELECT id, CASE {arms} ELSE NULL END AS w FROM ice.ns.src"
    ns_branch = "SELECT id, n AS v FROM ice.ns.nsrc WHERE id > {i}"
    ns_union = " UNION ALL ".join(ns_branch.format(i=index) for index in range(50))
    ns_arms = " ".join(f"WHEN id % 211 = {index} THEN n" for index in range(200))
    ns_case = f"SELECT id, CASE {ns_arms} ELSE n END AS w FROM ice.ns.nsrc"
    times: dict[str, list[float]] = {name: [] for name in NAMES}
    for index in range(REPEATS):
        create(spark, f"ice.ns.i{index}")
        times["insert"].append(
            timed(spark, f"INSERT INTO ice.ns.i{index} SELECT id, {VALUE} FROM ice.ns.src")
        )
        create(spark, f"ice.ns.m{index}")
        times["merge"].append(timed(spark, merge.format(t=f"ice.ns.m{index}")))
        create(spark, f"ice.ns.r{index}", MOR)
        times["merge_mor"].append(timed(spark, merge.format(t=f"ice.ns.r{index}")))
        times["read_union"].append(timed(spark, union))
        times["read_wide_case"].append(timed(spark, wide_case))
        times["read_ns_union"].append(timed(spark, ns_union))
        times["read_ns_case"].append(timed(spark, ns_case))
    spark.stop()
    return times


def summarize(sets: list[dict[str, list[float]]]) -> dict[str, Any]:
    """Return each set's median and the median of those medians."""
    report: dict[str, Any] = {}
    for name in NAMES:
        medians = [round(statistics.median(one[name]), 2) for one in sets]
        report[name] = {
            "set_medians_ms": medians,
            "median_of_medians_ms": round(statistics.median(medians), 2),
        }
    return report


def main() -> None:
    """Print and write the baseline."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    note_before = quiet_note()
    sets: list[dict[str, list[float]]] = []
    for index in range(SETS):
        print(f"set {index + 1} {quiet_note()}", flush=True)
        with tempfile.TemporaryDirectory(prefix="tsns-coercion-cost-") as warehouse:
            sets.append(measure(Path(warehouse)))
    payload = {
        "repeats": REPEATS,
        "sets": SETS,
        "rows": ROWS,
        "before": note_before,
        "after": quiet_note(),
        "statements": summarize(sets),
    }
    args.output.write_text(json.dumps(payload, indent=1) + "\n", encoding="utf-8")
    print(json.dumps(payload["statements"]), flush=True)


if __name__ == "__main__":
    main()
