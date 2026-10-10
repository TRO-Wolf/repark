"""ICE-TSNS-NARROW-REFUSE-1: time statements that touch no nanosecond column.

``python _time_ice_tsns_narrow_refuse_1.py`` prints, for the installed build, the median wall
time in milliseconds of four statements over a table with no nanosecond column: an INSERT of a
query whose expressions hold untyped NULLs beside microsecond timestamps, a MERGE with a matched
update and a not-matched insert, the same MERGE in merge-on-read, and an ``EXPLAIN`` of a
fifty-branch union, which is planning alone. Run it against two builds in turn, interleaved, to
compare them: the unit adds no plan pass, so the two must agree within the noise of a run.
"""

from __future__ import annotations

import json
import statistics
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from repark import ReparkSession

ROWS = 2_000
REPEATS = 15
VALUE = (
    "coalesce(t, NULL) AS a, CASE WHEN id % 2 = 0 THEN t ELSE NULL END AS b, "
    "if(id > 3, n, NULL) AS c, array(t, NULL)[0] AS d, coalesce(s, 'x') AS s"
)
COLUMNS = "id INT, a TIMESTAMP, b TIMESTAMP, c TIMESTAMP_NTZ, d TIMESTAMP, s STRING"
MOR = (
    ", 'write.delete.mode' = 'merge-on-read', 'write.update.mode' = 'merge-on-read', "
    "'write.merge.mode' = 'merge-on-read'"
)


def timed(spark: Any, statement: str) -> float:
    """Run ``statement`` and return its wall time in milliseconds."""
    began = time.perf_counter()
    spark.sql(statement).toArrow()
    return (time.perf_counter() - began) * 1_000


def create(spark: Any, table: str, mode: str = "") -> None:
    """Create one target table and seed half of the source rows."""
    spark.sql(
        f"CREATE TABLE {table} ({COLUMNS}) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '3'{mode})"
    )
    spark.sql(f"INSERT INTO {table} SELECT id, {VALUE} FROM ice.ns.src WHERE id % 2 = 0")


def measure(warehouse: Path) -> dict[str, float]:
    """Return the median milliseconds of each statement."""
    spark = (
        ReparkSession.builder.appName("ice-tsns-narrow-refuse-1-cost")
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
    merge = (
        "MERGE INTO {t} t USING (SELECT id, " + VALUE + " FROM ice.ns.src) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET a = s.a, b = coalesce(s.b, NULL), c = s.c "
        "WHEN NOT MATCHED THEN INSERT (id, a, b, c, d, s) VALUES (s.id, s.a, s.b, s.c, s.d, s.s)"
    )
    branch = "SELECT id, " + VALUE + " FROM ice.ns.src WHERE id > {i}"
    union = " UNION ALL ".join(branch.format(i=index) for index in range(50))
    times: dict[str, list[float]] = {"insert": [], "merge": [], "merge_mor": [], "explain": []}
    for index in range(REPEATS):
        create(spark, f"ice.ns.i{index}")
        times["insert"].append(
            timed(spark, f"INSERT INTO ice.ns.i{index} SELECT id, {VALUE} FROM ice.ns.src")
        )
        create(spark, f"ice.ns.m{index}")
        times["merge"].append(timed(spark, merge.format(t=f"ice.ns.m{index}")))
        create(spark, f"ice.ns.r{index}", MOR)
        times["merge_mor"].append(timed(spark, merge.format(t=f"ice.ns.r{index}")))
        times["explain"].append(timed(spark, "EXPLAIN " + union))
    spark.stop()
    return {name: round(statistics.median(values), 2) for name, values in times.items()}


def main() -> None:
    """Print the medians as one JSON line."""
    with tempfile.TemporaryDirectory(prefix="tsns-narrow-cost-") as warehouse:
        json.dump(measure(Path(warehouse)), sys.stdout)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
