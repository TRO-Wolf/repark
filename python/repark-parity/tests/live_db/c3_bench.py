"""C-3 benchmark: one 10M-row mixed-type read on RePark, ConnectorX and pandas + SQLAlchemy.

Not collected by pytest. Run it from a scratch environment that holds a release `repark` wheel
and the comparators, with `REPARK_PG_URL` pointing at the C-0 container:

    python c3_bench.py load      create and fill the table
    python c3_bench.py run       time every case and print the JSON report
    python c3_bench.py drop      drop the table

`run` starts one fresh interpreter per case, so no engine's memory or pool is shared with another.
"""

from __future__ import annotations

import gc
import json
import os
import platform
import statistics
import subprocess
import sys
import time
from importlib import metadata
from typing import Any
from urllib.parse import urlparse

SCHEMA = "c3_bench"
TABLE = SCHEMA + ".mixed"
QUERY = "SELECT * FROM " + TABLE
ROWS = int(os.environ.get("C3_BENCH_ROWS", "10000000"))
RUNS = int(os.environ.get("C3_BENCH_RUNS", "3"))
CREATE = (
    "CREATE TABLE {table} AS SELECT g::int8 AS id, (g % 100000)::int4 AS qty, "
    "(g % 100)::int2 AS grade, (g * 0.37)::float8 AS score, "
    "((g % 1000000) / 100.0)::numeric(12,2) AS amount, "
    "'customer-' || (g % 50000) AS name, DATE '2020-01-01' + (g % 2000)::int AS day, "
    "TIMESTAMPTZ '2020-01-01 00:00:00+00' + g * INTERVAL '1 second' AS at, g % 3 = 0 AS flag "
    "FROM generate_series(1, {rows}) g"
)
CASES = (
    "repark-1",
    "repark-4",
    "repark-8",
    "repark-8-pool4",
    "connectorx-1",
    "connectorx-4",
    "connectorx-8",
    "pandas-1",
)
PAIRS = (
    ("repark-1", "connectorx-1"),
    ("repark-4", "connectorx-4"),
    ("repark-8", "connectorx-8"),
    ("repark-8-pool4", "connectorx-8"),
)
PACKAGES = ("repark", "connectorx", "pandas", "SQLAlchemy", "psycopg2-binary", "pyarrow")


def url() -> str:
    """Return the container URL, or stop with the instruction to start it."""
    found = os.environ.get("REPARK_PG_URL")
    if not found:
        sys.exit("REPARK_PG_URL unset: make pg-up")
    return found


def admin() -> Any:
    """Open an autocommit psycopg2 connection to the container."""
    import psycopg2

    connection = psycopg2.connect(url())
    connection.autocommit = True
    return connection


def load() -> None:
    """Create the benchmark table, key it and analyse it."""
    connection = admin()
    with connection.cursor() as cursor:
        cursor.execute("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE")
        cursor.execute("CREATE SCHEMA " + SCHEMA)
        cursor.execute(CREATE.format(table=TABLE, rows=ROWS))
        cursor.execute("ALTER TABLE " + TABLE + " ADD PRIMARY KEY (id)")
        cursor.execute("VACUUM ANALYZE " + TABLE)
        cursor.execute("SELECT count(*), pg_total_relation_size(%s) FROM " + TABLE, (TABLE,))
        print(json.dumps(dict(zip(("rows", "bytes"), cursor.fetchone(), strict=True))))
    connection.close()


def drop() -> None:
    """Drop the benchmark schema."""
    connection = admin()
    with connection.cursor() as cursor:
        cursor.execute("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE")
    connection.close()


def read_repark(partitions: int, pool: int) -> tuple[int, str]:
    """Read the table through `spark.read.jdbc` into one Arrow table."""
    import repark

    spark = repark.ReparkSession.builder.config("spark.sql.session.timeZone", "UTC").getOrCreate()
    properties = {"sslmode": "disable", "pool_max_size": str(pool)}
    if partitions > 1:
        frame = spark.read.jdbc(
            url(),
            TABLE,
            column="id",
            lowerBound=1,
            upperBound=ROWS,
            numPartitions=partitions,
            properties=properties,
        )
    else:
        frame = spark.read.jdbc(url(), TABLE, properties=properties)
    table = frame.to_arrow()
    return table.num_rows, str(table.schema).replace("\n", "; ")


def read_connectorx(partitions: int) -> tuple[int, str]:
    """Read the table through ConnectorX `read_sql` with an Arrow return."""
    import connectorx

    if partitions > 1:
        table = connectorx.read_sql(
            url(),
            QUERY,
            return_type="arrow",
            partition_on="id",
            partition_range=(1, ROWS),
            partition_num=partitions,
        )
    else:
        table = connectorx.read_sql(url(), QUERY, return_type="arrow")
    return table.num_rows, str(table.schema).replace("\n", "; ")


def read_pandas() -> tuple[int, str]:
    """Read the table through pandas `read_sql` over a SQLAlchemy engine."""
    import pandas
    import sqlalchemy

    parsed = urlparse(url())
    engine = sqlalchemy.create_engine(parsed._replace(scheme="postgresql+psycopg2").geturl())
    try:
        frame = pandas.read_sql(QUERY, engine)
    finally:
        engine.dispose()
    kinds = "; ".join(name + ": " + str(kind) for name, kind in frame.dtypes.items())
    return len(frame), kinds


def read_case(case: str) -> tuple[int, str]:
    """Run one read of the named case."""
    engine, _, rest = case.partition("-")
    partitions = int(rest.split("-")[0])
    if engine == "repark":
        pool = 4 if case.endswith("pool4") else max(partitions, 4)
        return read_repark(partitions, pool)
    if engine == "connectorx":
        return read_connectorx(partitions)
    return read_pandas()


def time_case(case: str) -> None:
    """Time `RUNS` reads of one case in this interpreter and print one JSON line."""
    seconds: list[float] = []
    rows = 0
    shape = ""
    for _ in range(RUNS):
        gc.collect()
        started = time.perf_counter()
        rows, shape = read_case(case)
        seconds.append(time.perf_counter() - started)
    print(json.dumps({"case": case, "rows": rows, "seconds": seconds, "schema": shape}))


def versions() -> dict[str, str]:
    """Name the installed version of every package the report depends on."""
    found: dict[str, str] = {}
    for package in PACKAGES:
        try:
            found[package] = metadata.version(package)
        except metadata.PackageNotFoundError:
            found[package] = "absent"
    return found


def run() -> None:
    """Time every case in its own interpreter and print the report."""
    results: dict[str, dict[str, Any]] = {}
    for case in CASES:
        done = subprocess.run(
            [sys.executable, __file__, "case", case],
            capture_output=True,
            check=False,
            text=True,
        )
        if done.returncode != 0:
            results[case] = {"error": done.stderr.strip().splitlines()[-1:]}
            continue
        entry = json.loads(done.stdout.strip().splitlines()[-1])
        median = statistics.median(entry["seconds"])
        entry["median_seconds"] = round(median, 3)
        entry["rows_per_second"] = round(entry["rows"] / median)
        entry["seconds"] = [round(value, 3) for value in entry["seconds"]]
        results[case] = entry
    factors: dict[str, float] = {}
    for ours, theirs in PAIRS:
        if "rows_per_second" in results.get(ours, {}) and "rows_per_second" in results.get(
            theirs, {}
        ):
            ratio = results[ours]["rows_per_second"] / results[theirs]["rows_per_second"]
            factors[ours + " / " + theirs] = round(ratio, 3)
    report = {
        "query": QUERY,
        "rows": ROWS,
        "runs": RUNS,
        "python": platform.python_version(),
        "cpus": len(os.sched_getaffinity(0)),
        "versions": versions(),
        "cases": results,
        "repark_over_connectorx": factors,
    }
    print(json.dumps(report, indent=1, sort_keys=True))


def main() -> None:
    """Dispatch the sub-command."""
    command = sys.argv[1] if len(sys.argv) > 1 else ""
    if command == "load":
        load()
    elif command == "drop":
        drop()
    elif command == "run":
        run()
    elif command == "case" and len(sys.argv) > 2:
        time_case(sys.argv[2])
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
