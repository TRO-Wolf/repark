"""C-3 partition oracle: Spark 4.1.2 JDBC partitioned reads of Postgres recorded verbatim."""

from __future__ import annotations

import hashlib
import json
import os
import random
import re
import subprocess
import sys
from datetime import UTC, datetime
from importlib.util import find_spec
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

SCHEMA = "c3o"
TABLE = SCHEMA + ".t"
DRIVER_CLASS = "org.postgresql.Driver"
URL_ENV = "C3_PG_URL"
JAR_ENV = "C3_PGJDBC_JAR"
OUT_ENV = "C3_OUT"
GRID_OUT_ENV = "C3_GRID_OUT"
GRID_FILE = "c3_stride_grid.txt"
JARS_DIR = Path("/tmp/oc-worker/direct/wo/connect-1-6/d-m2/jars")
GRID_SEED = 20261007
GRID_RANDOM = 700
I64_MIN = -(2**63)
I64_MAX = 2**63 - 1
SETUP = (
    "DROP SCHEMA IF EXISTS c3o CASCADE",
    "CREATE SCHEMA c3o",
    "CREATE TABLE c3o.t (id int8, small int2, n int4, d date, ts timestamp,"
    " tstz timestamptz, num numeric(10,2), f float8, b bool, s text)",
    "INSERT INTO c3o.t SELECT g, g, g * 10, DATE '2024-01-01' + g::int,"
    " TIMESTAMP '2024-01-01 00:00:00' + g * INTERVAL '1 hour',"
    " TIMESTAMPTZ '2024-01-01 00:00:00+00' + g * INTERVAL '1 hour',"
    " g * 1.5, g * 0.5, g % 2 = 0, 'r' || g FROM generate_series(1, 20) g",
    "INSERT INTO c3o.t (id, n) VALUES (101, NULL), (102, NULL), (103, -500), (104, 5000),"
    " (105, 0), (106, 200)",
    'CREATE TABLE c3o.mx (id int8, "Mixed" int4)',
    "INSERT INTO c3o.mx SELECT g, g * 10 FROM generate_series(1, 20) g",
    'CREATE TABLE c3o.twins (id int8, "Mixed" int4, "mixed" int4)',
    "INSERT INTO c3o.twins SELECT g, g * 10, g * 10 FROM generate_series(1, 20) g",
)
ALL_FOUR = (
    ("partitionColumn", "n"),
    ("lowerBound", "0"),
    ("upperBound", "200"),
    ("numPartitions", "4"),
)
GRID_EDGES: tuple[tuple[int, int, int], ...] = (
    (0, 200, 4),
    (0, 100, 3),
    (1, 10, 3),
    (0, 10, 3),
    (0, 3, 10),
    (0, 1, 2),
    (0, 2, 2),
    (-100, 100, 4),
    (-100, -10, 3),
    (-7, 5, 5),
    (5, 5, 4),
    (0, 0, 2),
    (I64_MIN, I64_MAX, 2),
    (I64_MIN, I64_MAX, 3),
    (I64_MIN, I64_MAX, 4),
    (I64_MIN, I64_MAX, 7),
    (I64_MIN, I64_MAX, 64),
    (I64_MIN, 0, 2),
    (I64_MIN, 0, 5),
    (0, I64_MAX, 2),
    (0, I64_MAX, 3),
    (0, I64_MAX, 64),
    (I64_MAX - 10, I64_MAX, 4),
    (I64_MAX - 10, I64_MAX, 20),
    (I64_MAX - 1, I64_MAX, 2),
    (I64_MIN, I64_MIN + 10, 4),
    (I64_MIN, I64_MIN + 10, 20),
    (I64_MIN, I64_MIN + 1, 2),
    (-1, I64_MAX, 2),
    (-2, I64_MAX, 3),
    (I64_MIN, 1, 3),
    (1, 1_000_000_000_000_000_000, 3),
    (1, 1_000_000_000_000_000_000, 7),
    (-1_000_000_000_000_000_000, 7, 3),
    (999_999_999_999_999_999, 1_000_000_000_000_000_007, 3),
    (0, 1_000_000, 64),
    (0, 63, 64),
    (0, 64, 64),
    (0, 65, 64),
    (1, 0, 4),
)


class Pg:
    __slots__ = ("dsn", "jdbc_url", "password", "user")

    def __init__(self, dsn: str, jdbc_url: str, user: str, password: str) -> None:
        self.dsn = dsn
        self.jdbc_url = jdbc_url
        self.user = user
        self.password = password


def run_psql(dsn: str, script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["psql", dsn, "-X", "-q", "-t", "-A", "-v", "ON_ERROR_STOP=1", "-c", script],
        capture_output=True,
        check=False,
        text=True,
        timeout=120,
    )


def build_session(jar: Path) -> SparkSession:
    from pyspark.sql import SparkSession

    os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
    return (
        SparkSession.builder.master("local[2]")
        .appName("c3-partition-oracle")
        .config("spark.jars", str(jar))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def reader_of(spark: SparkSession, pg: Pg, options: tuple[tuple[str, str], ...]) -> Any:
    reader = (
        spark.read.format("jdbc")
        .option("url", pg.jdbc_url)
        .option("user", pg.user)
        .option("password", pg.password)
        .option("driver", DRIVER_CLASS)
    )
    for key, value in options:
        reader = reader.option(key, value)
    return reader


def call_or_none(target: Any, name: str) -> Any:
    call = getattr(target, name, None)
    if call is None:
        return None
    try:
        return call()
    except Exception:
        return None


def error_of(exc: Exception, pg: Pg) -> dict[str, Any]:
    java_exc = getattr(exc, "java_exception", None)
    targets = (exc,) if java_exc is None else (exc, java_exc)
    error_class: Any = None
    sqlstate: Any = None
    for target in targets:
        if error_class is None:
            error_class = call_or_none(target, "getErrorClass")
        if error_class is None:
            error_class = call_or_none(target, "getCondition")
        if sqlstate is None:
            sqlstate = call_or_none(target, "getSqlState")
    message = str(exc)[:500].replace(pg.jdbc_url, "<pg-url>")
    if pg.password:
        message = message.replace(pg.password, "<pg-password>")
    return {
        "class": type(exc).__module__ + "." + type(exc).__name__,
        "error_class": error_class,
        "message": message,
        "sqlstate": sqlstate,
    }


def where_clauses(frame: DataFrame) -> list[str | None]:
    parts = frame._jdf.queryExecution().analyzed().relation().parts()
    return [part.whereClause() for part in parts]


def placement(frame: DataFrame) -> list[list[int]]:
    from pyspark.sql.functions import spark_partition_id

    rows = frame.select(spark_partition_id().alias("p"), "id").collect()
    count = frame.rdd.getNumPartitions()
    placed: list[list[int]] = [[] for _ in range(count)]
    for row in rows:
        placed[row["p"]].append(row["id"])
    return [sorted(ids) for ids in placed]


def shape_cell(
    spark: SparkSession,
    pg: Pg,
    cell: str,
    options: tuple[tuple[str, str], ...],
) -> dict[str, Any]:
    shown = [list(pair) for pair in options]
    entry: dict[str, Any] = {
        "id": "C3-" + cell,
        "options": shown,
        "error": None,
        "where": None,
        "placement": None,
        "rows": None,
    }
    try:
        frame = reader_of(spark, pg, options).load()
        entry["where"] = where_clauses(frame)
        entry["placement"] = placement(frame)
        entry["rows"] = frame.count()
    except Exception as exc:
        entry["error"] = error_of(exc, pg)
    return entry


def with_table(*options: tuple[str, str]) -> tuple[tuple[str, str], ...]:
    return (("dbtable", TABLE), *options)


def replaced(key: str, value: str) -> tuple[tuple[str, str], ...]:
    return with_table(*((name, value if name == key else old) for name, old in ALL_FOUR))


def without(key: str) -> tuple[tuple[str, str], ...]:
    return with_table(*(pair for pair in ALL_FOUR if pair[0] != key))


def bounded(column: str, lower: str, upper: str, count: str) -> tuple[tuple[str, str], ...]:
    return with_table(
        ("partitionColumn", column),
        ("lowerBound", lower),
        ("upperBound", upper),
        ("numPartitions", count),
    )


def on_table(table: str, column: str) -> tuple[tuple[str, str], ...]:
    return (
        ("dbtable", table),
        ("partitionColumn", column),
        ("lowerBound", "0"),
        ("upperBound", "200"),
        ("numPartitions", "4"),
    )


def define_shapes() -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    subquery = "(SELECT id, n FROM c3o.t WHERE id < 100) AS sub"
    return (
        ("P01-all-four", with_table(*ALL_FOUR)),
        ("P02-unpartitioned", with_table()),
        ("M01-no-column", without("partitionColumn")),
        ("M02-no-lower", without("lowerBound")),
        ("M03-no-upper", without("upperBound")),
        ("M04-no-count", without("numPartitions")),
        ("M05-count-only", with_table(("numPartitions", "4"))),
        ("M06-column-only", with_table(("partitionColumn", "n"))),
        ("M07-bounds-only", with_table(("lowerBound", "0"), ("upperBound", "200"))),
        ("B01-reversed", bounded("n", "200", "0", "4")),
        ("B02-equal", bounded("n", "50", "50", "4")),
        ("B03-narrow", bounded("n", "0", "3", "10")),
        ("B04-inside-data", bounded("n", "50", "150", "4")),
        ("B05-negative", bounded("n", "-600", "-400", "2")),
        ("B06-lower-not-integer", bounded("n", "abc", "200", "4")),
        ("B07-lower-fraction", bounded("n", "1.5", "200", "4")),
        ("B08-upper-past-i64", bounded("n", "0", "9223372036854775808", "4")),
        ("N01-count-zero", replaced("numPartitions", "0")),
        ("N02-count-one", replaced("numPartitions", "1")),
        ("N03-count-negative", replaced("numPartitions", "-1")),
        ("N04-count-not-integer", replaced("numPartitions", "x")),
        ("N05-count-two", replaced("numPartitions", "2")),
        ("N06-count-large", replaced("numPartitions", "64")),
        ("T01-int8", bounded("id", "0", "20", "4")),
        ("T02-int2", bounded("small", "0", "20", "4")),
        ("T03-text", bounded("s", "0", "20", "4")),
        ("T04-date-date-bounds", bounded("d", "2024-01-02", "2024-01-21", "4")),
        ("T05-date-integer-bounds", bounded("d", "0", "20", "4")),
        (
            "T06-timestamp",
            bounded("ts", "2024-01-01 00:00:00", "2024-01-02 00:00:00", "4"),
        ),
        (
            "T07-timestamptz",
            bounded("tstz", "2024-01-01 00:00:00", "2024-01-02 00:00:00", "4"),
        ),
        ("T08-numeric", bounded("num", "0", "30", "4")),
        ("T09-float8", bounded("f", "0", "10", "4")),
        ("T10-bool", bounded("b", "0", "1", "2")),
        ("C01-missing-column", bounded("nope", "0", "200", "4")),
        ("C02-upper-case-column", bounded("N", "0", "200", "4")),
        ("C03-quoted-column", bounded('"n"', "0", "200", "4")),
        ("C04-quoted-upper-case-column", bounded('"N"', "0", "200", "4")),
        ("C05-quoted-other-case", on_table("c3o.mx", '"mixed"')),
        ("C06-bare-other-case", on_table("c3o.mx", "MIXED")),
        ("C07-quoted-exact", on_table("c3o.mx", '"Mixed"')),
        ("C08-twins-unpartitioned", (("dbtable", "c3o.twins"),)),
        ("C09-twins-quoted-exact", on_table("c3o.twins", '"mixed"')),
        ("C10-twins-bare", on_table("c3o.twins", "mixed")),
        ("N07-count-past-int", replaced("numPartitions", "3000000000")),
        ("N08-count-int-max", bounded("n", "0", "3", "2147483647")),
        ("N09-count-padded", replaced("numPartitions", " 4")),
        (
            "T11-timestamp-date-bounds",
            bounded("ts", "2024-01-01", "2024-01-02", "4"),
        ),
        (
            "Q01-query-with-column",
            (("query", "SELECT id, n FROM c3o.t"), *ALL_FOUR),
        ),
        ("Q02-subquery-dbtable", (("dbtable", subquery), *ALL_FOUR)),
    )


def jdbc_method_cell(spark: SparkSession, pg: Pg) -> dict[str, Any]:
    props = {"user": pg.user, "password": pg.password, "driver": DRIVER_CLASS}
    entry: dict[str, Any] = {
        "id": "C3-J01-jdbc-method",
        "options": [["column", "n"], ["lowerBound", "0"], ["upperBound", "200"], ["n", "4"]],
        "error": None,
        "where": None,
        "placement": None,
        "rows": None,
    }
    try:
        frame = spark.read.jdbc(
            pg.jdbc_url,
            TABLE,
            column="n",
            lowerBound=0,
            upperBound=200,
            numPartitions=4,
            properties=props,
        )
        entry["where"] = where_clauses(frame)
        entry["placement"] = placement(frame)
        entry["rows"] = frame.count()
    except Exception as exc:
        entry["error"] = error_of(exc, pg)
    return entry


def properties_cell(spark: SparkSession, pg: Pg) -> dict[str, Any]:
    props = {"user": pg.user, "password": pg.password, "driver": DRIVER_CLASS}
    props.update(dict(ALL_FOUR))
    entry: dict[str, Any] = {
        "id": "C3-J02-jdbc-properties",
        "options": [list(pair) for pair in ALL_FOUR],
        "error": None,
        "where": None,
        "placement": None,
        "rows": None,
    }
    try:
        frame = spark.read.jdbc(pg.jdbc_url, TABLE, properties=props)
        entry["where"] = where_clauses(frame)
        entry["placement"] = placement(frame)
        entry["rows"] = frame.count()
    except Exception as exc:
        entry["error"] = error_of(exc, pg)
    return entry


def limit_cell(spark: SparkSession, pg: Pg) -> dict[str, Any]:
    frame = reader_of(spark, pg, with_table(*ALL_FOUR)).load().limit(3)
    utils = spark._jvm.PythonSQLUtils
    plan = str(utils.explainString(frame._jdf.queryExecution(), "formatted"))
    return {
        "id": "C3-L01-limit-over-partitions",
        "explain": plan,
        "rows": len(frame.collect()),
    }


def grid_triples() -> list[tuple[int, int, int]]:
    rng = random.Random(GRID_SEED)
    spans = (10, 1000, 10**6, 10**12, 10**15, 10**17, 10**18, I64_MAX)
    counts = (2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 16, 31, 32, 33, 63, 64)
    triples = list(GRID_EDGES)
    while len(triples) < len(GRID_EDGES) + GRID_RANDOM:
        span = rng.choice(spans)
        first = rng.randint(-span, span)
        second = rng.randint(-span, span)
        if rng.random() < 0.15:
            second = first + rng.randint(0, 80)
        lower = max(I64_MIN, min(first, second))
        upper = min(I64_MAX, max(first, second))
        triples.append((lower, upper, rng.choice(counts)))
    return triples


def cuts_of(where: list[str | None]) -> list[int]:
    cuts: list[int] = []
    for clause in where[:-1]:
        found = re.search(r'"n" < (-?\d+)', clause or "")
        if found is None:
            message = "no upper bound in " + repr(clause)
            raise ValueError(message)
        cuts.append(int(found.group(1)))
    return cuts


def grid_cell(spark: SparkSession, pg: Pg, triple: tuple[int, int, int]) -> dict[str, Any]:
    lower, upper, count = triple
    entry: dict[str, Any] = {"lower": lower, "upper": upper, "count": count}
    try:
        frame = reader_of(spark, pg, bounded("n", str(lower), str(upper), str(count))).load()
        where = where_clauses(frame)
        entry["partitions"] = len(where)
        entry["cuts"] = cuts_of(where)
    except Exception as exc:
        entry["error"] = error_of(exc, pg)["message"][:160]
    return entry


def grid_line(entry: dict[str, Any]) -> str:
    head = "{} {} {}".format(entry["lower"], entry["upper"], entry["count"])
    if "error" in entry:
        return head + " refused"
    return " ".join([head, "cuts", *(str(cut) for cut in entry["cuts"])])


def find_jar() -> Path | None:
    override = os.environ.get(JAR_ENV)
    if override:
        return Path(override) if Path(override).is_file() else None
    jars = sorted(JARS_DIR.glob("postgresql-42.7.*.jar"))
    return jars[-1] if jars else None


def pg_from_env() -> Pg | None:
    dsn = os.environ.get(URL_ENV)
    if not dsn:
        return None
    parsed = urlparse(dsn)
    jdbc_url = f"jdbc:postgresql://{parsed.hostname}:{parsed.port}{parsed.path}"
    return Pg(dsn, jdbc_url, parsed.username or "", parsed.password or "")


def record_all(spark: SparkSession, pg: Pg) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    cells: list[dict[str, Any]] = []
    for cell, options in define_shapes():
        cells.append(shape_cell(spark, pg, cell, options))
    cells.append(jdbc_method_cell(spark, pg))
    cells.append(properties_cell(spark, pg))
    cells.append(limit_cell(spark, pg))
    for entry in cells:
        print(json.dumps(entry, sort_keys=True))
    grid = [grid_cell(spark, pg, triple) for triple in grid_triples()]
    return cells, grid


def main() -> int:
    pg = pg_from_env()
    jar = find_jar()
    if pg is None or jar is None or find_spec("pyspark") is None:
        print("SKIP: needs " + URL_ENV + ", the pgjdbc jar and pyspark")
        return 0
    done = run_psql(pg.dsn, ";\n".join(SETUP) + ";")
    if done.returncode != 0:
        print("SKIP: setup failed: " + (done.stderr or done.stdout).strip())
        return 0
    version = run_psql(pg.dsn, "SHOW server_version").stdout.strip()
    spark = build_session(jar)
    try:
        cells, grid = record_all(spark, pg)
        spark_version = spark.version
    finally:
        spark.stop()
        run_psql(pg.dsn, "DROP SCHEMA IF EXISTS c3o CASCADE")
    here = Path(__file__).resolve().parent
    grid_text = "\n".join(grid_line(entry) for entry in grid) + "\n"
    recording = {
        "preamble": {
            "date": datetime.now(UTC).date().isoformat(),
            "grid_random": GRID_RANDOM,
            "grid_seed": GRID_SEED,
            "grid_sha256": hashlib.sha256(grid_text.encode()).hexdigest(),
            "grid_triples": len(grid),
            "pgjdbc": jar.name,
            "pgjdbc_sha256": hashlib.sha256(jar.read_bytes()).hexdigest(),
            "postgres": version,
            "setup": list(SETUP),
            "spark": spark_version,
        },
        "cells": cells,
    }
    out = Path(os.environ.get(OUT_ENV) or here / "c3_partition_oracle.json")
    out.write_text(json.dumps(recording, indent=1, sort_keys=True) + "\n")
    Path(os.environ.get(GRID_OUT_ENV) or here / GRID_FILE).write_text(grid_text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
