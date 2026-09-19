"""Record or re-check the ICE-DROP-NS-1 Spark oracle cells on live PySpark 4.1.2.

Usage (an interpreter with PySpark 4.1.2 on the path)::

    PYTHONPATH=<sparkenv site-packages> JAVA_HOME=<java-17> SPARK_LOCAL_IP=127.0.0.1 \
        python python/repark/tests/_record_ice_drop_ns_1_oracle.py \
        --warehouse /tmp/drop-ns-oracle-wh record
    ... check

``record`` prints the fixture JSON to stdout; ``check`` re-derives every cell
and exits non-zero naming the first mismatch against the committed
``ice_drop_ns_1_spark_oracle.json``. The Iceberg runtime GAV comes from
:mod:`_oracle_pins` (CP-8: never restate a version literal). Namespace stems
derive from the cell id exactly like the measuring harness, so ``check``
reproduces the committed namespace names.

pins: ice-drop-ns-1/C-001
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV

FIXTURE = Path(__file__).with_name("ice_drop_ns_1_spark_oracle.json")
ICEBERG_SPARK_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
INMEMORY_CATALOG_CLASS = "org.apache.iceberg.spark.SparkCatalog"
INMEMORY_CATALOG_IMPL = "org.apache.iceberg.inmemory.InMemoryCatalog"
HADOOP_CATALOG_TYPE = "hadoop"
CALL_COUNTER_PATTERN = re.compile(r"calling o\d+\.sql")
NON_ALNUM_PATTERN = re.compile(r"[^a-z0-9]")
STACKTRACE_PATTERN = re.compile(r"\n\s*(JVM stacktrace|at |\tat ).*", flags=re.S)

CellSpec = tuple[str, str, str, bool, bool, bool]

CELL_SPECS: tuple[CellSpec, ...] = (
    ("NONEMPTY", "DROP NAMESPACE non-empty", "DROP NAMESPACE {N}", True, False, False),
    (
        "CASCADE",
        "DROP NAMESPACE non-empty CASCADE",
        "DROP NAMESPACE {N} CASCADE",
        True,
        False,
        False,
    ),
    (
        "RESTRICT",
        "DROP NAMESPACE non-empty RESTRICT",
        "DROP NAMESPACE {N} RESTRICT",
        True,
        False,
        False,
    ),
    (
        "IFEXISTS",
        "DROP NAMESPACE IF EXISTS non-empty",
        "DROP NAMESPACE IF EXISTS {N}",
        True,
        False,
        False,
    ),
    (
        "IFEXISTS-CASCADE",
        "DROP NAMESPACE IF EXISTS non-empty CASCADE",
        "DROP NAMESPACE IF EXISTS {N} CASCADE",
        True,
        False,
        False,
    ),
    ("DATABASE", "DROP DATABASE non-empty", "DROP DATABASE {N}", True, False, False),
    (
        "SCHEMA-CASCADE",
        "DROP SCHEMA non-empty CASCADE",
        "DROP SCHEMA {N} CASCADE",
        True,
        False,
        False,
    ),
    ("EMPTY", "DROP NAMESPACE empty", "DROP NAMESPACE {N}", False, False, False),
    (
        "EMPTY-CASCADE",
        "DROP NAMESPACE empty CASCADE",
        "DROP NAMESPACE {N} CASCADE",
        False,
        False,
        False,
    ),
    (
        "AFTER-DROP-TABLE",
        "DROP NAMESPACE after its only table dropped",
        "DROP NAMESPACE {N}",
        True,
        False,
        True,
    ),
    (
        "CHILD-NS",
        "DROP NAMESPACE with a child namespace only",
        "DROP NAMESPACE {N}",
        False,
        True,
        False,
    ),
    (
        "CHILD-NS-CASCADE",
        "DROP NAMESPACE with a child namespace CASCADE",
        "DROP NAMESPACE {N} CASCADE",
        False,
        True,
        False,
    ),
    ("MISSING", "DROP NAMESPACE missing", "DROP NAMESPACE {N}_missing", False, False, False),
)

CATALOGS: tuple[str, ...] = ("sc", "hc")


def leaf_of(cell_id: str) -> str:
    """Derive the harness namespace leaf for a cell id."""
    safe = NON_ALNUM_PATTERN.sub("_", cell_id.lower())
    return f"n_t_{safe}"


def spark_session(warehouse: Path) -> Any:
    """Build the live Spark session with InMemory ``sc`` and Hadoop ``hc`` catalogs."""
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[1]")
        .appName("ice-drop-ns-1-oracle")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config("spark.sql.extensions", ICEBERG_SPARK_EXTENSIONS)
        .config("spark.sql.catalog.sc", INMEMORY_CATALOG_CLASS)
        .config("spark.sql.catalog.sc.catalog-impl", INMEMORY_CATALOG_IMPL)
        .config("spark.sql.catalog.sc.warehouse", str(warehouse))
        .config("spark.sql.catalog.hc", INMEMORY_CATALOG_CLASS)
        .config("spark.sql.catalog.hc.type", HADOOP_CATALOG_TYPE)
        .config("spark.sql.catalog.hc.warehouse", str(warehouse / "hc"))
        .config("spark.sql.warehouse.dir", str(warehouse / "spark-warehouse"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
    )
    ivy = os.environ.get("REPARK_ORACLE_IVY")
    if ivy:
        builder = builder.config("spark.jars.ivy", ivy)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    return session


def error_info(error: BaseException) -> dict[str, str]:
    """Reduce a PySpark exception to its class and its stacktrace-free message."""
    message = str(error).strip()
    message = STACKTRACE_PATTERN.sub("", message)
    return {"type": type(error).__name__, "msg": message[:700]}


def collected_rows(spark: Any, sql: str) -> list[list[Any]]:
    """Collect a SQL statement into sorted plain rows."""
    collected = spark.sql(sql).collect()
    return sorted((list(row) for row in collected), key=repr)


def run_cell(spark: Any, catalog: str, spec: CellSpec) -> dict[str, Any]:
    """Run one oracle cell against live Spark and return its observation record."""
    suffix, title, template, setup_table, has_child, drop_first = spec
    cell_id = f"NS-{suffix}-{catalog.upper()}"
    leaf = leaf_of(cell_id)
    namespace = f"{catalog}.{leaf}"
    spark.sql(f"CREATE NAMESPACE {namespace}").collect()
    if setup_table:
        spark.sql(f"CREATE TABLE {namespace}.t (id INT) USING iceberg").collect()
        spark.sql(f"INSERT INTO {namespace}.t VALUES (1)").collect()
    if has_child:
        spark.sql(f"CREATE NAMESPACE {namespace}.c").collect()
    if drop_first:
        spark.sql(f"DROP TABLE {namespace}.t").collect()
    error: dict[str, str] | None = None
    try:
        spark.sql(template.format(N=namespace)).collect()
    except Exception as failure:
        info = error_info(failure)
        error = {"type": info["type"], "msg": info["msg"][:300]}
    observation: dict[str, Any] = {
        "err": None if error is None else [["msg", error["msg"]], ["type", error["type"]]],
        "ns_after": collected_rows(spark, f"SHOW NAMESPACES IN {catalog} LIKE '{leaf}'"),
    }
    if setup_table and not drop_first:
        try:
            observation["table_after"] = collected_rows(spark, f"SELECT * FROM {namespace}.t")
        except Exception as failure:
            observation["table_after"] = "ERR " + type(failure).__name__
    try:
        spark.sql(f"DROP TABLE IF EXISTS {namespace}.t").collect()
        spark.sql(f"DROP NAMESPACE IF EXISTS {namespace}.c").collect()
        spark.sql(f"DROP NAMESPACE IF EXISTS {namespace}").collect()
    except Exception:
        pass
    return {
        "id": cell_id,
        "group": "NS",
        "title": title,
        "engine": "spark",
        "status": "ok",
        "obs": observation,
        "notes": [],
        "secs": 0.0,
    }


def record_all(warehouse: Path) -> list[dict[str, Any]]:
    """Run every cell on live Spark and return the fixture-shaped records."""
    if warehouse.exists():
        shutil.rmtree(warehouse)
    warehouse.mkdir(parents=True)
    spark = spark_session(warehouse)
    try:
        cells: list[dict[str, Any]] = []
        for catalog in CATALOGS:
            for spec in CELL_SPECS:
                cells.append(run_cell(spark, catalog, spec))
        return cells
    finally:
        spark.stop()


def normalized_message(message: str) -> str:
    """Hide the volatile PySpark call-site counter so runs compare equal."""
    return CALL_COUNTER_PATTERN.sub("calling o0.sql", message)


def normalized_cell(cell: dict[str, Any]) -> dict[str, Any]:
    """Project a cell record onto its drift-compared shape."""
    observation = dict(cell.get("obs", {}))
    error = observation.get("err")
    if error is not None:
        pairs = dict(error)
        pairs["msg"] = normalized_message(str(pairs.get("msg", "")))
        observation["err"] = pairs
    return {"id": cell.get("id"), "obs": observation}


def check_against_fixture(derived: list[dict[str, Any]]) -> list[str]:
    """Compare live-derived cells against the fixture, ignoring timings."""
    expected = json.loads(FIXTURE.read_text(encoding="utf-8"))
    wanted = {cell["id"]: normalized_cell(cell) for cell in expected}
    mismatches: list[str] = []
    for cell in derived:
        want = wanted.get(cell["id"])
        if want is None:
            mismatches.append(f"{cell['id']}: extra cell not in fixture")
        elif normalized_cell(cell) != want:
            mismatches.append(
                f"{cell['id']}:\n  want={json.dumps(want, default=str)[:600]}"
                f"\n  got ={json.dumps(normalized_cell(cell), default=str)[:600]}"
            )
    for cell_id in wanted:
        if cell_id not in {cell["id"] for cell in derived}:
            mismatches.append(f"{cell_id}: fixture cell missing from derivation")
    return mismatches


def main(argv: list[str]) -> int:
    """Record the oracle fixture or check the fixture against live Spark."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("mode", choices=("record", "check"))
    args = parser.parse_args(argv)
    derived = record_all(args.warehouse)
    if args.mode == "record":
        print(json.dumps(derived, indent=1, default=str))
        return 0
    mismatches = check_against_fixture(derived)
    if mismatches:
        print(f"{len(mismatches)} oracle cell(s) drifted from {FIXTURE}:", flush=True)
        for mismatch in mismatches:
            print(mismatch, flush=True)
        return 1
    print(f"oracle fixture {FIXTURE.name} reproduces on live Spark", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
