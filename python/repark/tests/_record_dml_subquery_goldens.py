"""Record mode for the G3-E8 DML-subquery corpus — re-derive every Spark half from live Iceberg.

NOT a ``test_`` module: pytest never collects it. It is the driver that produced the recorded Spark
halves in ``test_dml_subquery_parity.py``, committed so the "recorded against live PySpark 4.1.2 +
Iceberg" claim is falsifiable from inside the repo rather than only from the session that made it.

**Why this driver provisions Iceberg.** Vanilla PySpark cannot run `DELETE`/`UPDATE` against temp
views — row-level DML needs a real table format. The live/record oracle elsewhere in the suite is
plain PySpark 4.1.2 with no Iceberg jar. This driver therefore pins and fetches the Iceberg Spark
runtime by Maven coordinates (never commits the binary) and stands up a local Hadoop warehouse
catalog.

**Pinned GAV (same ruling as the MERGE corpus — exact Spark-minor match):**

    org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0

Fetched at record time via ``spark.jars.packages``. CI stays JVM-free — this driver is never
collected by pytest and is not on any CI path.

**Recipe (re-derivable):**

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_dml_subquery_goldens.py

Requires: Java 17 (zulu-17), ``uv sync --extra record`` (pyspark==4.1.2), network on the first
resolve for the Ivy/Maven fetch (cached thereafter).

**JVM coordination.** Only ONE local Spark driver at a time: before running, check that no other
lane holds one (``pgrep -af 'pyspark|SparkSubmit'``, ignoring any standing container cluster —
``deploy.master`` / ``deploy.worker`` / ``HistoryServer`` / ``HiveThriftServer2`` /
``CoarseGrainedExecutorBackend`` are somebody else's long-lived infrastructure, not a record run)
and wait until it is clear.

The driver imports ``ROWS`` from the COMMITTED test module and runs each row's OWN lifecycle recipe
(the same helpers the suite uses) on a live Spark+Iceberg session. Exit 0 means every recorded half
still reproduces: schema name/type/nullability first, then values. It never edits the corpus —
re-recording is a human decision, and a driver that rewrote its own oracle would launder drift.
"""

from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

# Run as a script from anywhere: the corpus is a sibling module, imported by name so the driver
# reads the SAME rows the suite asserts (never a copy).
sys.path.insert(0, str(Path(__file__).resolve().parent))

if TYPE_CHECKING:
    import pyarrow as pa
    from test_dml_subquery_parity import DmlSubqueryRow


# Spark-side catalog name (Hadoop catalog over a local warehouse). Distinct from repark's "mem".
SPARK_CATALOG = "local"
SPARK_NAMESPACE = "ns"


def _signature(table: pa.Table) -> list[tuple[str, str, bool]]:
    """The schema signature the parity comparator asserts on: name, Arrow type, nullability."""
    return [(field.name, str(field.type), field.nullable) for field in table.schema]


def _spark_iceberg_session(warehouse: Path) -> Any:
    """Build the recorded-basis Spark session with the pinned Iceberg runtime + local catalog.

    Config surface is pinned here (not guessed): ``local[2]``, ANSI on,
    ``spark.sql.shuffle.partitions=2``, UI off — the basis every recorded corpus in this suite
    uses — plus the Iceberg extensions and a Hadoop catalog rooted at ``warehouse``.
    """
    from pyspark.sql import SparkSession
    from test_dml_subquery_parity import ICEBERG_SPARK_RUNTIME_GAV

    return (
        SparkSession.builder.master("local[2]")
        .appName("repark-dml-subquery-record")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{SPARK_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{SPARK_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{SPARK_CATALOG}.warehouse", str(warehouse))
        .getOrCreate()
    )


def _record_row(spark: Any, row: DmlSubqueryRow) -> str | None:
    """Re-derive one row's Spark half. Returns a mismatch report, or None when it matches.

    Both kinds run the same Spark-side recipe: a `content` row's Spark half and a `split` row's
    Spark half are both "the statement succeeded, here is the table afterwards". Only repark's
    half differs between the kinds, and repark is not this driver's business.
    """
    from test_dml_subquery_parity import run_dml_lifecycle

    assert row.spark is not None
    live = run_dml_lifecycle(spark, row, catalog=SPARK_CATALOG, namespace=SPARK_NAMESPACE)
    recorded = row.spark
    if _signature(live) == _signature(recorded) and live.equals(recorded):
        print(f"[G3-E8] {row.name} [{row.kind}] PASS")
        return None
    return (
        f"[G3-E8] {row.name} [{row.kind}] MISMATCH\n"
        f"    live schema     = {_signature(live)}\n"
        f"    recorded schema = {_signature(recorded)}\n"
        f"    live rows       = {live.to_pydict()}\n"
        f"    recorded rows   = {recorded.to_pydict()}"
    )


def _assert_warehouse_clean(spark: Any) -> str | None:
    """Cleanup probe: after every row, the Spark catalog must list no per-row table."""
    from test_dml_subquery_parity import ROWS

    try:
        listed = spark.sql(f"SHOW TABLES IN {SPARK_CATALOG}.{SPARK_NAMESPACE}").collect()
        names = {row.tableName if hasattr(row, "tableName") else row[1] for row in listed}
    except Exception as exc:
        return f"[G3-E8] cleanup probe: SHOW TABLES failed: {exc}"
    stray = sorted(
        name for name in names if any(name in (row.name, f"{row.name}_keys") for row in ROWS)
    )
    if stray:
        return f"[G3-E8] cleanup probe FAIL: stray tables after the run: {stray}"
    print(f"[G3-E8] lifecycle cleanup PASS (tables={sorted(names)})")
    return None


def main() -> int:
    """Re-derive every recorded Spark half; return 0 when all of them still reproduce."""
    from test_dml_subquery_parity import ICEBERG_SPARK_RUNTIME_GAV, ROWS

    warehouse = Path(tempfile.mkdtemp(prefix="repark-dml-subquery-record-"))
    print(f"record warehouse = {warehouse}")
    print(f"Iceberg GAV      = {ICEBERG_SPARK_RUNTIME_GAV}")
    spark = _spark_iceberg_session(warehouse)
    try:
        spark.sparkContext.setLogLevel("ERROR")
        mismatches: list[str] = []
        for row in ROWS:
            report = _record_row(spark, row)
            if report is not None:
                mismatches.append(report)
        cleanup_report = _assert_warehouse_clean(spark)
        if cleanup_report is not None:
            mismatches.append(cleanup_report)
    finally:
        spark.stop()
        shutil.rmtree(warehouse, ignore_errors=True)

    for report in mismatches:
        print(report)
    print(f"\nrecord mode: {len(ROWS)} rows re-derived, {len(mismatches)} mismatch(es)")
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
