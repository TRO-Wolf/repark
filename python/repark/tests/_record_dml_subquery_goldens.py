"""Record mode for the G3-E8 DML-subquery corpus — re-derive every Spark half from live Iceberg.

NOT a ``test_`` module: pytest never collects it, and it is on no CI path. Vanilla PySpark cannot
run `DELETE`/`UPDATE` against temp views, so this driver pins and fetches the Iceberg Spark
runtime by Maven coordinates (never commits the binary) and stands up a local Hadoop warehouse
catalog. Pinned GAV (same ruling as the MERGE corpus — exact Spark-minor match):

    org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0

Recipe (needs Java 17 (zulu-17), ``uv sync --extra record``, network on the first Maven/Ivy fetch;
cached thereafter):

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_dml_subquery_goldens.py

Only ONE local Spark driver at a time: check no other lane holds one (``pgrep -af
'pyspark|SparkSubmit'``, ignoring standing container infrastructure) and wait until it is clear.
The driver imports ``ROWS`` from the committed test module and runs each row's own lifecycle recipe
on a live Spark+Iceberg session. Exit 0 means every recorded half reproduces (schema, then
values). It never edits the corpus — re-recording is a human decision; a driver that rewrote its
own oracle would launder drift.
"""

from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

# Run as a script from anywhere: import the sibling corpus by name — the driver must read the
# SAME rows the suite asserts, never a copy.
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

    Both kinds run the same Spark-side recipe; only repark's half differs between the kinds.
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
