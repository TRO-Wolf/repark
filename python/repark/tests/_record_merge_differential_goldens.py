"""Record mode for the MERGE INTO differential corpus — re-derive Spark halves from live Iceberg.

NOT a ``test_`` module: pytest never collects it. It is the driver that produced the recorded Spark
halves in ``test_merge_differential_parity.py``, committed so the "recorded against live PySpark
4.1.2 + Iceberg" claim is falsifiable from inside the repo.

**Why this driver provisions Iceberg.** Vanilla PySpark cannot run ``MERGE INTO`` against temp
views — it needs a real table format. The live/record oracle elsewhere in the suite is plain
PySpark 4.1.2 with no Iceberg jar. This unit's record path therefore pins and fetches the
Iceberg Spark runtime by Maven coordinates (never commits the binary) and stands up a local
Hadoop warehouse catalog.

**Pinned GAV (Q2 ruling — exact Spark-minor match):**

    org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0

Fetched at record time via ``spark.jars.packages``. CI stays JVM-free — this driver is never
collected by pytest and is not on any CI path.

**Recipe (re-derivable).** First the full parity-live sync line (load-bearing flags; dual-wired
Makefile ↔ ``parity-live.yml``)::

    uv sync --locked --extra record \\
        --extra numpy --extra pandas --extra polars --extra ml-ext \\
        --no-install-package repark

Then the record driver::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_merge_differential_goldens.py

Requires: Java 17 (zulu-17), the sync above (``record`` extra → pyspark==4.1.2), network on first
resolve for the Ivy/Maven fetch (then cached under ``~/.ivy2.5.2/jars``).

The driver imports ``ROWS`` from the COMMITTED test module and runs each row's OWN lifecycle
recipe (the same helpers the suite uses) on a live Spark+Iceberg session. Exit 0 means every
recorded half still reproduces (content: schema name/type/nullability then values; error: needle
in message; split: Spark success half matches). It never edits the corpus — re-recording is a
human decision.

**Lifecycle helper.** ``create → seed → MERGE → read back`` (and the error-path twin) live in
``test_merge_differential_parity`` — imported here so there is one helper, not two copies. A
failed MERGE drops the per-row target in ``finally``; the driver's warehouse is a temp directory
that is removed on exit.
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
    from test_merge_differential_parity import MergeDiffRow


# Spark-side catalog name (Hadoop catalog over a local warehouse). Distinct from repark's "mem".
SPARK_CATALOG = "local"
SPARK_NAMESPACE = "ns"


def _signature(table: pa.Table) -> list[tuple[str, str, bool]]:
    """The schema signature the parity comparator asserts on: name, Arrow type, nullability."""
    return [(field.name, str(field.type), field.nullable) for field in table.schema]


def _spark_iceberg_session(warehouse: Path) -> Any:
    """Build the recorded-basis Spark session with the pinned Iceberg runtime + local catalog.

    Config surface is pinned here (not guessed): ``local[2]``, ANSI on,
    ``spark.sql.shuffle.partitions=2``, UI off — the same basis the timezone record driver uses —
    plus the Iceberg extensions and a Hadoop catalog rooted at ``warehouse``.
    """
    from pyspark.sql import SparkSession
    from test_merge_differential_parity import ICEBERG_SPARK_RUNTIME_GAV

    return (
        SparkSession.builder.master("local[2]")
        .appName("repark-merge-differential-record")
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


def _record_content_row(spark: Any, row: MergeDiffRow) -> str | None:
    """Re-derive one content row. Returns a mismatch report, or None when it matches."""
    from test_merge_differential_parity import run_merge_lifecycle

    assert row.spark is not None
    live = run_merge_lifecycle(
        spark,
        row,
        catalog=SPARK_CATALOG,
        namespace=SPARK_NAMESPACE,
        with_cow_props=False,  # Spark Iceberg 1.11 accepts MERGE without COW TBLPROPERTIES
    )
    recorded = row.spark
    if _signature(live) == _signature(recorded) and live.equals(recorded):
        print(f"[G3] {row.name} [content] PASS")
        return None
    return (
        f"[G3] {row.name} [content] MISMATCH\n"
        f"    live schema     = {_signature(live)}\n"
        f"    recorded schema = {_signature(recorded)}\n"
        f"    live rows       = {live.to_pydict()}\n"
        f"    recorded rows   = {recorded.to_pydict()}"
    )


def _record_error_row(spark: Any, row: MergeDiffRow) -> str | None:
    """Re-derive one error row: Spark must raise and the needle must appear."""
    from test_merge_differential_parity import run_merge_expect_error

    assert row.spark_error_needle is not None
    message = run_merge_expect_error(
        spark,
        row,
        catalog=SPARK_CATALOG,
        namespace=SPARK_NAMESPACE,
        with_cow_props=False,  # Spark Iceberg 1.11 accepts MERGE without COW TBLPROPERTIES
    )
    if row.spark_error_needle in message:
        print(f"[G3] {row.name} [error] PASS ({row.spark_error_needle})")
        return None
    return (
        f"[G3] {row.name} [error] MISMATCH\n"
        f"    expected needle = {row.spark_error_needle!r}\n"
        f"    live message    = {message!r}"
    )


def _record_split_row(spark: Any, row: MergeDiffRow) -> str | None:
    """Re-derive one split row: Spark succeeds; compare the success half to the recorded golden."""
    from test_merge_differential_parity import run_merge_lifecycle

    assert row.spark is not None
    # Spark side of a split is a successful MERGE — same as content.
    live = run_merge_lifecycle(
        spark,
        row,
        catalog=SPARK_CATALOG,
        namespace=SPARK_NAMESPACE,
        with_cow_props=False,  # Spark Iceberg 1.11 accepts MERGE without COW TBLPROPERTIES
    )
    recorded = row.spark
    if _signature(live) == _signature(recorded) and live.equals(recorded):
        print(f"[G3] {row.name} [split/spark-success] PASS")
        return None
    return (
        f"[G3] {row.name} [split/spark-success] MISMATCH\n"
        f"    live schema     = {_signature(live)}\n"
        f"    recorded schema = {_signature(recorded)}\n"
        f"    live rows       = {live.to_pydict()}\n"
        f"    recorded rows   = {recorded.to_pydict()}"
    )


def _record_row(spark: Any, row: MergeDiffRow) -> str | None:
    """Dispatch by row kind."""
    if row.kind == "content":
        return _record_content_row(spark, row)
    if row.kind == "error":
        return _record_error_row(spark, row)
    if row.kind == "split":
        return _record_split_row(spark, row)
    return f"[G3] {row.name} unknown kind {row.kind!r}"


def _assert_warehouse_clean_after_error(spark: Any) -> str | None:
    """Provocation: after the error row, the Spark catalog must not list the error table."""
    from test_merge_differential_parity import ROWS, run_merge_expect_error

    error_row = next(row for row in ROWS if row.kind == "error")
    # run again to exercise cleanup on the Spark side
    _ = run_merge_expect_error(
        spark,
        error_row,
        catalog=SPARK_CATALOG,
        namespace=SPARK_NAMESPACE,
        with_cow_props=False,  # Spark Iceberg 1.11 accepts MERGE without COW TBLPROPERTIES
    )
    # listTables via Spark SQL
    try:
        listed = spark.sql(f"SHOW TABLES IN {SPARK_CATALOG}.{SPARK_NAMESPACE}").collect()
        names = {row.tableName if hasattr(row, "tableName") else row[1] for row in listed}
    except Exception as exc:
        return f"[G3] cleanup probe: SHOW TABLES failed: {exc}"
    if error_row.name in names:
        return (
            f"[G3] cleanup probe FAIL: stray table {error_row.name!r} still listed after "
            f"failed MERGE; tables={sorted(names)}"
        )
    print(f"[G3] lifecycle cleanup after failed MERGE PASS (tables={sorted(names)})")
    return None


def main() -> int:
    """Re-derive every recorded Spark half; return 0 when all of them still reproduce."""
    from test_merge_differential_parity import ICEBERG_SPARK_RUNTIME_GAV, ROWS

    warehouse = Path(tempfile.mkdtemp(prefix="repark-merge-diff-record-"))
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
        cleanup_report = _assert_warehouse_clean_after_error(spark)
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
