"""Record mode for the V-4 partition-value corpus — re-derive every Spark half from live Iceberg.

NOT a ``test_`` module: pytest never collects it. It is the driver that produced the recorded
Spark halves in ``test_partition_value_audit.py``, committed so the "recorded against live
PySpark 4.1.2 + Iceberg" claim is falsifiable from inside the repo.

**Why this driver provisions Iceberg.** Partition VALUES live in Iceberg manifests. Vanilla
PySpark cannot write Iceberg tables. The driver pins and fetches the Iceberg Spark runtime
by Maven coordinates (never commits the binary) and stands up a local Hadoop warehouse.

**Pinned GAV** (same ruling as the MERGE / DML corpora — exact Spark-minor match, derived
from ``_oracle_pins`` / the ``record`` extra, never restated):

    org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0

**Recipe (re-derivable):**

    uv sync --locked --extra record \\
        --extra numpy --extra pandas --extra polars --extra ml-ext \\
        --no-install-package repark

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_partition_value_goldens.py

Requires: Java 17 (zulu-17), the sync above (pyspark==4.1.2), network on the first Ivy
resolve. Hold ``/tmp/grok-jvm-record.lock`` (MARKER=v4-<step>) for the whole run.

The driver imports ``ROWS`` from the COMMITTED test module and runs each row's OWN
lifecycle recipe. Exit 0 means every recorded half still reproduces. ``--dump`` prints
paste-ready ``_table`` / ``_meta`` constructors (it never edits the corpus).
"""

from __future__ import annotations

import argparse
import datetime as dt
import shutil
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

# Run as a script from anywhere: the corpus is a sibling module, imported by name so the
# driver reads the SAME rows the suite asserts (never a copy).
sys.path.insert(0, str(Path(__file__).resolve().parent))

if TYPE_CHECKING:
    import pyarrow as pa
    from test_partition_value_audit import PartitionValueRow, WriteAudit

SPARK_CATALOG = "local"
SPARK_NAMESPACE = "ns"


def _signature(table: pa.Table) -> list[tuple[str, str, bool]]:
    """The schema signature the parity comparator asserts on."""
    return [(field.name, str(field.type), field.nullable) for field in table.schema]


def _spark_iceberg_session(warehouse: Path) -> Any:
    """Build the recorded-basis Spark session with the pinned Iceberg runtime + Hadoop catalog."""
    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("repark-partition-value-record")
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


def _type_ctor(arrow_type: pa.DataType) -> str:
    """Python source for an Arrow type used in a pasted ``_table`` constructor."""
    import pyarrow as pa

    if pa.types.is_int64(arrow_type):
        return "_I64"
    if pa.types.is_int32(arrow_type):
        return "_I32"
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "_STR"
    if pa.types.is_date32(arrow_type):
        return "_DATE"
    if pa.types.is_timestamp(arrow_type):
        return f'pa.timestamp("{arrow_type.unit}", {arrow_type.tz!r})'
    return f"pa.type_for_alias({str(arrow_type)!r})"


def _py_literal(value: object) -> str:
    """Python source for one cell."""
    if value is None:
        return "None"
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            return (
                f"dt.datetime({value.year}, {value.month}, {value.day}, "
                f"{value.hour}, {value.minute}, {value.second}, {value.microsecond})"
            )
        utc = value.astimezone(dt.UTC)
        return (
            f"dt.datetime({utc.year}, {utc.month}, {utc.day}, "
            f"{utc.hour}, {utc.minute}, {utc.second}, {utc.microsecond}, tzinfo=dt.UTC)"
        )
    if isinstance(value, dt.date):
        return f"dt.date({value.year}, {value.month}, {value.day})"
    return repr(value)


def _emit_table(table: pa.Table) -> str:
    """Paste-ready ``_table(...)`` constructor."""
    fields = ", ".join(
        f"({field.name!r}, {_type_ctor(field.type)}, {field.nullable})" for field in table.schema
    )
    values_parts: list[str] = []
    for name in table.column_names:
        cells = ", ".join(_py_literal(cell) for cell in table.column(name).to_pylist())
        values_parts.append(f"{name!r}: [{cells}]")
    values = ", ".join(values_parts)
    return f"_table([{fields}], {{{values}}})"


def _emit_audit(name: str, audit: WriteAudit) -> None:
    """Print one row's live Spark observation as paste-ready constructors."""
    print(f"\n# ----- {name} -----")
    if audit.write_error is not None:
        print(f"# write_error = {audit.write_error!r}")
        return
    if audit.data is not None:
        print(f"# data schema = {_signature(audit.data)}")
        print(f"spark_data={_emit_table(audit.data)},")
    if audit.meta_error is not None:
        print(f"# meta_error = {audit.meta_error!r}")
        return
    if audit.meta is not None:
        print(f"# meta schema = {_signature(audit.meta)}")
        print(f"spark_meta={_emit_table(audit.meta)},")


def _record_row(spark: Any, row: PartitionValueRow, *, dump: bool) -> str | None:
    """Re-derive one row against live Spark. Returns a mismatch report, or None when it matches."""
    from test_partition_value_audit import SESSION_TIME_ZONE_KEY, run_write_lifecycle

    spark.conf.set(SESSION_TIME_ZONE_KEY, row.session_time_zone)
    live = run_write_lifecycle(spark, row, catalog=SPARK_CATALOG, namespace=SPARK_NAMESPACE)
    if dump:
        _emit_audit(row.name, live)
        return None

    if row.kind == "error":
        if live.write_error is None:
            return f"[{row.name}] expected Spark to refuse, but the write committed"
        if row.spark_error_needle is None:
            return (
                f"[{row.name}] Spark refused ({live.write_error!r}) but spark_error_needle "
                f"is unset — paste the token and re-run"
            )
        if row.spark_error_needle not in live.write_error:
            return (
                f"[{row.name}] Spark error missing {row.spark_error_needle!r}: {live.write_error!r}"
            )
        print(f"[V-4] {row.name} [error] PASS")
        return None

    if live.write_error is not None:
        if row.kind == "split" and row.spark_error_needle:
            # Unexpected: this row claims Spark succeeds.
            return f"[{row.name}] Spark refused (split claimed success): {live.write_error!r}"
        return f"[{row.name}] Spark write failed: {live.write_error!r}"

    if row.spark_data is None:
        return f"[{row.name}] spark_data is unset — re-run with --dump and paste the golden"
    assert live.data is not None
    from repark_parity import FrameMismatchError, assert_frames_equal

    try:
        assert_frames_equal(live.data, row.spark_data)
    except FrameMismatchError as mismatch:
        return (
            f"[{row.name}] DATA MISMATCH: {mismatch}\n"
            f"    live schema     = {_signature(live.data)}\n"
            f"    recorded schema = {_signature(row.spark_data)}\n"
            f"    live rows       = {live.data.to_pydict()}\n"
            f"    recorded rows   = {row.spark_data.to_pydict()}"
        )

    if row.spark_meta_error_needle is not None:
        if live.meta_error is None:
            return f"[{row.name}] expected Spark meta refuse, but meta read succeeded"
        if row.spark_meta_error_needle not in live.meta_error:
            return (
                f"[{row.name}] Spark meta error missing {row.spark_meta_error_needle!r}: "
                f"{live.meta_error!r}"
            )
        print(f"[V-4] {row.name} [data+meta-refuse] PASS")
        return None

    if live.meta_error is not None:
        return f"[{row.name}] Spark meta read failed: {live.meta_error!r}"
    if row.spark_meta is None:
        return f"[{row.name}] spark_meta is unset — re-run with --dump and paste the golden"
    assert live.meta is not None
    try:
        assert_frames_equal(live.meta, row.spark_meta)
    except FrameMismatchError as mismatch:
        return (
            f"[{row.name}] META MISMATCH: {mismatch}\n"
            f"    live schema     = {_signature(live.meta)}\n"
            f"    recorded schema = {_signature(row.spark_meta)}\n"
            f"    live rows       = {live.meta.to_pydict()}\n"
            f"    recorded rows   = {row.spark_meta.to_pydict()}"
        )
    print(f"[V-4] {row.name} [{row.kind}] PASS")
    return None


def main() -> int:
    """Re-derive every recorded Spark half; return 0 when all of them still reproduce."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dump",
        action="store_true",
        help="print paste-ready constructors instead of comparing (first-record path)",
    )
    args = parser.parse_args()

    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV
    from test_partition_value_audit import ROWS

    warehouse = Path(tempfile.mkdtemp(prefix="repark-partition-value-record-"))
    print(f"record warehouse = {warehouse}")
    print(f"Iceberg GAV      = {ICEBERG_SPARK_RUNTIME_GAV}")
    spark = _spark_iceberg_session(warehouse)
    try:
        spark.sparkContext.setLogLevel("ERROR")
        mismatches: list[str] = []
        for row in ROWS:
            report = _record_row(spark, row, dump=args.dump)
            if report is not None:
                mismatches.append(report)
    finally:
        spark.stop()
        shutil.rmtree(warehouse, ignore_errors=True)

    for report in mismatches:
        print(report)
    print(f"\nrecord mode: {len(ROWS)} rows re-derived, {len(mismatches)} mismatch(es)")
    return 0 if args.dump else (1 if mismatches else 0)


if __name__ == "__main__":
    raise SystemExit(main())
