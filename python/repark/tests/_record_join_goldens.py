"""Record mode for the joins differential corpus  -  re-derive every Spark half from live PySpark.

NOT a ``test_`` module: pytest never collects it. It imports ``ROWS`` from the committed test
module and runs each row's OWN recipe  -  the same helpers the suite uses  -  on a live PySpark
session, so the recorded golden and the asserted recipe cannot drift apart. Run it (needs a JVM
and ``pyspark``, i.e. ``uv sync --extra record``)::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_join_goldens.py

Exit code 0 means every recorded half reproduces bit-for-bit and every error class still raises
with its needle; non-zero prints the live values to paste back after a deliberate move. It never
edits the corpus  -  re-recording is a human decision; a driver that rewrote its own oracle would
launder drift.

Spark basis pinned here, not guessed: ``local[2]``, ANSI on, shuffle partitions 2, UI off  -  the
same basis the other corpus record drivers use.

**JVM serialization.** When W-4 (or another lane) is recording, coordinate via
``/tmp/grok-jvm-record.lock`` (exclusive create / flock). This driver itself does not take the
lock  -  the operator / orchestrator holds it around the process.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

# Run as a script from anywhere: import the sibling corpus by name — the driver must read the
# SAME rows the suite asserts, never a copy.
sys.path.insert(0, str(Path(__file__).resolve().parent))

if TYPE_CHECKING:
    import pyarrow as pa
    from test_join_parity import JoinRow


def _signature(table: pa.Table) -> list[tuple[str, str, bool]]:
    """The schema signature the parity comparator asserts on: name, Arrow type, nullability."""
    return [(field.name, str(field.type), field.nullable) for field in table.schema]


def _spark_session() -> Any:
    """The recorded basis, built once (``local[2]``, ANSI on, shuffle=2, UI off)."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("repark-join-parity-record")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _record_content_or_split(spark: Any, row: JoinRow) -> str | None:
    """Re-derive one content or split (Spark-success) row. None = match; else a report.

    Comparison is order-insensitive (same discipline as ``assert_frames_equal``): join result
    sets are unordered unless an ORDER BY pins them.
    """
    from test_join_parity import run_join_content

    from repark_parity import FrameMismatchError, assert_frames_equal

    assert row.spark is not None
    try:
        live = run_join_content(spark, row)
    except Exception as exc:
        return (
            f"[G4] {row.name} [{row.kind}] UNEXPECTED RAISE\n"
            f"    live raised {type(exc).__name__}: {exc!s:.300}"
        )

    recorded = row.spark
    try:
        assert_frames_equal(live, recorded)
    except FrameMismatchError as mismatch:
        return (
            f"[G4] {row.name} [{row.kind}] MISMATCH\n"
            f"    {mismatch}\n"
            f"    live schema     = {_signature(live)}\n"
            f"    recorded schema = {_signature(recorded)}\n"
            f"    live rows       = {live.to_pydict()}\n"
            f"    recorded rows   = {recorded.to_pydict()}"
        )
    print(f"[G4] {row.name} [{row.kind}] PASS")
    return None


def _record_error_row(spark: Any, row: JoinRow) -> str | None:
    """Re-derive one error row: Spark must raise and the needle must appear."""
    from test_join_parity import run_join_expect_error

    assert row.spark_error_needle is not None
    try:
        message = run_join_expect_error(spark, row)
    except AssertionError as exc:
        return f"[G4] {row.name} [error] MISMATCH\n    {exc}"
    if row.spark_error_needle in message:
        print(f"[G4] {row.name} [error] PASS ({row.spark_error_needle})")
        return None
    return (
        f"[G4] {row.name} [error] MISMATCH\n"
        f"    expected needle = {row.spark_error_needle!r}\n"
        f"    live message    = {message!r}"
    )


def _record_row(spark: Any, row: JoinRow) -> str | None:
    """Dispatch by row kind."""
    if row.kind == "error":
        return _record_error_row(spark, row)
    if row.kind in ("content", "split"):
        return _record_content_or_split(spark, row)
    return f"[G4] {row.name} unknown kind {row.kind!r}"


def main() -> int:
    """Re-derive every recorded Spark half; return 0 when all of them still reproduce."""
    from test_join_parity import ROWS

    spark = _spark_session()
    try:
        spark.sparkContext.setLogLevel("ERROR")
        mismatches: list[str] = []
        for row in ROWS:
            report = _record_row(spark, row)
            if report is not None:
                mismatches.append(report)
    finally:
        spark.stop()

    for report in mismatches:
        print(report)
    print(f"\nrecord mode: {len(ROWS)} rows re-derived, {len(mismatches)} mismatch(es)")
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
