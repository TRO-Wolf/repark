"""Record mode for the cast-failure corpus — re-derive every Spark half from live PySpark.

NOT a ``test_`` module: pytest never collects it. It is the driver that produced the recorded
Spark halves / error needles in ``test_cast_failure_parity.py``, committed so the "recorded
against live PySpark 4.1.2" claim is falsifiable from inside the repo rather than only from the
session that made it (the golden-drift blind spot ``docs/testing.md`` names).

It imports ``ROWS`` from the COMMITTED test module and runs each row's OWN recipe — the same
helpers the suite uses — on a live PySpark session. The recorded golden and the asserted recipe
therefore cannot drift apart: there is one recipe, not two copies.

Raise-class / error rows re-check that live Spark still raises with the recorded needle rather
than returning a table. Split rows whose Spark half is a success re-derive the table; split rows
whose Spark half is a raise re-check the needle.

Run it (needs a JVM and ``pyspark``, i.e. ``uv sync --extra record``)::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_cast_failure_goldens.py

Hold ``/tmp/grok-jvm-record.lock`` around the process (conductor B4). Exit code 0 means every
recorded half still reproduces bit-for-bit (schema name/type/nullability then values) and every
error-class still raises with its needle. Non-zero prints each mismatch with the live schema and
rows (or the live exception), which are the values to paste back into the module after deciding
the move is deliberate. It never edits the corpus — re-recording is a human decision, and a
driver that rewrote its own oracle would launder drift.

The Spark session basis is pinned here, not guessed: ``local[2]``, ANSI on,
``spark.sql.shuffle.partitions=2``, UI off, ``spark.sql.session.timeZone=UTC`` — UTC is load-
bearing for the TIMESTAMP→INT split (unix seconds).
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

# Run as a script from anywhere: the corpus is a sibling module, imported by name so the driver
# reads the SAME rows the suite asserts (never a copy).
sys.path.insert(0, str(Path(__file__).resolve().parent))

if TYPE_CHECKING:
    import pyarrow as pa
    from test_cast_failure_parity import CastRow


def _signature(table: pa.Table) -> list[tuple[str, str, bool]]:
    """The schema signature the parity comparator asserts on: name, Arrow type, nullability."""
    return [(field.name, str(field.type), field.nullable) for field in table.schema]


def _spark_session() -> Any:
    """The recorded basis, built once (``local[2]``, ANSI on, shuffle=2, UI off, timeZone=UTC)."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("repark-cast-failure-record")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _record_content(spark: Any, row: CastRow) -> str | None:
    """Re-derive one content row against live Spark. None = match; else a report."""
    from test_cast_failure_parity import run_cast_content

    from repark_parity import FrameMismatchError, assert_frames_equal

    assert row.spark is not None
    try:
        live = run_cast_content(spark, row)
    except Exception as exc:
        return (
            f"[G6] {row.name} [content] UNEXPECTED RAISE\n"
            f"    live raised {type(exc).__name__}: {exc!s:.300}"
        )

    recorded = row.spark
    try:
        assert_frames_equal(live, recorded)
    except FrameMismatchError as mismatch:
        return (
            f"[G6] {row.name} [content] MISMATCH\n"
            f"    {mismatch}\n"
            f"    live schema     = {_signature(live)}\n"
            f"    recorded schema = {_signature(recorded)}\n"
            f"    live rows       = {live.to_pydict()}\n"
            f"    recorded rows   = {recorded.to_pydict()}"
        )
    print(f"[G6] {row.name} [content] PASS")
    return None


def _record_error_row(spark: Any, row: CastRow) -> str | None:
    """Re-derive one error row: Spark must raise and the needle must appear."""
    from test_cast_failure_parity import run_cast_expect_error

    assert row.spark_error_needle is not None
    try:
        message = run_cast_expect_error(spark, row)
    except AssertionError as exc:
        return f"[G6] {row.name} [error] MISMATCH\n    {exc}"
    if row.spark_error_needle in message:
        print(f"[G6] {row.name} [error] PASS ({row.spark_error_needle})")
        return None
    return (
        f"[G6] {row.name} [error] MISMATCH\n"
        f"    expected needle = {row.spark_error_needle!r}\n"
        f"    live message    = {message!r}"
    )


def _record_split(spark: Any, row: CastRow) -> str | None:
    """Re-derive a split: Spark-success table or Spark-raise needle, depending on which_raises."""
    from test_cast_failure_parity import run_cast_content, run_cast_expect_error

    from repark_parity import FrameMismatchError, assert_frames_equal

    if row.which_raises == "repark":
        # Spark succeeds — re-derive the table half.
        assert row.spark is not None
        try:
            live = run_cast_content(spark, row)
        except Exception as exc:
            return (
                f"[G6] {row.name} [split/spark-success] UNEXPECTED RAISE\n"
                f"    live raised {type(exc).__name__}: {exc!s:.300}"
            )
        try:
            assert_frames_equal(live, row.spark)
        except FrameMismatchError as mismatch:
            return (
                f"[G6] {row.name} [split/spark-success] MISMATCH\n"
                f"    {mismatch}\n"
                f"    live schema     = {_signature(live)}\n"
                f"    recorded schema = {_signature(row.spark)}\n"
                f"    live rows       = {live.to_pydict()}\n"
                f"    recorded rows   = {row.spark.to_pydict()}"
            )
        print(f"[G6] {row.name} [split/spark-success] PASS")
        return None

    # which_raises == "spark": Spark refuses — re-check the needle.
    assert row.spark_error_needle is not None
    try:
        message = run_cast_expect_error(spark, row)
    except AssertionError as exc:
        return f"[G6] {row.name} [split/spark-raise] MISMATCH\n    {exc}"
    if row.spark_error_needle in message:
        print(f"[G6] {row.name} [split/spark-raise] PASS ({row.spark_error_needle})")
        return None
    return (
        f"[G6] {row.name} [split/spark-raise] MISMATCH\n"
        f"    expected needle = {row.spark_error_needle!r}\n"
        f"    live message    = {message!r}"
    )


def _record_row(spark: Any, row: CastRow) -> str | None:
    """Dispatch by row kind."""
    if row.kind == "error":
        return _record_error_row(spark, row)
    if row.kind == "content":
        return _record_content(spark, row)
    if row.kind == "split":
        return _record_split(spark, row)
    return f"[G6] {row.name} unknown kind {row.kind!r}"


def main() -> int:
    """Re-derive every recorded Spark half; return 0 when all of them still reproduce."""
    from test_cast_failure_parity import ROWS

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
