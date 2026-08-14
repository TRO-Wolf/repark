"""Record mode for the decimal128 corpus — re-derive every `spark` half from live PySpark.

NOT a `test_` module: pytest never collects it. It is the driver that produced the recorded Spark
halves in `test_decimal128_parity.py`, committed so the "recorded against live PySpark 4.1.2"
claim is falsifiable from inside the repo rather than only from the session that made it
(the golden-drift blind spot `docs/testing.md` names).

It imports `ROWS` / `CTAS_ROWS` from the COMMITTED test module and runs each row's OWN recipe —
the same `run_row` the assertions use — on a live PySpark session. The recorded golden and the
asserted recipe therefore cannot drift apart: there is one recipe, not two copies.

Raise-class rows (`spark_raises`) re-check that live Spark still raises a matching exception
class rather than returning a table. CTAS `spark_select` halves (when set) are re-derived the
same way as ordinary equality goldens.

Run it (needs a JVM and `pyspark`, i.e. `uv sync --extra record`)::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_decimal128_goldens.py

Exit code 0 means every recorded half still reproduces bit-for-bit (schema name/type/nullability
then values) and every raise-class still raises. Non-zero prints each mismatch with the live
schema and rows (or the live exception), which are the values to paste back into the module after
deciding the move is deliberate. It never edits the corpus — re-recording is a human decision,
and a driver that rewrote its own oracle would launder drift.

The Spark session basis is the one the corpus was recorded under and is pinned here, not guessed:
`local[2]`, ANSI on, `spark.sql.shuffle.partitions=2`, UI off — the same basis
`_live_parity.build_spark_engine` uses for the live oracle tier.
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
    from test_decimal128_parity import CtasRow, DecimalRow


def _signature(table: pa.Table) -> list[tuple[str, str, bool]]:
    """The schema signature the parity comparator asserts on: name, Arrow type, nullability."""
    return [(field.name, str(field.type), field.nullable) for field in table.schema]


def _spark_session() -> Any:
    """The recorded basis, built once (`local[2]`, ANSI on, shuffle=2, UI off)."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("repark-decimal128-record")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _matches_raise(exc: BaseException, expected_substring: str) -> bool:
    """True when the exception class name / MRO / message contains the recorded raise token."""
    names = [type(exc).__name__, *[base.__name__ for base in type(exc).mro()]]
    if any(expected_substring in name for name in names):
        return True
    return expected_substring in str(exc)


def _apply_row_session_conf(spark: Any, row: DecimalRow) -> None:
    """Honor ``row.session_conf`` on the live Spark session (U5 both-knob-state records).

    Resets ``spark.sql.ansi.enabled`` to the recorded-basis default (true) when a row
    does not override it, so an OFF twin cannot leak into the next ON row.
    """
    ansi = "true"
    for key, value in row.session_conf:
        if key == "spark.sql.ansi.enabled":
            ansi = value
        else:
            spark.conf.set(key, value)
    spark.conf.set("spark.sql.ansi.enabled", ansi)


def _record_row(spark: Any, row: DecimalRow) -> str | None:
    """Re-derive one differential row against live Spark. None = match; else a report."""
    from test_decimal128_parity import run_row

    _apply_row_session_conf(spark, row)

    if row.spark_raises is not None:
        try:
            live = run_row(row, spark)
        except Exception as exc:
            if _matches_raise(exc, row.spark_raises):
                print(f"[{row.gap}] {row.name} RAISE:{row.spark_raises} PASS")
                return None
            return (
                f"[{row.gap}] {row.name} RAISE MISMATCH\n"
                f"    expected raise containing {row.spark_raises!r}\n"
                f"    live raised {type(exc).__name__}: {exc!s:.300}"
            )
        return (
            f"[{row.gap}] {row.name} RAISE MISMATCH\n"
            f"    expected raise containing {row.spark_raises!r}\n"
            f"    live returned schema={_signature(live)} rows={live.to_pydict()}"
        )

    # repark_raises rows still have a successful Spark half to re-derive.
    try:
        live = run_row(row, spark)
    except Exception as exc:
        return (
            f"[{row.gap}] {row.name} UNEXPECTED RAISE\n"
            f"    live raised {type(exc).__name__}: {exc!s:.300}"
        )

    recorded = row.spark
    if recorded is None:
        return (
            f"[{row.gap}] {row.name} MISSING SPARK GOLDEN\n"
            f"    live schema={_signature(live)} rows={live.to_pydict()}"
        )

    if _signature(live) == _signature(recorded) and live.equals(recorded):
        print(f"[{row.gap}] {row.name} PASS")
        return None
    return (
        f"[{row.gap}] {row.name} MISMATCH\n"
        f"    live schema     = {_signature(live)}\n"
        f"    recorded schema = {_signature(recorded)}\n"
        f"    live rows       = {live.to_pydict()}\n"
        f"    recorded rows   = {recorded.to_pydict()}"
    )


def _record_ctas_select(spark: Any, row: CtasRow) -> str | None:
    """Re-derive a CTAS row's optional Spark SELECT half. None when absent or matching."""
    if row.spark_select is None:
        print(f"[CTAS] {row.name} (no spark_select oracle — repark-only write pin) SKIP")
        return None
    try:
        frame = spark.sql(row.select_sql)
        to_arrow = getattr(frame, "to_arrow", None) or frame.toArrow
        live = to_arrow()
    except Exception as exc:
        return (
            f"[CTAS] {row.name} UNEXPECTED RAISE\n"
            f"    live raised {type(exc).__name__}: {exc!s:.300}"
        )
    recorded = row.spark_select
    if _signature(live) == _signature(recorded) and live.equals(recorded):
        print(f"[CTAS] {row.name} SELECT PASS")
        return None
    return (
        f"[CTAS] {row.name} SELECT MISMATCH\n"
        f"    live schema     = {_signature(live)}\n"
        f"    recorded schema = {_signature(recorded)}\n"
        f"    live rows       = {live.to_pydict()}\n"
        f"    recorded rows   = {recorded.to_pydict()}"
    )


def main() -> int:
    """Re-derive every recorded Spark half; return 0 when all of them still reproduce."""
    from test_decimal128_parity import CTAS_ROWS, ROWS

    spark = _spark_session()
    try:
        mismatches = [report for row in ROWS if (report := _record_row(spark, row)) is not None]
        mismatches.extend(
            report for row in CTAS_ROWS if (report := _record_ctas_select(spark, row)) is not None
        )
    finally:
        spark.stop()

    for report in mismatches:
        print(report)
    total = len(ROWS) + sum(1 for row in CTAS_ROWS if row.spark_select is not None)
    print(f"\nrecord mode: {total} spark halves re-derived, {len(mismatches)} mismatch(es)")
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
