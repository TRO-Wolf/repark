"""Record mode for the float-agg corpus — re-derive every `spark` half from live PySpark.

NOT a `test_` module: pytest never collects it. It is the driver that produced the recorded Spark
halves in `test_float_agg_parity.py`, committed so the "recorded against live PySpark 4.1.2"
claim is falsifiable from inside the repo rather than only from the session that made it
(the golden-drift blind spot `docs/testing.md` names).

It imports `ROWS` from the COMMITTED test module and runs each row's OWN recipe —
the same `run_row` the assertions use — on a live PySpark session. The recorded golden and the
asserted recipe therefore cannot drift apart: there is one recipe, not two copies.

Run it (needs a JVM and `pyspark`, i.e. `uv sync --extra record`). Hold the conductor JVM lock
(`/tmp/grok-jvm-record.lock`) first::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_float_agg_goldens.py

Exit code 0 means every recorded half still reproduces bit-for-bit (schema name/type/nullability
then values). Non-zero prints each mismatch with the live schema and rows, which are the values
to paste back into the module after deciding the move is deliberate. It never edits the corpus —
re-recording is a human decision, and a driver that rewrote its own oracle would launder drift.

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
    from test_float_agg_parity import FloatAggRow


def _signature(table: pa.Table) -> list[tuple[str, str, bool]]:
    """The schema signature the parity comparator asserts on: name, Arrow type, nullability."""
    return [(field.name, str(field.type), field.nullable) for field in table.schema]


def _spark_session() -> Any:
    """The recorded basis, built once (`local[2]`, ANSI on, shuffle=2, UI off)."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("repark-float-agg-record")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _record_row(spark: Any, row: FloatAggRow) -> str | None:
    """Re-derive one differential row against live Spark. None = match; else a report."""
    from test_float_agg_parity import run_row

    try:
        live = run_row(spark, row)
    except Exception as exc:
        return (
            f"[G7] {row.name} UNEXPECTED RAISE\n    live raised {type(exc).__name__}: {exc!s:.300}"
        )

    recorded = row.spark
    if recorded is None:
        live_bits = [
            float(value).hex() if value is not None else None
            for value in live.column(0).to_pylist()
        ]
        return (
            f"[G7] {row.name} MISSING SPARK GOLDEN\n"
            f"    live schema={_signature(live)} rows={live.to_pydict()}\n"
            f"    live bits={live_bits}"
        )

    if _signature(live) == _signature(recorded) and live.equals(recorded):
        print(f"[G7] {row.name} PASS")
        print(f"     schema={_signature(live)} rows={live.to_pydict()}")
        return None
    live_bits = [
        float(value).hex() if value is not None else None for value in live.column(0).to_pylist()
    ]
    return (
        f"[G7] {row.name} MISMATCH\n"
        f"    live schema     = {_signature(live)}\n"
        f"    recorded schema = {_signature(recorded)}\n"
        f"    live rows       = {live.to_pydict()}\n"
        f"    recorded rows   = {recorded.to_pydict()}\n"
        f"    live bits       = {live_bits}"
    )


def main() -> int:
    """Re-derive every recorded Spark half; return 0 when all of them still reproduce."""
    from test_float_agg_parity import ROWS

    spark = _spark_session()
    try:
        mismatches = [report for row in ROWS if (report := _record_row(spark, row)) is not None]
    finally:
        spark.stop()

    for report in mismatches:
        print(report)
    print(f"\nrecord mode: {len(ROWS)} spark halves re-derived, {len(mismatches)} mismatch(es)")
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
