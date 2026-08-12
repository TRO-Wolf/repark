"""Record mode for the three-valued-logic corpus — re-derive every Spark half from live PySpark.

NOT a ``test_`` module: pytest never collects it. It is the driver that produced the recorded
Spark halves in ``test_three_valued_logic_parity.py``, committed so the "recorded against live
PySpark 4.1.2" claim is falsifiable from inside the repo rather than only from the session that
made it (the golden-drift blind spot ``docs/testing.md`` names).

It imports ``ROWS`` from the COMMITTED test module and runs each row's OWN recipe — the same
helpers the suite uses — on a live PySpark session. The recorded golden and the asserted recipe
therefore cannot drift apart: there is one recipe, not two copies.

Run it (needs a JVM and ``pyspark``, i.e. ``uv sync --extra record``)::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_tvl_goldens.py

``--emit`` prints paste-ready ``_table`` / ``_one_row`` snippets for every successful Spark half.

Exit code 0 means every recorded half still reproduces bit-for-bit (schema name/type/nullability
then values). Non-zero prints each mismatch with the live schema and rows, which are the values
to paste back into the module after deciding the move is deliberate. It never edits the corpus —
re-recording is a human decision, and a driver that rewrote its own oracle would launder drift.

The Spark session basis is pinned here, not guessed: ``local[2]``, ANSI on,
``spark.sql.shuffle.partitions=2``, UI off — the same basis the other corpus record drivers use.

**JVM serialization.** Hold ``/tmp/grok-jvm-record.lock`` (exclusive create / flock) and
``pgrep -af 'pyspark|SparkSubmit'`` (ignore the standing containerized cluster) before starting.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

# Run as a script from anywhere: the corpus is a sibling module, imported by name so the driver
# reads the SAME rows the suite asserts (never a copy).
sys.path.insert(0, str(Path(__file__).resolve().parent))

if TYPE_CHECKING:
    import pyarrow as pa
    from test_three_valued_logic_parity import TvlRow


def _signature(table: pa.Table) -> list[tuple[str, str, bool]]:
    """The schema signature the parity comparator asserts on: name, Arrow type, nullability."""
    return [(field.name, str(field.type), field.nullable) for field in table.schema]


def _spark_session() -> Any:
    """The recorded basis, built once (``local[2]``, ANSI on, shuffle=2, UI off)."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("repark-tvl-parity-record")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _type_source(arrow_type: pa.DataType) -> str:
    """Python source that reconstructs ``arrow_type`` via the ``pa`` alias."""
    import pyarrow as pa

    if pa.types.is_int64(arrow_type):
        return "pa.int64()"
    if pa.types.is_int32(arrow_type):
        return "pa.int32()"
    if pa.types.is_float64(arrow_type):
        return "pa.float64()"
    if pa.types.is_float32(arrow_type):
        return "pa.float32()"
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "pa.string()"
    if pa.types.is_boolean(arrow_type):
        return "pa.bool_()"
    if pa.types.is_decimal(arrow_type):
        return f"pa.decimal128({arrow_type.precision}, {arrow_type.scale})"
    return f"pa.type_for_alias({str(arrow_type)!r})"


def _scalar_source(value: object) -> str:
    """Python source for one cell value."""
    if value is None:
        return "None"
    if isinstance(value, bool):
        return repr(value)
    if isinstance(value, str):
        return repr(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, int):
        return repr(value)
    return repr(value)


def _emit_table(table: pa.Table) -> str:
    """Paste-ready ``_table`` / ``_one_row`` constructor for a recorded Spark half."""
    fields_parts: list[str] = []
    values_parts: list[str] = []
    for field in table.schema:
        name = field.name
        type_source = _type_source(field.type)
        fields_parts.append(f'("{name}", {type_source}, {field.nullable})')
        column = table.column(name).to_pylist()
        values_parts.append(
            f'"{name}": [' + ", ".join(_scalar_source(cell) for cell in column) + "]"
        )
    fields_src = "[" + ", ".join(fields_parts) + "]"
    values_src = "{" + ", ".join(values_parts) + "}"
    if table.num_rows == 1:
        # Prefer _one_row for single-row truth-table pins.
        one_values = {field.name: table.column(field.name).to_pylist()[0] for field in table.schema}
        one_src = (
            "{"
            + ", ".join(f'"{name}": {_scalar_source(value)}' for name, value in one_values.items())
            + "}"
        )
        return f"_one_row(\n    {fields_src},\n    {one_src},\n)"
    return f"_table(\n    {fields_src},\n    {values_src},\n)"


def _record_row(spark: Any, row: TvlRow, *, emit: bool) -> str | None:
    """Re-derive one differential row against live Spark. None = match; else a report."""
    from test_three_valued_logic_parity import run_tvl_content

    try:
        live = run_tvl_content(spark, row)
    except Exception as exc:
        return (
            f"[G12] {row.name} UNEXPECTED RAISE\n    live raised {type(exc).__name__}: {exc!s:.300}"
        )

    if emit:
        print(f"\n# --- {row.name} ({row.family}) ---")
        print(f"# schema={_signature(live)}")
        print(f"# rows={live.to_pydict()}")
        print(_emit_table(live))
        return None

    recorded = row.spark
    if recorded is None:
        return (
            f"[G12] {row.name} MISSING SPARK GOLDEN\n"
            f"    live schema={_signature(live)} rows={live.to_pydict()}\n"
            f"    paste:\n{_emit_table(live)}"
        )

    # Order-insensitive compare via the parity comparator (same discipline as the suite).
    from repark_parity import FrameMismatchError, assert_frames_equal

    try:
        assert_frames_equal(live, recorded)
    except FrameMismatchError as mismatch:
        return (
            f"[G12] {row.name} MISMATCH\n"
            f"    {mismatch}\n"
            f"    live schema     = {_signature(live)}\n"
            f"    recorded schema = {_signature(recorded)}\n"
            f"    live rows       = {live.to_pydict()}\n"
            f"    recorded rows   = {recorded.to_pydict()}\n"
            f"    paste:\n{_emit_table(live)}"
        )
    print(f"[G12] {row.name} PASS")
    return None


def main(argv: list[str] | None = None) -> int:
    """Re-derive every recorded Spark half; return 0 when all of them still reproduce."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--emit",
        action="store_true",
        help="print paste-ready _table/_one_row snippets for every successful Spark half",
    )
    args = parser.parse_args(argv)

    from test_three_valued_logic_parity import ROWS

    spark = _spark_session()
    try:
        mismatches = [
            report
            for row in ROWS
            if (report := _record_row(spark, row, emit=args.emit)) is not None
        ]
    finally:
        spark.stop()

    for report in mismatches:
        print(report)
    print(f"\nrecord mode: {len(ROWS)} spark halves re-derived, {len(mismatches)} mismatch(es)")
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
