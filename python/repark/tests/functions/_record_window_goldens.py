"""Record mode for the window-function corpus — re-derive every `spark` half from live PySpark.

Not collected by pytest: this driver produced the recorded Spark halves in
`test_window_parity.py` and re-runs each row's own `run_row` on a live PySpark session,
so the golden and the recipe cannot drift apart. Raise-class rows (`spark_raises`)
re-check that live Spark still raises a matching exception class. Exit code 0 means
every recorded half still reproduces bit-for-bit and every raise-class still raises;
a mismatch prints the live schema and rows (or the live exception). It never edits the
corpus — re-recording is a human decision, and a driver that rewrote its own oracle
would launder drift.

Run it (needs a JVM and `pyspark`, i.e. `uv sync --extra record`)::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_window_goldens.py

With ``--emit`` it prints paste-ready ``_table(...)`` / ``_one_row(...)`` snippets for
every row (used when first recording or re-deriving after a deliberate recipe change);
without it, only PASS / MISMATCH / MISSING. The session basis matches the one
`_live_parity.build_spark_engine` uses for the live oracle tier.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

# Run as a script from anywhere: import the corpus sibling by name (same rows the
# suite asserts, never a copy).
sys.path.insert(0, str(Path(__file__).resolve().parent))

if TYPE_CHECKING:
    import pyarrow as pa
    from test_window_parity import WindowRow


def _signature(table: pa.Table) -> list[tuple[str, str, bool]]:
    """The schema signature the parity comparator asserts on: name, Arrow type, nullability."""
    return [(field.name, str(field.type), field.nullable) for field in table.schema]


def _spark_session() -> Any:
    """The recorded basis, built once (`local[2]`, ANSI on, shuffle=2, UI off)."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("repark-window-record")
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


def _emit_table(table: pa.Table) -> str:
    """Paste-ready ``_table(...)`` / ``_one_row(...)`` source for a live Spark half."""
    fields_parts: list[str] = []
    values_parts: list[str] = []
    for field in table.schema:
        type_repr = _type_source(field.type)
        fields_parts.append(f"({field.name!r}, {type_repr}, {field.nullable})")
        column = table.column(field.name).to_pylist()
        values_parts.append(f"{field.name!r}: {_values_source(column, field.type)}")
    fields_joined = ",\n        ".join(fields_parts)
    values_joined = ",\n        ".join(values_parts)
    if table.num_rows == 1:
        # Prefer the single-row helper when every column is a scalar list of length 1.
        single = {field.name: table.column(field.name).to_pylist()[0] for field in table.schema}
        values_single = ", ".join(
            f"{name!r}: {_scalar_source(value, table.schema.field(name).type)}"
            for name, value in single.items()
        )
        return f"_one_row(\n        [{fields_joined}],\n        {{{values_single}}},\n    )"
    return f"_table(\n        [{fields_joined}],\n        {{{values_joined}}},\n    )"


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


def _scalar_source(value: object, arrow_type: pa.DataType) -> str:
    """Python source for one cell value."""
    import pyarrow as pa

    if value is None:
        return "None"
    if pa.types.is_decimal(arrow_type):
        from decimal import Decimal

        if isinstance(value, Decimal):
            return f'Decimal("{value}")'
        return f'Decimal("{value}")'
    if isinstance(value, str):
        return repr(value)
    if isinstance(value, bool):
        return repr(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, int):
        return repr(value)
    return repr(value)


def _values_source(values: list[object], arrow_type: pa.DataType) -> str:
    """Python source for a column value list."""
    parts = [_scalar_source(value, arrow_type) for value in values]
    return "[" + ", ".join(parts) + "]"


def _record_row(spark: Any, row: WindowRow, *, emit: bool) -> str | None:
    """Re-derive one differential row against live Spark. None = match; else a report."""
    from test_window_parity import run_row

    if row.spark_raises is not None:
        try:
            live = run_row(row, spark)
        except Exception as exc:
            if _matches_raise(exc, row.spark_raises):
                print(f"[{row.family}] {row.name} RAISE:{row.spark_raises} PASS")
                return None
            return (
                f"[{row.family}] {row.name} RAISE MISMATCH\n"
                f"    expected raise containing {row.spark_raises!r}\n"
                f"    live raised {type(exc).__name__}: {exc!s:.300}"
            )
        return (
            f"[{row.family}] {row.name} RAISE MISMATCH\n"
            f"    expected raise containing {row.spark_raises!r}\n"
            f"    live returned schema={_signature(live)} rows={live.to_pydict()}"
        )

    try:
        live = run_row(row, spark)
    except Exception as exc:
        return (
            f"[{row.family}] {row.name} UNEXPECTED RAISE\n"
            f"    live raised {type(exc).__name__}: {exc!s:.300}"
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
            f"[{row.family}] {row.name} MISSING SPARK GOLDEN\n"
            f"    live schema={_signature(live)} rows={live.to_pydict()}\n"
            f"    paste:\n{_emit_table(live)}"
        )

    if _signature(live) == _signature(recorded) and live.equals(recorded):
        print(f"[{row.family}] {row.name} PASS")
        return None
    return (
        f"[{row.family}] {row.name} MISMATCH\n"
        f"    live schema     = {_signature(live)}\n"
        f"    recorded schema = {_signature(recorded)}\n"
        f"    live rows       = {live.to_pydict()}\n"
        f"    recorded rows   = {recorded.to_pydict()}"
    )


def main(argv: list[str] | None = None) -> int:
    """Re-derive every recorded Spark half; return 0 when all of them still reproduce."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--emit",
        action="store_true",
        help="print paste-ready _table/_one_row snippets for every successful Spark half",
    )
    args = parser.parse_args(argv)

    from test_window_parity import ROWS

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
