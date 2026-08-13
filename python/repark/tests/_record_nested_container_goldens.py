"""Record mode for the nested-container corpus — re-derive every Spark half from live PySpark.

NOT a ``test_`` module: pytest never collects it. It is the driver that produced the recorded
Spark halves in ``test_nested_container_parity.py``, committed so the "recorded against live
PySpark 4.1.2" claim is falsifiable from inside the repo rather than only from the session that
made it.

It imports ``ROWS`` from the COMMITTED test module and runs each row's OWN recipe — the same
``run_row`` the suite uses — on a live PySpark session. The recorded golden and the asserted
recipe therefore cannot drift apart: there is one recipe, not two copies.

Run it (needs a JVM and ``pyspark``, i.e. ``uv sync --extra record``)::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_nested_container_goldens.py

With ``--emit`` the driver prints paste-ready ``_table(...)`` snippets for every row (Spark and,
when the engines diverge, a repark half from a live repark session). Without the flag it only
reports PASS / MISMATCH / MISSING and never rewrites the corpus.

Exit code 0 means every recorded half still reproduces bit-for-bit (schema name/type/nullability
then values). Non-zero prints each mismatch with the live schema and rows. It never edits the
corpus — re-recording is a human decision.

The Spark session basis is pinned here: ``local[2]``, ANSI on, ``spark.sql.shuffle.partitions=2``,
UI off — the same basis the other corpus record drivers use.

**JVM serialization.** Coordinate via ``/tmp/grok-jvm-record.lock`` (exclusive create / flock)
when other lanes are recording. This driver itself does not take the lock — the operator /
orchestrator holds it around the process.
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
    from test_nested_container_parity import NestedRow


def _signature(table: pa.Table) -> list[tuple[str, str, bool]]:
    """The schema signature the parity comparator asserts on: name, Arrow type, nullability."""
    return [(field.name, str(field.type), field.nullable) for field in table.schema]


def _spark_session() -> Any:
    """The recorded basis, built once (``local[2]``, ANSI on, shuffle=2, UI off)."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("repark-nested-container-record")
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
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "pa.string()"
    if pa.types.is_boolean(arrow_type):
        return "pa.bool_()"
    if pa.types.is_map(arrow_type):
        return f"pa.map_({_type_source(arrow_type.key_type)}, {_type_source(arrow_type.item_type)})"
    if pa.types.is_struct(arrow_type):
        fields = ", ".join(f"({field.name!r}, {_type_source(field.type)})" for field in arrow_type)
        return f"pa.struct([{fields}])"
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        # Preserve the list value-field name (item vs element) and its nullability.
        value_field = arrow_type.value_field
        return (
            f"pa.list_(pa.field({value_field.name!r}, "
            f"{_type_source(value_field.type)}, nullable={value_field.nullable}))"
        )
    return f"pa.type_for_alias({str(arrow_type)!r})"


def _scalar_source(value: object, arrow_type: pa.DataType) -> str:
    """Python literal source for one cell."""
    import pyarrow as pa

    if value is None:
        return "None"
    if pa.types.is_map(arrow_type):
        assert isinstance(value, (list, tuple))
        pairs = ", ".join(
            f"({_scalar_source(entry[0], arrow_type.key_type)}, "
            f"{_scalar_source(entry[1], arrow_type.item_type)})"
            for entry in value
        )
        return f"[{pairs}]"
    if pa.types.is_struct(arrow_type):
        assert isinstance(value, dict)
        parts = ", ".join(
            f"{field.name!r}: {_scalar_source(value.get(field.name), field.type)}"
            for field in arrow_type
        )
        return f"{{{parts}}}"
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        assert isinstance(value, list)
        child = arrow_type.value_type
        return "[" + ", ".join(_scalar_source(item, child) for item in value) + "]"
    return repr(value)


def _values_source(column: list[object], arrow_type: pa.DataType) -> str:
    """Python list literal source for one column."""
    return "[" + ", ".join(_scalar_source(cell, arrow_type) for cell in column) + "]"


def _emit_table(table: pa.Table) -> str:
    """Paste-ready ``_table(...)`` source for a live half."""
    fields_parts: list[str] = []
    values_parts: list[str] = []
    for field in table.schema:
        type_repr = _type_source(field.type)
        fields_parts.append(f"({field.name!r}, {type_repr}, {field.nullable})")
        column = table.column(field.name).to_pylist()
        values_parts.append(f"{field.name!r}: {_values_source(column, field.type)}")
    fields_joined = ",\n            ".join(fields_parts)
    values_joined = ",\n            ".join(values_parts)
    return (
        f"_table(\n"
        f"        [\n            {fields_joined},\n        ],\n"
        f"        {{\n            {values_joined},\n        }},\n"
        f"    )"
    )


def _record_row(spark: Any, row: NestedRow) -> str | None:
    """Re-derive one content row. None = match; else a report string (or MISSING paste)."""
    from test_nested_container_parity import run_row

    from repark_parity import FrameMismatchError, assert_frames_equal

    try:
        live = run_row(row, spark)
    except Exception as exc:
        return (
            f"[G18] {row.name} UNEXPECTED RAISE\n    live raised {type(exc).__name__}: {exc!s:.300}"
        )

    if row.spark is None:
        return (
            f"[G18] {row.name} MISSING spark golden\n"
            f"    live schema = {_signature(live)}\n"
            f"    live rows   = {live.to_pylist()}\n"
            f"    paste:\n{_emit_table(live)}"
        )

    try:
        assert_frames_equal(live, row.spark)
    except FrameMismatchError as mismatch:
        return (
            f"[G18] {row.name} MISMATCH\n"
            f"    {mismatch}\n"
            f"    live schema     = {_signature(live)}\n"
            f"    recorded schema = {_signature(row.spark)}\n"
            f"    live rows       = {live.to_pylist()}\n"
            f"    recorded rows   = {row.spark.to_pylist()}\n"
            f"    paste:\n{_emit_table(live)}"
        )
    print(f"[G18] {row.name} PASS")
    return None


def _emit_all(spark: Any) -> None:
    """Print paste-ready Spark (and repark, when divergent) halves for every row."""
    from test_nested_container_parity import ROWS, run_row

    from repark_parity import FrameMismatchError, assert_frames_equal

    repark_session = None
    try:
        import repark
        from repark.session import _reset_active_session_for_tests

        _reset_active_session_for_tests()
        repark_session = repark.ReparkSession.builder.appName("nested-container-emit").getOrCreate()
    except Exception as exc:
        # Emit is still useful with Spark-only halves when repark is not importable.
        print(f"# repark session unavailable for emit: {type(exc).__name__}: {exc!s:.200}")

    try:
        for row in ROWS:
            live_spark = run_row(row, spark)
            print(f"\n# ===== {row.name} SPARK =====")
            print(_emit_table(live_spark))
            if repark_session is None:
                continue
            live_repark = run_row(row, repark_session)
            try:
                assert_frames_equal(live_repark, live_spark)
                print(f"# {row.name}: EQUALITY (repark matches spark) — repark=None")
            except FrameMismatchError:
                print(f"# ===== {row.name} REPARK (disclosure) =====")
                print(_emit_table(live_repark))
    finally:
        if repark_session is not None:
            repark_session.stop()
            from repark.session import _reset_active_session_for_tests

            _reset_active_session_for_tests()


def main() -> int:
    """Re-derive every recorded Spark half; return 0 when all of them still reproduce."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--emit",
        action="store_true",
        help="print paste-ready _table(...) snippets (Spark + divergent repark halves)",
    )
    args = parser.parse_args()

    from test_nested_container_parity import ROWS

    spark = _spark_session()
    try:
        spark.sparkContext.setLogLevel("ERROR")
        if args.emit:
            _emit_all(spark)
            return 0

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
