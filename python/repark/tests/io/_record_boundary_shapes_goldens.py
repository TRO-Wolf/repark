"""Record mode for the G10 boundary-shape corpus — re-derive every Spark half.

Not collected by pytest: this driver produced the recorded Spark halves in
``test_boundary_shapes_parity.py`` and re-runs each row's own ``run_row`` on a
live PySpark session, so the golden and the recipe cannot drift apart. Exit
code 0 means every recorded half still reproduces; a mismatch prints and the
exit code is non-zero. It never edits the corpus — re-recording is a human
decision.

Run it (needs a JVM and ``pyspark``, i.e. ``uv sync --extra record``)::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_boundary_shapes_goldens.py

With ``--emit`` it prints paste-ready constructors (Spark, plus a divergent
repark half) instead of only reporting PASS / MISMATCH / MISSING. The pyspark
pin is derived from ``python/repark-parity/pyproject.toml``'s record extra
(CP-8: never restate a version literal). When other lanes record, the operator
holds ``/tmp/grok-jvm-record.lock`` (exclusive create) around this process;
the driver itself does not take the lock.
"""

from __future__ import annotations

import argparse
import re
import sys
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING, Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

if TYPE_CHECKING:
    import pyarrow as pa
    from test_boundary_shapes_parity import PandasShape, ShapeRow


def _repo_root() -> Path:
    """Repository root (this file lives at python/repark/tests/)."""
    return Path(__file__).resolve().parents[3]


def _pinned_pyspark_coordinate() -> str:
    """pyspark pin from the project's record extra — not a restated literal (CP-8)."""
    text = (_repo_root() / "python" / "repark-parity" / "pyproject.toml").read_text(
        encoding="utf-8"
    )
    match = re.search(r'record\s*=\s*\[\s*"pyspark==([^"]+)"\s*\]', text)
    if match is None:
        raise RuntimeError(
            "failed to parse pyspark pin from python/repark-parity/pyproject.toml "
            "[project.optional-dependencies] record"
        )
    return match.group(1)


def _signature(table: pa.Table) -> list[tuple[str, str, bool]]:
    """The schema signature the parity comparator asserts on."""
    return [(field.name, str(field.type), field.nullable) for field in table.schema]


def _spark_session() -> Any:
    """The recorded G10 basis (arrow-on toPandas + UTC)."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("repark-boundary-shapes-record")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


def _type_source(arrow_type: pa.DataType) -> str:
    """Python source that reconstructs ``arrow_type`` via the ``pa`` alias."""
    import pyarrow as pa

    if pa.types.is_int64(arrow_type):
        return "pa.int64()"
    if pa.types.is_int32(arrow_type):
        return "pa.int32()"
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "pa.string()"
    if pa.types.is_boolean(arrow_type):
        return "pa.bool_()"
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return "pa.binary()"
    if pa.types.is_timestamp(arrow_type):
        tz = arrow_type.tz
        if tz is None:
            return f"pa.timestamp({arrow_type.unit!r})"
        return f"pa.timestamp({arrow_type.unit!r}, tz={tz!r})"
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
        value_field = arrow_type.value_field
        return (
            f"pa.list_(pa.field({value_field.name!r}, "
            f"{_type_source(value_field.type)}, nullable={value_field.nullable}))"
        )
    return f"pa.type_for_alias({str(arrow_type)!r})"


def _scalar_source(value: object, arrow_type: pa.DataType) -> str:
    """Python literal source for one Arrow cell."""
    import pyarrow as pa

    if value is None:
        return "None"
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        assert isinstance(value, (bytes, bytearray))
        return f"bytes({list(value)!r})" if value != b"hello" else "b'hello'"
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
    """Python list literal source for one Arrow column."""
    return "[" + ", ".join(_scalar_source(cell, arrow_type) for cell in column) + "]"


def _emit_table(table: pa.Table) -> str:
    """Paste-ready ``_table(...)`` source for a live Arrow half."""
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


def _emit_pandas(shape: PandasShape) -> str:
    """Paste-ready ``_pandas_shape(...)`` source for a live pandas half."""
    dtypes = ", ".join(f"({name!r}, {kind!r})" for name, kind in shape.dtypes)
    kinds = ", ".join(f"({name!r}, {list(cell_kinds)!r})" for name, cell_kinds in shape.cell_kinds)
    value_parts = []
    for name, cells in shape.values:
        value_parts.append(f"{name!r}: {list(cells)!r}")
    values_joined = ", ".join(value_parts)
    return (
        f"_pandas_shape(\n"
        f"            [{dtypes}],\n"
        f"            [{kinds}],\n"
        f"            {{{values_joined}}},\n"
        f"        )"
    )


def _record_row(spark: Any, row: ShapeRow) -> str | None:
    """Re-derive one row. None = match; else a report string."""
    from test_boundary_shapes_parity import (
        PandasShape,
        assert_pandas_shapes_equal,
        run_row,
    )

    from repark_parity import FrameMismatchError, assert_frames_equal

    try:
        live = run_row(row, spark)
    except Exception as exc:
        return (
            f"[G10] {row.name} UNEXPECTED RAISE\n    live raised {type(exc).__name__}: {exc!s:.300}"
        )

    if row.spark is None:
        paste = _emit_pandas(live) if isinstance(live, PandasShape) else _emit_table(live)
        return f"[G10] {row.name} MISSING spark golden\n    live = {live!r}\n    paste:\n{paste}"

    try:
        if isinstance(row.spark, PandasShape):
            assert isinstance(live, PandasShape)
            assert_pandas_shapes_equal(live, row.spark)
        else:
            assert_frames_equal(live, row.spark)
    except FrameMismatchError as mismatch:
        paste = _emit_pandas(live) if isinstance(live, PandasShape) else _emit_table(live)
        return f"[G10] {row.name} MISMATCH\n    {mismatch}\n    paste:\n{paste}"
    print(f"[G10] {row.name} PASS")
    return None


def _emit_all(spark: Any) -> None:
    """Print paste-ready Spark (and repark, when divergent) halves for every row."""
    from test_boundary_shapes_parity import (
        ROWS,
        PandasShape,
        _halves_differ,
        run_row,
    )

    repark_session = None
    try:
        import repark
        from repark.spark.session import _reset_active_session_for_tests

        _reset_active_session_for_tests()
        repark_session = repark.ReparkSession.builder.appName("boundary-shapes-emit").getOrCreate()
    except Exception as exc:
        print(f"# repark session unavailable for emit: {type(exc).__name__}: {exc!s:.200}")

    try:
        for row in ROWS:
            live_spark = run_row(row, spark)
            print(f"\n# ===== {row.name} SPARK ({row.surface}) =====")
            if isinstance(live_spark, PandasShape):
                print(_emit_pandas(live_spark))
            else:
                print(_emit_table(live_spark))
            if repark_session is None:
                continue
            live_repark = run_row(row, repark_session)
            if _halves_differ(live_repark, live_spark):
                print(f"# ===== {row.name} REPARK (disclosure) =====")
                if isinstance(live_repark, PandasShape):
                    print(_emit_pandas(live_repark))
                else:
                    print(_emit_table(live_repark))
            else:
                print(f"# {row.name}: EQUALITY (repark matches spark) — repark=None")
    finally:
        if repark_session is not None:
            repark_session.stop()
            from repark.spark.session import _reset_active_session_for_tests

            _reset_active_session_for_tests()


def main() -> int:
    """Re-derive every recorded Spark half; return 0 when all of them still reproduce."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--emit",
        action="store_true",
        help="print paste-ready constructors (Spark + divergent repark halves)",
    )
    args = parser.parse_args()

    pinned = _pinned_pyspark_coordinate()
    runtime = version("pyspark")
    if runtime != pinned:
        print(
            f"pyspark runtime {runtime} != project record extra pin {pinned} "
            "(CP-8: derive from pyproject, do not restate)"
        )
        return 1
    print(f"pyspark {runtime} matches record extra pin")

    from test_boundary_shapes_parity import ROWS

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
