"""Orchestrate the facade-boundary battery and render its markdown tables."""

from __future__ import annotations

import functools
import statistics
import time
from pathlib import Path
from typing import Any

from facade import cells
from facade.fixture import COLUMNS, ensure_fixture

CELL_GROUPS = ("export", "collect", "rows", "create", "chain")


def _read(session: Any, path: Path) -> Any:
    """Read the fixture parquet as a repark DataFrame."""
    return session.read.parquet(str(path))


def _export_cells(session: Any, bed: Path, rows: int, iterations: int) -> list[dict[str, Any]]:
    """Arrow-export controls that this unit must not move."""
    frame = _read(session, ensure_fixture(bed, rows))
    narrow = frame.select("id", "v")
    return [
        cells.time_cell(
            f"export/{rows}/to_arrow", lambda: frame.to_arrow().num_rows, iterations=iterations
        ),
        cells.time_cell(
            f"export/{rows}/to_arrow_2col",
            lambda: narrow.to_arrow().num_rows,
            iterations=iterations,
        ),
        cells.time_cell(
            f"export/{rows}/toPandas", lambda: len(frame.toPandas()), iterations=iterations
        ),
        cells.time_cell(f"export/{rows}/count", frame.count, iterations=iterations),
    ]


def _collect_cells(session: Any, bed: Path, rows: int, iterations: int) -> list[dict[str, Any]]:
    """End-to-end ``collect()`` on the seven-column frame and on a two-column projection."""
    frame = _read(session, ensure_fixture(bed, rows))
    narrow = frame.select("id", "v")
    inner = min(iterations, cells.COLLECT_ITERATIONS)
    return [
        cells.time_cell(
            f"collect_old/{rows}",
            functools.partial(cells.collect_with_old_converter, frame),
            iterations=inner,
        ),
        cells.time_cell(f"collect/{rows}", lambda: len(frame.collect()), iterations=inner),
        cells.time_cell(f"collect_2col/{rows}", lambda: len(narrow.collect()), iterations=inner),
    ]


def _rows_cells(session: Any, bed: Path, rows: int, iterations: int) -> list[dict[str, Any]]:
    """Matched A/B of the two row converters over one set of pre-collected batches."""
    frame = _read(session, ensure_fixture(bed, rows))
    batches = list(frame.to_arrow_batches())
    inner = min(iterations, cells.COLLECT_ITERATIONS)
    return [
        cells.time_cell(
            f"rows_old/{rows}",
            functools.partial(cells.rows_via_old, batches),
            iterations=inner,
        ),
        cells.time_cell(
            f"rows_new/{rows}",
            functools.partial(cells.rows_via_new, batches),
            iterations=inner,
        ),
    ]


def _create_cells(session: Any, bed: Path, iterations: int) -> list[dict[str, Any]]:
    """``createDataFrame`` old-vs-new pairs plus the pandas control."""
    import pyarrow.parquet as pq

    table = pq.read_table(str(ensure_fixture(bed, cells.CREATE_ROWS)))
    tuples = [tuple(row.values()) for row in table.to_pylist()]
    frame = table.to_pandas()
    names = list(COLUMNS)
    nested = cells.build_nested_tuples(cells.CREATE_NESTED_ROWS)
    nested_names = list(cells.CREATE_NESTED_NAMES)
    explicit = cells.CREATE_EXPLICIT_DDL
    inner = min(iterations, cells.COLLECT_ITERATIONS)
    return [
        cells.time_cell(
            f"create_old/{cells.CREATE_ROWS}/tuples_count",
            functools.partial(cells.create_with_old_dispatcher, session, tuples, names),
            iterations=inner,
        ),
        cells.time_cell(
            f"create/{cells.CREATE_ROWS}/tuples_count",
            lambda: session.createDataFrame(tuples, schema=names).count(),
            iterations=inner,
        ),
        cells.time_cell(
            f"create/{cells.CREATE_ROWS}/pandas_count",
            lambda: session.createDataFrame(frame).count(),
            iterations=inner,
        ),
        cells.time_cell(
            f"create_old/{cells.CREATE_NESTED_ROWS}/nested_count",
            functools.partial(cells.create_with_old_dispatcher, session, nested, nested_names),
            iterations=inner,
        ),
        cells.time_cell(
            f"create/{cells.CREATE_NESTED_ROWS}/nested_count",
            lambda: session.createDataFrame(nested, schema=nested_names).count(),
            iterations=inner,
        ),
        cells.time_cell(
            f"create_old/{cells.CREATE_ROWS}/explicit_count",
            functools.partial(cells.create_with_old_dispatcher, session, tuples, explicit),
            iterations=inner,
        ),
        cells.time_cell(
            f"create/{cells.CREATE_ROWS}/explicit_count",
            lambda: session.createDataFrame(tuples, schema=explicit).count(),
            iterations=inner,
        ),
    ]


def _chain_cells(session: Any, bed: Path, depth: int, iterations: int) -> list[dict[str, Any]]:
    """Stacked, pre-unit and collapsed-shape chain builds at one depth, plus its execution."""
    from repark.spark.dataframe.core import DataFrame

    base = _read(session, ensure_fixture(bed, cells.CREATE_ROWS))
    stacked = cells.time_cell(
        f"chain/{depth}/build_only",
        functools.partial(cells.build_chain_stacked, base, depth),
        iterations=iterations,
    )
    collapsed = cells.time_cell(
        f"chain_collapsed/{depth}/build_only",
        functools.partial(cells.build_chain_collapsed, base, depth),
        iterations=iterations,
    )
    built = cells.build_chain_stacked(base, depth)
    counted = cells.time_cell(f"chain/{depth}/count", built.count, iterations=iterations)
    shipped_columns = DataFrame.columns
    shipped_iter = DataFrame._iter_bound_columns
    DataFrame.columns = property(cells._old_columns)
    DataFrame._iter_bound_columns = cells._old_iter_bound_columns
    try:
        old = cells.time_cell(
            f"chain_old/{depth}/build_only",
            functools.partial(cells.build_chain_stacked, base, depth),
            iterations=iterations,
        )
    finally:
        DataFrame.columns = shipped_columns
        DataFrame._iter_bound_columns = shipped_iter
    return [old, stacked, collapsed, counted]


def _floor(session: Any, bed: Path, repeats: int, iterations: int) -> dict[str, Any]:
    """Repeat the floor cell and report the spread of its medians."""
    frame = _read(session, ensure_fixture(bed, cells.CREATE_ROWS))
    medians = [
        cells.time_cell(
            cells.FLOOR_CELL,
            lambda: len(frame.collect()),
            iterations=min(iterations, cells.COLLECT_ITERATIONS),
        )["median_ms"]
        for _ in range(repeats)
    ]
    return {
        "cell": cells.FLOOR_CELL,
        "repeats": repeats,
        "medians_ms": medians,
        "floor_ms": max(medians) - min(medians),
        "mean_ms": statistics.mean(medians),
    }


def run_battery(
    *,
    bed: Path,
    groups: tuple[str, ...] = CELL_GROUPS,
    iterations: int = cells.DEFAULT_ITERATIONS,
    floor_repeats: int = cells.DEFAULT_FLOOR_REPEATS,
) -> dict[str, Any]:
    """Run the requested cell groups on a release module and return the run record."""
    proof = cells.release_proof()
    started = time.perf_counter()
    load_start = cells.load1()
    session = cells.build_session()
    records: list[dict[str, Any]] = []
    try:
        for rows in cells.EXPORT_ROWS:
            if "export" in groups:
                records += _export_cells(session, bed, rows, iterations)
            if "collect" in groups:
                records += _collect_cells(session, bed, rows, iterations)
            if "rows" in groups:
                records += _rows_cells(session, bed, rows, iterations)
        if "create" in groups:
            records += _create_cells(session, bed, iterations)
        if "chain" in groups:
            for depth in cells.CHAIN_DEPTHS:
                records += _chain_cells(session, bed, depth, iterations)
        floor = _floor(session, bed, floor_repeats, iterations) if "collect" in groups else None
    finally:
        session.stop()
    return {
        "native": proof,
        "groups": list(groups),
        "iterations": iterations,
        "threads": cells.THREADS,
        "bed": str(bed),
        "run_date": time.strftime("%Y-%m-%d"),
        "load1_start": load_start,
        "load1_end": cells.load1(),
        "wall_seconds": time.perf_counter() - started,
        "floor": floor,
        "cells": records,
    }


def render_markdown(result: dict[str, Any]) -> str:
    """Render the run record as the baseline's cell table."""
    lines = [
        f"# facade boundary run ({result['run_date']})",
        "",
        f"- native `{result['native']['native_bytes']}` B, "
        f"debug_assertions `{result['native']['debug_assertions']}`",
        f"- threads `{result['threads']}` iterations `{result['iterations']}`",
        f"- load1 `{result['load1_start']:.2f}` -> `{result['load1_end']:.2f}`",
        "",
        "| cell | median_ms | min_ms | spread_ms | load1_start | load1_end |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for row in result["cells"]:
        lines.append(
            f"| {row['cell']} | {row['median_ms']:.2f} | {row['min_ms']:.2f} | "
            f"{row['spread_ms']:.2f} | {row['load1_start']:.2f} | {row['load1_end']:.2f} |"
        )
    floor = result.get("floor")
    if floor is not None:
        medians = ", ".join(f"{value:.2f}" for value in floor["medians_ms"])
        lines += [
            "",
            f"Floor cell `{floor['cell']}`: {floor['repeats']} repeats, medians {medians}, "
            f"floor **{floor['floor_ms']:.2f} ms**.",
        ]
    lines.append("")
    return "\n".join(lines)
