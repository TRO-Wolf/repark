"""FACADE-5 step 0 display-renderer baseline: FETCH and FORMAT legs timed separately."""

from __future__ import annotations

import datetime
import html as html_module
import io
import json
import os
import statistics
import subprocess
import sys
import time
from contextlib import redirect_stdout
from decimal import Decimal
from pathlib import Path
from typing import Any

FRAME_ROWS = 1100
N_VALUES: tuple[int, ...] = (20, 1000)
STYLED_MAX_ROWS: tuple[int, ...] = (10, 0)
WARMUPS = 1
REPS = 5
IDLE_PROCESSES: tuple[str, ...] = ("cargo", "rustc", "maturin")
LOAD_CEILING = 6.0
IDLE_SLEEP_S = 15.0

BASE_DATE = datetime.date(2024, 1, 1)
BASE_TS = datetime.datetime(2024, 1, 1, 12, 0, 0)

FLAT7_DDL = "i BIGINT, f DOUBLE, s STRING, b BOOLEAN, d DATE, t TIMESTAMP, dc DECIMAL(38, 18)"
NESTED_DDL = "id INT, st STRUCT<a: INT, b: STRING>, li ARRAY<INT>, m MAP<STRING, INT>"


def load1() -> float:
    """The 1-minute load average."""
    return os.getloadavg()[0]


def builders_busy() -> bool:
    """Whether any cargo/rustc/maturin process is running on the box."""
    return any(
        subprocess.run(
            ["pgrep", "-x", name],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode
        == 0
        for name in IDLE_PROCESSES
    )


def wait_for_idle() -> None:
    """Block until no builder runs and the 1-minute load is under the ceiling."""
    while builders_busy() or load1() >= LOAD_CEILING:
        time.sleep(IDLE_SLEEP_S)


def native_is_release() -> bool:
    """True when the installed native module was built without debug assertions."""
    import repark._native as native

    return bool(getattr(native, "__debug_assertions__", True)) is False


def build_session() -> Any:
    """One facade session at 8 shuffle partitions and UTC."""
    from repark import ReparkSession

    active = ReparkSession.getActiveSession()
    if active is not None:
        active.stop()
    return (
        ReparkSession.builder.appName("facade-5-display-baseline")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def flat7_row(index: int) -> tuple[Any, ...]:
    """One flat seven-type row whose string cell exceeds every truncate cap."""
    return (
        index,
        index + 0.5,
        f"s{index}-" + "x" * 40,
        index % 2 == 0,
        BASE_DATE + datetime.timedelta(days=index % 365),
        BASE_TS + datetime.timedelta(seconds=index),
        Decimal(index).scaleb(-2),
    )


def wide50_ddl() -> str:
    """The 50-column DDL alternating BIGINT and STRING."""
    return ", ".join(
        f"c{index:02d} {'BIGINT' if index % 2 == 0 else 'STRING'}" for index in range(50)
    )


def wide50_row(index: int) -> tuple[Any, ...]:
    """One 50-column row alternating ints and truncatable strings."""
    return tuple(
        index * 100 + column if column % 2 == 0 else f"v{column}-{index}-" + "y" * 30
        for column in range(50)
    )


def nested_row(index: int) -> tuple[Any, ...]:
    """One struct/array/map row whose nested strings exceed every truncate cap."""
    return (
        index,
        (index, f"s{index}-" + "x" * 30),
        [index, index + 1, index + 2, index + 3],
        {f"k{index}": index, f"k{index + 1}": index + 1},
    )


def build_fixtures(session: Any) -> dict[str, Any]:
    """The three fixture frames, built once outside every timed region."""
    return {
        "flat7": session.createDataFrame(
            [flat7_row(index) for index in range(FRAME_ROWS)], FLAT7_DDL
        ),
        "wide50": session.createDataFrame(
            [wide50_row(index) for index in range(FRAME_ROWS)], wide50_ddl()
        ),
        "nested": session.createDataFrame(
            [nested_row(index) for index in range(FRAME_ROWS)], NESTED_DDL
        ),
    }


def capture_show(frame: Any, *args: Any, **kwargs: Any) -> str:
    """Run ``frame.show(...)`` and return stdout without the trailing newline."""
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        frame.show(*args, **kwargs)
    return buffer.getvalue().removesuffix("\n")


def fetch_spark(frame: Any, fetch_n: int) -> dict[str, Any]:
    """FETCH leg for the spark doors: capped rows to an Arrow table."""
    return {"table": frame.limit(fetch_n).to_arrow()}


def format_ascii(leg: dict[str, Any], cap: int | None) -> str:
    """FORMAT leg: the PySpark ASCII grid over the pre-materialized table."""
    from repark.spark.dataframe.plan_collapse import _format_show_table

    return _format_show_table(leg["table"], truncate_at=cap)


def format_vertical(leg: dict[str, Any], cap: int | None, n: int) -> str:
    """FORMAT leg: the vertical ``-RECORD`` layout over the pre-fetched table."""
    from repark.spark.dataframe.plan_collapse import _format_show_vertical

    table = leg["table"]
    has_more = table.num_rows > n
    shown = table.slice(0, n)
    total_rows = n + 1 if has_more else None
    return _format_show_vertical(shown, truncate_at=cap, n=n, total_rows=total_rows)


def footer_text(max_rows: int) -> str:
    """The ``only showing top N`` footer line the eager doors append."""
    return f"only showing top {max_rows} row" + ("s" if max_rows != 1 else "")


def format_eager(leg: dict[str, Any], cap: int | None, n: int) -> str:
    """FORMAT leg: the abutted eager-eval grid plus the footer."""
    from repark.spark.dataframe.plan_collapse import _format_eager_eval_table

    table = leg["table"]
    has_more = table.num_rows > n
    shown = table.slice(0, n)
    rendered = _format_eager_eval_table(shown, truncate_at=cap)
    if has_more:
        rendered = f"{rendered}\n{footer_text(n)}"
    return rendered


def format_html(leg: dict[str, Any], cap: int | None, n: int) -> str:
    """FORMAT leg: the ``_repr_html_`` table body plus the footer."""
    from repark.spark.dataframe.plan_collapse import _table_to_cell_rows

    table = leg["table"]
    has_more = table.num_rows > n
    shown = table.slice(0, n)
    names = list(shown.column_names)
    rows = _table_to_cell_rows(shown, truncate_at=None, style="spark")
    if cap is not None and cap > 0:
        rows = [[cell[:cap] for cell in row] for row in rows]
    safe_names = [html_module.escape(name, quote=True) for name in names]
    parts = [
        "<table border='1'>",
        "<tr>" + "".join(f"<th>{name}</th>" for name in safe_names) + "</tr>",
    ]
    for row in rows:
        safe_cells = [html_module.escape(cell, quote=True) for cell in row]
        parts.append("<tr>" + "".join(f"<td>{cell}</td>" for cell in safe_cells) + "</tr>")
    parts.append("</table>")
    html = "\n".join(parts)
    if has_more:
        html = f"{html}\n{footer_text(n)}"
    return html


def styled_head_tail(style: str, n: int, max_rows: int) -> tuple[int, int, bool]:
    """The keep-set split a styled door applies once the probe fills."""
    if style == "polars":
        head_n, tail_n = 0, 0
        if n > 0:
            keep = min(n, max_rows)
            head_n = (keep + 1) // 2
            tail_n = keep - head_n
        return head_n, tail_n, tail_n > 0 or n >= max_rows
    if n <= 0:
        return 0, 0, False
    head_n = n // 2
    if head_n == 0:
        head_n = 1
    return head_n, n - head_n, n - head_n > 0


def fetch_styled(frame: Any, style: str, n: int, max_rows: int) -> dict[str, Any]:
    """FETCH leg for styled doors: probe, count when it fills, head and tail tables."""
    probe_limit = max_rows + 1
    probe_table = frame.limit(probe_limit).to_arrow()
    leg: dict[str, Any] = {"probe_table": probe_table, "style": style}
    if probe_table.num_rows < probe_limit:
        leg["total_rows"] = probe_table.num_rows
        head_n = max(n, 0) if style == "polars" else min(n, probe_table.num_rows)
        leg["head_table"] = probe_table.slice(0, head_n)
        leg["tail_table"] = None
        leg["use_ellipsis"] = False
        return leg
    total_rows = frame.count()
    head_n, tail_n, use_ellipsis = styled_head_tail(style, n, max_rows)
    leg["total_rows"] = total_rows
    leg["use_ellipsis"] = use_ellipsis
    if style == "polars":
        leg["head_table"] = probe_table.slice(0, head_n)
    else:
        leg["head_table"] = (
            frame.limit(head_n).to_arrow() if head_n > 0 else frame.limit(0).to_arrow()
        )
    leg["tail_table"] = (
        frame._preview_tail_rows(tail_n, total_rows=total_rows) if tail_n > 0 else None
    )
    return leg


def format_styled(frame: Any, leg: dict[str, Any], n: int, cap: int | None) -> str:
    """FORMAT leg for styled doors: cell extraction plus the box renderer."""
    from repark.spark.dataframe.display import _display_session_ints
    from repark.spark.dataframe.plan_collapse import (
        _display_type_labels_from_arrow,
        _format_duckdb_show,
        _format_polars_show,
        _table_to_cell_rows,
    )

    style = leg["style"]
    head_table = leg["head_table"]
    tail_table = leg["tail_table"]
    use_ellipsis = leg["use_ellipsis"]
    total_rows = leg["total_rows"]
    col_names = list(frame.columns)
    _, max_cols, _ = _display_session_ints(frame)
    type_labels = _display_type_labels_from_arrow(head_table, style=style)
    head_rows = _table_to_cell_rows(head_table, truncate_at=cap, style=style)
    tail_rows = (
        _table_to_cell_rows(tail_table, truncate_at=cap, style=style)
        if tail_table is not None
        else []
    )
    shown = len(head_rows) + len(tail_rows)
    if style == "polars":
        return _format_polars_show(
            col_names,
            type_labels,
            head_rows,
            tail_rows if use_ellipsis else [],
            total_rows=total_rows,
            show_ellipsis=use_ellipsis,
            max_cols=max_cols,
        )
    return _format_duckdb_show(
        col_names,
        type_labels,
        head_rows,
        tail_rows if use_ellipsis else [],
        total_rows=total_rows,
        shown_rows=shown,
        show_ellipsis=use_ellipsis,
    )


def set_conf(session: Any, key: str, value: str | None) -> None:
    """Set or unset one runtime conf key."""
    if value is None:
        session.conf.unset(key)
    else:
        session.conf.set(key, value)


def run_pair(frame: Any, fetch_fn: Any, format_fn: Any) -> tuple[float, float, str]:
    """One fetch+format rep; returns (fetch_s, format_s, rendered)."""
    started = time.perf_counter()
    leg = fetch_fn()
    fetched = time.perf_counter()
    rendered = format_fn(leg)
    formatted = time.perf_counter()
    return fetched - started, formatted - fetched, rendered


def door_cells(session: Any, fixtures: dict[str, Any]) -> list[dict[str, Any]]:
    """Every (door, n, truncate, shape) cell descriptor."""
    cells: list[dict[str, Any]] = []
    for shape_name in fixtures:
        for n in N_VALUES:
            for truncate in ("on", "off"):
                cap: int | None = 20 if truncate == "on" else None
                cells.append(
                    {
                        "door": "ascii_show",
                        "shape": shape_name,
                        "n": n,
                        "truncate": truncate,
                        "fetch_n": n,
                        "cap": cap,
                        "show_args": {"n": n, "truncate": truncate == "on"},
                    }
                )
                cells.append(
                    {
                        "door": "vertical_show",
                        "shape": shape_name,
                        "n": n,
                        "truncate": truncate,
                        "fetch_n": n + 1,
                        "cap": cap,
                        "show_args": {"n": n, "truncate": truncate == "on", "vertical": True},
                    }
                )
                eager_cap: int | None = 20 if truncate == "on" else 0
                for door in ("eager_repr", "html"):
                    cells.append(
                        {
                            "door": door,
                            "shape": shape_name,
                            "n": n,
                            "truncate": truncate,
                            "fetch_n": n + 1,
                            "cap": eager_cap,
                        }
                    )
                for style in ("polars", "duckdb"):
                    for max_rows in STYLED_MAX_ROWS:
                        effective = max_rows if max_rows > 0 else n
                        cells.append(
                            {
                                "door": f"{style}_show",
                                "shape": shape_name,
                                "n": n,
                                "truncate": truncate,
                                "max_rows": effective,
                                "cap": None if truncate == "off" else "str_len",
                                "show_args": {"n": n, "truncate": truncate == "on"},
                            }
                        )
    return cells


def configure_cell(session: Any, cell: dict[str, Any]) -> None:
    """Point the session at the style and conf the cell's door would see."""
    door = cell["door"]
    set_conf(session, "spark.sql.repl.eagerEval.enabled", None)
    set_conf(session, "spark.sql.repl.eagerEval.maxNumRows", None)
    set_conf(session, "spark.sql.repl.eagerEval.truncate", None)
    set_conf(session, "repark.display.max_rows", None)
    if door in {"polars_show", "duckdb_show"}:
        session.display_style = door.removesuffix("_show")
        if cell["max_rows"] != 10:
            session.conf.set("repark.display.max_rows", str(cell["max_rows"]))
    else:
        session.display_style = "spark"
    if door in {"eager_repr", "html"}:
        session.conf.set("spark.sql.repl.eagerEval.enabled", "true")
        session.conf.set("spark.sql.repl.eagerEval.maxNumRows", str(cell["n"]))
        session.conf.set(
            "spark.sql.repl.eagerEval.truncate", "20" if cell["truncate"] == "on" else "0"
        )


def legs_for(session: Any, cell: dict[str, Any], frame: Any) -> tuple[Any, Any]:
    """The (fetch_fn, format_fn) pair reproducing the door's two legs."""
    door = cell["door"]
    if door == "ascii_show":
        return (
            lambda: fetch_spark(frame, cell["fetch_n"]),
            lambda leg: format_ascii(leg, cell["cap"]),
        )
    if door == "vertical_show":
        return (
            lambda: fetch_spark(frame, cell["fetch_n"]),
            lambda leg: format_vertical(leg, cell["cap"], cell["n"]),
        )
    if door == "eager_repr":
        return (
            lambda: fetch_spark(frame, cell["fetch_n"]),
            lambda leg: format_eager(leg, cell["cap"], cell["n"]),
        )
    if door == "html":
        return (
            lambda: fetch_spark(frame, cell["fetch_n"]),
            lambda leg: format_html(leg, cell["cap"], cell["n"]),
        )
    style = cell["door"].removesuffix("_show")
    max_rows = cell["max_rows"]
    cap = cell["cap"]

    def fetch() -> dict[str, Any]:
        return fetch_styled(frame, style, cell["n"], max_rows)

    def format_leg(leg: dict[str, Any]) -> str:
        resolved = None
        if cap == "str_len":
            from repark.spark.dataframe.display import _display_session_ints

            resolved = _display_session_ints(frame)[2]
        else:
            resolved = cap
        return format_styled(frame, leg, cell["n"], resolved)

    return fetch, format_leg


def door_check(session: Any, cell: dict[str, Any], frame: Any, rendered: str) -> bool:
    """Whether the leg-composed string equals the real door's bytes."""
    door = cell["door"]
    if door == "eager_repr":
        return repr(frame) == rendered
    if door == "html":
        return frame._repr_html_() == rendered
    args = cell["show_args"]
    shown = capture_show(
        frame, args["n"], truncate=args["truncate"], vertical=args.get("vertical", False)
    )
    return shown == rendered


def cell_label(cell: dict[str, Any]) -> str:
    """The report row name for one cell."""
    label = f"{cell['door']}/{cell['shape']}/n{cell['n']}/trunc-{cell['truncate']}"
    if "max_rows" in cell:
        label += f"/mr{cell['max_rows']}"
    return label


def machine_header() -> dict[str, Any]:
    """CPU/kernel/python/pyarrow/native facts recorded beside the numbers."""
    import pyarrow as pa

    import repark._native as native

    cpu = "unknown"
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.is_file():
        for line in cpuinfo.read_text(encoding="utf-8").splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    native_file = getattr(native, "__file__", "")
    native_size = Path(native_file).stat().st_size if native_file else 0
    return {
        "cpu": cpu,
        "kernel": os.uname().release,
        "python": sys.version.split()[0],
        "pyarrow": pa.__version__,
        "native_bytes": native_size,
        "release_proof": native_is_release(),
    }


def main() -> int:
    """Run the full battery and write JSON to the argument path."""
    out_json = (
        Path(sys.argv[1]) if len(sys.argv) > 1 else Path("/tmp/facade-5-display-baseline.json")
    )
    if not native_is_release():
        raise SystemExit("native module is not a release build; refusing to measure")
    session = build_session()
    header = machine_header()
    run_load_start = load1()
    fixtures = build_fixtures(session)
    cells = door_cells(session, fixtures)
    results: list[dict[str, Any]] = []
    for cell in cells:
        frame = fixtures[cell["shape"]]
        configure_cell(session, cell)
        fetch_fn, format_fn = legs_for(session, cell, frame)
        wait_for_idle()
        for _ in range(WARMUPS):
            run_pair(frame, fetch_fn, format_fn)
        fetch_samples: list[float] = []
        format_samples: list[float] = []
        rendered = ""
        load_start = load1()
        for _ in range(REPS):
            fetch_s, format_s, rendered = run_pair(frame, fetch_fn, format_fn)
            fetch_samples.append(fetch_s)
            format_samples.append(format_s)
        load_end = load1()
        ok = door_check(session, cell, frame, rendered)
        wall = [f + g for f, g in zip(fetch_samples, format_samples, strict=True)]
        results.append(
            {
                "cell": cell_label(cell),
                "fetch_median_ms": statistics.median(fetch_samples) * 1000.0,
                "format_median_ms": statistics.median(format_samples) * 1000.0,
                "wall_median_ms": statistics.median(wall) * 1000.0,
                "format_share": statistics.median(format_samples) / statistics.median(wall),
                "fetch_samples_ms": [sample * 1000.0 for sample in fetch_samples],
                "format_samples_ms": [sample * 1000.0 for sample in format_samples],
                "door_check": ok,
                "load_start": load_start,
                "load_end": load_end,
            }
        )
        print(
            f"{cell_label(cell):>44}  fetch {statistics.median(fetch_samples) * 1000:9.3f} ms  "
            f"format {statistics.median(format_samples) * 1000:9.3f} ms  "
            f"share {statistics.median(format_samples) / statistics.median(wall):5.1%}  "
            f"door={'ok' if ok else 'MISMATCH'}  load {load_start:.2f}->{load_end:.2f}",
            flush=True,
        )
    session.display_style = "spark"
    payload = {
        "header": header,
        "run_load": [run_load_start, load1()],
        "frame_rows": FRAME_ROWS,
        "reps": REPS,
        "warmups": WARMUPS,
        "cells": results,
    }
    out_json.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {out_json}")
    session.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
