#!/usr/bin/env python3
"""CLI entry for the TPC-H scoreboard (R-TPCH-HARNESS + R-TPCH-V3 + B1 Sail).

Usage::

    python python/repark-parity/bench/tpch/run_tpch.py --sf 1 \\
        [--out /tmp/tpch-sf1.json] [--report task/tpch-report-2026-07-31.md] \\
        [--repeats 3] [--timeout 120] [--queries 1,2,3]

    # V3 SF10 (subprocess isolation, 300s default, disk gate ≥30 GiB):
    python …/run_tpch.py --sf 10 --repeats 1 --report-append task/tpch-report-….md

    # V3 Iceberg leg (SF1, local memory catalog):
    python …/run_tpch.py --sf 1 --storage iceberg \\
        --warehouse ~/.cache/repark-tpch/iceberg-sf1 --report-append …

    # B1 Sail leg / three-way (pysail in this interpreter or --sail-python):
    python …/run_tpch.py --sf 1 --engine sail --report task/sail-bench-report-….md
    python …/run_tpch.py --sf 1 --engine both --sail-python /path/to/sail-venv/bin/python

Never touches AWS. Parquet cache: ``~/.cache/repark-tpch (or $XDG_CACHE_HOME/repark-tpch)/sf{N}/``.
Never adds pysail to repo dependency files.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TPC-H repark vs DuckDB scoreboard")
    parser.add_argument("--sf", type=float, default=1.0, help="scale factor (default 1; max 100)")
    parser.add_argument("--repeats", type=int, default=3, help="median-of-N wall times")
    parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="seconds per query side (default 120; SF≥10 defaults to 300)",
    )
    parser.add_argument(
        "--timeout-retry",
        type=float,
        default=None,
        help="seconds for the single post-TIMEOUT retry (default 300; B1 Slow vs hung)",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=None,
        help="parquet cache root (default ~/.cache/repark-tpch (or $XDG_CACHE_HOME/repark-tpch))",
    )
    parser.add_argument("--out", type=Path, default=None, help="write JSON scoreboard")
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="write Markdown report (overwrite)",
    )
    parser.add_argument(
        "--report-append",
        type=Path,
        default=None,
        help="append Markdown section to an existing report (V3 legs)",
    )
    parser.add_argument(
        "--ledger",
        type=Path,
        default=None,
        help="write status ledger JSON (for SF0.01 smoke pins)",
    )
    parser.add_argument(
        "--queries",
        type=str,
        default=None,
        help="comma-separated query numbers to run (default: all 22)",
    )
    parser.add_argument(
        "--isolation",
        choices=("inprocess", "subprocess"),
        default=None,
        help="query isolation (default: inprocess; SF≥10 → subprocess)",
    )
    parser.add_argument(
        "--storage",
        choices=("parquet", "iceberg"),
        default="parquet",
        help="repark table storage (iceberg = V3 local memory-catalog leg)",
    )
    parser.add_argument(
        "--engine",
        choices=("repark", "sail", "both"),
        default="repark",
        help="subject engine (B1: sail | both for three-way repark/Sail/DuckDB)",
    )
    parser.add_argument(
        "--sail-python",
        type=Path,
        default=None,
        help="Sail-venv python for engine=both when pysail is not in this interpreter",
    )
    parser.add_argument(
        "--warehouse",
        type=Path,
        default=None,
        help="Iceberg local warehouse directory (storage=iceberg)",
    )
    parser.add_argument(
        "--min-free-gib",
        type=float,
        default=30.0,
        help="SF≥10 disk gate (default 30 GiB free; skip with FINDING if below)",
    )
    parser.add_argument(
        "--title",
        type=str,
        default=None,
        help="Markdown report H1 title override",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    # Hard cap: SF>100 is multi-hundred-GB territory; refuse NaN/inf/≤0 too (C2-SEC-003).
    if not math.isfinite(args.sf) or args.sf <= 0 or args.sf > 100:
        print(
            f"error: --sf must be finite and in (0, 100]; got {args.sf} "
            "(SF10 ≈ 10GB parquet; higher needs deliberate disk planning)",
            file=sys.stderr,
        )
        return 2
    if args.repeats < 1 or args.repeats > 20:
        print(f"error: --repeats must be in [1, 20]; got {args.repeats}", file=sys.stderr)
        return 2
    if args.timeout is not None and (
        not math.isfinite(args.timeout) or args.timeout <= 0 or args.timeout > 3600
    ):
        print(
            f"error: --timeout must be finite and in (0, 3600]; got {args.timeout}",
            file=sys.stderr,
        )
        return 2
    if args.timeout_retry is not None and (
        not math.isfinite(args.timeout_retry)
        or args.timeout_retry <= 0
        or args.timeout_retry > 7200
    ):
        print(
            f"error: --timeout-retry must be finite and in (0, 7200]; got {args.timeout_retry}",
            file=sys.stderr,
        )
        return 2
    # Load this directory as a real package so relative imports in runner/work.
    import types

    tpch_dir = Path(__file__).resolve().parent
    package_name = "repark_tpch_bench"
    package = types.ModuleType(package_name)
    package.__path__ = [str(tpch_dir)]  # type: ignore[attr-defined]
    sys.modules[package_name] = package

    import importlib

    runner = importlib.import_module(f"{package_name}.runner")
    datagen = importlib.import_module(f"{package_name}.datagen")
    render_markdown_report = runner.render_markdown_report
    run_scoreboard = runner.run_scoreboard
    status_ledger = runner.status_ledger

    warehouse = args.warehouse
    if args.storage == "iceberg" and warehouse is None:
        warehouse = datagen.default_data_root() / f"iceberg-sf{_sf_label(args.sf)}"

    query_filter: set[int] | None = None
    if args.queries:
        try:
            query_filter = {int(part.strip()) for part in args.queries.split(",") if part.strip()}
        except ValueError:
            print(
                f"error: --queries must be comma-separated integers; got {args.queries!r}",
                file=sys.stderr,
            )
            return 2
        if not query_filter:
            print("error: --queries resolved to an empty set", file=sys.stderr)
            return 2

    board = run_scoreboard(
        scale_factor=args.sf,
        data_root=args.data_root,
        repeats=args.repeats,
        timeout_s=args.timeout,
        timeout_retry_s=args.timeout_retry,
        query_filter=query_filter,
        isolation=args.isolation,
        storage=args.storage,
        warehouse=warehouse,
        min_free_disk_gib=args.min_free_gib,
        engine=args.engine,
        sail_python=args.sail_python,
    )

    title = args.title
    if title is None and args.engine == "both":
        title = (
            f"TPC-H B1 three-way SF{args.sf:g} (repark / Sail / DuckDB) — "
            f"{board.environment.get('machine', '')}"
        )
    elif title is None and args.engine == "sail":
        title = f"TPC-H B1 Sail leg SF{args.sf:g} — {board.environment.get('machine', '')}"
    elif title is None and args.storage == "iceberg":
        title = f"TPC-H V3 Iceberg leg SF{args.sf:g} — {board.environment.get('machine', '')}"
    elif title is None and args.sf >= 10:
        title = f"TPC-H V3 SF{args.sf:g} scoreboard — {board.environment.get('machine', '')}"

    report_md = render_markdown_report(board, title=title)
    print(report_md)

    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(board.to_json() + "\n", encoding="utf-8")
        print(f"JSON → {args.out}", file=sys.stderr)

    if args.report is not None:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(report_md, encoding="utf-8")
        print(f"report → {args.report}", file=sys.stderr)

    if args.report_append is not None:
        args.report_append.parent.mkdir(parents=True, exist_ok=True)
        prior = ""
        if args.report_append.is_file():
            prior = args.report_append.read_text(encoding="utf-8").rstrip() + "\n\n"
        # Append uses ## section under existing V1 H1 when title starts with TPC-H V3.
        section = report_md
        if section.startswith("# "):
            # Demote H1 → H2 so it nests under the V1 report file.
            first_nl = section.find("\n")
            head = section[2:first_nl] if first_nl > 0 else section[2:]
            body = section[first_nl + 1 :] if first_nl > 0 else ""
            section = f"## {head.strip()}\n{body}"
        args.report_append.write_text(prior + section.rstrip() + "\n", encoding="utf-8")
        print(f"report-append → {args.report_append}", file=sys.stderr)

    if args.ledger is not None:
        args.ledger.parent.mkdir(parents=True, exist_ok=True)
        args.ledger.write_text(
            json.dumps(status_ledger(board), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(f"ledger → {args.ledger}", file=sys.stderr)

    # Distinct exit codes (E1-L-005): usage=2; WRONG=3; ERROR=4; TIMEOUT=5; DIED=6.
    return runner.exit_code_for_board(board)


def _sf_label(scale_factor: float) -> str:
    if scale_factor == int(scale_factor):
        return str(int(scale_factor))
    return f"{scale_factor:g}"


if __name__ == "__main__":
    raise SystemExit(main())
