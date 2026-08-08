#!/usr/bin/env python3
"""CLI entry for the TPC-DS scoreboard (R-TPCDS-HARNESS / D1).

Usage::

    python python/repark-parity/bench/tpcds/run_tpcds.py --sf 1 \\
        [--out /tmp/tpcds-sf1.json] [--report task/tpcds-report-2026-07-31.md] \\
        [--repeats 3] [--timeout 120] [--timeout-retry 300] [--queries 1,2,3]

    # CI smoke seed (SF0.01, curated queries):
    python …/run_tpcds.py --sf 0.01 --repeats 1 --queries 3,6,7,19,42,52,55,82,91,96

Never touches AWS. Parquet cache: ``~/.cache/repark-tpcds (or $XDG_CACHE_HOME)/sf{N}/``.
D1: parquet temp views only — no Iceberg leg.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TPC-DS repark vs DuckDB scoreboard")
    parser.add_argument("--sf", type=float, default=1.0, help="scale factor (default 1; max 100)")
    parser.add_argument("--repeats", type=int, default=3, help="median-of-N wall times")
    parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="seconds per query side (default 120)",
    )
    parser.add_argument(
        "--timeout-retry",
        type=float,
        default=None,
        help="seconds for the single post-TIMEOUT retry (default 300)",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=None,
        help="parquet cache root (default ~/.cache/repark-tpcds or $XDG_CACHE_HOME)",
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
        help="append Markdown section to an existing report",
    )
    parser.add_argument(
        "--ledger",
        type=Path,
        default=None,
        help="write status ledger JSON (for SF1 smoke pins)",
    )
    parser.add_argument(
        "--queries",
        type=str,
        default=None,
        help="comma-separated query numbers to run (default: all 99)",
    )
    parser.add_argument(
        "--isolation",
        choices=("inprocess", "subprocess"),
        default=None,
        help="query isolation (default: inprocess)",
    )
    parser.add_argument(
        "--min-free-gib",
        type=float,
        default=None,
        help="SF>=1 disk gate (default 5 GiB free; skip with FINDING if below)",
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

    # Hard cap: SF>100 is multi-hundred-GB territory; refuse NaN/inf/≤0 too.
    if not math.isfinite(args.sf) or args.sf <= 0 or args.sf > 100:
        print(
            f"error: --sf must be finite and in (0, 100]; got {args.sf} "
            "(SF1 TPC-DS is multi-GB parquet; higher needs deliberate disk planning)",
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

    tpcds_dir = Path(__file__).resolve().parent
    package_name = "repark_tpcds_bench"
    package = types.ModuleType(package_name)
    package.__path__ = [str(tpcds_dir)]  # type: ignore[attr-defined]
    sys.modules[package_name] = package

    import importlib

    runner = importlib.import_module(f"{package_name}.runner")
    render_markdown_report = runner.render_markdown_report
    run_scoreboard = runner.run_scoreboard
    status_ledger = runner.status_ledger

    query_filter: set[int] | None = None
    if args.queries:
        query_filter = set()
        for part in args.queries.split(","):
            token = part.strip()
            if not token:
                continue
            try:
                number = int(token)
            except ValueError:
                print(
                    f"error: --queries entries must be integers 1..99; got {token!r}",
                    file=sys.stderr,
                )
                return 2
            if number < 1 or number > 99:
                print(
                    f"error: --queries entry out of range 1..99; got {number}",
                    file=sys.stderr,
                )
                return 2
            query_filter.add(number)
        if not query_filter:
            print("error: --queries parsed to empty set", file=sys.stderr)
            return 2

    board = run_scoreboard(
        scale_factor=args.sf,
        data_root=args.data_root,
        repeats=args.repeats,
        timeout_s=args.timeout,
        timeout_retry_s=args.timeout_retry,
        query_filter=query_filter,
        isolation=args.isolation,
        min_free_disk_gib=args.min_free_gib,
    )

    title = args.title
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
        section = report_md
        if section.startswith("# "):
            first_nl = section.find("\n")
            head = section[2:first_nl] if first_nl > 0 else section[2:]
            body = section[first_nl + 1 :] if first_nl > 0 else ""
            section = f"## {head.strip()}\n{body}"
        args.report_append.write_text(prior + section.rstrip() + "\n", encoding="utf-8")
        print(f"report-append → {args.report_append}", file=sys.stderr)

    if args.ledger is not None:
        args.ledger.parent.mkdir(parents=True, exist_ok=True)
        # Full SF matrix pin: require 99 when no --queries filter (smoke ledger).
        expect_count = None if query_filter is not None else 99
        if board.skipped:
            expect_count = None
        try:
            ledger_payload = status_ledger(board, expect_query_count=expect_count)
        except ValueError as exc:
            print(f"error: ledger not written: {exc}", file=sys.stderr)
            return 2
        args.ledger.write_text(
            json.dumps(ledger_payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(f"ledger → {args.ledger}", file=sys.stderr)

    # Distinct exit codes: usage=2; WRONG=3; ERROR=4; TIMEOUT=5; DIED=6.
    return runner.exit_code_for_board(board)


if __name__ == "__main__":
    raise SystemExit(main())
