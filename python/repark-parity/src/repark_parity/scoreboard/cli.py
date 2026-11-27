from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from repark_parity.scoreboard.classify import diff_files
from repark_parity.scoreboard.controls import verify_controls
from repark_parity.scoreboard.corpus import check_corpus, ingest, load_exclusions
from repark_parity.scoreboard.launch import BATCH_SIZE
from repark_parity.scoreboard.replay import record_spark, replay
from repark_parity.scoreboard.settings import (
    MissingSettingError,
    add_setting_flags,
    resolve_settings,
)
from repark_parity.scoreboard.summary import (
    build_scoreboard,
    compare_scoreboards,
    flaky_cells,
    read_json,
    render_comparison,
    render_scoreboard,
    write_json,
)
from repark_parity.scoreboard.witness import witnesses

RUN_SETTINGS = ("cpus", "memory_floor_mib", "output_root")
REPARK_SETTINGS = ("package", "repark_python", "spark_python", "dependency_path", "java_home")
SPARK_SETTINGS = ("spark_python", "java_home", "spark_drivers")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m repark_parity.scoreboard")
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser(
        "replay", help="replay the RePark half against the recorded Spark answers"
    )
    add_setting_flags(run, RUN_SETTINGS + REPARK_SETTINGS)
    run.add_argument("destination", type=Path)
    run.add_argument(
        "--lane", action="append", help="replay whole lanes instead of every comparable cell"
    )
    run.add_argument("--cells", type=Path, help="a file of cell ids, one per line, to replay")
    run.add_argument("--jobs", type=int, default=1)
    run.add_argument("--batch-size", type=int, default=BATCH_SIZE)

    record = commands.add_parser("record", help="record fresh Spark answers for a case file")
    add_setting_flags(record, tuple(dict.fromkeys(RUN_SETTINGS + SPARK_SETTINGS + REPARK_SETTINGS)))
    record.add_argument("cases", type=Path)
    record.add_argument("destination", type=Path)
    record.add_argument(
        "--with-repark", action="store_true", help="also run the RePark half and diff"
    )

    diff = commands.add_parser("diff", help="classify one lane's answer files")
    for name in ("cases", "spark", "repark", "output"):
        diff.add_argument(name, type=Path)

    controls = commands.add_parser("controls", help="check a replayed controls lane")
    controls.add_argument("directory", type=Path)
    controls.add_argument(
        "--strict", action="store_true", help="fail when an expected class moved (audit package)"
    )

    flaky = commands.add_parser("flaky", help="list cells whose answers differ between runs")
    flaky.add_argument("runs", type=Path, nargs="+")
    flaky.add_argument("--output", type=Path, required=True)

    summary = commands.add_parser("summary", help="build one scoreboard from a replay directory")
    summary.add_argument("replay_dir", type=Path)
    summary.add_argument("--output", type=Path, required=True)
    summary.add_argument("--markdown", type=Path)
    summary.add_argument("--flaky", type=Path, help="the JSON the flaky command wrote")
    summary.add_argument("--label", required=True)
    summary.add_argument("--revision", required=True)
    summary.add_argument("--measured", required=True, help="the measurement date, YYYY-MM-DD")

    compare = commands.add_parser("compare", help="compare two scoreboards cell by cell")
    compare.add_argument("base", type=Path)
    compare.add_argument("head", type=Path)
    compare.add_argument("--output", type=Path, required=True)
    compare.add_argument("--markdown", type=Path)

    inspect = commands.add_parser("inspect", help="print joined witnesses from a lane directory")
    inspect.add_argument("directory", type=Path)
    inspect.add_argument("match", nargs="?", default="")
    inspect.add_argument("--class", dest="kind")
    inspect.add_argument("--limit", type=int, default=20)

    store = commands.add_parser("ingest", help="store a lane recorded live in the corpus")
    store.add_argument("lane")
    store.add_argument("cases", type=Path)
    store.add_argument("spark", type=Path)
    store.add_argument("--replace", action="store_true")

    commands.add_parser("check-corpus", help="check the stored corpus digests and ids")
    return parser


def _write_outputs(value: dict, output: Path, markdown: Path | None, text: str) -> None:
    write_json(output, value)
    if markdown:
        markdown.write_text(text, encoding="utf-8")
    sys.stdout.write(text)


def _dispatch(args: argparse.Namespace) -> None:
    settings = resolve_settings(args, os.environ)
    if args.command == "replay":
        cells = args.cells.read_text().split() if args.cells else None
        target = replay(settings, args.destination, args.lane, cells, args.jobs, args.batch_size)
        sys.stdout.write(f"{target}\n")
    elif args.command == "record":
        target = record_spark(settings, args.cases, args.destination, args.with_repark)
        sys.stdout.write(f"{target}\n")
    elif args.command == "diff":
        rows = diff_files(args.cases, args.spark, args.repark, args.output)
        sys.stdout.write(f"{len(rows)} cells classified\n")
    elif args.command == "controls":
        summary, moved = verify_controls(args.directory, args.strict)
        for row in moved:
            sys.stdout.write(json.dumps(row, sort_keys=True) + "\n")
        sys.stdout.write(f"{summary}; {len(moved)} expected classes moved\n")
    elif args.command == "flaky":
        found = flaky_cells(args.runs)
        write_json(args.output, {"flaky": found})
        sys.stdout.write(f"{len(found)} cells differ between runs\n")
    elif args.command == "summary":
        flaky = set(read_json(args.flaky)["flaky"]) if args.flaky else set()
        meta = {
            "label": args.label,
            "revision": args.revision,
            "measured": args.measured,
            "spark": "4.1.2",
        }
        board = build_scoreboard(args.replay_dir, load_exclusions(), flaky, meta)
        _write_outputs(board, args.output, args.markdown, render_scoreboard(board))
    elif args.command == "compare":
        report = compare_scoreboards(read_json(args.base), read_json(args.head))
        _write_outputs(report, args.output, args.markdown, render_comparison(report))
    elif args.command == "inspect":
        for row in witnesses(args.directory, args.match, args.kind, args.limit):
            sys.stdout.write(row + "\n")
    elif args.command == "ingest":
        count = ingest(args.lane, args.cases, args.spark, args.replace)
        sys.stdout.write(f"stored {count} cells in lane {args.lane}\n")
    else:
        counts = check_corpus()
        sys.stdout.write(json.dumps({"lanes": len(counts), "cells": sum(counts.values())}) + "\n")


def main(argv: list[str] | None = None) -> int:
    """Run one scoreboard command and return its exit status."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = _parser().parse_args(argv)
    try:
        _dispatch(args)
    except MissingSettingError as error:
        sys.stderr.write(f"{error}\n")
        return 2
    return 0
