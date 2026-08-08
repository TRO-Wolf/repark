#!/usr/bin/env python3
"""CLI entry for R-WRITE-BENCH (local-fs CTAS + append + MERGE + OVERWRITE).

Modes
-----
* ``ctas`` (default) - original SF CTAS+append K x file-size matrix
* ``merge`` - 1M/10M x narrow/wide x K MERGE matrix (r22 extension)
* ``overwrite`` - INSERT OVERWRITE peak RSS 1M/10M x narrow/wide (OTH-004)
* ``extension`` - merge + overwrite only (prior CTAS NO_K stands; not re-run)
* ``all`` - ctas + merge + overwrite

Usage::

    # Prefer SF1 CTAS matrix (prior unit):
    python …/run_write_bench.py --mode ctas --sf 1 --assert-release \\
        --report task/write-bench-report-….md

    # r22 extension (MERGE + OVERWRITE RSS):
    python …/run_write_bench.py --mode extension --assert-release \\
        --report task/write-bench-report-….md --out /tmp/write-bench-ext.json

**Prerequisite:** ``maturin develop --release`` then ``--assert-release`` or
``REPARK_WRITE_BENCH_RELEASE=1`` (debug-wheel trap). Never AWS.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Allow `python …/run_write_bench.py` (script path) as well as package-relative import.
if __name__ == "__main__" and (__package__ is None or __package__ == ""):
    _HERE = Path(__file__).resolve().parent
    sys.path.insert(0, str(_HERE.parent))
    __package__ = "write"  # package bootstrap for script execution
    if "write" not in sys.modules:
        import types

        _pkg = types.ModuleType("write")
        _pkg.__path__ = [str(_HERE)]  # type: ignore[attr-defined]
        sys.modules["write"] = _pkg

from .merge_runner import (
    DEFAULT_MERGE_K,
    DEFAULT_WIDTHS,
    render_merge_markdown,
    run_merge_matrix,
    write_merge_json,
)
from .merge_runner import (
    DEFAULT_ROW_COUNTS as MERGE_DEFAULT_ROWS,
)
from .overwrite_runner import (
    DEFAULT_ROW_COUNTS as OW_DEFAULT_ROWS,
)
from .overwrite_runner import (
    render_overwrite_markdown,
    run_overwrite_matrix,
    write_overwrite_json,
)
from .runner import (
    DEFAULT_FILE_SIZE_BYTES,
    DEFAULT_K_VALUES,
    parse_file_size,
    render_markdown_report,
    run_matrix,
    write_json,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "R-WRITE-BENCH: local-fs Iceberg CTAS+append / MERGE / INSERT OVERWRITE matrices"
        )
    )
    parser.add_argument(
        "--mode",
        choices=("ctas", "merge", "overwrite", "extension", "all"),
        default="ctas",
        help="bench mode (default ctas = prior SF matrix; extension = merge+overwrite)",
    )
    # --- CTAS/append (prior) ---
    parser.add_argument(
        "--sf",
        type=float,
        default=1.0,
        help="TPC-H scale factor for CTAS source parquet (default 1; SF1 preferred)",
    )
    parser.add_argument(
        "--k",
        type=str,
        default=None,
        help=(
            "comma-separated repark.write.max-concurrent-files "
            f"(ctas default {','.join(str(v) for v in DEFAULT_K_VALUES)}; "
            f"merge default {','.join(str(v) for v in DEFAULT_MERGE_K)})"
        ),
    )
    parser.add_argument(
        "--file-sizes",
        type=str,
        default=",".join(str(value) for value in DEFAULT_FILE_SIZE_BYTES),
        help="comma-separated write.target-file-size-bytes (CTAS mode; int or 64MiB form)",
    )
    parser.add_argument(
        "--table",
        type=str,
        default="lineitem",
        help="TPC-H source table for CTAS mode (default lineitem)",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=None,
        help="TPC-H parquet cache root (default ~/.cache/repark-tpch)",
    )
    parser.add_argument(
        "--warehouse",
        type=Path,
        default=None,
        help="local Iceberg warehouse root for cell dirs",
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=1,
        help="median-of-N per CTAS cell (default 1)",
    )
    # --- extension axes ---
    parser.add_argument(
        "--rows",
        type=str,
        default=",".join(str(value) for value in MERGE_DEFAULT_ROWS),
        help="comma-separated target/source row counts for merge/overwrite (default 1e6,1e7)",
    )
    parser.add_argument(
        "--width",
        type=str,
        default=",".join(DEFAULT_WIDTHS),
        help="comma-separated widths: narrow,wide (default both)",
    )
    parser.add_argument(
        "--source-fraction",
        type=float,
        default=0.10,
        help="MERGE source size as fraction of target (default 0.10)",
    )
    parser.add_argument(
        "--no-cow",
        action="store_true",
        help="MERGE mode: skip COW leg (MoR only; faster matrix)",
    )
    parser.add_argument("--out", type=Path, default=None, help="write JSON scoreboard")
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="write Markdown report (overwrite)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="DEBUG logging",
    )
    parser.add_argument(
        "--assert-release",
        action="store_true",
        help=(
            "assert timed run used maturin develop --release "
            "(or set REPARK_WRITE_BENCH_RELEASE=1); without this, report marks release UNVERIFIED"
        ),
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    mode = args.mode
    sections: list[str] = []
    failed = False
    log = logging.getLogger(__name__)

    try:
        row_counts = [int(part.strip()) for part in args.rows.split(",") if part.strip()]
        widths = [part.strip() for part in args.width.split(",") if part.strip()]
        for width in widths:
            if width not in ("narrow", "wide"):
                msg = f"width must be narrow|wide; got {width!r}"
                raise ValueError(msg)
    except ValueError as exc:
        print(f"usage error: {exc}", file=sys.stderr)
        return 2

    # ----- CTAS (prior) -----
    if mode in ("ctas", "all"):
        try:
            k_default = DEFAULT_K_VALUES
            k_text = args.k if args.k is not None else ",".join(str(v) for v in k_default)
            k_values = [int(part.strip()) for part in k_text.split(",") if part.strip()]
            file_sizes = [
                parse_file_size(part.strip()) for part in args.file_sizes.split(",") if part.strip()
            ]
        except ValueError as exc:
            print(f"usage error: {exc}", file=sys.stderr)
            return 2
        if not k_values or not file_sizes:
            print("usage error: --k and --file-sizes must be non-empty", file=sys.stderr)
            return 2
        try:
            board = run_matrix(
                scale_factor=args.sf,
                k_values=k_values,
                file_size_bytes=file_sizes,
                data_root=args.data_root,
                warehouse_root=args.warehouse,
                source_table=args.table,
                repeats=args.repeats,
                assert_release=args.assert_release,
            )
        except ValueError as exc:
            print(f"usage error: {exc}", file=sys.stderr)
            return 2
        sections.append(render_markdown_report(board))
        if args.out is not None:
            write_json(board, args.out.with_suffix(".ctas.json") if mode == "all" else args.out)
            log.info("wrote CTAS JSON")
        if any(cell.error for cell in board.cells):
            failed = True

    # ----- MERGE extension -----
    if mode in ("merge", "extension", "all"):
        try:
            k_default = DEFAULT_MERGE_K
            k_text = args.k if args.k is not None else ",".join(str(v) for v in k_default)
            k_values = [int(part.strip()) for part in k_text.split(",") if part.strip()]
            for k_value in k_values:
                if k_value < 1:
                    msg = f"concurrency K must be >= 1; got {k_value}"
                    raise ValueError(msg)
        except ValueError as exc:
            print(f"usage error: {exc}", file=sys.stderr)
            return 2
        if not row_counts:
            print("usage error: --rows must be non-empty for merge", file=sys.stderr)
            return 2
        merge_wh = None
        if args.warehouse is not None:
            merge_wh = args.warehouse / "merge"
        try:
            merge_board = run_merge_matrix(
                row_counts=row_counts,
                widths=widths,  # type: ignore[arg-type]
                k_values=k_values,
                source_fraction=args.source_fraction,
                warehouse_root=merge_wh,
                assert_release=args.assert_release,
                run_cow=not args.no_cow,
            )
        except ValueError as exc:
            print(f"usage error: {exc}", file=sys.stderr)
            return 2
        sections.append(render_merge_markdown(merge_board))
        if args.out is not None:
            out_path = (
                args.out.with_suffix(".merge.json") if mode in ("all", "extension") else args.out
            )
            if mode == "extension":
                out_path = args.out.with_name(args.out.stem + ".merge.json")
            write_merge_json(merge_board, out_path)
            log.info("wrote MERGE JSON %s", out_path)
        if any(cell.error for cell in merge_board.cells):
            failed = True

    # ----- OVERWRITE RSS extension -----
    if mode in ("overwrite", "extension", "all"):
        if not row_counts:
            print("usage error: --rows must be non-empty for overwrite", file=sys.stderr)
            return 2
        # Prefer OW defaults when user left MERGE defaults - same 1M/10M.
        ow_rows = row_counts if row_counts else list(OW_DEFAULT_ROWS)
        ow_wh = None
        if args.warehouse is not None:
            ow_wh = args.warehouse / "overwrite"
        try:
            ow_board = run_overwrite_matrix(
                row_counts=ow_rows,
                widths=widths,  # type: ignore[arg-type]
                warehouse_root=ow_wh,
                assert_release=args.assert_release,
            )
        except ValueError as exc:
            print(f"usage error: {exc}", file=sys.stderr)
            return 2
        sections.append(render_overwrite_markdown(ow_board))
        if args.out is not None:
            if mode == "overwrite":
                out_path = args.out
            else:
                out_path = args.out.with_name(args.out.stem + ".overwrite.json")
            write_overwrite_json(ow_board, out_path)
            log.info("wrote OVERWRITE JSON %s", out_path)
        if any(cell.error for cell in ow_board.cells):
            failed = True

    report_md = "\n\n---\n\n".join(sections) if sections else "# empty report\n"
    # Extension preamble when both MERGE + OW present without CTAS re-run.
    if mode == "extension":
        preamble = (
            "# R-WRITE-BENCH r22 extension (MERGE + INSERT OVERWRITE RSS)\n\n"
            "**Prior CTAS local-fs verdict stands:** `NO_K_BENEFIT_ON_LOCAL_FS` "
            "(see `task/write-bench-report-2026-08-06.md` / ledger-archive). "
            "This run does **not** re-execute the SF1 CTASxK matrix unless "
            "`--mode all` is requested. Loud contradiction would require re-running "
            "CTAS and seeing >=15% K speedup - not claimed here.\n\n"
            "Measurement only. No product / fork / knob-default edits. "
            "Authored-By: Grok.\n"
        )
        report_md = preamble + "\n---\n\n" + report_md

    print(report_md)
    if args.report is not None:
        report_path = args.report.expanduser()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(report_md, encoding="utf-8")
        log.info("wrote report %s", report_path)

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
