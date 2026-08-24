#!/usr/bin/env python3
"""MW-7 scale-measurement CLI (LOCAL filesystem only; never AWS).

Calibrate first, then run at scale — the charter's feasibility protocol::

    python python/repark-parity/bench/mw7/run_mw7.py \\
        --rows 1000000 --merges 10 --scratch /path/to/scratch --out calibration.json
    python python/repark-parity/bench/mw7/run_mw7.py \\
        --rows 10000000 --merges 100 --scratch /path/to/scratch --out full.json

The scratch tree holds the warehouses and the Parquet seed. It is never committed and the
operator deletes it when the run is read (PROJECT.md: generators are checked in, data is
not). The JSON is the artifact the ledger tables are transcribed from.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from mw7.measure import LegResult, RunResult, run_scale_measurement


def format_leg_table(leg: LegResult) -> str:
    """The per-checkpoint table for one leg, as fixed-width text."""
    header = (
        f"{'merges':>7} {'data_f':>7} {'del_f':>7} {'del_rec':>10} {'manif':>6} "
        f"{'mlist_B':>8} {'rows':>10} {'count_p50':>10} {'part_p50':>10} {'point_p50':>10} "
        f"{'point_p99':>10}"
    )
    lines = [f"--- leg {leg.mode} ({leg.table}) ---", header]
    for point in leg.checkpoints:
        timings = {scan.label: scan for scan in point.scans}
        lines.append(
            f"{point.merges_done:>7} {point.census.data_files:>7} "
            f"{point.census.delete_files:>7} {point.census.delete_records:>10} "
            f"{point.census.manifests:>6} {point.census.manifest_list_bytes:>8} "
            f"{point.row_count:>10} {timings['count_star'].p50_ms:>10.1f} "
            f"{timings['predicate_partition'].p50_ms:>10.1f} "
            f"{timings['predicate_point'].p50_ms:>10.1f} "
            f"{timings['predicate_point'].p99_ms:>10.1f}"
        )
    lines.append("--- maintenance ---")
    lines.append(
        f"{'procedure':>30} {'wall_s':>9} {'data_f':>7} {'del_f':>7} {'manif':>6} {'mlist_B':>8}"
    )
    for step in leg.maintenance:
        lines.append(
            f"{step.procedure:>30} {step.wall_seconds:>9.2f} {step.census_after.data_files:>7} "
            f"{step.census_after.delete_files:>7} {step.census_after.manifests:>6} "
            f"{step.census_after.manifest_list_bytes:>8}"
        )
    after = {scan.label: scan for scan in leg.after_maintenance.scans}
    lines.append(
        f"after maintenance: rows={leg.after_maintenance.row_count} "
        f"count_p50={after['count_star'].p50_ms:.1f}ms "
        f"part_p50={after['predicate_partition'].p50_ms:.1f}ms "
        f"point_p50={after['predicate_point'].p50_ms:.1f}ms "
        f"point_p99={after['predicate_point'].p99_ms:.1f}ms"
    )
    lines.append(
        f"warehouse bytes {leg.warehouse_bytes_before_maintenance} -> "
        f"{leg.warehouse_bytes_after_maintenance}; ctas {leg.ctas_seconds:.1f}s; "
        f"merges total {sum(leg.merge_seconds):.1f}s; leg wall {leg.wall_seconds:.1f}s"
    )
    return "\n".join(lines)


def format_projection(result: RunResult, target_rows: int, target_merges: int) -> str:
    """Project this run's wall clock onto a bigger run, the charter's feasibility gate.

    MERGE cost scales with the rows each MERGE touches, which scales with the table, so the
    merge phase is projected quadratically in the row ratio (rows x rows-per-merge) and
    linearly in the merge count. CTAS and the scans are projected linearly in rows. The
    projection is deliberately crude and stated as such; it exists to answer "hours or
    days", not to predict a number.
    """
    row_ratio = target_rows / result.rows
    merge_ratio = target_merges / result.merges
    lines = [f"--- projection to rows={target_rows} merges={target_merges} ---"]
    total = 0.0
    for leg in result.legs:
        merge_total = sum(leg.merge_seconds)
        other = leg.wall_seconds - merge_total
        projected = merge_total * row_ratio * row_ratio * merge_ratio + other * row_ratio
        total += projected
        lines.append(
            f"{leg.mode}: merges {merge_total:.1f}s x {row_ratio:.0f}^2 x {merge_ratio:.0f} "
            f"+ other {other:.1f}s x {row_ratio:.0f} = {projected / 3600:.2f} h"
        )
    lines.append(f"TOTAL projected {total / 3600:.2f} h")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    """The CLI surface."""
    parser = argparse.ArgumentParser(description="MW-7 Iceberg scale measurement (measure only)")
    parser.add_argument("--rows", type=int, default=1_000_000, help="seed rows per leg")
    parser.add_argument("--merges", type=int, default=10, help="MERGEs per leg")
    parser.add_argument("--partitions", type=int, default=16, help="identity partitions on `part`")
    parser.add_argument(
        "--touch-fraction", type=float, default=0.02, help="fraction of rows each MERGE touches"
    )
    parser.add_argument("--checkpoint-every", type=int, default=10, help="measure every N merges")
    parser.add_argument("--reps", type=int, default=7, help="repetitions per timed scan (min 5)")
    parser.add_argument(
        "--target-file-size-bytes",
        type=int,
        default=8 * 1024 * 1024,
        help="write.target-file-size-bytes on each CTAS",
    )
    parser.add_argument("--modes", default="mor,cow", help="comma-separated legs to run (mor, cow)")
    parser.add_argument("--scratch", required=True, help="scratch root for warehouses + Parquet")
    parser.add_argument("--out", default="", help="write the full result as JSON to this path")
    parser.add_argument("--host-note", default="", help="free text recorded with the result")
    parser.add_argument(
        "--project-to",
        default="",
        help="rows:merges to project this run's wall clock onto (feasibility gate)",
    )
    return parser


def main() -> int:
    """Run the measurement and print the tables. Returns a process exit code."""
    args = build_parser().parse_args()
    result = run_scale_measurement(
        root=Path(args.scratch),
        rows=args.rows,
        merges=args.merges,
        partitions=args.partitions,
        touch_fraction=args.touch_fraction,
        checkpoint_every=args.checkpoint_every,
        reps=args.reps,
        target_file_size_bytes=args.target_file_size_bytes,
        modes=[mode.strip() for mode in args.modes.split(",") if mode.strip()],
        host_note=args.host_note,
    )
    for leg in result.legs:
        print(format_leg_table(leg))
    print(
        f"peak RSS {result.peak_rss_bytes / (1024 * 1024):.0f} MiB; "
        f"run wall {result.wall_seconds / 60:.1f} min"
    )
    if args.project_to:
        target_rows, target_merges = (int(part) for part in args.project_to.split(":"))
        print(format_projection(result, target_rows, target_merges))
    if args.out:
        Path(args.out).write_text(result.model_dump_json(indent=2), encoding="utf-8")
        print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
