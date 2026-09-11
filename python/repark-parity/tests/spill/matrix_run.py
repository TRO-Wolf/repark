"""Full-tier spill-matrix driver: every roster cell x N reps, resumable, folded to CSV."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import time
from pathlib import Path
from typing import Any, Final

from matrix_cells import (
    FULL_CELL_TIMEOUT_S,
    FULL_CELLS,
    FULL_LIMIT_BYTES,
    FULL_PARTITIONS,
    RosterRow,
    json_out_path,
    worker_argv,
)
from matrix_generators import input_arrow_bytes, payload_extra
from matrix_harness import KILLED, OUTCOME_VOCABULARY, CellRecord, run_cell

UNSTABLE: Final[str] = "UNSTABLE"

_CELL_NOTES: Final[dict[str, str]] = {
    "sort": "ExternalSorter spills; the reference spilling operator",
    "hash_aggregate": "GroupedHashAggregateStream spills (row_hash)",
    "hash_join": (
        "build side has no spill path; upstream epic "
        "https://github.com/apache/datafusion/issues/24768"
    ),
    "sort_merge_join": (
        "forced by datafusion.optimizer.prefer_hash_join=false; input sorts and the "
        "buffered side can spill"
    ),
    "window_sliding": (
        "BoundedWindowAggExec takes no pool reservation; upstream accounting epic "
        "https://github.com/apache/datafusion/issues/22758"
    ),
    "window_unbounded": (
        "WindowAggExec takes no pool reservation; only the SortExec beneath it can spill; "
        "https://github.com/apache/datafusion/issues/22758"
    ),
    "dynamic_flatten": (
        "UnnestExec takes no pool reservation; https://github.com/apache/datafusion/issues/22758"
    ),
    "nested_loop_join": (
        "upstream fallback re-executes the build child and panics; repark reports the pool "
        "refusal instead (H3-SPILL-NLJ-1 FIXED); "
        "https://github.com/apache/datafusion/issues/24661"
    ),
    "collect": (
        "facade boundary takes no pool reservation; MemoryError is H3-SPILL-COLLECT-1 "
        "FIXED; upstream accounting epic https://github.com/apache/datafusion/issues/22758"
    ),
}

CSV_FIELDS: Final[tuple[str, ...]] = (
    "operator",
    "multiple",
    "input_bytes",
    "outcome_1",
    "outcome_2",
    "outcome_3",
    "outcome",
    "spill_bytes",
    "seconds",
    "notes",
)


def load_done(results_path: Path) -> set[tuple[str, int, int]]:
    """Return the (operator, multiplier, rep) keys already recorded in the results file."""
    done: set[tuple[str, int, int]] = set()
    if not results_path.is_file():
        return done
    for line in results_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        done.add((record["operator"], record["multiplier"], record["rep"]))
    return done


def run_rep(rep: int, scratch_dir: Path, results_path: Path) -> None:
    """Run every roster cell once for `rep`, appending each record to the results file."""
    done = load_done(results_path)
    rep_dir = scratch_dir / f"rep{rep}"
    rep_dir.mkdir(parents=True, exist_ok=True)
    with results_path.open("a", encoding="utf-8") as handle:
        for spec in FULL_CELLS:
            if (spec.operator, spec.multiplier, rep) in done:
                continue
            json_out = json_out_path(rep_dir, spec)
            record = run_cell(
                worker_argv(spec, json_out, FULL_PARTITIONS, FULL_LIMIT_BYTES),
                json_out,
                rep_dir,
                FULL_CELL_TIMEOUT_S,
                operator=spec.operator,
                multiplier=spec.multiplier,
                refusal_names=spec.refusal_names,
            )
            handle.write(json.dumps({"rep": rep, **record.model_dump()}) + "\n")
            handle.flush()
            print(
                f"rep {rep} {spec.operator} {spec.multiplier}x -> {record.outcome} "
                f"({record.wall_ms / 1000.0:.1f}s)",
                flush=True,
            )


def load_rep_records(results_path: Path) -> dict[tuple[str, int], list[tuple[int, CellRecord]]]:
    """Group recorded runs by (operator, multiplier), keeping each record's rep."""
    grouped: dict[tuple[str, int], list[tuple[int, CellRecord]]] = {}
    if not results_path.is_file():
        return grouped
    for line in results_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        raw = json.loads(line)
        rep = raw.pop("rep")
        record = CellRecord(**raw)
        grouped.setdefault((record.operator, record.multiplier), []).append((rep, record))
    return grouped


def cell_note(spec: RosterRow, reps: list[tuple[int, CellRecord]]) -> str:
    """The CSV notes column: the roster note plus the measured detail this cell earned."""
    parts = [_CELL_NOTES.get(spec.operator, "")]
    for _, record in reps:
        if record.outcome == "refused" and record.error_type:
            detail = f"refused as {record.error_type}"
            if detail not in parts:
                parts.append(detail)
        if record.outcome == KILLED:
            tail = (record.message or "").strip().splitlines()
            fragment = tail[-1][:160] if tail else f"returncode {record.returncode}"
            parts.append(f"KILLED: {fragment}")
    return "; ".join(part for part in parts if part)


def fold_rows(results_path: Path, reps: int) -> tuple[list[dict[str, Any]], list[str]]:
    """Fold the recorded runs into one CSV row per cell; return rows and failure names."""
    grouped = load_rep_records(results_path)
    rows: list[dict[str, Any]] = []
    failures: list[str] = []
    for spec in FULL_CELLS:
        cell_reps = sorted(grouped.get((spec.operator, spec.multiplier), []))
        outcomes = [record.outcome for _, record in cell_reps]
        padded = outcomes + [""] * (reps - len(outcomes))
        outcome = outcomes[0] if len(outcomes) == reps and len(set(outcomes)) == 1 else UNSTABLE
        if outcome not in OUTCOME_VOCABULARY:
            failures.append(f"{spec.operator}-{spec.multiplier}x")
        spill_values = [r.spill_bytes for _, r in cell_reps if r.spill_bytes is not None]
        walls = [r.wall_ms / 1000.0 for _, r in cell_reps]
        rows.append(
            {
                "operator": spec.operator,
                "multiple": spec.multiplier,
                "input_bytes": input_arrow_bytes(payload_extra(spec.multiplier * FULL_LIMIT_BYTES)),
                "outcome_1": padded[0],
                "outcome_2": padded[1] if reps > 1 else "",
                "outcome_3": padded[2] if reps > 2 else "",
                "outcome": outcome,
                "spill_bytes": max(spill_values) if spill_values else "",
                "seconds": f"{statistics.median(walls):.1f}" if walls else "",
                "notes": cell_note(spec, cell_reps),
            }
        )
    return rows, failures


def write_csv(csv_path: Path, rows: list[dict[str, Any]]) -> None:
    """Write the folded matrix CSV."""
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(CSV_FIELDS))
        writer.writeheader()
        writer.writerows(rows)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the driver argv."""
    parser = argparse.ArgumentParser(description="neveroom-1 full spill matrix")
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--csv", type=Path, default=None)
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--fold-only", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the missing reps, then fold the results file into the CSV."""
    args = parse_args(argv)
    started = time.perf_counter()
    if not args.fold_only:
        for rep in range(1, args.reps + 1):
            run_rep(rep, args.scratch, args.results)
    rows, failures = fold_rows(args.results, args.reps)
    if args.csv is not None:
        write_csv(args.csv, rows)
    elapsed = (time.perf_counter() - started) / 60.0
    print(f"matrix: {len(rows)} cells, failures: {failures or 'none'} ({elapsed:.1f} min)")
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
