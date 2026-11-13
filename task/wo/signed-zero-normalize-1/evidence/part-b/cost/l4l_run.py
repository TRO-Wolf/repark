from __future__ import annotations

import argparse
import contextlib
import io
import json
import statistics
import sys
from pathlib import Path
from typing import Any

PAIRS = (
    ("04d84ab9-aa", "04d84ab9"),
    ("head", "04d84ab9"),
    ("276dc503", "04d84ab9"),
    ("head", "276dc503"),
)
PARTS = ("p6", "rest")


def load(root: Path, build: str, run: str) -> tuple[dict[str, Any], dict[str, float]]:
    out: dict[str, Any] = {}
    work: dict[str, float] = {}
    for part in PARTS:
        folder = root / f"{build}-{run}-{part}"
        out.update(json.loads((folder / f"{build}.json").read_text(encoding="utf-8")))
        work.update(json.loads((folder / f"{build}.work.json").read_text(encoding="utf-8")))
    return out, work


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--s4", type=Path, required=True)
    parser.add_argument("--runs", required=True)
    arguments = parser.parse_args()
    sys.path.insert(0, str(arguments.s4))
    with contextlib.redirect_stdout(io.StringIO()):
        from like_for_like_s4 import outcome_class
    spark = json.loads((arguments.s4 / "spark.json").read_text(encoding="utf-8"))
    nondet = set(json.loads((arguments.s4 / "nondet.json").read_text(encoding="utf-8")))
    runs = arguments.runs.split(",")
    print("| pair | run | like cells | work A (s) | work B (s) | A/B |")
    print("|---|---|---|---|---|---|")
    medians = {}
    for a, b in PAIRS:
        ratios = []
        for run in runs:
            out_a, work_a = load(arguments.root, a, run)
            out_b, work_b = load(arguments.root, b, run)
            like = [
                key
                for key in out_b
                if key in out_a
                and key not in nondet
                and key in spark
                and key in work_a
                and key in work_b
                and outcome_class(out_b[key], spark[key]) == outcome_class(out_a[key], spark[key])
            ]
            total_a = sum(work_a[key] for key in like)
            total_b = sum(work_b[key] for key in like)
            ratios.append(total_a / total_b)
            print(
                f"| {a}/{b} | {run} | {len(like)} | {total_a:.1f} | {total_b:.1f} | "
                f"{total_a / total_b:.4f} |"
            )
        medians[f"{a}/{b}"] = (statistics.median(ratios), max(ratios) - min(ratios))
    print()
    print("| pair | median A/B | spread | runs |")
    print("|---|---|---|---|")
    for pair, (median, spread) in medians.items():
        print(f"| {pair} | {median:.4f} | {spread:.4f} | {len(runs)} |")


if __name__ == "__main__":
    main()
