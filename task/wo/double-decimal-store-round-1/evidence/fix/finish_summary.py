from __future__ import annotations

import argparse
import contextlib
import io
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

BUILDS = ("base", "head", "base-aa")
PAIRS = (("head", "base"), ("base-aa", "base"))
PARTS = ("p6", "rest")


def ratio(top: float | None, bottom: float | None) -> str:
    return f"{top / bottom:.4f}" if top is not None and bottom else "-"


def query_table(raw_files: list[Path]) -> None:
    runs: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    answers: dict[tuple[str, str], set[str]] = defaultdict(set)
    for raw in raw_files:
        for line in raw.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            case = record.get("case", "cast")
            for query, seconds in record["times"].items():
                runs[(case, query, record["label"])].append(seconds)
            for query, answer in record.get("answers", {}).items():
                answers[(case, query)].add(json.dumps(answer))
    keys = sorted({(case, query) for case, query, _ in runs})
    print(
        "| case | query | runs | base | head | base-aa | head/base | base-aa/base | answers agree |"
    )
    print("|---|---|---|---|---|---|---|---|---|")
    for case, query in keys:
        medians = {
            build: statistics.median(runs[(case, query, build)])
            if runs.get((case, query, build))
            else None
            for build in BUILDS
        }
        count = len(runs.get((case, query, "head"), []))
        cells = " | ".join(
            f"{medians[build]:.4f}" if medians[build] is not None else "-" for build in BUILDS
        )
        agree = (
            "-"
            if (case, query) not in answers
            else ("yes" if len(answers[(case, query)]) == 1 else "NO")
        )
        print(
            f"| {case} | {query} | {count} | {cells} | {ratio(medians['head'], medians['base'])} | "
            f"{ratio(medians['base-aa'], medians['base'])} | {agree} |"
        )


def load_c012(root: Path, build: str, run: str) -> tuple[dict[str, Any], dict[str, float]]:
    out: dict[str, Any] = {}
    work: dict[str, float] = {}
    for part in PARTS:
        folder = root / f"{build}-{run}-{part}"
        out.update(json.loads((folder / f"{build}.json").read_text(encoding="utf-8")))
        work.update(json.loads((folder / f"{build}.work.json").read_text(encoding="utf-8")))
    return out, work


def c012_table(root: Path, s4: Path, runs: list[str]) -> None:
    sys.path.insert(0, str(s4))
    with contextlib.redirect_stdout(io.StringIO()):
        from like_for_like_s4 import outcome_class
    spark = json.loads((s4 / "spark.json").read_text(encoding="utf-8"))
    nondet = set(json.loads((s4 / "nondet.json").read_text(encoding="utf-8")))
    print("| pair | run | like cells | work A (s) | work B (s) | A/B |")
    print("|---|---|---|---|---|---|")
    medians = {}
    for a, b in PAIRS:
        ratios = []
        for run in runs:
            out_a, work_a = load_c012(root, a, run)
            out_b, work_b = load_c012(root, b, run)
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


def sample_table(samples: Path) -> None:
    print("| run | case | build | rc | wall (s) | load1 before | load1 at sample | waited 60 s |")
    print("|---|---|---|---|---|---|---|---|")
    for line in samples.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        sample = json.loads(line)
        print(
            f"| {sample['run']} | {sample['case']} | {sample['build']} | {sample['rc']} | "
            f"{sample['wall']} | {sample['load_before']} | {sample['load_at_sample']} | "
            f"{'yes' if sample['waited'] else ''} |"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", type=Path, nargs="+", required=True)
    parser.add_argument("--c012-root", type=Path)
    parser.add_argument("--s4", type=Path)
    parser.add_argument("--runs", default="1,2,3,4,5")
    parser.add_argument("--samples", type=Path)
    arguments = parser.parse_args()
    query_table(arguments.raw)
    if arguments.c012_root is not None and arguments.s4 is not None:
        print()
        c012_table(arguments.c012_root, arguments.s4, arguments.runs.split(","))
    if arguments.samples is not None:
        print()
        sample_table(arguments.samples)


if __name__ == "__main__":
    main()
