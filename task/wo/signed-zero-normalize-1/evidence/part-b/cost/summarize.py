from __future__ import annotations

import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

BUILDS = ("04d84ab9", "04d84ab9-aa", "276dc503", "head")


def seconds_of(record: dict[str, Any]) -> dict[str, float]:
    if "statements" in record:
        out = {key: value["seconds"] for key, value in record["statements"].items()}
        out["all"] = sum(out.values())
        return out
    return {"all": record["seconds"]}


def ratio(top: float | None, bottom: float | None) -> str:
    return f"{top / bottom:.4f}" if top is not None and bottom else "-"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--samples", type=Path)
    arguments = parser.parse_args()
    runs: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    for line in arguments.raw.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        for part, value in seconds_of(record).items():
            runs[(record["case"], part, record["label"])].append(value)
    keys = sorted({(case, part) for case, part, _ in runs})
    print(
        "| case | part | runs | 04d84ab9 | 04d84ab9-aa | 276dc503 | head | aa/04d84ab9 | "
        "276dc503/04d84ab9 | head/04d84ab9 | head/276dc503 |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for case, part in keys:
        medians: dict[str, float | None] = {}
        for build in BUILDS:
            values = runs.get((case, part, build), [])
            medians[build] = statistics.median(values) if values else None
        count = len(runs.get((case, part, "head"), []))
        cells = " | ".join(
            f"{medians[build]:.3f}" if medians[build] is not None else "-" for build in BUILDS
        )
        base, twin, diag, head = (medians[build] for build in BUILDS)
        print(
            f"| {case} | {part} | {count} | {cells} | {ratio(twin, base)} | {ratio(diag, base)} | "
            f"{ratio(head, base)} | {ratio(head, diag)} |"
        )
    if arguments.samples is None:
        return
    print()
    print(
        "| run | case | build | rc | wall (s) | load1 before | load1 at sample "
        "| waited 60 s | above 4 |"
    )
    print("|---|---|---|---|---|---|---|---|---|")
    for line in arguments.samples.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        sample = json.loads(line)
        high = "yes" if sample["load_at_sample"] > 4 else ""
        print(
            f"| {sample['run']} | {sample['case']} | {sample['build']} | {sample['rc']} | "
            f"{sample['wall']} | {sample['load_before']} | {sample['load_at_sample']} | "
            f"{'yes' if sample['waited'] else ''} | {high} |"
        )


if __name__ == "__main__":
    main()
