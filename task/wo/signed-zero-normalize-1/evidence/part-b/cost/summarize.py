from __future__ import annotations

import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

BUILDS = ("04d84ab9", "276dc503", "head")


def seconds_of(record: dict[str, Any]) -> dict[str, float]:
    if "statements" in record:
        out = {key: value["seconds"] for key, value in record["statements"].items()}
        out["all"] = sum(out.values())
        return out
    return {"all": record["seconds"]}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", type=Path, required=True)
    arguments = parser.parse_args()
    runs: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    for line in arguments.raw.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        for part, value in seconds_of(record).items():
            runs[(record["case"], part, record["label"])].append(value)
    keys = sorted({(case, part) for case, part, _ in runs})
    header = "| case | part | runs | 04d84ab9 | 276dc503 | head |"
    print(header + " 276dc503/04d84ab9 | head/04d84ab9 | head/276dc503 |")
    print("|---|---|---|---|---|---|---|---|---|")
    for case, part in keys:
        medians = {}
        for build in BUILDS:
            values = runs.get((case, part, build), [])
            medians[build] = statistics.median(values) if values else None
        count = len(runs.get((case, part, "head"), []))
        if None in medians.values():
            continue
        base, diag, head = (medians[build] for build in BUILDS)
        print(
            f"| {case} | {part} | {count} | {base:.3f} | {diag:.3f} | {head:.3f} | "
            f"{diag / base:.4f} | {head / base:.4f} | {head / diag:.4f} |"
        )


if __name__ == "__main__":
    main()
