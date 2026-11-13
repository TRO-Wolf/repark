from __future__ import annotations

import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path

PHASES = ("sql", "explain", "total")


def load(path: Path) -> dict[str, dict[str, float]]:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
    return {row["cid"]: row for row in rows[1:]}


def merged(paths: list[Path]) -> dict[str, dict[str, float]]:
    runs = [load(path) for path in paths]
    out = {}
    for cid, first in runs[0].items():
        row = {"passes": first["passes"] or 0}
        for phase in PHASES:
            values = [run[cid][phase] for run in runs if run[cid][phase] is not None]
            row[phase] = statistics.median(values) if values else 0.0
        out[cid] = row
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, nargs="+", required=True)
    parser.add_argument("--head", type=Path, nargs="+", required=True)
    arguments = parser.parse_args()
    base, head = merged(arguments.base), merged(arguments.head)
    groups: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for cid, before in base.items():
        after = head[cid]
        for group in (cid.split("-")[0], "all"):
            for phase in PHASES:
                groups[group][phase].append(1000 * (after[phase] - before[phase]))
            groups[group]["passes"].append(after["passes"] - before["passes"])
    print("| group | cells | sql Δ ms | explain Δ ms | sql+collect Δ ms | extra passes |")
    print("|---|---|---|---|---|---|")
    for group in sorted(groups, key=lambda name: (name == "all", name)):
        values = groups[group]
        sums = [f"{sum(values[phase]):.1f}" for phase in PHASES]
        print(f"| {group} | {len(values['sql'])} | {' | '.join(sums)} | {sum(values['passes'])} |")


if __name__ == "__main__":
    main()
