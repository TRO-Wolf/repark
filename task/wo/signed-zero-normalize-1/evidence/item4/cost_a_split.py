import argparse
import collections
import json
import statistics
from pathlib import Path

ORDER = ("base", "oldhead", "head")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reads", type=Path, required=True)
    parser.add_argument("--moves", type=Path, required=True)
    parser.add_argument("--cells", type=Path, required=True)
    arguments = parser.parse_args()
    records = [
        json.loads(line)
        for line in arguments.reads.read_text(encoding="utf-8").splitlines()
        if line
    ]
    cells = [r for r in records if r.get("record") == "cell" and r["route"] == "spark-sql"]
    moved = json.loads(arguments.moves.read_text(encoding="utf-8"))
    moves = {m["key"]: (m["old"]["label"], m["new"]["label"]) for m in moved}
    runs = [
        json.loads(line)
        for line in arguments.cells.read_text(encoding="utf-8").splitlines()
        if line
    ]
    groups: dict[str, set[int]] = collections.defaultdict(set)
    for index, cell in enumerate(cells):
        move = moves.get(f"{cell['cid']}|{cell['route']}|{cell['layout']}")
        groups["all"].add(index)
        groups["unchanged-label" if move is None else f"{move[0]}->{move[1]}"].add(index)
    labels = [label for label in ORDER if any(r["label"] == label for r in runs)]
    for group, members in sorted(groups.items()):
        medians = {}
        for label in labels:
            totals = [
                sum(c[2] for c in r["cells"] if c[0] in members)
                for r in runs
                if r["label"] == label
            ]
            medians[label] = statistics.median(totals)
        ratios = " ".join(
            f"{label}={medians[label] * 1000:.1f}ms ({medians[label] / medians['base']:.4f})"
            for label in labels
        )
        print(f"{group} n={len(members)} {ratios}")


if __name__ == "__main__":
    main()
