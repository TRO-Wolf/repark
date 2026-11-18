from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path


def show(cell: dict | None) -> str:
    if cell is None:
        return "-"
    if "error" in cell:
        return "RAISE " + (cell["error"] or cell.get("text", "")[:60])
    return ",".join(cell["rows"])


def same(want: dict, got: dict) -> bool:
    if "error" in want or "error" in got:
        return "error" in want and "error" in got and want["error"] == got["error"]
    return want.get("rows") == got.get("rows")


def verdict(want: dict, got: dict) -> str:
    if same(want, got):
        return "EQUAL"
    if "error" in got and "error" not in want:
        return "RAISE"
    return "WRONG"


def main() -> None:
    runs = json.loads(Path(sys.argv[1]).read_text())
    builds = [name for name in runs if name != "spark"]
    tally: Counter = Counter()
    for mode in ("true", "false"):
        for key in sorted(runs["spark"][mode]):
            want = runs["spark"][mode][key]
            found = {b: verdict(want, runs[b][mode].get(key, {"error": "MISSING"})) for b in builds}
            for build, seen in found.items():
                tally[(build, seen)] += 1
                if build != "main":
                    tally[(build, found["main"] + "->" + seen)] += 1
            answers = " | ".join(b + ": " + show(runs[b][mode].get(key)) for b in builds)
            print(mode, key, "| spark:", show(want), "|", answers)
    for item, count in sorted(tally.items()):
        print("count", item, count)


if __name__ == "__main__":
    main()
