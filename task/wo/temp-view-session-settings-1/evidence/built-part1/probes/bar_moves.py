from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

ORDER = ["EQUAL", "BOTH_RAISE", "BOTH_RAISE_OTHER", "VALUE", "TYPE", "REPARK_ANSWERS"]
ORDER += ["REPARK_RAISES", "CREATE_DIFF"]
OK = {"EQUAL", "BOTH_RAISE"}


def move(row: dict) -> str:
    same = row["main"] == row["head"] and row["main_answer"] == row["head_answer"]
    before, after = row["main"] in OK, row["head"] in OK
    if before and after:
        return "stay equal" if same else "EQUAL->EQUAL changed"
    if before:
        return "EQUAL->RAISE" if row["head"] == "REPARK_RAISES" else "EQUAL->WRONG"
    if after:
        return "WRONG->EQUAL"
    return "stay non-equal" if same else "WRONG->WRONG changed"


def main() -> None:
    lines = Path(sys.argv[1]).read_text().splitlines()
    rows = [json.loads(line) for line in lines]
    for build in ("main", "head"):
        seen = Counter(row[build] for row in rows)
        print(build, len(rows), " ".join(f"{k}={seen[k]}" for k in ORDER if seen[k]))
    moves = Counter((row["scope"], move(row)) for row in rows)
    for item in sorted(moves):
        print(item, moves[item])
    matching = [
        row["cell"] for row in rows if row.get("matching") and not move(row).startswith("stay")
    ]
    print("matching-settings moves", len(matching))


if __name__ == "__main__":
    main()
