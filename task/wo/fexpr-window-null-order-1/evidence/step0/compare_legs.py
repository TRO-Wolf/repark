import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any


def labels(path: str) -> dict[tuple[str, str], dict[str, Any]]:
    out = {}
    with Path(path).open(encoding="utf-8") as source:
        for line in source:
            item = json.loads(line)
            out[(item["cell"], item["door"])] = item
    return out


def main(main_labels: str, head_labels: str, target: str) -> None:
    before = labels(main_labels)
    after = labels(head_labels)
    assert before.keys() == after.keys()
    moves = Counter()
    lines = ["| Cell | Door | Main | Head |", "|---|---|---|---|"]
    for key in sorted(before):
        old, new = before[key], after[key]
        if old["repark"] == new["repark"]:
            moves[(key[1], "unchanged")] += 1
            continue
        moves[(key[1], f"{old['label']} -> {new['label']}")] += 1
        lines.append(f"| `{key[0]}` | {key[1]} | {old['label']} | {new['label']} |")
    summary = ["| Door | Move | Cells |", "|---|---|---|"]
    summary += [f"| {door} | {move} | {count} |" for (door, move), count in sorted(moves.items())]
    text = "# Main against head\n\n" + "\n".join(summary) + "\n\n## Every moved cell\n\n"
    Path(target).write_text(text + "\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main(*sys.argv[1:4])
