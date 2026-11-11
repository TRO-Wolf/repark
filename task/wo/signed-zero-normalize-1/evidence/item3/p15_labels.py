import argparse
import collections
import json
from pathlib import Path
from typing import Any

TRUTH = [4, 6]


def cells(path: Path) -> list[dict[str, Any]]:
    records = (json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line)
    return [record for record in records if record.get("record") == "cell"]


def label(record: dict[str, Any]) -> str:
    if record["outcome"] != "ANSWERED":
        return "REFUSED"
    reached = record.get("ids") == TRUTH and not record.get("damaged")
    if record["seat"] != "delete":
        reached = reached and not record.get("lost")
    if reached:
        return "EQUAL"
    return "WRONG-READ" if record["seat"] == "select" else "WRONG-WRITE"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--head", type=Path, required=True)
    arguments = parser.parse_args()
    base, head = cells(arguments.base), cells(arguments.head)
    if len(base) != len(head):
        raise SystemExit(f"cell counts differ: {len(base)} against {len(head)}")
    moves: collections.Counter[tuple[str, str]] = collections.Counter()
    for old, new in zip(base, head, strict=True):
        if old["key"] != new["key"]:
            raise SystemExit(f"cell order differs: {old['key']} against {new['key']}")
        moves[(label(old), label(new))] += 1
        print(new["key"], label(old), "->", label(new), new.get("ids"))
    for (old, new), count in sorted(moves.items()):
        print(f"{old} -> {new}: {count}")


if __name__ == "__main__":
    main()
