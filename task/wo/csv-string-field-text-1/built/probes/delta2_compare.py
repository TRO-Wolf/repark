import json
import sys
from pathlib import Path
from typing import Any

Record = dict[str, Any]


def load(path: str) -> dict[tuple[str, str], Record]:
    found = Path(path)
    if not found.exists():
        return {}
    records = map(json.loads, found.read_text().splitlines())
    return {(record["ansi"], record["tag"]): record for record in records}


def strip(record: Record) -> Record:
    return {key: value for key, value in record.items() if key not in ("ansi", "tag", "text")}


def verdict(got: Record | None, want: Record) -> str:
    if got is None:
        return "MISSING"
    if "error" in want and "error" in got:
        return "BOTHERR"
    if "error" in got:
        return "RAISE"
    if "error" in want:
        return "WRONG"
    return "EQUAL" if strip(got) == strip(want) else "WRONG"


def short(record: Record) -> str:
    if "error" in record:
        return "ERR " + record["text"][:70]
    return ", ".join(record["names"]) + " : " + "; ".join(record["rows"])[:60]


def main() -> None:
    spark = load(sys.argv[1])
    builds = [(name, load(path)) for name, path in (arg.split("=", 1) for arg in sys.argv[2:])]
    print("| cell | ANSI | Spark | " + " | ".join(name for name, _ in builds) + " |")
    print("|---|---|---|" + "---|" * len(builds))
    for (ansi, tag), want in spark.items():
        verdicts = [verdict(rows.get((ansi, tag)), want) for _, rows in builds]
        print(f"| `{tag}` | {ansi} | {short(want)} | " + " | ".join(verdicts) + " |")


main()
