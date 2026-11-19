import json
import sys
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path

CODES = {"ERR ARITHMETIC_OVERFLOW": "AO", "ERR NUMERIC_VALUE_OUT_OF_RANGE": "NVOOR"}


def value(item: object) -> str:
    return "NULL" if item is None else str(item)


def short(text: str) -> str:
    got = json.loads(text)
    if isinstance(got, str):
        return " / ".join(CODES.get(p, p[:48]) for p in got.split(" | "))
    if isinstance(got, dict):
        return f"rows={got['rows']} nulls={got['nulls']}"
    if not got or not got[0]:
        return "no rows"
    if len(got) == 1 and len(got[0]) == 1:
        return value(got[0][0])
    if len(got) == 1 and len(got[0]) == 4:
        return "{}/{} {}..{}".format(*(value(v) for v in got[0]))
    seen = list(dict.fromkeys(f"{value(r[0])}:{value(r[-1])}" for r in got))
    if len(seen) == 1 and len(got) > 1:
        return f"{seen[0].split(':', 1)[1]} x{len(got)} rows"
    return " ".join(seen)


def cell(answers: dict[str, int]) -> str:
    ranked = sorted(answers.items(), key=lambda kv: -kv[1])
    if len(ranked) == 1:
        return short(ranked[0][0])
    return " / ".join(f"{short(k)} x{n}" for k, n in ranked)


def load(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


Column = Callable[[dict], str]


def table(
    rows: list[dict], column: Column, exact: dict | None, keys: list[str] | None = None
) -> None:
    grid: dict[tuple[str, str], dict[str, str]] = defaultdict(dict)
    columns: list[str] = []
    for row in rows:
        name = column(row)
        if name not in columns:
            columns.append(name)
        grid[(row["query"], row.get("ansi", ""))][name] = cell(row["answers"])
    head = ["query", "ANSI", *columns] + (["exact (seat A)"] if exact else [])
    print("| " + " | ".join(head) + " |")
    print("|" + "---|" * len(head))
    order = keys or sorted({q for q, _ in grid})
    for ansi in ("false", "true"):
        for query in order:
            if (query, ansi) not in grid:
                continue
            got = grid[(query, ansi)]
            line = [f"`{query}`", ansi, *(got.get(c, "") for c in columns)]
            if exact:
                truth = exact.get(ansi, {}).get(query)
                line.append(short(truth) if truth else "")
            print("| " + " | ".join(line) + " |")
    print()


def main() -> None:
    raw, which = Path(sys.argv[1]), sys.argv[2]
    exact = json.loads((raw / "exact.json").read_text())
    if which == "spark":
        rows = load(raw / "spark.jsonl")
        table(rows, lambda r: f"{r['master']} cg {r['codegen'][:2]}", exact)
    elif which == "frames":
        table(load(raw / "frames-spark.jsonl"), lambda r: f"{r['master']} ANSI {r['ansi']}", exact)
        table(load(raw / "frames.jsonl"), lambda r: r.get("tag", "?"), exact)
    elif which in ("main", "sum", "spill", "skip"):
        rows = load(raw / f"{which}.jsonl")
        table(rows, lambda r: r.get("tag", "?"), exact if which != "sum" else None)
    elif which == "door":
        rows = load(raw / "door.jsonl")
        reads = [r for r in rows if not r["query"].startswith(("make/", "file/"))]
        table(reads, lambda r: f"SET {r['tp']}", exact)


if __name__ == "__main__":
    main()
