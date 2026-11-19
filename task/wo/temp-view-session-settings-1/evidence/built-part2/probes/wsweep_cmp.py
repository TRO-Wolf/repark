import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

ws = Path(sys.argv[1])
base, other = sys.argv[2], sys.argv[3]
SHOWN = int(sys.argv[4]) if len(sys.argv) > 4 else 20
TABLE = re.compile(r"`?sc`?\.`?ns`?\.`?t\d+`?|t\d+")


def load(tag: str) -> dict[str, dict[str, Any]]:
    found = {}
    for ansi in ("true", "false"):
        path = ws / f"{tag}-{ansi}.jsonl"
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            record = json.loads(line)
            if record["cell"] != "done":
                found[record["cell"]] = record
    return found


def answer(record: dict[str, Any]) -> tuple:
    if record.get("na"):
        return ("na",)
    if "wrote" in record:
        return ("wrote", tuple(record["wrote"]["rows"]), tuple(record["wrote"]["types"]))
    if "source_error" in record:
        return ("source", record["source_error"], TABLE.sub("T", record["text"]))
    return ("refused", record["error"], TABLE.sub("T", record["text"]))


a, b = load(base), load(other)
keys = sorted(set(a) & set(b))
moves: Counter = Counter()
listed = []
for key in keys:
    x, y = answer(a[key]), answer(b[key])
    if x != y:
        moves[f"{x[0]}->{y[0]}"] += 1
        listed.append((key, x, y))
print(f"{base} vs {other}: {len(keys)} common cells, {len(listed)} differ", dict(moves))
print("by door", dict(Counter(key.split("/")[0] for key, _, _ in listed)))
for key, x, y in listed[:SHOWN]:
    print(" ", key, "|", str(x)[:110], "|", str(y)[:110])
