import json
import re
import sys
from pathlib import Path

out_dir, tag, before, after = sys.argv[1:5]
BRACKET = re.compile(r"\[([A-Z_]+)")


def load(who: str, ansi: str) -> dict:
    found = {}
    for line in (Path(out_dir) / f"{tag}-{who}-{ansi}.jsonl").open():
        r = json.loads(line)
        if r["key"] != "provenance" and not r["key"].startswith("setup/"):
            found[r["key"]] = r["result"]
    return found


def short(result: object) -> str:
    if isinstance(result, str):
        m = BRACKET.search(result)
        return "RAISE " + (m.group(1) if m else result[:80])
    values = ",".join(row[-1] for row in result["rows"])
    return f"{result['types'][-1]} {values}"


moved = 0
for ansi in ("true", "false"):
    old, new = load(before, ansi), load(after, ansi)
    for key in old:
        if old[key] != new.get(key):
            moved += 1
            print(f"| `{key}` | {ansi} | {short(old[key])} | {short(new.get(key))} |")
print(f"moved {moved}")
