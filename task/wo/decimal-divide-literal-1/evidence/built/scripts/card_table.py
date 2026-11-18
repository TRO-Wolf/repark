import json
import re
import sys
from pathlib import Path

out_dir, tag, *who = sys.argv[1:]
BRACKET = re.compile(r"\[([A-Z_]+)")


def load(name: str, ansi: str) -> dict:
    found = {}
    for line in (Path(out_dir) / f"{tag}-{name}-{ansi}.jsonl").open():
        r = json.loads(line)
        if r["key"] != "provenance":
            found[r["key"]] = r.get("result")
    return found


def short(result: object) -> str:
    if isinstance(result, str):
        m = BRACKET.search(result)
        return "RAISE " + m.group(1) if m else result[:60]
    if not isinstance(result, dict):
        return str(result)
    return f"`{result['types'][-1]}` " + ", ".join(row[-1] for row in result["rows"])


print("| cell | ANSI | " + " | ".join(who) + " |")
print("|---|---|" + "---|" * len(who))
for ansi in ("true", "false"):
    found = [load(name, ansi) for name in who]
    for key in found[0]:
        print(f"| `{key}` | {ansi} | " + " | ".join(short(f.get(key)) for f in found) + " |")
