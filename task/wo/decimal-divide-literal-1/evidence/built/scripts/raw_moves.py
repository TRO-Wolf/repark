import json
import sys
from pathlib import Path

before, after = Path(sys.argv[1]), Path(sys.argv[2])


def load(path: Path) -> dict:
    found = {}
    for line in path.open():
        r = json.loads(line)
        if r.get("key") != "provenance":
            found[r["key"]] = {k: v for k, v in r.items() if k not in ("native", "ms")}
    return found


cells = moved = 0
for old_file in sorted(before.rglob("*.jsonl")):
    rel = old_file.relative_to(before)
    if rel.name.startswith("extra-main"):
        continue
    old, new = load(old_file), load(after / rel)
    cells += len(old)
    for key, value in old.items():
        if new.get(key) != value:
            moved += 1
            print(f"{rel} | {key} | {str(value)[:160]} | {str(new.get(key))[:160]}")
    for key in sorted(set(new) - set(old)):
        print(f"{rel} | {key} | (absent) | {str(new[key])[:160]}")
print(f"cells {cells} moved {moved}")
