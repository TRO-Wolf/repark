import json
import sys
from pathlib import Path


def trim(value: object, explain: bool) -> object:
    if isinstance(value, str) and value.startswith("ERR:") and " == SQL" in value:
        return value.split(" == SQL", 1)[0]
    if isinstance(value, str) and (explain or value.startswith("ERR:")):
        return value.rsplit(" ", 1)[0]
    if isinstance(value, list):
        return [trim(v, explain) for v in value]
    if isinstance(value, dict):
        return {k: trim(v, explain) for k, v in value.items()}
    return value


for path in sys.argv[1:]:
    p = Path(path)
    out = []
    for line in p.open():
        rec = json.loads(line)
        rec["result"] = trim(rec.get("result"), rec["key"].startswith("explain/"))
        out.append(json.dumps(rec))
    p.write_text("\n".join(out) + "\n")
    print(p.name, len(out))
