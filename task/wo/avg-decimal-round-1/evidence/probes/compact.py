import json
import re
import sys
from pathlib import Path

src, dst = Path(sys.argv[1]), Path(sys.argv[2])
SCRATCH = re.compile(r"/[^\s\"'<>]*/scratchpad")
for path in sorted(src.glob("*.jsonl")):
    out = []
    for line in path.open():
        rec = json.loads(line)
        rec.pop("ms", None)
        rec.pop("sql", None)
        for field in ("result", "statement", "schema"):
            value = rec.get(field)
            if isinstance(value, str) and value.startswith(("ERR:", "SETUP-ERR:")):
                rec[field] = value[:200].rsplit(" ", 1)[0]
        text = json.dumps(rec, sort_keys=True)
        text = SCRATCH.sub("<scratchpad>", text)
        out.append(text)
    (dst / path.name).write_text("\n".join(out) + "\n")
    print(path.name, len(out))
