import json
import re
import sys
from pathlib import Path

src, dst = Path(sys.argv[1]), Path(sys.argv[2])
SCRATCH = re.compile(r"/tmp/[^/\s\"']+/[^\s\"']*?/scratchpad")
HOME = re.compile(r"/home/[a-z]+")
for path in sorted(src.glob("*.jsonl")):
    out = []
    for line in path.open():
        rec = json.loads(line)
        rec.pop("ms", None)
        for field in ("result", "statement"):
            value = rec.get(field)
            if isinstance(value, str) and value.startswith(("ERR:", "SETUP-ERR:")):
                rec[field] = value.rsplit(" ", 1)[0]
        if not path.name.startswith("side-"):
            rec.pop("sql", None)
        text = json.dumps(rec, sort_keys=True)
        text = SCRATCH.sub("<scratchpad>", text)
        text = HOME.sub("<home>", text)
        out.append(text)
    (dst / path.name).write_text("\n".join(out) + "\n")
    print(path.name, len(out))
