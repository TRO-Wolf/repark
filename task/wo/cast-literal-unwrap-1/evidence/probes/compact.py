import json
import re
import sys
from pathlib import Path

source, target = Path(sys.argv[1]), Path(sys.argv[2])
DROP = ("sql", "ms")
SCRATCH = re.compile(r"/tmp/[^\s'\"`]*?/scratchpad/")
HOME = re.compile(r"/home/[^/\s'\"`]+/")
TMP = re.compile(r"/tmp/[^\s'\"`\]]*")
CAP = 160


def scrub(text: str) -> str:
    return TMP.sub("<tmp>", HOME.sub("<home>/", SCRATCH.sub("<scratchpad>/", text)))


for path in sorted(source.glob("*.jsonl")):
    if path.name.startswith("test-"):
        continue
    rows = []
    for line in path.read_text().splitlines():
        r = json.loads(scrub(line))
        kept = {k: v for k, v in r.items() if k not in DROP}
        for k, v in kept.items():
            if isinstance(v, str) and v.startswith(("ERR:", "SETUP-ERR:")):
                kept[k] = v[:CAP].rsplit(" ", 1)[0]
        rows.append(kept)
    with (target / path.name).open("w") as fh:
        for r in rows:
            fh.write(json.dumps(r, sort_keys=True) + "\n")
    print(path.name, len(rows))
for path in sorted(source.glob("*.txt")):
    (target / path.name).write_text(scrub(path.read_text()))
    print(path.name)
