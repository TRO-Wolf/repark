import json
import re
import sys
from pathlib import Path

src, dst = Path(sys.argv[1]), Path(sys.argv[2])
SCRATCH = re.compile(r"[A-Za-z0-9_.:/-]*scratchpad/")
HOME = re.compile(r"/(?:home)/[A-Za-z0-9_.-]+/")
TRUNCATED = re.compile(r"(file:)?/?tmp/[a-z]+-[0-9]+/[A-Za-z0-9_.:/-]*")


def trim(node: object) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "text" and isinstance(value, str) and len(value) >= 250:
                node[key] = value.rsplit(" ", 1)[0]
            else:
                trim(value)


def scrub(text: str) -> str:
    return TRUNCATED.sub("<scratchpad>/", HOME.sub("<home>/", SCRATCH.sub("<scratchpad>/", text)))


for path in sorted(src.iterdir()):
    if path.suffix == ".jsonl":
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        for row in rows:
            row.pop("ms", None)
            trim(row)
        text = "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n"
    elif path.suffix == ".json":
        data = json.loads(path.read_text())
        trim(data)
        text = json.dumps(data, indent=1, sort_keys=True) + "\n"
    elif path.suffix == ".txt":
        text = path.read_text()
    else:
        continue
    (dst / path.name).write_text(scrub(text))
    print("copied " + path.name)
