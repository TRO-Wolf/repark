from __future__ import annotations

import json
import sys
from pathlib import Path

CLASSES = {
    "part2": "TEMP-VIEW-SESSION-SETTINGS-1 part 2: a DataFrame frame inlined into a SQL view "
    "follows the reader",
    "frame": "direct gap: the DataFrame frame answers unlike Spark under its own setting",
}


def same(want: dict, got: dict) -> bool:
    if "error" in want or "error" in got:
        return "error" in want and "error" in got and want["error"] == got["error"]
    return want.get("rows") == got.get("rows")


def main() -> None:
    spark = json.loads(Path(sys.argv[1]).read_text())
    head = json.loads(Path(sys.argv[2]).read_text())
    found: dict = {"classes": CLASSES}
    for mode in ("true", "false"):
        held = {}
        for key, want in sorted(spark[mode].items()):
            got = head[mode][key]
            if not key.startswith("sweep/") or same(want, got):
                continue
            _, family, shape, order, read = key.split("/")
            first, second = order.split(">")
            sibling = "/".join(["sweep", family, shape, order, "read_" + first])
            own = same(spark[mode][sibling], head[mode][sibling])
            kind = "part2" if read == "read_" + second and own else "frame"
            held[key] = {"class": kind, "answer": got["error"] if "error" in got else got["rows"]}
        found[mode] = held
    Path(sys.argv[3]).write_text(json.dumps(found, sort_keys=True, indent=1) + "\n")
    print({mode: len(found[mode]) for mode in ("true", "false")})


if __name__ == "__main__":
    main()
