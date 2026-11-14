from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

ABSOLUTE = re.compile(r"(file:)?/(tmp|home)/[^\s'\",)\]]*")


def scrub_text(text: str) -> str:
    def replace(match: re.Match[str]) -> str:
        path = match.group(0)
        if path.endswith("_native.abi3.so"):
            return "<package>/repark/_native.abi3.so"
        return "<local>/" + path.rsplit("/", 1)[-1]

    return ABSOLUTE.sub(replace, text)


def scrub(value: Any) -> Any:
    if isinstance(value, str):
        return scrub_text(value)
    if isinstance(value, list):
        return [scrub(item) for item in value]
    if isinstance(value, dict):
        return {key: scrub(item) for key, item in value.items()}
    return value


def main() -> int:
    source, target, build = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
    with target.open("w") as sink:
        for line in source.read_text().splitlines():
            record = scrub(json.loads(line))
            if record.get("leg") == "setup":
                record["build"] = build
            sink.write(json.dumps(record, sort_keys=False) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
