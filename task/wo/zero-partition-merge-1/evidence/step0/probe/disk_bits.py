from __future__ import annotations

import json
import struct
import sys
from pathlib import Path

import pyarrow.orc as orc
import pyarrow.parquet as pq


def bits(value: float | None, width: int) -> str | None:
    if value is None:
        return None
    return struct.pack(">d" if width == 8 else ">f", value).hex()


def read_file(path: str) -> dict[str, list[str | None]] | str:
    local = path.removeprefix("file://").removeprefix("file:")
    if local.endswith(".parquet"):
        table = pq.read_table(local, columns=["id", "d", "f"])
    elif local.endswith(".orc"):
        table = orc.read_table(local, columns=["id", "d", "f"])
    else:
        return "not-read"
    rows = table.to_pylist()
    return {str(r["id"]): [bits(r["d"], 8), bits(r["f"], 4)] for r in rows}


def main() -> int:
    source, target = Path(sys.argv[1]), Path(sys.argv[2])
    with target.open("w") as sink:
        for line in source.read_text().splitlines():
            record = json.loads(line)
            files = record.get("checks", {}).get("files")
            if not isinstance(files, list):
                continue
            out = []
            for content, partition, count, path in (f + [None] * (4 - len(f)) for f in files):
                if content != 0 or path is None:
                    continue
                try:
                    out.append([partition, count, read_file(path)])
                except Exception as error:
                    out.append([partition, count, f"error: {error}"[:200]])
            sink.write(
                json.dumps({"table": record["table"], "leg": record["leg"], "disk": out}) + "\n"
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
