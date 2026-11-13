from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
STEP0 = HERE.parent / "step0"


def load_probe() -> Any:
    spec = importlib.util.spec_from_file_location("szb_probe", STEP0 / "szb_probe.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules["szb_probe"] = module
    spec.loader.exec_module(module)
    return module


def read(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--head-leg", type=Path, required=True)
    parser.add_argument("--spark-leg", type=Path, required=True)
    parser.add_argument("--base-labels", type=Path, required=True)
    parser.add_argument("--head-labels", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    probe = load_probe()
    items = {item["cid"]: item for item in probe.expand_reads()}
    truth = {}
    for record in read(args.spark_leg):
        if (
            record.get("record") == "cell"
            and record["route"] == "spark-sql"
            and record["layout"] == "view"
        ):
            truth[record["cid"]] = record

    base = {
        (row["cid"], row["route"]): row["label"]
        for row in read(args.base_labels)
        if not row.get("kind")
    }
    head = {
        (row["cid"], row["route"]): row["label"]
        for row in read(args.head_labels)
        if not row.get("kind")
    }
    setups = []
    cells = []
    for record in read(args.head_leg):
        if record.get("record") == "setup" and record["outcome"] == "ANSWERED":
            setups.append({"door": record["door"], "text": record["text"]})
        if record.get("record") != "cell" or record.get("family") != "read":
            continue
        key = (record["cid"], record["route"])
        expected = truth.get(record["cid"])
        if head.get(key) != "EQUAL" or expected is None or expected["outcome"] != "ANSWERED":
            continue
        item = items[record["cid"]]
        door = "native" if record["route"] == "native" else "spark-sql"
        names = probe.table_names("repark", door, "one")
        ctx = {"engine": "repark"}
        cells.append(
            {
                "cid": record["cid"],
                "route": record["route"],
                "build": record["build"],
                "text": record["text"],
                "pre": probe.side_statements(ctx, item, "pre", names),
                "post": probe.side_statements(ctx, item, "post", names),
                "ordered": bool(record["ordered"]),
                "truth": expected["rows"],
                "key_row": base.get(key) != "EQUAL",
            }
        )
    with args.out.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps({"setup": setups}, sort_keys=True) + "\n")
        for cell in cells:
            handle.write(json.dumps(cell, sort_keys=True) + "\n")
    key_rows = sum(cell["key_row"] for cell in cells)
    print(
        f"setup {len(setups)} cells {len(cells)} key rows"
        f" {key_rows} controls {len(cells) - key_rows}"
    )


if __name__ == "__main__":
    main()
