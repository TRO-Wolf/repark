import collections
import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

DE = Path(__file__).resolve().parents[1]
ROOT = DE.parents[3]
OUT = ROOT / "python/repark/tests/sql/data/signed_zero_normalize_1_cells.jsonl"
RESIDUES = {
    (cid, route): "DECIMAL-EXPR-FLOAT-COMPARE-1"
    for cid in ("D2-1", "D2-3", "D5-1", "D5-3")
    for route in ("spark-sql", "filter-str")
}
HEAD_NAMES = {
    ("D6-2", "spark-sql"): ["ice.ns.t_split_cow.d IS NOT DISTINCT FROM Decimal128(Some(0),1,1)"],
}
GROUPS = {"D2", "D3", "D4", "D5", "D6", "D13", "A1", "A2", "A3"}
SOURCES = (
    (DE / "item1/raw/repark-68b09d18-reads.jsonl", DE / "raw/spark-signed-zero-reads.jsonl"),
    (DE / "item1/raw/repark-68b09d18-add.jsonl", DE / "item1/raw/spark-add.jsonl"),
)


def load_probe() -> ModuleType:
    spec = importlib.util.spec_from_file_location("probe_sz", DE / "probe_signed_zero.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules["probe_sz"] = module
    spec.loader.exec_module(module)
    return module


def read(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def main() -> None:
    probe = load_probe()
    out = []
    counts = collections.Counter()
    for mine_path, spark_path in SOURCES:
        spark = {
            (r["cid"], r["route"], r["layout"]): r
            for r in read(spark_path)
            if r.get("record") == "cell"
        }
        for record in read(mine_path):
            if record.get("record") != "cell" or record["group"] not in GROUPS:
                continue
            if record["layout"] != record["test_layout"]:
                continue
            if record["group"] == "A2" and record["route"] != "native":
                continue
            truth = spark.get((record["cid"], "spark-sql", record["truth_layout"]))
            same_route = "spark-sql" if record["route"] == "native" else record["route"]
            same = spark.get((record["cid"], same_route, record["test_layout"]))
            label = "+".join(probe.label_of(record, truth, same))
            if truth is None or truth["outcome"] != "ANSWERED":
                counts["skipped-no-truth"] += 1
                continue
            residue = RESIDUES.get((record["cid"], record["route"]))
            out.append(
                {
                    "residue": residue,
                    "cid": record["cid"],
                    "group": record["group"],
                    "route": record["route"],
                    "text": record["text"],
                    "ordered": bool(record.get("ordered")),
                    "cols": (record["cols"] or HEAD_NAMES.get((record["cid"], record["route"])))
                    if record["group"] == "D6"
                    else None,
                    "truth": truth["rows"],
                    "base": label,
                }
            )
            counts[(record["group"], label.split("+")[0])] += 1
    OUT.write_text("".join(json.dumps(cell, sort_keys=True) + "\n" for cell in out))
    print(len(out), "cells")
    for key, value in sorted(counts.items(), key=str):
        print(key, value)


if __name__ == "__main__":
    main()
