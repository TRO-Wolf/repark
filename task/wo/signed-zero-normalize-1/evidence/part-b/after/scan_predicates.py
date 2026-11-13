from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
READS = HERE / "raw" / "repark-4f2108ae-reads.jsonl"
SCAN = re.compile(r"IcebergTableScan[^\n]*")


def doors(warehouse: Path) -> dict[str, Any]:
    import repark
    from repark import ReparkSession
    from repark._native import PyReparkSession

    root = warehouse / "native"
    root.mkdir(parents=True)
    native = PyReparkSession.native(config={"repark.sql.allowCreateFormatVersion3": "true"})
    native.register_memory_catalog("ice", str(root))
    native.create_namespace("ice", "szb", str(root / "szb"))
    repark._ANSI_NATIVE = native
    facade = ReparkSession.builder.appName("scan-predicates").getOrCreate()
    facade.register_memory_catalog("ice", str(warehouse / "facade"))
    facade.sql("CREATE NAMESPACE ice.szb").collect()
    return {"spark-sql": facade.sql, "native": repark.sql}


def text_of(frame: Any) -> str:
    table = frame.toArrow() if hasattr(frame, "toArrow") else frame.to_arrow()
    return "\n".join(str(value) for value in table.column(table.num_columns - 1).to_pylist())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    arguments = parser.parse_args()
    run = doors(arguments.warehouse)
    records = [json.loads(line) for line in READS.read_text(encoding="utf-8").splitlines() if line]
    for record in records:
        if record.get("record") == "setup":
            run[record["door"]](record["text"])
    out = []
    for record in records:
        if record.get("record") != "cell" or record["route"] not in run:
            continue
        try:
            plan = text_of(run[record["route"]]("EXPLAIN " + record["text"]))
            scans = sorted(SCAN.findall(plan))
            error = None
        except Exception as caught:
            scans = []
            error = str(caught)[:200]
        out.append(
            {
                "cid": record["cid"],
                "route": record["route"],
                "scans": scans,
                "error": error,
                "label": arguments.label,
            }
        )
    with arguments.out.open("w", encoding="utf-8") as handle:
        for row in out:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    print(f"cells {len(out)} with scans {sum(bool(row['scans']) for row in out)}")


if __name__ == "__main__":
    main()
