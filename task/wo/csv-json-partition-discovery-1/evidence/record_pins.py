from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

TESTS_IO = Path(__file__).resolve().parents[4] / "python" / "repark" / "tests" / "io"


def _layouts() -> object:
    spec = importlib.util.spec_from_file_location(
        "_csv_json_partition_layouts", TESTS_IO / "_csv_json_partition_layouts.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> None:
    from pyspark.sql import SparkSession

    layouts = _layouts()
    scratch = Path(sys.argv[1])
    base = scratch / "fx"
    work = scratch / "work"
    layouts.build(base)
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("csvjson-pins")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    recorded = [
        {"case": case, "expected": layouts.run(spark, base, work, case)} for case in layouts.cases()
    ]
    spark.stop()
    out = TESTS_IO / "csv_json_partition_discovery_1_cases.json"
    out.write_text(json.dumps(recorded, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"RECORDED {len(recorded)} {out.name}")


if __name__ == "__main__":
    main()
