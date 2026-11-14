from __future__ import annotations

import hashlib
import json
import math
import re
import sys
from pathlib import Path
from typing import Any

import repark._native as rn
from repark import ReparkSession

SETS = {
    "P_spread": "d >= 1990.0",
    "P_const": "d >= 99.0",
    "P_mix": "d >= 1975.0",
    "P_const_nan": "d > 98.5",
    "P_spread_nan": "d > 1985.5",
    "R_one": "d > 5.0",
    "R_many": "d > 5.0",
    "R_nullnan": "d > 5.0",
}
KEYS = ("files_ranges_pruned_statistics", "row_groups_pruned_statistics")


def token(value: Any) -> Any:
    if isinstance(value, float) and math.isnan(value):
        return "NaN"
    return value


def main() -> None:
    roots, out = [Path(a) for a in sys.argv[1].split(",")], Path(sys.argv[2])
    print("native", rn.__file__, flush=True)
    spark = ReparkSession.builder.appName("nmm-delta-prune").getOrCreate()
    result = {"native_sha256": hashlib.sha256(Path(rn.__file__).read_bytes()).hexdigest()}
    for name, where in SETS.items():
        folder = next(r / name for r in roots if (r / name).is_dir())
        spark.read.parquet(str(folder)).createOrReplaceTempView("p")
        for label, text in {
            "sel": f"SELECT count(*) FROM p WHERE {where}",
            "eqnan": "SELECT count(*) FROM p WHERE d = CAST('NaN' AS DOUBLE)",
            "isnan": "SELECT count(*) FROM p WHERE isnan(d)",
        }.items():
            rows = spark.sql(text).toArrow().to_pylist()
            result[f"{name}|{label}"] = [[token(v) for v in r.values()] for r in rows]
        plan = "\n".join(
            str(v)
            for row in spark.sql(f"EXPLAIN ANALYZE SELECT count(*) FROM p WHERE {where}").collect()
            for v in row
        )
        result[f"{name}|metrics"] = {
            key: re.findall(rf"{key}=(\d+) total → (\d+) matched", plan) for key in KEYS
        }
    out.write_text(json.dumps(result, indent=1))
    print("done", flush=True)


if __name__ == "__main__":
    main()
