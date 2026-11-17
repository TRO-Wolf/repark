import json
import sys
from pathlib import Path

D, T = Path(sys.argv[1]), Path(sys.argv[2])
a = json.loads((D / "oracle" / "spark-false.json").read_text())
b = json.loads((D / "oracle" / "spark-true.json").read_text())


def clean(v: object) -> object:
    if isinstance(v, str):
        parts = v.split(":")
        return parts[0] + ":" + parts[1]
    if isinstance(v, dict) and "statement" in v:
        v = dict(v)
        v["statement"] = v["statement"] if v["statement"] == "ok" else clean(v["statement"])
        v["rows"] = clean(v["rows"]) if isinstance(v["rows"], str) else v["rows"]
    return v


out = {
    "spark": a["version"],
    "jar": "iceberg-spark-runtime-4.1_2.13-1.11.0.jar",
    "session_zone": "UTC",
    "cells": {
        "false": {k: clean(v) for k, v in a["cells"].items()},
        "true": {k: clean(v) for k, v in b["cells"].items()},
    },
}
assert set(out["cells"]["false"]) == set(out["cells"]["true"])
text = json.dumps(out, sort_keys=True, separators=(",", ":")) + "\n"
(T / "decimal_common_type_1_spark_oracle.json").write_text(text)
print(len(out["cells"]["false"]))
