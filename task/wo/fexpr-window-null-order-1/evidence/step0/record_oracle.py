import json
import sys
from pathlib import Path
from typing import Any

FRAGMENT_DOORS = ("fexpr", "fexpr_withcolumn")
CONTROL_DOORS = (
    "sparksql",
    "selectexpr",
    "orderby_fexpr",
    "orderby_fexpr_method",
    "sort_string",
    "sort_col",
    "typed_window",
    "fexpr_agg",
    "fexpr_groupby",
)


def group_of(record: dict[str, Any]) -> str | None:
    door, verdict = record["door"], record["label"]
    if door in FRAGMENT_DOORS and verdict == "WRONG-READ":
        return "fixed"
    if door in FRAGMENT_DOORS and verdict == "EQUAL":
        return "fragment_equal"
    if door in CONTROL_DOORS and verdict == "EQUAL":
        return "control"
    if door == "native" and record["repark"]["outcome"] == "rows":
        return "native"
    return None


def main(labels_path: str, target_path: str) -> None:
    cells = []
    with Path(labels_path).open(encoding="utf-8") as source:
        for line in source:
            record = json.loads(line)
            group = group_of(record)
            if group is None:
                continue
            truth = record["repark"] if group == "native" else record["spark"]
            cells.append(
                {
                    "cell": record["cell"],
                    "door": record["door"],
                    "group": group,
                    "text": record["repark"]["text"],
                    "expected": truth["rows"],
                    "source": "repark-main" if group == "native" else "spark-4.1.2",
                }
            )
    cells.sort(key=lambda item: (item["group"], item["cell"], item["door"]))
    head = {
        "oracle": "PySpark 4.1.2 local[2]; native cells hold main's own answer",
        "recorder": "task/wo/fexpr-window-null-order-1/evidence/step0/record_oracle.py",
    }
    lines = [json.dumps(cell, sort_keys=True, separators=(",", ":")) for cell in cells]
    with Path(target_path).open("w", encoding="utf-8") as out:
        out.write("{" + json.dumps(head, sort_keys=True)[1:-1] + ',"cells":[\n')
        out.write(",\n".join(lines))
        out.write("\n]}\n")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
