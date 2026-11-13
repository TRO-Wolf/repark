import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any


def load(path: str) -> tuple[dict[str, Any] | None, dict[tuple[str, str], dict[str, Any]]]:
    header = None
    records = {}
    with Path(path).open(encoding="utf-8") as source:
        for line in source:
            item = json.loads(line)
            if "header" in item:
                header = item["header"]
                continue
            records[(item["cell"], item["door"])] = item
    return header, records


def oracle_door(door: str) -> str:
    return "sparksql" if door == "native" else door


def label(mine: dict[str, Any], truth: dict[str, Any] | None) -> str:
    if truth is None:
        return "NO-ORACLE"
    if mine["outcome"] == "error" and truth["outcome"] == "error":
        return "BOTH-REFUSED"
    if mine["outcome"] == "error":
        return "REFUSED-WHERE-SPARK-ACCEPTED"
    if truth["outcome"] == "error":
        return "ACCEPTED-WHERE-SPARK-REFUSED"
    return "EQUAL" if mine["rows"] == truth["rows"] else "WRONG-READ"


def labelled(spark: dict[Any, Any], repark: dict[Any, Any]) -> list[tuple[Any, ...]]:
    out = []
    for key, mine in repark.items():
        cell, door = key
        truth = spark.get((cell, oracle_door(door)))
        out.append((cell, door, label(mine, truth), mine, truth))
    return out


def family(cell: str) -> str:
    parts = cell.split("|")
    return "|".join(parts[:2])


def render(spark_path: str, repark_path: str, labels_path: str, table_path: str) -> None:
    spark_header, spark = load(spark_path)
    repark_header, repark = load(repark_path)
    rows = labelled(spark, repark)
    with Path(labels_path).open("w", encoding="utf-8") as out:
        for cell, door, verdict, mine, truth in rows:
            record = {"cell": cell, "door": door, "label": verdict, "repark": mine, "spark": truth}
            out.write(json.dumps(record, sort_keys=True) + "\n")
    totals = Counter((door, verdict) for _, door, verdict, _, _ in rows)
    by_family = Counter((family(cell), door, verdict) for cell, door, verdict, _, _ in rows)
    lines = [
        "# Step 0 table",
        "",
        f"Spark leg: `{json.dumps(spark_header, sort_keys=True)}`",
        "",
        f"RePark leg: `{json.dumps(repark_header, sort_keys=True)}`",
        "",
        "The native door is labelled against Spark's `spark.sql` answer for information only; "
        "it keeps its own dialect.",
        "",
        "## Totals by door",
        "",
        "| Door | Label | Cells |",
        "|---|---|---|",
    ]
    lines += [
        f"| {door} | {verdict} | {count} |" for (door, verdict), count in sorted(totals.items())
    ]
    lines += [
        "",
        "## By family and door",
        "",
        "| Family | Door | Label | Cells |",
        "|---|---|---|---|",
    ]
    lines += [
        f"| `{fam}` | {door} | {verdict} | {count} |"
        for (fam, door, verdict), count in sorted(by_family.items())
    ]
    lines += [
        "",
        "## Every non-EQUAL cell",
        "",
        "| Cell | Door | Label | RePark | Spark |",
        "|---|---|---|---|---|",
    ]
    for cell, door, verdict, mine, truth in sorted(rows, key=lambda item: (item[0], item[1])):
        if verdict == "EQUAL":
            continue
        lines.append(f"| `{cell}` | {door} | {verdict} | {answer(mine)} | {answer(truth)} |")
    with Path(table_path).open("w", encoding="utf-8") as out:
        out.write("\n".join(lines) + "\n")


def answer(record: dict[str, Any] | None) -> str:
    if record is None:
        return "none"
    if record["outcome"] == "error":
        text = record["error"]
        if len(text) > 120:
            text = text[:120].rsplit(" ", 1)[0] + " …"
        return "error: " + text.replace("|", "/")
    return "`" + json.dumps(record["rows"]).replace("|", "/") + "`"


if __name__ == "__main__":
    render(*sys.argv[1:5])
