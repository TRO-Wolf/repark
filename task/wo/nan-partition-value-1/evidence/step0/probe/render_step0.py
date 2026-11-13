import json
import sys
from pathlib import Path
from typing import Any

src = Path(sys.argv[1])
dst = Path(sys.argv[2])
build = sys.argv[3]
dst.mkdir(parents=True, exist_ok=True)
names = {
    "repark-main-own.jsonl": f"repark-{build}-writes.jsonl",
    "repark-main-reads-spark.jsonl": f"repark-{build}-reads-spark.jsonl",
    "spark.jsonl": "spark-own.jsonl",
    "manifests-repark-main.jsonl": f"manifests-repark-{build}.jsonl",
    "manifests-spark-own.jsonl": "manifests-spark-own.jsonl",
    "spark-cross-main.jsonl": f"spark-reads-repark-{build}.jsonl",
}
loaded = {}
for name, out in names.items():
    path = src / name
    if not path.exists():
        continue
    lines = []
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        if "native" in record:
            record["native"] = (
                "<out>/" + Path(record["native"]).parent.parent.name + "/repark/_native.abi3.so"
            )
        lines.append(record)
    (dst / out).write_text("".join(json.dumps(r, sort_keys=True) + "\n" for r in lines))
    loaded[name] = lines


def special(value: Any) -> Any:
    if isinstance(value, float) and value != value:
        return "NaN"
    if isinstance(value, float) and value in (float("inf"), float("-inf")):
        return "Infinity" if value > 0 else "-Infinity"
    if isinstance(value, list):
        return [special(v) for v in value]
    if isinstance(value, dict):
        return {k: special(v) for k, v in value.items()}
    return value


def by_cell(name: str) -> dict[str, Any]:
    return {
        r["cell"]: special(r.get("rows", r.get("error")))
        for r in loaded.get(name, [])
        if "cell" in r
    }


rp = by_cell("repark-main-own.jsonl")
sp = by_cell("spark.jsonl")
rr = by_cell("repark-main-reads-spark.jsonl")
sx = by_cell("spark-cross-main.jsonl")


def show(value: Any) -> str:
    return json.dumps(value).replace("|", "\\|")


rows = [
    "| Cell | RePark on its own table | Spark on its own table | Verdict |",
    "|---|---|---|---|",
]
for cell, value in rp.items():
    parts = cell.split("|")
    check, ty, kind = parts[0], parts[1], parts[2]
    door = kind if kind in ("ctas", "writeto", "saveastable") else "insert"
    spark_kind = "part" if kind in ("ctas", "writeto", "saveastable") else kind
    spark_cell = f"{check}|{ty}|{spark_kind}|spark-own"
    other = sp.get(spark_cell)

    def canon(v: Any, check: str = check) -> str:
        if isinstance(v, list) and check in ("files", "partitions"):
            return sorted(json.dumps(x) for x in v)
        return json.dumps(v)

    verdict = "—" if other is None else ("EQUAL" if canon(value) == canon(other) else "DIFFERS")
    shown = show(other) if other is not None else "—"
    rows.append(f"| `{check}` {ty} {kind} ({door}) | {show(value)} | {shown} | {verdict} |")
read_rows = [
    "| Cell | RePark reading Spark's table | Spark on its own table | Verdict |",
    "|---|---|---|---|",
]
for cell, value in rr.items():
    check, ty, kind, _ = cell.split("|")
    other = sp.get(f"{check}|{ty}|{kind}|spark-own")

    def canon(v: Any, check: str = check) -> str:
        if isinstance(v, list) and check == "files":
            return sorted(json.dumps(x) for x in v)
        return json.dumps(v)

    verdict = "—" if other is None else ("EQUAL" if canon(value) == canon(other) else "DIFFERS")
    shown = show(other) if other is not None else "—"
    read_rows.append(f"| `{check}` {ty} {kind} | {show(value)} | {shown} | {verdict} |")
cross_rows = [
    "| Cell | Spark reading RePark's table | Spark on its own table | Verdict |",
    "|---|---|---|---|",
]
for cell, value in sx.items():
    parts = cell.split("|")
    if len(parts) != 4 or parts[3] != "spark-reads-repark":
        continue
    check, ty, kind, _ = parts
    spark_kind = "part" if kind in ("ctas", "writeto", "saveastable") else kind
    other = sp.get(f"{check}|{ty}|{spark_kind}|spark-own")

    def canon(v: Any, check: str = check) -> str:
        if isinstance(v, list) and check == "files":
            return sorted(json.dumps(x) for x in v)
        return json.dumps(v)

    verdict = "—" if other is None else ("EQUAL" if canon(value) == canon(other) else "DIFFERS")
    shown = show(other) if other is not None else "—"
    cross_rows.append(f"| `{check}` {ty} {kind} | {show(value)} | {shown} | {verdict} |")
java = [r for r in loaded.get("spark.jsonl", []) if r.get("cell", "").startswith("java-json")]
java_rows = ["| Java call | Result |", "|---|---|"]
for r in java:
    java_rows.append(f"| `{r['cell']}` | {show({k: v for k, v in r.items() if k != 'cell'})} |")
(dst / f"table-{build}.md").write_text(
    "\n".join(
        [
            "## Writes",
            "",
            *rows,
            "",
            "## Spark reading RePark's tables",
            "",
            *cross_rows,
            "",
            "## The read half",
            "",
            *read_rows,
            "",
            "## Java's JSON for non-finite doubles",
            "",
            *java_rows,
            "",
        ]
    )
)
print(len(rows) - 2, len(read_rows) - 2, len(java_rows) - 2)
