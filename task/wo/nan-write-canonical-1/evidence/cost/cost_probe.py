from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path
from typing import Any

ROWS = 10_000_000
CATALOG = "sc"
NS = "ns"
SIGN_SET_NAN = "sqrt(CAST(-1 - id % 3 AS DOUBLE))"
CASES = (
    ("u_insert", False, False),
    ("u_insert", False, True),
    ("p_insert", True, False),
    ("p_insert", True, True),
    ("p_ctas", True, False),
    ("p_ctas", True, True),
)


def source(nan: bool, rows: int) -> str:
    plain = "CAST(id AS DOUBLE) * 0.5"
    key = "CAST(id % 8 AS DOUBLE)"
    if nan:
        plain = f"CASE WHEN id % 100 = 0 THEN {SIGN_SET_NAN} ELSE {plain} END"
        key = f"CASE WHEN id % 100 = 0 THEN {SIGN_SET_NAN} ELSE {key} END"
    return f"SELECT id, {key} AS p, {plain} AS d FROM range(0, {rows}) r"


def open_session(warehouse: Path) -> Any:
    from repark import ReparkSession

    return (
        ReparkSession.builder.appName("nan-write-canonical-1-cost")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", str(warehouse))
        .getOrCreate()
    )


def statements(name: str, partitioned: bool, nan: bool, rows: int) -> tuple[list[str], str]:
    table = f"{CATALOG}.{NS}.{name}_{'nan' if nan else 'plain'}"
    spec = " PARTITIONED BY (p)" if partitioned else ""
    if name.endswith("ctas"):
        return [], f"CREATE TABLE {table} USING iceberg{spec} AS {source(nan, rows)}"
    create = f"CREATE TABLE {table} (id BIGINT, p DOUBLE, d DOUBLE) USING iceberg{spec}"
    return [create], f"INSERT INTO {table} {source(nan, rows)}"


def sample(arguments: argparse.Namespace) -> int:
    warehouse = Path(arguments.warehouse)
    warehouse.mkdir(parents=True, exist_ok=False)
    spark = open_session(warehouse)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NS}").collect()
    spark.sql(
        f"CREATE TABLE {CATALOG}.{NS}.warm USING iceberg PARTITIONED BY (p) AS {source(True, 1000)}"
    ).collect()
    with Path(arguments.out).open("a", encoding="utf-8") as sink:
        for name, partitioned, nan in CASES:
            setup, timed = statements(name, partitioned, nan, arguments.rows)
            for text in setup:
                spark.sql(text).collect()
            start = time.perf_counter()
            spark.sql(timed).collect()
            seconds = time.perf_counter() - start
            table = timed.split()[2]
            count = spark.sql(f"SELECT count(*) FROM {table}").collect()[0][0]
            record = {
                "label": arguments.label,
                "sample": arguments.sample,
                "case": f"{name}_{'nan1pct' if nan else 'nonan'}",
                "rows": count,
                "seconds": round(seconds, 4),
            }
            sink.write(json.dumps(record, sort_keys=True) + "\n")
    return 0


def render(arguments: argparse.Namespace) -> int:
    records = [json.loads(line) for line in Path(arguments.out).read_text().splitlines() if line]
    noise = {int(item) for item in arguments.noise.split(",") if item}
    paired = [r for r in records if r["sample"] not in noise]
    cases = list(dict.fromkeys(r["case"] for r in records))
    print("| case | base median s | head median s | head / base | base-base noise |")
    print("|---|---|---|---|---|")
    totals: dict[str, list[float]] = {}
    for case in cases:
        base = [r["seconds"] for r in paired if r["case"] == case and r["label"] == "base"]
        head = [r["seconds"] for r in paired if r["case"] == case and r["label"] == "head"]
        pair = sorted(
            (r for r in records if r["case"] == case and r["sample"] in noise),
            key=lambda r: r["sample"],
        )
        spread = pair[1]["seconds"] / pair[0]["seconds"] if len(pair) == 2 else float("nan")
        b, h = statistics.median(base), statistics.median(head)
        totals.setdefault("base", []).append(b)
        totals.setdefault("head", []).append(h)
        print(f"| {case} | {b:.3f} | {h:.3f} | {h / b:.3f}x | {spread:.3f}x |")
    b, h = sum(totals["base"]), sum(totals["head"])
    print(f"| sum of medians | {b:.3f} | {h:.3f} | {h / b:.3f}x | |")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", default="")
    parser.add_argument("--sample", type=int, default=0)
    parser.add_argument("--warehouse", default="")
    parser.add_argument("--out", required=True)
    parser.add_argument("--rows", type=int, default=ROWS)
    parser.add_argument("--render", action="store_true")
    parser.add_argument("--noise", default="")
    arguments = parser.parse_args()
    return render(arguments) if arguments.render else sample(arguments)


if __name__ == "__main__":
    raise SystemExit(main())
