from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
READS = HERE / "raw" / "repark-4f2108ae-reads.jsonl"
BASE_LABELS = HERE.parent / "step0" / "labels.jsonl"
UDF = "__repark_normalize_zero__("
CATALOG = "ice"
NS = "szb"


def confounded() -> set[tuple[str, str]]:
    rows = [json.loads(line) for line in BASE_LABELS.read_text(encoding="utf-8").splitlines()]
    return {
        (row["cid"], row["route"])
        for row in rows
        if row["type"] in ("double", "float") and row["decimal_twin"] not in (None, "", "EQUAL")
    }


def doors(warehouse: Path) -> dict[str, Any]:
    import repark
    from repark import ReparkSession, functions
    from repark._native import PyReparkSession
    from repark.spark.window import Window

    root = warehouse / "native"
    root.mkdir(parents=True)
    native = PyReparkSession.native(config={"repark.sql.allowCreateFormatVersion3": "true"})
    native.register_memory_catalog(CATALOG, str(root))
    native.create_namespace(CATALOG, NS, str(root / NS))
    repark._ANSI_NATIVE = native
    facade = ReparkSession.builder.appName("confounded-wrappers").getOrCreate()
    facade.register_memory_catalog(CATALOG, str(warehouse / "facade"))
    facade.sql(f"CREATE NAMESPACE {CATALOG}.{NS}").collect()
    return {
        "sql": {"spark-sql": facade.sql, "native": repark.sql},
        "facade": facade,
        "F": functions,
        "W": Window,
    }


def logical_of_sql(frame: Any) -> str:
    table = frame.toArrow() if hasattr(frame, "toArrow") else frame.to_arrow()
    kinds = table.column(0).to_pylist()
    plans = table.column(table.num_columns - 1).to_pylist()
    return "\n".join(
        str(plan) for kind, plan in zip(kinds, plans, strict=True) if kind == "logical_plan"
    )


def logical_of_frame(frame: Any) -> str:
    text = frame._explain_text(mode="extended")
    start = text.find("== Optimized Logical Plan ==")
    end = text.find("== Physical Plan ==")
    return text[start:end] if start >= 0 else text


def wrapped_args(plan: str) -> list[str]:
    found = []
    index = plan.find(UDF)
    while index >= 0:
        depth = 1
        cursor = index + len(UDF)
        while cursor < len(plan) and depth:
            depth += {"(": 1, ")": -1}.get(plan[cursor], 0)
            cursor += 1
        found.append(plan[index + len(UDF) : cursor - 1])
        index = plan.find(UDF, index + len(UDF))
    return found


def frame_of(run: dict[str, Any], record: dict[str, Any]) -> Any:
    session = run["facade"]
    scope = {"F": run["F"], "W": run["W"], "t": session.table(f"{CATALOG}.{NS}.t_s")}
    if record.get("build"):
        scope["u"] = session.table(f"{CATALOG}.{NS}.{record['build']}_s")
    return eval(record["text"], scope)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    arguments = parser.parse_args()
    run = doors(arguments.warehouse)
    wanted = confounded()
    records = [json.loads(line) for line in READS.read_text(encoding="utf-8").splitlines() if line]
    for record in records:
        if record.get("record") == "setup":
            run["sql"][record["door"]](record["text"]).collect()
    out = []
    for record in records:
        if record.get("record") != "cell" or (record["cid"], record["route"]) not in wanted:
            continue
        if record["outcome"] != "ANSWERED" or record["layout"] != "one":
            continue
        try:
            if record["route"] in run["sql"]:
                plan = logical_of_sql(run["sql"][record["route"]]("EXPLAIN " + record["text"]))
            else:
                plan = logical_of_frame(frame_of(run, record))
            error = None
        except Exception as caught:
            plan = ""
            error = str(caught)[:200]
        args = wrapped_args(plan)
        out.append(
            {
                "cid": record["cid"],
                "route": record["route"],
                "text": record["text"],
                "wrappers": len(args),
                "nested": any(UDF in arg for arg in args),
                "args": args,
                "error": error,
                "label": arguments.label,
            }
        )
    with arguments.out.open("w", encoding="utf-8") as handle:
        for row in out:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    print(f"cells {len(out)} wrapped {sum(bool(row['wrappers']) for row in out)}")


if __name__ == "__main__":
    main()
