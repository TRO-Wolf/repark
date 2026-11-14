from __future__ import annotations

import json
import re
import statistics
import sys
import time
from pathlib import Path
from typing import Any

FILES = 100
ROWS_PER_FILE = 100_000
ROW_GROUP_ROWS = 50_000
STATS_ROWS = 10_000_000
REPEATS = 15
SELECTIVE = "d > 9949999.5"
QUERIES = {
    "count_all": "SELECT count(*) FROM many",
    "count_selective": f"SELECT count(*) FROM many WHERE {SELECTIVE}",
    "max_stats": "SELECT max(d) FROM single",
}
METRICS = (
    "row_groups_pruned_statistics",
    "files_ranges_pruned_statistics",
    "bytes_scanned",
    "output_rows",
)


def write_data(root: Path) -> None:
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    many = root / "many"
    many.mkdir(parents=True, exist_ok=True)
    for index in range(FILES):
        start = index * ROWS_PER_FILE
        ids = np.arange(start, start + ROWS_PER_FILE, dtype=np.int64)
        table = pa.table({"id": ids, "d": ids.astype(np.float64)})
        pq.write_table(table, many / f"part-{index:03d}.parquet", row_group_size=ROW_GROUP_ROWS)
    single = root / "single"
    single.mkdir(parents=True, exist_ok=True)
    ids = np.arange(STATS_ROWS, dtype=np.int64)
    table = pa.table({"id": ids, "d": ids.astype(np.float64) * 0.5})
    pq.write_table(table, single / "part-000.parquet", row_group_size=1_000_000)


def metric_values(text: str) -> dict[str, list[str]]:
    found: dict[str, list[str]] = {}
    for name in METRICS:
        found[name] = re.findall(rf"{name}=([^,\]]+)", text)
    return found


def explain(session: Any, query: str) -> dict[str, Any]:
    rows = session.sql(f"EXPLAIN ANALYZE {query}").collect()
    text = "\n".join(str(value) for row in rows for value in row)
    scans = [line.strip() for line in text.splitlines() if "DataSourceExec" in line]
    return {"metrics": metric_values("\n".join(scans)), "scan_lines": scans}


def run(build: str, root: Path, out: Path) -> None:
    from repark import ReparkSession

    session = ReparkSession.builder.appName("nan-minmax-cost").getOrCreate()
    session.read.parquet(str(root / "many")).createOrReplaceTempView("many")
    session.read.parquet(str(root / "single")).createOrReplaceTempView("single")
    record: dict[str, Any] = {"build": build, "queries": {}}
    for name, query in QUERIES.items():
        answer = [list(row) for row in session.sql(query).collect()]
        for _ in range(3):
            session.sql(query).collect()
        samples = []
        for _ in range(REPEATS):
            began = time.perf_counter()
            session.sql(query).collect()
            samples.append(time.perf_counter() - began)
        record["queries"][name] = {
            "answer": answer,
            "median_s": statistics.median(samples),
            "samples_s": samples,
            **explain(session, query),
        }
    with out.open("a") as sink:
        sink.write(json.dumps(record, sort_keys=True) + "\n")


def report(raw: Path, base: str, head: str) -> None:
    records = [json.loads(line) for line in raw.read_text().splitlines()]
    builds = {base: [r for r in records if r["build"] == base]}
    builds[head] = [r for r in records if r["build"] == head]
    pairs = list(zip(builds[base], builds[head], strict=False))
    print(f"Pairs: {len(pairs)} (base `{base}`, head `{head}`), ", end="")
    print(f"{REPEATS} timed runs per query per sample.")
    print()
    print("| query | answer | main median (ms) | head median (ms) ", end="")
    print("| head / main (median of pair ratios) |")
    print("|---|---|---|---|---|")
    for name in QUERIES:
        ratios = [h["queries"][name]["median_s"] / b["queries"][name]["median_s"] for b, h in pairs]
        main_ms = statistics.median(b["queries"][name]["median_s"] for b, _ in pairs) * 1000
        head_ms = statistics.median(h["queries"][name]["median_s"] for _, h in pairs) * 1000
        answer = pairs[0][1]["queries"][name]["answer"]
        ratio = statistics.median(ratios)
        print(f"| `{name}` | {answer} | {main_ms:.2f} | {head_ms:.2f} | {ratio:.3f} |")
    print()
    print(
        "| query | build | row groups (pruned by statistics) | file ranges (pruned by statistics) |"
    )
    print("|---|---|---|---|")
    for name in QUERIES:
        for tag in (base, head):
            metrics = builds[tag][0]["queries"][name]["metrics"]
            groups = "; ".join(metrics["row_groups_pruned_statistics"]) or "none"
            files = "; ".join(metrics["files_ranges_pruned_statistics"]) or "none"
            print(f"| `{name}` | {tag} | {groups} | {files} |")


def main() -> int:
    mode, root = sys.argv[1], Path(sys.argv[2])
    if mode == "report":
        report(root, sys.argv[3], sys.argv[4])
        return 0
    if mode == "data":
        write_data(root)
        return 0
    run(mode, root, Path(sys.argv[3]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
