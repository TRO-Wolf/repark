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
    "topk": "SELECT id, d FROM many ORDER BY d DESC LIMIT 10",
    "max_descending": "SELECT max(d) FROM many_desc",
}
VIEWS = ("many", "single", "many_desc")
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
    descending = root / "many_desc"
    descending.mkdir(parents=True, exist_ok=True)
    for index in range(FILES):
        start = (FILES - 1 - index) * ROWS_PER_FILE
        ids = np.arange(start, start + ROWS_PER_FILE, dtype=np.int64)
        table = pa.table({"id": ids, "d": ids.astype(np.float64)})
        name = descending / f"part-{index:03d}.parquet"
        pq.write_table(table, name, row_group_size=ROW_GROUP_ROWS)


def metric_values(text: str) -> dict[str, list[str]]:
    found: dict[str, list[str]] = {}
    for name in METRICS:
        found[name] = re.findall(rf"{name}=([^,\]]+)", text)
    return found


def explain(session: Any, query: str, root: Path) -> dict[str, Any]:
    rows = session.sql(f"EXPLAIN ANALYZE {query}").collect()
    text = "\n".join(str(value) for row in rows for value in row)
    for prefix in (str(root.resolve()), str(root)):
        text = text.replace(prefix, "cost-data").replace(prefix.lstrip("/"), "cost-data")
    scans = [line.strip() for line in text.splitlines() if "DataSourceExec" in line]
    return {"metrics": metric_values("\n".join(scans)), "scan_lines": scans}


def run(build: str, root: Path, out: Path) -> None:
    from repark import ReparkSession, _native

    package = Path(_native.__file__).resolve().parent.parent.name
    session = ReparkSession.builder.appName("nan-minmax-cost").getOrCreate()
    for view in VIEWS:
        if (root / view).exists():
            session.read.parquet(str(root / view)).createOrReplaceTempView(view)
    record: dict[str, Any] = {"build": build, "package": package, "queries": {}}
    for name, query in QUERIES.items():
        if not all((root / view).exists() for view in VIEWS if f" {view}" in query):
            continue
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
            **explain(session, query, root),
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
    for name in [q for q in QUERIES if q in pairs[0][0]["queries"]]:
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
    for name in [q for q in QUERIES if q in pairs[0][0]["queries"]]:
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
