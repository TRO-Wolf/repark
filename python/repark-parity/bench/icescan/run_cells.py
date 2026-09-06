"""Time the PERF-ICE-SCAN-1 read cells: median, spread, plan shape, load."""

import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from repark import ReparkSession, _native

sys.path.insert(0, str(Path(__file__).resolve().parent))
import gen_bed

CELLS = {
    "count_star": "SELECT count(*) FROM {table}",
    "count_id": "SELECT count(id) FROM {table}",
    "sum_all": "SELECT sum(v), sum(vi), sum(ts), count(s) FROM {table}",
    "string_len": "SELECT sum(length(s)) FROM {table}",
}

ITERATIONS = 5


def lane_root() -> Path:
    """The lane checkout this probe runs from."""
    out = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], check=True, capture_output=True, text=True
    )
    return Path(out.stdout.strip())


def refuse_unless_release(lane: Path) -> None:
    """Refuse anything but the lane release module."""
    import repark

    path = Path(repark.__file__).resolve()
    assert lane in path.parents, path
    assert _native.__debug_assertions__ is False


def load1() -> float:
    """The 1-minute load average."""
    return os.getloadavg()[0]


def time_cell(engine: ReparkSession, cell: str, sql: str) -> dict[str, Any]:
    """One warm-up plus five timed runs of sql, with answers and plan shape."""
    engine.sql(sql).to_arrow()
    samples = []
    start_load = load1()
    for _ in range(ITERATIONS):
        started = time.perf_counter()
        answer = engine.sql(sql).to_arrow()
        samples.append((time.perf_counter() - started) * 1000.0)
    rows = engine.sql(f"EXPLAIN {sql}").collect()
    physical = "\n".join(row["plan"] for row in rows if row["plan_type"] == "physical_plan")
    first = answer.slice(0, 1).to_pylist()
    return {
        "cell": cell,
        "sql": sql,
        "samples_ms": [round(sample, 3) for sample in samples],
        "median_ms": round(statistics.median(samples), 3),
        "min_ms": round(min(samples), 3),
        "spread_ms": round(max(samples) - min(samples), 3),
        "load1_start": round(start_load, 2),
        "load1_end": round(load1(), 2),
        "answer_rows": answer.num_rows,
        "answer_first": first,
        "scans": physical.count("IcebergTableScan"),
        "plan": physical,
    }


def main() -> None:
    """Time every read cell on the bed at the given warehouse."""
    lane = lane_root()
    refuse_unless_release(lane)
    bed = Path(sys.argv[1])
    out = Path(sys.argv[2])
    engine = (
        ReparkSession.builder.appName("scan-cells")
        .config("spark.sql.shuffle.partitions", "8")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    try:
        engine.register_memory_catalog("bed", bed / "wh")
        gen_bed.build(engine, bed)
        engine.read.parquet(str(bed / "synth_1e6")).createOrReplaceTempView("pq1e6")
        engine.read.parquet(str(bed / "synth_1e7")).createOrReplaceTempView("pq1e7")
        tables = {
            "t_plain": "bed.ns.t_plain",
            "t_plain7": "bed.ns.t_plain7",
            "parquet1e6": "pq1e6",
            "parquet1e7": "pq1e7",
        }
        results = []
        for name, table in tables.items():
            for cell, template in CELLS.items():
                sql = template.format(table=table)
                result = time_cell(engine, f"iceberg_read/{name}/{cell}", sql)
                results.append(result)
                print(
                    f"{result['cell']}: median {result['median_ms']} ms, "
                    f"spread {result['spread_ms']}, scans {result['scans']}, "
                    f"load {result['load1_start']}->{result['load1_end']}",
                    flush=True,
                )
        dv_count = time_cell(
            engine, "iceberg_read/t_dv/count_star", "SELECT count(*) FROM bed.ns.t_dv"
        )
        results.append(dv_count)
        print(f"iceberg_read/t_dv/count_star: median {dv_count['median_ms']} ms", flush=True)
        dv_sum = time_cell(
            engine,
            "iceberg_read/t_dv/sum_all",
            "SELECT sum(v), sum(vi), sum(ts), count(s) FROM bed.ns.t_dv",
        )
        results.append(dv_sum)
        print(f"iceberg_read/t_dv/sum_all: median {dv_sum['median_ms']} ms", flush=True)
        out.write_text(json.dumps(results, indent=1))
    finally:
        engine.stop()


if __name__ == "__main__":
    main()
