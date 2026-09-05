"""The two or three Spark comparison cells: same fixture, bounded driver memory, spill measured."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import resource
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

CELLS: dict[str, str] = {
    "sort": "SELECT id, h FROM base ORDER BY h",
    "hash_join": "SELECT l.id, r.payload FROM base l JOIN other r ON l.h = r.h",
    "collect_list": "SELECT g % 8 AS k, collect_list(h) AS a FROM base GROUP BY g % 8",
    "nested_loop_join": "SELECT l.id, r.v FROM base l JOIN other r ON l.v < r.v",
}

BASE_SELECT: str = (
    "SELECT id, md5(cast(id AS string)) AS h, id % 1024 AS g, "
    "concat(md5(cast(id AS string)), md5(cast(id + 1 AS string))) AS payload, "
    "cast(id AS double) * 1.5 AS v FROM RANGE({rows})"
)


def jvm_peak_rss_bytes() -> int:
    """Peak resident set of the reaped JVM child in bytes; 0 until `spark.stop()` reaps it."""
    return resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss * 1024


ERROR_MARKERS: tuple[str, ...] = (
    "OutOfMemoryError",
    "Exception in thread",
    "ERROR Executor",
    "SparkException",
    "GC overhead limit exceeded",
)


def error_lines(text: str) -> list[str]:
    """Every JVM stderr line naming a failure, so a published claim can cite what was recorded."""
    return [line.strip() for line in text.splitlines() if any(m in line for m in ERROR_MARKERS)][
        :40
    ]


@contextlib.contextmanager
def captured_stderr(path: Path) -> Iterator[None]:
    """Redirect file descriptor 2 to `path`, so the JVM's own stderr is captured with Python's."""
    path.parent.mkdir(parents=True, exist_ok=True)
    sys.stderr.flush()
    saved = os.dup(2)
    handle = path.open("wb")
    try:
        os.dup2(handle.fileno(), 2)
        yield
    finally:
        sys.stderr.flush()
        os.dup2(saved, 2)
        os.close(saved)
        handle.close()


def spill_totals(event_log_dir: Path) -> dict[str, int]:
    """Sum memory and disk spill over every task-end event Spark logged."""
    memory = 0
    disk = 0
    tasks = 0
    records = 0
    for path in sorted(event_log_dir.rglob("*")):
        if not path.is_file() or path.name.startswith((".", "appstatus")):
            continue
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if '"Event":"SparkListenerTaskEnd"' not in line.replace(" ", ""):
                continue
            payload = json.loads(line)
            metrics = payload.get("Task Metrics") or {}
            tasks += 1
            memory += int(metrics.get("Memory Bytes Spilled", 0))
            disk += int(metrics.get("Disk Bytes Spilled", 0))
            shuffle = metrics.get("Shuffle Write Metrics") or {}
            records += int(shuffle.get("Shuffle Records Written", 0))
    return {
        "memory_bytes_spilled": memory,
        "disk_bytes_spilled": disk,
        "tasks_seen": tasks,
        "shuffle_records_written": records,
    }


def build_spark(event_log_dir: Path, driver_memory: str, partitions: int) -> Any:
    """A local Spark whose driver heap is `driver_memory`, with the event log armed."""
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-memory {driver_memory} pyspark-shell"
    from pyspark.sql import SparkSession

    event_log_dir.mkdir(parents=True, exist_ok=True)
    return (
        SparkSession.builder.appName("h3-spill-spark")
        .master(f"local[{partitions}]")
        .config("spark.sql.shuffle.partitions", str(partitions))
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", event_log_dir.as_uri())
        .config("spark.eventLog.compress", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def run_cell(spark: Any, cell: str, rows: int, right_rows: int) -> dict[str, Any]:
    """Run one comparison cell to completion through the `noop` sink and time it.

    The sink matters: `count()` lets Spark's optimizer drop the very operator under test
    (an `ORDER BY` a count does not need), so the first draft measured a plan without a sort.
    """
    spark.sql(BASE_SELECT.format(rows=rows)).createOrReplaceTempView("base")
    spark.sql(BASE_SELECT.format(rows=right_rows)).createOrReplaceTempView("other")
    started = time.perf_counter()
    frame = spark.sql(CELLS[cell])
    frame.write.format("noop").mode("overwrite").save()
    wall_ms = (time.perf_counter() - started) * 1000.0
    heap = int(spark.sparkContext._jvm.java.lang.Runtime.getRuntime().maxMemory())
    return {"outcome": "ok", "wall_ms": wall_ms, "jvm_max_heap_bytes": heap}


def main(argv: list[str] | None = None) -> int:
    """CLI for one Spark comparison cell."""
    parser = argparse.ArgumentParser(description="H3-SPILL-1 Spark comparison cell")
    parser.add_argument("--cell", required=True, choices=sorted(CELLS))
    parser.add_argument("--rows", type=int, default=10_000_000)
    parser.add_argument("--right-rows", type=int, default=1_000_000)
    parser.add_argument("--driver-memory", default="1g")
    parser.add_argument("--partitions", type=int, default=4)
    parser.add_argument("--scratch", required=True)
    parser.add_argument("--json-out", type=Path, required=True)
    args = parser.parse_args(argv)
    event_log_dir = Path(args.scratch) / f"events-{args.cell}-{args.driver_memory}"
    stderr_path = Path(args.scratch) / f"stderr-{args.cell}-{args.driver_memory}.log"
    with captured_stderr(stderr_path):
        spark = build_spark(event_log_dir, args.driver_memory, args.partitions)
        try:
            payload = run_cell(spark, args.cell, args.rows, args.right_rows)
        except BaseException as error:
            if isinstance(error, (KeyboardInterrupt, SystemExit)):
                raise
            payload = {"outcome": "error", "message": f"{type(error).__name__}: {error}"[:600]}
        finally:
            spark.stop()
    captured = stderr_path.read_text(encoding="utf-8", errors="replace")
    payload.update(spill_totals(event_log_dir))
    payload["cell"] = args.cell
    payload["rows"] = args.rows
    payload["driver_memory"] = args.driver_memory
    payload["jvm_peak_rss_bytes"] = jvm_peak_rss_bytes()
    payload["jvm_error_lines"] = error_lines(captured)
    payload["jvm_stderr_tail"] = captured[-4000:]
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(payload), encoding="utf-8")
    print(json.dumps(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
