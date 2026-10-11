"""Measure a 200-epoch ``availableNow`` run for the MB-4 lineage-audit gate (owner ruling D5).

Run it once on each of two builds of the native module and compare the two result files.
The gate is: the audited driver takes at most 1.05 times the no-audit driver, on the median
of three runs, on a quiet box.

Usage:
    python task/wo/microbatch/mb4_lineage_timing.py measure <result.json> [--runs 3] [--epochs 200]
    python task/wo/microbatch/mb4_lineage_timing.py compare <no_audit.json> <audited.json>

``measure`` builds a source of one-row commits in a scratch warehouse, then times three doors
with one file per batch: a ``foreachBatch`` body that appends to the sink, a ``foreachBatch``
body that writes nothing, and ``toTable`` as the control whose code the audit does not touch.
``compare`` prints the three ratios and exits 1 when a ``foreachBatch`` ratio is above 1.05.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import tempfile
import time
from functools import partial
from pathlib import Path
from typing import Any

GATE_RATIO = 1.05
DOORS = ("foreach_append", "foreach_no_write", "to_table")
GATED_DOORS = ("foreach_append", "foreach_no_write")


def append_to(sink: str, frame: Any, batch_id: int) -> None:
    """Append the batch frame to the declared sink."""
    frame.writeTo(sink).append()


def write_nothing(frame: Any, batch_id: int) -> None:
    """Leave the sink to the driver's stamp-only commit."""


def start_query(spark: Any, door: str, sink: str, checkpoint: str, name: str) -> Any:
    """Start one ``availableNow`` query over the scratch source through ``door``."""
    writer = (
        spark.readStream.option("streaming-max-files-per-micro-batch", "1")
        .table("sc.t.src")
        .writeStream.option("checkpointLocation", checkpoint)
        .queryName(name)
        .trigger(availableNow=True)
    )
    if door == "to_table":
        return writer.toTable(sink)
    body = partial(append_to, sink) if door == "foreach_append" else write_nothing
    return writer.foreachBatch(body).option("repark.cdc.sink", sink).start()


def measure(result: Path, runs: int, epochs: int) -> None:
    """Time each door ``runs`` times and write the wall-clock seconds to ``result``."""
    from repark import _native
    from repark.spark.session.session_core import ReparkSession

    root = tempfile.mkdtemp(prefix="mb4-lineage-timing-")
    spark = ReparkSession.builder.appName("mb4-lineage-timing").getOrCreate()
    _native._streaming_tests_allow_local_catalog(spark._ensure_alive())
    spark.register_memory_catalog("sc", f"{root}/wh")
    spark.sql("CREATE NAMESPACE sc.t")
    spark.sql("CREATE TABLE sc.t.src (id BIGINT, k STRING)")
    for row in range(epochs):
        spark.sql(f"INSERT INTO sc.t.src VALUES ({row}, 'k')")
    seconds: dict[str, list[float]] = {door: [] for door in DOORS}
    for run in range(runs):
        for door in DOORS:
            sink = f"sc.t.snk_{door}_{run}"
            spark.sql(f"CREATE TABLE {sink} (id BIGINT, k STRING)")
            began = time.perf_counter()
            query = start_query(spark, door, sink, f"{root}/ck", f"q_{door}_{run}")
            query.awaitTermination()
            seconds[door].append(time.perf_counter() - began)
            if query.lastProgress["batchId"] != epochs - 1:
                raise RuntimeError(f"{door} run {run} ended at {query.lastProgress['batchId']}")
    spark.stop()
    medians = {door: statistics.median(values) for door, values in seconds.items()}
    result.write_text(
        json.dumps({"epochs": epochs, "runs": runs, "seconds": seconds, "median": medians}),
        encoding="utf-8",
    )
    for door in DOORS:
        print(f"{door}: median {medians[door]:.3f} s of {[round(v, 3) for v in seconds[door]]}")


def compare(no_audit: Path, audited: Path) -> int:
    """Print the audited/no-audit ratio per door and return 1 when the gate fails."""
    before = json.loads(no_audit.read_text(encoding="utf-8"))["median"]
    after = json.loads(audited.read_text(encoding="utf-8"))["median"]
    failed = False
    for door in DOORS:
        ratio = after[door] / before[door]
        gated = door in GATED_DOORS
        verdict = "control" if not gated else ("over the gate" if ratio > GATE_RATIO else "ok")
        failed = failed or (gated and ratio > GATE_RATIO)
        print(f"{door}: {before[door]:.3f} s -> {after[door]:.3f} s, ratio {ratio:.3f} ({verdict})")
    return 1 if failed else 0


def main() -> int:
    """Parse the command line and run ``measure`` or ``compare``."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    measuring = commands.add_parser("measure")
    measuring.add_argument("result", type=Path)
    measuring.add_argument("--runs", type=int, default=3)
    measuring.add_argument("--epochs", type=int, default=200)
    comparing = commands.add_parser("compare")
    comparing.add_argument("no_audit", type=Path)
    comparing.add_argument("audited", type=Path)
    arguments = parser.parse_args()
    if arguments.command == "measure":
        measure(arguments.result, arguments.runs, arguments.epochs)
        return 0
    return compare(arguments.no_audit, arguments.audited)


if __name__ == "__main__":
    sys.exit(main())
