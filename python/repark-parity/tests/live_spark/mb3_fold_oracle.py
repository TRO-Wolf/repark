"""MB-3 fold-1 oracle: four Spark 4.1.2 + Iceberg 1.11.0 cells recorded verbatim."""

from __future__ import annotations

import calendar
import inspect
import json
import os
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from mb0_bench import (
    NAMESPACE,
    Bench,
    Collect,
    append,
    await_query,
    build_session,
    checkpoint_logs,
    create_source,
    error_of,
    progress_batches,
)
from mb0_streaming_oracle import fresh_dir, preamble, scrub
from pyspark.errors import PySparkException
from pyspark.sql import DataFrame

ONE_FILE = {"streaming-max-files-per-micro-batch": "1"}
INTERVAL_MS = 2_000
SCHEDULE_TIMEOUT_S = 60.0
RETENTION_KEY = "spark.sql.streaming.numRecentProgressUpdates"
EXPECTED_CELLS = 4

Cell = Callable[[Bench], tuple[str, dict[str, Any]]]
Shape = Callable[[DataFrame, DataFrame], DataFrame]


class FailWhenFlagged:
    def __init__(self, flag: Path, collect: Collect) -> None:
        self.flag = flag
        self.collect = collect

    def __call__(self, frame: DataFrame, batch_id: int) -> None:
        self.collect(frame, batch_id)
        if self.flag.exists():
            raise RuntimeError(f"mb3 injected failure at batch {batch_id}")


class JoinShape:
    def __init__(self, join: Callable[[str, DataFrame, DataFrame], DataFrame], how: str) -> None:
        self.join = join
        self.how = how

    def __call__(self, stream: DataFrame, static: DataFrame) -> DataFrame:
        return self.join(self.how, stream, static)


def union_static(stream: DataFrame, static: DataFrame) -> DataFrame:
    return stream.union(static)


def static_union_stream(stream: DataFrame, static: DataFrame) -> DataFrame:
    return static.union(stream)


def stream_join_static(how: str, stream: DataFrame, static: DataFrame) -> DataFrame:
    return stream.join(static, "id", how)


def static_join_stream(how: str, stream: DataFrame, static: DataFrame) -> DataFrame:
    return static.join(stream, "id", how)


def shape_run(bench: Bench, name: str, source: str, static: str, build: Shape) -> dict[str, Any]:
    collect = Collect()
    stream = bench.spark.readStream.format("iceberg").options(**ONE_FILE).load(source)
    fixed = bench.spark.table(static)
    try:
        frame = build(stream, fixed)
    except PySparkException as exc:
        return {"refused_at": "analysis", "error": error_of(exc), "batches": collect.batches}
    writer = (
        frame.writeStream.foreachBatch(collect)
        .option("checkpointLocation", bench.checkpoint(name))
        .trigger(availableNow=True)
    )
    try:
        query = writer.start()
    except PySparkException as exc:
        return {"refused_at": "start", "error": error_of(exc), "batches": collect.batches}
    error = await_query(query)
    return {
        "refused_at": "run" if error else None,
        "error": error,
        "batches": collect.batches,
    }


def cell_static_shapes(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "j1_src")
    append(bench, source, [1, 2, 3])
    append(bench, source, [4, 5])
    static = create_source(bench, "j1_static")
    append(bench, static, [2, 900])
    shapes: list[tuple[str, Shape]] = [
        ("stream_union_static", union_static),
        ("static_union_stream", static_union_stream),
    ]
    for how in ("inner", "left", "right", "full", "left_semi", "left_anti"):
        shapes.append((f"stream_{how}_join_static", JoinShape(stream_join_static, how)))
        shapes.append((f"static_{how}_join_stream", JoinShape(static_join_stream, how)))
    answer = {name: shape_run(bench, f"j1_{name}", source, static, build) for name, build in shapes}
    return "rows", answer


def epoch_millis(timestamp: str) -> int:
    seconds, millis = timestamp.removesuffix("Z").split(".")
    return calendar.timegm(time.strptime(seconds, "%Y-%m-%dT%H:%M:%S")) * 1000 + int(millis)


def cell_trigger_schedule(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "p1_src")
    for value in (1, 2, 3, 4):
        append(bench, source, [value])
    collect = Collect()
    query = (
        bench.spark.readStream.format("iceberg")
        .options(**ONE_FILE)
        .load(source)
        .writeStream.foreachBatch(collect)
        .option("checkpointLocation", bench.checkpoint("p1"))
        .trigger(processingTime=f"{INTERVAL_MS} milliseconds")
        .start()
    )
    deadline = time.monotonic() + SCHEDULE_TIMEOUT_S
    while len(query._jsq.recentProgress()) < 4 and time.monotonic() < deadline:
        time.sleep(0.05)
    progress = [json.loads(entry.json()) for entry in query._jsq.recentProgress()]
    query.stop()
    triggers = [
        {
            "batchId": entry["batchId"],
            "numInputRows": entry["numInputRows"],
            "timestamp": entry["timestamp"],
            "millis_past_interval_boundary": epoch_millis(entry["timestamp"]) % INTERVAL_MS,
        }
        for entry in progress
    ]
    return "progress", {"interval_ms": INTERVAL_MS, "triggers": triggers}


def cell_progress_retention_zero(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "g1_src")
    for value in (1, 2, 3):
        append(bench, source, [value])
    collect = Collect()
    before = bench.spark.conf.get(RETENTION_KEY)
    set_error = None
    try:
        bench.spark.conf.set(RETENTION_KEY, "0")
    except PySparkException as exc:
        set_error = error_of(exc)
    answer: dict[str, Any] = {"default": before, "set_error": set_error}
    if set_error is None:
        query = (
            bench.spark.readStream.format("iceberg")
            .options(**ONE_FILE)
            .load(source)
            .writeStream.foreachBatch(collect)
            .option("checkpointLocation", bench.checkpoint("g1"))
            .trigger(availableNow=True)
            .start()
        )
        answer["error"] = await_query(query)
        answer["recent_progress_count"] = len(query._jsq.recentProgress())
        answer["last_progress_is_null"] = query._jsq.lastProgress() is None
        answer["batches"] = collect.batches
        bench.spark.conf.set(RETENTION_KEY, before)
    return "progress", answer


def cell_replay_window(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "w9_src")
    append(bench, source, [1, 2, 3])
    checkpoint = bench.checkpoint("w9")
    flag = bench.checkpoints / "w9-fail.flag"
    runs: list[dict[str, Any]] = []
    for step in ("fail", "restart"):
        collect = Collect()
        if step == "fail":
            flag.write_text("fail\n")
        else:
            flag.unlink()
            append(bench, source, [4, 5])
        query = (
            bench.spark.readStream.format("iceberg")
            .load(source)
            .writeStream.foreachBatch(FailWhenFlagged(flag, collect))
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start()
        )
        error = await_query(query)
        runs.append(
            {
                "step": step,
                "error_class": error["class"] if error else None,
                "batches": collect.batches,
                "progress_batches": progress_batches(query),
                "offsets": checkpoint_logs(checkpoint, "offsets"),
                "commits": checkpoint_logs(checkpoint, "commits"),
            }
        )
    return "rows", {"runs": runs}


CELLS: tuple[tuple[str, str, Cell, tuple[Any, ...]], ...] = (
    (
        "MB3-J1",
        "plan.static_frame_shapes.answer",
        cell_static_shapes,
        (
            shape_run,
            JoinShape,
            union_static,
            static_union_stream,
            stream_join_static,
            static_join_stream,
        ),
    ),
    ("MB3-P1", "trigger.processing_time_schedule.progress", cell_trigger_schedule, (epoch_millis,)),
    ("MB3-G1", "progress.retention_zero.answer", cell_progress_retention_zero, ()),
    ("MB3-W9", "write.restart_uncommitted_grown_source.rows", cell_replay_window, (FailWhenFlagged,)),
)


def record_all(bench: Bench) -> list[dict[str, Any]]:
    bench.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
    recorded: list[dict[str, Any]] = []
    for cell_id, field, cell, helpers in CELLS:
        kind, answer = cell(bench)
        entry = {
            "cell": cell_id,
            "statement": "\n".join(inspect.getsource(part) for part in (cell, *helpers)),
            "kind": kind,
            "answer": answer,
            "field": field,
        }
        recorded.append(json.loads(scrub(json.dumps(entry, sort_keys=True), bench)))
        print(json.dumps(recorded[-1], sort_keys=True), flush=True)
    return recorded


def main() -> int:
    warehouse = fresh_dir("MB0_WAREHOUSE", "warehouse")
    checkpoints = fresh_dir("MB0_CHECKPOINTS", "checkpoints")
    out = Path(os.environ.get("MB0_OUT") or Path(__file__).with_suffix(".json"))
    spark = build_session(warehouse)
    spark.sparkContext.setLogLevel("ERROR")
    try:
        cells = record_all(Bench(spark, warehouse, checkpoints))
        document = {"preamble": preamble(spark), "cells": cells}
    finally:
        spark.stop()
    out.write_text(json.dumps(document, indent=2, sort_keys=True, ensure_ascii=False) + "\n")
    return 0 if len(cells) == EXPECTED_CELLS else 1


if __name__ == "__main__":
    sys.exit(main())
