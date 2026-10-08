"""MB-0 streaming oracle: 29 Spark 4.1.2 + Iceberg 1.11.0 cells recorded verbatim."""

from __future__ import annotations

import functools
import inspect
import json
import os
import sys
import tempfile
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
    create_sink,
    create_source,
    error_of,
    last_snapshot,
    operations,
    progress_batches,
    rows_of,
    snapshot_log,
    table_rows,
)
from pyspark.errors import PySparkException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf

FAIL_ID = 3
PROGRESS_TIMEOUT_S = 120.0
FUTURE_MS = 3_600_000
LANDING_MS = 6_000
EXPECTED_CELLS = 29


Cell = Callable[[Bench], tuple[str, dict[str, Any]]]


class AppendTo:
    def __init__(self, table: str) -> None:
        self.table = table

    def __call__(self, frame: DataFrame, batch_id: int) -> None:
        frame.writeTo(self.table).append()


def overwrite_snapshot(bench: Bench, table: str) -> None:
    bench.spark.sql(f"INSERT OVERWRITE {table} SELECT id + 10, k FROM {table}")


def delete_snapshot(bench: Bench, table: str) -> None:
    bench.spark.sql(f"DELETE FROM {table} WHERE id = 1")


def replace_snapshot(bench: Bench, table: str) -> None:
    short = table.removeprefix("local.")
    bench.spark.sql(
        f"CALL local.system.rewrite_data_files(table => '{short}', "
        "options => map('rewrite-all', 'true'))"
    )


def fail_when_flagged(flag: str, value: int) -> int:
    if Path(flag).exists() and value == FAIL_ID:
        raise RuntimeError(f"mb0 injected failure at id {value}")
    return value


def mutation_cell(
    bench: Bench,
    name: str,
    mutate: Callable[[Bench, str], None],
    options: dict[str, str],
) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, name)
    append(bench, source, [1, 2])
    append(bench, source, [3])
    collect = Collect()
    checkpoint = bench.checkpoint(name)
    first = (
        bench.spark.readStream.format("iceberg")
        .options(**options)
        .load(source)
        .writeStream.foreachBatch(collect)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    first_error = await_query(first)
    mutate(bench, source)
    append(bench, source, [4])
    second = (
        bench.spark.readStream.format("iceberg")
        .options(**options)
        .load(source)
        .writeStream.foreachBatch(collect)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    error = first_error or await_query(second)
    context = {"operations": operations(bench, source), "batches": collect.batches}
    if error:
        return "error", {**error, **context}
    return "rows", {"schema": collect.schema, **context}


def cell_r1(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "r1")
    append(bench, source, [1, 2])
    append(bench, source, [3])
    collect = Collect()
    checkpoint = bench.checkpoint("r1")
    for ids in ([4, 5], []):
        query = (
            bench.spark.readStream.format("iceberg")
            .load(source)
            .writeStream.foreachBatch(collect)
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        if ids:
            append(bench, source, ids)
    query = (
        bench.spark.readStream.format("iceberg")
        .load(source)
        .writeStream.foreachBatch(collect)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return "rows", {
        "schema": collect.schema,
        "batches": collect.batches,
        "operations": operations(bench, source),
    }


def cell_r2(bench: Bench) -> tuple[str, dict[str, Any]]:
    return mutation_cell(bench, "r2", overwrite_snapshot, {})


def cell_r3(bench: Bench) -> tuple[str, dict[str, Any]]:
    return mutation_cell(
        bench, "r3", overwrite_snapshot, {"streaming-skip-overwrite-snapshots": "true"}
    )


def cell_r4(bench: Bench) -> tuple[str, dict[str, Any]]:
    return mutation_cell(
        bench, "r4", overwrite_snapshot, {"streaming-skip-delete-snapshots": "true"}
    )


def cell_r5(bench: Bench) -> tuple[str, dict[str, Any]]:
    return mutation_cell(bench, "r5", delete_snapshot, {})


def cell_r6(bench: Bench) -> tuple[str, dict[str, Any]]:
    return mutation_cell(bench, "r6", delete_snapshot, {"streaming-skip-delete-snapshots": "true"})


def cell_r7(bench: Bench) -> tuple[str, dict[str, Any]]:
    return mutation_cell(
        bench, "r7", delete_snapshot, {"streaming-skip-overwrite-snapshots": "true"}
    )


def cell_r8(bench: Bench) -> tuple[str, dict[str, Any]]:
    return mutation_cell(bench, "r8", replace_snapshot, {})


def cell_r9(bench: Bench) -> tuple[str, dict[str, Any]]:
    return mutation_cell(
        bench, "r9", replace_snapshot, {"streaming-skip-overwrite-snapshots": "true"}
    )


def cell_r10(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "r10")
    for ids in ([1], [2], [3]):
        append(bench, source, ids)
        time.sleep(0.25)
    stamps = [
        row[0]
        for row in bench.spark.sql(
            f"SELECT unix_millis(committed_at) FROM {source}.snapshots ORDER BY committed_at"
        ).collect()
    ]
    cases = {
        "midway_between_first_and_second_commit": (stamps[0] + stamps[1]) // 2,
        "equal_to_second_commit": stamps[1],
    }
    answer: dict[str, Any] = {"operations": operations(bench, source), "cases": {}}
    for label, millis in cases.items():
        collect = Collect()
        query = (
            bench.spark.readStream.format("iceberg")
            .option("stream-from-timestamp", str(millis))
            .load(source)
            .writeStream.foreachBatch(collect)
            .option("checkpointLocation", bench.checkpoint(f"r10-{label}"))
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        answer["schema"] = collect.schema
        answer["cases"][label] = collect.batches
    return "rows", answer


def cell_r11(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "r11")
    append(bench, source, [1, 2, 3, 4, 5, 6], files=3)
    collect = Collect()
    query = (
        bench.spark.readStream.format("iceberg")
        .option("streaming-max-files-per-micro-batch", "1")
        .load(source)
        .writeStream.foreachBatch(collect)
        .option("checkpointLocation", bench.checkpoint("r11"))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return "rows", {
        "schema": collect.schema,
        "batches": collect.batches,
        "batch_count": len(collect.batches),
        "operations": operations(bench, source),
    }


def cell_r12(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "r12")
    append(bench, source, [1, 2, 3, 4, 5, 6], files=2)
    collect = Collect()
    query = (
        bench.spark.readStream.format("iceberg")
        .option("streaming-max-rows-per-micro-batch", "1")
        .load(source)
        .writeStream.foreachBatch(collect)
        .option("checkpointLocation", bench.checkpoint("r12"))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return "rows", {
        "schema": collect.schema,
        "batches": collect.batches,
        "batch_count": len(collect.batches),
        "operations": operations(bench, source),
    }


def cell_r13(bench: Bench) -> tuple[str, dict[str, Any]]:
    checkpoint = Path(bench.checkpoint("r11"))
    offsets = {
        name: (checkpoint / "offsets" / name).read_text().splitlines()
        for name in checkpoint_logs(str(checkpoint), "offsets")
    }
    commits = {
        name: (checkpoint / "commits" / name).read_text().splitlines()
        for name in checkpoint_logs(str(checkpoint), "commits")
    }
    return "checkpoint", {
        "metadata": (checkpoint / "metadata").read_text().splitlines(),
        "offsets": offsets,
        "commits": commits,
        "entries": sorted(str(p.relative_to(checkpoint)) for p in checkpoint.rglob("*")),
    }


def snapshot_ids(bench: Bench, table: str) -> list[int]:
    return [
        row[0]
        for row in bench.spark.sql(
            f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at"
        ).collect()
    ]


def snapshot_millis(bench: Bench, table: str) -> list[int]:
    return [
        row[0]
        for row in bench.spark.sql(
            f"SELECT unix_millis(committed_at) FROM {table}.snapshots ORDER BY committed_at"
        ).collect()
    ]


def offset_positions(bench: Bench, table: str, checkpoint: str) -> list[dict[str, Any]]:
    ordinals = {snapshot: index for index, snapshot in enumerate(snapshot_ids(bench, table))}
    ends = []
    for name in checkpoint_logs(checkpoint, "offsets"):
        lines = (Path(checkpoint) / "offsets" / name).read_text().splitlines()
        offset = json.loads(lines[-1])
        ends.append(
            {
                "batch": int(name),
                "snapshot_ordinal": ordinals.get(offset["snapshot_id"]),
                "position": offset["position"],
                "scan_all_files": offset["scan_all_files"],
            }
        )
    return ends


def option_run(
    bench: Bench, source: str, checkpoint: str, options: dict[str, str]
) -> dict[str, Any]:
    collect = Collect()
    query = (
        bench.spark.readStream.format("iceberg")
        .options(**options)
        .load(source)
        .writeStream.foreachBatch(collect)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    error = await_query(query)
    return {
        "error": error,
        "batches": collect.batches,
        "progress_batches": progress_batches(query),
        "offsets": offset_positions(bench, source, checkpoint),
    }


def wait_past(millis: int) -> None:
    while time.time() * 1000 <= millis + 500:
        time.sleep(0.1)


def cell_r14(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "r14")
    append(bench, source, [1])
    future = snapshot_millis(bench, source)[-1] + FUTURE_MS
    options = {"stream-from-timestamp": str(future)}
    checkpoint = bench.checkpoint("r14")
    before_append = option_run(bench, source, checkpoint, options)
    append(bench, source, [2])
    resumed = option_run(bench, source, checkpoint, options)
    fresh = option_run(bench, source, bench.checkpoint("r14-fresh"), options)
    landing = create_source(bench, "r14_landing")
    append(bench, landing, [10])
    target = snapshot_millis(bench, landing)[-1] + LANDING_MS
    landing_options = {"stream-from-timestamp": str(target)}
    landing_checkpoint = bench.checkpoint("r14-landing")
    append(bench, landing, [11])
    before_landing = option_run(bench, landing, landing_checkpoint, landing_options)
    wait_past(target)
    append(bench, landing, [12])
    after_landing = option_run(bench, landing, landing_checkpoint, landing_options)
    return "rows", {
        "past_head": {
            "below_t": [stamp < future for stamp in snapshot_millis(bench, source)],
            "operations": operations(bench, source),
            "started_before_append": before_append,
            "resumed_after_append": resumed,
            "fresh_after_append": fresh,
        },
        "later_landing": {
            "below_t": [stamp < target for stamp in snapshot_millis(bench, landing)],
            "operations": operations(bench, landing),
            "before_landing": before_landing,
            "after_landing": after_landing,
        },
    }


def cell_r15(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "r15")
    for ids in ([1, 2], [3, 4], [5, 6]):
        append(bench, source, ids)
    single = create_source(bench, "r15_single")
    append(bench, single, [1, 2, 3, 4, 5, 6], files=3)
    answer: dict[str, Any] = {
        "operations": operations(bench, source),
        "single_snapshot_operations": operations(bench, single),
    }
    for rows in ("3", "4", "5"):
        options = {"streaming-max-rows-per-micro-batch": rows}
        answer[f"three_snapshots_max_rows_{rows}"] = option_run(
            bench, source, bench.checkpoint(f"r15-three-{rows}"), options
        )
        answer[f"one_snapshot_max_rows_{rows}"] = option_run(
            bench, single, bench.checkpoint(f"r15-one-{rows}"), options
        )
    return "rows", answer


def cell_r16(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "r16")
    bench.spark.sql(f"INSERT OVERWRITE {source} VALUES (1, 'k1'), (2, 'k0')")
    append(bench, source, [3])
    run = option_run(bench, source, bench.checkpoint("r16"), {})
    context = {
        "operations": operations(bench, source),
        "batches": run["batches"],
        "progress_batches": run["progress_batches"],
        "offsets": run["offsets"],
    }
    if run["error"]:
        return "error", {**run["error"], **context}
    return "rows", context


def cell_r17(bench: Bench) -> tuple[str, dict[str, Any]]:
    answer: dict[str, Any] = {}
    for name, mutate in (("r17", overwrite_snapshot), ("r17_delete", delete_snapshot)):
        source = create_source(bench, name)
        append(bench, source, [1])
        append(bench, source, [2])
        mutate(bench, source)
        options = {"streaming-max-files-per-micro-batch": "1"}
        run = option_run(bench, source, bench.checkpoint(name), options)
        answer[name] = {"operations": operations(bench, source), **run}
    return "rows", answer


def processing_time_run(
    bench: Bench, source: str, checkpoint: str, options: dict[str, str]
) -> dict[str, Any]:
    collect = Collect()
    query = (
        bench.spark.readStream.format("iceberg")
        .options(**options)
        .load(source)
        .writeStream.foreachBatch(collect)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="0 seconds")
        .start()
    )
    error = None
    terminated = False
    try:
        terminated = query.awaitTermination(PROGRESS_TIMEOUT_S)
    except PySparkException as exc:
        error = error_of(exc)
        terminated = True
    still_active = query.isActive
    if still_active:
        query.stop()
    return {
        "error": error,
        "terminated_on_its_own": terminated,
        "active_after_wait": still_active,
        "batches": collect.batches,
        "progress_batches": progress_batches(query),
        "offsets": offset_positions(bench, source, checkpoint),
    }


def cell_r18(bench: Bench) -> tuple[str, dict[str, Any]]:
    answer: dict[str, Any] = {}
    for name, mutate in (("r18", overwrite_snapshot), ("r18_delete", delete_snapshot)):
        source = create_source(bench, name)
        append(bench, source, [1])
        append(bench, source, [2])
        mutate(bench, source)
        options = {"streaming-max-files-per-micro-batch": "1"}
        run = processing_time_run(bench, source, bench.checkpoint(name), options)
        answer[name] = {"operations": operations(bench, source), **run}
    return "rows", answer


def fanout_cell(bench: Bench, name: str, fanout: str) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, f"{name}_src")
    append(bench, source, [1, 2, 3, 4], files=2)
    sink = create_sink(bench, f"{name}_sink", partition="k")
    query = (
        bench.spark.readStream.format("iceberg")
        .load(source)
        .writeStream.format("iceberg")
        .outputMode("append")
        .option("fanout-enabled", fanout)
        .option("checkpointLocation", bench.checkpoint(name))
        .trigger(availableNow=True)
        .toTable(sink)
    )
    error = await_query(query)
    if error:
        return "error", error
    snapshot = last_snapshot(bench, sink)
    return "summary", {
        "operation": snapshot["operation"],
        "keys": sorted(snapshot["summary"]),
        **table_rows(bench, sink),
    }


def cell_w1(bench: Bench) -> tuple[str, dict[str, Any]]:
    return fanout_cell(bench, "w1", "false")


def cell_w2(bench: Bench) -> tuple[str, dict[str, Any]]:
    return fanout_cell(bench, "w2", "true")


def cell_w3(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "w3_src")
    append(bench, source, [1, 2, 3])
    sink = create_sink(bench, "w3_sink", columns="k STRING, count BIGINT")
    runs = []
    for ids in ([4, 5], []):
        counts = bench.spark.readStream.format("iceberg").load(source).groupBy("k").count()
        query = (
            counts.writeStream.format("iceberg")
            .outputMode("complete")
            .option("checkpointLocation", bench.checkpoint("w3"))
            .trigger(availableNow=True)
            .toTable(sink)
        )
        error = await_query(query)
        if error:
            return "error", error
        runs.append(
            {
                "batches": progress_batches(query),
                "operations": operations(bench, sink),
                **table_rows(bench, sink),
            }
        )
        if ids:
            append(bench, source, ids)
    return "rows", {"runs": runs}


def cell_w4(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "w4_src")
    append(bench, source, [1, 2])
    sink = create_sink(bench, "w4_sink")
    for ids in ([3, 4], []):
        query = (
            bench.spark.readStream.format("iceberg")
            .load(source)
            .writeStream.foreachBatch(AppendTo(sink))
            .option("checkpointLocation", bench.checkpoint("w4"))
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        if ids:
            append(bench, source, ids)
    snapshots = [
        {
            "operation": entry["operation"],
            "added-records": entry["summary"].get("added-records"),
            "keys": sorted(entry["summary"]),
        }
        for entry in snapshot_log(bench, sink)
    ]
    return "summary", {"snapshots": snapshots, **table_rows(bench, sink)}


def cell_w5(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "w5_src")
    append(bench, source, [1, 2])
    sink = create_sink(bench, "w5_sink")
    checkpoint = bench.checkpoint("w5")
    runs = []
    for ids in ([3, 4], [], []):
        query = (
            bench.spark.readStream.format("iceberg")
            .load(source)
            .writeStream.format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .toTable(sink)
        )
        query.awaitTermination()
        runs.append(
            {
                "batches": progress_batches(query),
                "offsets": checkpoint_logs(checkpoint, "offsets"),
                "commits": checkpoint_logs(checkpoint, "commits"),
            }
        )
        if ids:
            append(bench, source, ids)
    final = table_rows(bench, sink)
    ids_seen = [row[0] for row in final["rows"]]
    return "rows", {
        "runs": runs,
        "duplicates": len(ids_seen) - len(set(ids_seen)),
        "operations": operations(bench, sink),
        **final,
    }


def cell_w6(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "w6_src")
    append(bench, source, [1, 2])
    sink = create_sink(bench, "w6_sink")
    checkpoint = bench.checkpoint("w6")
    flag = bench.checkpoints / "w6-fail.flag"
    guard = sf.udf(functools.partial(fail_when_flagged, str(flag)), "bigint")
    runs: list[dict[str, Any]] = []
    for step in ("commit", "kill", "restart"):
        if step == "kill":
            append(bench, source, [3, 4])
            flag.write_text("fail\n")
        if step == "restart":
            flag.unlink()
        query = (
            bench.spark.readStream.format("iceberg")
            .load(source)
            .withColumn("id", guard(sf.col("id")))
            .writeStream.format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .toTable(sink)
        )
        error = await_query(query)
        runs.append(
            {
                "step": step,
                "error": error,
                "batches": progress_batches(query),
                "offsets": checkpoint_logs(checkpoint, "offsets"),
                "commits": checkpoint_logs(checkpoint, "commits"),
                "sink_rows": rows_of(bench.spark.table(sink)),
            }
        )
    final = table_rows(bench, sink)
    ids_seen = [row[0] for row in final["rows"]]
    return "rows", {
        "runs": runs,
        "duplicates": len(ids_seen) - len(set(ids_seen)),
        "operations": operations(bench, sink),
        **final,
    }


def cell_w7(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "w7_src")
    append(bench, source, [1, 2, 3])
    sink = create_sink(bench, "w7_sink")
    query = (
        bench.spark.readStream.format("iceberg")
        .load(source)
        .writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", bench.checkpoint("w7"))
        .trigger(availableNow=True)
        .toTable(sink)
    )
    query.awaitTermination()
    snapshot = last_snapshot(bench, sink)
    return "summary", {"operation": snapshot["operation"], "summary": snapshot["summary"]}


def cell_w8(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "w8_src")
    append(bench, source, [1, 2])
    sink = create_sink(bench, "w8_sink")
    default_location = bench.spark.conf.get("spark.sql.streaming.checkpointLocation", None)
    try:
        query = (
            bench.spark.readStream.format("iceberg")
            .load(source)
            .writeStream.format("iceberg")
            .outputMode("append")
            .trigger(availableNow=True)
            .toTable(sink)
        )
    except PySparkException as exc:
        return "error", {**error_of(exc), "session_checkpoint_location": default_location}
    error = await_query(query)
    if error:
        return "error", {**error, "session_checkpoint_location": default_location}
    return "rows", {
        "session_checkpoint_location": default_location,
        "batches": progress_batches(query),
        **table_rows(bench, sink),
    }


def once_run(bench: Bench, name: str, options: dict[str, str]) -> dict[str, Any]:
    source = create_source(bench, f"{name}_src")
    append(bench, source, [1, 2])
    append(bench, source, [3])
    sink = create_sink(bench, f"{name}_sink")
    query = (
        bench.spark.readStream.format("iceberg")
        .options(**options)
        .load(source)
        .writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", bench.checkpoint(name))
        .trigger(once=True)
        .toTable(sink)
    )
    error = await_query(query)
    if error:
        return {"error": error}
    return {
        "batches": progress_batches(query),
        "is_active": query.isActive,
        **table_rows(bench, sink),
    }


def cell_t1(bench: Bench) -> tuple[str, dict[str, Any]]:
    answer = {
        "default_options": once_run(bench, "t1", {}),
        "max_files_per_micro_batch_1": once_run(
            bench, "t1_max_files", {"streaming-max-files-per-micro-batch": "1"}
        ),
    }
    errors = [run["error"] for run in answer.values() if "error" in run]
    return ("error" if errors else "rows"), answer


def cell_t2(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "t2")
    append(bench, source, [1, 2])
    append(bench, source, [3])
    collect = Collect()
    query = (
        bench.spark.readStream.format("iceberg")
        .option("streaming-max-files-per-micro-batch", "1")
        .load(source)
        .writeStream.foreachBatch(collect)
        .option("checkpointLocation", bench.checkpoint("t2"))
        .trigger(availableNow=True)
        .start()
    )
    returned = query.awaitTermination()
    return "rows", {
        "schema": collect.schema,
        "batches": collect.batches,
        "batch_count": len(collect.batches),
        "await_termination_returned": returned,
        "is_active": query.isActive,
        "exception": None if query.exception() is None else str(query.exception()),
    }


def cell_t3(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "t3_src")
    append(bench, source, [1, 2, 3])
    sink = create_sink(bench, "t3_sink")
    query = (
        bench.spark.readStream.format("iceberg")
        .load(source)
        .writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", bench.checkpoint("t3"))
        .trigger(processingTime="1 hour")
        .toTable(sink)
    )
    deadline = time.monotonic() + PROGRESS_TIMEOUT_S
    while query._jsq.lastProgress() is None and time.monotonic() < deadline:
        time.sleep(0.1)
    last = query._jsq.lastProgress()
    progress = json.loads(last.json()) if last is not None else None
    status = dict(query.status)
    active_before_stop = query.isActive
    stopped = query.stop()
    awaited = query.awaitTermination()
    awaited_with_timeout = query.awaitTermination(1)
    return "progress", {
        "last_progress": progress,
        "status": status,
        "recent_progress_count": len(query.recentProgress),
        "is_active_before_stop": active_before_stop,
        "stop_returned": stopped,
        "await_termination_returned": awaited,
        "await_termination_1s_returned": awaited_with_timeout,
        "is_active_after_stop": query.isActive,
        "exception": None if query.exception() is None else str(query.exception()),
        "status_after_stop": dict(query.status),
        **table_rows(bench, sink),
    }


CELLS: tuple[tuple[str, str, Cell, tuple[Any, ...]], ...] = (
    ("MB0-R1", "read.appends.rows", cell_r1, ()),
    ("MB0-R2", "read.overwrite_unskipped.error", cell_r2, (mutation_cell, overwrite_snapshot)),
    ("MB0-R3", "read.overwrite_skipped.rows", cell_r3, (mutation_cell, overwrite_snapshot)),
    ("MB0-R4", "read.overwrite_wrong_skip.error", cell_r4, (mutation_cell, overwrite_snapshot)),
    ("MB0-R5", "read.delete_unskipped.error", cell_r5, (mutation_cell, delete_snapshot)),
    ("MB0-R6", "read.delete_skipped.rows", cell_r6, (mutation_cell, delete_snapshot)),
    ("MB0-R7", "read.delete_wrong_skip.error", cell_r7, (mutation_cell, delete_snapshot)),
    ("MB0-R8", "read.replace_unskipped.answer", cell_r8, (mutation_cell, replace_snapshot)),
    ("MB0-R9", "read.replace_skip_overwrite.answer", cell_r9, (mutation_cell, replace_snapshot)),
    ("MB0-R10", "read.from_timestamp.rows", cell_r10, ()),
    ("MB0-R11", "read.max_files.batches", cell_r11, ()),
    ("MB0-R12", "read.max_rows.batches", cell_r12, ()),
    ("MB0-R13", "read.checkpoint.offset", cell_r13, ()),
    (
        "MB0b-R14",
        "read.from_timestamp_past_head.rows",
        cell_r14,
        (option_run, offset_positions, snapshot_millis, wait_past),
    ),
    ("MB0b-R15", "read.max_rows_crossing.batches", cell_r15, (option_run, offset_positions)),
    ("MB0b-R16", "read.first_snapshot_overwrite.answer", cell_r16, (option_run,)),
    (
        "MB0b-R17",
        "read.available_now_capped_overwrite.answer",
        cell_r17,
        (option_run, offset_positions, overwrite_snapshot, delete_snapshot),
    ),
    (
        "MB0b-R18",
        "read.processing_time_capped_overwrite.answer",
        cell_r18,
        (processing_time_run, offset_positions, overwrite_snapshot, delete_snapshot),
    ),
    ("MB0-W1", "write.append_no_fanout.summary", cell_w1, (fanout_cell,)),
    ("MB0-W2", "write.append_fanout.summary", cell_w2, (fanout_cell,)),
    ("MB0-W3", "write.complete.answer", cell_w3, ()),
    ("MB0-W4", "write.foreach_batch.snapshots", cell_w4, (AppendTo,)),
    ("MB0-W5", "write.restart_committed.rows", cell_w5, ()),
    ("MB0-W6", "write.restart_uncommitted.rows", cell_w6, (fail_when_flagged,)),
    ("MB0-W7", "write.summary_keys.summary", cell_w7, ()),
    ("MB0-W8", "write.no_checkpoint.answer", cell_w8, ()),
    ("MB0-T1", "trigger.once.answer", cell_t1, (once_run,)),
    ("MB0-T2", "trigger.available_now.batches", cell_t2, ()),
    ("MB0-T3", "trigger.progress.json", cell_t3, ()),
)


def scrub(text: str, bench: Bench) -> str:
    return (
        text.replace(str(bench.checkpoints), "$CHECKPOINTS")
        .replace(str(bench.warehouse), "$WAREHOUSE")
        .replace(str(Path(__file__).resolve()), "$RECORDER")
    )


def statement_of(cell: Callable[..., Any], helpers: tuple[Any, ...]) -> str:
    return "\n".join(inspect.getsource(part) for part in (cell, *helpers))


def selected_cells() -> tuple[tuple[str, str, Cell, tuple[Any, ...]], ...]:
    wanted = {part.strip() for part in os.environ.get("MB0_CELLS", "").split(",") if part.strip()}
    if not wanted:
        return CELLS
    unknown = wanted - {cell_id for cell_id, *_ in CELLS}
    if unknown:
        raise SystemExit(f"MB0_CELLS names unknown cells: {sorted(unknown)}")
    return tuple(entry for entry in CELLS if entry[0] in wanted)


def record_all(
    bench: Bench, cells: tuple[tuple[str, str, Cell, tuple[Any, ...]], ...] = CELLS
) -> dict[str, dict[str, Any]]:
    bench.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
    recorded: dict[str, dict[str, Any]] = {}
    for cell_id, field, cell, helpers in cells:
        kind, answer = cell(bench)
        entry = {
            "cell": cell_id,
            "statement": statement_of(cell, helpers),
            "kind": kind,
            "answer": answer,
            "field": field,
        }
        recorded[cell_id] = json.loads(scrub(json.dumps(entry, sort_keys=True), bench))
        print(json.dumps(recorded[cell_id], sort_keys=True), flush=True)
    return recorded


def preamble(spark: SparkSession) -> dict[str, Any]:
    build = spark._jvm.org.apache.iceberg.IcebergBuild
    return {
        "spark": spark.version,
        "iceberg": build.version(),
        "iceberg_full": build.fullVersion(),
        "catalog": "hadoop",
        "catalog_cache_enabled": spark.conf.get("spark.sql.catalog.local.cache-enabled"),
        "master": spark.sparkContext.master,
        "python": sys.version.split()[0],
        "paths": {
            "$WAREHOUSE": "the private Hadoop-catalog warehouse root of the run",
            "$CHECKPOINTS": "the private checkpoint root of the run",
            "$RECORDER": "this recorder file",
        },
    }


def fresh_dir(variable: str, prefix: str) -> Path:
    configured = os.environ.get(variable)
    path = Path(configured) if configured else Path(tempfile.mkdtemp(prefix=f"mb0-{prefix}-"))
    path.mkdir(parents=True, exist_ok=True)
    if any(path.iterdir()):
        raise SystemExit(f"{variable}={path} must be empty")
    return path.resolve()


def merged_document(
    out: Path, fresh_preamble: dict[str, Any], recorded: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    if len(recorded) == len(CELLS):
        return {
            "preamble": fresh_preamble,
            "cells": [recorded[cell_id] for cell_id, *_ in CELLS],
        }
    kept = json.loads(out.read_text())
    for key in ("spark", "iceberg_full", "catalog", "master"):
        if kept["preamble"][key] != fresh_preamble[key]:
            raise SystemExit(
                f"preamble {key} moved: {kept['preamble'][key]} != {fresh_preamble[key]}"
            )
    by_id = {entry["cell"]: entry for entry in kept["cells"]}
    by_id.update(recorded)
    missing = [cell_id for cell_id, *_ in CELLS if cell_id not in by_id]
    if missing:
        raise SystemExit(f"cells missing after the merge: {missing}")
    return {"preamble": kept["preamble"], "cells": [by_id[cell_id] for cell_id, *_ in CELLS]}


def main() -> int:
    warehouse = fresh_dir("MB0_WAREHOUSE", "warehouse")
    checkpoints = fresh_dir("MB0_CHECKPOINTS", "checkpoints")
    out = Path(os.environ.get("MB0_OUT") or Path(__file__).with_suffix(".json"))
    cells = selected_cells()
    spark = build_session(warehouse)
    spark.sparkContext.setLogLevel("ERROR")
    try:
        recorded = record_all(Bench(spark, warehouse, checkpoints), cells)
        fresh_preamble = preamble(spark)
    finally:
        spark.stop()
    document = merged_document(out, fresh_preamble, recorded)
    out.write_text(json.dumps(document, indent=2, sort_keys=True, ensure_ascii=False) + "\n")
    return 0 if len(document["cells"]) == len(CELLS) == EXPECTED_CELLS else 1


if __name__ == "__main__":
    sys.exit(main())
