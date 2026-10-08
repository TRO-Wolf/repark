"""MB-0c facade oracle: 29 Spark 4.1.2 + Iceberg 1.11.0 cells recorded verbatim."""

from __future__ import annotations

import inspect
import json
import os
import re
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from mb0_bench import (
    NAMESPACE,
    Bench,
    append,
    build_session,
    create_sink,
    create_source,
    error_of,
    progress_batches,
    schema_of,
    table_rows,
)
from mb0_streaming_oracle import AppendTo, fresh_dir, preamble, scrub
from pyspark.errors import PySparkException
from pyspark.sql import DataFrame, SparkSession

EXPECTED_CELLS = 29

TRIGGER_MILLIS_RE = re.compile(r"^ProcessingTimeTrigger\((\d+)\)$")

Cell = Callable[[Bench], tuple[str, dict[str, Any]]]


def frame_flags(frame: DataFrame) -> dict[str, Any]:
    return {"isStreaming": frame.isStreaming, "schema": schema_of(frame)}


def trigger_millis(spark: SparkSession, text: str) -> dict[str, Any]:
    rendered = spark._jvm.org.apache.spark.sql.streaming.Trigger.ProcessingTime(text).toString()
    match = TRIGGER_MILLIS_RE.match(rendered)
    if match is None:
        raise SystemExit(f"unexpected trigger rendering: {rendered}")
    return {"millis": int(match.group(1)), "rendered": rendered}


def cell_f1(bench: Bench) -> tuple[str, dict[str, Any]]:
    try:
        frame = bench.spark.readStream.load()
    except PySparkException as exc:
        return "error", error_of(exc)
    return "rows", {"unexpected_frame": frame_flags(frame)}


def cell_f2(bench: Bench) -> tuple[str, dict[str, Any]]:
    try:
        frame = bench.spark.readStream.load(str(bench.warehouse / "nothing"))
    except PySparkException as exc:
        return "error", error_of(exc)
    return "rows", {"unexpected_frame": frame_flags(frame)}


def cell_f3(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "f3")
    append(bench, source, [1, 2])
    append(bench, source, [3])
    return "rows", frame_flags(bench.spark.readStream.table(source))


def cell_f4(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "f4")
    append(bench, source, [1, 2])
    append(bench, source, [3])
    return "rows", frame_flags(bench.spark.readStream.format("ICEBERG").load(source))


def cell_f5(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "f5")
    append(bench, source, [1, 2])
    append(bench, source, [3])
    return "rows", frame_flags(bench.spark.readStream.format("Iceberg").load(source))


def cell_f6(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "f6")
    append(bench, source, [1, 2])
    append(bench, source, [3])
    return "rows", frame_flags(bench.spark.readStream.format("parquet").table(source))


def cell_f7(bench: Bench) -> tuple[str, dict[str, Any]]:
    try:
        frame = bench.spark.readStream.format("iceberg").load()
    except PySparkException as exc:
        return "error", error_of(exc)
    return "rows", {"unexpected_frame": frame_flags(frame)}


def cell_f8(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "f8")
    append(bench, source, [1, 2])
    append(bench, source, [3])
    frame = bench.spark.readStream.format("iceberg").option("path", source).load()
    return "rows", frame_flags(frame)


def cell_f9(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "f9")
    append(bench, source, [1, 2])
    append(bench, source, [3])
    sink = create_sink(bench, "f9_sink")
    query = (
        bench.spark.readStream.format("iceberg")
        .load(source)
        .writeStream.option("checkpointLocation", bench.checkpoint("f9"))
        .trigger(availableNow=True)
        .toTable(sink)
    )
    query.awaitTermination()
    return "rows", {"progress_batches": progress_batches(query), **table_rows(bench, sink)}


def cell_f10(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "f10")
    append(bench, source, [1, 2])
    append(bench, source, [3])
    sink = create_sink(bench, "f10_sink")
    query = (
        bench.spark.readStream.format("iceberg")
        .load(source)
        .writeStream.format("parquet")
        .option("checkpointLocation", bench.checkpoint("f10"))
        .trigger(availableNow=True)
        .toTable(sink)
    )
    query.awaitTermination()
    return "rows", {"progress_batches": progress_batches(query), **table_rows(bench, sink)}


def cell_f11(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "f11")
    append(bench, source, [1, 2])
    append(bench, source, [3])
    sink = create_sink(bench, "f11_sink")
    query = (
        bench.spark.readStream.format("iceberg")
        .load(source)
        .writeStream.format("bogusfmt")
        .option("checkpointLocation", bench.checkpoint("f11"))
        .trigger(availableNow=True)
        .toTable(sink)
    )
    query.awaitTermination()
    return "rows", {"progress_batches": progress_batches(query), **table_rows(bench, sink)}


def cell_f12(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "f12")
    append(bench, source, [1, 2])
    append(bench, source, [3])
    sink = create_sink(bench, "f12_sink")
    query = (
        bench.spark.readStream.format("iceberg")
        .load(source)
        .writeStream.format("ICEBERG")
        .option("checkpointLocation", bench.checkpoint("f12"))
        .trigger(availableNow=True)
        .toTable(sink)
    )
    query.awaitTermination()
    return "rows", {"progress_batches": progress_batches(query), **table_rows(bench, sink)}


def cell_f13(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "f13")
    append(bench, source, [1, 2])
    append(bench, source, [3])
    stream = bench.spark.readStream.format("iceberg").load(source)
    try:
        query = (
            stream.writeStream.option("checkpointLocation", bench.checkpoint("f13"))
            .trigger(availableNow=True)
            .start()
        )
    except PySparkException as exc:
        return "error", error_of(exc)
    query.stop()
    return "rows", {"unexpected_start": True}


def cell_f14(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "f14")
    append(bench, source, [1, 2])
    append(bench, source, [3])
    out = bench.checkpoints / "f14_out"
    query = (
        bench.spark.readStream.format("iceberg")
        .load(source)
        .writeStream.option("checkpointLocation", bench.checkpoint("f14"))
        .trigger(availableNow=True)
        .start(str(out))
    )
    query.awaitTermination()
    files = sum(1 for _ in out.glob("*.parquet"))
    rows = bench.spark.read.parquet(str(out)).count()
    return "rows", {"progress_batches": progress_batches(query), "rows": rows, "files": files}


def cell_f15(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "f15")
    append(bench, source, [1, 2])
    append(bench, source, [3])
    try:
        frame = bench.spark.readStream.format("parquet").load(source)
    except PySparkException as exc:
        return "error", error_of(exc)
    return "rows", {"unexpected_frame": frame_flags(frame)}


def cell_t1(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "t1")
    append(bench, source, [1])
    stream = bench.spark.readStream.format("iceberg").load(source)
    texts = [
        "5 seconds",
        "1 minute",
        "0 seconds",
        "100 milliseconds",
        "1 hour 30 minutes",
        "interval 5 seconds",
        "INTERVAL '5' SECOND",
        "5 secs",
        "5",
        "bogus",
        "-1 seconds",
        "1 month",
        "1 year",
        "1.5 seconds",
        "PT5S",
        "5 Seconds",
        " 5 seconds ",
        "1 day",
        "1 week",
        "5s",
        "5 microseconds",
        "0",
        "1 second",
        "1 hour",
        "1 millisecond",
        "1 microsecond",
        "1 nanosecond",
        "2 hours",
        "500 milliseconds",
        "2 days 3 hours 4 minutes 5 seconds 6 milliseconds 7 microseconds",
        "1.5 minutes",
        "0.5 seconds",
        "0.0005 seconds",
        "+5 seconds",
        "5 seconds -1 minute",
        "-0 seconds",
        "1 month 1 second",
        "2 years",
        "1.5 months",
        "INTERVAL 5 seconds",
        "interval interval 5 seconds",
        "1 nanos",
        "1 millis",
        "1 micros",
        "1 ms",
        "1 us",
        "1 ns",
        "1 hr",
        "1 min",
        "1 sec",
        "1 d",
        "1 w",
        "1 m",
        "1 s",
        "1 y",
        "5 millisecondss",
        "seconds",
        "5.5.5 seconds",
        "0 days",
        "100000 days",
        "106751 days",
        "106752 days",
        "999999999999 days",
        "1.5 hours",
        "1.5 days",
        "1.5 milliseconds",
        "1.5 microseconds",
        "1 minute 30.5 seconds",
        "0.5 milliseconds",
    ]
    outcomes: dict[str, dict[str, Any]] = {}
    for text in texts:
        trigger_outcome: str | dict[str, Any] = "accepted"
        try:
            stream.writeStream.format("iceberg").trigger(processingTime=text)
        except PySparkException as exc:
            trigger_outcome = error_of(exc)
        try:
            parsed_outcome: dict[str, Any] = trigger_millis(bench.spark, text)
        except PySparkException as exc:
            parsed_outcome = error_of(exc)
        outcomes[text] = {"trigger": trigger_outcome, "parsed": parsed_outcome}
    return "rows", {"strings": outcomes}


def cell_t1b(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "t1b")
    append(bench, source, [1])
    stream = bench.spark.readStream.format("iceberg").load(source)
    texts = [
        "2147483647 days",
        "2147483648 days",
        "106751991167 days",
        "3000000000 hours",
        "2200000000 hours",
        "2600000000 hours",
        "3000000000 minutes",
        "200000000000 minutes",
        "3000000000 seconds",
        "10000000000000 seconds",
        "10000000000000000000 milliseconds",
        "10000000000000000000000 microseconds",
        "3000000000 months",
        "2147483648 months",
        "3000000000 weeks",
        "306783378 weeks",
        "306783379 weeks",
        "100000000 days 200000 hours",
        "-100000000 days -200000 hours",
        "106700000 days",
        "2000000 hours",
        "106700000 days 2000000 hours",
        "-106700000 days -2000000 hours",
        "1 day 2600000000 hours",
        "1 day 2147483648 days",
        "2600000000.5 hours",
        "9999999999999999999.5 seconds",
        "2600000000 hours bogus",
        "bogus 2600000000 hours",
        "interval",
        "interval 5",
        "5  seconds",
        "5\tseconds",
        "- 5 seconds",
        "+ 5 seconds",
        ".5 seconds",
        "5. seconds",
        "-0.0005 seconds",
        "0 months",
        "1 second 1 second",
        "1 second 1 day",
        "５ seconds",  # noqa: RUF001 — the fullwidth digit is the probed input
        "5 séconds",
    ]
    outcomes: dict[str, dict[str, Any]] = {}
    for text in texts:
        trigger_outcome: str | dict[str, Any] = "accepted"
        try:
            stream.writeStream.format("iceberg").trigger(processingTime=text)
        except PySparkException as exc:
            trigger_outcome = error_of(exc)
        try:
            parsed_outcome: dict[str, Any] = trigger_millis(bench.spark, text)
        except PySparkException as exc:
            parsed_outcome = error_of(exc)
        outcomes[text] = {"trigger": trigger_outcome, "parsed": parsed_outcome}
    return "rows", {"strings": outcomes}


def cell_t2(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "t2")
    append(bench, source, [1])
    stream = bench.spark.readStream.format("iceberg").load(source)
    calls: list[tuple[str, dict[str, Any]]] = [
        ("none", {}),
        ("two", {"once": True, "availableNow": True}),
        ("once_false", {"once": False}),
        ("once_zero", {"once": 0}),
        ("available_now_false", {"availableNow": False}),
        ("available_now_zero", {"availableNow": 0}),
        ("processing_time_none", {"processingTime": None}),
        ("processing_time_int", {"processingTime": 5}),
        ("processing_time_blank", {"processingTime": "  "}),
        ("processing_time_empty", {"processingTime": ""}),
        ("continuous_empty", {"continuous": ""}),
    ]
    outcomes: dict[str, Any] = {}
    for name, kwargs in calls:
        try:
            stream.writeStream.format("iceberg").trigger(**kwargs)
            outcomes[name] = "accepted"
        except PySparkException as exc:
            outcomes[name] = error_of(exc)
    return "rows", {"calls": outcomes}


def cell_t3(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "t3")
    append(bench, source, [1, 2])
    append(bench, source, [3])
    stream = bench.spark.readStream.format("iceberg").load(source)
    bogus: str | dict[str, Any] = "accepted"
    try:
        stream.writeStream.format("iceberg").trigger(continuous="bogus")
    except PySparkException as exc:
        bogus = error_of(exc)
    sink = create_sink(bench, "t3_sink")
    started: Any = None
    table_outcome: str | dict[str, Any] = "accepted"
    try:
        started = (
            stream.writeStream.trigger(continuous="5 seconds")
            .option("checkpointLocation", bench.checkpoint("t3"))
            .toTable(sink)
        )
    except PySparkException as exc:
        table_outcome = error_of(exc)
    finally:
        if started is not None:
            started.stop()
    return "rows", {"continuous_bogus": bogus, "continuous_iceberg_toTable": table_outcome}


def cell_o1(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "o1")
    append(bench, source, [1])
    stream = bench.spark.readStream.format("iceberg").load(source)
    try:
        stream.writeStream.format("iceberg").outputMode("bogus")
    except PySparkException as exc:
        return "error", error_of(exc)
    return "rows", {"unexpected_accept": True}


def cell_o2(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "o2")
    append(bench, source, [1])
    stream = bench.spark.readStream.format("iceberg").load(source)
    try:
        stream.writeStream.format("iceberg").outputMode("")
    except PySparkException as exc:
        return "error", error_of(exc)
    return "rows", {"unexpected_accept": True}


def cell_d1(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "d1")
    append(bench, source, [1, 2])
    append(bench, source, [3])
    stream = bench.spark.readStream.format("iceberg").load(source)
    try:
        rows = stream.collect()
    except PySparkException as exc:
        return "error", error_of(exc)
    return "rows", {"unexpected_rows": sorted([list(row) for row in rows])}


def cell_d2(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "d2")
    append(bench, source, [1, 2])
    append(bench, source, [3])
    sink = create_sink(bench, "d2_sink")
    query = (
        bench.spark.readStream.format("iceberg")
        .load(source)
        .writeStream.foreachBatch(AppendTo(sink))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return "rows", {"progress_batches": progress_batches(query), **table_rows(bench, sink)}


def cell_m1(bench: Bench) -> tuple[str, dict[str, Any]]:
    try:
        found = bench.spark.streams.get("bogus")
    except PySparkException as exc:
        return "error", error_of(exc)
    return "rows", {"unexpected_result": repr(found)}


def cell_m2(bench: Bench) -> tuple[str, dict[str, Any]]:
    found = bench.spark.streams.get("00000000-0000-0000-0000-000000000000")
    return "rows", {"result": None if found is None else repr(found)}


def cell_m3(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "m3")
    append(bench, source, [1, 2])
    append(bench, source, [3])
    sink = create_sink(bench, "m3_sink")
    first = (
        bench.spark.readStream.format("iceberg")
        .load(source)
        .writeStream.queryName("mb0c_dup")
        .option("checkpointLocation", bench.checkpoint("m3_first"))
        .trigger(processingTime="1 hour")
        .toTable(sink)
    )
    try:
        second = (
            bench.spark.readStream.format("iceberg")
            .load(source)
            .writeStream.queryName("mb0c_dup")
            .option("checkpointLocation", bench.checkpoint("m3_second"))
            .trigger(availableNow=True)
            .toTable(create_sink(bench, "m3_sink2"))
        )
    except PySparkException as exc:
        first.stop()
        return "error", error_of(exc)
    first.stop()
    second.stop()
    return "rows", {"unexpected_second": True}


def cell_w1(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "w1")
    append(bench, source, [1, 2])
    append(bench, source, [3])
    sink = bench.table("w1_sink")
    query = (
        bench.spark.readStream.format("iceberg")
        .load(source)
        .writeStream.option("checkpointLocation", bench.checkpoint("w1"))
        .trigger(availableNow=True)
        .toTable(sink)
    )
    query.awaitTermination()
    return "rows", {"progress_batches": progress_batches(query), **table_rows(bench, sink)}


def cell_w2(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "w2")
    append(bench, source, [1, 2])
    append(bench, source, [3])
    sink = create_sink(bench, "w2_sink")
    query = (
        bench.spark.readStream.format("iceberg")
        .load(source)
        .writeStream.option("checkpointLocation", bench.checkpoint("w2"))
        .trigger(availableNow=True)
        .toTable(sink, partitionBy="k")
    )
    query.awaitTermination()
    return "rows", {"progress_batches": progress_batches(query), **table_rows(bench, sink)}


def cell_w3(bench: Bench) -> tuple[str, dict[str, Any]]:
    source = create_source(bench, "w3")
    append(bench, source, [1, 2])
    append(bench, source, [3])
    stream = bench.spark.readStream.format("iceberg").load(source)
    sink = create_sink(bench, "w3_sink")
    try:
        query = (
            stream.writeStream.option("checkpointLocation", "")
            .trigger(availableNow=True)
            .toTable(sink)
        )
    except PySparkException as exc:
        return "error", error_of(exc)
    query.stop()
    return "rows", {"unexpected_start": True}


CELLS: tuple[tuple[str, str, Cell, tuple[Any, ...]], ...] = (
    ("MB0c-F1", "facade.format.load_no_format_no_path.error", cell_f1, (frame_flags,)),
    ("MB0c-F2", "facade.format.load_no_format_missing_path.error", cell_f2, (frame_flags,)),
    ("MB0c-F3", "facade.format.table_no_format.rows", cell_f3, (frame_flags,)),
    ("MB0c-F4", "facade.format.load_uppercase_format.rows", cell_f4, (frame_flags,)),
    ("MB0c-F5", "facade.format.load_mixedcase_format.rows", cell_f5, (frame_flags,)),
    ("MB0c-F6", "facade.format.table_parquet_format_ignored.rows", cell_f6, (frame_flags,)),
    ("MB0c-F7", "facade.format.load_iceberg_no_path.error", cell_f7, (frame_flags,)),
    ("MB0c-F8", "facade.format.load_iceberg_path_option.rows", cell_f8, (frame_flags,)),
    ("MB0c-F9", "facade.format.totable_no_format.rows", cell_f9, ()),
    ("MB0c-F10", "facade.format.totable_parquet_format_ignored.rows", cell_f10, ()),
    ("MB0c-F11", "facade.format.totable_unknown_format_ignored.rows", cell_f11, ()),
    ("MB0c-F12", "facade.format.totable_uppercase_format.rows", cell_f12, ()),
    ("MB0c-F13", "facade.format.start_no_format_no_path.error", cell_f13, ()),
    ("MB0c-F14", "facade.format.start_no_format_path_runs.rows", cell_f14, ()),
    (
        "MB0c-F15",
        "facade.format.load_parquet_format_existing_table.error",
        cell_f15,
        (frame_flags,),
    ),
    ("MB0c-T1", "facade.trigger.processing_time_strings.answer", cell_t1, (trigger_millis,)),
    (
        "MB0c-T1B",
        "facade.trigger.overflow_and_edge_strings.answer",
        cell_t1b,
        (trigger_millis,),
    ),
    ("MB0c-T2", "facade.trigger.client_checks.answer", cell_t2, ()),
    ("MB0c-T3", "facade.trigger.continuous.answer", cell_t3, ()),
    ("MB0c-O1", "facade.output_mode.unknown.error", cell_o1, ()),
    ("MB0c-O2", "facade.output_mode.empty.error", cell_o2, ()),
    ("MB0c-D1", "facade.batch_action.collect_on_stream.error", cell_d1, ()),
    ("MB0c-D2", "facade.foreach_batch.no_checkpoint_runs.rows", cell_d2, (AppendTo,)),
    ("MB0c-M1", "facade.manager.get_malformed_id.error", cell_m1, ()),
    ("MB0c-M2", "facade.manager.get_unknown_id.rows", cell_m2, ()),
    ("MB0c-M3", "facade.manager.duplicate_query_name.error", cell_m3, ()),
    ("MB0c-W1", "facade.writer.totable_missing_table_creates.rows", cell_w1, ()),
    ("MB0c-W2", "facade.writer.totable_partition_by_ignored.rows", cell_w2, ()),
    ("MB0c-W3", "facade.writer.empty_checkpoint.error", cell_w3, ()),
)


def selected_cells() -> tuple[tuple[str, str, Cell, tuple[Any, ...]], ...]:
    wanted = {part.strip() for part in os.environ.get("MB0C_CELLS", "").split(",") if part.strip()}
    if not wanted:
        return CELLS
    unknown = wanted - {cell_id for cell_id, *_ in CELLS}
    if unknown:
        raise SystemExit(f"MB0C_CELLS names unknown cells: {sorted(unknown)}")
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
            "statement": "\n".join(inspect.getsource(part) for part in (cell, *helpers)),
            "kind": kind,
            "answer": answer,
            "field": field,
        }
        recorded[cell_id] = json.loads(scrub(json.dumps(entry, sort_keys=True), bench))
        print(json.dumps(recorded[cell_id], sort_keys=True), flush=True)
    return recorded


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
