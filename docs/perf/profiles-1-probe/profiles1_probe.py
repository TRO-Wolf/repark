from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import warnings
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from repark.spark import SparkSession

WORK = Path(__file__).resolve().parent / "data"
ROWS = 200_000
GROUPS = 8

KEYS = [
    ("datafusion.optimizer.prefer_hash_join", "false", ["join"]),
    ("datafusion.execution.target_partitions", "1", ["join", "scan"]),
    ("datafusion.execution.batch_size", "3", ["range"]),
    ("datafusion.optimizer.repartition_joins", "false", ["join"]),
    ("datafusion.optimizer.repartition_aggregations", "false", ["agg"]),
    ("datafusion.optimizer.repartition_file_scans", "false", ["scan"]),
    ("datafusion.execution.parquet.pushdown_filters", "true", ["scan"]),
    ("datafusion.execution.parquet.enable_page_index", "false", ["scan"]),
    ("datafusion.execution.parquet.bloom_filter_on_read", "false", ["scan"]),
    ("datafusion.execution.coalesce_batches", "false", ["agg", "join"]),
    ("repark.scan.concurrency_limit", "4", []),
    ("repark.batch.size", "3", ["range"]),
    ("datafusion.execution.parquet.compression", "uncompressed", ["write"]),
    ("datafusion.execution.parquet.max_row_group_size", "1000", ["write"]),
    ("datafusion.execution.parquet.bloom_filter_on_write", "true", ["write"]),
    ("datafusion.execution.parquet.write_batch_size", "1000", []),
    ("write.target-file-size-bytes", "134217728", []),
    ("write.distribution-mode", "hash", []),
    ("repark.merge.file_scoped_rewrite", "true", []),
    ("repark.merge.scan_pruning", "true", []),
]

VALIDATION_PROBES = [
    ("datafusion.execution.parquet.not_a_real_key", "true"),
    ("datafusion.execution.batch_size", "notanumber"),
    ("repark.scan.concurrency_limit", "0"),
    ("repark.scan.concurrency_limit", "abc"),
    ("repark.scan.concurrency-limit", "0"),
    ("repark.merge.file_scoped_rewrite", "maybe"),
    ("repark.merge.file-scoped-rewrite", "maybe"),
    ("repark.merge.scan_pruning", "maybe"),
    ("repark.merge.scan-pruning", "maybe"),
]


def build_source_parquet(path: Path, seed: int) -> None:
    k = pa.array(range(seed, seed + ROWS), type=pa.int64())
    g = pa.array([f"g{i % GROUPS}" for i in range(ROWS)], type=pa.string())
    v = pa.array([float(i) * 1.5 for i in range(ROWS)], type=pa.float64())
    s = pa.array([f"s-{seed}-{i}" for i in range(ROWS)], type=pa.string())
    pq.write_table(pa.table({"k": k, "g": g, "v": v, "s": s}), path)


def capture_explain(frame: object) -> str:
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        frame.explain(mode="simple")
    return buffer.getvalue()


def join_plan(spark: SparkSession) -> str:
    from repark.spark.functions import col

    left = spark.read.parquet(str(WORK / "left.parquet")).select(col("k"), col("v").alias("lv"))
    right = spark.read.parquet(str(WORK / "right.parquet")).select(col("k"), col("v").alias("rw"))
    joined = left.join(right, "k").select(left["lv"], right["rw"])
    return capture_explain(joined)


def agg_plan(spark: SparkSession) -> str:
    spark.read.parquet(str(WORK / "left.parquet")).createOrReplaceTempView("probe_agg_left")
    return capture_explain(spark.sql("SELECT g, sum(v) AS total FROM probe_agg_left GROUP BY g"))


def scan_plan(spark: SparkSession) -> str:
    frame = spark.read.parquet(str(WORK / "left.parquet"))
    return capture_explain(frame.filter("v > 150000"))


def range_batches(spark: SparkSession) -> int:
    return len(list(spark.range(10).to_arrow_batches()))


def write_parquet(spark: SparkSession, path: Path) -> dict[str, object]:
    frame = spark.read.parquet(str(WORK / "left.parquet"))
    frame.write.parquet(str(path))
    return file_facts(path)


def file_facts(path: Path) -> dict[str, object]:
    target = path
    part_files = 1
    if path.is_dir():
        files = sorted(child for child in path.iterdir() if child.is_file())
        part_files = len(files)
        target = files[0]
    parquet = pq.ParquetFile(target)
    columns = [parquet.metadata.row_group(0).column(i) for i in range(parquet.metadata.num_columns)]
    codecs = sorted({str(column.compression) for column in columns})
    bloom_lengths = [getattr(column, "bloom_filter_length", None) for column in columns]
    return {
        "bytes": target.stat().st_size,
        "part_files": part_files,
        "row_groups": parquet.metadata.num_row_groups,
        "rows": parquet.metadata.num_rows,
        "codecs": codecs,
        "bloom_filter_lengths": bloom_lengths,
    }


def scrub_work_dir(plan: str) -> str:
    raw = str(WORK)
    return plan.replace(raw, "<work>").replace(raw.lstrip(os.sep), "<work>")


def collect_subjects(
    spark: SparkSession,
    write_path: Path | None = None,
) -> dict[str, object]:
    subjects: dict[str, object] = {
        "join": scrub_work_dir(join_plan(spark)),
        "agg": scrub_work_dir(agg_plan(spark)),
        "scan": scrub_work_dir(scan_plan(spark)),
        "range_batches": range_batches(spark),
    }
    if write_path is not None:
        subjects["write"] = write_parquet(spark, write_path)
    return subjects


def open_session(configs: list[tuple[str, str]]) -> SparkSession:
    builder = SparkSession.builder
    for key, value in configs:
        builder = builder.config(key, value)
    return builder.getOrCreate()


def try_build_with_warnings(configs: list[tuple[str, str]]) -> tuple[str, list[str]]:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            session = open_session(configs)
        except Exception as error:
            return "refused", [str(error)]
        texts = [str(warning.message) for warning in caught]
    session.stop()
    return "accepted", texts


def runtime_probe(session: SparkSession, key: str, value: str) -> dict[str, str | None]:
    try:
        session.conf.set(key, value)
        set_outcome = "accepted"
        set_error = None
    except Exception as error:
        set_outcome = "refused"
        set_error = str(error)
    try:
        read_back = session.conf.get(key)
    except Exception as error:
        read_back = f"<get refused: {error}>"
    return {"set": set_outcome, "set_error": set_error, "get": read_back}


def main() -> None:
    global WORK
    os.environ["REPARK_CONFIG"] = ""
    WORK = Path(tempfile.mkdtemp(prefix="profiles1-"))
    build_source_parquet(WORK / "left.parquet", 0)
    build_source_parquet(WORK / "right.parquet", 1)

    report: dict[str, object] = {"baseline": {}, "keys": {}, "validation": {}, "runtime": {}}

    session = open_session([])
    report["baseline"] = collect_subjects(session, write_path=WORK / "out_default")
    session.stop()

    for key, value, subjects in KEYS:
        entry: dict[str, object] = {"set_value": value, "subjects": subjects}
        session = open_session([(key, value)])
        entry["conf_get"] = session.conf.get(key)
        entry["conf_getall"] = session.conf.getAll.get(key)
        write_path = WORK / ("out_" + key.replace(".", "_").replace("-", "_"))
        need_write = "write" in subjects
        full = collect_subjects(session, write_path=write_path if need_write else None)
        full["range"] = full["range_batches"]
        entry["measured"] = {name: full[name] for name in subjects}
        entry["runtime_set"] = runtime_probe(session, key, value)
        session.stop()
        report["keys"][key] = entry

    for key, value in VALIDATION_PROBES:
        outcome, texts = try_build_with_warnings([(key, value)])
        report["validation"][f"{key}={value}"] = {"outcome": outcome, "detail": texts}

    for key, value in [
        ("datafusion.execution.parquet.not_a_real_key", "true"),
        ("datafusion.execution.batch_size", "notanumber"),
    ]:
        session = open_session([])
        report["runtime"][f"conf.set {key}={value}"] = runtime_probe(session, key, value)
        session.stop()

    session = open_session([])
    report["runtime"]["conf.set datafusion.execution.batch_size=5 then range(10)"] = {
        "set": runtime_probe(session, "datafusion.execution.batch_size", "5"),
        "batches_after": range_batches(session),
    }
    session.stop()

    print(json.dumps(report, indent=1, sort_keys=True))


if __name__ == "__main__":
    main()
