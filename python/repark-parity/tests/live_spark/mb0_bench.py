"""MB-0 recorder plumbing: the Spark session, the bench, and the table and stream helpers."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from pyspark.errors import PySparkException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

NAMESPACE = "local.mb0"
SOURCE_COLUMNS = "id BIGINT, k STRING"
JAR_NAME = "iceberg-spark-runtime-4.1_2.13-1.11.0.jar"
JAR_COORDINATE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
JAR_CANDIDATES = (
    Path.home() / ".ivy2" / "jars" / f"org.apache.iceberg_{JAR_NAME}",
    Path.home()
    / ".ivy2"
    / "cache"
    / "org.apache.iceberg"
    / "iceberg-spark-runtime-4.1_2.13"
    / "jars"
    / JAR_NAME,
    Path.home()
    / ".m2"
    / "repository"
    / "org"
    / "apache"
    / "iceberg"
    / "iceberg-spark-runtime-4.1_2.13"
    / "1.11.0"
    / JAR_NAME,
)
SUMMARY_COUNTS = (
    "added-data-files",
    "added-records",
    "deleted-data-files",
    "deleted-records",
    "added-delete-files",
    "added-position-deletes",
)


class Bench:
    __slots__ = ("checkpoints", "spark", "warehouse")

    def __init__(self, spark: SparkSession, warehouse: Path, checkpoints: Path) -> None:
        self.spark = spark
        self.warehouse = warehouse
        self.checkpoints = checkpoints

    def table(self, name: str) -> str:
        return f"{NAMESPACE}.{name}"

    def checkpoint(self, name: str) -> str:
        return str(self.checkpoints / name)


class Collect:
    def __init__(self) -> None:
        self.schema: list[list[Any]] = []
        self.batches: list[dict[str, Any]] = []

    def __call__(self, frame: DataFrame, batch_id: int) -> None:
        self.schema = schema_of(frame)
        self.batches.append({"batch": batch_id, "rows": rows_of(frame)})


def iceberg_jar() -> Path:
    override = os.environ.get("MB0_ICEBERG_JAR")
    candidates = (Path(override),) if override else JAR_CANDIDATES
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise SystemExit(f"missing {JAR_COORDINATE} ({JAR_NAME})")


def build_session(warehouse: Path) -> SparkSession:
    os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
    return (
        SparkSession.builder.master("local[2]")
        .appName("mb0-streaming-oracle")
        .config("spark.jars", str(iceberg_jar()))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", str(warehouse))
        .config("spark.sql.catalog.local.cache-enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def create_source(bench: Bench, name: str) -> str:
    table = bench.table(name)
    bench.spark.sql(
        f"CREATE TABLE {table} ({SOURCE_COLUMNS}) USING iceberg "
        "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')"
    )
    return table


def create_sink(bench: Bench, name: str, columns: str = SOURCE_COLUMNS, partition: str = "") -> str:
    table = bench.table(name)
    partitioned = f" PARTITIONED BY ({partition})" if partition else ""
    bench.spark.sql(f"CREATE TABLE {table} ({columns}) USING iceberg{partitioned}")
    return table


def append(bench: Bench, table: str, ids: list[int], files: int = 1) -> None:
    rows = [(i, f"k{i % 2}") for i in ids]
    frame = bench.spark.createDataFrame(rows, SOURCE_COLUMNS)
    frame.repartitionByRange(files, "id").writeTo(table).append()


def schema_of(frame: DataFrame) -> list[list[Any]]:
    return [[f.name, f.dataType.simpleString(), f.nullable] for f in frame.schema.fields]


def rows_of(frame: DataFrame) -> list[list[Any]]:
    return sorted([list(row) for row in frame.collect()])


def table_rows(bench: Bench, table: str) -> dict[str, Any]:
    frame = bench.spark.table(table)
    return {"schema": schema_of(frame), "rows": rows_of(frame)}


def snapshot_log(bench: Bench, table: str) -> list[dict[str, Any]]:
    return [
        row.asDict()
        for row in bench.spark.sql(
            f"SELECT operation, summary FROM {table}.snapshots ORDER BY committed_at"
        ).collect()
    ]


def operations(bench: Bench, table: str) -> list[dict[str, str]]:
    return [
        {
            "operation": entry["operation"],
            **{key: entry["summary"][key] for key in SUMMARY_COUNTS if key in entry["summary"]},
        }
        for entry in snapshot_log(bench, table)
    ]


def last_snapshot(bench: Bench, table: str) -> dict[str, Any]:
    return snapshot_log(bench, table)[-1]


def jvm_causes(exc: PySparkException) -> list[str]:
    chain: list[str] = []
    origin = getattr(exc, "_origin", None)
    while origin is not None and len(chain) < 8:
        chain.append(origin.getClass().getName())
        origin = origin.getCause()
    return chain


def error_of(exc: PySparkException) -> dict[str, Any]:
    return {
        "class": exc.getCondition() or type(exc).__name__,
        "exception": f"{type(exc).__module__}.{type(exc).__name__}",
        "sqlstate": exc.getSqlState(),
        "causes": jvm_causes(exc),
        "text": str(exc),
    }


def await_query(query: StreamingQuery) -> dict[str, Any] | None:
    try:
        query.awaitTermination()
    except PySparkException as exc:
        return error_of(exc)
    return None


def progress_batches(query: StreamingQuery) -> list[dict[str, int]]:
    return [
        {"batchId": progress["batchId"], "numInputRows": progress["numInputRows"]}
        for progress in (json.loads(p.json()) for p in query._jsq.recentProgress())
    ]


def checkpoint_logs(checkpoint: str, log: str) -> list[str]:
    directory = Path(checkpoint) / log
    if not directory.is_dir():
        return []
    names = [p.name for p in directory.iterdir() if p.name.isdigit()]
    return sorted(names, key=int)
