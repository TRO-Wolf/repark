"""Bucket, sort, and cluster writer layout on a local memory-catalog Iceberg table.

pins: io-bucket-cluster-1/C-002
"""

from __future__ import annotations

from pathlib import Path

from repark.errors import PySparkNotImplementedError
from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrameWriter.bucketBy",
    "DataFrameWriter.bucket_by",
    "DataFrameWriter.sortBy",
    "DataFrameWriter.sort_by",
    "DataFrameWriter.clusterBy",
    "DataFrameWriter.cluster_by",
    "DataFrameWriterV2.clusterBy",
    "DataFrameWriterV2.cluster_by",
]

BUCKET_FEATURE = "bucketBy on an Iceberg table (use writeTo(...).partitionedBy(F.bucket(n, col)))"
CLUSTER_FEATURE = "clusterBy on an Iceberg table"


def expect(label: str, got: object, wanted: object) -> None:
    if got != wanted:
        raise SystemExit(f"{label} {got!r} != {wanted!r}")


def expect_not_implemented(label: str, call: object, feature: str) -> None:
    try:
        call()
    except PySparkNotImplementedError as error:
        expect(f"{label} condition", error.getCondition(), "NOT_IMPLEMENTED")
        expect(f"{label} feature", (error.getMessageParameters() or {}).get("feature"), feature)
    else:
        raise SystemExit(f"{label} did not refuse with NOT_IMPLEMENTED")


def main() -> None:
    """Chain the layout setters and pin the Iceberg write refusals on one local frame."""
    repark = ReparkSession.builder.appName("ex-io-bucket-cluster").master("local[1]").getOrCreate()
    repark.register_memory_catalog("local", Path.cwd() / "ex_io_bucket_cluster_wh")
    repark.sql("CREATE NAMESPACE local.ns")
    try:
        frame = repark.sql("SELECT * FROM (VALUES ('x', 1, 2)) AS t(key, a, b)")
        writer = frame.write
        expect("bucketBy chains", writer.bucketBy(2, "a").sortBy("b") is writer, True)
        expect_not_implemented(
            "bucketBy saveAsTable",
            lambda: writer.saveAsTable("local.ns.bk"),
            BUCKET_FEATURE,
        )
        snake_writer = frame.write.bucket_by(2, ["a"]).sort_by("b")
        expect_not_implemented(
            "bucket_by saveAsTable",
            lambda: snake_writer.saveAsTable("local.ns.bk_snake"),
            BUCKET_FEATURE,
        )
        clustered = frame.write.clusterBy("a")
        expect_not_implemented(
            "clusterBy saveAsTable",
            lambda: clustered.saveAsTable("local.ns.cl"),
            CLUSTER_FEATURE,
        )
        snake_clustered = frame.write.cluster_by("a")
        expect_not_implemented(
            "cluster_by saveAsTable",
            lambda: snake_clustered.saveAsTable("local.ns.cl_snake"),
            CLUSTER_FEATURE,
        )
        v2_writer = frame.writeTo("local.ns.v2c").clusterBy("a")
        expect_not_implemented(
            "v2 clusterBy create",
            lambda: v2_writer.create(),
            CLUSTER_FEATURE,
        )
        v2_snake = frame.writeTo("local.ns.v2c2").cluster_by("a")
        expect_not_implemented(
            "v2 cluster_by createOrReplace",
            lambda: v2_snake.createOrReplace(),
            CLUSTER_FEATURE,
        )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
