"""AWS cross-read leg: live Spark reads the AWS acceptance tables.

Discovery over the scratch namespace on Glue and on S3 Tables: every
table of this run's prefix is read on both engines and compared
row-equal through the shared helper. Same module gate as the AWS legs plus
a PySpark skip. Never drops anything. Not run until the owner wires it.

pins: ice-spark-cross-read-1/C-004
"""

from __future__ import annotations

import os
import time
from typing import Any

import pytest
from _acceptance import (
    ACCEPTANCE_NAMESPACE,
    GLUE_WAREHOUSE,
    S3TABLES_CATALOG,
    SILVER_CATALOG,
    assert_glue_scratch_namespace_location,
    assert_real_buckets_configured,
    glue_catalog_config,
    run_table_prefix,
    s3tables_catalog_config,
    s3tables_spark_catalog_config,
)
from _acceptance_cross_read import (
    assert_outcomes_expected,
    cross_read_table_pair,
    format_outcome_table,
)
from _acceptance_v3 import current_metadata_location
from _oracle_pins import ICEBERG_RUNTIME_VERSION, ICEBERG_SPARK_RUNTIME_GAV

from repark import ReparkSession

pytestmark = pytest.mark.skipif(
    os.environ.get("REPARK_AWS_ACCEPTANCE") != "1",
    reason=(
        "real-AWS cross-read harness: set REPARK_AWS_ACCEPTANCE=1 to run "
        "(companion to the AWS acceptance legs — never routine development)"
    ),
)


def _discover_tables(spark: ReparkSession, catalog: str, namespace: str) -> list[str]:
    """Run-prefixed scratch table names under one namespace, non-temporary only."""
    prefix = run_table_prefix()
    arrow = spark.sql(f"SHOW TABLES IN {catalog}.{namespace}").to_arrow()
    names = arrow.column("tableName").to_pylist()
    temporary = arrow.column("isTemporary").to_pylist()
    return sorted(
        str(name)
        for name, temp in zip(names, temporary, strict=True)
        if str(name).startswith(prefix) and not temp
    )


def _spark_session_for_catalog(catalog_config: dict[str, str], app_name: str) -> Any:
    """Live Spark session with the Iceberg runtime plus the AWS bundle jars."""
    from pyspark.sql import SparkSession

    bundle = f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_RUNTIME_VERSION}"
    builder = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", f"{ICEBERG_SPARK_RUNTIME_GAV},{bundle}")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
    )
    for key, value in catalog_config.items():
        builder = builder.config(key, value)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    return session


def _metadata_file_or_none(repark: ReparkSession, table: str) -> str | None:
    """Newest metadata file of one table, or None when the log is unreadable."""
    try:
        return current_metadata_location(repark, table)
    except Exception:
        return None


def _cross_read_namespace(repark: ReparkSession, spark: Any, catalog: str, namespace: str) -> None:
    """Read every discovered scratch table on both engines and assert row-equal."""
    tables = _discover_tables(repark, catalog, namespace)
    print(f"Cross-read {catalog}.{namespace}: discovered {len(tables)} tables", flush=True)
    assert tables, f"no {run_table_prefix()} tables under {catalog}.{namespace} to cross-read"
    outcomes = []
    for number, name in enumerate(tables, start=1):
        table = f"{catalog}.{namespace}.{name}"
        print(f"Cross-read [{number}/{len(tables)}] {table}: metadata lookup", flush=True)
        started = time.monotonic()
        metadata_file = _metadata_file_or_none(repark, table)
        print(f"Cross-read {table}: engine pair", flush=True)
        outcome = cross_read_table_pair(
            repark,
            table,
            spark,
            table,
            metadata_file,
        )
        outcomes.append(outcome)
        elapsed = time.monotonic() - started
        print(f"Cross-read {table}: {outcome.status} ({elapsed:.3f}s)", flush=True)
        print(format_outcome_table([outcome]), flush=True)
    print(format_outcome_table(outcomes))
    assert_outcomes_expected(outcomes)


@pytest.mark.timeout(600)
def test_cross_read_glue_acceptance_tables() -> None:
    """Live Spark reads every Glue scratch acceptance table row-equal."""
    pytest.importorskip("pyspark")
    assert_real_buckets_configured()
    builder = ReparkSession.builder.appName("xread-aws-glue")
    for key, value in glue_catalog_config(SILVER_CATALOG, GLUE_WAREHOUSE).items():
        builder = builder.config(key, value)
    repark = builder.getOrCreate()
    try:
        assert_glue_scratch_namespace_location(repark, GLUE_WAREHOUSE)
        spark = _spark_session_for_catalog(
            glue_catalog_config(SILVER_CATALOG, GLUE_WAREHOUSE), "xread-aws-glue-spark"
        )
        _cross_read_namespace(repark, spark, SILVER_CATALOG, ACCEPTANCE_NAMESPACE)
    finally:
        repark.stop()


@pytest.mark.timeout(600)
def test_cross_read_s3tables_acceptance_tables() -> None:
    """Live Spark reads every S3 Tables scratch acceptance table row-equal."""
    pytest.importorskip("pyspark")
    arn = os.environ.get("TABLE_BUCKET_ARN")
    if not arn:
        pytest.skip(
            "S3 Tables cross-read needs TABLE_BUCKET_ARN (a us-east-2 table-bucket ARN); "
            "absent → skip so the Glue bullet is unaffected"
        )
    assert_real_buckets_configured()
    builder = ReparkSession.builder.appName("xread-aws-s3tables")
    for key, value in s3tables_catalog_config(S3TABLES_CATALOG, arn).items():
        builder = builder.config(key, value)
    repark = builder.getOrCreate()
    try:
        spark = _spark_session_for_catalog(
            s3tables_spark_catalog_config(S3TABLES_CATALOG, arn), "xread-aws-s3tables-spark"
        )
        _cross_read_namespace(repark, spark, S3TABLES_CATALOG, ACCEPTANCE_NAMESPACE)
    finally:
        repark.stop()
