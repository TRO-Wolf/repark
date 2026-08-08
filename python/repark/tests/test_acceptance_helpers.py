"""AWS-free unit tests for the acceptance-harness helpers — these run EVERYWHERE (no gate).

They cover the pure builders + the ``deduplicate`` transform in ``_acceptance``, and assert the
gated harness (``test_aws_acceptance.py``) carries no DROP/DELETE SQL against AWS. The gated
real-AWS test itself lives in ``test_aws_acceptance.py`` behind the ``REPARK_AWS_ACCEPTANCE`` gate.
"""

from __future__ import annotations

import pathlib

import pytest
from _acceptance import (
    ACCEPTANCE_NAMESPACE,
    ACCEPTANCE_TABLE_PREFIX,
    GLUE_WAREHOUSE,
    ICEBERG_TABLE_PROPERTIES,
    PRODUCTION_NAMESPACE,
    TARGET_FILE_SIZE_BYTES,
    acceptance_namespace_location,
    bronze_path,
    ctas_sql,
    deduplicate,
    fq_table,
    glue_catalog_config,
    merge_sql,
    s3tables_catalog_config,
)

from repark import ReparkSession

_TESTS_DIR = pathlib.Path(__file__).resolve().parent


def test_bronze_path_is_the_s3a_parquet_path() -> None:
    assert (
        bronze_path("appointment", "2026-07-01")
        == "s3a://example-bronze-bucket-v1/bronze/appointment/2026-07-01.parquet"
    )


def test_bronze_path_uses_the_s3a_scheme_not_s3() -> None:
    # The script reads bronze via s3a (Hadoop FS), distinct from the s3 warehouse scheme.
    assert bronze_path("clinic", "2026-01-01").startswith("s3a://")


def test_fq_table_is_three_part() -> None:
    assert fq_table("glue_catalog", "testing_repark_acceptance", "survey") == (
        "glue_catalog.testing_repark_acceptance.survey"
    )


def test_acceptance_namespace_location_is_under_the_glue_warehouse() -> None:
    # ADV-1: the harness creates the scratch namespace WITH this `location` (SQL `LOCATION`
    # also works since WG-5), so
    # a CTAS on the RequireExplicitLocation Glue catalog does not hit N5. The warehouse's trailing
    # slash must not double up.
    location = acceptance_namespace_location(GLUE_WAREHOUSE)
    assert location == "s3://example-warehouse/testing_repark_acceptance"
    assert location.startswith("s3://")
    assert "//testing_" not in location  # trailing-slash warehouse must not double the separator


def test_acceptance_namespace_location_appends_when_warehouse_has_no_trailing_slash() -> None:
    assert (
        acceptance_namespace_location("s3://bucket/warehouse")
        == "s3://bucket/warehouse/testing_repark_acceptance"
    )


def test_glue_catalog_config_matches_the_measured_block() -> None:
    cfg = glue_catalog_config("glue_catalog", GLUE_WAREHOUSE)
    assert cfg == {
        "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.glue_catalog.catalog-impl": ("org.apache.iceberg.aws.glue.GlueCatalog"),
        "spark.sql.catalog.glue_catalog.warehouse": "s3://example-warehouse/",
        "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    }


def test_glue_catalog_config_carries_the_glue_impl_and_s3_warehouse() -> None:
    cfg = glue_catalog_config("glue_catalog", GLUE_WAREHOUSE)
    assert cfg["spark.sql.catalog.glue_catalog.catalog-impl"].endswith("GlueCatalog")
    assert cfg["spark.sql.catalog.glue_catalog.warehouse"].startswith("s3://")


def test_s3tables_catalog_config_carries_the_arn_as_warehouse() -> None:
    # AWS-free: a DUMMY ARN (never a real one) proves the config shape. The S3 Tables catalog-impl
    # is used and the ARN is passed as `warehouse` (RePark carries it into `table_bucket_arn`).
    dummy_arn = "arn:aws:s3tables:us-east-2:000000000000:bucket/dummy-acceptance-bucket"
    cfg = s3tables_catalog_config("s3tables_catalog", dummy_arn)
    assert cfg == {
        "spark.sql.catalog.s3tables_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.s3tables_catalog.catalog-impl": (
            "org.apache.iceberg.aws.s3tables.S3TablesCatalog"
        ),
        "spark.sql.catalog.s3tables_catalog.warehouse": dummy_arn,
        "spark.sql.catalog.s3tables_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    }


def test_iceberg_table_properties_carry_the_real_block() -> None:
    # format-version 2, copy-on-write for every write mode, and a target file size.
    assert "'format-version' = 2" in ICEBERG_TABLE_PROPERTIES
    assert ICEBERG_TABLE_PROPERTIES.count("'copy-on-write'") == 3
    assert "'write.delete.mode' = 'copy-on-write'" in ICEBERG_TABLE_PROPERTIES
    assert "'write.update.mode' = 'copy-on-write'" in ICEBERG_TABLE_PROPERTIES
    assert "'write.merge.mode' = 'copy-on-write'" in ICEBERG_TABLE_PROPERTIES
    assert f"'write.target-file-size-bytes' = '{TARGET_FILE_SIZE_BYTES}'" in (
        ICEBERG_TABLE_PROPERTIES
    )


def test_ctas_sql_shape() -> None:
    table = "glue_catalog.testing_repark_acceptance.testing_appointment"
    sql = ctas_sql(table, "iv_temp_data")
    assert sql.startswith(f"CREATE TABLE IF NOT EXISTS {table}")
    assert "USING iceberg" in sql
    assert "AS SELECT * FROM iv_temp_data" in sql
    assert "'format-version' = 2" in sql


def test_merge_sql_shape_keys_on_the_id_column() -> None:
    table = "glue_catalog.testing_repark_acceptance.testing_appointment"
    sql = merge_sql(table, "iv_temp_data", "appointment_id")
    assert sql.startswith(f"MERGE INTO {table} AS Target")
    assert "ON Target.appointment_id = Source.appointment_id" in sql
    assert "WHEN MATCHED THEN UPDATE SET *" in sql
    assert "WHEN NOT MATCHED THEN INSERT *" in sql


def test_scratch_namespace_is_never_the_production_namespace() -> None:
    # A guard against a copy-paste regression aiming the harness at production silver. Namespace
    # AND created tables carry the testing_ prefix so they read as disposable in the Glue console.
    assert ACCEPTANCE_NAMESPACE == "testing_repark_acceptance"
    assert ACCEPTANCE_NAMESPACE != PRODUCTION_NAMESPACE
    assert ACCEPTANCE_TABLE_PREFIX == "testing_"


def test_the_gated_harness_has_no_drop_or_delete_against_aws() -> None:
    # Structural guard: the harness must NEVER emit a DROP/DELETE. Read the gated module's source
    # and assert no destructive SQL keyword appears (belt-and-suspenders for cleanup-is-manual).
    source = (_TESTS_DIR / "test_aws_acceptance.py").read_text(encoding="utf-8")
    upper = source.upper()
    assert "DROP TABLE" not in upper
    assert "DELETE FROM" not in upper
    assert "DROP NAMESPACE" not in upper
    # The only DROP the harness uses is dropTempView (a session-local view, never an AWS object).
    assert "DROP_TABLE" not in upper


def test_deduplicate_keeps_the_newest_row_per_id() -> None:
    # AWS-free: a memory session proves the dedup transform keeps the latest ingestion_timestamp.
    spark = ReparkSession.builder.appName("pytest-dedup").getOrCreate()
    df = spark.sql(
        "SELECT 1 AS id, 'old' AS name, TIMESTAMP '2020-01-01 00:00:00' AS ingestion_timestamp "
        "UNION ALL SELECT 1, 'new', TIMESTAMP '2020-01-02 00:00:00' "
        "UNION ALL SELECT 2, 'only', TIMESTAMP '2020-01-01 00:00:00'"
    )
    out = deduplicate(df, id_col="id")
    got = {r["id"]: r["name"] for r in out.to_arrow().to_pylist()}
    assert got == {1: "new", 2: "only"}


def test_placeholder_buckets_refuse_a_real_aws_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """The committed defaults are placeholders; `assert_real_buckets_configured` must refuse them
    by name so a credentialed job never issues signed requests against squattable global names."""
    import _acceptance

    monkeypatch.setattr(_acceptance, "BRONZE_BUCKET", _acceptance._PLACEHOLDER_BRONZE_BUCKET)
    monkeypatch.setattr(_acceptance, "GLUE_WAREHOUSE", _acceptance._PLACEHOLDER_WAREHOUSE)
    with pytest.raises(RuntimeError) as excinfo:
        _acceptance.assert_real_buckets_configured()
    message = str(excinfo.value)
    assert "REPARK_ACCEPT_BRONZE_BUCKET" in message
    assert "REPARK_ACCEPT_WAREHOUSE" in message


def test_operator_buckets_pass_the_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    """Real operator-supplied buckets (non-placeholder) satisfy the guard."""
    import _acceptance

    monkeypatch.setattr(_acceptance, "BRONZE_BUCKET", "acme-bronze-real")
    monkeypatch.setattr(_acceptance, "GLUE_WAREHOUSE", "s3://acme-warehouse-real/")
    _acceptance.assert_real_buckets_configured()  # must not raise
