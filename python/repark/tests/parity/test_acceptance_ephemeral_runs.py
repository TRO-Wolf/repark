"""AWS-free pins for the run-owned table prefix of the aws-acceptance legs.

pins: aws-ephemeral-runs-1/C-001, C-002, C-003, C-010
"""

from __future__ import annotations

import pathlib
import re

import pytest
from _acceptance import (
    LOCAL_RUN_ID,
    RUN_ID_ENV,
    run_table_prefix,
    s3tables_catalog_config,
    s3tables_spark_catalog_config,
)

_TESTS_DIR = pathlib.Path(__file__).resolve().parent


def test_run_table_prefix_folds_the_run_id_into_the_prefix() -> None:
    assert run_table_prefix({RUN_ID_ENV: "40079497082"}) == "testing_r40079497082_"


def test_run_table_prefix_defaults_to_the_local_marker_when_the_id_is_absent_or_empty() -> None:
    assert run_table_prefix({}) == f"testing_r{LOCAL_RUN_ID}_"
    assert run_table_prefix({RUN_ID_ENV: ""}) == f"testing_r{LOCAL_RUN_ID}_"


@pytest.mark.parametrize("bad", ["abc", "12a", "1_2", "-5", " 7", "7 ", "1.0", "r5", "\u0663"])
def test_run_table_prefix_refuses_an_id_that_is_not_plain_ascii_digits(bad: str) -> None:
    with pytest.raises(ValueError, match=RUN_ID_ENV):
        run_table_prefix({RUN_ID_ENV: bad})


def test_run_table_prefix_reads_the_process_environment_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(RUN_ID_ENV, "812")
    assert run_table_prefix() == "testing_r812_"
    monkeypatch.delenv(RUN_ID_ENV)
    assert run_table_prefix() == f"testing_r{LOCAL_RUN_ID}_"


def test_run_prefixes_of_different_runs_are_never_a_prefix_of_one_another() -> None:
    first = run_table_prefix({RUN_ID_ENV: "77"})
    second = run_table_prefix({RUN_ID_ENV: "770"})
    assert not first.startswith(second)
    assert not second.startswith(first)


@pytest.mark.parametrize(
    "filename",
    [
        "test_aws_acceptance.py",
        "test_aws_acceptance_commit_unknown.py",
        "test_aws_acceptance_cross_release.py",
        "test_aws_acceptance_cross_read.py",
        "../../../dbt-repark/tests/test_aws_acceptance_gold.py",
    ],
)
def test_every_leg_names_its_tables_through_the_run_prefix(filename: str) -> None:
    source = (_TESTS_DIR / filename).read_text(encoding="utf-8")
    assert "run_table_prefix()" in source, filename
    assert "{ACCEPTANCE_TABLE_PREFIX}" not in source, filename
    assert "testing_cu_" not in source, filename
    assert "testing_xrel_" not in source, filename


_EXAMPLE_ARN = "arn:aws:s3tables:eu-west-9:123456789012:bucket/example-table-bucket"


def test_s3tables_spark_catalog_config_is_the_rest_block_with_every_key() -> None:
    block = s3tables_spark_catalog_config("s3t", _EXAMPLE_ARN)
    assert block == {
        "spark.sql.catalog.s3t": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.s3t.type": "rest",
        "spark.sql.catalog.s3t.uri": "https://s3tables.eu-west-9.amazonaws.com/iceberg",
        "spark.sql.catalog.s3t.warehouse": _EXAMPLE_ARN,
        "spark.sql.catalog.s3t.rest.sigv4-enabled": "true",
        "spark.sql.catalog.s3t.rest.signing-name": "s3tables",
        "spark.sql.catalog.s3t.rest.signing-region": "eu-west-9",
        "spark.sql.catalog.s3t.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    }


def test_s3tables_spark_catalog_config_takes_the_region_from_the_arn_only() -> None:
    other = _EXAMPLE_ARN.replace("eu-west-9", "ap-south-7")
    block = s3tables_spark_catalog_config("s3t", other)
    assert block["spark.sql.catalog.s3t.uri"] == "https://s3tables.ap-south-7.amazonaws.com/iceberg"
    assert block["spark.sql.catalog.s3t.rest.signing-region"] == "ap-south-7"
    source = (_TESTS_DIR.parent / "_acceptance.py").read_text(encoding="utf-8")
    body = source[source.index("def s3tables_spark_catalog_config") :]
    body = body[: body.index("\ndef ", 1)]
    assert not re.search(r"(us|eu|ap|sa|ca|me|af)-[a-z]+-[0-9]", body)


def test_s3tables_spark_catalog_config_leaves_the_repark_block_untouched() -> None:
    repark_block = s3tables_catalog_config("s3t", _EXAMPLE_ARN)
    assert repark_block["spark.sql.catalog.s3t.catalog-impl"].endswith("S3TablesCatalog")
    assert "spark.sql.catalog.s3t.type" not in repark_block
    assert "spark.sql.catalog.s3t.catalog-impl" not in s3tables_spark_catalog_config(
        "s3t", _EXAMPLE_ARN
    )


@pytest.mark.parametrize(
    "bad", ["", "s3://bucket", "arn:aws:s3:::bucket", "arn:aws:s3tables:us-east-2:12:bucket/x"]
)
def test_s3tables_spark_catalog_config_refuses_a_value_that_is_not_a_table_bucket_arn(
    bad: str,
) -> None:
    with pytest.raises(ValueError, match="table bucket ARN"):
        s3tables_spark_catalog_config("s3t", bad)
