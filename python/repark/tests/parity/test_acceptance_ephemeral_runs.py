"""AWS-free pins for the run-and-leg-owned table prefix of the aws-acceptance legs.

pins: aws-ephemeral-runs-1/C-001, C-002, C-003, C-010, C-019
"""

from __future__ import annotations

import pathlib
import re

import pytest
from _acceptance import (
    LEG_ENV,
    LEGS,
    LOCAL_LEG,
    LOCAL_RUN_ID,
    RUN_ID_ENV,
    run_object_stem,
    run_table_prefix,
    s3tables_catalog_config,
    s3tables_spark_catalog_config,
)

_TESTS_DIR = pathlib.Path(__file__).resolve().parent


def test_run_table_prefix_folds_the_run_id_and_the_leg_into_the_prefix() -> None:
    environ = {RUN_ID_ENV: "40079497082", LEG_ENV: "u"}
    assert run_table_prefix(environ) == "testing_r40079497082_u_"
    assert run_object_stem(environ) == "u12s3path_r40079497082_u_"


def test_run_table_prefix_defaults_to_the_local_run_and_leg_when_absent_or_empty() -> None:
    expected = f"testing_r{LOCAL_RUN_ID}_{LOCAL_LEG}_"
    assert run_table_prefix({}) == expected
    assert run_table_prefix({RUN_ID_ENV: "", LEG_ENV: ""}) == expected
    assert run_object_stem({}) == f"u12s3path_r{LOCAL_RUN_ID}_{LOCAL_LEG}_"


@pytest.mark.parametrize("bad", ["abc", "12a", "1_2", "-5", " 7", "7 ", "1.0", "r5", "\u0663"])
def test_run_table_prefix_refuses_an_id_that_is_not_plain_ascii_digits(bad: str) -> None:
    with pytest.raises(ValueError, match=RUN_ID_ENV):
        run_table_prefix({RUN_ID_ENV: bad})


@pytest.mark.parametrize("bad", ["acceptance", "U", "xx", "a", " u", "u_", "dbt", "*"])
def test_run_table_prefix_refuses_a_leg_that_is_not_acc_u_or_x(bad: str) -> None:
    with pytest.raises(ValueError, match=LEG_ENV):
        run_table_prefix({LEG_ENV: bad})
    with pytest.raises(ValueError, match=LEG_ENV):
        run_object_stem({LEG_ENV: bad})


@pytest.mark.parametrize("leg", LEGS)
def test_run_table_prefix_reads_the_process_environment_by_default(
    monkeypatch: pytest.MonkeyPatch, leg: str
) -> None:
    monkeypatch.setenv(RUN_ID_ENV, "812")
    monkeypatch.setenv(LEG_ENV, leg)
    assert run_table_prefix() == f"testing_r812_{leg}_"
    monkeypatch.delenv(RUN_ID_ENV)
    monkeypatch.delenv(LEG_ENV)
    assert run_table_prefix() == f"testing_r{LOCAL_RUN_ID}_{LOCAL_LEG}_"


def test_prefixes_of_different_runs_and_legs_are_never_a_prefix_of_one_another() -> None:
    prefixes = [
        run_table_prefix({RUN_ID_ENV: run, LEG_ENV: leg}) for run in ("77", "770") for leg in LEGS
    ]
    for first in prefixes:
        for second in prefixes:
            assert first == second or not first.startswith(second)


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


def test_the_s3_path_write_names_its_objects_through_the_run_object_stem() -> None:
    source = (_TESTS_DIR / "test_aws_acceptance.py").read_text(encoding="utf-8")
    assert "run_object_stem()" in source
    assert "/u12s3path_{" not in source


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
