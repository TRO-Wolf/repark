"""AWS-free pins for the run-and-leg-owned table prefix of the aws-acceptance legs.

pins: aws-ephemeral-runs-1/C-001, C-002, C-003, C-010, C-019, C-023, C-026
"""

from __future__ import annotations

import ast
import pathlib
import re

import pytest
from _acceptance import (
    LEG_ENV,
    LEGS,
    LOCAL_LEG,
    LOCAL_RUN_ATTEMPT,
    LOCAL_RUN_ID,
    RUN_ATTEMPT_ENV,
    RUN_ID_ENV,
    run_object_stem,
    run_table_prefix,
    s3tables_catalog_config,
    s3tables_spark_catalog_config,
)

_TESTS_DIR = pathlib.Path(__file__).resolve().parent


def test_run_table_prefix_folds_the_run_id_attempt_and_leg_into_the_prefix() -> None:
    environ = {RUN_ID_ENV: "40079497082", RUN_ATTEMPT_ENV: "2", LEG_ENV: "u"}
    assert run_table_prefix(environ) == "testing_r40079497082a2_u_"
    assert run_object_stem(environ) == "u12s3path_r40079497082a2_u_"


def test_run_table_prefix_defaults_to_the_local_triple_when_absent_or_empty() -> None:
    expected = f"testing_r{LOCAL_RUN_ID}a{LOCAL_RUN_ATTEMPT}_{LOCAL_LEG}_"
    assert run_table_prefix({}) == expected
    assert run_table_prefix({RUN_ID_ENV: "", RUN_ATTEMPT_ENV: "", LEG_ENV: ""}) == expected
    assert run_object_stem({}) == f"u12s3path_r{LOCAL_RUN_ID}a{LOCAL_RUN_ATTEMPT}_{LOCAL_LEG}_"


@pytest.mark.parametrize("bad", ["abc", "12a", "1_2", "-5", " 7", "7 ", "1.0", "r5", "\u0663"])
@pytest.mark.parametrize("variable", [RUN_ID_ENV, RUN_ATTEMPT_ENV])
def test_run_table_prefix_refuses_an_id_or_attempt_that_is_not_plain_ascii_digits(
    variable: str, bad: str
) -> None:
    with pytest.raises(ValueError, match=variable):
        run_table_prefix({variable: bad})


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
    monkeypatch.setenv(RUN_ATTEMPT_ENV, "3")
    monkeypatch.setenv(LEG_ENV, leg)
    assert run_table_prefix() == f"testing_r812a3_{leg}_"
    monkeypatch.delenv(RUN_ID_ENV)
    monkeypatch.delenv(RUN_ATTEMPT_ENV)
    monkeypatch.delenv(LEG_ENV)
    assert run_table_prefix() == f"testing_r{LOCAL_RUN_ID}a{LOCAL_RUN_ATTEMPT}_{LOCAL_LEG}_"


def test_prefixes_of_different_runs_attempts_and_legs_are_never_a_prefix_of_one_another() -> None:
    prefixes = [
        run_table_prefix({RUN_ID_ENV: run, RUN_ATTEMPT_ENV: attempt, LEG_ENV: leg})
        for run in ("77", "770")
        for attempt in ("1", "10")
        for leg in LEGS
    ]
    for first in prefixes:
        for second in prefixes:
            assert first == second or not first.startswith(second)


def _scratch_literals(source: str) -> list[tuple[int, str]]:
    """Non-docstring strings beginning ``testing`` and ``+`` joins with a ``test`` operand."""
    tree = ast.parse(source)
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            first = node.body[0] if node.body else None
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
                docstrings.add(id(first.value))
    found = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in docstrings
            and node.value.startswith("testing")
        ):
            found.append((node.lineno, node.value))
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            for operand in (node.left, node.right):
                if (
                    isinstance(operand, ast.Constant)
                    and isinstance(operand.value, str)
                    and operand.value.startswith("test")
                ):
                    found.append((node.lineno, operand.value))
    return found


_LEG_FILES = [
    "test_aws_acceptance.py",
    "test_aws_acceptance_commit_unknown.py",
    "test_aws_acceptance_cross_release.py",
    "test_aws_acceptance_cross_read.py",
    "../../../dbt-repark/tests/test_aws_acceptance_gold.py",
]


@pytest.mark.parametrize("filename", _LEG_FILES)
def test_no_leg_file_holds_a_string_that_starts_with_testing_outside_a_docstring(
    filename: str,
) -> None:
    source = (_TESTS_DIR / filename).read_text(encoding="utf-8")
    found = [f"{filename}:{line}: {text!r}" for line, text in _scratch_literals(source)]
    assert not found, found


@pytest.mark.parametrize("filename", _LEG_FILES)
def test_every_leg_file_names_scratch_tables_through_the_run_prefix(filename: str) -> None:
    source = (_TESTS_DIR / filename).read_text(encoding="utf-8")
    assert "run_table_prefix()" in source, filename
    assert "{ACCEPTANCE_TABLE_PREFIX}" not in source, filename


@pytest.mark.parametrize(
    "source",
    [
        'name = f"testing_mw4_mor_{token}"\n',
        'stem = "testing_" + token\n',
        'name = "testing_cu_" + door\n',
        'name = f"{prefix}x" if flag else "testing_literal"\n',
        'name = "testing" + f"_mw4_mor_{token}"\n',
        'name = "test" + "ing_" + token\n',
        'name = token + "test" + "ing_"\n',
        'name = "testing"\n',
    ],
)
def test_the_literal_scan_catches_every_spelling_of_a_hard_coded_scratch_name(
    source: str,
) -> None:
    assert _scratch_literals(source)


def test_the_literal_scan_allows_the_prefix_function_and_docstrings() -> None:
    clean = (
        '"""Module about testing_r1a1_acc_ names."""\n'
        "def leg():\n"
        '    """Makes testing_x tables."""\n'
        '    return f"{run_table_prefix()}mw4_{token}"\n'
    )
    assert _scratch_literals(clean) == []


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


@pytest.mark.parametrize("partition", ["aws-cn", "aws-us-gov", "aws-iso", "gcp"])
def test_s3tables_spark_catalog_config_refuses_a_partition_other_than_aws(partition: str) -> None:
    arn = _EXAMPLE_ARN.replace("arn:aws:", f"arn:{partition}:")
    with pytest.raises(ValueError, match="partition"):
        s3tables_spark_catalog_config("s3t", arn)
