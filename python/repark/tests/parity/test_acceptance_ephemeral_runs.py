"""AWS-free pins for the run-owned table prefix of the aws-acceptance legs.

pins: aws-ephemeral-runs-1/C-001, C-002, C-003
"""

from __future__ import annotations

import pathlib

import pytest
from _acceptance import LOCAL_RUN_ID, RUN_ID_ENV, run_table_prefix

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
