"""Offline pin for the CREATE OR REPLACE twice acceptance helper on a memory catalog."""

from __future__ import annotations

import pathlib

from _acceptance_replace import (
    assert_replace_twice_outcome,
    run_create_or_replace_twice,
)

from repark import ReparkSession


def test_create_or_replace_twice_on_memory_catalog(tmp_path: pathlib.Path) -> None:
    """The helper's measured shape holds on ``register_memory_catalog`` (no AWS, no skip)."""
    spark = ReparkSession.builder.appName("pytest-replace-twice").getOrCreate()
    spark.register_memory_catalog("mem", tmp_path)
    owned = tmp_path / "owned"
    spark.sql(f"CREATE NAMESPACE mem.ns LOCATION '{owned}'")

    outcome = run_create_or_replace_twice(spark, "mem", "ns", "replace2mem")
    assert outcome.table == "mem.ns.replace2mem"
    assert_replace_twice_outcome(outcome)
