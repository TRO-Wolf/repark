"""``getOrCreate`` on an existing session registers NEWLY-configured catalogs (R-GETORCREATE).

PySpark parity: Spark instantiates catalogs lazily per name, so a catalog configured by a LATER
builder works against the already-active session; an already-instantiated name keeps its
registration regardless of changed conf.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import pytest

from repark import ReparkSession


def _user_warnings(record: list[warnings.WarningMessage]) -> list[str]:
    return [str(w.message) for w in record if issubclass(w.category, UserWarning)]


def test_new_catalog_on_existing_session_registers_without_warning(tmp_path: Path) -> None:
    spark = ReparkSession.builder.getOrCreate()
    try:
        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            reused = (
                ReparkSession.builder.config("spark.sql.catalog.late_cat.type", "memory")
                .config("spark.sql.catalog.late_cat.warehouse", str(tmp_path / "wh"))
                .getOrCreate()
            )
        assert reused is spark, "reuse path must hand back the active session"
        assert not _user_warnings(record), _user_warnings(record)
        reused.sql("CREATE NAMESPACE late_cat.ns")
        reused.sql("CREATE TABLE late_cat.ns.t USING iceberg AS SELECT 1 AS id UNION ALL SELECT 2")
        assert reused.sql("SELECT * FROM late_cat.ns.t").count() == 2
    finally:
        spark.stop()


def test_repeat_getorcreate_with_the_same_added_builder_does_not_rewarn(tmp_path: Path) -> None:
    spark = ReparkSession.builder.getOrCreate()
    try:
        builder = ReparkSession.builder.config("spark.sql.catalog.late_rep.type", "memory").config(
            "spark.sql.catalog.late_rep.warehouse", str(tmp_path / "wh")
        )
        builder.getOrCreate()
        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            again = builder.getOrCreate()
        assert again is spark
        assert not _user_warnings(record), (
            "the added catalog block is folded into the recorded builder config, so the SAME "
            f"builder must not re-warn: {_user_warnings(record)}"
        )
    finally:
        spark.stop()


def test_same_name_different_config_warns_and_keeps_the_original(tmp_path: Path) -> None:
    wh_original = tmp_path / "wh_original"
    spark = (
        ReparkSession.builder.config("spark.sql.catalog.keep.type", "memory")
        .config("spark.sql.catalog.keep.warehouse", str(wh_original))
        .getOrCreate()
    )
    try:
        spark.sql("CREATE NAMESPACE keep.ns")
        spark.sql("CREATE TABLE keep.ns.t USING iceberg AS SELECT 42 AS answer")

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            reused = (
                ReparkSession.builder.config("spark.sql.catalog.keep.type", "memory")
                .config("spark.sql.catalog.keep.warehouse", str(tmp_path / "wh_other"))
                .getOrCreate()
            )
        assert reused is spark
        messages = _user_warnings(record)
        assert messages and "some configuration may not apply" in messages[0], messages
        assert "already-registered catalogs keep their configuration" in messages[0], messages
        # The ORIGINAL registration still serves — no silent re-point at the new warehouse.
        assert reused.sql("SELECT * FROM keep.ns.t").collect()[0]["answer"] == 42
    finally:
        spark.stop()


def test_malformed_late_catalog_block_raises_like_the_build_path(tmp_path: Path) -> None:
    spark = ReparkSession.builder.getOrCreate()
    try:
        with pytest.raises(Exception, match="warehouse"):
            # memory kind REQUIRES a warehouse — same loud failure class as at build.
            ReparkSession.builder.config("spark.sql.catalog.broken.type", "memory").getOrCreate()
    finally:
        spark.stop()
