"""registry-16b-1 pins: conf unset, WAP keys and the JDBC format door answer main today.

pins: registry-16b-1/C-001, C-002, C-003
"""

from __future__ import annotations

import pytest

from repark import SparkSession
from repark.errors import IllegalArgumentException


def _session(app_name: str) -> SparkSession:
    """Build a plain facade session for one scenario."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def _seeded_session(app_name: str) -> SparkSession:
    """Build a facade session with ``spark.sql.shuffle.partitions`` seeded to 7."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "7")
        .getOrCreate()
    )


def test_conf_unset_builder_sql_conf_raises_on_get() -> None:
    """conf.unset hides the value; conf.get raises is-not-set. pins: registry-16b-1/C-001"""
    spark = _seeded_session("registry-16b-1-unset-raises")
    spark.conf.set("spark.sql.shuffle.partitions", "3")
    spark.conf.unset("spark.sql.shuffle.partitions")
    with pytest.raises(Exception, match="is not set"):
        spark.conf.get("spark.sql.shuffle.partitions")
    spark.stop()


def test_sql_reset_restores_builder_value() -> None:
    """SQL RESET after a conf.set restores the builder value. pins: registry-16b-1/C-001"""
    spark = _seeded_session("registry-16b-1-reset-restores")
    spark.conf.set("spark.sql.shuffle.partitions", "3")
    spark.sql("RESET spark.sql.shuffle.partitions")
    assert spark.conf.get("spark.sql.shuffle.partitions") == "7"
    spark.stop()


def test_conf_wap_keys_store_and_report_modifiable() -> None:
    """WAP keys store and isModifiable answers True. pins: registry-16b-1/C-002"""
    spark = _session("registry-16b-1-wap-conf")
    spark.conf.set("spark.wap.branch", "b1")
    assert spark.conf.get("spark.wap.branch") == "b1"
    spark.conf.set("spark.wap.id", "i1")
    assert spark.conf.get("spark.wap.id") == "i1"
    assert spark.conf.isModifiable("spark.wap.branch") is True
    spark.stop()


def test_sql_set_wap_branch_raises() -> None:
    """SQL SET of a WAP key raises instead of the pair row. pins: registry-16b-1/C-002"""
    spark = _session("registry-16b-1-wap-sql-set")
    with pytest.raises(Exception, match="spark"):
        spark.sql("SET spark.wap.branch=b2")
    spark.stop()


def test_format_jdbc_non_postgres_url_is_not_the_declared_refusal() -> None:
    """Format-door jdbc on a mysql url is not the declared refusal. pins: registry-16b-1/C-003"""
    spark = _session("registry-16b-1-jdbc-format")
    reader = spark.read.format("jdbc").option("url", "jdbc:mysql://127.0.0.1:1/db")
    reader = reader.option("dbtable", "t")
    with pytest.raises(Exception) as caught:
        reader.load()
    assert not str(caught.value).startswith("[NOT_IMPLEMENTED] jdbc")
    with pytest.raises(Exception) as declared:
        spark.read.jdbc("jdbc:mysql://127.0.0.1:1/db", "t")
    message = str(declared.value)
    assert "[NOT_IMPLEMENTED]" in message
    assert "jdbc" in message
    spark.stop()


def test_format_jdbc_missing_url_names_postgres() -> None:
    """Format-door jdbc without url names the postgres path. pins: registry-16b-1/C-003"""
    spark = _session("registry-16b-1-jdbc-missing-url")
    with pytest.raises(IllegalArgumentException) as caught:
        spark.read.format("jdbc").option("dbtable", "t").load()
    assert "format('postgres')" in str(caught.value)
    spark.stop()
