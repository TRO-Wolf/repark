from __future__ import annotations

from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import IllegalArgumentException

URL = "postgresql://u:pw@h/db"
MASKED = "postgresql://u:***@h/db"
CASE_KEY = "spark.sql.caseSensitive"
ANSI_KEY = "spark.sql.ansi.enabled"
MERGE_KEY = "spark.sql.iceberg.merge-schema"
TIMESTAMP_KEY = "spark.sql.timestampType"
BUILDER_KEYS = (CASE_KEY, ANSI_KEY, MERGE_KEY, TIMESTAMP_KEY)
CONF_SET_KEYS = (CASE_KEY, ANSI_KEY, MERGE_KEY)


def _assert_masked(message: str, key: str, *, sqlstate: bool) -> None:
    assert MASKED in message
    assert "u:pw@" not in message
    assert key in message
    if sqlstate:
        assert "[INVALID_CONF_VALUE.TYPE_MISMATCH]" in message
        assert "SQLSTATE: 22022" in message


@pytest.mark.parametrize("key", BUILDER_KEYS)
def test_builder_config_masks_a_url_password(key: str) -> None:
    with pytest.raises(IllegalArgumentException) as caught:
        ReparkSession.builder.config(key, URL).getOrCreate()
    _assert_masked(str(caught.value), key, sqlstate=key == MERGE_KEY)


def test_repark_toml_conf_masks_a_url_password(tmp_path: Path) -> None:
    path = tmp_path / "repark.toml"
    path.write_text(
        '[default.conf]\n"spark.sql.caseSensitive" = "postgresql://u:pw@h/db"\n',
        encoding="utf-8",
    )
    with pytest.raises(IllegalArgumentException) as caught:
        ReparkSession.builder.config_file(str(path)).getOrCreate()
    _assert_masked(str(caught.value), CASE_KEY, sqlstate=False)


@pytest.mark.parametrize("key", CONF_SET_KEYS)
def test_conf_set_masks_a_url_password(key: str) -> None:
    spark = ReparkSession.builder.getOrCreate()
    try:
        with pytest.raises(IllegalArgumentException) as caught:
            spark.conf.set(key, URL)
        _assert_masked(str(caught.value), key, sqlstate=True)
    finally:
        spark.stop()


def test_reset_masks_a_url_stored_as_the_ansi_builder_value() -> None:
    spark = ReparkSession.builder.config(ANSI_KEY, "true").getOrCreate()
    try:
        spark.conf.set(ANSI_KEY, "false")
        spark._builder_config[ANSI_KEY] = URL
        with pytest.raises(IllegalArgumentException) as caught:
            spark.sql(f"RESET {ANSI_KEY}").collect()
        _assert_masked(str(caught.value), ANSI_KEY, sqlstate=False)
    finally:
        spark.stop()
