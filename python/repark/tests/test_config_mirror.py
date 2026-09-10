from __future__ import annotations

import tomllib
from pathlib import Path

import pytest
from pydantic import ValidationError

from repark import ReparkSession
from repark.config import DisplayConfig, ProfileConfig, ReparkConfig, SessionConfig


def test_unknown_display_key_refuses() -> None:
    with pytest.raises(ValidationError):
        DisplayConfig.model_validate({"style": "spark", "nonesuch": "x"})


def test_unknown_session_key_refuses() -> None:
    with pytest.raises(ValidationError):
        SessionConfig.model_validate({"batch_size": 8, "nonesuch": 1})


def test_session_digit_string_accepted() -> None:
    parsed = SessionConfig.model_validate({"batch_size": "4096"})
    assert parsed.batch_size == "4096"


def test_session_negative_knob_refuses() -> None:
    with pytest.raises(ValidationError):
        SessionConfig.model_validate({"batch_size": -1})


def test_boolean_values_refuse() -> None:
    with pytest.raises(ValidationError):
        DisplayConfig.model_validate({"max_rows": True})
    with pytest.raises(ValidationError):
        SessionConfig.model_validate({"batch_size": True})
    with pytest.raises(ValidationError):
        ProfileConfig.model_validate({"conf": {"flag": False}})


def test_unknown_database_kind_refuses() -> None:
    with pytest.raises(ValidationError):
        ProfileConfig.model_validate({"database": {"hive": {"main": {"host": "h"}}}})


def test_catalog_name_with_dot_refuses() -> None:
    with pytest.raises(ValidationError):
        ProfileConfig.model_validate({"catalog": {"a.b": {"type": "memory"}}})


def test_empty_catalog_block_refuses() -> None:
    with pytest.raises(ValidationError):
        ProfileConfig.model_validate({"catalog": {"ghost": {}}})


def test_name_collision_across_families_refuses() -> None:
    with pytest.raises(ValidationError):
        ProfileConfig.model_validate(
            {
                "catalog": {"acme": {"type": "memory"}},
                "database": {"postgres": {"acme": {"host": "h"}}},
            }
        )


def test_rendered_toml_parses_to_same_tables() -> None:
    built = ReparkConfig(
        profiles={
            "default": ProfileConfig(
                display=DisplayConfig(style="spark", max_rows=20),
                session=SessionConfig(batch_size=4096, target_partitions=3),
                conf={"example.probe.key": "from-file"},
                catalog={"local": {"type": "memory", "warehouse": "/tmp/wh"}},
            ),
            "prod": ProfileConfig(conf={"example.probe.key": "from-prod"}),
        }
    )
    parsed = tomllib.loads(built.to_toml())
    assert parsed["default"]["display"] == {"style": "spark", "max_rows": 20}
    assert parsed["default"]["session"] == {"batch_size": 4096, "target_partitions": 3}
    assert parsed["default"]["conf"] == {"example": {"probe": {"key": "from-file"}}}
    assert parsed["default"]["catalog"] == {"local": {"type": "memory", "warehouse": "/tmp/wh"}}
    assert parsed["prod"]["conf"] == {"example": {"probe": {"key": "from-prod"}}}


def test_rendered_file_builds_session_with_file_values(tmp_path: Path) -> None:
    built = ReparkConfig(
        profiles={
            "default": ProfileConfig(
                display=DisplayConfig(style="spark"),
                session=SessionConfig(batch_size=4096, target_partitions=3),
                conf={"example.probe.key": "from-file"},
            )
        }
    )
    path = built.save(tmp_path / "repark.toml")
    spark = ReparkSession.builder.configFile(str(path)).getOrCreate()
    try:
        assert spark.conf.get("example.probe.key") == "from-file"
        assert spark.display_style == "spark"
    finally:
        spark.stop()


def test_rendered_database_source_refuses_at_load_until_cfg_2(tmp_path: Path) -> None:
    built = ReparkConfig(
        profiles={
            "default": ProfileConfig(
                database={"postgres": {"company_db": {"host": "db.example.com"}}}
            )
        }
    )
    path = built.save(tmp_path / "repark.toml")
    with pytest.raises(Exception, match="CFG-2"):
        ReparkSession.builder.configFile(str(path)).getOrCreate()
