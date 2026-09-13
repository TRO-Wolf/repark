from __future__ import annotations

from pathlib import Path

import pytest

import repark
from repark import ReparkSession
from repark.errors import PySparkException, UnsupportedOperationException


def _write_source_config(tmp_path: Path, *, auto_register: bool | None = None) -> Path:
    lines = [
        "[default.database.postgres.company_db]",
        'host = "db.example.com"',
        'dbname = "analytics"',
        'password = "s3cr3t"',
    ]
    if auto_register is not None:
        lines.append(f"auto_register = {'true' if auto_register else 'false'}")
    path = tmp_path / "repark.toml"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _session_from(tmp_path: Path, *, auto_register: bool | None = None) -> ReparkSession:
    path = _write_source_config(tmp_path, auto_register=auto_register)
    return ReparkSession.builder.configFile(str(path)).getOrCreate()


def test_sources_lists_declared_source_with_redacted_properties(tmp_path: Path) -> None:
    spark = _session_from(tmp_path)
    try:
        rows = spark.sources()
    finally:
        spark.stop()
    assert [row.name for row in rows] == ["company_db"]
    row = rows[0]
    assert row.kind == "postgres"
    assert row.key_path == "default.database.postgres.company_db"
    assert row.auto_register is True
    assert row.properties["host"] == "db.example.com"
    assert row.properties["dbname"] == "analytics"
    assert row.properties["password"] == "***"
    assert "s3cr3t" not in repr(row.properties)


def test_source_ping_raises_connector_refusal(tmp_path: Path) -> None:
    spark = _session_from(tmp_path)
    try:
        handle = spark.source("company_db")
        assert handle.name == "company_db"
        assert handle.kind == "postgres"
        assert handle.key_path == "default.database.postgres.company_db"
        with pytest.raises(UnsupportedOperationException) as excinfo:
            handle.ping()
    finally:
        spark.stop()
    message = str(excinfo.value)
    assert "default.database.postgres.company_db" in message
    assert "postgres" in message
    assert "1.10" in message


def test_unknown_source_raises_naming_declared_sources(tmp_path: Path) -> None:
    spark = _session_from(tmp_path)
    try:
        with pytest.raises(PySparkException) as excinfo:
            spark.source("nope")
    finally:
        spark.stop()
    message = str(excinfo.value)
    assert "nope" in message
    assert "company_db" in message


def test_select_under_source_name_raises_connector_refusal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _write_source_config(tmp_path)
    monkeypatch.setenv("REPARK_CONFIG", str(path))
    monkeypatch.setattr(repark, "_ANSI_NATIVE", None)
    with pytest.raises(UnsupportedOperationException) as excinfo:
        repark.sql("SELECT * FROM company_db.public.t")
    message = str(excinfo.value)
    assert "company_db" in message
    assert "postgres" in message
    assert "1.10" in message
    assert "not found" not in message
    assert "does not exist" not in message


def test_auto_register_false_listed_not_registered(tmp_path: Path) -> None:
    spark = _session_from(tmp_path, auto_register=False)
    try:
        rows = spark.sources()
        assert [row.name for row in rows] == ["company_db"]
        assert rows[0].auto_register is False
        with pytest.raises(Exception) as excinfo:
            spark.sql("SELECT * FROM company_db.public.t").to_arrow()
    finally:
        spark.stop()
    message = str(excinfo.value)
    assert "company_db" in message
    assert "1.10" not in message


def test_list_catalogs_with_auto_registered_source(tmp_path: Path) -> None:
    spark = _session_from(tmp_path)
    try:
        catalogs = spark.catalog.listCatalogs()
    finally:
        spark.stop()
    assert catalogs == [
        ("spark_catalog", None),
    ]
