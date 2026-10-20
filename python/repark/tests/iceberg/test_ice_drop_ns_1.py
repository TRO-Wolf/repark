"""ICE-DROP-NS-1 — ``DROP NAMESPACE`` on a non-empty namespace refuses like Spark.

Oracle: ``ice_drop_ns_1_spark_oracle.json`` (live PySpark 4.1.2 +
iceberg-spark-runtime-4.1_2.13:1.11.0 over InMemory and Hadoop catalogs,
re-derived by ``_record_ice_drop_ns_1_oracle.py``). Spark refuses every
non-empty drop with ``Namespace <ns> is not empty`` (CASCADE drops nothing)
and drops an empty namespace; a missing namespace answers
``[SCHEMA_NOT_FOUND]``. Offline tier pins RePark against the fixture; the
live tier replays the generator and checks the fixture. The ANSI door
(``DROP SCHEMA``) is pinned in Rust, where that door is reachable: the
Python ``repark.sql`` callable plans catalog DDL through plain DataFusion
and never reaches the native router, so no Python spelling can pin it.

pins: ice-drop-ns-1/C-001, C-002, C-003, C-004, C-005
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark.session import _reset_active_session_for_tests

LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — the live Spark re-derivation is skipped (CI is JVM-free)"
CELLS: dict[str, Any] = {
    str(cell["id"]): cell
    for cell in json.loads(
        Path(__file__).with_name("ice_drop_ns_1_spark_oracle.json").read_text(encoding="utf-8")
    )
}
CATALOGS: tuple[str, str] = ("sc", "hc")


@pytest.fixture
def session(tmp_path: Path) -> Iterator[Any]:
    """A RePark session with two memory catalogs mirroring the oracle setup."""
    _reset_active_session_for_tests()
    active = ReparkSession.builder.appName("test-ice-drop-ns-1").getOrCreate()
    active.register_memory_catalog("sc", tmp_path / "wh")
    active.register_memory_catalog("hc", tmp_path / "hc")
    yield active
    active.stop()
    _reset_active_session_for_tests()


def _cell(cell_id: str) -> dict[str, Any]:
    """Return the oracle record for a cell id."""
    return dict(CELLS[cell_id])


def _spark_error_message(cell_id: str) -> str:
    """Return Spark's recorded error message for a refusal cell."""
    error = _cell(cell_id)["obs"]["err"]
    assert error is not None, f"{cell_id} must refuse on Spark"
    return str(dict(error)["msg"])


def _execute(active: Any, sql: str) -> None:
    """Run one statement to completion."""
    active.sql(sql).collect()


def _seed_table(active: Any, namespace: str) -> None:
    """Create a namespace holding one single-row Iceberg table."""
    _execute(active, f"CREATE NAMESPACE {namespace}")
    _execute(active, f"CREATE TABLE {namespace}.t (id INT) USING iceberg")
    _execute(active, f"INSERT INTO {namespace}.t VALUES (1)")


def _namespace_listed(active: Any, catalog: str, leaf: str) -> bool:
    """Report whether a namespace still lists on the facade door."""
    listed = active.sql(f"SHOW NAMESPACES IN {catalog} LIKE '{leaf}'").to_arrow().to_pylist()
    return any(leaf in str(row) for row in listed)


def _table_ids(active: Any, namespace: str) -> list[Any]:
    """Read the single-column ids of a namespace table on the Arrow path."""
    return list(active.sql(f"SELECT id FROM {namespace}.t").to_arrow().column("id").to_pylist())


def _refusal_text(active: Any, sql: str) -> str:
    """Run a refusing statement and return the AnalysisException text."""
    with pytest.raises(AnalysisException) as excinfo:
        active.sql(sql).collect()
    return str(excinfo.value)


def _assert_survives(active: Any, catalog: str, leaf: str, namespace: str) -> None:
    """Assert a refused drop left the namespace and its rows behind."""
    assert _namespace_listed(active, catalog, leaf), f"{namespace} must still list"
    assert _table_ids(active, namespace) == [1], f"{namespace}.t must still read [[1]]"


@pytest.mark.parametrize("catalog", CATALOGS)
def test_drop_namespace_nonempty_refuses(session: Any, catalog: str) -> None:
    """A plain non-empty drop refuses and keeps every row. pins: ice-drop-ns-1/C-002"""
    leaf = "ns_nonempty"
    namespace = f"{catalog}.{leaf}"
    _seed_table(session, namespace)
    assert "is not empty" in _spark_error_message(f"NS-NONEMPTY-{catalog.upper()}")
    text = _refusal_text(session, f"DROP NAMESPACE {namespace}")
    assert f"Namespace {leaf} is not empty." in text, text
    assert "Contains 1 table(s)." in text, text
    _assert_survives(session, catalog, leaf, namespace)


@pytest.mark.parametrize("catalog", CATALOGS)
def test_drop_namespace_cascade_refuses(session: Any, catalog: str) -> None:
    """CASCADE drops nothing on a non-empty namespace. pins: ice-drop-ns-1/C-002"""
    leaf = "ns_cascade"
    namespace = f"{catalog}.{leaf}"
    _seed_table(session, namespace)
    assert "is not empty" in _spark_error_message(f"NS-CASCADE-{catalog.upper()}")
    text = _refusal_text(session, f"DROP NAMESPACE {namespace} CASCADE")
    assert f"Namespace {leaf} is not empty." in text, text
    _assert_survives(session, catalog, leaf, namespace)


@pytest.mark.parametrize("catalog", CATALOGS)
def test_drop_namespace_restrict_refuses(session: Any, catalog: str) -> None:
    """RESTRICT refuses like the plain form. pins: ice-drop-ns-1/C-002"""
    leaf = "ns_restrict"
    namespace = f"{catalog}.{leaf}"
    _seed_table(session, namespace)
    assert "is not empty" in _spark_error_message(f"NS-RESTRICT-{catalog.upper()}")
    text = _refusal_text(session, f"DROP NAMESPACE {namespace} RESTRICT")
    assert f"Namespace {leaf} is not empty." in text, text
    _assert_survives(session, catalog, leaf, namespace)


@pytest.mark.parametrize("catalog", CATALOGS)
def test_drop_namespace_if_exists_refuses(session: Any, catalog: str) -> None:
    """IF EXISTS does not bypass the non-empty refusal. pins: ice-drop-ns-1/C-002"""
    leaf = "ns_ifexists"
    namespace = f"{catalog}.{leaf}"
    _seed_table(session, namespace)
    assert "is not empty" in _spark_error_message(f"NS-IFEXISTS-{catalog.upper()}")
    text = _refusal_text(session, f"DROP NAMESPACE IF EXISTS {namespace}")
    assert f"Namespace {leaf} is not empty." in text, text
    _assert_survives(session, catalog, leaf, namespace)


@pytest.mark.parametrize("catalog", CATALOGS)
def test_drop_namespace_if_exists_cascade_refuses(session: Any, catalog: str) -> None:
    """IF EXISTS plus CASCADE still refuses. pins: ice-drop-ns-1/C-002"""
    leaf = "ns_ifexists_cascade"
    namespace = f"{catalog}.{leaf}"
    _seed_table(session, namespace)
    assert "is not empty" in _spark_error_message(f"NS-IFEXISTS-CASCADE-{catalog.upper()}")
    text = _refusal_text(session, f"DROP NAMESPACE IF EXISTS {namespace} CASCADE")
    assert f"Namespace {leaf} is not empty." in text, text
    _assert_survives(session, catalog, leaf, namespace)


@pytest.mark.parametrize("catalog", CATALOGS)
def test_drop_database_nonempty_refuses(session: Any, catalog: str) -> None:
    """DATABASE spells the same refusal as NAMESPACE. pins: ice-drop-ns-1/C-002"""
    leaf = "ns_database"
    namespace = f"{catalog}.{leaf}"
    _seed_table(session, namespace)
    assert "is not empty" in _spark_error_message(f"NS-DATABASE-{catalog.upper()}")
    text = _refusal_text(session, f"DROP DATABASE {namespace}")
    assert f"Namespace {leaf} is not empty." in text, text
    _assert_survives(session, catalog, leaf, namespace)


@pytest.mark.parametrize("catalog", CATALOGS)
def test_drop_schema_cascade_refuses(session: Any, catalog: str) -> None:
    """SCHEMA CASCADE spells the same refusal. pins: ice-drop-ns-1/C-002"""
    leaf = "ns_schema_cascade"
    namespace = f"{catalog}.{leaf}"
    _seed_table(session, namespace)
    assert "is not empty" in _spark_error_message(f"NS-SCHEMA-CASCADE-{catalog.upper()}")
    text = _refusal_text(session, f"DROP SCHEMA {namespace} CASCADE")
    assert f"Namespace {leaf} is not empty." in text, text
    _assert_survives(session, catalog, leaf, namespace)


@pytest.mark.parametrize("catalog", CATALOGS)
@pytest.mark.parametrize("suffix", ["", " CASCADE"])
def test_drop_namespace_empty_drops(session: Any, catalog: str, suffix: str) -> None:
    """An empty namespace drops, with or without CASCADE. pins: ice-drop-ns-1/C-003"""
    leaf = "ns_empty" if not suffix else "ns_empty_cascade"
    namespace = f"{catalog}.{leaf}"
    cell_suffix = "EMPTY" if not suffix else "EMPTY-CASCADE"
    assert _cell(f"NS-{cell_suffix}-{catalog.upper()}")["obs"]["err"] is None
    _execute(session, f"CREATE NAMESPACE {namespace}")
    _execute(session, f"DROP NAMESPACE {namespace}{suffix}")
    assert not _namespace_listed(session, catalog, leaf), f"{namespace} must be gone"


@pytest.mark.parametrize("catalog", CATALOGS)
def test_drop_namespace_after_table_dropped_drops(session: Any, catalog: str) -> None:
    """A namespace emptied by an explicit table drop drops. pins: ice-drop-ns-1/C-003"""
    leaf = "ns_after_drop_table"
    namespace = f"{catalog}.{leaf}"
    assert _cell(f"NS-AFTER-DROP-TABLE-{catalog.upper()}")["obs"]["err"] is None
    _seed_table(session, namespace)
    _execute(session, f"DROP TABLE {namespace}.t")
    _execute(session, f"DROP NAMESPACE {namespace}")
    assert not _namespace_listed(session, catalog, leaf), f"{namespace} must be gone"


@pytest.mark.parametrize("catalog", CATALOGS)
def test_drop_namespace_missing_raises_schema_not_found(session: Any, catalog: str) -> None:
    """A missing namespace answers SCHEMA_NOT_FOUND. pins: ice-drop-ns-1/C-004"""
    message = _spark_error_message(f"NS-MISSING-{catalog.upper()}")
    assert "SCHEMA_NOT_FOUND" in message, message
    text = _refusal_text(session, f"DROP NAMESPACE {catalog}.ns_missing_no_such")
    assert "SCHEMA_NOT_FOUND" in text, text


@pytest.mark.parametrize("catalog", CATALOGS)
def test_drop_namespace_child_boundary(session: Any, catalog: str) -> None:
    """Nested namespaces stay out of scope and refuse loud. pins: ice-drop-ns-1/C-005"""
    for suffix in ("CHILD-NS", "CHILD-NS-CASCADE"):
        message = _spark_error_message(f"NS-{suffix}-{catalog.upper()}")
        assert "is not empty" in message, message
        if catalog == "sc":
            assert "child namespace(s)" in message, message
    leaf = "ns_child_ns"
    namespace = f"{catalog}.{leaf}"
    _execute(session, f"CREATE NAMESPACE {namespace}")
    text = _refusal_text(session, f"CREATE NAMESPACE {namespace}.c")
    assert "two-part" in text, text


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_live_oracle_fixture_reproduces(tmp_path: Path) -> None:
    """The generator re-derives the fixture on live Spark. pins: ice-drop-ns-1/C-001"""
    generator = Path(__file__).with_name("_record_ice_drop_ns_1_oracle.py")
    environ = dict(os.environ)
    environ.setdefault("JAVA_HOME", "/usr/lib/jvm/zulu-17-amd64")
    environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    completed = subprocess.run(
        [
            sys.executable,
            str(generator),
            "--warehouse",
            str(tmp_path / "live-wh"),
            "check",
        ],
        capture_output=True,
        text=True,
        env=environ,
        timeout=1200,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
