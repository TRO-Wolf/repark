from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from _ice_held_frame_snapshot_1_cells import Cells
from _ice_held_frame_snapshot_1_enc_1_cells import EncCells

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

pytestmark = pytest.mark.xdist_group("ice_held_frame_snapshot_1")
ORACLE = json.loads(
    Path(__file__).with_name("ice_held_frame_snapshot_1_spark_oracle.json").read_text()
)
KEYS = [(ansi, key) for ansi in sorted(ORACLE) for key in sorted(ORACLE[ansi])]
SEED = ["(1, 'a')", "(2, 'b')"]
GROWN = [*SEED, "(9, 'z')"]
DOUBLED = ["(1, 'a')", "(1, 'a')", "(2, 'b')", "(2, 'b')"]


def _refusal(table: str) -> str:
    return (
        "ERROR UnsupportedOperationException: Table db."
        + table
        + " carries property 'encryption.key-id': RePark has no table encryption and refuses"
        " to write plaintext into a table that asks for it (ENC-1)."
    )


REFUSED = {
    "refused_sql_insert": _refusal("e1"),
    "refused_sql_delete": _refusal("e2"),
    "refused_held_append": _refusal("e3"),
    "refused_frame_insert_into": _refusal("e4"),
}
PAIRS = {
    "keyed_read": SEED,
    "keyed_view_read": SEED,
    "keyed_append": _refusal("e7"),
    "keyed_insert_into": _refusal("e8"),
    "keyed_sql_from_view": _refusal("e9"),
    "key_added_read": GROWN,
    "key_added_append": _refusal("e11"),
    "key_removed_read": GROWN,
    "key_removed_append": ["OK", DOUBLED],
}


def _session(root: Path, ansi: str) -> ReparkSession:
    return (
        ReparkSession.builder.appName("ice-held-frame-snapshot-1")
        .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", str(root / "lc"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
        .getOrCreate()
    )


@pytest.fixture(scope="module")
def measured(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, Any]]:
    found = {}
    for ansi in sorted(ORACLE):
        session = _session(tmp_path_factory.mktemp("held-" + ansi), ansi)
        session.sql("CREATE NAMESPACE IF NOT EXISTS lc.db").collect()
        found[ansi] = Cells(session).run()
        session.stop()
        _reset_active_session_for_tests()
    yield found


@pytest.mark.parametrize(("ansi", "key"), KEYS)
def test_a_held_iceberg_frame_answers_as_spark(
    measured: dict[str, Any], ansi: str, key: str
) -> None:
    assert measured[ansi][key] == ORACLE[ansi][key], (ansi, key, measured[ansi][key])


@pytest.fixture(scope="module")
def enc_measured(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, Any]]:
    session = _session(tmp_path_factory.mktemp("held-enc-1"), "true")
    session.sql("CREATE NAMESPACE IF NOT EXISTS lc.db").collect()
    found = EncCells(session).run()
    session.stop()
    _reset_active_session_for_tests()
    yield found


@pytest.mark.parametrize("key", sorted(REFUSED))
def test_a_held_frame_read_after_a_refused_write_answers_as_before(
    enc_measured: dict[str, Any], key: str
) -> None:
    found = enc_measured[key]
    assert found["refusal"] == REFUSED[key], (key, found)
    assert found["held_after"] == found["before"] == found["fresh_after"] == SEED, (key, found)


@pytest.mark.parametrize("key", sorted(PAIRS))
def test_a_map_served_table_answers_and_refuses_as_a_fresh_guarded_load(
    enc_measured: dict[str, Any], key: str
) -> None:
    map_served, fresh = enc_measured[key]
    assert map_served == fresh, (key, map_served, fresh)
    assert map_served == PAIRS[key], (key, map_served)
