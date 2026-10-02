"""cdc S0 pins: the five capture failure scenarios, xfail until the 1.7 producer."""

from __future__ import annotations

import os
from typing import Any

import pytest

_PIN = pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="cdc S0 pin: the producer arrives at 1.7",
)


def _table(names: dict[str, str]) -> str:
    """Return the quoted schema-qualified src table for this cell."""
    return f'"{names["schema"]}".src'


def _create_table(conn: Any, names: dict[str, str]) -> None:
    """Create the scenario table in the cell schema."""
    conn.execute(f"CREATE TABLE {_table(names)} (id integer PRIMARY KEY, v text)")


def _create_publication(conn: Any, names: dict[str, str]) -> None:
    """Create the cell publication for the scenario table."""
    conn.execute(f'CREATE PUBLICATION "{names["publication"]}" FOR TABLE {_table(names)}')


def _create_slot(conn: Any, names: dict[str, str]) -> None:
    """Create the pgoutput logical slot under the cell name."""
    conn.execute(
        "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')",
        (names["slot"],),
    )


def _assert_checkpoint(conn: Any, names: dict[str, str]) -> None:
    """Assert the producer has checkpointed through the current WAL LSN."""
    target = conn.execute("SELECT pg_current_wal_lsn()").fetchone()[0]
    row = conn.execute(
        "SELECT confirmed_flush_lsn >= %s FROM pg_replication_slots WHERE slot_name = %s",
        (target, names["slot"]),
    ).fetchone()
    assert row is not None
    assert row[0]


def _peek_row_count(conn: Any, names: dict[str, str]) -> int:
    """Return how many change rows a peek of the cell slot currently yields."""
    rows = conn.execute(
        "SELECT * FROM pg_logical_slot_peek_binary_changes("
        "%s, NULL, NULL, 'proto_version', '1', 'publication_names', %s)",
        (names["slot"], names["publication"]),
    ).fetchall()
    return len(rows)


def test_fixture_isolates_names(pg_live_factory) -> None:
    """Two cells in one session get distinct names, and A's objects do not survive its teardown."""
    with pg_live_factory() as (conn_b, names_b):
        with pg_live_factory() as (conn_a, names_a):
            assert names_a["schema"] != names_b["schema"]
            assert names_a["publication"] != names_b["publication"]
            assert names_a["slot"] != names_b["slot"]
            conn_a.execute(f'CREATE PUBLICATION "{names_a["publication"]}"')
            conn_a.execute(
                "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')",
                (names_a["slot"],),
            )
        tag = names_a["schema"].removeprefix("t_")
        slots = conn_b.execute(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE %s",
            (f"%{tag}%",),
        ).fetchall()
        publications = conn_b.execute(
            "SELECT pubname FROM pg_publication WHERE pubname LIKE %s",
            (f"%{tag}%",),
        ).fetchall()
        namespaces = conn_b.execute(
            "SELECT nspname FROM pg_namespace WHERE nspname LIKE %s",
            (f"%{tag}%",),
        ).fetchall()
        assert slots == []
        assert publications == []
        assert namespaces == []


@_PIN
def test_crash_after_commit_before_checkpoint(pg_live) -> None:
    """Inserts commit, and the producer must checkpoint through that WAL LSN."""
    conn, names = pg_live
    _create_table(conn, names)
    _create_publication(conn, names)
    _create_slot(conn, names)
    conn.execute(f"INSERT INTO {_table(names)} (id, v) VALUES (%s, %s)", (1, "committed"))
    _assert_checkpoint(conn, names)


@_PIN
def test_snapshot_to_wal_handover_under_concurrent_writes(pg_live) -> None:
    """Seed before the slot, then write after it from a second connection."""
    conn, names = pg_live
    _create_table(conn, names)
    conn.execute(f"INSERT INTO {_table(names)} (id, v) VALUES (%s, %s)", (1, "seed"))
    _create_publication(conn, names)
    _create_slot(conn, names)
    psycopg = pytest.importorskip("psycopg")
    second = psycopg.connect(os.environ["REPARK_PG_URL"], autocommit=True)
    try:
        second.execute(f"INSERT INTO {_table(names)} (id, v) VALUES (%s, %s)", (2, "wal"))
        second.execute(f"UPDATE {_table(names)} SET v = %s WHERE id = %s", ("updated", 1))
    finally:
        second.close()
    _assert_checkpoint(conn, names)


@_PIN
def test_replay_and_duplicate_delivery(pg_live) -> None:
    """Two peeks of the same slot return the same non-zero row count."""
    conn, names = pg_live
    _create_table(conn, names)
    _create_publication(conn, names)
    _create_slot(conn, names)
    conn.execute(f"INSERT INTO {_table(names)} (id, v) VALUES (%s, %s)", (1, "once"))
    first = _peek_row_count(conn, names)
    second = _peek_row_count(conn, names)
    assert first == second
    assert first > 0
    _assert_checkpoint(conn, names)


@_PIN
def test_schema_change_and_partial_update_image(pg_live) -> None:
    """ADD COLUMN then UPDATE only the new column under default replica identity."""
    conn, names = pg_live
    _create_table(conn, names)
    _create_publication(conn, names)
    _create_slot(conn, names)
    conn.execute(f"INSERT INTO {_table(names)} (id, v) VALUES (%s, %s)", (1, "before"))
    conn.execute(f"ALTER TABLE {_table(names)} ADD COLUMN extra text")
    conn.execute(f"UPDATE {_table(names)} SET extra = %s WHERE id = %s", ("partial", 1))
    _assert_checkpoint(conn, names)


@_PIN
def test_lost_slot_or_unavailable_wal(pg_live) -> None:
    """After the slot is dropped, the producer must recreate a slot of the same name."""
    conn, names = pg_live
    _create_table(conn, names)
    _create_publication(conn, names)
    _create_slot(conn, names)
    conn.execute(f"INSERT INTO {_table(names)} (id, v) VALUES (%s, %s)", (1, "lost"))
    conn.execute("SELECT pg_drop_replication_slot(%s)", (names["slot"],))
    row = conn.execute(
        "SELECT slot_name FROM pg_replication_slots WHERE slot_name = %s",
        (names["slot"],),
    ).fetchone()
    assert row is not None
