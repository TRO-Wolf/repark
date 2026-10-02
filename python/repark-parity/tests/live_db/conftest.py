"""Live Postgres cells: one disposable container, unique names per test, explicit cleanup."""

from __future__ import annotations

import contextlib
import os
import uuid
from collections.abc import Generator, Iterator
from typing import Any

import pytest


@contextlib.contextmanager
def _pg_live_cell() -> Iterator[tuple[Any, dict[str, str]]]:
    """Create a tagged schema and yield (conn, names); drop publication, slot and schema on exit."""
    url = os.environ.get("REPARK_PG_URL")
    if not url:
        pytest.skip("REPARK_PG_URL unset: make pg-up")
    psycopg = pytest.importorskip("psycopg")
    tag = uuid.uuid4().hex[:12]
    names = {"schema": f"t_{tag}", "publication": f"pub_{tag}", "slot": f"slot_{tag}"}
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{names["schema"]}"')
        try:
            yield conn, names
        finally:
            conn.execute(f'DROP PUBLICATION IF EXISTS "{names["publication"]}"')
            conn.execute(
                "SELECT pg_drop_replication_slot(slot_name)"
                " FROM pg_replication_slots WHERE slot_name = %s",
                (names["slot"],),
            )
            conn.execute(f'DROP SCHEMA "{names["schema"]}" CASCADE')


@pytest.fixture
def pg_live() -> Generator[tuple[Any, dict[str, str]], None, None]:
    """Yield (connection, names) for one test; drop everything the test created on exit."""
    with _pg_live_cell() as cell:
        yield cell


@pytest.fixture
def pg_live_factory() -> Generator[Any, None, None]:
    """Yield the isolation helper so one test can open two independent cells."""
    if not os.environ.get("REPARK_PG_URL"):
        pytest.skip("REPARK_PG_URL unset: make pg-up")
    yield _pg_live_cell
