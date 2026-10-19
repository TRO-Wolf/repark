"""UPDATE-SET-SUBQUERY-WALK-1: ``repark.sql()`` refuses a scalar subquery anywhere in SET.

The native door has no identity path, so ``1 + (SELECT …)``, ``COALESCE((SELECT …), 0)``
and ``CASE … THEN (SELECT …) END`` raise the ruling-90 text and leave the Iceberg table
as seeded.

pins: ice-struct-pairing-1/C-034
"""

from __future__ import annotations

from pathlib import Path

import pytest

import repark
from repark._native import PyReparkSession

REFUSAL = (
    "UPDATE with SET column = (SELECT ...) is not supported on this statement: RePark cannot "
    "write the subquery value here, so it refuses before any write."
)
SEEDED = [(0, 10), (1, 20), (2, 30)]
SHAPES = (
    ("plus", "1 + (SELECT v FROM {src})"),
    ("coalesce", "COALESCE((SELECT v FROM {src}), 0)"),
    ("case_then", "CASE WHEN true THEN (SELECT v FROM {src}) ELSE 0 END"),
)


def _rows(table: str) -> list[tuple[int, int]]:
    frame = repark.sql(f"SELECT id, v FROM {table} ORDER BY id").to_arrow()
    return [(int(row["id"]), int(row["v"])) for row in frame.to_pylist()]


def test_a_scalar_subquery_inside_the_set_expression_refuses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each nested SET shape raises the one text and the three seeded rows stay put."""
    native = PyReparkSession.native()
    native.register_memory_catalog("ice", str(tmp_path))
    monkeypatch.setattr(repark, "_ANSI_NATIVE", native)
    location = tmp_path / "ns"
    location.mkdir()
    repark.sql(f"CREATE SCHEMA ice.ns WITH (location = '{location}')").collect()
    repark.sql(
        "CREATE TABLE ice.ns.src AS SELECT CAST(7 AS INT) AS id, CAST(7 AS INT) AS v"
    ).collect()
    for name, value in SHAPES:
        table = f"ice.ns.walk_{name}"
        repark.sql(
            f"CREATE TABLE {table} AS "
            "SELECT CAST(0 AS INT) AS id, CAST(10 AS INT) AS v "
            "UNION ALL SELECT CAST(1 AS INT), CAST(20 AS INT) "
            "UNION ALL SELECT CAST(2 AS INT), CAST(30 AS INT)"
        ).collect()
        statement = f"UPDATE {table} SET v = {value.format(src='ice.ns.src')} WHERE true"
        with pytest.raises(Exception) as caught:
            repark.sql(statement).collect()
        assert REFUSAL in str(caught.value), str(caught.value)
        assert _rows(table) == SEEDED, statement
