"""ICE-ROWID-ORDER-1: one statement's v3 row ids are deterministic.

The a/b/c cells read their expected answer from the recorded
``fixtures/torture/data/ice_rowid_order_1/spark_rowid_abc_oracle.json``: twelve runs
of ``INSERT INTO t SELECT`` on a v3 table partitioned by ``cat`` answer partition
to ``min(_row_id)`` = a:0, b:100, c:200 in 12 of 12 on Spark (Hadoop and InMemory
catalogs alike), literal ``VALUES`` a:0, b:2, c:4 and CTAS a:0, b:100, c:200. Each
pin repeats its shape twelve times on fresh RePark memory-catalog tables and
asserts all twelve runs give one mapping and it equals Spark's recorded one.

The eight-category cell reads its expected answer from
``spark_rowid_order_oracle.json``: under the default configuration Spark's file
order is its hash-partitioner task order ``z, x, m, a, q, b, c, d``, deterministic
per configuration but not partition-value order. RePark appends a statement's data
files in ascending partition value, then write-task index (fork #300
F-ROWID-ORDER-1), so the pin asserts twelve runs give one ascending mapping and,
as a DECLARED divergence, that Spark's recorded default-configuration order
differs from it: a future convergence reds the pin. Row ids stay spec-correct on
both engines either way: contiguous, unique, every file's ``first_row_id`` the
running sum.

pins: ice-rowid-order-1/C-003, C-004, C-005, C-006
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from repark import ReparkSession

_DATA = "repark-parity/fixtures/torture/data/ice_rowid_order_1"
_FIXTURE_DIR = Path(__file__).resolve().parents[2] / _DATA
_ABC: dict[str, Any] = json.loads((_FIXTURE_DIR / "spark_rowid_abc_oracle.json").read_text())[
    "cells"
]
_ORDER: dict[str, Any] = json.loads((_FIXTURE_DIR / "spark_rowid_order_oracle.json").read_text())
_CATALOG = "ice_rowid_order_1"
_NAMESPACE = "ns"
_RUNS = 12
_ALLOW_V3 = "repark.sql.allowCreateFormatVersion3"
_ABC_CASE = "CASE WHEN id % 3 = 0 THEN 'a' WHEN id % 3 = 1 THEN 'b' ELSE 'c' END"
_ORDER_CASE = " ".join(
    f"WHEN id % 8 = {index} THEN '{cat}'" for index, cat in enumerate(_ORDER["cats"])
)
_ABC_SELECT_MAPPING = [["a", 0], ["b", 100], ["c", 200]]
_ABC_VALUES_MAPPING = [["a", 0], ["b", 2], ["c", 4]]
_ORDER_ASCENDING = ["a", "b", "c", "d", "m", "q", "x", "z"]
_ORDER_SPARK_DEFAULT = ["z", "x", "m", "a", "q", "b", "c", "d"]


def _session(warehouse: Path) -> ReparkSession:
    """Return a RePark session with a memory catalog at `warehouse`, v3 CREATE allowed."""
    session = (
        ReparkSession.builder.appName("ice-rowid-order-1").config(_ALLOW_V3, "true").getOrCreate()
    )
    session.register_memory_catalog(_CATALOG, warehouse)
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {_CATALOG}.{_NAMESPACE}")
    return session


def _mapping(session: ReparkSession, table: str) -> list[list[Any]]:
    """Return one table's partition to ``min(_row_id)`` listing in ``min`` order."""
    return [
        [row.asDict()["cat"], row.asDict()["m"]]
        for row in session.sql(
            f"SELECT cat, min(_row_id) AS m FROM {table} GROUP BY cat ORDER BY m"
        ).collect()
    ]


def _insert_select_table(session: ReparkSession, name: str, source: str) -> str:
    """Create one a/b/c v3 table and fill it from `source`, returning its name."""
    table = f"{_CATALOG}.{_NAMESPACE}.{name}"
    session.sql(
        f"CREATE TABLE {table} (id BIGINT, cat STRING, v DOUBLE) USING iceberg "
        "PARTITIONED BY (cat) TBLPROPERTIES ('format-version'='3')"
    ).collect()
    session.sql(f"INSERT INTO {table} SELECT id, cat, v FROM {source}").collect()
    return table


def _abc_source(session: ReparkSession, name: str) -> str:
    """Create the 300-row a/b/c source table, returning its name."""
    source = f"{_CATALOG}.{_NAMESPACE}.{name}"
    session.sql(
        f"CREATE TABLE {source} (id BIGINT, cat STRING, v DOUBLE) USING iceberg "
        "TBLPROPERTIES ('format-version'='3')"
    ).collect()
    session.sql(
        f"INSERT INTO {source} SELECT id, {_ABC_CASE} AS cat, CAST(id AS DOUBLE) AS v "
        "FROM range(300)"
    ).collect()
    return source


def test_insert_select_mapping_is_one_and_matches_spark(tmp_path: Path) -> None:
    """Twelve ``INSERT ... SELECT`` runs give one mapping: Spark's recorded a:0, b:100, c:200."""
    assert _ABC["rowid_sc_select"]["distinct"] == {'[["a", 0], ["b", 100], ["c", 200]]': 12}
    assert _ABC["rowid_mc_select"]["distinct"] == {'[["a", 0], ["b", 100], ["c", 200]]': 12}
    session = _session(tmp_path / "wh")
    try:
        source = _abc_source(session, "src")
        mappings = [
            _mapping(session, _insert_select_table(session, f"t{i}", source)) for i in range(_RUNS)
        ]
    finally:
        session.stop()
    assert len({json.dumps(mapping) for mapping in mappings}) == 1, mappings
    assert mappings[0] == _ABC_SELECT_MAPPING, mappings[0]


def test_values_mapping_is_one_and_matches_spark(tmp_path: Path) -> None:
    """Twelve literal ``VALUES`` runs give one mapping: Spark's recorded a:0, b:2, c:4."""
    assert _ABC["rowid_sc_values"]["distinct"] == {'[["a", 0], ["b", 2], ["c", 4]]': 12}
    assert _ABC["rowid_mc_values"]["distinct"] == {'[["a", 0], ["b", 2], ["c", 4]]': 12}
    session = _session(tmp_path / "wh")
    try:
        mappings = []
        for index in range(_RUNS):
            table = f"{_CATALOG}.{_NAMESPACE}.v{index}"
            session.sql(
                f"CREATE TABLE {table} (id BIGINT, cat STRING) USING iceberg "
                "PARTITIONED BY (cat) TBLPROPERTIES ('format-version'='3')"
            ).collect()
            session.sql(
                f"INSERT INTO {table} VALUES (1, 'b'), (2, 'a'), (3, 'c'), "
                "(4, 'a'), (5, 'b'), (6, 'c')"
            ).collect()
            mappings.append(_mapping(session, table))
    finally:
        session.stop()
    assert len({json.dumps(mapping) for mapping in mappings}) == 1, mappings
    assert mappings[0] == _ABC_VALUES_MAPPING, mappings[0]


def test_ctas_mapping_is_one_and_matches_spark(tmp_path: Path) -> None:
    """Twelve CTAS runs give one mapping: Spark's recorded a:0, b:100, c:200."""
    assert _ABC["rowid_sc_ctas"]["distinct"] == {'[["a", 0], ["b", 100], ["c", 200]]': 12}
    assert _ABC["rowid_mc_ctas"]["distinct"] == {'[["a", 0], ["b", 100], ["c", 200]]': 12}
    session = _session(tmp_path / "wh")
    try:
        source = _abc_source(session, "csrc")
        mappings = []
        for index in range(_RUNS):
            table = f"{_CATALOG}.{_NAMESPACE}.c{index}"
            session.sql(
                f"CREATE TABLE {table} USING iceberg PARTITIONED BY (cat) "
                f"TBLPROPERTIES ('format-version'='3') AS SELECT id, cat, v FROM {source}"
            ).collect()
            mappings.append(_mapping(session, table))
    finally:
        session.stop()
    assert len({json.dumps(mapping) for mapping in mappings}) == 1, mappings
    assert mappings[0] == _ABC_SELECT_MAPPING, mappings[0]


def test_eight_category_mapping_is_one_and_ascending(tmp_path: Path) -> None:
    """Twelve eight-category runs give one mapping: ascending partition value."""
    session = _session(tmp_path / "wh")
    try:
        source = f"{_CATALOG}.{_NAMESPACE}.osrc"
        session.sql(
            f"CREATE TABLE {source} (id BIGINT, cat STRING, v DOUBLE) USING iceberg "
            "TBLPROPERTIES ('format-version'='3')"
        ).collect()
        session.sql(
            f"INSERT INTO {source} SELECT id, CASE {_ORDER_CASE} END AS cat, "
            "CAST(id AS DOUBLE) AS v FROM range(400)"
        ).collect()
        mappings = [
            _mapping(session, _insert_select_table(session, f"o{index}", source))
            for index in range(_RUNS)
        ]
    finally:
        session.stop()
    assert len({json.dumps(mapping) for mapping in mappings}) == 1, mappings
    assert [row[0] for row in mappings[0]] == _ORDER_ASCENDING, mappings[0]
    assert [row[1] for row in mappings[0]] == [50 * index for index in range(8)], mappings[0]


def test_recorded_spark_default_order_differs_from_ascending() -> None:
    """Spark's recorded default-configuration file order is not ascending partition value."""
    cell = _ORDER["cells"]["aqe=True rows=400 dist=hash"]
    assert cell["distinct_file_orders"] == [json.dumps(_ORDER_SPARK_DEFAULT)], cell
    assert _ORDER_SPARK_DEFAULT != _ORDER_ASCENDING
