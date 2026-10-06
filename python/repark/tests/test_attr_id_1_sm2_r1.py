from __future__ import annotations

import json
from pathlib import Path

from repark import ReparkSession


def _open(warehouse: Path) -> ReparkSession:
    return (
        ReparkSession.builder.appName("attr-id-1-sm2-r1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", str(warehouse / "sc"))
        .getOrCreate()
    )


def _sort_source_ids(warehouse: Path) -> list[int]:
    files = sorted(
        (warehouse / "sc" / "ns" / "o1" / "metadata").glob("v*.metadata.json"),
        key=lambda item: int(item.name.split(".")[0][1:]),
    )
    meta = json.loads(files[-1].read_text(encoding="utf-8"))
    current = [
        order
        for order in meta["sort-orders"]
        if order["order-id"] == meta["default-sort-order-id"]
    ]
    return [field["source-id"] for field in current[0]["fields"]]


def test_nested_write_ordered_by_replaces_the_sort_key(tmp_path: Path) -> None:
    session = _open(tmp_path)
    session.conf.set("spark.sql.caseSensitive", "false")
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    session.sql(
        "CREATE TABLE sc.ns.o1 (id INT NOT NULL, cat STRING, s STRUCT<a: INT>)"
        " USING iceberg"
    ).collect()
    session.sql("ALTER TABLE sc.ns.o1 WRITE ORDERED BY CAT").collect()
    assert _sort_source_ids(tmp_path) == [2]
    session.sql("ALTER TABLE sc.ns.o1 WRITE ORDERED BY s.A").collect()
    assert _sort_source_ids(tmp_path) == [4]
    session.stop()
