"""ICE-TSNS-NARROW-REFUSE-1, fold 2: the redesigned rule through the DataFrame writers.

The store refuses a nanosecond value the analyzer narrowed wherever the stored column's own
lineage holds it, and stores a narrowing the statement wrote. These pins hold the frame doors
(``writeTo().append()`` over a frame from ``spark.sql``, the functions API, a column of a frame
that holds the narrowing) and what a read shows: its schema, its Parquet field ids and the text
of ``EXPLAIN VERBOSE``. What a door stored is read from every Parquet file under the table.

pins: ice-tsns-narrow-refuse-1/C-028, C-029, C-030, C-031, C-032, C-033, C-034
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import _ice_tsns_narrow_refuse_1_doors as doors
import pyarrow.parquet as pq
import pytest

from repark import functions

ZONE = "America/New_York"
SRC = "ice.ns.wide"
SIBLING = "TIMESTAMP '2001-02-03 04:05:06.654321'"
KINDS = {"ns": "timestamp_ns", "tzns": "timestamptz_ns"}
CASES = [(source, target) for source in KINDS for target in KINDS.values()]
PLAIN_BESIDE_A_NARROWING = (
    "SELECT id, max({c}) AS v FROM {s} WHERE coalesce({c}, ts) IS NOT NULL "
    "GROUP BY ROLLUP (id) HAVING id IS NOT NULL",
    "SELECT id, v FROM (SELECT id, max({c}) AS v, count(coalesce({c}, ts)) AS n FROM {s} "
    "GROUP BY CUBE (id) HAVING id IS NOT NULL) g",
    "SELECT o.id, l.v FROM {s} o, LATERAL (SELECT o.{c} AS v, coalesce(o.{c}, o.ts) AS w) l",
)
NARROWED = (
    "SELECT id, coalesce(array({c}), array(ts))[0] AS v FROM {s}",
    "SELECT id, array_append(array({c}), ts)[0] AS v FROM {s}",
    "SELECT id, array_union(array({c}), array(ts))[0] AS v FROM {s}",
    "SELECT id, flatten(array(array({c}), array(ts)))[0] AS v FROM {s}",
    "SELECT id, (CASE WHEN id > 0 THEN array({c}) ELSE array(ts) END)[0] AS v FROM {s}",
    "SELECT id, coalesce({c}, DATE '2001-02-03') AS v FROM {s}",
    "SELECT id, {c} AS v FROM {s} UNION ALL SELECT 100, DATE '2001-02-03'",
    "SELECT o.id, l.v FROM {s} o, LATERAL (SELECT coalesce(o.{c}, CAST(NULL AS TIMESTAMP)) AS v) l",
    "SELECT o.id, l.v FROM {s} o, LATERAL (SELECT CASE WHEN o.id > 0 THEN o.{c} ELSE "
    + SIBLING
    + " END AS v) l",
    "SELECT id, CAST(CAST(coalesce({c}, NULL) AS STRING) AS {n}) AS v FROM {s}",
    "SELECT id, timestamp_micros(unix_micros(coalesce({c}, ts))) AS v FROM {s}",
    "SELECT id, max(coalesce({c}, ts)) AS v FROM {s} GROUP BY ROLLUP (id) HAVING id IS NOT NULL",
)
NAIVE_ONLY = (
    "SELECT id, CAST(date_trunc('second', coalesce(ns, ts)) AS DATE) AS v FROM {s}",
    "SELECT id, CAST(coalesce(ns, ntz) AS TIMESTAMP) AS v FROM {s}",
    "SELECT id, CAST(coalesce(ns, CAST(NULL AS TIMESTAMP_NTZ)) AS DATE) AS v FROM {s}",
    "SELECT id, CAST(date_trunc('second', coalesce(ns, NULL)) AS TIMESTAMP) AS v FROM {s}",
)
REFUSED = [
    (source, target, query)
    for source, target in CASES
    for query in (*NARROWED, *(NAIVE_ONLY if source == "ns" else ()))
]
INTERNAL = ("__repark", "Timestamp(", "Interval", "the table ``")


def open_rule_session(warehouse: Path) -> Any:
    """Open the matrix session and add a source that also holds microsecond siblings."""
    spark = doors.open_session(ZONE, warehouse)
    spark.sql(
        f"CREATE TABLE {SRC} (id INT, ns timestamp_ns, tzns timestamptz_ns, ts TIMESTAMP, "
        "ntz TIMESTAMP_NTZ) USING iceberg TBLPROPERTIES ('format-version' = '3')"
    )
    spark.sql(
        f"INSERT INTO {SRC} SELECT id, ns, tzns, {SIBLING}, "
        f"TIMESTAMP_NTZ '2001-02-03 04:05:06.654321' FROM {doors.SRC}"
    )
    return spark


def append_frame(spark: Any, warehouse: Path, name: str, kind: str, frame: Any) -> list[Any]:
    """Create ``ice.ns.<name>`` with a ``kind`` column, append ``frame``, return the store."""
    spark.sql(
        f"CREATE TABLE ice.ns.{name} (id INT, v {kind}) USING iceberg "
        "TBLPROPERTIES ('format-version' = '3')"
    )
    frame.writeTo(f"ice.ns.{name}").append()
    return doors.stored(warehouse, f"ice.ns.{name}")


def spell(query: str, source: str) -> str:
    """Return ``query`` over the ``source`` column of the wide table."""
    return query.format(c=source, s=SRC, n=KINDS[source])


@pytest.mark.parametrize(("source", "target"), CASES, ids="|".join)
def test_a_written_cast_beside_a_microsecond_value_stores_through_the_frame_writers(
    tmp_path: Path, source: str, target: str
) -> None:
    """``coalesce(CAST(c AS TIMESTAMP), ts)`` stores what ``CAST(c AS TIMESTAMP)`` stores."""
    spark = open_rule_session(tmp_path)
    twin = spark.sql(f"SELECT id, CAST({source} AS TIMESTAMP) AS v FROM {SRC}")
    reference = append_frame(spark, tmp_path, "twin", target, twin)
    assert len(reference) == len(doors.MOMENTS)
    written = {
        "coalesce": f"SELECT id, coalesce(CAST({source} AS TIMESTAMP), ts) AS v FROM {SRC}",
        "array": f"SELECT id, array(CAST({source} AS TIMESTAMP), ts)[0] AS v FROM {SRC}",
    }
    for name, query in written.items():
        assert append_frame(spark, tmp_path, name, target, spark.sql(query)) == reference, query
    api = spark.table(SRC).select(
        "id",
        functions.coalesce(functions.col(source).cast("timestamp"), functions.col("ts")).alias("v"),
    )
    assert append_frame(spark, tmp_path, "api", target, api) == reference


@pytest.mark.parametrize("source", list(KINDS))
def test_a_plain_column_stores_beside_a_narrowing_through_the_frame_writers(
    tmp_path: Path, source: str
) -> None:
    """A plain column keeps nine digits when a narrowing sits beside it under any plan node."""
    spark = open_rule_session(tmp_path)
    whole = [[row, tick] for row, tick in zip(doors.IDS, doors.NANOS, strict=True)]
    for number, query in enumerate(PLAIN_BESIDE_A_NARROWING):
        frame = spark.sql(spell(query, source))
        stored = append_frame(spark, tmp_path, f"plain{number}", KINDS[source], frame)
        assert stored == sorted(whole, key=repr), query


@pytest.mark.parametrize(("source", "target", "query"), REFUSED)
def test_a_narrowed_value_refuses_through_the_frame_writers(
    tmp_path: Path, source: str, target: str, query: str
) -> None:
    """The frame writer refuses by name, stores nothing, and the table takes the next write."""
    spark = open_rule_session(tmp_path)
    frame = spark.sql(spell(query, source))
    with pytest.raises(Exception, match="narrowed from nanoseconds to microseconds") as caught:
        append_frame(spark, tmp_path, "refused", target, frame)
    text = str(caught.value)
    assert "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]" in text, text
    assert "SQLSTATE: KD000" in text, text
    assert not [name for name in INTERNAL if name in text], text
    assert doors.stored(tmp_path, "ice.ns.refused") == []
    assert spark.sql("SELECT count(*) FROM ice.ns.refused.snapshots").toArrow().to_pylist() == [
        {"count(*)": 0}
    ]
    spark.sql(f"SELECT id, CAST(NULL AS {target}) AS v FROM {SRC}").writeTo(
        "ice.ns.refused"
    ).append()
    assert spark.sql("SELECT count(*) AS n FROM ice.ns.refused").toArrow().to_pylist() == [
        {"n": len(doors.MOMENTS)}
    ]


@pytest.mark.parametrize(("source", "target"), CASES, ids="|".join)
def test_a_written_cast_over_a_frame_column_that_holds_the_narrowing_stores(
    tmp_path: Path, source: str, target: str
) -> None:
    """A cast written over a frame, a view or a subquery column stores as the inline cast does."""
    spark = open_rule_session(tmp_path)
    twin = spark.sql(f"SELECT id, CAST({source} AS TIMESTAMP) AS v FROM {SRC}")
    reference = append_frame(spark, tmp_path, "twin", target, twin)
    narrowed = spark.sql(f"SELECT id, coalesce({source}, ts) AS v FROM {SRC}")
    narrowed.createOrReplaceTempView("narrowed_frame")
    frames = {
        "inline": spark.sql(
            f"SELECT id, CAST(coalesce({source}, ts) AS TIMESTAMP) AS v FROM {SRC}"
        ),
        "api": narrowed.select("id", functions.col("v").cast("timestamp").alias("v")),
        "expr": narrowed.selectExpr("id", "CAST(v AS TIMESTAMP) AS v"),
        "view": spark.sql("SELECT id, CAST(v AS TIMESTAMP) AS v FROM narrowed_frame"),
        "subquery": spark.sql(
            f"SELECT id, CAST(v AS TIMESTAMP) AS v FROM "
            f"(SELECT id, coalesce({source}, NULL) AS v FROM {SRC}) q"
        ),
    }
    for name, frame in frames.items():
        assert append_frame(spark, tmp_path, name, target, frame) == reference, name


def test_a_read_shows_no_mark_and_keeps_its_schema(tmp_path: Path) -> None:
    """A read's schema, Parquet field ids and ``EXPLAIN VERBOSE`` carry nothing of the unit."""
    spark = open_rule_session(tmp_path)
    plain = spark.sql(f"SELECT id, ns AS v FROM {SRC}").toArrow().schema.field("v").metadata
    assert plain == {b"PARQUET:field_id": b"2"}
    branches = ("SELECT 0, NULL", f"SELECT 0, {SIBLING}", "SELECT 0, CAST(NULL AS TIMESTAMP)")
    for number, branch in enumerate(branches):
        frame = spark.sql(f"SELECT id, ns AS v FROM {SRC} UNION ALL {branch}")
        assert frame.toArrow().schema.field("v").metadata == plain, branch
        path = tmp_path / f"read{number}"
        frame.write.parquet(str(path))
        written = [pq.read_schema(file).field("v").metadata for file in path.rglob("*.parquet")]
        assert written, branch
        assert all(found == plain for found in written), (branch, written)
    reads = (
        f"SELECT id, coalesce(ns, ts) AS v FROM {SRC}",
        f"SELECT id, ns AS v FROM {SRC} UNION ALL SELECT 0, NULL",
        f"SELECT id, array_append(array(tzns), ts)[0] FROM {SRC}",
        f"SELECT id, CAST(v AS TIMESTAMP) FROM (SELECT id, coalesce(ns, ts) AS v FROM {SRC}) q",
    )
    for read in reads:
        shown = "\n".join(
            str(row) for row in spark.sql(f"EXPLAIN VERBOSE {read}").toArrow().to_pylist()
        )
        assert "__repark_narrowed" not in shown, read
        assert "repark.untyped_null" not in shown, read
