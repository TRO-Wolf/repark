"""ICE-TSNS-NARROW-REFUSE-1, fold 3: record what a read answers on the installed build.

``python _record_ice_tsns_narrow_refuse_1_reads.py <commit>`` runs every statement of ``READS``
in one session and rewrites ``ice_tsns_narrow_refuse_1_reads_base.json`` with the Arrow schema of
the result (field metadata included), its rows and the logical rows of the plain ``EXPLAIN``.
``--output <path>`` writes a build's answers elsewhere, for a comparison. The fixture is the
base: main ``2c33f289``, the build the unit must not change a read of.

pins: ice-tsns-narrow-refuse-1/C-040
"""

from __future__ import annotations

import argparse
import json
import re
import tempfile
from pathlib import Path
from typing import Any

import _ice_tsns_narrow_refuse_1_doors as doors

FIXTURE = Path(__file__).with_name("ice_tsns_narrow_refuse_1_reads_base.json")
ZONE = "America/New_York"
SRC = "ice.ns.wide"
SIBLING = "TIMESTAMP '2001-02-03 04:05:06.654321'"
GENERATED = re.compile(r"snapshot_id=\d+|__repark_[a-z]+_[0-9a-f]+|_repark_[a-z]+_[0-9a-f]+|#\d+")
WRITTEN_BESIDE = (
    "array_intersect(array(ns), array(CAST(ns AS TIMESTAMP)))",
    "array_union(array(ns), array(CAST(ns AS TIMESTAMP)))",
    "array_except(array(tzns), array(CAST(tzns AS TIMESTAMP)))",
    "array_distinct(array(ns, CAST(ns AS TIMESTAMP)))",
    "array(CAST(ns AS TIMESTAMP), ns)",
    "map(CAST(ns AS TIMESTAMP), 1, ns, 2)",
    "coalesce(CAST(ns AS TIMESTAMP), ns)",
    "coalesce(ns, CAST(ns AS TIMESTAMP))",
    "greatest(CAST(tzns AS TIMESTAMP), tzns)",
    "least(ns, CAST(ns AS TIMESTAMP))",
    "if(id > 3, tzns, CAST(tzns AS TIMESTAMP))",
    "CASE WHEN id > 3 THEN CAST(ns AS TIMESTAMP) ELSE ns END",
    "nullif(ns, CAST(ns AS TIMESTAMP))",
    "nullif(CAST(tzns AS TIMESTAMP), tzns)",
    "CAST(ns AS TIMESTAMP) = ns",
    "CAST(tzns AS TIMESTAMP) < tzns",
    "ns IN (CAST(ns AS TIMESTAMP))",
    "ns BETWEEN CAST(ns AS TIMESTAMP) AND ns",
    "array_contains(array(ns), CAST(ns AS TIMESTAMP))",
    "array_position(array(tzns), CAST(tzns AS TIMESTAMP))",
    "arrays_overlap(array(ns), array(CAST(ns AS TIMESTAMP)))",
    "array_remove(array(ns), CAST(ns AS TIMESTAMP))",
    "datediff(ns, CAST(ns AS TIMESTAMP))",
    "ns - CAST(ns AS TIMESTAMP)",
    "coalesce(CAST(ns AS TIMESTAMP), ts)",
    "coalesce(TRY_CAST(ns AS TIMESTAMP), ts)",
    "coalesce(TRY_CAST(tzns AS TIMESTAMP), ts)",
    "typeof(array(CAST(ns AS TIMESTAMP), ns))",
)
NARROWED = (
    "coalesce(ns, ts)",
    "coalesce(ns, NULL)",
    "CASE WHEN id > 3 THEN ns ELSE ts END",
    "if(id > 2, tzns, NULL)",
    "greatest(ns, ntz)",
    "array(ns, ts)",
    "coalesce(ns, CAST(NULL AS TIMESTAMP))",
    "coalesce(tzns, CAST(NULL AS TIMESTAMP_NTZ))",
    "coalesce(tzns, DATE '2001-02-03')",
    "transform(array(ns), x -> coalesce(x, ts))",
    "coalesce(array(ns), array(ts))",
    "named_struct('f', coalesce(ns, ts))",
    "map('a', ns, 'b', ts)",
    "nullif(ns, ts)",
    "least(tzns, ts)",
    "CAST(coalesce(ns, ts) AS STRING)",
    "typeof(coalesce(ns, ts))",
    "CAST(coalesce(ns, ts) AS TIMESTAMP)",
    "date_trunc('second', if(id > 0, tzns, ts))",
    "lag(ns, 1, ts) OVER (ORDER BY id)",
)
WHOLE = (
    "SELECT id, ns AS v FROM {s} UNION ALL SELECT id, ts FROM {s}",
    "SELECT id, ns AS v FROM {s} UNION ALL SELECT 0, NULL",
    "SELECT id, tzns AS v FROM {s} UNION ALL SELECT 1, DATE '2001-02-03'",
    "SELECT id, v FROM (SELECT id, CAST(ns AS TIMESTAMP) AS v FROM {s} UNION ALL SELECT id, ns "
    "FROM {s}) q",
    "SELECT id FROM {s} WHERE coalesce(ns, ts) > TIMESTAMP '2000-01-01 00:00:00'",
    "SELECT id FROM {s} WHERE CAST(ns AS TIMESTAMP) > TIMESTAMP '2000-01-01 00:00:00'",
    "SELECT count(*) AS n FROM {s} a JOIN {s} b ON CAST(a.ns AS TIMESTAMP) = b.ns",
    "SELECT max(coalesce(ns, ts)) AS a, min(CASE WHEN id > 1 THEN tzns ELSE ts END) AS b FROM {s}",
    "SELECT * FROM (SELECT id, coalesce(ns, ts) FROM {s}) q",
    "SELECT id, CAST(v AS TIMESTAMP) AS x FROM (SELECT id, coalesce(ns, ts) AS v FROM {s}) q",
    "SELECT id, CAST(ts AS TIMESTAMP) AS x FROM (SELECT id, ts FROM {s}) q",
    "SELECT id, CAST(ts AS TIMESTAMP), CAST(ns AS TIMESTAMP) FROM {s}",
    "SELECT id, CAST(ts AS TIMESTAMP) FROM cut_view",
    "SELECT id, CAST(v AS TIMESTAMP) FROM cut_view",
    "SELECT id, CAST(ts AS TIMESTAMP) AS x FROM plain_view",
    "SELECT id, coalesce(CAST(ts AS TIMESTAMP), ts) AS x FROM plain_view",
    "SELECT id FROM plain_view WHERE CAST(ts AS TIMESTAMP) > TIMESTAMP '2000-01-01'",
    "SELECT id, CAST(ntz AS TIMESTAMP) AS x FROM plain_view",
    "SELECT max(CAST(ts AS TIMESTAMP)) AS x FROM plain_view GROUP BY id",
    "SELECT id, CAST(ts AS TIMESTAMP) AS x FROM frame_view",
    "SELECT o.id, l.v FROM {s} o, LATERAL (SELECT coalesce(o.ns, CAST(NULL AS TIMESTAMP)) AS v) l",
    "INSERT INTO ice.ns.sink SELECT id, coalesce(ns, ts) FROM {s}",
    "INSERT INTO ice.ns.sink SELECT id, coalesce(CAST(ns AS TIMESTAMP), ts) FROM {s}",
    "INSERT INTO ice.ns.sink SELECT id, CAST(v AS TIMESTAMP) FROM cut_view",
    "INSERT INTO ice.ns.micros SELECT id, coalesce(ns, ts) FROM {s}",
)
READS = (
    *(f"SELECT id, {value} AS v FROM {{s}}" for value in (*WRITTEN_BESIDE, *NARROWED)),
    *WHOLE,
)


def open_reads_session(warehouse: Path) -> Any:
    """Open the matrix session with a wide source, two sinks and the three views."""
    spark = doors.open_session(ZONE, warehouse)
    spark.sql(
        f"CREATE TABLE {SRC} (id INT, ns timestamp_ns, tzns timestamptz_ns, ts TIMESTAMP, "
        "ntz TIMESTAMP_NTZ) USING iceberg TBLPROPERTIES ('format-version' = '3')"
    )
    spark.sql(
        f"INSERT INTO {SRC} SELECT id, ns, tzns, {SIBLING}, "
        f"TIMESTAMP_NTZ '2001-02-03 04:05:06.654321' FROM {doors.SRC}"
    )
    for name, kind in (("sink", "timestamp_ns"), ("micros", "TIMESTAMP")):
        spark.sql(
            f"CREATE TABLE ice.ns.{name} (id INT, v {kind}) USING iceberg "
            "TBLPROPERTIES ('format-version' = '3')"
        )
    spark.sql(
        f"CREATE TEMPORARY VIEW cut_view AS SELECT id, ts, ns, coalesce(ns, ts) AS v FROM {SRC}"
    )
    spark.sql(f"CREATE TEMPORARY VIEW plain_view AS SELECT id, ts, ntz FROM {SRC}")
    spark.table(SRC).select("id", "ts").createOrReplaceTempView("frame_view")
    return spark


def plain(text: str) -> str:
    """Replace what a run names itself (snapshot ids, generated relations) by a fixed word."""
    return GENERATED.sub("<generated>", text)


def explained(spark: Any, statement: str) -> list[list[str]]:
    """Return the logical rows of the plain ``EXPLAIN`` of ``statement``."""
    try:
        rows = spark.sql(f"EXPLAIN {statement}").toArrow().to_pylist()
    except Exception as error:
        return [["error", plain(f"{type(error).__name__}: {error}")[:400]]]
    return [
        [row["plan_type"], plain(row["plan"])] for row in rows if "physical" not in row["plan_type"]
    ]


def answered(spark: Any, statement: str) -> dict[str, Any]:
    """Return what ``statement`` answers: its schema, its rows and its plain ``EXPLAIN``."""
    answer: dict[str, Any] = {"explain": explained(spark, statement)}
    if statement.startswith("INSERT"):
        return answer
    try:
        table = spark.sql(statement).toArrow()
    except Exception as error:
        answer["error"] = plain(f"{type(error).__name__}: {error}")[:400]
        return answer
    answer["schema"] = str(table.schema)
    answer["rows"] = sorted(str(row) for row in table.to_pylist())
    return answer


def record() -> dict[str, Any]:
    """Answer every statement of ``READS`` in one fresh session."""
    with tempfile.TemporaryDirectory(prefix="tsns-reads-") as scratch:
        spark = open_reads_session(Path(scratch))
        answers = {statement: answered(spark, statement.replace("{s}", SRC)) for statement in READS}
        spark.stop()
    return answers


def main() -> None:
    """Record the fixture, or a build's answers beside it."""
    parser = argparse.ArgumentParser()
    parser.add_argument("commit")
    parser.add_argument("--output", type=Path, default=FIXTURE)
    arguments = parser.parse_args()
    payload = {"commit": arguments.commit, "zone": ZONE, "reads": record()}
    arguments.output.write_text(json.dumps(payload, indent=0, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
