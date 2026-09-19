"""Record or re-check the ICE-TT-RESOLVE-1 Spark oracle cells on live PySpark.

Usage (a PySpark 4.1.2 interpreter with a JVM on PATH)::

    python python/repark/tests/_record_ice_tt_resolve_1_oracle.py \
        --warehouse <empty-dir> record
    ... check

``record`` replays the 47 time-travel shapes on format versions 2 and 3 (94
cells) plus the 21 TT2 logic-critic shapes on version 2 against live Spark
4.1.2 + Iceberg 1.11.0 over an InMemory catalog and prints the fixture JSON
to stdout. ``check`` replays the same cells and exits non-zero naming the
first mismatch against the committed ``ice_tt_resolve_1_spark_oracle.json``,
comparing status, rows, and the error shape while ignoring run-stamped keys
(``secs``, ``error_step``) and normalizing run-varying snapshot ids and
instants in messages. The Iceberg runtime GAV comes from :mod:`_oracle_pins`
(CP-8: never restate a version literal).

pins: ice-tt-resolve-1/C-001
"""

from __future__ import annotations

import argparse
import datetime
import itertools
import json
import re
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV

FIXTURE = Path(__file__).with_name("ice_tt_resolve_1_spark_oracle.json")
ICEBERG_SPARK_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
UTC = datetime.UTC
NY = datetime.timezone(datetime.timedelta(hours=-4))
CELL_SLEEP = 2.2
VOLATILE_KEYS = ("secs", "error_step", "notes")
LONG_ID = re.compile(r"\b\d{10,}\b")
INSTANT = re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?")


def _spark_session(warehouse: Path, zone: str = "UTC") -> Any:
    """Build the live oracle session over an InMemory Iceberg catalog."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[4]")
        .appName("ice-tt-resolve-1-oracle")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config("spark.sql.extensions", ICEBERG_SPARK_EXTENSIONS)
        .config("spark.sql.catalog.sc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.sc.catalog-impl", "org.apache.iceberg.inmemory.InMemoryCatalog")
        .config("spark.sql.catalog.sc.warehouse", str(warehouse))
        .config("spark.sql.session.timeZone", zone)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def _snapshots(session: Any, table: str) -> tuple[list[int], list[Any]]:
    """Return snapshot ids and commit instants in history order."""
    rows = session.sql(f"SELECT snapshot_id, committed_at FROM {table}.snapshots").collect()
    return [int(row["snapshot_id"]) for row in rows], [row["committed_at"] for row in rows]


def _to_utc(value: Any) -> Any:
    """Normalize a Spark timestamp cell to UTC."""
    if isinstance(value, datetime.datetime) and value.tzinfo is not None:
        return value.astimezone(UTC)
    return value


def _seed(session: Any, table: str, version: int) -> dict[str, Any]:
    """Create the three-snapshot shape with tag t0 on S0 and branch b0 on S1."""
    session.sql(f"DROP TABLE IF EXISTS {table}")
    session.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version'='{version}')"
    )
    session.sql(f"INSERT INTO {table} VALUES (1, 'a', 'x'), (2, 'b', 'y')")
    time.sleep(CELL_SLEEP)
    session.sql(f"INSERT INTO {table} VALUES (3, 'c', 'x')")
    time.sleep(CELL_SLEEP)
    session.sql(f"DELETE FROM {table} WHERE id = 1")
    ids, stamps = _snapshots(session, table)
    stamps = [_to_utc(stamp) for stamp in stamps]
    session.sql(f"ALTER TABLE {table} CREATE TAG t0 AS OF VERSION {ids[0]}")
    session.sql(f"ALTER TABLE {table} CREATE BRANCH b0 AS OF VERSION {ids[1]}")
    mids = [a + (b - a) / 2 for a, b in itertools.pairwise(stamps)]
    return {"ids": ids, "stamps": stamps, "mids": mids}


def _fmt(value: Any) -> str:
    """Format an instant as Spark prints timestamp options."""
    return value.strftime("%Y-%m-%d %H:%M:%S.%f")


def _fmt_ny(value: Any) -> str:
    """Format an instant as a New York wall clock."""
    return value.astimezone(NY).strftime("%Y-%m-%d %H:%M:%S.%f")


def _ms(value: Any) -> int:
    """Epoch milliseconds of an instant."""
    return int(value.timestamp() * 1000)


def _cell_shapes() -> list[dict[str, Any]]:
    """Describe the 47 oracle shapes; each runs on format versions 2 and 3."""
    shapes: list[dict[str, Any]] = []

    def reader(shape: str, title: str, action: str) -> None:
        shapes.append({"family": "reader", "shape": shape, "title": title, "action": action})

    def query(shape: str, title: str, template: str, zone: str = "UTC") -> None:
        shapes.append(
            {"family": "query", "shape": shape, "title": title, "template": template, "zone": zone}
        )

    reader("VAS-INT", "versionAsOf=<snapshot id int>", "version_int")
    reader("VAS-STR", "versionAsOf='<snapshot id>'", "version_str")
    reader("VAS-TAG", "versionAsOf=<tag>", "version_tag")
    reader("VAS-BRANCH", "versionAsOf=<branch>", "version_branch")
    reader("VAS-MAIN", "versionAsOf='main'", "version_main")
    reader("VAS-MISSING-ERR", "versionAsOf=<unknown id>", "version_missing")
    reader("VAS-UNKNOWN-REF-ERR", "versionAsOf=<unknown ref>", "version_unknown_ref")
    reader("VAS-LOWER", "versionasof lower-case key", "version_lower")
    reader("VAS-TABLE-API", "read.option(versionAsOf).table", "version_table_api")
    reader("VAS-TAG-TABLE-API", "read.option(versionAsOf=tag).table", "version_tag_table_api")
    reader("TAS-STR", "timestampAsOf='<mid ts>'", "ts_mid")
    reader("TAS-STR-EXACT", "timestampAsOf=<exact commit ts of S1>", "ts_exact")
    reader("TAS-STR-T", "timestampAsOf ISO 'T' form", "ts_t_form")
    reader("TAS-DATE", "timestampAsOf='2999-01-01' (date only)", "ts_date")
    reader("TAS-BEFORE-ERR", "timestampAsOf before first snapshot", "ts_before")
    reader("TAS-GARBAGE-ERR", "timestampAsOf='not a ts'", "ts_garbage")
    reader("TAS-EPOCH-SEC", "timestampAsOf='<epoch seconds>'", "ts_epoch_sec")
    reader("TAS-EPOCH-MS", "timestampAsOf='<epoch ms>'", "ts_epoch_ms")
    reader("TAS-NY", "timestampAsOf wall clock in session tz New York", "ts_ny")
    reader("TAS-TABLE-API", "read.option(timestampAsOf).table", "ts_table_api")
    reader("VAS-TAS-ERR", "versionAsOf + timestampAsOf together", "err_version_and_ts")
    reader("VAS-SNAPID-ERR", "versionAsOf + snapshot-id together", "err_version_and_snapid")
    reader("VAS-SNAPID-SAME", "versionAsOf + snapshot-id same id", "err_version_and_snapid_same")
    reader("VAS-BRANCH-OPT", "versionAsOf + branch option", "err_version_and_branch")
    reader("TAS-ASOF-ERR", "timestampAsOf + as-of-timestamp", "err_ts_and_asof")
    reader("VAS-SELECTOR-ERR", "versionAsOf on t.branch_b0", "err_version_on_branch")
    query(
        "EXPR-CAST",
        "TIMESTAMP AS OF CAST('<mid>' AS TIMESTAMP)",
        "SELECT * FROM {T} TIMESTAMP AS OF CAST('{M1}' AS TIMESTAMP)",
    )
    query(
        "LIT-TS",
        "TIMESTAMP AS OF TIMESTAMP '<mid>'",
        "SELECT * FROM {T} TIMESTAMP AS OF TIMESTAMP '{M1}'",
    )
    query("STR", "TIMESTAMP AS OF '<mid>'", "SELECT * FROM {T} TIMESTAMP AS OF '{M1}'")
    query(
        "EPOCH-INT",
        "TIMESTAMP AS OF <epoch seconds int>",
        "SELECT * FROM {T} TIMESTAMP AS OF {SEC1}",
    )
    query(
        "EPOCH-MS-INT", "TIMESTAMP AS OF <epoch ms int>", "SELECT * FROM {T} TIMESTAMP AS OF {MS1}"
    )
    query(
        "EPOCH-DEC",
        "TIMESTAMP AS OF <epoch seconds decimal>",
        "SELECT * FROM {T} TIMESTAMP AS OF {SEC1}.5",
    )
    query(
        "CURRENT-TS",
        "TIMESTAMP AS OF current_timestamp()",
        "SELECT * FROM {T} TIMESTAMP AS OF current_timestamp()",
    )
    query(
        "EXPR-ARITH",
        "TIMESTAMP AS OF CAST(...) + INTERVAL 1 SECOND",
        "SELECT * FROM {T} TIMESTAMP AS OF CAST('{M1}' AS TIMESTAMP) - INTERVAL 1 DAYS",
    )
    query(
        "TO-TS",
        "TIMESTAMP AS OF to_timestamp('<mid>')",
        "SELECT * FROM {T} TIMESTAMP AS OF to_timestamp('{M1}')",
    )
    query(
        "DATE-LIT",
        "TIMESTAMP AS OF DATE '2999-01-01'",
        "SELECT * FROM {T} TIMESTAMP AS OF DATE '2999-01-01'",
    )
    query(
        "FST-INT",
        "FOR SYSTEM_TIME AS OF <epoch seconds int>",
        "SELECT * FROM {T} FOR SYSTEM_TIME AS OF {SEC1}",
    )
    query(
        "FST-EXPR",
        "FOR SYSTEM_TIME AS OF CAST(...)",
        "SELECT * FROM {T} FOR SYSTEM_TIME AS OF CAST('{M1}' AS TIMESTAMP)",
    )
    query(
        "STR-GARBAGE-ERR",
        "TIMESTAMP AS OF 'garbage'",
        "SELECT * FROM {T} TIMESTAMP AS OF 'garbage'",
    )
    query("COLREF-ERR", "TIMESTAMP AS OF <column ref>", "SELECT * FROM {T} TIMESTAMP AS OF id")
    query(
        "SUBQ-ERR",
        "TIMESTAMP AS OF (scalar subquery)",
        "SELECT * FROM {T} TIMESTAMP AS OF (SELECT CAST('{M1}' AS TIMESTAMP))",
    )
    query("NULL-ERR", "TIMESTAMP AS OF NULL", "SELECT * FROM {T} TIMESTAMP AS OF NULL")
    query(
        "RAND-ERR",
        "TIMESTAMP AS OF non-deterministic",
        (
            "SELECT * FROM {T} TIMESTAMP AS OF CAST('{M1}' AS TIMESTAMP)"
            " + make_interval(0,0,0,0,0,0,rand())"
        ),
    )
    query(
        "EXPR-NY",
        "TIMESTAMP AS OF CAST('<NY wall>' AS TIMESTAMP) in NY session",
        "SELECT * FROM {T} TIMESTAMP AS OF CAST('{M1NY}' AS TIMESTAMP)",
        zone="NY",
    )
    query(
        "STR-NY",
        "TIMESTAMP AS OF '<NY wall>' in NY session",
        "SELECT * FROM {T} TIMESTAMP AS OF '{M1NY}'",
        zone="NY",
    )
    query(
        "EPOCH-NY",
        "TIMESTAMP AS OF <epoch sec> in NY session",
        "SELECT * FROM {T} TIMESTAMP AS OF {SEC1}",
        zone="NY",
    )
    query("VAS-EXPR", "VERSION AS OF <int expr> 1+1", "SELECT * FROM {T} VERSION AS OF {S0} + 0")
    return shapes


def _run_reader(session: Any, table: str, seed: dict[str, Any], action: str) -> dict[str, Any]:
    """Run one reader shape, returning the rows or the refusal."""
    ids = seed["ids"]
    mids = seed["mids"]
    stamps = seed["stamps"]
    sec = _ms(mids[0]) // 1000
    base = session.read.format("iceberg")
    if action == "version_int":
        return {"rows": base.option("versionAsOf", ids[0]).load(table)}
    if action == "version_str":
        return {"rows": base.option("versionAsOf", str(ids[0])).load(table)}
    if action == "version_tag":
        return {"rows": base.option("versionAsOf", "t0").load(table)}
    if action == "version_branch":
        return {"rows": base.option("versionAsOf", "b0").load(table)}
    if action == "version_main":
        return {"rows": base.option("versionAsOf", "main").load(table)}
    if action == "version_missing":
        return {"rows": base.option("versionAsOf", 12345).load(table)}
    if action == "version_unknown_ref":
        return {"rows": base.option("versionAsOf", "nope").load(table)}
    if action == "version_lower":
        return {"rows": base.option("versionasof", ids[0]).load(table)}
    if action == "version_table_api":
        return {"rows": session.read.option("versionAsOf", ids[0]).table(table)}
    if action == "version_tag_table_api":
        return {"rows": session.read.option("versionAsOf", "t0").table(table)}
    if action == "ts_mid":
        return {"rows": base.option("timestampAsOf", _fmt(mids[0])).load(table)}
    if action == "ts_exact":
        return {"rows": base.option("timestampAsOf", _fmt(stamps[1])).load(table)}
    if action == "ts_t_form":
        return {
            "rows": base.option("timestampAsOf", mids[0].strftime("%Y-%m-%dT%H:%M:%S.%f")).load(
                table
            )
        }
    if action == "ts_date":
        return {"rows": base.option("timestampAsOf", "2999-01-01").load(table)}
    if action == "ts_before":
        return {"rows": base.option("timestampAsOf", "2000-01-01 00:00:00").load(table)}
    if action == "ts_garbage":
        return {"rows": base.option("timestampAsOf", "not a ts").load(table)}
    if action == "ts_epoch_sec":
        return {"rows": base.option("timestampAsOf", str(sec)).load(table)}
    if action == "ts_epoch_ms":
        return {"rows": base.option("timestampAsOf", str(_ms(mids[0]))).load(table)}
    if action == "ts_ny":
        return {"rows": base.option("timestampAsOf", _fmt_ny(mids[0])).load(table)}
    if action == "ts_table_api":
        return {"rows": session.read.option("timestampAsOf", _fmt(mids[0])).table(table)}
    if action == "err_version_and_ts":
        return {
            "rows": base.option("versionAsOf", ids[0])
            .option("timestampAsOf", _fmt(mids[0]))
            .load(table)
        }
    if action in ("err_version_and_snapid", "err_version_and_snapid_same"):
        other = ids[0] if action == "err_version_and_snapid_same" else ids[1]
        return {"rows": base.option("versionAsOf", ids[0]).option("snapshot-id", other).load(table)}
    if action == "err_version_and_branch":
        return {"rows": base.option("versionAsOf", ids[0]).option("branch", "b0").load(table)}
    if action == "err_ts_and_asof":
        return {
            "rows": base.option("timestampAsOf", _fmt(mids[0]))
            .option("as-of-timestamp", _ms(mids[1]))
            .load(table)
        }
    if action == "err_version_on_branch":
        return {"rows": base.option("versionAsOf", ids[0]).load(table + ".branch_b0")}
    if action == "ts_date_z":
        return {"rows": base.option("timestampAsOf", "2999-01-01Z").load(table)}
    if action == "ts_nosec":
        return {"rows": base.option("timestampAsOf", "2999-01-01 00:00").load(table)}
    if action == "err_version_on_tag":
        return {"rows": base.option("versionAsOf", ids[0]).load(table + ".tag_t0")}
    if action == "err_ts_on_branch":
        return {"rows": base.option("timestampAsOf", _fmt(mids[0])).load(table + ".branch_b0")}
    raise AssertionError(f"unknown reader action {action!r}")


def _seed_tt2(session: Any, table: str, version: int) -> dict[str, Any]:
    """Create the two-snapshot critic shape with tag t0 and branch b0 on S0."""
    session.sql(f"DROP TABLE IF EXISTS {table}")
    session.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version'='{version}')"
    )
    session.sql(f"INSERT INTO {table} VALUES (1, 'a', 'x'), (2, 'b', 'y')")
    time.sleep(CELL_SLEEP)
    session.sql(f"INSERT INTO {table} VALUES (3, 'c', 'x')")
    ids, stamps = _snapshots(session, table)
    stamps = [_to_utc(stamp) for stamp in stamps]
    session.sql(f"ALTER TABLE {table} CREATE TAG t0 AS OF VERSION {ids[0]}")
    session.sql(f"ALTER TABLE {table} CREATE BRANCH b0 AS OF VERSION {ids[0]}")
    mids = [a + (b - a) / 2 for a, b in itertools.pairwise(stamps)]
    return {"ids": ids, "stamps": stamps, "mids": mids}


def _tt2_shapes() -> list[dict[str, Any]]:
    """Describe the 21 logic-critic shapes; each records once on version 2."""
    shapes: list[dict[str, Any]] = []

    def reader(key: str, cell: str, title: str, action: str) -> None:
        shapes.append({"key": key, "id": cell, "title": title, "kind": "reader", "action": action})

    def query(key: str, cell: str, title: str, template: str, zone: str = "UTC") -> None:
        shapes.append(
            {
                "key": key,
                "id": cell,
                "title": title,
                "kind": "query",
                "template": template,
                "zone": zone,
            }
        )

    def scalar(key: str, cell: str, title: str, template: str, zone: str = "UTC") -> None:
        shapes.append(
            {
                "key": key,
                "id": cell,
                "title": title,
                "kind": "scalar",
                "template": template,
                "zone": zone,
            }
        )

    query(
        "random_err",
        "TT2-RANDOM-ERR",
        "TIMESTAMP AS OF ... + make_interval(random())",
        "SELECT * FROM {T} TIMESTAMP AS OF CAST('{M1}' AS TIMESTAMP)"
        " + make_interval(0,0,0,0,0,0,random())",
    )
    query(
        "uuid_err",
        "TT2-UUID-ERR",
        "TIMESTAMP AS OF with uuid() inside",
        "SELECT * FROM {T} TIMESTAMP AS OF CAST(concat('{M1}', substr(uuid(), 1, 0)) AS TIMESTAMP)",
    )
    query(
        "with_rand_subq",
        "TT2-WITH-RAND-SUBQ",
        "TIMESTAMP AS OF (WITH x AS (SELECT rand() r) SELECT ts + make_interval(...r))",
        "SELECT * FROM {T} TIMESTAMP AS OF (WITH z AS (SELECT rand() AS r)"
        " SELECT CAST('{M1}' AS TIMESTAMP) + make_interval(0,0,0,0,0,0,r) FROM z)",
    )
    query(
        "date_z_future",
        "TT2-DATE-Z-FUTURE",
        "TIMESTAMP AS OF '2999-01-01Z'",
        "SELECT * FROM {T} TIMESTAMP AS OF '2999-01-01Z'",
    )
    scalar(
        "cast_date_z",
        "TT2-CAST-DATE-Z",
        "SELECT CAST('2020-06-01Z' AS TIMESTAMP) in NY",
        "SELECT CAST(CAST('2020-06-01Z' AS TIMESTAMP) AS STRING),"
        " unix_timestamp(CAST('2020-06-01Z' AS TIMESTAMP))",
        zone="NY",
    )
    scalar(
        "cast_nosec_z",
        "TT2-CAST-NOSEC-Z",
        "SELECT CAST('2020-06-01T00:00Z' AS TIMESTAMP) in NY",
        "SELECT unix_timestamp(CAST('2020-06-01T00:00Z' AS TIMESTAMP))",
        zone="NY",
    )
    scalar(
        "cast_plus0000",
        "TT2-CAST-PLUS0000",
        "SELECT CAST('2020-06-01 00:00:00+0000' AS TIMESTAMP) in NY",
        "SELECT unix_timestamp(CAST('2020-06-01 00:00:00+0000' AS TIMESTAMP))",
        zone="NY",
    )
    scalar(
        "cast_dst_gap",
        "TT2-CAST-DST-GAP",
        "SELECT CAST('2026-03-08 02:30:00' AS TIMESTAMP) in NY (gap)",
        "SELECT unix_timestamp(CAST('2026-03-08 02:30:00' AS TIMESTAMP))",
        zone="NY",
    )
    scalar(
        "cast_dst_overlap",
        "TT2-CAST-DST-OVERLAP",
        "SELECT CAST('2026-11-01 01:30:00' AS TIMESTAMP) in NY (overlap)",
        "SELECT unix_timestamp(CAST('2026-11-01 01:30:00' AS TIMESTAMP))",
        zone="NY",
    )
    scalar(
        "cast_short",
        "TT2-CAST-SHORT",
        "SELECT CAST('2020-6-1 1:2:3' AS TIMESTAMP) UTC",
        "SELECT unix_timestamp(CAST('2020-6-1 1:2:3' AS TIMESTAMP))",
    )
    scalar(
        "cast_nanos",
        "TT2-CAST-NANOS",
        "SELECT CAST('2020-06-01 00:00:00.123456789' AS TIMESTAMP) UTC",
        "SELECT CAST(CAST('2020-06-01 00:00:00.123456789' AS TIMESTAMP) AS STRING)",
    )
    scalar(
        "cast_year_only",
        "TT2-CAST-YEAR-ONLY",
        "SELECT CAST('2020' AS TIMESTAMP)",
        "SELECT unix_timestamp(CAST('2020' AS TIMESTAMP))",
    )
    reader("tas_date_z", "TT2-TAS-DATE-Z", "reader timestampAsOf='2999-01-01Z'", "ts_date_z")
    reader("tas_nosec", "TT2-TAS-NOSEC", "reader timestampAsOf 'yyyy-MM-dd HH:mm' mid", "ts_nosec")
    reader(
        "tas_tag_selector",
        "TT2-TAS-TAG-SELECTOR-ERR",
        "versionAsOf on t.tag_t0",
        "err_version_on_tag",
    )
    reader(
        "tas_ts_tag_selector",
        "TT2-TAS-TS-TAG-SELECTOR-ERR",
        "timestampAsOf on t.branch_b0",
        "err_ts_on_branch",
    )
    query(
        "sql_alias_join",
        "TT2-SQL-ALIAS-JOIN",
        "TIMESTAMP AS OF ... a JOIN ... b ON a.id=b.id",
        "SELECT a.id, b.id FROM {T} TIMESTAMP AS OF CAST('{M1}' AS TIMESTAMP) a"
        " FULL OUTER JOIN {T} TIMESTAMP AS OF '2999-01-01' b ON a.id = b.id",
    )
    query(
        "sql_alias_as",
        "TT2-SQL-ALIAS-AS",
        "TIMESTAMP AS OF '<mid>' AS t2 WHERE t2.id > 0",
        "SELECT t2.id FROM {T} TIMESTAMP AS OF '{M1}' AS t2 WHERE t2.id > 0",
    )
    query(
        "sql_version_alias",
        "TT2-SQL-VERSION-ALIAS",
        "VERSION AS OF <id> t2",
        "SELECT t2.id FROM {T} VERSION AS OF {S0} t2",
    )
    query(
        "sql_ts_tag_selector",
        "TT2-SQL-TS-TAG-SELECTOR",
        "SELECT FROM t.tag_t0 TIMESTAMP AS OF",
        "SELECT * FROM {T}.tag_t0 TIMESTAMP AS OF '2999-01-01'",
    )
    query(
        "sql_vas_branch_selector",
        "TT2-SQL-VAS-BRANCH-SELECTOR",
        "SELECT FROM t.branch_b0 VERSION AS OF id",
        "SELECT * FROM {T}.branch_b0 VERSION AS OF {S1}",
    )
    return shapes


def _format_query(template: str, table: str, seed: dict[str, Any]) -> str:
    """Fill a query template from a live seed."""
    ids = seed["ids"]
    mids = seed["mids"]
    sec = _ms(mids[0]) // 1000
    return template.format(
        T=table,
        S0=ids[0],
        S1=ids[1],
        M1=_fmt(mids[0]),
        M1NY=_fmt_ny(mids[0]),
        SEC1=sec,
        MS1=_ms(mids[0]),
    )


def _df_payload(frame: Any) -> dict[str, Any]:
    """Collect a Spark frame to fixture rows and columns."""
    rows = frame.collect()
    fields = list(frame.schema.fields)
    return {
        "rows": [[row[name] for name in [field.name for field in fields]] for row in rows],
        "cols": [field.name for field in fields],
    }


def _error_payload(error: BaseException) -> dict[str, Any]:
    """Reduce a PySpark failure to the fixture error shape."""
    payload: dict[str, Any] = {"type": type(error).__name__, "msg": str(error).strip()}
    for attr in ("getCondition", "getErrorClass", "getSqlState"):
        try:
            payload[attr] = getattr(error, attr)()
        except Exception:
            payload[attr] = None
    return payload


def _record(warehouse: Path) -> list[dict[str, Any]]:
    """Replay every shape on both format versions and return fixture cells."""
    sessions: dict[str, Any] = {}
    cells: list[dict[str, Any]] = []
    try:
        for version in (2, 3):
            for shape in _cell_shapes():
                zone = shape.get("zone", "UTC")
                key = zone
                if key not in sessions:
                    sessions[key] = _spark_session(
                        warehouse, "America/New_York" if zone == "NY" else "UTC"
                    )
                session = sessions[key]
                table = f"sc.ns.t_{shape['shape'].lower()}_v{version}"
                started = time.time()
                seed = _seed(session, table, version)
                asked = (
                    shape["action"]
                    if shape["family"] == "reader"
                    else _format_query(shape["template"], table, seed)
                )
                try:
                    if shape["family"] == "reader":
                        outcome = _run_reader(session, table, seed, shape["action"])
                        payload = _df_payload(outcome["rows"])
                        cells.append(
                            {
                                "id": f"TT-DF-{shape['shape']}-V{version}",
                                "group": "TT",
                                "title": f"{shape['title']} (v{version})",
                                "engine": "spark",
                                "status": "ok",
                                "obs": payload,
                                "notes": [],
                                "secs": round(time.time() - started, 2),
                            }
                        )
                    else:
                        payload = _df_payload(session.sql(asked))
                        cells.append(
                            {
                                "id": f"TT-SQL-{shape['shape']}-V{version}",
                                "group": "TT",
                                "title": f"{shape['title']} (v{version})",
                                "engine": "spark",
                                "status": "ok",
                                "obs": payload,
                                "notes": [],
                                "secs": round(time.time() - started, 2),
                            }
                        )
                except Exception as error:
                    cells.append(
                        {
                            "id": (
                                f"TT-{'DF' if shape['family'] == 'reader' else 'SQL'}-"
                                f"{shape['shape']}-V{version}"
                            ),
                            "group": "TT",
                            "title": f"{shape['title']} (v{version})",
                            "engine": "spark",
                            "status": "error",
                            "error": _error_payload(error),
                            "error_step": asked,
                            "obs": {},
                            "notes": [],
                            "secs": round(time.time() - started, 2),
                        }
                    )
        for shape in _tt2_shapes():
            zone = shape.get("zone", "UTC")
            key = zone
            if key not in sessions:
                sessions[key] = _spark_session(
                    warehouse, "America/New_York" if zone == "NY" else "UTC"
                )
            session = sessions[key]
            table = f"sc.ns.t_tt2_{shape['key']}_v2"
            started = time.time()
            seed = _seed_tt2(session, table, 2)
            asked = (
                shape["action"]
                if shape["kind"] == "reader"
                else _format_query(shape["template"], table, seed)
            )
            try:
                if shape["kind"] == "reader":
                    outcome = _run_reader(session, table, seed, shape["action"])
                    payload = _df_payload(outcome["rows"])
                else:
                    payload = _df_payload(session.sql(asked))
                cells.append(
                    {
                        "id": shape["id"],
                        "group": "TT2",
                        "title": shape["title"],
                        "engine": "spark",
                        "status": "ok",
                        "obs": payload,
                        "notes": [],
                        "secs": round(time.time() - started, 2),
                    }
                )
            except Exception as error:
                cells.append(
                    {
                        "id": shape["id"],
                        "group": "TT2",
                        "title": shape["title"],
                        "engine": "spark",
                        "status": "error",
                        "error": _error_payload(error),
                        "error_step": asked,
                        "obs": {},
                        "notes": [],
                        "secs": round(time.time() - started, 2),
                    }
                )
    finally:
        for session in sessions.values():
            session.stop()
    return cells


def _normalize(message: str) -> str:
    """Replace run-varying ids and instants so recordings compare."""
    return INSTANT.sub("{TS}", LONG_ID.sub("{SID}", message))


def _check(cells: list[dict[str, Any]]) -> str | None:
    """Compare a fresh recording against the fixture, naming the first gap."""
    expected = {
        cell["id"]: cell for cell in json.loads(FIXTURE.read_text(encoding="utf-8"))["cells"]
    }
    for cell in cells:
        want = expected.get(cell["id"])
        if want is None:
            return f"new cell {cell['id']} has no fixture row"
        if want["status"] != cell["status"]:
            return f"{cell['id']}: status {cell['status']!r} != fixture {want['status']!r}"
        if cell["status"] == "ok":
            if want["obs"] != cell["obs"]:
                return f"{cell['id']}: rows differ from fixture"
        else:
            want_error = want["error"]
            got_error = cell["error"]
            if want_error["type"] != got_error["type"]:
                return (
                    f"{cell['id']}: error {got_error['type']!r} != fixture {want_error['type']!r}"
                )
            for key in ("getCondition", "getErrorClass", "getSqlState"):
                if want_error.get(key) != got_error.get(key):
                    return (
                        f"{cell['id']}: {key} {got_error.get(key)!r}"
                        f" != fixture {want_error.get(key)!r}"
                    )
            if _normalize(want_error["msg"]) != _normalize(got_error["msg"]):
                return f"{cell['id']}: message differs from fixture"
    return None


def main(argv: list[str]) -> int:
    """Record the oracle to stdout or re-check it against the fixture."""
    parser = argparse.ArgumentParser(description="Record or check the ICE-TT-RESOLVE-1 oracle")
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("mode", choices=("record", "check"))
    args = parser.parse_args(argv)
    args.warehouse.mkdir(parents=True, exist_ok=True)
    cells = _record(args.warehouse)
    if args.mode == "record":
        fixture = {
            "provenance": {
                "spark": "4.1.2",
                "iceberg": "1.11.0",
                "catalog": "InMemoryCatalog",
                "recorded": datetime.date.today().isoformat(),
                "cells": len(cells),
                "shapes": "47 shapes x format v2 and v3, plus 21 TT2 logic-critic cells",
                "table": (
                    "3 snapshots S0 insert (1,a,x),(2,b,y); S1 insert (3,c,x); "
                    "S2 delete id=1; commits 2.2s apart; tag t0 -> S0, branch b0 -> S1; "
                    "mid = halfway between S0 and S1 commits"
                ),
                "session_time_zone": "UTC unless the cell needs America/New_York",
            },
            "cells": cells,
        }
        print(json.dumps(fixture, indent=1))
        return 0
    gap = _check(cells)
    if gap is not None:
        print(f"oracle drift: {gap}", file=sys.stderr)
        return 1
    print(f"oracle matches the fixture on all {len(cells)} cells")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
