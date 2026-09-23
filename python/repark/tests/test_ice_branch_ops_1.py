"""ICE-BRANCH-OPS-1 pins: four Iceberg branch procedures on both doors.

Replays the recorded Spark oracle
(``branch_ops_1_truth.json``, PySpark 4.1.2 + Iceberg 1.11.0) against RePark:
``fast_forward``, ``cherrypick_snapshot``, ``set_current_snapshot`` and
``rollback_to_timestamp`` — output schemas, rows with snapshot ids resolved to
log positions, and every recorded error shape. Snapshot ids are random per run,
so the truth stores positions; the suite resolves them against its own log and
asserts the log structure step by step (``new_pos``).

The live tier (``REPARK_PARITY_LIVE=1``) replays Spark on the same shapes and
cross-reads both directions: Spark reads after each RePark procedure, RePark
reads after each Spark procedure.

Error-class map: Spark raises client-side ``IllegalArgumentException`` itself
and server-side Java errors through Py4J. RePark has no JVM, so the
procedure-layer validations surface as ``IllegalArgumentException`` with the
same message, missing/wrong-typed routine arguments surface as
``AnalysisException`` with Spark's routine-binding text, and JVM cast failures
(``NumberFormatException``, ``DateTimeException``) surface as the base
``PySparkException`` with the same operative text, like every other
commit-time Java error.

pins: ice-branch-ops-1/C-001, C-002, C-003, C-004, C-005
pins: ice-branch-ops-1/C-006, C-007, C-008
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from repark import ReparkSession
from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    ParseException,
    PySparkException,
)

_TRUTH = json.loads(
    (Path(__file__).resolve().parent / "branch_ops_1_truth.json").read_text(encoding="utf-8")
)
_LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
_LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — live Spark cell skipped (routine CI is JVM-free)"
_CATALOG = "mem"
_NAMESPACE = "ns"
_ZONE_KEY = "spark.sql.session.timeZone"
_NY = "America/New_York"
_SLEEP = 0.12

_EXC: dict[str, type[BaseException]] = {
    "IllegalArgumentException": IllegalArgumentException,
    "Py4JJavaError": PySparkException,
    "AnalysisException": AnalysisException,
    "ParseException": ParseException,
    "NumberFormatException": PySparkException,
    "DateTimeException": PySparkException,
}


def _swap(sql: str) -> str:
    """Rewrite the recorded ``bo.`` catalog prefix to the suite catalog."""
    return sql.replace("bo.", f"{_CATALOG}.")


def _to_utc_naive(value: Any) -> datetime:
    """Normalize a snapshot timestamp cell to naive UTC."""
    assert isinstance(value, datetime), f"expected datetime, got {value!r}"
    if value.tzinfo is not None:
        return value.astimezone(UTC).replace(tzinfo=None)
    return value


def _read_log(session: ReparkSession, table: str) -> list[dict[str, Any]]:
    """Return the table snapshot log ordered by commit time with positions."""
    arrow = session.sql(
        f"SELECT snapshot_id, parent_id, operation, committed_at "
        f"FROM {_CATALOG}.{_NAMESPACE}.{table}.snapshots ORDER BY committed_at"
    ).to_arrow()
    rows = [
        {
            "id": snap,
            "parent_id": parent,
            "operation": op,
            "ts": _to_utc_naive(ts),
        }
        for snap, parent, op, ts in zip(
            arrow.column("snapshot_id").to_pylist(),
            arrow.column("parent_id").to_pylist(),
            arrow.column("operation").to_pylist(),
            arrow.column("committed_at").to_pylist(),
            strict=True,
        )
    ]
    stamps = [row["ts"] for row in rows]
    assert stamps == sorted(stamps), "snapshot log order is not commit order"
    assert len(set(stamps)) == len(stamps), f"committed_at tie breaks positions: {stamps}"
    return [{"pos": i, **row} for i, row in enumerate(rows)]


def _fmt(when: datetime) -> str:
    """Render a naive UTC instant as a Spark wall clock with micros."""
    return when.strftime("%Y-%m-%d %H:%M:%S.%f")


def _mid(first: datetime, second: datetime) -> datetime:
    """Return the midpoint instant between two commit times."""
    return first + (second - first) / 2


def _resolve(template: str, log: list[dict[str, Any]]) -> str:
    """Substitute truth markers against the live log for execution."""
    entries = {entry["pos"]: entry for entry in log}
    out = template
    for token in sorted(set(re.findall(r"\{snap:\d+\}", template))):
        out = out.replace(token, str(entries[int(token[6:-1])]["id"]))
    for token in sorted(set(re.findall(r"\{ts:\d+\}", template))):
        out = out.replace(token, _fmt(entries[int(token[4:-1])]["ts"]))
    for token in sorted(set(re.findall(r"\{ts_mid:\d+:\d+\}", template))):
        first, second = token[8:-1].split(":")
        out = out.replace(token, _fmt(_mid(entries[int(first)]["ts"], entries[int(second)]["ts"])))
    for token in sorted(set(re.findall(r"\{ts_ny_mid:\d+:\d+\}", template))):
        first, second = token[11:-1].split(":")
        mid = _mid(entries[int(first)]["ts"], entries[int(second)]["ts"])
        out = out.replace(
            token,
            mid.replace(tzinfo=UTC).astimezone(ZoneInfo(_NY)).strftime("%Y-%m-%d %H:%M:%S.%f"),
        )
    return out


def _expected_ms(resolved_sql: str) -> str:
    """Parse the resolved timestamp argument as UTC epoch millis."""
    match = re.search(r"TIMESTAMP '([^']+)'", resolved_sql)
    assert match, f"no TIMESTAMP argument in {resolved_sql!r}"
    wall = match.group(1)
    try:
        instant = datetime.strptime(wall, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        instant = datetime.strptime(wall, "%Y-%m-%d %H:%M:%S")
    return str(int(instant.replace(tzinfo=UTC).timestamp() * 1000))


def _resolve_prefix(prefix: str, log: list[dict[str, Any]], resolved_sql: str) -> str:
    """Resolve snapshot and millis markers in an expected error prefix."""
    by_pos = {entry["pos"]: entry["id"] for entry in log}
    out = prefix
    for token in sorted(set(re.findall(r"\{snap:\d+\}", prefix))):
        out = out.replace(token, str(by_pos[int(token[6:-1])]))
    if "{ms_of_arg}" in out:
        out = out.replace("{ms_of_arg}", _expected_ms(resolved_sql))
    return out


def _norm_cell(value: Any) -> Any:
    """Compare Arrow cells by string form so int widths do not matter."""
    return None if value is None else str(value)


def _resolve_row(row: list[Any], log: list[dict[str, Any]]) -> list[Any]:
    """Resolve one expected output row against the live log."""
    ids = {entry["pos"]: entry["id"] for entry in log}
    out = []
    for cell in row:
        if cell is None:
            out.append(None)
        elif isinstance(cell, dict) and "pos" in cell:
            out.append(str(ids[cell["pos"]]))
        elif isinstance(cell, dict) and "lit" in cell:
            out.append(str(cell["lit"]))
        else:
            raise AssertionError(f"unexpected truth cell {cell!r}")
    return out


def _check_call_output(
    arrow: Any, sql: str, columns: list[list[str]], rows: list[Any], log: list[dict[str, Any]]
) -> None:
    """Assert a CALL output schema and its position-resolved rows."""
    assert [[f.name, str(f.type)] for f in arrow.schema] == columns, sql
    got = sorted([_norm_cell(v) for v in row.values()] for row in arrow.to_pylist())
    want = sorted(_resolve_row(row, log) for row in rows)
    assert got == want, f"{sql}: got {got}, want {want}"


def _assert_error(
    session: ReparkSession,
    sql: str,
    exc: str,
    prefix: str | None,
    log: list[dict[str, Any]],
    needles: list[str] | None = None,
) -> None:
    """Assert a CALL refuses with the recorded class and message needle."""
    assert exc in _EXC, f"unmapped oracle exception {exc!r}"
    with pytest.raises(_EXC[exc]) as caught:
        session.sql(sql).to_arrow()
    wants = (
        [_resolve_prefix(prefix, log, sql)]
        if needles is None and prefix is not None
        else [_resolve_prefix(needle, log, sql) for needle in (needles or [])]
    )
    assert wants, f"{sql}: error cell carries no needle"
    for want in wants:
        assert want in str(caught.value), f"{sql}: {want!r} not in {caught.value}"


def _assert_refs(
    session: ReparkSession, table: str, refs: dict[str, dict[str, Any]], log: list[dict[str, Any]]
) -> None:
    """Assert the ref map with snapshots resolved to positions."""
    ids = {entry["id"]: entry["pos"] for entry in log}
    arrow = session.sql(
        f"SELECT name, type, snapshot_id FROM {_CATALOG}.{_NAMESPACE}.{table}.refs"
    ).to_arrow()
    got = {
        name: {"type": kind, "pos": ids[snap]}
        for name, kind, snap in zip(
            arrow.column("name").to_pylist(),
            arrow.column("type").to_pylist(),
            arrow.column("snapshot_id").to_pylist(),
            strict=True,
        )
    }
    assert got == refs, f"{table}.refs: got {got}, want {refs}"


def _slim(log: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Reduce a live log to the position/parent/operation chain."""
    ids = {entry["id"]: entry["pos"] for entry in log}
    return [
        {
            "pos": entry["pos"],
            "parent_pos": (ids[entry["parent_id"]] if entry["parent_id"] is not None else None),
            "operation": entry["operation"],
        }
        for entry in log
    ]


def _newest_metadata(warehouse: Path, table: str) -> Path:
    """Return the newest table metadata file under a catalog warehouse."""
    cands = [
        path
        for path in warehouse.rglob(f"{table}/metadata/*.metadata.json")
        if (path.parent.parent.name == table)
    ]
    assert cands, f"no metadata for {table} under {warehouse}"
    return max(cands, key=lambda path: path.stat().st_mtime_ns)


def _replay(
    session: ReparkSession,
    warehouse: Path,
    table: str,
    steps: list[dict[str, Any]],
    final_snaps: list[dict[str, Any]],
) -> None:
    """Replay truth steps in order, asserting structure and every check."""
    zone = "UTC"
    created = False
    for step in steps:
        if "note" in step:
            continue
        if "check_rows" in step:
            arrow = session.sql(
                f"SELECT id, s FROM {_CATALOG}.{_NAMESPACE}.{table} ORDER BY id"
            ).to_arrow()
            got = sorted(
                zip(
                    arrow.column("id").to_pylist(),
                    arrow.column("s").to_pylist(),
                    strict=True,
                )
            )
            want = sorted((row[0], row[1]) for row in step["rows"])
            assert got == want, f"{step['check_rows']}: got {got}, want {want}"
            continue
        if "check_refs" in step:
            _assert_refs(session, table, step["refs"], _read_log(session, table))
            continue
        if "check_row_ids" in step:
            arrow = session.sql(
                f"SELECT _row_id, id FROM {_CATALOG}.{_NAMESPACE}.{table} ORDER BY id"
            ).to_arrow()
            got = [
                [rid, row_id]
                for rid, row_id in zip(
                    arrow.column("id").to_pylist(),
                    arrow.column("_row_id").to_pylist(),
                    strict=True,
                )
            ]
            assert got == step["rows"], f"row ids: got {got}, want {step['rows']}"
            continue
        if "check_next_row_id" in step:
            doc = json.loads(_newest_metadata(warehouse, table).read_text(encoding="utf-8"))
            assert doc.get("next-row-id") == step["value"], doc.get("next-row-id")
            continue
        sql = _swap(step["sql"])
        want_zone = step.get("zone", "UTC")
        if want_zone != zone:
            session.conf.set(_ZONE_KEY, want_zone)
            zone = want_zone
        log = _read_log(session, table) if created else []
        resolved = _resolve(sql, log)
        if "expect_error" in step:
            _assert_error(
                session,
                resolved,
                step["expect_error"]["exc"],
                step["expect_error"].get("prefix"),
                log,
                step["expect_error"].get("needles"),
            )
            assert len(_read_log(session, table)) == len(log), f"{resolved} grew the log"
            continue
        arrow = session.sql(resolved).to_arrow()
        if sql.upper().startswith("CREATE TABLE"):
            created = True
        grown = _read_log(session, table)
        if step.get("new_pos") is not None:
            assert len(grown) == step["new_pos"] + 1, (
                f"{resolved}: log length {len(grown)}, want {step['new_pos'] + 1}"
            )
            time.sleep(_SLEEP)
        else:
            assert len(grown) == len(log), f"{resolved} grew the log: {len(log)} -> {len(grown)}"
        if "expect_call" in step:
            _check_call_output(
                arrow,
                resolved,
                step["expect_call"]["columns"],
                step["expect_call"]["rows"],
                grown,
            )
    if zone != "UTC":
        session.conf.set(_ZONE_KEY, "UTC")
    assert _slim(_read_log(session, table)) == final_snaps


@pytest.fixture
def spark(tmp_path: Path) -> Any:
    """A session with a memory catalog for the recorded-shape replay."""
    session = (
        ReparkSession.builder.appName("pytest-ice-branch-ops-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog(_CATALOG, tmp_path / "warehouse")
    session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
    yield session
    session.stop()


def test_branch_ops_v2_script(spark: ReparkSession, tmp_path: Path) -> None:
    """The recorded v2 script replays on RePark with Spark's answers."""
    assert _TRUTH["oracle"]["spark"] == "4.1.2"
    _replay(spark, tmp_path / "warehouse", "ops", _TRUTH["steps"], _TRUTH["snapshots"])


def test_branch_ops_v3_lineage(spark: ReparkSession, tmp_path: Path) -> None:
    """Cherry-pick on a v3 table keeps Spark's row lineage."""
    _replay(
        spark,
        tmp_path / "warehouse",
        "ops3",
        _TRUTH["v3"]["steps"],
        _TRUTH["v3"]["snapshots"],
    )


def test_rollback_to_timestamp_invalid_typed_literal_keeps_full_parse_message(
    spark: ReparkSession,
) -> None:
    """A malformed TIMESTAMP literal keeps the branch-operations producer message."""
    sql = "CALL mem.system.rollback_to_timestamp('ns.ops', TIMESTAMP 'x')"
    expected = (
        '[INVALID_TYPED_LITERAL] The value of the typed literal "TIMESTAMP" is invalid: '
        "'x'. SQLSTATE: 42604"
    )
    with pytest.raises(ParseException) as caught:
        spark.sql(sql).to_arrow()
    message = str(caught.value)
    assert message.splitlines()[0] == expected
    assert "SQL error" not in message
    assert "ParserError(" not in message


def _live_rows(session: Any, sql: str) -> list[tuple[Any, Any]]:
    """Collect a live two-column row query as a sorted multiset."""
    return sorted((row["id"], row["s"]) for row in session.sql(sql).toArrow().to_pylist())


def _newest_metadata_file(table_root: Path) -> Path:
    """Return the newest table metadata file regardless of naming scheme."""
    metas = list((table_root / "metadata").glob("*.metadata.json"))
    assert metas, f"no metadata under {table_root}"
    return max(metas, key=lambda path: path.stat().st_mtime_ns)


def _live_adopt(repark: ReparkSession, table_root: Path, name: str) -> None:
    """Adopt the newest Spark metadata file under a fresh RePark table name."""
    newest = _newest_metadata_file(table_root)
    repark.sql(f"CALL rp.system.register_table(table => 'ns.{name}', metadata_file => '{newest}')")


def _spark_adopt(spark: Any, table_root: Path, catalog: str, name: str) -> str:
    """Adopt the newest metadata file (either engine's) as a fresh Spark table."""
    newest = _newest_metadata_file(table_root)
    spark.sql(
        f"CALL {catalog}.system.register_table(table => 'ns.{name}', metadata_file => '{newest}')"
    )
    return f"{catalog}.ns.{name}"


def _live_read_log(session: ReparkSession, catalog: str, table: str) -> list[dict[str, Any]]:
    """Return a live RePark table's snapshot log with positions."""
    arrow = session.sql(
        f"SELECT snapshot_id, parent_id, operation, committed_at "
        f"FROM {catalog}.{_NAMESPACE}.{table}.snapshots ORDER BY committed_at"
    ).to_arrow()
    log = [
        {
            "id": snap,
            "parent_id": parent,
            "operation": op,
            "ts": _to_utc_naive(ts),
        }
        for snap, parent, op, ts in zip(
            arrow.column("snapshot_id").to_pylist(),
            arrow.column("parent_id").to_pylist(),
            arrow.column("operation").to_pylist(),
            arrow.column("committed_at").to_pylist(),
            strict=True,
        )
    ]
    return [{"pos": i, **row} for i, row in enumerate(log)]


def _live_main_id(session: ReparkSession, catalog: str, table: str) -> int:
    """Return the snapshot id the live table's main branch points at."""
    arrow = session.sql(
        f"SELECT snapshot_id FROM {catalog}.{_NAMESPACE}.{table}.refs WHERE name = 'main'"
    ).to_arrow()
    return arrow.column("snapshot_id").to_pylist()[0]


def _live_select_older(log: list[dict[str, Any]], head: int, stamp: datetime) -> int:
    """Walk main ancestry for the latest snapshot strictly older than stamp."""
    by_id = {row["id"]: row for row in log}
    best: dict[str, Any] | None = None
    cursor: int | None = head
    seen = set()
    while cursor is not None and cursor not in seen:
        seen.add(cursor)
        row = by_id[cursor]
        if row["ts"] < stamp and (best is None or row["ts"] > best["ts"]):
            best = row
        cursor = row["parent_id"]
    assert best is not None, f"no ancestor older than {stamp} from {head}"
    return best["id"]


def _replay_adopted(
    spark: Any,
    repark: ReparkSession,
    spark_warehouse: Path,
    live_catalog: str,
    section: dict[str, Any],
) -> None:
    """Stage a recorded shape on live Spark, adopt it, and pin RePark's answer."""
    table = section["table"]
    for item in section["stage"]:
        if "sql" in item:
            spark.sql(item["sql"].replace("bo.", f"{live_catalog}.")).collect()
        elif "df_overwrite" in item:
            spec = item["df_overwrite"]
            frame = spark.createDataFrame(spec["rows"], ["id", "s"])
            frame.writeTo(spec["table"].replace("bo.", f"{live_catalog}.")).overwritePartitions()
        elif "conf_set" in item:
            for key, value in item["conf_set"].items():
                spark.conf.set(key, value)
        elif "conf_unset" in item:
            spark.conf.unset(item["conf_unset"])
        else:
            raise AssertionError(f"unrouted adopted stage item {item!r}")
        time.sleep(0.3)
    table_root = spark_warehouse / "ns" / table
    _live_adopt(repark, table_root, table)
    log = _live_read_log(repark, "rp", table)
    for step in section["steps"]:
        if "check_rows" in step:
            assert _live_rows(
                repark, f"SELECT id, s FROM rp.{_NAMESPACE}.{table} ORDER BY id"
            ) == sorted((row[0], row[1]) for row in step["rows"]), step["check_rows"]
            continue
        if "check_refs" in step:
            ids = {entry["id"]: entry["pos"] for entry in log}
            arrow = repark.sql(
                f"SELECT name, type, snapshot_id FROM rp.{_NAMESPACE}.{table}.refs"
            ).to_arrow()
            got = {
                name: {"type": kind, "pos": ids[snap]}
                for name, kind, snap in zip(
                    arrow.column("name").to_pylist(),
                    arrow.column("type").to_pylist(),
                    arrow.column("snapshot_id").to_pylist(),
                    strict=True,
                )
            }
            assert got == step["refs"], f"{table}.refs: got {got}, want {step['refs']}"
            continue
        if "check_summary" in step:
            by_pos = {entry["pos"]: entry["id"] for entry in log}
            snap_id = by_pos[step["pos"]]
            doc = json.loads(_newest_metadata_file(table_root).read_text(encoding="utf-8"))
            snap = next(s for s in doc["snapshots"] if s["snapshot-id"] == snap_id)
            for key, value in step["props"].items():
                assert snap["summary"].get(key) == value, (key, snap["summary"])
            continue
        sql = step["sql"].replace("bo.", "rp.")
        resolved = _resolve(sql, log)
        if "expect_error" in step:
            _assert_error(
                repark,
                resolved,
                step["expect_error"]["exc"],
                step["expect_error"].get("prefix"),
                log,
                step["expect_error"].get("needles"),
            )
            assert len(_live_read_log(repark, "rp", table)) == len(log)
            continue
        arrow = repark.sql(resolved).to_arrow()
        grown = _live_read_log(repark, "rp", table)
        if step.get("new_pos") is not None:
            assert len(grown) == step["new_pos"] + 1, (
                f"{resolved}: log length {len(grown)}, want {step['new_pos'] + 1}"
            )
            time.sleep(0.3)
            log = grown
        else:
            assert len(grown) == len(log), f"{resolved} grew the log"
        if "expect_call" in step:
            _check_call_output(
                arrow, resolved, step["expect_call"]["columns"], step["expect_call"]["rows"], grown
            )
    assert _slim(_live_read_log(repark, "rp", table)) == section["snapshots"]


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_live_branch_ops_cross_reads(tmp_path: Path) -> None:
    """Spark and RePark read back each other's procedure commits on one table."""
    import _live_parity as lp

    warehouse = tmp_path / "spark-warehouse"
    spark = lp.build_spark_iceberg_engine(warehouse, catalog="bo_live").session
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bo_live.ns")
    spark.sql(
        "CREATE TABLE bo_live.ns.ops (id INT, s STRING) USING iceberg "
        "TBLPROPERTIES ('format-version'='2')"
    )
    spark.sql("INSERT INTO bo_live.ns.ops VALUES (1, 'a'), (2, 'b')")
    time.sleep(0.3)
    spark.sql("INSERT INTO bo_live.ns.ops VALUES (4, 'd')")
    time.sleep(0.3)
    spark.sql("ALTER TABLE bo_live.ns.ops CREATE BRANCH feat")
    spark.sql("INSERT INTO bo_live.ns.ops.branch_feat VALUES (3, 'c')")
    time.sleep(0.3)
    spark.sql("CALL bo_live.system.fast_forward('ns.ops', 'main', 'feat')")

    repark = ReparkSession.builder.appName("ice-branch-ops-1-live").getOrCreate()
    try:
        repark.register_memory_catalog("rp", tmp_path / "repark-warehouse")
        repark.sql("CREATE NAMESPACE rp.ns")
        table_root = warehouse / "ns" / "ops"
        _live_adopt(repark, table_root, "ops")
        want = _live_rows(spark, "SELECT id, s FROM bo_live.ns.ops")
        assert _live_rows(repark, "SELECT id, s FROM rp.ns.ops") == want

        repark.sql("INSERT INTO rp.ns.ops VALUES (5, 'e')")
        time.sleep(0.3)
        back = _spark_adopt(spark, table_root, "bo_live", "ops_r1")
        assert _live_rows(spark, f"SELECT id, s FROM {back}") == _live_rows(
            repark, "SELECT id, s FROM rp.ns.ops"
        )

        spark.sql("ALTER TABLE bo_live.ns.ops CREATE BRANCH wip")
        spark.sql("INSERT INTO bo_live.ns.ops.branch_wip VALUES (6, 'f')")
        time.sleep(0.3)
        spark.sql("INSERT INTO bo_live.ns.ops VALUES (7, 'g')")
        time.sleep(0.3)
        staged = (
            spark.sql("SELECT snapshot_id FROM bo_live.ns.ops.refs WHERE name = 'wip'")
            .toArrow()
            .to_pylist()[0]["snapshot_id"]
        )
        spark.sql(f"CALL bo_live.system.cherrypick_snapshot('ns.ops', {staged})")
        _live_adopt(repark, table_root, "ops_cp")
        want = _live_rows(spark, "SELECT id, s FROM bo_live.ns.ops")
        assert _live_rows(repark, "SELECT id, s FROM rp.ns.ops_cp") == want

        head = repark.sql(
            "CALL rp.system.set_current_snapshot(table => 'ns.ops_cp', ref => 'wip')"
        ).to_arrow()
        assert head.schema.names == ["previous_snapshot_id", "current_snapshot_id"]
        back = _spark_adopt(spark, table_root, "bo_live", "ops_r2")
        assert _live_rows(spark, f"SELECT id, s FROM {back}") == _live_rows(
            repark, "SELECT id, s FROM rp.ns.ops_cp"
        )

        cp_log = _live_read_log(repark, "rp", "ops_cp")
        stamp = _fmt(cp_log[2]["ts"])
        pre_main = _live_main_id(repark, "rp", "ops_cp")
        want_main = _live_select_older(cp_log, pre_main, cp_log[2]["ts"])
        repark.sql(
            f"CALL rp.system.rollback_to_timestamp('ns.ops_cp', TIMESTAMP '{stamp}')"
        ).to_arrow()
        assert _live_main_id(repark, "rp", "ops_cp") == want_main
        back = _spark_adopt(spark, table_root, "bo_live", "ops_r3")
        assert _live_rows(spark, f"SELECT id, s FROM {back}") == _live_rows(
            repark, "SELECT id, s FROM rp.ns.ops_cp"
        )
    finally:
        repark.stop()


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_live_branch_ops_adopted_shapes(tmp_path: Path) -> None:
    """RePark cherry-picks Spark-staged dynamic-overwrite and WAP snapshots."""
    import _live_parity as lp

    assert _TRUTH["oracle"]["spark"] == "4.1.2"
    warehouse = tmp_path / "adopt-warehouse"
    spark = lp.build_spark_iceberg_engine(warehouse, catalog="bo_adopt").session
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bo_adopt.ns")
    repark = ReparkSession.builder.appName("ice-branch-ops-1-adopt").getOrCreate()
    try:
        repark.register_memory_catalog("rp", tmp_path / "adopt-repark-warehouse")
        repark.sql("CREATE NAMESPACE rp.ns")
        _replay_adopted(spark, repark, warehouse, "bo_adopt", _TRUTH["adopted"]["dyn"])
        _replay_adopted(spark, repark, warehouse, "bo_adopt", _TRUTH["adopted"]["wap"])
    finally:
        repark.stop()


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_live_branch_ops_errors_match_recorded_prefixes(tmp_path: Path) -> None:
    """Live Spark still refuses the recorded error shapes with the same text."""
    import _live_parity as lp

    warehouse = tmp_path / "spark-warehouse"
    spark = lp.build_spark_iceberg_engine(warehouse, catalog="bo_err").session
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bo_err.ns")
    spark.sql(
        "CREATE TABLE bo_err.ns.err (id INT) USING iceberg TBLPROPERTIES ('format-version'='2')"
    )
    spark.sql("INSERT INTO bo_err.ns.err VALUES (1)")
    spark.sql("ALTER TABLE bo_err.ns.err CREATE BRANCH b")
    for sql, needle in [
        ("CALL bo_err.system.fast_forward('ns.err', 'main', 'nope')", "Ref does not exist"),
        (
            "CALL bo_err.system.set_current_snapshot('ns.err', 1, 'b')",
            "Either snapshot_id or ref must be provided",
        ),
        (
            "CALL bo_err.system.rollback_to_timestamp('ns.err', TIMESTAMP '2000-01-01 00:00:00')",
            "Cannot roll back, no valid snapshot older than",
        ),
        (
            "CALL bo_err.system.cherrypick_snapshot('ns.err', 123456789)",
            "Cannot cherry-pick unknown snapshot ID",
        ),
    ]:
        try:
            spark.sql(sql).collect()
            raise AssertionError(f"live Spark accepted {sql!r}")
        except Exception as exc:
            assert needle in str(exc), f"{sql}: {needle!r} not in {exc}"
