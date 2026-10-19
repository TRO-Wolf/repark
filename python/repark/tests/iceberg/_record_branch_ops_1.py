"""Spark oracle recorder for ICE-BRANCH-OPS-1.

Runs the scripted branch-procedure shapes on live PySpark 4.1.2 + Iceberg 1.11.0
and writes the truth JSON the pin suite replays. Snapshot ids are random per run,
so every id-typed value is recorded as a position in the time-ordered snapshot
log, and every id-taking CALL is stored as a template (``{snap:3}``,
``{ts:3}``, ``{ts_mid:9:11}``, ``{ts_ny_mid:9:11}``) the suite resolves against
its own log. The suite replays ``steps`` in order, so the recorder also stores
each step's log growth (``new_pos``) as the structural pin.

Regenerate only on a new oracle recording basis (same GAV as
:mod:`_oracle_pins`). Needs pyspark + a JVM (the ``record`` extra); routine CI
never runs this module. Usage::

    .venv/bin/python python/repark/tests/_record_branch_ops_1.py <truth-out-path>

pins: ice-branch-ops-1/C-001, C-002, C-003, C-004, C-005
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

TABLE_DDL = "(id INT, s STRING) USING iceberg TBLPROPERTIES ('format-version'='2')"
V3_DDL = "(id INT, s STRING) USING iceberg TBLPROPERTIES ('format-version'='3')"
SLEEP = 1.2


def _values(rows: list[tuple[int, str]]) -> str:
    """Render row tuples as a Spark VALUES list."""
    return ",".join(f"({i},'{s}')" for i, s in rows)


class Recorder:
    """One Spark session producing one ordered step list plus a truth document."""

    def __init__(self, warehouse: Path) -> None:
        """Create the recorder against a fresh warehouse."""
        from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV
        from pyspark.sql import SparkSession

        self.spark = (
            SparkSession.builder.master("local[2]")
            .appName("ice-branch-ops-1-record")
            .config("spark.driver.memory", "2g")
            .config("spark.ui.enabled", "false")
            .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
            .config(
                "spark.jars.ivy",
                os.environ.get("BRANCH_OPS_IVY", str(Path.home() / ".ivy2")),
            )
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.bo", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.bo.type", "hadoop")
            .config("spark.sql.catalog.bo.warehouse", str(warehouse))
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("ERROR")
        self.banner = {
            "spark_version": self.spark.version,
            "session_tz": self.spark.conf.get("spark.sql.session.timeZone"),
        }

    def stop(self) -> None:
        """Stop the Spark session."""
        self.spark.stop()


class Script:
    """Ordered step builder over one table; positions come from its live log."""

    def __init__(self, rec: Recorder, table: str) -> None:
        """Bind the builder to a recorder and a fully qualified table name."""
        self.rec = rec
        self.table = table
        self.steps: list[dict[str, Any]] = []

    def log(self) -> list[dict[str, Any]]:
        """Return the table's time-ordered snapshot log with positions."""
        try:
            arrow = self.rec.spark.sql(
                f"SELECT snapshot_id, parent_id, operation, "
                f"CAST(committed_at AS STRING) AS ts FROM {self.table}.snapshots "
                f"ORDER BY committed_at"
            ).toArrow()
        except Exception:
            return []
        rows = arrow.to_pylist()
        return [
            {
                "pos": i,
                "id": r["snapshot_id"],
                "parent_id": r["parent_id"],
                "operation": r["operation"],
                "ts": r["ts"],
            }
            for i, r in enumerate(rows)
        ]

    def pos_of(self, snap_id: int) -> int:
        """Return the log position of a snapshot id."""
        for entry in self.log():
            if entry["id"] == snap_id:
                return entry["pos"]
        raise AssertionError(f"snapshot {snap_id} missing from {self.table} log")

    def snap(self, snap_id: int) -> str:
        """Render a snapshot id as a position marker template."""
        return f"{{snap:{self.pos_of(snap_id)}}}"

    def ref(self, name: str) -> int | None:
        """Return the snapshot id a ref points at, or None when absent."""
        rows = (
            self.rec.spark.sql(f"SELECT snapshot_id FROM {self.table}.refs WHERE name = '{name}'")
            .toArrow()
            .to_pylist()
        )
        return rows[0]["snapshot_id"] if rows else None

    def resolve(self, template: str) -> str:
        """Substitute position markers against the live log for execution."""
        entries = {entry["pos"]: entry for entry in self.log()}
        out = template
        for token in sorted(set(re.findall(r"\{snap:\d+\}", template))):
            out = out.replace(token, str(entries[int(token[6:-1])]["id"]))
        for token in sorted(set(re.findall(r"\{ts:\d+\}", template))):
            out = out.replace(token, entries[int(token[4:-1])]["ts"])
        for token in sorted(set(re.findall(r"\{ts_mid:\d+:\d+\}", template))):
            first, second = token[8:-1].split(":")
            out = out.replace(token, _mid_ts(entries[int(first)]["ts"], entries[int(second)]["ts"]))
        for token in sorted(set(re.findall(r"\{ts_ny_mid:\d+:\d+\}", template))):
            first, second = token[11:-1].split(":")
            mid = _mid_ts(entries[int(first)]["ts"], entries[int(second)]["ts"])
            out = out.replace(token, _in_zone(mid, "America/New_York"))
        return out

    def do(self, sql: str) -> None:
        """Run a mutating statement and record its log growth."""
        before = len(self.log())
        self.rec.spark.sql(self.resolve(sql)).collect()
        time.sleep(SLEEP)
        after = self.log()
        self.steps.append({"sql": sql, "new_pos": len(after) - 1 if len(after) > before else None})

    def note(self, label: str, payload: dict[str, Any]) -> None:
        """Append a non-executing documentation step."""
        self.steps.append({"note": label, **payload})

    def rows_now(self) -> list[list[Any]]:
        """Collect current (id, s) rows unordered."""
        return [
            [r["id"], r["s"]]
            for r in self.rec.spark.sql(f"SELECT id, s FROM {self.table}").toArrow().to_pylist()
        ]

    def refs_now(self) -> dict[str, dict[str, Any]]:
        """Map every ref to its type plus snapshot position."""
        log = self.log()
        ids = {entry["id"]: entry["pos"] for entry in log}
        rows = (
            self.rec.spark.sql(f"SELECT name, type, snapshot_id FROM {self.table}.refs")
            .toArrow()
            .to_pylist()
        )
        return {r["name"]: {"type": r["type"], "pos": ids[r["snapshot_id"]]} for r in rows}

    def expect_rows(self, label: str) -> None:
        """Pin the current row multiset."""
        self.steps.append({"check_rows": label, "rows": sorted(self.rows_now())})

    def expect_refs(self, label: str) -> None:
        """Pin the current ref map."""
        self.steps.append({"check_refs": label, "refs": self.refs_now()})

    def summary_now(self) -> dict[str, str]:
        """Return the newest snapshot's summary as string pairs."""
        rows = (
            self.rec.spark.sql(f"SELECT summary FROM {self.table}.snapshots ORDER BY committed_at")
            .toArrow()
            .to_pylist()
        )
        return {str(k): str(v) for k, v in rows[-1]["summary"]}

    def expect_summary(self, label: str, keys: list[str]) -> None:
        """Pin deterministic summary properties of the newest snapshot."""
        summary = self.summary_now()
        self.steps.append(
            {
                "check_summary": label,
                "pos": len(self.log()) - 1,
                "props": {key: summary[key] for key in keys},
            }
        )

    def call(self, template: str, label: str) -> None:
        """Run a CALL template and pin its output or error cell."""
        before = len(self.log())
        try:
            arrow = self.rec.spark.sql(self.resolve(template)).toArrow()
            schema = [[f.name, str(f.type)] for f in arrow.schema]
            rows = [[repr(v) for v in row.values()] for row in arrow.to_pylist()]
            cell: dict[str, Any] = {"ok": True, "schema": schema, "rows": rows}
        except Exception as exc:
            cell = {"ok": False, "exc": type(exc).__name__, "msg": str(exc)}
        after = self.log()
        step: dict[str, Any] = {
            "sql": template,
            "label": label,
            "new_pos": len(after) - 1 if len(after) > before else None,
        }
        if cell["ok"]:
            step["expect_call"] = {
                "columns": cell["schema"],
                "rows": _resolve_rows(after, cell["rows"]),
            }
        elif label == "rt_bad_typed":
            step["expect_error"] = _typed_literal_needles(cell)
        else:
            step["expect_error"] = _error_cell(cell)
        self.steps.append(step)


def _resolve_rows(log: list[dict[str, Any]], rows: list[list[str]]) -> list[list[Any]]:
    """Map repr'd snapshot-id cells to ``{"pos": n}`` markers."""
    ids = {entry["id"]: entry["pos"] for entry in log}
    resolved = []
    for row in rows:
        out = []
        for cell in row:
            try:
                num = int(cell)
            except ValueError:
                num = None
            if num is not None and num in ids:
                out.append({"pos": ids[num]})
            elif cell == "None":
                out.append(None)
            elif len(cell) >= 2 and cell.startswith("'") and cell.endswith("'"):
                out.append({"lit": cell[1:-1]})
            else:
                out.append({"lit": cell})
        resolved.append(out)
    return resolved


def _error_cell(cell: dict[str, Any]) -> dict[str, Any]:
    """Reduce a raw error cell to the exception class + operative message."""
    java = next(
        (line.strip() for line in cell["msg"].splitlines() if "org.apache.iceberg" in line),
        cell["msg"].strip().splitlines()[0][:300],
    )
    chunks = java.split(": ", 2)
    message = chunks[2] if len(chunks) == 3 else java
    return {"exc": cell["exc"], "prefix": message[:300]}


def _typed_literal_needles(cell: dict[str, Any]) -> dict[str, Any]:
    """Pin a typed-literal refusal as its quoteless spans.

    The RePark Parse door Debug-renders the sqlparser error, which escapes every
    double quote, so one contiguous needle can never match Spark's operative line.
    Both spans stay verbatim Spark text.
    """
    operative = next(
        line.strip() for line in cell["msg"].splitlines() if "INVALID_TYPED_LITERAL" in line
    )
    parts = operative.split('"')
    assert len(parts) == 3, operative
    return {"exc": cell["exc"], "needles": [parts[0], parts[2]]}


def _slim(log: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Reduce a snapshot log to positions, parents, and operations."""
    ids = {entry["id"]: entry["pos"] for entry in log}
    return [
        {
            "pos": e["pos"],
            "parent_pos": ids.get(e["parent_id"]) if e["parent_id"] is not None else None,
            "operation": e["operation"],
        }
        for e in log
    ]


def _mid_ts(first: str, second: str) -> str:
    """Return the midpoint UTC wall clock between two snapshot timestamps."""
    left = _parse_ts(first)
    right = _parse_ts(second)
    return (left + (right - left) / 2).strftime("%Y-%m-%d %H:%M:%S.%f")


def _parse_ts(raw: str) -> datetime:
    """Parse a snapshot timestamp with or without fractional seconds."""
    try:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=UTC)
    except ValueError:
        return datetime.strptime(raw.split(".")[0], "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)


def _mark_ids(text: str, idmap: dict[int, int]) -> str:
    """Replace snapshot ids in text with ``{snap:POS}`` markers."""
    parts = []
    for token in re.split(r"(\d{9,})", text):
        if token.isdigit() and int(token) in idmap:
            parts.append(f"{{snap:{idmap[int(token)]}}}")
        else:
            parts.append(token)
    return "".join(parts)


def _in_zone(wall_utc: str, zone: str) -> str:
    """Render a UTC wall clock in an IANA zone."""
    from zoneinfo import ZoneInfo

    instant = datetime.strptime(wall_utc, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=UTC)
    return instant.astimezone(ZoneInfo(zone)).strftime("%Y-%m-%d %H:%M:%S.%f")


def record(warehouse: Path, out: Path) -> None:
    """Run the full script on both tables and write the truth JSON."""
    rec = Recorder(warehouse)
    try:
        ops = Script(rec, "bo.ns.ops")
        rec.spark.sql("CREATE NAMESPACE IF NOT EXISTS bo.ns").collect()
        ops.do("CREATE TABLE bo.ns.ops " + TABLE_DDL)
        ops.do("INSERT INTO bo.ns.ops VALUES " + _values([(1, "a")]))
        ops.do("ALTER TABLE bo.ns.ops CREATE BRANCH old")
        ops.do("INSERT INTO bo.ns.ops VALUES " + _values([(2, "b")]))
        ops.do("ALTER TABLE bo.ns.ops CREATE TAG t1")
        ops.do("ALTER TABLE bo.ns.ops CREATE BRANCH feat")
        ops.do("INSERT INTO bo.ns.ops.branch_feat VALUES " + _values([(4, "d")]))
        ops.call("CALL bo.system.fast_forward('ns.ops', 'main', 'feat')", "ff_positional")
        ops.expect_refs("refs_after_ff")
        ops.do("INSERT INTO bo.ns.ops VALUES " + _values([(3, "c")]))
        ops.do("ALTER TABLE bo.ns.ops CREATE BRANCH feat2")
        ops.do("INSERT INTO bo.ns.ops.branch_feat2 VALUES " + _values([(6, "f")]))
        ops.call(
            "CALL bo.system.fast_forward(table => 'ns.ops', branch => 'main', to => 'feat2')",
            "ff_named",
        )
        ops.call("CALL bo.system.fast_forward('ns.ops', 'feat2', 'feat2')", "ff_same")
        ops.call("CALL bo.system.fast_forward('ns.ops', 'main', 'nope')", "ff_unknown_to")
        ops.call("CALL bo.system.fast_forward('ns.ops', 't1', 'feat2')", "ff_tag_as_branch")
        ops.call("CALL bo.system.fast_forward('ns.ops', 'main', 'old')", "ff_not_descendant")
        ops.call("CALL bo.system.fast_forward('ns.ops', 'newb', 'feat2')", "ff_unknown_branch")
        ops.call(
            "CALL bo.system.fast_forward(table => 'ns.ops', branch => 'newb', to => 't1')",
            "ff_to_tag",
        )
        ops.call("CALL bo.system.fast_forward('ns.ops', 'old', 't1')", "ff_tag_forward")
        ops.do("ALTER TABLE bo.ns.ops CREATE BRANCH wip")
        ops.do("INSERT INTO bo.ns.ops.branch_wip VALUES " + _values([(7, "g")]))
        ops.do("INSERT INTO bo.ns.ops VALUES " + _values([(8, "h")]))
        wip = ops.ref("wip")
        assert wip is not None
        ops.call(
            "CALL bo.system.cherrypick_snapshot('ns.ops', " + ops.snap(wip) + ")",
            "cp_positional",
        )
        ops.call(
            "CALL bo.system.cherrypick_snapshot("
            "table => 'ns.ops', snapshot_id => " + ops.snap(wip) + ")",
            "cp_duplicate",
        )
        ops.call("CALL bo.system.cherrypick_snapshot('ns.ops', 123456789)", "cp_unknown")
        first = ops.log()[0]["id"]
        ops.call(
            "CALL bo.system.cherrypick_snapshot('ns.ops', " + ops.snap(first) + ")",
            "cp_ancestor",
        )
        ops.do("ALTER TABLE bo.ns.ops CREATE BRANCH delb")
        ops.do("DELETE FROM bo.ns.ops.branch_delb WHERE id = 1")
        ops.do("INSERT INTO bo.ns.ops VALUES " + _values([(9, "i")]))
        dele = ops.ref("delb")
        assert dele is not None
        ops.call(
            "CALL bo.system.cherrypick_snapshot('ns.ops', " + ops.snap(dele) + ")",
            "cp_delete",
        )
        ops.do("ALTER TABLE bo.ns.ops CREATE BRANCH ovb")
        ops.do("INSERT OVERWRITE bo.ns.ops.branch_ovb VALUES " + _values([(10, "j")]))
        ops.do("INSERT INTO bo.ns.ops VALUES " + _values([(11, "k")]))
        over = ops.ref("ovb")
        assert over is not None
        ops.call(
            "CALL bo.system.cherrypick_snapshot('ns.ops', " + ops.snap(over) + ")",
            "cp_overwrite_static",
        )
        log = ops.log()
        ops.call(
            "CALL bo.system.rollback_to_timestamp('ns.ops', TIMESTAMP '{ts:2}')",
            "rt_positional",
        )
        ops.expect_rows("rows_after_rt")
        ops.call(
            "CALL bo.system.rollback_to_timestamp(table => 'ns.ops', "
            "timestamp => TIMESTAMP '{ts:2}')",
            "rt_named",
        )
        ops.do("CALL bo.system.set_current_snapshot(table => 'ns.ops', ref => 'feat2')")
        log = ops.log()
        head_pos = ops.pos_of(ops.ref("main") or -1)
        ops.call(
            f"CALL bo.system.rollback_to_timestamp('ns.ops', TIMESTAMP '{{ts:{head_pos}}}')",
            "rt_exact_equal",
        )
        ops.call(
            "CALL bo.system.rollback_to_timestamp('ns.ops', TIMESTAMP '2000-01-01 00:00:00')",
            "rt_before_first",
        )
        log = ops.log()
        ops.do("CALL bo.system.set_current_snapshot('ns.ops', " + ops.snap(log[11]["id"]) + ")")
        log = ops.log()
        mid = _mid_ts(log[9]["ts"], log[11]["ts"])
        ny_mid = _in_zone(mid, "America/New_York")
        ops.note("tz_setup", {"mid_utc": mid, "mid_ny": ny_mid})
        probe = (
            rec.spark.sql(f"SELECT UNIX_MICROS(TIMESTAMP '{ny_mid}') AS u").toArrow().to_pylist()
        )
        ops.note("tz_literal_eval", {"unix_micros": probe[0]["u"]})
        rec.spark.conf.set("spark.sql.session.timeZone", "America/New_York")
        probe_ny = (
            rec.spark.sql(f"SELECT UNIX_MICROS(TIMESTAMP '{ny_mid}') AS u").toArrow().to_pylist()
        )
        ops.note("tz_literal_eval_ny", {"unix_micros": probe_ny[0]["u"]})
        ops.call(
            "CALL bo.system.rollback_to_timestamp('ns.ops', TIMESTAMP '{ts_ny_mid:9:11}')",
            "rt_tz_ny",
        )
        ops.steps[-1]["zone"] = "America/New_York"
        ops.call(
            "CALL bo.system.rollback_to_timestamp('ns.ops', TIMESTAMP '{ts_mid:9:11}')",
            "rt_tz_utc_wall_in_ny",
        )
        ops.steps[-1]["zone"] = "America/New_York"
        rec.spark.conf.set("spark.sql.session.timeZone", "UTC")
        ops.expect_refs("refs_after_rt")
        log = ops.log()
        pos0 = log[0]["id"]
        ops.call(
            "CALL bo.system.set_current_snapshot('ns.ops', " + ops.snap(pos0) + ")",
            "sc_by_id",
        )
        ops.expect_rows("rows_after_sc_id")
        ops.call(
            "CALL bo.system.set_current_snapshot(table => 'ns.ops', ref => 'feat2')",
            "sc_by_ref_named",
        )
        ops.call(
            "CALL bo.system.set_current_snapshot(table => 'ns.ops', ref => 't1')",
            "sc_ref_to_tag",
        )
        ops.call(
            "CALL bo.system.set_current_snapshot("
            "table => 'ns.ops', snapshot_id => " + ops.snap(pos0) + ")",
            "sc_by_id_named",
        )
        ops.call(
            "CALL bo.system.set_current_snapshot('ns.ops', " + ops.snap(pos0) + ", 'feat2')",
            "sc_both",
        )
        ops.call("CALL bo.system.set_current_snapshot('ns.ops')", "sc_neither")
        ops.call("CALL bo.system.set_current_snapshot('ns.ops', 123456789)", "sc_unknown_id")
        ops.call(
            "CALL bo.system.set_current_snapshot(table => 'ns.ops', ref => 'nope')",
            "sc_unknown_ref",
        )
        ops.call("CALL bo.system.rollback_to_timestamp('ns.ops', '{ts:5}')", "rt_string_utc")
        rec.spark.conf.set("spark.sql.session.timeZone", "America/New_York")
        ops.call("CALL bo.system.rollback_to_timestamp('ns.ops', '{ts:5}')", "rt_string_ny")
        ops.steps[-1]["zone"] = "America/New_York"
        rec.spark.conf.set("spark.sql.session.timeZone", "UTC")
        ops.expect_rows("final_rows")
        ops.expect_refs("final_refs")
        ops.call("CALL bo.system.fast_forward('ns.ops', '', 'feat2')", "ff_empty_branch")
        ops.call("CALL bo.system.fast_forward('ns.ops', '   ', 'feat2')", "ff_ws_branch")
        ops.call("CALL bo.system.cherrypick_snapshot('ns.ops')", "cp_missing_arg")
        ops.call("CALL bo.system.cherrypick_snapshot('ns.ops', 'feat2')", "cp_wrongtype_str")
        ops.call("CALL bo.system.rollback_to_timestamp('ns.ops')", "rt_missing_arg")
        ops.call("CALL bo.system.rollback_to_timestamp('ns.ops', 123)", "rt_int_arg")
        ops.call(
            "CALL bo.system.rollback_to_timestamp(table => 'ns.ops', timestamp => 123)",
            "rt_named_int_arg",
        )
        ops.call("CALL bo.system.rollback_to_timestamp('ns.ops', 'not-a-time')", "rt_bad_string")
        ops.call(
            "CALL bo.system.rollback_to_timestamp('ns.ops', TIMESTAMP 'not-a-time')",
            "rt_bad_typed",
        )
        wip = ops.ref("wip")
        assert wip is not None
        ops.do("CALL bo.system.set_current_snapshot('ns.ops', " + ops.snap(wip) + ")")
        ops.call(
            "CALL bo.system.rollback_to_timestamp('ns.ops', TIMESTAMP '{ts:11}')",
            "rt_lateral",
        )
        ops.expect_rows("rows_after_rt_lateral")
        ops.do("CALL bo.system.set_current_snapshot('ns.ops', " + ops.snap(log[11]["id"]) + ")")
        ops.call(
            "CALL bo.system.rollback_to_timestamp(table => 'ns.ops', "
            "timestamp => TIMESTAMP '{ts:2}')",
            "rt_named2",
        )
        ops.expect_rows("rows_after_rt_named2")
        ops.do("CALL bo.system.set_current_snapshot('ns.ops', " + ops.snap(log[11]["id"]) + ")")
        ops.call("CALL bo.system.rollback_to_timestamp('ns.ops', '{ts:2}')", "rt_string2")
        ops.expect_rows("rows_after_rt_string2")
        ops.do("ALTER TABLE bo.ns.ops CREATE BRANCH ffb")
        ops.do("INSERT INTO bo.ns.ops.branch_ffb VALUES " + _values([(12, "l")]))
        ffb = ops.ref("ffb")
        assert ffb is not None
        ops.call(
            "CALL bo.system.cherrypick_snapshot('ns.ops', " + ops.snap(ffb) + ")",
            "cp_ff",
        )
        ops.expect_rows("rows_after_cp_ff")
        ops.expect_refs("refs_after_cp_ff")
        ops.do("ALTER TABLE bo.ns.ops CREATE BRANCH ffdel")
        ops.do("DELETE FROM bo.ns.ops.branch_ffdel WHERE id = 12")
        ffdel = ops.ref("ffdel")
        assert ffdel is not None
        ops.call(
            "CALL bo.system.cherrypick_snapshot('ns.ops', " + ops.snap(ffdel) + ")",
            "cp_ff_delete",
        )
        ops.expect_rows("rows_after_cp_ff_delete")
        ops.expect_refs("r2_final_refs")

        v3 = Script(rec, "bo.ns.ops3")
        v3.do("CREATE TABLE bo.ns.ops3 " + V3_DDL)
        v3.do("INSERT INTO bo.ns.ops3 VALUES " + _values([(1, "a")]))
        v3.do("ALTER TABLE bo.ns.ops3 CREATE BRANCH b1")
        v3.do("UPDATE bo.ns.ops3.branch_b1 SET s = 'z' WHERE id = 1")
        v3.do("INSERT INTO bo.ns.ops3.branch_b1 VALUES " + _values([(2, "b")]))
        v3.do("INSERT INTO bo.ns.ops3 VALUES " + _values([(3, "c")]))
        b1 = v3.ref("b1")
        assert b1 is not None
        v3.call("CALL bo.system.cherrypick_snapshot('ns.ops3', " + v3.snap(b1) + ")", "ops3_cp")
        row_ids = (
            rec.spark.sql("SELECT _row_id, id FROM bo.ns.ops3 ORDER BY id").toArrow().to_pylist()
        )
        v3.steps.append(
            {"check_row_ids": "ops3_row_ids", "rows": [[r["id"], r["_row_id"]] for r in row_ids]}
        )
        metas = sorted((warehouse / "ns" / "ops3" / "metadata").glob("v*.metadata.json"))
        newest = max(metas, key=lambda p: p.stat().st_mtime_ns)
        v3.steps.append(
            {
                "check_next_row_id": "ops3_next_row_id",
                "value": json.loads(newest.read_text(encoding="utf-8")).get("next-row-id"),
            }
        )
        v3.expect_rows("ops3_rows")

        dyn = Script(rec, "bo.ns.ops_dyn")
        dyn_stage: list[dict[str, Any]] = [
            {
                "sql": "CREATE TABLE bo.ns.ops_dyn (id INT, s STRING) USING iceberg "
                "PARTITIONED BY (s) TBLPROPERTIES ('format-version'='2')"
            },
            {"sql": "INSERT INTO bo.ns.ops_dyn VALUES " + _values([(1, "a"), (2, "b")])},
            {"sql": "ALTER TABLE bo.ns.ops_dyn CREATE BRANCH db"},
        ]
        for item in dyn_stage:
            rec.spark.sql(item["sql"]).collect()
            time.sleep(SLEEP)
        frame = rec.spark.sql("SELECT 10 AS id, 'a' AS s UNION ALL SELECT 20, 'b'")
        frame.writeTo("bo.ns.ops_dyn.branch_db").overwritePartitions()
        time.sleep(SLEEP)
        dyn_stage.append(
            {
                "df_overwrite": {
                    "table": "bo.ns.ops_dyn.branch_db",
                    "rows": [[10, "a"], [20, "b"]],
                }
            }
        )
        rec.spark.sql("INSERT INTO bo.ns.ops_dyn VALUES " + _values([(3, "c")])).collect()
        time.sleep(SLEEP)
        dyn_stage.append({"sql": "INSERT INTO bo.ns.ops_dyn VALUES " + _values([(3, "c")])})
        db = dyn.ref("db")
        assert db is not None
        dyn.call(
            "CALL bo.system.cherrypick_snapshot('ns.ops_dyn', " + dyn.snap(db) + ")",
            "dyn_pick",
        )
        dyn.expect_rows("dyn_rows")
        dyn.expect_refs("dyn_refs")

        wap = Script(rec, "bo.ns.ops_wap")
        wap_stage: list[dict[str, Any]] = [
            {
                "sql": "CREATE TABLE bo.ns.ops_wap (id INT, s STRING) USING iceberg "
                "TBLPROPERTIES ('format-version'='2')"
            },
            {"sql": "INSERT INTO bo.ns.ops_wap VALUES " + _values([(1, "a")])},
            {"sql": "ALTER TABLE bo.ns.ops_wap SET TBLPROPERTIES ('write.wap.enabled'='true')"},
        ]
        for item in wap_stage:
            rec.spark.sql(item["sql"]).collect()
            time.sleep(SLEEP)
        rec.spark.conf.set("spark.wap.id", "r2wapid")
        wap_stage.append({"conf_set": {"spark.wap.id": "r2wapid"}})
        before_wap = {entry["id"] for entry in wap.log()}
        rec.spark.sql("INSERT INTO bo.ns.ops_wap VALUES " + _values([(2, "b")])).collect()
        time.sleep(SLEEP)
        staged_wap = [entry for entry in wap.log() if entry["id"] not in before_wap]
        assert len(staged_wap) == 1
        wap_stage.append({"sql": "INSERT INTO bo.ns.ops_wap VALUES " + _values([(2, "b")])})
        rec.spark.sql(
            "ALTER TABLE bo.ns.ops_wap UNSET TBLPROPERTIES ('write.wap.enabled')"
        ).collect()
        time.sleep(SLEEP)
        wap_stage.append(
            {"sql": "ALTER TABLE bo.ns.ops_wap UNSET TBLPROPERTIES ('write.wap.enabled')"}
        )
        rec.spark.sql("INSERT INTO bo.ns.ops_wap VALUES " + _values([(3, "c")])).collect()
        time.sleep(SLEEP)
        wap_stage.append({"sql": "INSERT INTO bo.ns.ops_wap VALUES " + _values([(3, "c")])})
        wap.call(
            "CALL bo.system.cherrypick_snapshot('ns.ops_wap', "
            + wap.snap(staged_wap[0]["id"])
            + ")",
            "wap_pick",
        )
        wap.expect_summary("wap_summary", ["published-wap-id"])
        wap.expect_rows("wap_rows")
        wap.call(
            "CALL bo.system.cherrypick_snapshot('ns.ops_wap', "
            + wap.snap(staged_wap[0]["id"])
            + ")",
            "wap_pick_dup",
        )

        final_log = ops.log()
        idmap = {entry["id"]: entry["pos"] for entry in final_log}
        idmap.update({entry["id"]: entry["pos"] for entry in v3.log()})
        idmap.update({entry["id"]: entry["pos"] for entry in dyn.log()})
        idmap.update({entry["id"]: entry["pos"] for entry in wap.log()})
        for step in dyn.steps + wap.steps:
            if "expect_error" in step:
                step["expect_error"]["prefix"] = _mark_ids(step["expect_error"]["prefix"], idmap)
        for step in ops.steps:
            if "expect_error" in step:
                step["expect_error"]["prefix"] = _mark_ids(step["expect_error"]["prefix"], idmap)
                if step.get("label") == "rt_tz_ny":
                    step["expect_error"]["prefix"] = re.sub(
                        r"older than: \d+",
                        "older than: {ms_of_arg}",
                        step["expect_error"]["prefix"],
                    )
        for step in v3.steps:
            if "expect_error" in step:
                step["expect_error"]["prefix"] = _mark_ids(step["expect_error"]["prefix"], idmap)
        doc = {
            "unit": "ice-branch-ops-1",
            "oracle": {
                "spark": rec.banner["spark_version"],
                "session_tz": rec.banner["session_tz"],
                "iceberg": "1.11.0",
            },
            "table": {"namespace": "ns", "name": "ops", "format_version": 2},
            "steps": ops.steps,
            "snapshots": _slim(final_log),
            "v3": {"steps": v3.steps, "snapshots": _slim(v3.log())},
            "adopted": {
                "dyn": {
                    "table": "ops_dyn",
                    "stage": dyn_stage,
                    "steps": dyn.steps,
                    "snapshots": _slim(dyn.log()),
                },
                "wap": {
                    "table": "ops_wap",
                    "stage": wap_stage,
                    "steps": wap.steps,
                    "snapshots": _slim(wap.log()),
                },
            },
        }
        out.write_text(json.dumps(doc, indent=1, default=str), encoding="utf-8")
    finally:
        rec.stop()


def main(argv: list[str]) -> int:
    """Record the oracle into the truth path argument."""
    out = Path(argv[1])
    if len(argv) > 2:
        warehouse = Path(argv[2])
    else:
        default = os.environ.get("BRANCH_OPS_WAREHOUSE")
        warehouse = Path(default) if default else Path(tempfile.mkdtemp(prefix="branch-ops-"))
    if warehouse.exists():
        shutil.rmtree(warehouse)
    warehouse.mkdir(parents=True)
    record(warehouse, out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
