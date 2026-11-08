"""Door protection probe: what the Python native door does with the router's protected statements.

One invocation runs one leg (native, facade or spark) and writes a leg header line and one JSON
line per cell. ``--render`` reads the three leg files and writes the measured table. Measurement
only: no product code is touched and no result is judged against a Trino oracle.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import re
import sys
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any

ANSWERED = "ANSWERED"
REFUSED = "REFUSED"
PANIC = "PANIC"
SETUP_REFUSED = "SETUP-REFUSED"
PROTECTED = "PROTECTED"
WRONG_WRITE = "UNPROTECTED-WRONG-WRITE"
WRONG_READ = "UNPROTECTED-WRONG-READ"
OTHER = "UNPROTECTED-OTHER"
NOT_REACHABLE = "NOT-REACHABLE"
NOT_MEASURED = "NOT-MEASURED"
VERDICTS = (PROTECTED, WRONG_WRITE, WRONG_READ, OTHER, NOT_REACHABLE, PANIC, NOT_MEASURED)
CATALOGS = {"native": "ice", "facade": "ice", "spark": "hc"}
NAMESPACE = "ns"
DOORS = ("native", "facade", "spark")
T_SEED = ("id INT, v INT", "(1, 10), (2, 20), (3, 30)")
S_SEED = ("id INT, w INT", "(2, 20), (3, 99)")
N_SEED = ("id INT, v INT", "(1, 10), (2, NULL), (3, 30)")
R_SEED = ("id INT NOT NULL, v INT", "(1, 10)")
BIG_SEED = ("id INT, v INT", "(1, 2147483647)")
BASE = [[1, 10], [2, 20], [3, 30]]
REFUSE = "refuse"
EXECUTE = "execute"
UNMEASURED = "unmeasured"


def cell(
    cid: str, row: str, title: str, sql: str, router: str, basis: str, **extra: Any
) -> dict[str, Any]:
    """Build one cell spec with the defaults every cell shares."""
    spec: dict[str, Any] = {
        "id": cid,
        "row": row,
        "title": title,
        "sql": sql,
        "router": router,
        "basis": basis,
        "tables": {"t": T_SEED},
        "pre": [],
        "kind": "write",
        "expect_rows": None,
        "native": "run",
        "spark": "run",
    }
    spec.update(extra)
    return spec


def specs() -> list[dict[str, Any]]:
    """Return every cell spec in measurement order."""
    guards = "crates/repark-sql/src/guards/tests.rs"
    ansi = "crates/repark-sql/src/guards/ansi_door.rs"
    write_defaults = "crates/repark-sql/tests/ansi_write_defaults.rs"
    sub = {"t": T_SEED, "s": S_SEED}
    return [
        cell(
            "A1",
            "P01",
            "multi-statement read",
            "SELECT 1; SELECT 2",
            REFUSE,
            f"{guards}::two_statements_refuse_with_parse_syntax_error_class",
            kind="read",
            tables={},
        ),
        cell(
            "A2",
            "P01",
            "multi-statement write",
            "INSERT INTO {t} VALUES (4, 40); INSERT INTO {t} VALUES (5, 50)",
            REFUSE,
            f"{guards}::two_statements_refuse_with_parse_syntax_error_class",
        ),
        cell(
            "B1",
            "P02",
            "write to a four-part ref name",
            "INSERT INTO {t}.branch_dev VALUES (4, 40)",
            REFUSE,
            f"{guards}::write_to_branch_refuses",
        ),
        cell(
            "C1",
            "P03",
            "collation in an expression",
            "SELECT 'a' COLLATE UNICODE",
            REFUSE,
            f"{guards}::collation_valve_fires_on_expression_collate",
            kind="read",
            tables={},
        ),
        cell(
            "C2",
            "P03",
            "collation on a column",
            "CREATE TABLE {n1} (s STRING COLLATE UNICODE)",
            REFUSE,
            f"{guards}::collation_valve_fires_on_create_table_column_collate",
            tables={},
        ),
        cell(
            "C3",
            "P03",
            "collation in a cast type",
            "SELECT CAST('a' AS STRING COLLATE UNICODE)",
            REFUSE,
            f"{guards}::collation_valve_fires_on_cast_as_string_collate",
            kind="read",
            tables={},
        ),
        cell(
            "D1",
            "P04",
            "declared-absent function java_method",
            "SELECT java_method('java.lang.Math', 'abs', -1)",
            REFUSE,
            "crates/repark-sql/src/router/tests.rs::execute_refuses_every_armed_declared_name",
            kind="read",
            tables={},
        ),
        cell(
            "D2",
            "P04",
            "declared-deferred function hll_sketch_agg",
            "SELECT hll_sketch_agg(id) FROM {t}",
            REFUSE,
            "crates/repark-sql/src/router/tests.rs::execute_refuses_every_armed_declared_name",
            kind="read",
        ),
        cell(
            "E1",
            "P05",
            "CREATE EXTERNAL TABLE at a local path",
            "CREATE EXTERNAL TABLE ext_x STORED AS PARQUET LOCATION '{ext}'",
            REFUSE,
            f"{guards}::create_external_table_outside_warehouse_refuses",
            tables={},
            spark="skip",
        ),
        cell(
            "E2",
            "P05",
            "COPY TO a local path",
            "COPY (SELECT 1) TO '{ext}.parquet'",
            REFUSE,
            f"{guards}::local_filesystem_plan_refuses",
            tables={},
            spark="skip",
        ),
        cell(
            "F1",
            "P06",
            "DELETE with IN (SELECT col FROM table)",
            "DELETE FROM {t} WHERE id IN (SELECT id FROM {s})",
            EXECUTE,
            f"{ansi}::dml_subquery_in_delete_executes_and_deletes_exactly_the_match",
            tables=sub,
            expect_rows=[[1, 10]],
        ),
        cell(
            "F2",
            "P06",
            "UPDATE with IN (SELECT col FROM table)",
            "UPDATE {t} SET v = 5 WHERE id IN (SELECT id FROM {s})",
            EXECUTE,
            f"{ansi}::dml_subquery_correlated_in_and_update_in_execute",
            tables=sub,
            expect_rows=[[1, 10], [2, 5], [3, 5]],
        ),
        cell(
            "F3",
            "P06",
            "UPDATE with NOT IN (SELECT ...)",
            "UPDATE {t} SET v = 9 WHERE id NOT IN (SELECT id FROM {s})",
            REFUSE,
            f"{ansi}::dml_subquery_valve_refuses_end_to_end_and_writes_nothing",
            tables=sub,
        ),
        cell(
            "F4",
            "P06",
            "DELETE with = (scalar subquery)",
            "DELETE FROM {t} WHERE id = (SELECT max(id) FROM {s})",
            REFUSE,
            f"{ansi}::dml_subquery_valve_refuses_end_to_end_and_writes_nothing",
            tables=sub,
        ),
        cell(
            "F5",
            "P06",
            "DELETE with = ANY (SELECT ...)",
            "DELETE FROM {t} WHERE id = ANY (SELECT id FROM {s})",
            REFUSE,
            f"{ansi}::dml_subquery_valve_refuses_end_to_end_and_writes_nothing",
            tables=sub,
        ),
        cell(
            "F6",
            "P06",
            "DELETE with correlated EXISTS",
            "DELETE FROM {t} WHERE EXISTS (SELECT 1 FROM {s} WHERE {sn}.id = {tn}.id)",
            EXECUTE,
            f"{ansi}::dml_subquery_exists_delete_executes_uncorrelated_and_correlated",
            tables=sub,
            expect_rows=[[1, 10]],
        ),
        cell(
            "G1",
            "P07",
            "UPDATE SET column = (SELECT ...)",
            "UPDATE {t} SET v = (SELECT max(w) FROM {s})",
            REFUSE,
            "crates/repark-core/src/pre_execute/tests.rs::guard_refuses_an_update_whose_set_value_is_a_scalar_subquery",
            tables=sub,
        ),
        cell(
            "G2",
            "P08",
            "UPDATE assigns an incompatible literal",
            "UPDATE {t} SET v = 'abc'",
            REFUSE,
            "crates/repark-sql/tests/ansi_update_cast.rs::"
            "ansi_update_string_literal_into_bigint_stamps_cannot_safely_cast",
        ),
        cell(
            "H1",
            "P09",
            "INSERT VALUES with too few columns",
            "INSERT INTO {t} VALUES (4)",
            REFUSE,
            f"{write_defaults}::ansi_short_values_insert_stamps_not_enough_data_columns",
        ),
        cell(
            "H2",
            "P10",
            "INSERT omits a required column",
            "INSERT INTO {t} (v) VALUES (5)",
            REFUSE,
            f"{write_defaults}::ansi_insert_missing_required_column_refuses",
            tables={"t": R_SEED},
        ),
        cell(
            "H3",
            "P10",
            "INSERT with a partial column list",
            "INSERT INTO {t} (id) VALUES (4)",
            EXECUTE,
            f"{write_defaults}::ansi_insert_column_list_fills_write_default",
            expect_rows=[[1, 10], [2, 20], [3, 30], [4, None]],
        ),
        cell(
            "I1",
            "P11",
            "whole-table INSERT OVERWRITE",
            "INSERT OVERWRITE {t} SELECT 7 AS id, 70 AS v",
            REFUSE,
            "crates/repark-sql/src/partition_overwrite.rs::whole_table_insert_overwrite_stays_q9",
        ),
        cell(
            "J1",
            "P12",
            "CALL a maintenance procedure",
            "CALL {cat}.system.expire_snapshots(table => '{ns}.{tn}')",
            REFUSE,
            "crates/repark-sql/src/refusals/tests.rs::call_refusal_steers_to_callable_ops_and_names_the_trigger",
        ),
        cell(
            "J2",
            "P12",
            "ALTER TABLE ... EXECUTE",
            "ALTER TABLE {t} EXECUTE optimize",
            REFUSE,
            "crates/repark-sql/src/refusals/tests.rs::alter_execute_recognizer_fires_on_the_statement_shape",
        ),
        cell(
            "K1",
            "P13",
            "TRUNCATE TABLE",
            "TRUNCATE TABLE {t}",
            EXECUTE,
            "crates/repark-sql/src/truncate_tests.rs::truncate_table_wipes_rows_stamps_delete_and_preserves_history",
            expect_rows=[],
        ),
        cell(
            "L1",
            "P14",
            "DELETE WHERE false",
            "DELETE FROM {t} WHERE false",
            UNMEASURED,
            "no router pin for the false literal; expectation is Spark: no row changes",
        ),
        cell(
            "L2",
            "P14",
            "UPDATE WHERE NULL",
            "UPDATE {t} SET v = 0 WHERE NULL",
            UNMEASURED,
            "no router pin for the NULL literal; expectation is Spark: no row changes",
        ),
        cell(
            "L3",
            "P14",
            "DELETE WHERE 1 = 0",
            "DELETE FROM {t} WHERE 1 = 0",
            UNMEASURED,
            "no router pin for 1 = 0; expectation is Spark: no row changes",
        ),
        cell(
            "L4",
            "P14",
            "DELETE WHERE id = 99 (no match)",
            "DELETE FROM {t} WHERE id = 99",
            EXECUTE,
            "crates/repark-sql/tests/ansi_meta_delete.rs::"
            "the_ansi_door_commits_an_empty_delete_snapshot_on_a_no_match",
            expect_rows=BASE,
            expect_snapshot_delta=1,
        ),
        cell(
            "M1",
            "P15",
            "DELETE <> with a NULL row (control)",
            "DELETE FROM {t} WHERE v <> 10",
            EXECUTE,
            "Spark three-valued logic; control",
            tables={"t": N_SEED},
            expect_rows=[[1, 10], [2, None]],
        ),
        cell(
            "M2",
            "P15",
            "UPDATE v = v + 1 (control)",
            "UPDATE {t} SET v = v + 1 WHERE id = 2",
            EXECUTE,
            "Spark semantics; control",
            expect_rows=[[1, 10], [2, 21], [3, 30]],
        ),
        cell(
            "M3",
            "P15",
            "DELETE id IN (1, 2) (control)",
            "DELETE FROM {t} WHERE id IN (1, 2)",
            EXECUTE,
            "Spark semantics; control",
            expect_rows=[[3, 30]],
        ),
        cell(
            "N1",
            "P16",
            "MERGE INTO",
            "MERGE INTO {t} USING {s} ON {tn}.id = {sn}.id WHEN MATCHED THEN UPDATE SET v = {sn}.w",
            EXECUTE,
            "crates/repark-sql/src/merge/tests.rs::lowers_the_classic_upsert",
            tables=sub,
            expect_rows=[[1, 10], [2, 20], [3, 99]],
        ),
        cell(
            "O1",
            "P17",
            "ALTER TABLE ADD COLUMN",
            "ALTER TABLE {t} ADD COLUMN c INT",
            EXECUTE,
            "crates/repark-sql/tests/alter_map_key.rs (ADD COLUMN cases)",
            expect_rows=[[1, 10], [2, 20], [3, 30]],
        ),
        cell(
            "P1",
            "P18",
            "FOR VERSION AS OF the first snapshot",
            "SELECT * FROM {t} FOR VERSION AS OF {snap1} ORDER BY id",
            EXECUTE,
            "crates/repark-sql/src/time_travel/exec_tests.rs::version_as_of_pins_the_first_snapshot",
            kind="read",
            pre=["INSERT INTO {t} VALUES (4, 40)"],
            expect_rows=BASE,
        ),
        cell(
            "Q1",
            "P19",
            "CREATE TABLE with a nanosecond timestamp column",
            "CREATE TABLE {n1} (ts TIMESTAMP(9))",
            REFUSE,
            "crates/repark-sql/src/create_table/tests.rs::nanosecond_timestamp_columns_refuse_with_column_and_precision",
            tables={},
            spark="skip",
        ),
        cell(
            "Q2",
            "P19",
            "CREATE TEMPORARY TABLE",
            "CREATE TEMPORARY TABLE {n1} (id INT)",
            REFUSE,
            "crates/repark-sql/src/create_table/tests.rs::silently_droppable_clauses_all_refuse",
            tables={},
        ),
        cell(
            "Q3",
            "P19",
            "CREATE TABLE with a table COMMENT",
            "CREATE TABLE {n1} (id INT) COMMENT 'x'",
            REFUSE,
            "crates/repark-sql/src/create_table/tests.rs::silently_droppable_clauses_all_refuse",
            tables={},
        ),
        cell(
            "Q4",
            "P19",
            "CREATE TABLE with a path-separator name",
            'CREATE TABLE {cat}.{ns}."a/b" (id INT)',
            REFUSE,
            "crates/repark-sql/src/schema_ddl/tests.rs::escaping_identifiers_are_rejected",
            tables={},
        ),
        cell(
            "Q5",
            "P19",
            "CREATE TABLE with columns and AS SELECT",
            "CREATE TABLE {n1} (id INT) AS SELECT 1 AS id",
            REFUSE,
            "crates/repark-sql/src/create_table.rs:810 (no dedicated pin found)",
            tables={},
        ),
        cell(
            "R1",
            "P20",
            "DROP SCHEMA with a table inside",
            "DROP SCHEMA {cat}.{ns2}",
            REFUSE,
            "crates/repark-sql/src/schema_ddl/tests.rs::drop_schema_nonempty_refuses_and_keeps_everything",
            tables={},
            pre=["CREATE SCHEMA {cat}.{ns2}", "CREATE TABLE {cat}.{ns2}.x (id INT)"],
        ),
        cell(
            "R2",
            "P20",
            "CREATE SCHEMA with an unknown property",
            "CREATE SCHEMA {cat}.{ns3} WITH (foo = 'bar')",
            REFUSE,
            "crates/repark-sql/src/schema_ddl/tests.rs::unknown_schema_property_refuses_listing_support",
            tables={},
            spark="skip",
            post="CREATE TABLE {cat}.{ns3}.p (id INT){using}",
        ),
        cell(
            "S1",
            "P21",
            "INT addition overflow",
            "SELECT v + 1 AS r FROM {t}",
            REFUSE,
            "crates/repark-sql/tests/cross_door_int_overflow.rs::"
            "cross_door_int32_add_overflow_raises_on_both_doors_by_default",
            kind="read",
            tables={"t": BIG_SEED},
        ),
        cell(
            "T1",
            "P22",
            "DELETE on merge-on-read after a spec change",
            "DELETE FROM {t} WHERE id = 1",
            REFUSE,
            "crates/repark-sql/src/tests.rs::mor_unpartitioned_multi_spec_dml_refuses",
            native="fixture",
            tables={"t": ("id INT, v INT", "(1, 10)")},
            pre=["ALTER TABLE {t} DROP PARTITION FIELD bucket(4, id)"],
        ),
        cell(
            "T2",
            "P23",
            "INSERT into a table with encryption.key-id",
            "INSERT INTO {t} VALUES (2, 20)",
            REFUSE,
            "crates/repark-sql/tests/enc_1.rs::encrypted_table_insert_values_refuses",
            native="fixture",
            spark="skip",
            tables={"t": ("id INT, v INT", None)},
        ),
        cell(
            "T3",
            "P24",
            "partition-aligned DELETE (metadata delete)",
            "DELETE FROM {t} WHERE part = 'a'",
            EXECUTE,
            "crates/repark-sql/tests/ansi_meta_delete.rs::the_ansi_door_removes_whole_files_in_one_delete_snapshot_on_both_modes",
            native="fixture",
            expect_rows=[[2, "b"]],
            tables={"t": ("id INT, part STRING", "(1, 'a'), (2, 'b')")},
        ),
        cell(
            "T4",
            "P25",
            "ALTER TABLE CREATE BRANCH",
            "ALTER TABLE {t} CREATE BRANCH dev",
            EXECUTE,
            "crates/repark-sql/src/tests.rs::branch_and_tag_ddl_round_trips",
        ),
    ]


FIXTURES = {
    "T1": {
        "props": "PARTITIONED BY (bucket(4, id)) TBLPROPERTIES ('write.delete.mode' = "
        "'merge-on-read', 'write.update.mode' = 'merge-on-read')"
    },
    "T2": {"props": "TBLPROPERTIES ('encryption.key-id' = 'k1')"},
    "T3": {"props": "PARTITIONED BY (part)"},
}


def sanitize(text: str) -> str:
    """Return a lower-case table identifier."""
    return re.sub(r"[^a-z0-9_]", "_", text.lower())


def scrub(text: str, roots: list[str]) -> str:
    """Replace warehouse roots, temporary directories and the home directory."""
    for root in roots:
        text = text.replace(root, "<warehouse>")
    temporary = re.escape(tempfile.gettempdir())
    text = re.sub(temporary + r"/[^\s'\"),\]\\]*", "<tmp>", text)
    home = re.escape(str(Path.home()))
    return re.sub(home + r"(?:/[^\s'\"),\]\\]*)?", "<home>", text)


def sha256_of(path: Path) -> str:
    """Return the sha256 hex digest of a file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def excerpt(text: str, limit: int = 300) -> str:
    """Return the first ``limit`` characters, cut back to a word boundary."""
    if len(text) <= limit:
        return text
    head = text[:limit]
    boundary = head.rfind(" ")
    return head[:boundary] if boundary > 0 else head


def metadata_order(path: Path) -> tuple[int, str]:
    """Order metadata files by their leading version number."""
    match = re.match(r"v?(\d+)", path.name)
    return (int(match.group(1)) if match else -1, path.name)


def read_metadata(table_dir: Path) -> dict[str, Any]:
    """Read the newest metadata JSON of a table: snapshot count, current id and last operation."""
    candidates = sorted((table_dir / "metadata").glob("*.metadata.json"), key=metadata_order)
    if not candidates:
        return {"snapshots": None, "current": None, "operation": None, "ids": []}
    document = json.loads(candidates[-1].read_text(encoding="utf-8"))
    snapshots = document.get("snapshots") or []
    current = document.get("current-snapshot-id")
    operation = None
    for snapshot in snapshots:
        if snapshot.get("snapshot-id") == current:
            operation = (snapshot.get("summary") or {}).get("operation")
    return {
        "snapshots": len(snapshots),
        "current": current,
        "operation": operation,
        "ids": [snapshot.get("snapshot-id") for snapshot in snapshots],
    }


def table_names(root: Path) -> list[str]:
    """List every table directory under the warehouse namespaces."""
    found = []
    if root.is_dir():
        for namespace in sorted(root.iterdir()):
            if namespace.is_dir():
                found.extend(
                    f"{namespace.name}/{path.name}" for path in sorted(namespace.iterdir())
                )
    return found


def canon(rows: list[Any]) -> list[Any]:
    """Return rows sorted by their canonical JSON."""
    return sorted(rows, key=lambda row: json.dumps(row, sort_keys=True, default=str))


def to_rows(frame: Any) -> list[list[Any]]:
    """Collect a frame as a list of value lists."""
    table = frame.toArrow() if hasattr(frame, "toArrow") else frame.to_arrow()
    return canon([list(row.values()) for row in table.to_pylist()])


def spark_rows(frame: Any) -> list[list[Any]]:
    """Collect a Spark frame as a list of value lists."""
    return canon([list(row) for row in frame.collect()])


def error_record(ctx: dict[str, Any], error: BaseException, stage: str) -> dict[str, Any]:
    """Return the scrubbed, truncated record of one raised error."""
    text = scrub(str(error), ctx["roots"])
    panic = type(error).__name__ == "PanicException" or "panicked at" in text
    return {"class": type(error).__name__, "stage": stage, "message": excerpt(text), "panic": panic}


def attempt(ctx: dict[str, Any], call: Any, stage: str) -> tuple[Any, dict[str, Any] | None]:
    """Run one call and return its value, or the error record if it raised."""
    try:
        return call(), None
    except KeyboardInterrupt:
        raise
    except BaseException as error:
        return None, error_record(ctx, error, stage)


def run_rows(ctx: dict[str, Any], door: dict[str, Any], text: str) -> list[list[Any]]:
    """Run a statement on a door and return its rows."""
    frame = door["sql"](text)
    return spark_rows(frame) if door["name"] == "spark" else to_rows(frame)


def names_for(ctx: dict[str, Any], door: dict[str, Any], spec: dict[str, Any]) -> dict[str, str]:
    """Return the placeholder map for one cell on one door."""
    cat = door["catalog"]
    stem = sanitize(spec["id"])
    names = {
        "cat": cat,
        "ns": NAMESPACE,
        "ns2": f"n2{stem}",
        "ns3": f"n3{stem}",
        "n1": f"{cat}.{NAMESPACE}.{stem}_n1",
    }
    for key in ("t", "s"):
        names[f"{key}n"] = f"{stem}_{key}"
        names[key] = f"{cat}.{NAMESPACE}.{stem}_{key}"
    names["ext"] = str(ctx["ext_root"] / stem)
    names["using"] = "" if door["name"] == "native" else " USING iceberg"
    return names


def create_text(door: dict[str, Any], spec: dict[str, Any], key: str, qualified: str) -> str:
    """Return the CREATE TABLE text for one fixture table on a door."""
    fixture = FIXTURES.get(spec["id"])
    columns = spec["tables"][key][0]
    suffix = ""
    if door["name"] != "native":
        suffix = " USING iceberg"
        if fixture and key == "t":
            suffix += " " + fixture["props"]
    return f"CREATE TABLE {qualified} ({columns}){suffix}"


def fill(text: str, names: dict[str, str]) -> str:
    """Substitute the placeholders of a statement."""
    return text.format(**names)


def observe(
    ctx: dict[str, Any], door: dict[str, Any], names: dict[str, str], spec: dict[str, Any]
) -> dict[str, Any]:
    """Read the subject table's rows and metadata, and the set of tables in the warehouse."""
    state: dict[str, Any] = {"tables": table_names(door["root"])}
    if "t" in names and (spec["tables"].get("t") or spec["id"] in FIXTURES):
        table_dir = door["root"] / NAMESPACE / names["tn"]
        state["meta"] = read_metadata(table_dir)
        rows, error = attempt(
            ctx, lambda: run_rows(ctx, door, f"SELECT * FROM {names['t']}"), "observe"
        )
        state["rows"] = rows
        state["rows_error"] = error
    return state


def build_fixture(
    ctx: dict[str, Any], door: dict[str, Any], spec: dict[str, Any], names: dict[str, str]
) -> dict[str, Any] | None:
    """Create and seed the cell's tables; return an error record if setup fails."""
    for key, (_, seed) in spec["tables"].items():
        qualified = names[key]
        text = create_text(door, spec, key, qualified)
        _, error = attempt(ctx, lambda text=text: run_rows(ctx, door, text), "create")
        if error:
            return error
        if seed:
            insert = f"INSERT INTO {qualified} VALUES {seed}"
            _, error = attempt(ctx, lambda insert=insert: run_rows(ctx, door, insert), "seed")
            if error:
                return error
    return None


def run_cell(ctx: dict[str, Any], door: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
    """Run one cell on one door and return its record."""
    record: dict[str, Any] = {
        "record": "cell",
        "cell": spec["id"],
        "row": spec["row"],
        "door": door["name"],
    }
    names = names_for(ctx, door, spec)
    skipped = (
        spec["native"]
        if door["name"] == "native"
        else ("run" if door["name"] == "facade" else spec["spark"])
    )
    if skipped != "run":
        record["outcome"] = skipped.upper()
        record["statement"] = None
        return record
    setup_error = build_fixture(ctx, door, spec, names)
    if setup_error:
        record.update(outcome=SETUP_REFUSED, error=setup_error, statement=fill(spec["sql"], names))
        return record
    names["snap1"] = "0"
    for text in spec["pre"]:
        _, error = attempt(ctx, lambda text=text: run_rows(ctx, door, fill(text, names)), "pre")
        if error:
            record.update(outcome=SETUP_REFUSED, error=error, statement=fill(spec["sql"], names))
            return record
    before = observe(ctx, door, names, spec)
    if "t" in names and before.get("meta") and before["meta"]["ids"]:
        names["snap1"] = str(before["meta"]["ids"][0])
    statement = fill(spec["sql"], names)
    result, error = attempt(ctx, lambda: run_rows(ctx, door, statement), "statement")
    after = observe(ctx, door, names, spec)
    if spec.get("post"):
        _, post_error = attempt(ctx, lambda: run_rows(ctx, door, fill(spec["post"], names)), "post")
        record["post_ok"] = post_error is None
    record.update(
        statement=scrub(statement, ctx["roots"]),
        before=before,
        after=after,
        result=result if spec["kind"] == "read" else None,
        error=error,
        outcome=ANSWERED if error is None else (PANIC if error["panic"] else REFUSED),
    )
    return record


def build_context(arguments: argparse.Namespace, warehouse: Path) -> dict[str, Any]:
    """Open the door of the requested leg."""
    ext_root = warehouse / "ext"
    ext_root.mkdir(parents=True)
    root = warehouse / arguments.engine
    root.mkdir()
    ctx: dict[str, Any] = {"roots": [str(warehouse)], "ext_root": ext_root}
    if arguments.engine == "spark":
        ctx.update(spark_door(arguments, warehouse, root))
    else:
        ctx.update(repark_door(arguments, root))
    return ctx


def spark_door(arguments: argparse.Namespace, warehouse: Path, root: Path) -> dict[str, Any]:
    """Start the Spark oracle session and describe its door."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("door-inventory")
        .config("spark.ui.enabled", "false")
        .config("spark.local.dir", str(warehouse / "spark-local"))
        .config("spark.jars", arguments.ivy_jar)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.hc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hc.type", "hadoop")
        .config("spark.sql.catalog.hc.warehouse", str(root))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    door = {"name": "spark", "sql": session.sql, "catalog": "hc", "root": root}
    spark_rows(session.sql(f"CREATE NAMESPACE hc.{NAMESPACE}"))
    jar = Path(arguments.ivy_jar)
    header = {
        "record": "leg",
        "engine": "spark",
        "spark_version": session.version,
        "iceberg_jar": jar.name,
        "iceberg_jar_sha256": sha256_of(jar),
        "python": sys.version.split()[0],
    }
    return {"door": door, "header": header, "close": session.stop}


def repark_door(arguments: argparse.Namespace, root: Path) -> dict[str, Any]:
    """Open the native or facade door of the installed wheel."""
    import pyarrow

    import repark

    wheel = Path(arguments.wheel)
    header = {
        "record": "leg",
        "engine": arguments.engine,
        "wheel_file": wheel.name,
        "wheel_sha256": sha256_of(wheel),
        "repark_version": importlib.metadata.version("repark"),
        "pyarrow_version": pyarrow.__version__,
        "python": sys.version.split()[0],
    }
    if arguments.engine == "native":
        from repark._native import PyReparkSession

        native = PyReparkSession.native()
        native.register_memory_catalog("ice", str(root))
        native.create_namespace("ice", NAMESPACE, str(root / NAMESPACE))
        repark._ANSI_NATIVE = native
        door = {"name": "native", "sql": repark.sql, "catalog": "ice", "root": root}
    else:
        facade = repark.ReparkSession.builder.appName("door-inventory").getOrCreate()
        facade.register_memory_catalog("ice", str(root))
        door = {"name": "facade", "sql": facade.sql, "catalog": "ice", "root": root}
        spark_rows_like = facade.sql(
            f"CREATE NAMESPACE ice.{NAMESPACE} LOCATION '{root / NAMESPACE}'"
        )
        to_rows(spark_rows_like)
    return {"door": door, "header": header, "close": lambda: None}


def write_line(handle: Any, record: dict[str, Any], roots: list[str]) -> None:
    """Write one scrubbed JSON line."""
    handle.write(scrub(json.dumps(record, sort_keys=True, default=str), roots) + "\n")


def run_leg(arguments: argparse.Namespace) -> int:
    """Run every cell on one leg and write its JSON lines."""
    with tempfile.TemporaryDirectory(prefix="door-inventory-") as scratch:
        warehouse = Path(scratch)
        ctx = build_context(arguments, warehouse)
        out = Path(arguments.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", encoding="utf-8") as handle:
            write_line(handle, ctx["header"], ctx["roots"])
            for spec in specs():
                if arguments.only and spec["id"] not in arguments.only.split(","):
                    continue
                write_line(handle, run_cell(ctx, ctx["door"], spec), ctx["roots"])
                handle.flush()
        ctx["close"]()
    return 0


def load_leg(path: str) -> dict[str, dict[str, Any]]:
    """Load one leg file into a cell-id map."""
    cells: dict[str, dict[str, Any]] = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        record = json.loads(line)
        if record.get("record") == "cell":
            cells[record["cell"]] = record
    return cells


def changed(record: dict[str, Any]) -> bool:
    """Whether a cell changed the subject table or the set of tables."""
    before, after = record.get("before") or {}, record.get("after") or {}
    if before.get("tables") != after.get("tables") or record.get("post_ok"):
        return True
    if before.get("rows") != after.get("rows"):
        return True
    return (before.get("meta") or {}).get("current") != (after.get("meta") or {}).get("current")


def snapshot_delta(record: dict[str, Any]) -> int | None:
    """Return the snapshot count increase of a cell."""
    before = (record.get("before") or {}).get("meta") or {}
    after = (record.get("after") or {}).get("meta") or {}
    if before.get("snapshots") is None or after.get("snapshots") is None:
        return None
    return after["snapshots"] - before["snapshots"]


def native_verdict(spec: dict[str, Any], record: dict[str, Any]) -> tuple[str, str]:
    """Return the mechanical verdict of a native cell and a one-line reason."""
    outcome = record["outcome"]
    if outcome == "FIXTURE" or spec["native"] == "fixture":
        return (
            NOT_REACHABLE,
            "the native door cannot create the fixture (no table-property syntax)",
        )
    if outcome == PANIC:
        return PANIC, "panic"
    if outcome == SETUP_REFUSED:
        return NOT_MEASURED, "setup refused"
    moved = changed(record)
    answered = outcome == ANSWERED
    router = spec["router"]
    if router == EXECUTE and not answered and spec["id"] in {"N1", "O1", "P1", "T4"}:
        return NOT_REACHABLE, "the native door refuses the statement as unsupported syntax"
    if router in {REFUSE, UNMEASURED} and not answered:
        return (
            (WRONG_WRITE, "refused but the table or catalog changed")
            if moved
            else (PROTECTED, "refused, nothing changed")
        )
    if router == REFUSE:
        if spec["kind"] == "read":
            return WRONG_READ, "returned a value where the router refuses"
        return (
            (WRONG_WRITE, "executed and changed state where the router refuses")
            if moved
            else (OTHER, "executed with no state change where the router refuses")
        )
    if router == UNMEASURED:
        if moved and (record["after"].get("rows") != record["before"].get("rows")):
            return WRONG_WRITE, "rows changed where no row should change"
        delta = snapshot_delta(record)
        return (
            (OTHER, f"rows unchanged but {delta} new snapshot(s)")
            if moved
            else (PROTECTED, "no change")
        )
    if not answered:
        return (
            (WRONG_WRITE, "refused but state changed")
            if moved
            else (OTHER, "refused where the router executes; nothing changed")
        )
    expected = spec["expect_rows"]
    got = record["result"] if spec["kind"] == "read" else record["after"].get("rows")
    if expected is not None and canon(got or []) != canon(expected):
        return (
            WRONG_READ if spec["kind"] == "read" else WRONG_WRITE
        ), f"rows {got} differ from the router's {expected}"
    want = spec.get("expect_snapshot_delta")
    delta = snapshot_delta(record)
    if want is not None and delta != want:
        return OTHER, f"rows equal, snapshot delta {delta} where the router commits {want}"
    return PROTECTED, "matches the router's result"


def short(record: dict[str, Any] | None) -> str:
    """Return a short measured description of a leg record."""
    if record is None:
        return "not run"
    outcome = record["outcome"]
    if outcome not in {ANSWERED, REFUSED, PANIC, SETUP_REFUSED}:
        return outcome.lower()
    if outcome == ANSWERED:
        if record.get("result") is not None:
            return f"answered {excerpt(str(record['result'][:3]), 60)}"
        rows = (record.get("after") or {}).get("rows")
        delta = snapshot_delta(record)
        suffix = f", +{delta} snap" if delta is not None else ""
        return f"answered, rows {rows}{suffix}" if rows is not None else "answered"
    error = record.get("error") or {}
    return f"{outcome.lower()}: {error.get('class')} {excerpt(error.get('message', ''), 90)}"


def cell_text(text: str | None) -> str:
    """Escape a markdown table cell."""
    return (text or "").replace("|", "\\|").replace("\n", " ")


PREAMBLE = """# DOOR-PROTECTION-INVENTORY-1, Part 2: measured

Each cell ran on a fresh table with a single-statement seed, on the Python native door
(`PyReparkSession.native()` plus `register_memory_catalog`, driven through `repark.sql()`), on the
facade `spark.sql()` (the Spark door) and, where the statement is Spark SQL, on the Spark 4.1.2
oracle with an Iceberg Hadoop catalog. Wheel: the main release build `fdc61f26` (`origin/main`
`a4284f4d` differs only in docs). The native door's reference is Trino and no Trino oracle exists
here, so a native verdict is judged against the router's documented behaviour (the Rust pin named in
[inventory.md](inventory.md) and in the `router` column). Cells with no router pin (L1 to L3) are
judged against Spark: no row may change. The verdict is mechanical (see `native_verdict` in
[probe_doors.py](probe_doors.py)): the router column says what the router does, the rows and
snapshot counts come from the table before and after, and "state changed" also covers a new table
or a new schema.

`{t}` and `{s}` in a statement stand for the cell's fresh tables.

The `facade vs spark` column compares the facade with the oracle on the post-statement rows and on
refused-versus-answered; it is not a verdict.
"""


def agreement(facade: dict[str, Any] | None, oracle: dict[str, Any] | None) -> str:
    """Compare the facade with the Spark oracle on outcome kind and rows after."""
    if facade is None or oracle is None or oracle["outcome"] not in {ANSWERED, REFUSED}:
        return "no oracle"
    if facade["outcome"] != oracle["outcome"]:
        return f"DIFFERS ({facade['outcome'].lower()} vs {oracle['outcome'].lower()})"
    if facade["outcome"] == ANSWERED:
        left = (facade.get("after") or {}).get("rows")
        right = (oracle.get("after") or {}).get("rows")
        if facade.get("result") is not None or oracle.get("result") is not None:
            left, right = facade.get("result"), oracle.get("result")
        if left != right:
            return "DIFFERS (rows)"
    return "EQUAL"


def render(arguments: argparse.Namespace) -> int:
    """Write the measured table from the three leg files."""
    legs = {door: load_leg(getattr(arguments, door)) for door in DOORS}
    lines = [
        "| cell | statement | router | native outcome | verdict | facade | spark | "
        "facade vs spark |",
        "|---|---|---|---|---|---|---|---|",
    ]
    counts: Counter[str] = Counter()
    notes = []
    for spec in specs():
        native = legs["native"].get(spec["id"])
        if native is None:
            continue
        verdict, reason = native_verdict(spec, native)
        counts[verdict] += 1
        notes.append(f"- {spec['id']} ({spec['row']}) {verdict}: {reason}")
        facade, oracle = legs["facade"].get(spec["id"]), legs["spark"].get(spec["id"])
        lines.append(
            f"| {spec['id']} | `{cell_text(spec['sql'])}` | {spec['router']} | "
            f"{cell_text(short(native))} | {verdict} | {cell_text(short(facade))} | "
            f"{cell_text(short(oracle))} | {agreement(facade, oracle)} |"
        )
    summary = [
        "Verdict counts: " + ", ".join(f"{name} {counts.get(name, 0)}" for name in VERDICTS),
        "",
    ]
    body = [PREAMBLE, *summary, *lines, "", "## Verdict reasons", "", *notes, ""]
    Path(arguments.output).write_text("\n".join(body), encoding="utf-8")
    return 0


def parse_arguments(argv: list[str] | None) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=DOORS)
    parser.add_argument("--out")
    parser.add_argument("--wheel")
    parser.add_argument("--ivy-jar")
    parser.add_argument("--only")
    parser.add_argument("--render", action="store_true")
    parser.add_argument("--native")
    parser.add_argument("--facade")
    parser.add_argument("--spark")
    parser.add_argument("--output")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run one leg, or render the measured table."""
    arguments = parse_arguments(argv)
    if arguments.render:
        return render(arguments)
    return run_leg(arguments)


if __name__ == "__main__":
    sys.exit(main())
