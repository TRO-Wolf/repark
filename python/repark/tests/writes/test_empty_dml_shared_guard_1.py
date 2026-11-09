from __future__ import annotations

import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

import repark
from repark import ReparkSession
from repark._native import PyReparkSession

ORACLE = json.loads(
    (Path(__file__).parent / "empty_dml_shared_guard_1_spark_oracle.json").read_text(
        encoding="utf-8"
    )
)
SPARK = {(cell["cell"], cell["verb"], cell["kind"]): cell for cell in ORACLE["cells"]}
V3_CONFIG = {"repark.sql.allowCreateFormatVersion3": "true"}
STRUCT_NULL = "st = CAST(NULL AS STRUCT<x: INT>)"
EMPTY_PREDICATES = {
    "a-false": "false",
    "a-null": "NULL",
    "b-struct-null": STRUCT_NULL,
    "b-arr-and-false": "arr[1] = 1 AND 1 = 0",
    "u-required-is-null": "id IS NULL",
    "c-one-eq-zero": "1 = 0",
    "c-prim-eq-null": "prim_col = NULL",
}
CONTROL_PREDICATES = {"c-true": "true", "c-no-where": None, "c-selective": "id = 1"}
KINDS = {
    "v2-cow": ("2", "copy-on-write"),
    "v2-mor": ("2", "merge-on-read"),
    "v3": ("3", "copy-on-write"),
}
FACADE_KINDS = ("v2-cow", "v2-mor", "v3")
NATIVE_KINDS = ("v2-cow",)
METADATA_DELETE_RESIDUE = {("facade", "u-required-is-null", "DELETE")}
SEED_ROWS = [(1, 10, "a", 1, 1), (2, 20, "b", 2, 2)]
SEEDED = [(1, "a"), (2, "b"), (3, "c")]


def metadata_order(path: Path) -> tuple[int, str]:
    match = re.match(r"v?(\d+)", path.name)
    return (int(match.group(1)) if match else -1, path.name)


class Door:
    def __init__(self, name: str, root: Path, facade: ReparkSession | None) -> None:
        self.name = name
        self.root = root
        self.facade = facade

    def run(self, statement: str) -> list[dict[str, Any]]:
        if self.facade is None:
            return repark.sql(statement).to_arrow().to_pylist()
        return self.facade.sql(statement).toArrow().to_pylist()

    def create(self, table: str, kind: str) -> None:
        version, mode = KINDS[kind]
        columns = "id BIGINT NOT NULL, prim_col INT, flag STRING, st STRUCT<x: INT>, arr ARRAY<INT>"
        if self.facade is None:
            self.run(f"CREATE TABLE ice.ns.{table} ({columns})")
            array_open, array_close = "ARRAY[", "]"
        else:
            properties = (
                f"'format-version' = '{version}', 'write.delete.mode' = '{mode}', "
                f"'write.update.mode' = '{mode}'"
            )
            self.run(
                f"CREATE TABLE ice.ns.{table} ({columns}) USING iceberg "
                f"TBLPROPERTIES ({properties})"
            )
            array_open, array_close = "array(", ")"
        values = ", ".join(
            f"({row_id}, {prim}, '{flag}', named_struct('x', {x}), "
            f"{array_open}{element}{array_close})"
            for row_id, prim, flag, x, element in SEED_ROWS
        )
        self.run(f"INSERT INTO ice.ns.{table} VALUES {values}, (3, NULL, 'c', NULL, NULL)")

    def state(self, table: str, kind: str) -> dict[str, Any]:
        read = self.run(f"SELECT id, flag FROM ice.ns.{table} ORDER BY id")
        rows = [(row["id"], row["flag"]) for row in read]
        table_dir = self.root / "ns" / table
        metadata = sorted((table_dir / "metadata").glob("*.metadata.json"), key=metadata_order)
        document = json.loads(metadata[-1].read_text(encoding="utf-8"))
        data = table_dir / "data"
        files = sorted(str(path.relative_to(data)) for path in data.rglob("*") if path.is_file())
        lineage = None
        if kind.startswith("v3"):
            lineage = [
                (row["id"], row["_row_id"], row["_last_updated_sequence_number"])
                for row in self.run(
                    "SELECT id, _row_id, _last_updated_sequence_number "
                    f"FROM ice.ns.{table} ORDER BY id"
                )
            ]
        return {
            "rows": rows,
            "format": document.get("format-version"),
            "snapshot": (len(document.get("snapshots") or []), document.get("current-snapshot-id")),
            "files": files,
            "lineage": lineage,
        }


def native_door(root: Path, monkeypatch: pytest.MonkeyPatch) -> Door:
    native = PyReparkSession.native(config=V3_CONFIG)
    native.register_memory_catalog("ice", str(root))
    native.create_namespace("ice", "ns", str(root / "ns"))
    monkeypatch.setattr(repark, "_ANSI_NATIVE", native)
    return Door("native", root, None)


def facade_door(root: Path) -> Door:
    session = (
        ReparkSession.builder.appName("empty-dml-shared-guard-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog("ice", str(root))
    session.sql("CREATE NAMESPACE ice.ns").collect()
    return Door("facade", root, session)


@pytest.fixture(params=("native", "facade"))
def door(
    request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Door]:
    root = tmp_path / "warehouse"
    root.mkdir()
    opened = native_door(root, monkeypatch) if request.param == "native" else facade_door(root)
    yield opened
    if opened.facade is not None:
        opened.facade.stop()


def statement(table: str, verb: str, predicate: str | None, assignment: str = "flag = 'z'") -> str:
    if verb == "DELETE":
        head = f"DELETE FROM ice.ns.{table}"
    else:
        head = f"UPDATE ice.ns.{table} SET {assignment}"
    return head if predicate is None else f"{head} WHERE {predicate}"


def classify_rows(after: list[tuple[int, str]]) -> str:
    if after == SEEDED:
        return "unchanged"
    if after == []:
        return "all-deleted"
    if after == SEEDED[1:]:
        return "one-deleted"
    if after == [(row_id, "z") for row_id, _ in SEEDED]:
        return "all-flag-z"
    if after == [(1, "z"), *SEEDED[1:]]:
        return "one-flag-z"
    return f"other {after}"


def classify_lineage(before: list[tuple[int, int, int]], after: list[tuple[int, int, int]]) -> str:
    surviving = {row[0]: row for row in after}
    changed = [row for row in before if surviving.get(row[0]) != row]
    if not changed:
        return "none"
    return "all" if len(changed) == len(before) else "some"


def kinds_of(door: Door) -> tuple[str, ...]:
    return NATIVE_KINDS if door.facade is None else FACADE_KINDS


def run_cell(
    door: Door, kind: str, table: str, sql: str
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    door.create(table, kind)
    before = door.state(table, kind)
    assert before["rows"] == SEEDED, table
    assert before["format"] == int(KINDS[kind.split(" ")[0]][0]), table
    answer = door.run(sql)
    return before, door.state(table, kind), answer


@pytest.mark.parametrize("verb", ["DELETE", "UPDATE"])
@pytest.mark.parametrize("cell", sorted(EMPTY_PREDICATES))
def test_an_empty_dml_writes_and_commits_nothing(door: Door, cell: str, verb: str) -> None:
    failures = []
    for kind in kinds_of(door):
        spark = SPARK[(cell, verb, kind)]
        table = f"t_{cell.replace('-', '_')}_{verb.lower()}_{kind.replace('-', '_')}"
        sql = statement(table, verb, EMPTY_PREDICATES[cell])
        before, after, answer = run_cell(door, kind, table, sql)
        residue = (door.name, cell, verb) in METADATA_DELETE_RESIDUE
        observed = {
            "rows": classify_rows(after["rows"]),
            "snapshot_changed": after["snapshot"] != before["snapshot"],
            "files_changed": after["files"] != before["files"],
            "counts": [row["count"] for row in answer if "count" in row],
        }
        expected = {
            "rows": "unchanged",
            "snapshot_changed": residue,
            "files_changed": bool(spark["files_changed"]),
            "counts": [0] * len(observed["counts"]),
        }
        if before["lineage"] is not None:
            observed["lineage"] = classify_lineage(before["lineage"], after["lineage"])
            expected["lineage"] = spark["lineage"]
        if observed != expected:
            failures.append(f"{kind} {sql}: {observed} != {expected}")
    assert not failures, "\n".join(failures)


CONTROLS = [
    pytest.param(cell, verb, predicate, assignment, id=f"{cell}-{verb.lower()}")
    for cell, predicate in sorted(CONTROL_PREDICATES.items(), key=lambda item: item[0])
    for verb, assignment in (("DELETE", ""), ("UPDATE", "flag = 'z'"))
] + [pytest.param("c-identity-update", "UPDATE", "id = 1", "flag = flag", id="c-identity-update")]


@pytest.mark.parametrize(("cell", "verb", "predicate", "assignment"), CONTROLS)
def test_a_control_dml_still_writes_what_spark_writes(
    door: Door, cell: str, verb: str, predicate: str | None, assignment: str
) -> None:
    failures = []
    for kind in kinds_of(door):
        spark = SPARK[(cell, verb, kind)]
        table = f"t_{cell.replace('-', '_')}_{verb.lower()}_{kind.replace('-', '_')}"
        sql = statement(table, verb, predicate, assignment)
        before, after, _ = run_cell(door, kind, table, sql)
        observed = {
            "rows": classify_rows(after["rows"]),
            "snapshot_changed": after["snapshot"] != before["snapshot"],
            "files_changed": after["files"] != before["files"],
        }
        expected = {
            "rows": spark["rows"],
            "snapshot_changed": spark["snapshot_changed"],
            "files_changed": spark["files_changed"],
        }
        if before["lineage"] is not None:
            observed["lineage"] = classify_lineage(before["lineage"], after["lineage"])
            expected["lineage"] = spark["lineage"]
        if observed != expected:
            failures.append(f"{kind} {cell} {verb}: {observed} != {expected}")
    assert not failures, "\n".join(failures)


@pytest.mark.parametrize("verb", ["DELETE", "UPDATE"])
@pytest.mark.parametrize("kind", ["v2-cow", "v2-mor", "v3"])
def test_a_uuid_table_keeps_its_rows_for_an_empty_dml_and_loses_one_for_a_selective_one(
    tmp_path: Path, kind: str, verb: str
) -> None:
    door = facade_door(tmp_path)
    try:
        version, mode = KINDS[kind]
        failures = []
        for cell, predicate in (("b-uuid-null", "uuid_col = NULL"), ("c-selective", "id = 1")):
            assert SPARK[(cell, verb, f"{kind} + uuid")]["outcome"] == "ORACLE-MISSING"
            table = f"t_{cell.replace('-', '_')}_{verb.lower()}_{kind.replace('-', '_')}_uuid"
            door.run(
                f"CREATE TABLE ice.ns.{table} (id BIGINT NOT NULL, flag STRING, uuid_col UUID) "
                f"USING iceberg TBLPROPERTIES ('format-version' = '{version}', "
                f"'write.delete.mode' = '{mode}', 'write.update.mode' = '{mode}')"
            )
            door.run(
                f"INSERT INTO ice.ns.{table} VALUES "
                "(1, 'a', '00000000-0000-0000-0000-000000000001'), "
                "(2, 'b', '00000000-0000-0000-0000-000000000002'), "
                "(3, 'c', '00000000-0000-0000-0000-000000000003')"
            )
            before = door.state(table, kind)
            door.run(statement(table, verb, predicate))
            after = door.state(table, kind)
            reference = SPARK[(cell if cell == "c-selective" else "c-one-eq-zero", verb, kind)]
            observed = (
                classify_rows(after["rows"]),
                after["snapshot"] != before["snapshot"],
                after["files"] != before["files"],
            )
            expected = (
                reference["rows"],
                cell == "c-selective",
                bool(reference["files_changed"]),
            )
            if observed != expected:
                failures.append(f"{kind} {cell} {verb}: {observed} != {expected}")
        assert not failures, "\n".join(failures)
    finally:
        door.facade.stop()


@pytest.mark.parametrize(
    "statement_text",
    [
        "EXPLAIN ANALYZE DELETE FROM ice.ns.t_wrapped WHERE false",
        "EXPLAIN ANALYZE UPDATE ice.ns.t_wrapped SET flag = 'z' WHERE 1 = 0",
        "PREPARE p AS DELETE FROM ice.ns.t_wrapped WHERE false",
        "PREPARE p AS UPDATE ice.ns.t_wrapped SET flag = 'z' WHERE NULL",
    ],
)
def test_a_refused_wrapper_of_an_empty_dml_stays_refused(door: Door, statement_text: str) -> None:
    from repark.errors import AnalysisException

    door.create("t_wrapped", "v2-cow")
    before = door.state("t_wrapped", "v2-cow")
    with pytest.raises(AnalysisException, match="is refused: its statement writes to a table"):
        door.run(statement_text)
    assert door.state("t_wrapped", "v2-cow") == before
