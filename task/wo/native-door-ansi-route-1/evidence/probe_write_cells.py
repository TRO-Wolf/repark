from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

W_INSERT_T = "INSERT INTO ice.ns.t VALUES (1, 10), (2, 20), (3, 30), (4, 40)"
W_INSERT_S = "INSERT INTO ice.ns.s VALUES (2, 20), (3, 99)"
W_TABLES = (
    ("t", "id INT, v INT", W_INSERT_T),
    ("s", "id INT, w INT", W_INSERT_S),
)
E_TABLES = (("t", "id INT NOT NULL, v INT", W_INSERT_T),)
W_CELLS = (
    ("P-U1-in", "UPDATE ice.ns.t SET v = 5 WHERE id IN (SELECT id FROM ice.ns.s)"),
    ("P-D1-in", "DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.s)"),
    (
        "P-D3-exists_correlated",
        "DELETE FROM ice.ns.t WHERE EXISTS (SELECT 1 FROM ice.ns.s WHERE s.id = t.id)",
    ),
    ("P-D5-not_in", "DELETE FROM ice.ns.t WHERE id NOT IN (SELECT id FROM ice.ns.s)"),
    (
        "P-D6-exists_no_match",
        "DELETE FROM ice.ns.t WHERE EXISTS (SELECT 1 FROM ice.ns.s WHERE s.id = 100)",
    ),
    ("P-C1-delete_in_self", "DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.t)"),
)
E_CELLS = (
    ("E-u-required-is-null", "DELETE FROM ice.ns.t WHERE id IS NULL"),
    ("E-a-false", "DELETE FROM ice.ns.t WHERE false"),
    ("E-c-one-eq-zero", "DELETE FROM ice.ns.t WHERE 1 = 0"),
    ("E-selective-id-minus-1", "DELETE FROM ice.ns.t WHERE id = -1"),
)
COW = {"write.delete.mode": "copy-on-write", "write.update.mode": "copy-on-write"}
MOR = {"write.delete.mode": "merge-on-read", "write.update.mode": "merge-on-read"}
KIND_PROPERTIES = {
    "v2-cow": {"format-version": "2", **COW},
    "v2-mor": {"format-version": "2", **MOR},
    "v3-cow": {"format-version": "3", **COW},
}
KINDS = tuple(KIND_PROPERTIES)
V3_CONFIG = {"repark.sql.allowCreateFormatVersion3": "true"}
R_CTAS = (
    "CREATE TABLE IF NOT EXISTS offset_nested_sort_1_native AS "
    "SELECT value AS v, value % 7 AS k FROM generate_series(1, 5000)"
)
R_CTAS_PROBE = "SELECT count(*) AS n FROM offset_nested_sort_1_native"
R_DIALECT = "SET datafusion.sql_parser.dialect = 'MsSQL'"
R_BRACKETED_DELETE = "DELETE FROM [ice].[ns].[t] WHERE [id] IN (SELECT [id] FROM [ice].[ns].[s])"
R_KERNEL = "SELECT __repark_cast_timestamp_ns__('2026-01-02 03:04:05')"
PATH_PATTERN = re.compile(r"/(?:tmp|home)/[^\s'\"`)\]]*")
LABELS = (
    "EQUAL",
    "ROWS-DIFFER",
    "EXTRA-COMMIT",
    "MISSING-COMMIT",
    "BOTH-REFUSED",
    "REFUSED-WHERE-SPARK-ACCEPTED",
    "ACCEPTED-WHERE-SPARK-REFUSED",
    "SETUP-MISSING",
    "ORACLE-MISSING",
)


def scrub(text: str) -> str:
    return PATH_PATTERN.sub("<path>", text)


def describe(error: BaseException) -> str:
    return scrub(re.sub(r"\s+", " ", str(error)).strip())[:240]


def attempt(action: Callable[[], Any]) -> dict[str, Any]:
    try:
        value = action()
    except Exception as error:
        return {
            "outcome": "refused",
            "error_class": type(error).__name__,
            "error_text": describe(error),
            "value": None,
        }
    return {"outcome": "answered", "error_class": None, "error_text": None, "value": value}


@dataclass
class Handle:
    root: Path
    execute: Callable[[str], Any]
    query: Callable[[str], list[dict[str, Any]]]
    close: Callable[[], None]
    remove_root: bool


def repark_modules() -> tuple[Any, Any, Any]:
    import repark
    from repark import ReparkSession
    from repark._native import PyReparkSession

    return repark, ReparkSession, PyReparkSession


def open_native(root: Path, modules: tuple[Any, Any, Any]) -> Handle:
    repark, _, native_cls = modules
    root.mkdir(parents=True)
    native = native_cls.native(config=V3_CONFIG)
    native.register_memory_catalog("ice", str(root))
    native.create_namespace("ice", "ns", str(root / "ns"))
    saved = repark._ANSI_NATIVE
    repark._ANSI_NATIVE = native

    def restore() -> None:
        repark._ANSI_NATIVE = saved

    return Handle(
        root,
        lambda sql: repark.sql(sql).collect(),
        lambda sql: repark.sql(sql).to_arrow().to_pylist(),
        restore,
        True,
    )


def open_facade(root: Path, modules: tuple[Any, Any, Any]) -> Handle:
    _, reparksession, _ = modules
    root.mkdir(parents=True)
    session = (
        reparksession.builder.appName("write-cells")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog("ice", str(root))
    session.sql("CREATE NAMESPACE ice.ns").collect()
    return Handle(
        root,
        lambda sql: session.sql(sql).collect(),
        lambda sql: session.sql(sql).toArrow().to_pylist(),
        session.stop,
        True,
    )


def build_spark(jar: str, warehouse: Path) -> Any:
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("write-cells-oracle")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jar)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", str(warehouse))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def open_spark(root: Path, spark: Any) -> Handle:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.ns").collect()
    for table in ("t", "s"):
        spark.sql(f"DROP TABLE IF EXISTS ice.ns.{table} PURGE").collect()
        shutil.rmtree(root / "ns" / table, ignore_errors=True)
    return Handle(
        root,
        lambda sql: spark.sql(sql).collect(),
        lambda sql: [row.asDict() for row in spark.sql(sql).collect()],
        lambda: None,
        False,
    )


def create_statement(name: str, columns: str, kind: str, door: str) -> str:
    if door == "native" and kind == "v2-cow":
        return f"CREATE TABLE ice.ns.{name} ({columns})"
    props = ", ".join(f"'{key}' = '{value}'" for key, value in KIND_PROPERTIES[kind].items())
    using = "" if door == "native" else " USING iceberg"
    return f"CREATE TABLE ice.ns.{name} ({columns}){using} TBLPROPERTIES ({props})"


def setup(handle: Handle, tables: tuple[tuple[str, str, str], ...], kind: str, door: str) -> None:
    for name, columns, insert in tables:
        handle.execute(create_statement(name, columns, kind, door))
        handle.execute(insert)


def metadata_order(path: Path) -> tuple[int, str]:
    match = re.match(r"v?(\d+)", path.name)
    return (int(match.group(1)) if match else -1, path.name)


def table_state(root: Path, table: str) -> dict[str, Any]:
    table_dir = root / "ns" / table
    metadata_dir = table_dir / "metadata"
    documents = (
        sorted(metadata_dir.glob("*.metadata.json"), key=metadata_order)
        if metadata_dir.is_dir()
        else []
    )
    if not documents:
        return {
            "snapshot_id": None,
            "snapshot_count": 0,
            "data_files": 0,
            "operation": None,
            "summary": None,
            "format": None,
        }
    latest = documents[-1]
    document = json.loads(latest.read_text(encoding="utf-8"))
    snapshots = document.get("snapshots") or []
    current = document.get("current-snapshot-id")
    summary = next(
        (snap.get("summary") for snap in snapshots if snap.get("snapshot-id") == current),
        None,
    )
    data_dir = table_dir / "data"
    data_files = (
        [
            path
            for path in data_dir.rglob("*")
            if path.is_file() and not path.name.startswith(".") and not path.name.endswith(".crc")
        ]
        if data_dir.is_dir()
        else []
    )
    return {
        "snapshot_id": current,
        "snapshot_count": len(snapshots),
        "data_files": len(data_files),
        "operation": (summary or {}).get("operation"),
        "summary": summary,
        "format": document.get("format-version"),
    }


def observe(handle: Handle, table: str) -> dict[str, Any]:
    state = table_state(handle.root, table)
    try:
        state["rows"] = [
            [int(row["id"]), int(row["v"])]
            for row in handle.query(f"SELECT id, v FROM ice.ns.{table} ORDER BY id")
        ]
    except Exception as error:
        state["rows"] = None
        state["rows_error"] = describe(error)
    return state


def close_handle(handle: Handle, root: Path) -> None:
    try:
        handle.close()
    finally:
        if handle.remove_root:
            shutil.rmtree(root, ignore_errors=True)


def cell_record(leg: str, door: str, group: str, cell: str, kind: str, sql: str) -> dict[str, Any]:
    return {
        "meta": False,
        "leg": leg,
        "door": door,
        "group": group,
        "cell": cell,
        "kind": kind,
        "sql": sql,
        "status": "ran",
    }


def run_dml_cell(
    record: dict[str, Any],
    opener: Callable[[Path], Handle],
    root: Path,
    tables: tuple[tuple[str, str, str], ...],
    kind: str,
    door: str,
    sql: str,
) -> dict[str, Any]:
    try:
        handle = opener(root)
    except Exception as error:
        shutil.rmtree(root, ignore_errors=True)
        record.update(status="setup-failed", setup_error=describe(error))
        return record
    try:
        setup_result = attempt(lambda: setup(handle, tables, kind, door))
        if setup_result["outcome"] == "refused":
            record.update(status="setup-failed", setup_error=setup_result["error_text"])
            return record
        before = observe(handle, "t")
        result = attempt(lambda: handle.execute(sql))
        after = observe(handle, "t")
    finally:
        close_handle(handle, root)
    record.update(
        outcome=result["outcome"],
        error_class=result["error_class"],
        error_text=result["error_text"],
        rows_before=before.get("rows"),
        rows_after=after.get("rows"),
        rows_error=after.get("rows_error"),
        snapshot_before=before["snapshot_id"],
        snapshot_after=after["snapshot_id"],
        snapshot_count_before=before["snapshot_count"],
        snapshot_count_after=after["snapshot_count"],
        snapshot_changed=before["snapshot_id"] != after["snapshot_id"],
        files_before=before["data_files"],
        files_after=after["data_files"],
        operation_after=after["operation"],
        summary_after=after["summary"],
        format_version=after["format"],
    )
    return record


def run_r1(leg: str, door: str, opener: Callable[[Path], Handle], root: Path) -> dict[str, Any]:
    record = cell_record(leg, door, "R", "R1-ctas-unqualified", "-", R_CTAS)
    try:
        handle = opener(root)
    except Exception as error:
        shutil.rmtree(root, ignore_errors=True)
        record.update(status="setup-failed", setup_error=describe(error))
        return record
    try:
        result = attempt(lambda: handle.execute(R_CTAS))
        probe = attempt(lambda: handle.query(R_CTAS_PROBE))
    finally:
        close_handle(handle, root)
    record.update(
        outcome=result["outcome"],
        error_class=result["error_class"],
        error_text=result["error_text"],
        rows_after=probe["value"] if probe["outcome"] == "answered" else None,
        rows_error=probe["error_text"],
    )
    return record


def run_r2(
    leg: str, door: str, opener: Callable[[Path], Handle], root: Path, native_like: bool
) -> dict[str, Any]:
    record = cell_record(leg, door, "R", "R2-bracketed-delete", "-", R_BRACKETED_DELETE)
    try:
        handle = opener(root)
    except Exception as error:
        shutil.rmtree(root, ignore_errors=True)
        record.update(status="setup-failed", setup_error=describe(error))
        return record
    try:
        setup_result = attempt(lambda: setup(handle, W_TABLES, "v2-cow", door))
        if setup_result["outcome"] == "refused":
            record.update(status="setup-failed", setup_error=setup_result["error_text"])
        else:
            if native_like:
                attempt(lambda: handle.execute(R_DIALECT))
            result = attempt(lambda: handle.execute(R_BRACKETED_DELETE))
            after = observe(handle, "t")
            record.update(
                outcome=result["outcome"],
                error_class=result["error_class"],
                error_text=result["error_text"],
                rows_after=after.get("rows"),
                rows_error=after.get("rows_error"),
            )
    finally:
        close_handle(handle, root)
    return record


def run_r3(leg: str, door: str, opener: Callable[[Path], Handle], root: Path) -> dict[str, Any]:
    record = cell_record(leg, door, "R", "R3-kernel-by-name", "-", R_KERNEL)
    try:
        handle = opener(root)
    except Exception as error:
        shutil.rmtree(root, ignore_errors=True)
        record.update(status="setup-failed", setup_error=describe(error))
        return record
    try:
        result = attempt(lambda: handle.query(R_KERNEL))
    finally:
        close_handle(handle, root)
    record.update(
        outcome=result["outcome"],
        error_class=result["error_class"],
        error_text=result["error_text"],
        rows_after=result["value"],
    )
    return record


def not_attempted(record: dict[str, Any], reason: str) -> dict[str, Any]:
    record.update(status="not-attempted", setup_error=reason)
    return record


def door_plan(leg: str, door: str) -> dict[str, str]:
    if leg == "base" and door == "native":
        return {kind: ("ran" if kind == "v2-cow" else "not-attempted") for kind in KINDS}
    return dict.fromkeys(KINDS, "ran")


def run_leg(leg: str, scratch: Path, raw_dir: Path, spark_jar: str) -> None:
    scratch.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    records: list[dict[str, Any]] = []
    if leg == "spark":
        warehouse = scratch / "spark-warehouse"
        warehouse.mkdir(parents=True, exist_ok=True)
        spark = build_spark(spark_jar, warehouse)
        meta = {"meta": True, "leg": leg, "spark_version": spark.version}
        doors: list[tuple[str, Callable[[Path], Handle], bool]] = [
            ("spark", lambda root: open_spark(root, spark), True)
        ]
    else:
        modules = repark_modules()
        meta = {
            "meta": True,
            "leg": leg,
            "repark_file": scrub(modules[0].__file__),
            "python": sys.version.split()[0],
        }
        doors = [
            ("native", lambda root: open_native(root, modules), False),
            ("facade", lambda root: open_facade(root, modules), False),
        ]
    for door, opener, shared in doors:
        plan = door_plan(leg, door)
        for group, tables, cells in (("W", W_TABLES, W_CELLS), ("E", E_TABLES, E_CELLS)):
            for cell, sql in cells:
                for kind in KINDS:
                    record = cell_record(leg, door, group, cell, kind, sql)
                    if plan[kind] == "not-attempted":
                        records.append(
                            not_attempted(record, "base native creates v2 copy-on-write only")
                        )
                        continue
                    root = (
                        scratch / "spark-warehouse"
                        if shared
                        else scratch / f"{leg}-{door}-{group}-{cell}-{kind}"
                    )
                    records.append(run_dml_cell(record, opener, root, tables, kind, door, sql))
    for door, opener, shared in doors:
        if door == "facade":
            continue
        native_like = door != "spark"
        roots = {
            name: (scratch / "spark-warehouse" if shared else scratch / f"{leg}-{door}-{name}")
            for name in ("R1", "R2", "R3")
        }
        records.append(run_r1(leg, door, opener, roots["R1"]))
        records.append(run_r2(leg, door, opener, roots["R2"], native_like))
        records.append(run_r3(leg, door, opener, roots["R3"]))
    if leg == "spark":
        spark.stop()
    out = raw_dir / f"write-cells-{leg}.jsonl"
    with out.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(meta) + "\n")
        for record in records:
            handle.write(json.dumps(record, default=str) + "\n")
    print(f"wrote {len(records)} records to {out.name}")


def load_records(raw_dir: Path, leg: str) -> list[dict[str, Any]]:
    path = raw_dir / f"write-cells-{leg}.jsonl"
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
    return [row for row in rows if not row.get("meta")]


def door_id(record: dict[str, Any]) -> str:
    return "spark" if record["leg"] == "spark" else f"{record['leg']}/{record['door']}"


def label_for(record: dict[str, Any], reference: dict[str, Any] | None) -> str:
    if record["status"] != "ran":
        return "SETUP-MISSING"
    if reference is None or reference["status"] != "ran":
        return "ORACLE-MISSING"
    spark_answered = reference["outcome"] == "answered"
    door_answered = record["outcome"] == "answered"
    if not spark_answered and not door_answered:
        return "BOTH-REFUSED"
    if spark_answered and not door_answered:
        return "REFUSED-WHERE-SPARK-ACCEPTED"
    if door_answered and not spark_answered:
        return "ACCEPTED-WHERE-SPARK-REFUSED"
    if record["rows_after"] != reference["rows_after"]:
        return "ROWS-DIFFER"
    if record.get("snapshot_changed") and not reference.get("snapshot_changed"):
        return "EXTRA-COMMIT"
    if reference.get("snapshot_changed") and not record.get("snapshot_changed"):
        return "MISSING-COMMIT"
    return "EQUAL"


def format_rows(value: Any, error: str | None) -> str:
    if value is None:
        return f"unread: {error}"[:80] if error else "-"
    if isinstance(value, list) and all(isinstance(item, list) for item in value):
        return " ".join(f"{row[0]}:{row[1]}" for row in value) or "(none)"
    return json.dumps(value, default=str)[:120]


def outcome_text(record: dict[str, Any]) -> str:
    if record["status"] == "not-attempted":
        return f"not attempted ({record['setup_error']})"
    if record["status"] == "setup-failed":
        return f"setup failed: {record['setup_error'][:100]}"
    if record["outcome"] == "answered":
        return "answered"
    return f"refused {record['error_class']}: {(record['error_text'] or '')[:100]}"


def cell_line(record: dict[str, Any], label: str) -> str:
    files = (
        f"{record['files_before']} -> {record['files_after']}" if "files_before" in record else "-"
    )
    snapshot = record.get("snapshot_changed")
    snapshot_text = "-" if snapshot is None else ("yes" if snapshot else "no")
    rows = format_rows(record.get("rows_after"), record.get("rows_error"))
    cells = [
        record["group"],
        record["cell"],
        door_id(record),
        record["kind"],
        outcome_text(record),
        rows,
        snapshot_text,
        files,
        label,
    ]
    return "| " + " | ".join(str(cell).replace("|", "/") for cell in cells) + " |"


def render(raw_dir: Path, out: Path) -> None:
    records: list[dict[str, Any]] = []
    for leg in ("base", "proto", "spark"):
        records.extend(load_records(raw_dir, leg))
    reference = {
        (record["group"], record["cell"], record["kind"]): record
        for record in records
        if record["leg"] == "spark"
    }
    counts: Counter[tuple[str, str, str]] = Counter()
    lines = [
        "# Native-door write cells, measured against Spark (2026-11-08)",
        "",
        "Measured by the Claude Haiku 5.5 clerk under WO native-door-ansi-route-1 (ruling F). "
        "The reference is Spark 4.1.2 with Iceberg 1.11.0 (the oracle). The native door's own "
        "reference is Trino, which is unmeasured, so Spark stands in for it here.",
        "",
        "Door ids: `base/native` is `repark.sql()` on base (stock DataFusion); `base/facade` is "
        "`ReparkSession.sql()` on base; `proto/native` is `repark.sql()` routed through "
        "AnsiDialect; `proto/facade` is the facade on proto; `spark` is PySpark with Iceberg.",
        "",
        "`files` is the data-file count before and after, counting every non-hidden file under "
        "`data/` except `.crc` files, so Iceberg delete files are included. `snapshot changed` "
        "compares the current snapshot id. Group W and group E rows are the persisted rows "
        "sorted by id on table `ice.ns.t`. Spark rows carry the label `reference`.",
        "",
        "## Cells",
        "",
        "| group | cell | leg/door | kind | outcome | rows after (id:v) | snapshot changed | "
        "files | label |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for record in records:
        if record["leg"] == "spark":
            label = "reference" if record["status"] == "ran" else "ORACLE-MISSING"
        else:
            label = label_for(
                record, reference.get((record["group"], record["cell"], record["kind"]))
            )
            counts[(label, record["group"], door_id(record))] += 1
        lines.append(cell_line(record, label))
    lines.extend(["", "## Counts by label, group and leg/door", ""])
    door_ids = sorted({door_id(r) for r in records if r["leg"] != "spark"})
    for group in ("W", "E", "R"):
        present = [d for d in door_ids if any(k[1] == group and k[2] == d for k in counts)]
        lines.extend(
            [
                f"### Group {group}",
                "",
                "| label | " + " | ".join(present) + " |",
                "| --- | " + " | ".join("---" for _ in present) + " |",
            ]
        )
        for label in LABELS:
            values = [str(counts[(label, group, d)]) for d in present]
            lines.append("| " + label + " | " + " | ".join(values) + " |")
        lines.append("")
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {out.name} with {len(records)} rows")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leg", choices=("base", "proto", "spark"))
    parser.add_argument("--scratch", type=Path)
    parser.add_argument("--spark-jar", default="")
    parser.add_argument("--raw-dir", type=Path, required=True)
    parser.add_argument("--render", action="store_true")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)
    if args.render:
        if args.out is None:
            parser.error("--render needs --out")
        render(args.raw_dir, args.out)
        return 0
    if args.leg is None or args.scratch is None:
        parser.error("a leg run needs --leg and --scratch")
    if args.leg == "spark" and not args.spark_jar:
        parser.error("the spark leg needs --spark-jar")
    run_leg(args.leg, args.scratch, args.raw_dir, args.spark_jar)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
