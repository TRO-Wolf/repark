"""Step-0 evidence: every CTAS query-failure cell on the unmodified tree.

Runs the order's F rows times every spelling times hadoop and memory times v2
and v3, plus every K row, one PREPARE cell and one EXPLAIN ANALYZE cell. Each
cell records its outcome, the files left behind, the same-session catalog read
and the facade path read. Fresh-process reads live in fresh_reads.py, the
cancel cell in cancel_probe.py and the Spark columns in cells_spark.py.

Takes the warehouse root and the output path as arguments so no absolute path
is committed. Run with the clone's own interpreter.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from repark import ReparkSession

Q_OPTIMIZER = "SELECT CAST('q' AS INT) AS id, 'r' AS v"
Q_FIRST = "SELECT id, 10 / (id - 1) AS q FROM range(1, 6)"
Q_LAST = "SELECT id, 10 / (5 - id) AS q FROM range(1, 6)"
Q_LATE = "SELECT id, 10 / (199999 - id) AS q FROM range(0, 200000)"
Q_OK = "SELECT id, id * 2 AS q FROM range(1, 6)"
Q_PLAN = "SELECT id, no_such_column FROM range(1, 3)"
Q_SEED = "SELECT id, id AS q FROM range(1, 4)"

VERSIONS = ("2", "3")
CATALOGS = ("hr", "mm")
CODEC_KEY = "spark.sql.iceberg.compression-codec"
ALLOW_V3_KEY = "repark.sql.allowCreateFormatVersion3"
TARGET_SIZE_KEY = "write.target-file-size-bytes"
TARGET_SIZE_VALUE = "1024"


def open_session(warehouse: Path) -> ReparkSession:
    """Open one session over both catalogs, ANSI on, v3 creates allowed."""
    session = (
        ReparkSession.builder.appName("ctas-cleanup-step0")
        .config("spark.sql.catalog.hr", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hr.type", "hadoop")
        .config("spark.sql.catalog.hr.warehouse", str(warehouse / "hadoop"))
        .config("spark.sql.ansi.enabled", "true")
        .config(ALLOW_V3_KEY, "true")
        .getOrCreate()
    )
    session.register_memory_catalog("mm", warehouse / "memory")
    session.sql("CREATE NAMESPACE IF NOT EXISTS hr.ns")
    session.sql("CREATE NAMESPACE IF NOT EXISTS mm.ns")
    return session


def tree(root: Path) -> list[str] | str:
    """List every file under root relative to it, or ABSENT when missing."""
    if not root.exists():
        return "ABSENT"
    return sorted(str(path.relative_to(root)) for path in root.rglob("*") if path.is_file())


def scrub(text: str, warehouse: Path) -> str:
    """Replace machine paths in recorded text with stable placeholders."""
    return text.replace(str(warehouse), "<warehouse>").replace(str(Path.home()), "<home>")


def short_error(error: Exception, warehouse: Path) -> str:
    """Render one exception as a scrubbed single-line record."""
    first = str(error).splitlines()[0][:200] if str(error) else ""
    return scrub(f"{type(error).__name__}: {first}", warehouse)


def catalog_count(session: ReparkSession, catalog: str, table: str, warehouse: Path) -> int | str:
    """Read the row count through the catalog, or the refusal text."""
    try:
        rows = session.sql(f"SELECT count(*) AS c FROM {catalog}.ns.{table}")
        return int(rows.to_arrow().to_pylist()[0]["c"])
    except Exception as error:
        return short_error(error, warehouse)


def path_read(session: ReparkSession, table_dir: Path, warehouse: Path) -> dict[str, Any]:
    """Read the table directory as an Iceberg path, or the refusal text."""
    try:
        frame = session.read.format("iceberg").load(str(table_dir))
        return {"columns": list(frame.columns), "count": int(frame.count())}
    except Exception as error:
        return {"error": short_error(error, warehouse)}


def metadata_versions(table_dir: Path) -> dict[str, Any]:
    """Map each metadata json pattern to the format version it declares."""
    found: dict[str, Any] = {}
    meta = table_dir / "metadata"
    if not meta.is_dir():
        return found
    for path in sorted(meta.glob("*.metadata.json")):
        name = path.name
        pattern = "00000-*.metadata.json" if name.startswith("00000-") else name
        try:
            document = json.loads(path.read_text())
        except (OSError, ValueError):
            found[pattern] = "unreadable"
            continue
        found[pattern] = document.get("format-version", "?")
    return found


def properties_clause(properties: dict[str, str]) -> str:
    """Render TBLPROPERTIES for fixed safe keys, or nothing when empty."""
    if not properties:
        return ""
    pairs = ", ".join(f"'{key}'='{value}'" for key, value in properties.items())
    return f" TBLPROPERTIES ({pairs})"


def partition_clause(partition: str | None) -> str:
    """Render PARTITIONED BY for the fixed column, or nothing when plain."""
    if partition is None:
        return ""
    return f" PARTITIONED BY ({partition})"


def run_sql_create(
    session: ReparkSession,
    catalog: str,
    table: str,
    query: str,
    partition: str | None,
    properties: dict[str, str],
    options: dict[str, str],
) -> None:
    """Run the SQL CREATE TABLE AS spelling; options never reach SQL here."""
    del options
    statement = (
        f"CREATE TABLE {catalog}.ns.{table} USING iceberg"
        f"{partition_clause(partition)}{properties_clause(properties)} AS {query}"
    )
    session.sql(statement)


def run_sql_replace(
    session: ReparkSession,
    catalog: str,
    table: str,
    query: str,
    partition: str | None,
    properties: dict[str, str],
    options: dict[str, str],
) -> None:
    """Run the SQL CREATE OR REPLACE TABLE AS spelling on any target."""
    del options
    statement = (
        f"CREATE OR REPLACE TABLE {catalog}.ns.{table} USING iceberg"
        f"{partition_clause(partition)}{properties_clause(properties)} AS {query}"
    )
    session.sql(statement)


def run_sql_replace_verb(
    session: ReparkSession,
    catalog: str,
    table: str,
    query: str,
    partition: str | None,
    properties: dict[str, str],
    options: dict[str, str],
) -> None:
    """Run the SQL REPLACE TABLE AS spelling over an existing table."""
    del options
    statement = (
        f"REPLACE TABLE {catalog}.ns.{table} USING iceberg"
        f"{partition_clause(partition)}{properties_clause(properties)} AS {query}"
    )
    session.sql(statement)


def apply_frame(writer: Any, partition: str | None, properties: dict[str, str]) -> None:
    """Set table properties and partitioning on a writeTo writer."""
    for key, value in properties.items():
        writer.tableProperty(key, value)
    if partition is not None:
        writer.partitionedBy(partition)


def run_write_create(
    session: ReparkSession,
    catalog: str,
    table: str,
    query: str,
    partition: str | None,
    properties: dict[str, str],
    options: dict[str, str],
) -> None:
    """Run the writeTo create spelling with table properties and options."""
    writer = session.sql(query).writeTo(f"{catalog}.ns.{table}").using("iceberg")
    apply_frame(writer, partition, properties)
    for key, value in options.items():
        writer.option(key, value)
    writer.create()


def run_write_replace(
    session: ReparkSession,
    catalog: str,
    table: str,
    query: str,
    partition: str | None,
    properties: dict[str, str],
    options: dict[str, str],
) -> None:
    """Run the writeTo createOrReplace spelling with properties and options."""
    writer = session.sql(query).writeTo(f"{catalog}.ns.{table}").using("iceberg")
    apply_frame(writer, partition, properties)
    for key, value in options.items():
        writer.option(key, value)
    writer.createOrReplace()


def run_write_replace_only(
    session: ReparkSession,
    catalog: str,
    table: str,
    query: str,
    partition: str | None,
    properties: dict[str, str],
    options: dict[str, str],
) -> None:
    """Run the writeTo replace spelling over an existing table."""
    writer = session.sql(query).writeTo(f"{catalog}.ns.{table}").using("iceberg")
    apply_frame(writer, partition, properties)
    for key, value in options.items():
        writer.option(key, value)
    writer.replace()


def run_save(
    session: ReparkSession,
    catalog: str,
    table: str,
    query: str,
    partition: str | None,
    properties: dict[str, str],
    options: dict[str, str],
    mode: str | None,
) -> None:
    """Run one saveAsTable mode; properties have no saveAsTable channel."""
    del properties
    writer = session.sql(query).write.format("iceberg")
    if mode is not None:
        writer = writer.mode(mode)
    if partition is not None:
        writer = writer.partitionBy(partition)
    for key, value in options.items():
        writer = writer.option(key, value)
    writer.saveAsTable(f"{catalog}.ns.{table}")


def run_save_default(
    session: ReparkSession,
    catalog: str,
    table: str,
    query: str,
    partition: str | None,
    properties: dict[str, str],
    options: dict[str, str],
) -> None:
    """Run saveAsTable with no mode set on a new table."""
    run_save(session, catalog, table, query, partition, properties, options, None)


def run_save_append(
    session: ReparkSession,
    catalog: str,
    table: str,
    query: str,
    partition: str | None,
    properties: dict[str, str],
    options: dict[str, str],
) -> None:
    """Run saveAsTable in append mode on a new table."""
    run_save(session, catalog, table, query, partition, properties, options, "append")


def run_save_overwrite(
    session: ReparkSession,
    catalog: str,
    table: str,
    query: str,
    partition: str | None,
    properties: dict[str, str],
    options: dict[str, str],
) -> None:
    """Run saveAsTable in overwrite mode."""
    run_save(session, catalog, table, query, partition, properties, options, "overwrite")


def run_prepare(
    session: ReparkSession,
    catalog: str,
    table: str,
    query: str,
    partition: str | None,
    properties: dict[str, str],
    options: dict[str, str],
) -> None:
    """Run the carried statement verbatim for the PREPARE and EXPLAIN cells."""
    del catalog, table, partition, properties, options
    session.sql(query)


SPELLINGS: dict[str, Callable[..., None]] = {
    "sql": run_sql_create,
    "cor": run_sql_replace,
    "wcreate": run_write_create,
    "wcor": run_write_replace,
    "savedef": run_save_default,
    "saveapp": run_save_append,
    "saveovr": run_save_overwrite,
    "sqlreplace": run_sql_replace_verb,
    "wreplace": run_write_replace_only,
    "verbatim": run_prepare,
}

CREATE_SPELLINGS = ("sql", "cor", "wcreate", "wcor", "savedef", "saveapp", "saveovr")
K4_SPELLINGS = ("cor", "sqlreplace", "wcor", "wreplace", "saveovr")


def version_properties(spelling: str, version: str) -> dict[str, str]:
    """Carry format-version as TBLPROPERTIES except where no channel exists."""
    if spelling.startswith("save"):
        return {}
    return {"format-version": version}


def version_options(spelling: str, version: str) -> dict[str, str]:
    """Probe the saveAsTable v3 seat through a write option, else nothing."""
    if spelling.startswith("save") and version == "3":
        return {"format-version": version}
    return {}


def target_properties(spelling: str) -> dict[str, str]:
    """Carry the F5 target size as a table property except on saveAsTable."""
    if spelling.startswith("save"):
        return {}
    return {TARGET_SIZE_KEY: TARGET_SIZE_VALUE}


def target_options(spelling: str) -> dict[str, str]:
    """Carry the F5 target size as a write option on saveAsTable only."""
    if spelling.startswith("save"):
        return {TARGET_SIZE_KEY: TARGET_SIZE_VALUE}
    return {}


def seed_fresh(
    session: ReparkSession, catalog: str, table: str, version: str, warehouse: Path
) -> str:
    """Drop the target when present; report a seed failure as text."""
    del version
    try:
        session.sql(f"DROP TABLE IF EXISTS {catalog}.ns.{table}")
    except Exception as error:
        return short_error(error, warehouse)
    return ""


def seed_three_rows(
    session: ReparkSession, catalog: str, table: str, version: str, warehouse: Path
) -> str:
    """Seed a live three-row table of the requested version for K4 cells."""
    try:
        session.sql(f"DROP TABLE IF EXISTS {catalog}.ns.{table}")
        session.sql(
            f"CREATE TABLE {catalog}.ns.{table} USING iceberg"
            f" TBLPROPERTIES ('format-version'='{version}') AS {Q_SEED}"
        )
    except Exception as error:
        return short_error(error, warehouse)
    return ""


SEEDS: dict[str, Callable[..., str]] = {"fresh": seed_fresh, "seed3": seed_three_rows}


def build_cells() -> list[dict[str, Any]]:
    """Build every step-0 cell across rows, spellings, catalogs, versions."""
    cells: list[dict[str, Any]] = []
    for row, query in (("f1", Q_OPTIMIZER), ("f2", Q_FIRST), ("f3", Q_LAST)):
        for spelling in CREATE_SPELLINGS:
            for catalog in CATALOGS:
                for version in VERSIONS:
                    cells.append(
                        {
                            "cell": f"{row}-{spelling}-{catalog}-v{version}",
                            "table": f"{row}_{spelling}_{catalog}_v{version}",
                            "row": row,
                            "spelling": spelling,
                            "catalog": catalog,
                            "version": version,
                            "query": query,
                            "partition": None,
                            "properties": version_properties(spelling, version),
                            "options": version_options(spelling, version),
                            "seed": "fresh",
                            "session": "main",
                            "spark_probe": row in ("f1", "f3")
                            and spelling == "sql"
                            and catalog == "hr"
                            and version == "2",
                        }
                    )
    for spelling in CREATE_SPELLINGS:
        for catalog in CATALOGS:
            for version in VERSIONS:
                cells.append(
                    {
                        "cell": f"f4-{spelling}-{catalog}-v{version}",
                        "table": f"f4_{spelling}_{catalog}_v{version}",
                        "row": "f4",
                        "spelling": spelling,
                        "catalog": catalog,
                        "version": version,
                        "query": Q_LAST,
                        "partition": "q",
                        "properties": version_properties(spelling, version),
                        "options": version_options(spelling, version),
                        "seed": "fresh",
                        "session": "main",
                        "spark_probe": False,
                    }
                )
    for spelling in CREATE_SPELLINGS:
        for catalog in CATALOGS:
            for version in VERSIONS:
                properties = version_properties(spelling, version)
                properties.update(target_properties(spelling))
                options = version_options(spelling, version)
                options.update(target_options(spelling))
                cells.append(
                    {
                        "cell": f"f5-{spelling}-{catalog}-v{version}",
                        "table": f"f5_{spelling}_{catalog}_v{version}",
                        "row": "f5",
                        "spelling": spelling,
                        "catalog": catalog,
                        "version": version,
                        "query": Q_LATE,
                        "partition": None,
                        "properties": properties,
                        "options": options,
                        "seed": "fresh",
                        "session": "main",
                        "spark_probe": False,
                    }
                )
    for spelling in CREATE_SPELLINGS:
        for catalog in CATALOGS:
            for version in VERSIONS:
                if spelling.startswith("save") or spelling.startswith("w"):
                    properties = version_properties(spelling, version)
                    options = version_options(spelling, version)
                    options[TARGET_SIZE_KEY] = TARGET_SIZE_VALUE
                else:
                    properties = version_properties(spelling, version)
                    properties[TARGET_SIZE_KEY] = TARGET_SIZE_VALUE
                    options = {}
                cells.append(
                    {
                        "cell": f"f6-{spelling}-{catalog}-v{version}",
                        "table": f"f6_{spelling}_{catalog}_v{version}",
                        "row": "f6",
                        "spelling": spelling,
                        "catalog": catalog,
                        "version": version,
                        "query": Q_LATE,
                        "partition": None,
                        "properties": properties,
                        "options": options,
                        "seed": "fresh",
                        "session": "f6",
                        "spark_probe": spelling == "sql" and catalog == "hr" and version == "2",
                    }
                )
    for spelling in CREATE_SPELLINGS:
        for catalog in CATALOGS:
            for version in VERSIONS:
                cells.append(
                    {
                        "cell": f"k2-{spelling}-{catalog}-v{version}",
                        "table": f"k2_{spelling}_{catalog}_v{version}",
                        "row": "k2",
                        "spelling": spelling,
                        "catalog": catalog,
                        "version": version,
                        "query": Q_OK,
                        "partition": None,
                        "properties": version_properties(spelling, version),
                        "options": version_options(spelling, version),
                        "seed": "fresh",
                        "session": "main",
                        "spark_probe": False,
                    }
                )
    for catalog in CATALOGS:
        for version in VERSIONS:
            cells.append(
                {
                    "cell": f"k1-sql-{catalog}-v{version}",
                    "table": f"k1_sql_{catalog}_v{version}",
                    "row": "k1",
                    "spelling": "sql",
                    "catalog": catalog,
                    "version": version,
                    "query": Q_PLAN,
                    "partition": None,
                    "properties": version_properties("sql", version),
                    "options": {},
                    "seed": "fresh",
                    "session": "main",
                    "spark_probe": False,
                }
            )
            cells.append(
                {
                    "cell": f"k3-sql-{catalog}-v{version}",
                    "table": f"k3_sql_{catalog}_v{version}",
                    "row": "k3",
                    "spelling": "sql",
                    "catalog": catalog,
                    "version": version,
                    "query": Q_OK,
                    "partition": None,
                    "properties": version_properties("sql", version),
                    "options": {},
                    "seed": "fresh",
                    "session": "f6",
                    "spark_probe": False,
                }
            )
    for catalog in CATALOGS:
        cells.append(
            {
                "cell": f"k3-wcreate-{catalog}-v2",
                "table": f"k3_wcreate_{catalog}_v2",
                "row": "k3",
                "spelling": "wcreate",
                "catalog": catalog,
                "version": "2",
                "query": Q_OK,
                "partition": None,
                "properties": version_properties("wcreate", "2"),
                "options": {TARGET_SIZE_KEY: TARGET_SIZE_VALUE},
                "seed": "fresh",
                "session": "main",
                "spark_probe": False,
            }
        )
    for spelling in K4_SPELLINGS:
        for catalog in CATALOGS:
            for version in VERSIONS:
                cells.append(
                    {
                        "cell": f"k4-{spelling}-{catalog}-v{version}",
                        "table": f"k4_{spelling}_{catalog}_v{version}",
                        "row": "k4",
                        "spelling": spelling,
                        "catalog": catalog,
                        "version": version,
                        "query": Q_LAST,
                        "partition": None,
                        "properties": version_properties(spelling, version),
                        "options": {},
                        "seed": "seed3",
                        "session": "main",
                        "spark_probe": False,
                    }
                )
    cells.append(
        {
            "cell": "prepare-sql-hr-v2",
            "table": "pp1",
            "row": "prepare",
            "spelling": "verbatim",
            "catalog": "hr",
            "version": "2",
            "query": f"PREPARE ctas_p1 AS CREATE TABLE hr.ns.pp1 USING iceberg AS {Q_LAST}",
            "partition": None,
            "properties": {},
            "options": {},
            "seed": "fresh",
            "session": "main",
            "spark_probe": False,
        }
    )
    cells.append(
        {
            "cell": "explain-sql-hr-v2",
            "table": "ea1",
            "row": "explain",
            "spelling": "verbatim",
            "catalog": "hr",
            "version": "2",
            "query": f"EXPLAIN ANALYZE CREATE TABLE hr.ns.ea1 USING iceberg AS {Q_LAST}",
            "partition": None,
            "properties": {},
            "options": {},
            "seed": "fresh",
            "session": "main",
            "spark_probe": False,
        }
    )
    return cells


def run_cell(session: ReparkSession, cell: dict[str, Any], warehouse: Path) -> dict[str, Any]:
    """Run one cell and record its outcome, files and both same-process reads."""
    catalog = str(cell["catalog"])
    table = str(cell["table"])
    kind = "hadoop" if catalog == "hr" else "memory"
    table_dir = warehouse / kind / "ns" / table
    seed_error = SEEDS[str(cell["seed"])](session, catalog, table, str(cell["version"]), warehouse)
    before = tree(table_dir)
    outcome = "OK"
    error_text = ""
    try:
        SPELLINGS[str(cell["spelling"])](
            session,
            catalog,
            table,
            str(cell["query"]),
            cell["partition"],
            cell["properties"],
            cell["options"],
        )
    except Exception as error:
        outcome = type(error).__name__
        error_text = short_error(error, warehouse)
    after = tree(table_dir)
    return {
        "cell": cell["cell"],
        "row": cell["row"],
        "spelling": cell["spelling"],
        "catalog": catalog,
        "version": cell["version"],
        "table": f"{catalog}.ns.{table}",
        "session": cell["session"],
        "spark_probe": cell["spark_probe"],
        "outcome": outcome,
        "error": error_text,
        "seed_error": seed_error,
        "files_before": before,
        "files_after": after,
        "same_session": catalog_count(session, catalog, table, warehouse),
        "path": path_read(session, table_dir, warehouse),
        "meta_versions": metadata_versions(table_dir),
    }


def main(argv: list[str] | None = None) -> int:
    """Run the full step-0 table into the output path."""
    parser = argparse.ArgumentParser(description="Measure the CTAS failure table.")
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    warehouse = args.warehouse
    (warehouse / "hadoop").mkdir(parents=True, exist_ok=True)
    (warehouse / "memory").mkdir(parents=True, exist_ok=True)
    session = open_session(warehouse)
    codec_on = False
    records = []
    for cell in build_cells():
        want_codec = str(cell["session"]) == "f6"
        if want_codec and not codec_on:
            session.conf.set(CODEC_KEY, "zstd")
            codec_on = True
        elif codec_on and not want_codec:
            session.conf.unset(CODEC_KEY)
            codec_on = False
        record = run_cell(session, cell, warehouse)
        records.append(record)
        print(json.dumps(record, default=str), flush=True)
    if codec_on:
        session.conf.unset(CODEC_KEY)
    args.out.write_text(json.dumps(records, indent=1, default=str) + "\n")
    print(f"DONE {len(records)}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
