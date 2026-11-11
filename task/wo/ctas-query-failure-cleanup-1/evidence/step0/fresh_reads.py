"""Step-0 fresh-process catalog reads over the step-0 warehouse.

Opens one new session per catalog kind and rereads every step-0 table through
the catalog, so the table shows whether a fresh process sees anything the
writing session left. Takes the warehouse root, the cells file and the output
path as arguments. Run with the clone's own interpreter.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from repark import ReparkSession


def open_reader(warehouse: Path, catalog: str) -> ReparkSession:
    """Open a fresh session over one catalog kind with no writer state."""
    if catalog == "hr":
        session = (
            ReparkSession.builder.appName("ctas-cleanup-step0-fresh")
            .config("spark.sql.catalog.hr", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.hr.type", "hadoop")
            .config("spark.sql.catalog.hr.warehouse", str(warehouse / "hadoop"))
            .config("spark.sql.ansi.enabled", "true")
            .getOrCreate()
        )
        return session
    session = ReparkSession.builder.appName("ctas-cleanup-step0-fresh").getOrCreate()
    session.register_memory_catalog("mm", warehouse / "memory")
    return session


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


def main(argv: list[str] | None = None) -> int:
    """Reread every step-0 table from a fresh session per catalog kind."""
    parser = argparse.ArgumentParser(description="Reread step-0 tables from fresh sessions.")
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("--cells", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    rows = json.loads(args.cells.read_text())
    readers = {catalog: open_reader(args.warehouse, catalog) for catalog in ("hr", "mm")}
    records: dict[str, Any] = {}
    for index, row in enumerate(rows):
        catalog = str(row["catalog"])
        table = str(row["table"]).split(".")[-1]
        records[str(row["cell"])] = catalog_count(readers[catalog], catalog, table, args.warehouse)
        if index % 50 == 0:
            print(f"READ {index + 1}/{len(rows)}", flush=True)
    args.out.write_text(json.dumps(records, indent=1, default=str) + "\n")
    print(f"DONE {len(records)}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
