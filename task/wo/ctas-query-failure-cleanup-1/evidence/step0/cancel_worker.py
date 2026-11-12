"""Step-0 cancel worker: one slow CTAS the probe kills mid-flight.

Runs a single large successful CTAS so cancel_probe.py can terminate the
process after the begin file lands and measure what a kill leaves behind.
Takes the warehouse root and the table name as arguments.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from repark import ReparkSession

Q_SLOW = "SELECT id, id % 1000 AS q FROM range(0, 20000000)"


def main(argv: list[str] | None = None) -> int:
    """Run the slow CTAS; the probe decides when this process dies."""
    parser = argparse.ArgumentParser(description="Run one slow CTAS for the cancel probe.")
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("--table", type=str, required=True)
    args = parser.parse_args(argv)
    session = (
        ReparkSession.builder.appName("ctas-cleanup-step0-cancel")
        .config("spark.sql.catalog.hr", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hr.type", "hadoop")
        .config("spark.sql.catalog.hr.warehouse", str(args.warehouse / "hadoop"))
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    session.sql("CREATE NAMESPACE IF NOT EXISTS hr.ns")
    outcome = "OK"
    error_text = ""
    try:
        session.sql(f"CREATE TABLE hr.ns.{args.table} USING iceberg AS {Q_SLOW}")
    except Exception as error:
        outcome = type(error).__name__
        error_text = str(error).splitlines()[0][:200] if str(error) else ""
    print(json.dumps({"outcome": outcome, "error": error_text}), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
