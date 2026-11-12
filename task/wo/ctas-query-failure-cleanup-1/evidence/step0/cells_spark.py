"""Step-0 Spark oracle: F1, F3 and F6 plus Spark path reads of RePark leftovers.

Runs one CTAS per named row on Spark 4.1.2 with Iceberg 1.11, then reads the
three RePark probe leftovers through Spark's path reader. Takes the Spark
warehouse, the RePark warehouse, the probe cells file and the Iceberg runtime
jar as arguments so no absolute path is committed. Run with the oracle
interpreter under the single-JVM lock.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

Q_F1 = "SELECT CAST('q' AS INT) AS id, 'r' AS v"
Q_F3 = "SELECT id, 10 / (5 - id) AS q FROM range(1, 6)"
Q_F6 = "SELECT id, 10 / (199999 - id) AS q FROM range(0, 200000)"


def open_session(warehouse: Path, jar: Path) -> SparkSession:
    """Open the Spark oracle session over a hadoop catalog, ANSI on."""
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("ctas-cleanup-step0-spark")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.jars", str(jar))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.hd", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hd.type", "hadoop")
        .config("spark.sql.catalog.hd.warehouse", str(warehouse))
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hd.ns")
    return spark


def tree(root: Path) -> list[str] | str:
    """List every file under root relative to it, or ABSENT when missing."""
    if not root.exists():
        return "ABSENT"
    return sorted(str(path.relative_to(root)) for path in root.rglob("*") if path.is_file())


def scrub(text: str, roots: list[Path]) -> str:
    """Replace machine paths in recorded text with stable placeholders."""
    for root in roots:
        text = text.replace(str(root), "<warehouse>")
    return text.replace(str(Path.home()), "<home>")


def short_error(error: Exception, roots: list[Path]) -> str:
    """Render one exception as a scrubbed single-line record."""
    first = str(error).splitlines()[0][:200] if str(error) else ""
    return scrub(f"{type(error).__name__}: {first}", roots)


def summarize(files: list[str] | str) -> dict[str, Any]:
    """Condense a file tree to metadata patterns plus data file counts."""
    if isinstance(files, str):
        return {"tree": files}
    metadata = sorted(
        "00000-*.metadata.json" if name.startswith("metadata/00000-") else name
        for name in files
        if name.startswith("metadata/") and name.endswith(".metadata.json")
    )
    data = [name for name in files if name.startswith("data/")]
    return {"metadata": metadata, "data_count": len(data)}


def catalog_count(spark: SparkSession, table: str, roots: list[Path]) -> int | str:
    """Read the row count through the catalog, or the refusal text."""
    try:
        return int(spark.sql(f"SELECT count(*) AS c FROM hd.ns.{table}").collect()[0]["c"])
    except Exception as error:
        return short_error(error, roots)


def path_read(spark: SparkSession, table_dir: Path, roots: list[Path]) -> dict[str, Any]:
    """Read the table directory as an Iceberg path, or the refusal text."""
    try:
        frame = spark.read.format("iceberg").load(str(table_dir))
        return {"columns": list(frame.columns), "count": int(frame.count())}
    except Exception as error:
        return {"error": short_error(error, roots)}


def run_ctas(
    spark: SparkSession, label: str, table: str, statement: str, roots: list[Path]
) -> dict[str, Any]:
    """Run one Spark CTAS cell and record its outcome, files and reads."""
    spark.sql(f"DROP TABLE IF EXISTS hd.ns.{table}")
    table_dir = roots[0] / "ns" / table
    before = tree(table_dir)
    outcome = "OK"
    error_text = ""
    try:
        spark.sql(statement)
    except Exception as error:
        outcome = type(error).__name__
        error_text = short_error(error, roots)
    after = tree(table_dir)
    return {
        "cell": label,
        "outcome": outcome,
        "error": error_text,
        "files_before": summarize(before),
        "files_after": summarize(after),
        "same_session": catalog_count(spark, table, roots),
        "path": path_read(spark, table_dir, roots),
    }


def run_write_to(
    spark: SparkSession, label: str, table: str, query: str, roots: list[Path]
) -> dict[str, Any]:
    """Run the Spark F6 writeTo cell with a 1 KB target file size option."""
    spark.sql(f"DROP TABLE IF EXISTS hd.ns.{table}")
    table_dir = roots[0] / "ns" / table
    before = tree(table_dir)
    outcome = "OK"
    error_text = ""
    try:
        spark.sql(query).writeTo(f"hd.ns.{table}").using("iceberg").option(
            "target-file-size-bytes", "1024"
        ).create()
    except Exception as error:
        outcome = type(error).__name__
        error_text = short_error(error, roots)
    after = tree(table_dir)
    return {
        "cell": label,
        "outcome": outcome,
        "error": error_text,
        "files_before": summarize(before),
        "files_after": summarize(after),
        "same_session": catalog_count(spark, table, roots),
        "path": path_read(spark, table_dir, roots),
    }


def probe_cells(cells_path: Path) -> list[dict[str, Any]]:
    """Load the RePark probe cells the Spark path reader must revisit."""
    return [
        row
        for row in json.loads(cells_path.read_text())
        if isinstance(row, dict) and row.get("spark_probe")
    ]


def main(argv: list[str] | None = None) -> int:
    """Run the three Spark cells plus the RePark leftover path reads."""
    parser = argparse.ArgumentParser(description="Measure the Spark oracle columns.")
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("--repark-warehouse", type=Path, required=True)
    parser.add_argument("--cells", type=Path, required=True)
    parser.add_argument("--jar", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    roots = [args.warehouse, args.repark_warehouse]
    spark = open_session(args.warehouse, args.jar)
    print(
        json.dumps(
            {"banner": spark.version, "timezone": spark.conf.get("spark.sql.session.timeZone")}
        ),
        flush=True,
    )
    records = [
        run_ctas(spark, "spark-f1", "f1", f"CREATE TABLE hd.ns.f1 USING iceberg AS {Q_F1}", roots),
        run_ctas(spark, "spark-f3", "f3", f"CREATE TABLE hd.ns.f3 USING iceberg AS {Q_F3}", roots),
        run_write_to(spark, "spark-f6", "f6", Q_F6, roots),
    ]
    for row in probe_cells(args.cells):
        table = str(row["table"]).split(".")[-1]
        kind = "hadoop" if str(row["catalog"]) == "hr" else "memory"
        table_dir = args.repark_warehouse / kind / "ns" / table
        records.append(
            {
                "cell": f"spark-path-{row['cell']}",
                "repark_table": row["table"],
                "path": path_read(spark, table_dir, roots),
            }
        )
    args.out.write_text(json.dumps(records, indent=1, default=str) + "\n")
    for record in records:
        print(json.dumps(record, default=str), flush=True)
    print(f"DONE {len(records)}", flush=True)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
