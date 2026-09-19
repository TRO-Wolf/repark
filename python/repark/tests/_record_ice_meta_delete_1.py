"""Record Spark 4.1.2's answers for ICE-META-DELETE-1 — DELETE that covers whole data files.

NOT a ``test_`` module: pytest never collects it. It owns the 18 shapes, and the live leg
re-derives the 72 recorded cells (18 shapes x format v2/v3 x ``write.delete.mode``
merge-on-read/copy-on-write) from live PySpark into
``ice_meta_delete_1_spark_oracle.json``.

Each cell holds the surviving ``rows``, every snapshot's ``op`` plus the summary counters in
``SUMMARY_KEYS``, and the live data files' ``(content, record_count)`` pairs. The pins in
``test_ice_meta_delete_1.py`` read every expectation from that file and replay the same shapes
against RePark, so a critic with no Spark on the box runs them unchanged.

Run with a PySpark 4.1.2 interpreter that can resolve the Iceberg runtime GAV from
``_oracle_pins``::

    _record_ice_meta_delete_1.py [warehouse]          # rewrite the fixture
    _record_ice_meta_delete_1.py --check [warehouse]  # re-derive and exit non-zero on drift

``warehouse`` is created fresh (removed first) and defaults to a scratch directory.
"""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV

ORACLE_FILE = Path(__file__).with_name("ice_meta_delete_1_spark_oracle.json")
CATALOG = "sc"
NAMESPACE = "ns"
UNPART = "(id INT, cat STRING, v STRING)"
SEED2 = (
    "INSERT INTO {t} VALUES (1,'x','a'),(2,'x','b'),(3,'y','c')",
    "INSERT INTO {t} VALUES (7,'g','x')",
)
SEED3 = (*SEED2, "INSERT INTO {t} VALUES (8,'g','y'),(9,'g','z')")
NULL_SEED = (
    "INSERT INTO {t} VALUES (1,'x','a')",
    "INSERT INTO {t} VALUES (2,'x',NULL)",
)
SUMMARY_KEYS: tuple[str, ...] = (
    "added-data-files",
    "deleted-data-files",
    "added-delete-files",
    "removed-delete-files",
    "added-dvs",
    "removed-dvs",
    "added-position-deletes",
    "removed-position-deletes",
    "deleted-records",
    "added-records",
    "total-data-files",
    "total-delete-files",
    "total-records",
    "total-position-deletes",
)
SHAPES: dict[str, tuple[str, str, tuple[str, ...], tuple[str, ...]]] = {
    "whole_one_file": (UNPART, "", SEED2, ("DELETE FROM {t} WHERE id = 7",)),
    "whole_two_files": (UNPART, "", SEED3, ("DELETE FROM {t} WHERE id >= 7",)),
    "mixed_partial_and_whole": (UNPART, "", SEED2, ("DELETE FROM {t} WHERE id >= 3",)),
    "partial_only": (UNPART, "", SEED2, ("DELETE FROM {t} WHERE id = 2",)),
    "all_rows_true": (UNPART, "", SEED2, ("DELETE FROM {t} WHERE true",)),
    "all_rows_nopred": (UNPART, "", SEED2, ("DELETE FROM {t}",)),
    "no_match": (UNPART, "", SEED2, ("DELETE FROM {t} WHERE id = 99",)),
    "string_eq_whole": (UNPART, "", SEED2, ("DELETE FROM {t} WHERE cat = 'g'",)),
    "partition_select": (
        UNPART,
        "PARTITIONED BY (cat)",
        SEED2,
        ("DELETE FROM {t} WHERE cat = 'x'",),
    ),
    "partition_select_bucket": (
        UNPART,
        "PARTITIONED BY (bucket(4, id))",
        SEED2,
        ("DELETE FROM {t} WHERE id = 7",),
    ),
    "partition_plus_metrics": (
        UNPART,
        "PARTITIONED BY (cat)",
        SEED2,
        ("DELETE FROM {t} WHERE cat = 'x' AND id <= 2",),
    ),
    "partition_partial": (
        UNPART,
        "PARTITIONED BY (cat)",
        SEED2,
        ("DELETE FROM {t} WHERE cat = 'x' AND id = 1",),
    ),
    "prior_deletes_then_whole": (
        UNPART,
        "",
        SEED2,
        ("DELETE FROM {t} WHERE id = 1", "DELETE FROM {t} WHERE id <= 3"),
    ),
    "prior_deletes_then_rest": (
        UNPART,
        "",
        SEED2,
        ("DELETE FROM {t} WHERE id = 1", "DELETE FROM {t} WHERE id IN (2, 3)"),
    ),
    "is_null_whole": (UNPART, "", NULL_SEED, ("DELETE FROM {t} WHERE v IS NULL",)),
    "or_whole": (UNPART, "", SEED2, ("DELETE FROM {t} WHERE id = 7 OR id = 99",)),
    "not_in_whole": (UNPART, "", SEED2, ("DELETE FROM {t} WHERE id NOT IN (1, 2, 3)",)),
    "nondeterministic_like": (UNPART, "", SEED2, ("DELETE FROM {t} WHERE v LIKE 'x%'",)),
}
VERSIONS: tuple[int, ...] = (2, 3)
MODES: dict[str, str] = {
    "mor": "'write.delete.mode'='merge-on-read'",
    "cow": "'write.delete.mode'='copy-on-write'",
}


def cell_id(shape: str, version: int, mode: str) -> str:
    """Return the fixture key of one cell."""
    return f"{shape}_v{version}_{mode}"


def cell_ids() -> list[str]:
    """Return every recorded cell key, in shape order."""
    return [
        cell_id(shape, version, mode)
        for shape in SHAPES
        for version in VERSIONS
        for mode in MODES
    ]


def table_name(shape: str, version: int, mode: str) -> str:
    """Return the three-part table name of one cell."""
    return f"{CATALOG}.{NAMESPACE}.{cell_id(shape, version, mode)}"


def create_sql(shape: str, version: int, mode: str) -> str:
    """Return the CREATE TABLE statement of one cell."""
    columns, partition, _, _ = SHAPES[shape]
    return (
        f"CREATE TABLE {table_name(shape, version, mode)} {columns} USING iceberg {partition} "
        f"TBLPROPERTIES ('format-version'='{version}', {MODES[mode]})"
    )


def keep_summary(summary: dict[str, Any]) -> dict[str, str]:
    """Keep only the recorded summary counters of one snapshot."""
    return {key: value for key, value in summary.items() if key in SUMMARY_KEYS}


def load_cells() -> dict[str, Any]:
    """Return the recorded cells of the committed fixture."""
    return json.loads(ORACLE_FILE.read_text(encoding="utf-8"))["cells"]


def _spark(warehouse: Path) -> Any:
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[1]")
        .appName("record-ice-meta-delete-1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", str(warehouse))
        .getOrCreate()
    )


def _spark_cell(session: Any, shape: str, version: int, mode: str) -> dict[str, Any]:
    """Run one cell on live Spark and return its recorded shape."""
    table = table_name(shape, version, mode)
    _, _, seeds, dml = SHAPES[shape]
    cell: dict[str, Any] = {}
    try:
        session.sql(create_sql(shape, version, mode))
        for statement in (*seeds, *dml):
            session.sql(statement.format(t=table))
        cell["rows"] = [
            list(row) for row in session.sql(f"SELECT * FROM {table} ORDER BY id").collect()
        ]
        cell["snapshots"] = [
            {"op": row.operation, **keep_summary(dict(row.summary))}
            for row in session.sql(
                f"SELECT operation, summary FROM {table}.snapshots ORDER BY committed_at"
            ).collect()
        ]
        cell["files"] = [
            list(row)
            for row in session.sql(
                f"SELECT content, record_count FROM {table}.files ORDER BY content, record_count"
            ).collect()
        ]
    except Exception as error:  # noqa: BLE001 - a refusal is part of the recorded cell
        cell["error"] = f"{type(error).__name__}: {str(error)[:300]}"
    return cell


def record(warehouse: Path) -> dict[str, Any]:
    """Re-derive every cell from live Spark."""
    shutil.rmtree(warehouse, ignore_errors=True)
    session = _spark(warehouse)
    session.sparkContext.setLogLevel("ERROR")
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    cells = {
        cell_id(shape, version, mode): _spark_cell(session, shape, version, mode)
        for shape in SHAPES
        for version in VERSIONS
        for mode in MODES
    }
    session.stop()
    return cells


def main(argv: list[str]) -> int:
    """Rewrite the fixture, or check it against a fresh recording."""
    check = "--check" in argv
    rest = [argument for argument in argv if argument != "--check"]
    warehouse = Path(rest[0]) if rest else Path(tempfile.mkdtemp()) / "wh-meta-delete"
    fresh = record(warehouse)
    if check:
        recorded = load_cells()
        drift = [key for key in sorted(fresh) if fresh[key] != recorded.get(key)]
        for key in drift:
            print(f"DRIFT {key}: {json.dumps(fresh[key])}")
        print(f"meta-delete cells checked={len(fresh)} drift={len(drift)}")
        return 1 if drift else 0
    document = json.loads(ORACLE_FILE.read_text(encoding="utf-8"))
    document["cells"] = fresh
    ORACLE_FILE.write_text(json.dumps(document, indent=1) + "\n", encoding="utf-8")
    print(f"meta-delete cells recorded={len(fresh)} -> {ORACLE_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
