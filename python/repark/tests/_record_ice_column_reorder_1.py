"""Record the ICE-COLUMN-REORDER-1 Spark oracle into test_ice_column_reorder_1_truth.json.

Run once per Spark bump under the JVM lock; never imported by tests::

    REPARK_PARITY_LIVE=1 /tmp/oc-worker/jb-jvm.sh /tmp/sparkenv/bin/python \\
        python/repark/tests/_record_ice_column_reorder_1.py

pins: ice-column-reorder-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012
"""

from __future__ import annotations

import json
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any

import _oracle_pins as pins

_HERE = Path(__file__).resolve().parent
_TRUTH = _HERE / "test_ice_column_reorder_1_truth.json"
_CATALOG = "rec"
_NAMESPACE = "ns"

_PLAIN_DDL = "CREATE TABLE {t} (id INT, a STRING, b STRING) USING iceberg"
_PLAIN_SEED = "INSERT INTO {t} VALUES (1, 'a1', 'b1')"
_NESTED_DDL = "CREATE TABLE {t} (id INT, s STRUCT<a: INT, b: STRING>) USING iceberg"
_NESTED_SEED = "INSERT INTO {t} VALUES (1, named_struct('a', 1, 'b', 'x'))"


def _warehouse() -> Path:
    """Fresh temp warehouse for one recording run."""
    return Path(tempfile.mkdtemp(prefix="reorder-record-"))


def _table_dir(warehouse: Path, table: str) -> Path:
    """Hadoop-catalog table root for a recorded table."""
    return warehouse / _NAMESPACE / table


def _newest_metadata(table_dir: Path) -> dict[str, Any]:
    """Newest vN metadata document for a Hadoop-catalog table."""
    versions = sorted(
        (table_dir / "metadata").glob("v*.metadata.json"),
        key=lambda path: int(path.name[1:].split(".", 1)[0]),
    )
    return json.loads(versions[-1].read_text(encoding="utf-8"))


def _metadata_version(table_dir: Path) -> int:
    """The N of the newest vN.metadata.json."""
    versions = sorted(
        (table_dir / "metadata").glob("v*.metadata.json"),
        key=lambda path: int(path.name[1:].split(".", 1)[0]),
    )
    return int(versions[-1].name[1:].split(".", 1)[0])


def _json_value(value: Any) -> Any:
    """Plain-JSON projection of a Spark Row tree."""
    if hasattr(value, "asDict"):
        return {key: _json_value(item) for key, item in value.asDict().items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _snapshot(session: Any, warehouse: Path, table: str) -> dict[str, Any]:
    """Schema, DESCRIBE, column order and ordered rows for one table."""
    qualified = f"{_CATALOG}.{_NAMESPACE}.{table}"
    doc = _newest_metadata(_table_dir(warehouse, table))
    current = doc["current-schema-id"]
    schema = next(item for item in doc["schemas"] if item["schema-id"] == current)
    describe = [
        [row["col_name"], row["data_type"]]
        for row in session.sql(f"DESCRIBE {qualified}").collect()
    ]
    columns = session.sql(f"SELECT * FROM {qualified}").columns
    order = "id" if "id" in columns else columns[0]
    rows = [
        _json_value(tuple(row)) for row in session.sql(f"SELECT * FROM {qualified} ORDER BY {order}").collect()
    ]
    return {
        "schema_id": current,
        "metadata_version": _metadata_version(_table_dir(warehouse, table)),
        "fields": schema["fields"],
        "describe": describe,
        "select_columns": columns,
        "rows": rows,
    }


def _error_record(exc: Exception) -> dict[str, Any]:
    """Python class, Java class and first message line for a Spark refusal."""
    text = str(exc).strip()
    match = re.search(r": ((?:[a-zA-Z0-9_]+\.)+[A-Za-z0-9_]+(?:Exception|Error))\b", text)
    java_class = match.group(1) if match is not None else None
    if java_class is None:
        first = text.splitlines()[0][:500]
    else:
        carrying = next(
            line for line in text.splitlines() if java_class in line
        )
        first = carrying.strip().lstrip(":").strip()[:500]
    return {
        "python_class": type(exc).__name__,
        "java_class": java_class,
        "message_first_line": first,
    }


def _fresh(session: Any, table: str, ddl: str, seed: str) -> None:
    """Recreate one recording table from DDL plus seed."""
    qualified = f"{_CATALOG}.{_NAMESPACE}.{table}"
    session.sql(f"DROP TABLE IF EXISTS {qualified}")
    session.sql(ddl.format(t=qualified))
    session.sql(seed.format(t=qualified))


def _move_case(
    session: Any,
    warehouse: Path,
    table: str,
    ddl: str,
    seed: str,
    statement: str,
) -> dict[str, Any]:
    """Record one ALTER COLUMN move: before/after snapshots or the refusal."""
    _fresh(session, table, ddl, seed)
    qualified = f"{_CATALOG}.{_NAMESPACE}.{table}"
    before = _snapshot(session, warehouse, table)
    try:
        session.sql(statement.format(t=qualified))
    except Exception as exc:
        return {"statement": statement, "before": before, "error": _error_record(exc)}
    return {"statement": statement, "before": before, "after": _snapshot(session, warehouse, table)}


def _build_spark(warehouse: Path) -> Any:
    """Live PySpark + Hadoop Iceberg engine on the pinned GAV (no pyarrow needed)."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("reorder-record")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", pins.ICEBERG_SPARK_RUNTIME_GAV)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{_CATALOG}.warehouse", str(warehouse))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    return session


def record() -> dict[str, Any]:
    """Run the clause matrix on live Spark and return the truth document."""
    warehouse = _warehouse()
    session = _build_spark(warehouse)
    try:
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS {_CATALOG}.{_NAMESPACE}")
        cases: dict[str, Any] = {}
        cases["first_v2"] = _move_case(
            session, warehouse, "first_v2", _PLAIN_DDL, _PLAIN_SEED,
            "ALTER TABLE {t} ALTER COLUMN b FIRST",
        )
        cases["after_v2"] = _move_case(
            session, warehouse, "after_v2", _PLAIN_DDL, _PLAIN_SEED,
            "ALTER TABLE {t} ALTER COLUMN b AFTER id",
        )
        cases["noop_first_v2"] = _move_case(
            session, warehouse, "noop_first_v2", _PLAIN_DDL, _PLAIN_SEED,
            "ALTER TABLE {t} ALTER COLUMN id FIRST",
        )
        cases["noop_after_v2"] = _move_case(
            session, warehouse, "noop_after_v2", _PLAIN_DDL, _PLAIN_SEED,
            "ALTER TABLE {t} ALTER COLUMN a AFTER id",
        )
        cases["first_after_last_v2"] = _move_case(
            session, warehouse, "first_after_last_v2", _PLAIN_DDL, _PLAIN_SEED,
            "ALTER TABLE {t} ALTER COLUMN id AFTER b",
        )
        cases["self_v2"] = _move_case(
            session, warehouse, "self_v2", _PLAIN_DDL, _PLAIN_SEED,
            "ALTER TABLE {t} ALTER COLUMN b AFTER b",
        )
        cases["badref_v2"] = _move_case(
            session, warehouse, "badref_v2", _PLAIN_DDL, _PLAIN_SEED,
            "ALTER TABLE {t} ALTER COLUMN a AFTER nope",
        )
        cases["badcol_v2"] = _move_case(
            session, warehouse, "badcol_v2", _PLAIN_DDL, _PLAIN_SEED,
            "ALTER TABLE {t} ALTER COLUMN nope FIRST",
        )
        cases["nested_v2"] = _move_case(
            session, warehouse, "nested_v2", _NESTED_DDL, _NESTED_SEED,
            "ALTER TABLE {t} ALTER COLUMN s.b FIRST",
        )
        cases["struct_top_v2"] = _move_case(
            session, warehouse, "struct_top_v2", _NESTED_DDL, _NESTED_SEED,
            "ALTER TABLE {t} ALTER COLUMN s FIRST",
        )
        v3_ddl = _PLAIN_DDL + " TBLPROPERTIES ('format-version'='3')"
        cases["first_v3"] = _move_case(
            session, warehouse, "first_v3", v3_ddl, _PLAIN_SEED,
            "ALTER TABLE {t} ALTER COLUMN b FIRST",
        )
        part_ddl = (
            "CREATE TABLE {t} (id INT, a STRING, b STRING) "
            "USING iceberg PARTITIONED BY (b)"
        )
        cases["part_v2"] = _move_case(
            session, warehouse, "part_v2", part_ddl, _PLAIN_SEED,
            "ALTER TABLE {t} ALTER COLUMN b FIRST",
        )
        insert_table = "insert_v2"
        _fresh(session, insert_table, _PLAIN_DDL, _PLAIN_SEED)
        qualified = f"{_CATALOG}.{_NAMESPACE}.{insert_table}"
        session.sql(f"ALTER TABLE {qualified} ALTER COLUMN b FIRST")
        session.sql(f"INSERT INTO {qualified} VALUES ('b2', 2, 'a2')")
        cases["insert_v2"] = {
            "statement": "ALTER TABLE {t} ALTER COLUMN b FIRST + INSERT new-order",
            "after": _snapshot(session, warehouse, insert_table),
        }
        part_insert = "part_insert_v2"
        _fresh(session, part_insert, part_ddl, _PLAIN_SEED)
        part_qualified = f"{_CATALOG}.{_NAMESPACE}.{part_insert}"
        session.sql(f"ALTER TABLE {part_qualified} ALTER COLUMN b FIRST")
        session.sql(f"INSERT INTO {part_qualified} VALUES ('b3', 3, 'a3')")
        cases["part_insert_v2"] = {
            "statement": "partitioned ALTER COLUMN b FIRST + INSERT new-order",
            "after": _snapshot(session, warehouse, part_insert),
        }
        df_table = "df_v2"
        _fresh(session, df_table, _PLAIN_DDL, _PLAIN_SEED)
        df_qualified = f"{_CATALOG}.{_NAMESPACE}.{df_table}"
        session.sql(f"ALTER TABLE {df_qualified} ALTER COLUMN b AFTER id")
        columns = session.table(df_qualified).columns
        session.createDataFrame([(9, "b9", "a9")], ["id", "b", "a"]).writeTo(
            df_qualified
        ).append()
        cases["df_v2"] = {
            "statement": "ALTER COLUMN b AFTER id + df.columns + writeTo.append by name",
            "columns": columns,
            "after": _snapshot(session, warehouse, df_table),
        }
        return {
            "provenance": {
                "spark": "4.1.2",
                "iceberg": pins.ICEBERG_RUNTIME_VERSION,
                "gav": pins.ICEBERG_SPARK_RUNTIME_GAV,
                "note": pins.ICEBERG_SPARK_RUNTIME_NOTE,
            },
            "shapes": {
                "plain_ddl": _PLAIN_DDL,
                "plain_seed": _PLAIN_SEED,
                "nested_ddl": _NESTED_DDL,
                "nested_seed": _NESTED_SEED,
            },
            "cases": cases,
        }
    finally:
        shutil.rmtree(warehouse, ignore_errors=True)


def main() -> None:
    """Record the oracle and write the truth JSON beside this script."""
    truth = record()
    _TRUTH.write_text(json.dumps(truth, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"wrote {_TRUTH} ({len(truth['cases'])} cases)", flush=True)


if __name__ == "__main__":
    main()
