"""Record the ICE-SORTED-INSERT-2 Spark oracle — rewrite, binpack and NaN sort cells.

NOT a ``test_`` module: pytest never collects it. It builds each cell's table on live
PySpark from the plan below, reads every live data file's ``sort_order_id`` from
``{t}.files`` and its sort key straight out of the parquet bytes with pyarrow, and records
the result into ``ice_sorted_insert_2_spark_oracle.json`` beside this file.

Cells: the five partitioned COW shapes (INSERT OVERWRITE, MERGE matched UPDATE, MERGE
NOT MATCHED INSERT, UPDATE, DELETE) at format version 3 and again at 2; an unpartitioned
INSERT OVERWRITE; a re-ALTERed order (the stamp must be the current order id); a float
column carrying NULLs and NaNs; and a binpack ``rewrite_data_files`` before/after pair.

Subcommands::

    _record_ice_sorted_insert_2_oracle.py record <warehouse>   — rewrite the truth JSON
    _record_ice_sorted_insert_2_oracle.py check  <warehouse>   — re-derive, exit 1 on drift

``<warehouse>`` is created fresh (removed first). The Ivy cache comes from
``REPARK_ORACLE_IVY`` and defaults to the user's ``~/.ivy2``. The Iceberg runtime GAV comes
from ``_oracle_pins`` (CP-8), never a restated constant.

Spark basis: ``local[4]``, ``spark.driver.memory=2g``, UI off, Hadoop catalog,
``spark.sql.session.timeZone=UTC``.
"""

from __future__ import annotations

import itertools
import json
import math
import os
import shutil
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV

_HERE = Path(__file__).resolve().parent
_TRUTH_PATH = _HERE / "ice_sorted_insert_2_spark_oracle.json"
_CATALOG = "sc"
_NAMESPACE = "w"
_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
_ORACLE_NOTE = (
    f"PySpark 4.1.2 + {ICEBERG_SPARK_RUNTIME_GAV}, local[4], UTC, Hadoop catalog; "
    "per-file sortedness read from the parquet bytes with pyarrow"
)

_SEED = "SELECT (id * 7919) % 2000 AS id, CAST(id % 2 AS INT) AS p FROM range(2000)"
_NEW_KEYS = "SELECT ((id * 3571) % 900) + 5000 AS id, CAST(id % 2 AS INT) AS p FROM range(400)"
_MATCHED_UPDATE = (
    "MERGE INTO {t} t USING (SELECT CAST(1 AS BIGINT) AS id, CAST(1 AS INT) AS p) s "
    "ON t.id = s.id WHEN MATCHED THEN UPDATE SET id = t.id"
)
_NOT_MATCHED_INSERT = (
    "MERGE INTO {t} t USING (" + _NEW_KEYS + ") s ON t.id = s.id WHEN NOT MATCHED THEN INSERT *"
)
_FLOAT_SEED = (
    "SELECT id, CASE WHEN id % 13 = 0 THEN CAST('NaN' AS FLOAT)"
    " WHEN id % 17 = 0 THEN CAST(NULL AS FLOAT)"
    " ELSE CAST((id * 7919) % 2000 AS FLOAT) END AS f FROM range(2000)"
)
_BINPACK_CHUNK = (
    "INSERT INTO {t} SELECT ((id * 7919) % 2000) + {chunk} AS id,"
    " CAST(id % 2 AS INT) AS p FROM range(200)"
)
_REWRITE = (
    "CALL {catalog}.system.rewrite_data_files(table => '{ns}.{name}',"
    " options => map('min-input-files','2','rewrite-all','true'))"
)


def _partitioned_program(format_version: str) -> dict[str, Any]:
    """The five partitioned COW cells at one format version, cumulative on one table."""
    label = f"v{format_version}"
    return {
        "name": f"m{format_version}",
        "ddl": "(id BIGINT, p INT) USING iceberg PARTITIONED BY (p)"
        f" TBLPROPERTIES ('format-version'='{format_version}')",
        "order": "WRITE ORDERED BY (id)",
        "key": "id",
        "descending": False,
        "cells": [
            {
                "cell": f"{label}_partitioned_overwrite",
                "sql": ["INSERT OVERWRITE {t} " + _SEED],
            },
            {
                "cell": f"{label}_partitioned_merge_matched_update",
                "sql": [_MATCHED_UPDATE],
            },
            {
                "cell": f"{label}_partitioned_merge_not_matched_insert",
                "sql": [_NOT_MATCHED_INSERT],
            },
            {
                "cell": f"{label}_partitioned_update",
                "sql": ["UPDATE {t} SET id = id WHERE p = 0"],
            },
            {
                "cell": f"{label}_partitioned_delete",
                "sql": ["DELETE FROM {t} WHERE id = 7"],
            },
        ],
    }


def _plan() -> list[dict[str, Any]]:
    """Every program: one table, its declared order, and the cells recorded along it."""
    programs = [_partitioned_program("3"), _partitioned_program("2")]
    programs.append(
        {
            "name": "up",
            "ddl": "(id BIGINT) USING iceberg",
            "order": "WRITE ORDERED BY (id)",
            "key": "id",
            "descending": False,
            "cells": [
                {
                    "cell": "unpartitioned_overwrite",
                    "sql": [
                        "INSERT OVERWRITE {t} SELECT (id * 7919) % 2000 AS id FROM range(2000)"
                    ],
                }
            ],
        }
    )
    programs.append(
        {
            "name": "alt",
            "ddl": "(id BIGINT, p INT) USING iceberg PARTITIONED BY (p)",
            "order": "WRITE ORDERED BY (id)",
            "key": "id",
            "descending": True,
            "cells": [
                {
                    "cell": "alter_order_then_overwrite",
                    "sql": [
                        "INSERT OVERWRITE {t} SELECT (id * 7919) % 2000 AS id,"
                        " CAST(id % 2 AS INT) AS p FROM range(400)",
                        "ALTER TABLE {t} WRITE ORDERED BY (id DESC)",
                        "INSERT OVERWRITE {t} SELECT (id * 7919) % 2000 AS id,"
                        " CAST(id % 2 AS INT) AS p FROM range(400)",
                    ],
                }
            ],
        }
    )
    programs.append(
        {
            "name": "fl",
            "ddl": "(id BIGINT, f FLOAT) USING iceberg",
            "order": "WRITE ORDERED BY (f)",
            "key": "f",
            "descending": False,
            "cells": [
                {"cell": "float_nan_overwrite", "sql": ["INSERT OVERWRITE {t} " + _FLOAT_SEED]}
            ],
        }
    )
    programs.append(
        {
            "name": "bp",
            "ddl": "(id BIGINT, p INT) USING iceberg PARTITIONED BY (p)",
            "order": "WRITE ORDERED BY (id)",
            "key": "id",
            "descending": False,
            "cells": [
                {
                    "cell": "binpack_before",
                    "sql": [_BINPACK_CHUNK.format(t="{t}", chunk=chunk) for chunk in range(3)],
                },
                {"cell": "binpack_after", "sql": [_REWRITE]},
            ],
        }
    )
    return programs


def _sorted_under(values: list[Any], descending: bool) -> bool:
    """Sortedness under Spark's order: NULLS FIRST ascending, NaN above every value."""

    def rank(value: Any) -> tuple[int, float]:
        if value is None:
            return (0, 0.0)
        if isinstance(value, float) and math.isnan(value):
            return (2, 0.0)
        return (1, -float(value) if descending else float(value))

    keys = [rank(value) for value in values]
    return keys == sorted(keys)


def _float_layout(values: list[Any]) -> dict[str, Any]:
    """NULL/NaN placement of one float file: leading NULLs, then values, then a NaN block."""
    nulls = [index for index, value in enumerate(values) if value is None]
    nans = [
        index
        for index, value in enumerate(values)
        if isinstance(value, float) and math.isnan(value)
    ]
    finite = [value for value in values if value is not None and not math.isnan(value)]
    return {
        "null_count": len(nulls),
        "nan_count": len(nans),
        "nulls_first_block": nulls == list(range(len(nulls))),
        "nan_tail_block": nans == list(range(len(values) - len(nans), len(values))),
        "middle_strictly_ascending": all(a < b for a, b in itertools.pairwise(finite)),
    }


def _files(spark: Any, table: str, key: str, descending: bool) -> list[dict[str, Any]]:
    """Per-file stamp, row count, partition, key head and sortedness, read from the bytes."""
    import pyarrow.parquet as pq

    columns = {field.name for field in spark.table(f"{table}.files").schema.fields}
    column = "partition" if "partition" in columns else "spec_id"
    rows = spark.sql(
        f"SELECT file_path, sort_order_id, record_count, {column} AS partition FROM {table}.files"
    ).collect()
    out: list[dict[str, Any]] = []
    for row in rows:
        values = pq.read_table(row["file_path"].replace("file:", ""), columns=[key])
        keys = values.column(key).to_pylist()
        entry: dict[str, Any] = {
            "sort_order_id": row["sort_order_id"],
            "record_count": row["record_count"],
            "partition": str(row["partition"]),
            "key_head": [_plain(value) for value in keys[:6]],
            "key_sorted": _sorted_under(keys, descending),
        }
        if key == "f":
            entry["layout"] = _float_layout(keys)
        out.append(entry)
    return sorted(out, key=lambda entry: (entry["partition"], entry["record_count"], _head(entry)))


def _plain(value: Any) -> Any:
    """One key value as JSON-safe text: NaN rendered as its name, everything else verbatim."""
    if isinstance(value, float) and math.isnan(value):
        return "NaN"
    return value


def _head(entry: dict[str, Any]) -> str:
    """A stable text form of one file's key head, for deterministic ordering."""
    return json.dumps(entry["key_head"], default=str)


def _ops(spark: Any, table: str) -> list[str]:
    """The table's snapshot operations, oldest first."""
    return [
        row["operation"]
        for row in spark.sql(
            f"SELECT operation FROM {table}.snapshots ORDER BY committed_at"
        ).collect()
    ]


def _run_program(spark: Any, program: dict[str, Any], cells: dict[str, Any]) -> None:
    """Build one program's table and record every cell along it."""
    table = f"{_CATALOG}.{_NAMESPACE}.{program['name']}"
    spark.sql(f"CREATE TABLE {table} {program['ddl']}")
    spark.sql(f"ALTER TABLE {table} {program['order']}")
    for cell in program["cells"]:
        for statement in cell["sql"]:
            spark.sql(
                statement.format(t=table, catalog=_CATALOG, ns=_NAMESPACE, name=program["name"])
            )
        cells[cell["cell"]] = {
            "files": _files(spark, table, program["key"], program["descending"]),
            "ops": _ops(spark, table),
        }


def _spark_session(warehouse: Path) -> Any:
    """The recorded basis, built once."""
    from pyspark.sql import SparkSession

    ivy = os.environ.get("REPARK_ORACLE_IVY") or str(Path.home() / ".ivy2")
    return (
        SparkSession.builder.master("local[4]")
        .appName("repark-ice-sorted-insert-2-record")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config("spark.jars.ivy", ivy)
        .config("spark.sql.extensions", _EXTENSIONS)
        .config(f"spark.sql.catalog.{_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{_CATALOG}.warehouse", str(warehouse))
        .getOrCreate()
    )


def _record(warehouse: Path) -> dict[str, Any]:
    """Derive every cell from live Spark."""
    shutil.rmtree(warehouse, ignore_errors=True)
    warehouse.mkdir(parents=True)
    spark = _spark_session(warehouse)
    cells: dict[str, Any] = {}
    plan = _plan()
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {_CATALOG}.{_NAMESPACE}")
        for program in plan:
            _run_program(spark, program, cells)
    finally:
        spark.stop()
    return {"oracle": _ORACLE_NOTE, "plan": plan, "cells": cells}


def main(argv: list[str]) -> int:
    """``record <warehouse>`` rewrites the truth JSON; ``check <warehouse>`` reds on drift."""
    if len(argv) != 2 or argv[0] not in ("record", "check"):
        print(__doc__)
        return 2
    payload = _record(Path(argv[1]).expanduser())
    if argv[0] == "record":
        _TRUTH_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        print(f"wrote {_TRUTH_PATH}")
        return 0
    recorded = json.loads(_TRUTH_PATH.read_text(encoding="utf-8"))
    if recorded["cells"] == payload["cells"]:
        print("oracle matches the checked-in truth")
        return 0
    print("ORACLE DRIFT versus the checked-in truth:")
    for name, live in payload["cells"].items():
        if recorded["cells"].get(name) != live:
            print(f"  cell {name}: truth={json.dumps(recorded['cells'].get(name))[:400]}")
            print(f"  cell {name}: live ={json.dumps(live)[:400]}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
