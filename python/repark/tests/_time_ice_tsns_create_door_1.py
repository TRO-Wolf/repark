"""ICE-TSNS-CREATE-DOOR-1: time four statements that create or evolve a microsecond table."""

from __future__ import annotations

import argparse
import itertools
import json
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from repark import ReparkSession

SOURCE = "ice.ns.source"
V3 = "TBLPROPERTIES ('format-version' = '3')"
NAMES = itertools.count()


def open_session(warehouse: Path, rows: int) -> Any:
    """Open a v3-enabled session holding a microsecond source table of ``rows`` rows."""
    spark = (
        ReparkSession.builder.appName("ice-tsns-create-door-1-cost")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.register_memory_catalog("ice", str(warehouse))
    spark.sql("CREATE NAMESPACE ice.ns")
    frame = spark.range(rows).selectExpr("id", "timestamp_micros(id) AS t")
    frame.writeTo(SOURCE).tableProperty("format-version", "3").create()
    return spark


def run_ctas(spark: Any) -> None:
    """CREATE TABLE … AS SELECT * over the source."""
    spark.sql(f"CREATE TABLE ice.ns.t{next(NAMES)} USING iceberg {V3} AS SELECT * FROM {SOURCE}")


def run_save_overwrite(spark: Any) -> None:
    """``mode("overwrite").saveAsTable`` of the source frame over an existing table."""
    table = f"ice.ns.t{next(NAMES)}"
    spark.sql(f"CREATE TABLE {table} (id BIGINT) USING iceberg {V3}")
    spark.table(SOURCE).write.mode("overwrite").saveAsTable(table)


def run_merge_evolution(spark: Any) -> None:
    """MERGE WITH SCHEMA EVOLUTION adding the microsecond column from the source."""
    table = f"ice.ns.t{next(NAMES)}"
    spark.sql(f"CREATE TABLE {table} (id BIGINT) USING iceberg {V3}")
    spark.sql(
        f"MERGE WITH SCHEMA EVOLUTION INTO {table} t USING {SOURCE} s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )


def run_ctas_wide(spark: Any) -> None:
    """CREATE TABLE … AS over a 200-column query of the source, no nanosecond column in it."""
    columns = ", ".join(f"id + {index} AS c{index}" for index in range(200))
    spark.sql(
        f"CREATE TABLE ice.ns.t{next(NAMES)} USING iceberg {V3} AS "
        f"SELECT {columns}, t FROM {SOURCE} LIMIT 1000"
    )


STATEMENTS: dict[str, Callable[[Any], None]] = {
    "ctas": run_ctas,
    "save_overwrite": run_save_overwrite,
    "merge_schema_evolution": run_merge_evolution,
    "ctas_200_columns": run_ctas_wide,
}


def median_milliseconds(spark: Any, statement: Callable[[Any], None], repeats: int) -> float:
    """Return the median wall time of ``statement`` after one warm run."""
    taken: list[float] = []
    for _ in range(repeats + 1):
        start = time.perf_counter()
        statement(spark)
        taken.append(time.perf_counter() - start)
    return round(sorted(taken[1:])[repeats // 2] * 1_000, 3)


def main() -> None:
    """Print one JSON object of medians in milliseconds."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--repeats", type=int, default=5)
    arguments = parser.parse_args()
    result: dict[str, Any] = {"rows": arguments.rows}
    with tempfile.TemporaryDirectory(prefix="tsns-create-door-cost-") as directory:
        spark = open_session(Path(directory), arguments.rows)
        for name, statement in STATEMENTS.items():
            result[name] = median_milliseconds(spark, statement, arguments.repeats)
        spark.stop()
    print(json.dumps(result))


if __name__ == "__main__":
    main()
