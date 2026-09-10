"""PROFILES-1 query shapes: five reads and three writes over the bed."""

from __future__ import annotations

from collections.abc import Callable
from typing import Final

from pydantic import BaseModel

APPEND_FILE_COUNT: Final[int] = 8

SMOKE_APPEND_FILES: Final[int] = 2

MERGE_UPDATE_FRACTION: Final[float] = 0.10


class WriteScale(BaseModel):
    """Write-shape scale for one harness mode."""

    append_files: int
    merge_fraction: float


FULL_WRITE_SCALE: Final[WriteScale] = WriteScale(
    append_files=APPEND_FILE_COUNT, merge_fraction=MERGE_UPDATE_FRACTION
)

SMOKE_WRITE_SCALE: Final[WriteScale] = WriteScale(
    append_files=SMOKE_APPEND_FILES, merge_fraction=MERGE_UPDATE_FRACTION
)


def collect_sql(spark: object, query: str) -> None:
    """Run one SQL query and force it through the Arrow path."""
    spark.sql(query).to_arrow()  # type: ignore[attr-defined, union-attr]


def run_scan_filter(spark: object, dataset: str) -> None:
    """Scan one table with a selective filter on both read datasets."""
    if dataset == "futures":
        collect_sql(
            spark,
            "SELECT ticker, event_timestamp, close FROM futures "
            "WHERE total_volume > 1000000 AND close > 100.0",
        )
    else:
        collect_sql(
            spark,
            "SELECT l_orderkey, l_extendedprice FROM lineitem "
            "WHERE l_extendedprice > 50000.0 AND l_quantity < 10.0",
        )


def run_group_by(spark: object, dataset: str) -> None:
    """Group and aggregate one table on both read datasets."""
    if dataset == "futures":
        collect_sql(
            spark,
            "SELECT ticker, avg(close) AS avg_close, sum(total_volume) AS vol "
            "FROM futures GROUP BY ticker",
        )
    else:
        collect_sql(
            spark,
            "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS qty, "
            "avg(l_extendedprice) AS price FROM lineitem "
            "GROUP BY l_returnflag, l_linestatus",
        )


def run_hash_join(spark: object, dataset: str) -> None:
    """Equi-join lineitem to orders on the TPC-H leg."""
    del dataset
    collect_sql(
        spark,
        "SELECT o_orderkey, sum(l_extendedprice) AS revenue FROM lineitem "
        "JOIN orders ON l_orderkey = o_orderkey GROUP BY o_orderkey",
    )


def run_sort_merge_join(spark: object, dataset: str) -> None:
    """Join lineitem to orders with an order on the join key."""
    del dataset
    collect_sql(
        spark,
        "SELECT o_orderkey, o_orderdate, l_extendedprice FROM lineitem "
        "JOIN orders ON l_orderkey = o_orderkey ORDER BY o_orderkey",
    )


def run_window(spark: object, dataset: str) -> None:
    """Wide sliding mean over the futures leg."""
    del dataset
    collect_sql(
        spark,
        "SELECT ticker, event_timestamp, close, "
        "avg(close) OVER (PARTITION BY ticker ORDER BY event_timestamp "
        "ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS mean_20 "
        "FROM futures",
    )


ReadQuery: Final = tuple[Callable[[object, str], None], tuple[str, ...]]

WriteQuery: Final = tuple[Callable[[object, str, WriteScale], None], tuple[str, ...]]

READ_QUERIES: Final[dict[str, ReadQuery]] = {
    "scan_filter": (run_scan_filter, ("futures", "tpch")),
    "group_by": (run_group_by, ("futures", "tpch")),
    "hash_join": (run_hash_join, ("tpch",)),
    "sort_merge_join": (run_sort_merge_join, ("tpch",)),
    "window": (run_window, ("futures",)),
}


def run_append_files(spark: object, table: str, scale: WriteScale) -> None:
    """Append evenly sized batches, one file per batch."""
    for index in range(scale.append_files):
        base = 10_000_000 + index * 1_000_000
        collect_sql(
            spark,
            f"INSERT INTO {table} SELECT value + {base} AS id, "
            f"'{index % 8}' AS g, "
            f"CAST(value + {base} AS DOUBLE) AS v FROM range(1000)",
        )


def run_overwrite_partition(spark: object, table: str, scale: WriteScale) -> None:
    """Overwrite exactly one partition of the bed table."""
    del scale
    collect_sql(
        spark,
        f"INSERT OVERWRITE {table} PARTITION (g = '7') "
        f"SELECT value AS id, CAST(value AS DOUBLE) AS v FROM range(1000)",
    )


def run_merge_updates(spark: object, table: str, scale: WriteScale) -> None:
    """Merge updates touching a fixed fraction of the bed rows."""
    count = spark.sql(f"SELECT count(*) AS c FROM {table}").collect()[0][0]  # type: ignore[attr-defined, union-attr]
    source_rows = max(int(count * scale.merge_fraction), 1)
    collect_sql(
        spark,
        f"MERGE INTO {table} AS t USING "
        f"(SELECT value AS id, CAST(value AS DOUBLE) + 0.5 AS v FROM range({source_rows})) AS s "
        f"ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = s.v",
    )


WRITE_QUERIES: Final[dict[str, WriteQuery]] = {
    "append_files": (run_append_files, ("iceberg",)),
    "overwrite_partition": (run_overwrite_partition, ("iceberg",)),
    "merge_updates": (run_merge_updates, ("iceberg",)),
}
