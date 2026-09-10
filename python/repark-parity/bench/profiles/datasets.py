"""PROFILES-1 dataset builders for the three D-2 datasets."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tpch.datagen import ensure_parquet_sf

DATASETS: tuple[str, ...] = ("futures", "tpch", "iceberg")

FUTURES_NAME: str = "test_futures.parquet"

FUTURES_PATH: Path = Path.home() / "CodeRepos" / "myTemp" / "reTest" / FUTURES_NAME

FULL_TPCH_SF: float = 10.0

SMOKE_TPCH_SF: float = 0.01

FULL_ICEBERG_FILES: int = 200

SMOKE_ICEBERG_FILES: int = 4

SMOKE_FUTURES_ROWS: int = 2000

FULL_ROWS_PER_FILE: int = 5000

SMOKE_ROWS_PER_FILE: int = 200

ICEBERG_CATALOG: str = "cat"

ICEBERG_NAMESPACE: str = "ns"

ICEBERG_TABLE: str = "bed"

TPCH_VIEWS: tuple[str, ...] = ("lineitem", "orders", "customer", "part")


def futures_file(scratch: Path, *, smoke: bool) -> Path:
    """Return the futures parquet path, generating a tiny stand-in for smoke."""
    if not smoke:
        if not FUTURES_PATH.is_file():
            msg = f"owner futures parquet missing: {FUTURES_PATH}"
            raise FileNotFoundError(msg)
        return FUTURES_PATH
    out = scratch / "futures_tiny.parquet"
    if out.is_file() and out.stat().st_size > 0:
        return out
    _write_tiny_futures(out, SMOKE_FUTURES_ROWS)
    return out


def tpch_dir(scratch: Path, scale_factor: float) -> Path:
    """Return the TPC-H parquet directory through the shared dbgen path."""
    return ensure_parquet_sf(scale_factor, data_root=scratch / "tpch")


def iceberg_table() -> str:
    """Return the fully qualified bed table name."""
    return f"{ICEBERG_CATALOG}.{ICEBERG_NAMESPACE}.{ICEBERG_TABLE}"


def read_parquet(spark: object, path: Path) -> object:
    """Read one parquet path through the Spark facade."""
    return spark.read.parquet(str(path))  # type: ignore[attr-defined, union-attr]


def register_views(spark: object, futures_path: Path, tpch_path: Path) -> None:
    """Register the futures view and the TPC-H views the read queries use."""
    read_parquet(spark, futures_path).createOrReplaceTempView("futures")  # type: ignore[attr-defined]
    for view in TPCH_VIEWS:
        read_parquet(spark, tpch_path / f"{view}.parquet").createOrReplaceTempView(view)  # type: ignore[attr-defined]


def ensure_catalog(spark: object, warehouse: Path) -> None:
    """Register the memory catalog and namespace once per session."""
    spark.register_memory_catalog(ICEBERG_CATALOG, str(warehouse))  # type: ignore[attr-defined]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_NAMESPACE}")  # type: ignore[attr-defined]


def build_iceberg_table(spark: object, files: int, rows_per_file: int) -> str:
    """Build a fresh partitioned Iceberg v2 table with one append per file."""
    table = iceberg_table()
    spark.sql(f"DROP TABLE IF EXISTS {table}")  # type: ignore[attr-defined]
    spark.sql(  # type: ignore[attr-defined]
        f"CREATE TABLE {table} USING iceberg PARTITIONED BY (g) AS "
        f"SELECT value AS id, '0' AS g, "
        f"CAST(value AS DOUBLE) AS v FROM range({rows_per_file})"
    )
    for index in range(1, files):
        base = index * rows_per_file
        spark.sql(  # type: ignore[attr-defined]
            f"INSERT INTO {table} SELECT value + {base} AS id, "
            f"'{index % 8}' AS g, "
            f"CAST(value + {base} AS DOUBLE) AS v FROM range({rows_per_file})"
        )
    return table


def count_iceberg_files(warehouse: Path) -> int:
    """Count parquet data files under the warehouse."""
    return sum(1 for path in warehouse.rglob("*.parquet") if path.is_file())


def _write_tiny_futures(path: Path, rows: int) -> None:
    """Write a tiny parquet file mirroring the owner futures schema."""
    import datetime

    import pyarrow as pa
    import pyarrow.parquet as pq

    tickers = [f"T{i % 8}" for i in range(rows)]
    stamps = [datetime.datetime(2024, 1, 1) + datetime.timedelta(minutes=i) for i in range(rows)]
    days = [datetime.date(2024, 1, 1) + datetime.timedelta(days=i % 30) for i in range(rows)]
    table = pa.table(
        {
            "contract_symbol": pa.array(tickers, type=pa.large_string()),
            "ticker_epoch": pa.array(tickers, type=pa.large_string()),
            "event_date_utc": pa.array(days, type=pa.date32()),
            "event_timestamp_utc": pa.array(stamps, type=pa.timestamp("us")),
            "event_date": pa.array(days, type=pa.date32()),
            "event_timestamp": pa.array(stamps, type=pa.timestamp("us")),
            "ticker": pa.array(tickers, type=pa.large_string()),
            "open": pa.array([float(i) for i in range(rows)], type=pa.float64()),
            "high": pa.array([float(i) + 1.0 for i in range(rows)], type=pa.float64()),
            "low": pa.array([float(i) - 1.0 for i in range(rows)], type=pa.float64()),
            "close": pa.array([float(i) + 0.5 for i in range(rows)], type=pa.float64()),
            "total_volume": pa.array([1000 + i for i in range(rows)], type=pa.int64()),
            "total_ticks": pa.array([10 + i for i in range(rows)], type=pa.int64()),
            "up_ticks": pa.array([5 + i for i in range(rows)], type=pa.int64()),
            "down_ticks": pa.array([5 + i for i in range(rows)], type=pa.int64()),
            "up_volume": pa.array([500 + i for i in range(rows)], type=pa.int64()),
            "down_volume": pa.array([500 + i for i in range(rows)], type=pa.int64()),
            "open_interest": pa.array([100 + i for i in range(rows)], type=pa.int64()),
            "roll_in_date": pa.array(days, type=pa.date32()),
            "roll_out_date": pa.array(days, type=pa.date32()),
            "interval_minutes": pa.array([1] * rows, type=pa.int32()),
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
