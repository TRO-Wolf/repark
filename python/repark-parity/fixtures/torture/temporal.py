"""The temporal torture family: duration bounds, interval edges, epoch extremes."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Literal

import pyarrow as pa
import pyarrow.parquet as pq

from repark_parity.torture.family import (
    CSV_NAME,
    PARQUET_NAME,
    FamilyOutput,
    refuse_bad_rows,
    refuse_bad_seed,
    refuse_repository_output,
)

I64_MAX_US = 2**63 - 1
I64_MIN_US = -(2**63)
WHOLE_DAY_EDGE_DAYS = 106_751_991
US_PER_DAY = 86_400_000_000
ROW_CYCLE = 5

TS_US_VALUES = (0, -1, 1, I64_MAX_US, I64_MIN_US)
DAY_VALUES = (0, -1, 1, WHOLE_DAY_EDGE_DAYS - 1, WHOLE_DAY_EDGE_DAYS)
DURATION_US_VALUES = (0, 1, -1, I64_MAX_US, I64_MIN_US)
MONTH_DAY_NANO_MONTHS = (13, 0, 1, 0, 0)
MONTH_DAY_NANO_DAYS = (2, 1, 0, 0, 1)
MONTH_DAY_NANO_NANOS = (500, 0, 1, 0, 999)
TS_TEXT_VALUES = (
    "1970-01-01T00:00:00.000000",
    "1969-12-31T23:59:59.999999",
    "1970-01-01T00:00:00.000001",
    "1970-01-02T03:04:05.000006",
    "1969-12-30T12:34:56.000007",
)
DAY_TEXT_VALUES = ("1970-01-01", "1969-12-31", "1970-01-02", "1970-01-03", "1969-12-30")
DUR_TEXT_VALUES = (
    "0 00:00:00.000000",
    "1 00:00:00.000000",
    "-1 00:00:00.000000",
    "106751991 04:00:54.775807",
    "-106751991 04:00:54.775808",
)

PROMOTED_TYPE = pa.timestamp("us", "UTC")

DECLARED_PARQUET_SCHEMA = pa.schema(
    [
        pa.field("ts_us", pa.timestamp("us")),
        pa.field("day", pa.date32()),
        pa.field("duration_us", pa.duration("us")),
        pa.field("interval_months", pa.int32()),
        pa.field("interval_days", pa.int32()),
        pa.field("interval_nanos", pa.int64()),
    ]
)

DECLARED_CSV_SCHEMA = pa.schema(
    [
        pa.field("ts_text", PROMOTED_TYPE),
        pa.field("day_text", pa.date32()),
        pa.field("dur_text", pa.string()),
        pa.field("months", pa.int32()),
        pa.field("days", pa.int32()),
        pa.field("nanos", pa.int32()),
    ]
)


class TemporalFamily:
    """The temporal family implementor."""

    name = "temporal"

    def generate(self, rows: int, seed: int, out: Path) -> FamilyOutput:
        """Write the family's Parquet and CSV files under out."""
        refuse_bad_rows(rows)
        refuse_bad_seed(seed)
        refuse_repository_output(out)
        out.mkdir(parents=True, exist_ok=True)
        positions = [(index + seed) % ROW_CYCLE for index in range(rows)]
        table = pa.table(
            {
                "ts_us": pa.array(
                    [TS_US_VALUES[pos] for pos in positions], type=pa.timestamp("us")
                ),
                "day": pa.array([DAY_VALUES[pos] for pos in positions], type=pa.date32()),
                "duration_us": pa.array(
                    [DURATION_US_VALUES[pos] for pos in positions], type=pa.duration("us")
                ),
                "interval_months": pa.array(
                    [MONTH_DAY_NANO_MONTHS[pos] for pos in positions], type=pa.int32()
                ),
                "interval_days": pa.array(
                    [MONTH_DAY_NANO_DAYS[pos] for pos in positions], type=pa.int32()
                ),
                "interval_nanos": pa.array(
                    [MONTH_DAY_NANO_NANOS[pos] for pos in positions], type=pa.int64()
                ),
            }
        )
        pq.write_table(table, out / PARQUET_NAME)
        with (out / CSV_NAME).open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(("ts_text", "day_text", "dur_text", "months", "days", "nanos"))
            for pos in positions:
                writer.writerow(
                    [
                        TS_TEXT_VALUES[pos],
                        DAY_TEXT_VALUES[pos],
                        DUR_TEXT_VALUES[pos],
                        MONTH_DAY_NANO_MONTHS[pos],
                        MONTH_DAY_NANO_DAYS[pos],
                        MONTH_DAY_NANO_NANOS[pos],
                    ]
                )
        return FamilyOutput(rows=rows, parquet_path=out / PARQUET_NAME, csv_path=out / CSV_NAME)

    def expected_read_schema(self, fmt: Literal["parquet", "csv"]) -> pa.Schema | None:
        """Return the declared expectation for one format."""
        if fmt == "parquet":
            return DECLARED_PARQUET_SCHEMA
        if fmt == "csv":
            return DECLARED_CSV_SCHEMA
        return None


TEMPORAL_FAMILY = TemporalFamily()
