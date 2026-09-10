"""The decimal_overflow torture family: decimal(38) aggregates past the result boundary."""

from __future__ import annotations

import csv
from decimal import Decimal
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

COLUMN_NAMES = ("v", "w")
DECIMAL_PRECISION = 38
V_BASE_INT = 95 * 10**36
W_BASE_INT = 99 * 10**36

DECLARED_SCHEMA = pa.schema(
    [
        pa.field("v", pa.decimal128(DECIMAL_PRECISION, 0)),
        pa.field("w", pa.decimal128(DECIMAL_PRECISION, 0)),
    ]
)


def v_value(index: int, seed: int) -> Decimal:
    """One decimal(38,0) value whose column sum overflows the aggregate boundary."""
    return Decimal(V_BASE_INT + seed + index)


def w_value(index: int, seed: int) -> Decimal:
    """One decimal(38,0) value whose column sum wraps the i128 accumulator."""
    return Decimal(W_BASE_INT - seed - index)


class DecimalOverflowFamily:
    """The decimal_overflow family implementor."""

    name = "decimal_overflow"

    def generate(self, rows: int, seed: int, out: Path) -> FamilyOutput:
        """Write the family's Parquet and CSV files under out."""
        refuse_bad_rows(rows)
        refuse_bad_seed(seed)
        refuse_repository_output(out)
        out.mkdir(parents=True, exist_ok=True)
        vs = [v_value(index, seed) for index in range(rows)]
        ws = [w_value(index, seed) for index in range(rows)]
        table = pa.table(
            {
                "v": pa.array(vs, type=pa.decimal128(DECIMAL_PRECISION, 0)),
                "w": pa.array(ws, type=pa.decimal128(DECIMAL_PRECISION, 0)),
            }
        )
        pq.write_table(table, out / PARQUET_NAME)
        with (out / CSV_NAME).open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(COLUMN_NAMES)
            for index in range(rows):
                writer.writerow([str(vs[index]), str(ws[index])])
        return FamilyOutput(rows=rows, parquet_path=out / PARQUET_NAME, csv_path=out / CSV_NAME)

    def expected_read_schema(self, fmt: Literal["parquet", "csv"]) -> pa.Schema | None:
        """Return the declared expectation; both formats carry the same decimal types."""
        if fmt == "parquet" or fmt == "csv":
            return DECLARED_SCHEMA
        return None


DECIMAL_OVERFLOW_FAMILY = DecimalOverflowFamily()
