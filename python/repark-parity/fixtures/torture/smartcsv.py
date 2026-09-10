"""The smartcsv torture family: header normalisation, blank cells, widths, bool spellings."""

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

CSV_HEADER_NAMES = (
    "Order Total",
    "Qty",
    "Note",
    "PureBlank",
    "StrictFlag",
    "LooseFlag",
    "Small",
    "Wide",
    "Overflow",
)
STRICT_FLAG_SPELLINGS = ("true", "FALSE", "True", "false")
LOOSE_FLAG_SPELLINGS = ("yes", "no", "Y", "N")
OVERFLOW_BASE = 10**19

DECLARED_PARQUET_SCHEMA = pa.schema(
    [
        pa.field("Order Total", pa.string()),
        pa.field("Qty", pa.int32()),
        pa.field("Note", pa.string()),
        pa.field("PureBlank", pa.string()),
        pa.field("StrictFlag", pa.bool_()),
        pa.field("LooseFlag", pa.string()),
        pa.field("Small", pa.int32()),
        pa.field("Wide", pa.int64()),
        pa.field("Overflow", pa.decimal128(20, 0)),
    ]
)

DECLARED_CSV_SCHEMA = DECLARED_PARQUET_SCHEMA


def _currency_text(index: int, seed: int) -> str:
    """One currency-formatted text value with grouping commas."""
    cents = index * 7001 + seed * 101 + 99
    return f"${cents // 100:,}.{cents % 100:02d}"


def _note_value(index: int, seed: int) -> str:
    """One note value with blank cells mixed against text."""
    return "" if (index + seed) % 3 == 0 else f"note-{index + seed}"


def _small_value(index: int, seed: int) -> int:
    """One int32-fitting value."""
    return ((index + seed) % 200) - 100


def _wide_value(index: int, seed: int) -> int:
    """One int64-fitting, int32-overflowing value."""
    return 9_000_000_000 + index + seed


def _overflow_value(index: int, seed: int) -> Decimal:
    """One 20-digit decimal value past the Int64 width."""
    return Decimal(OVERFLOW_BASE + index + seed)


class SmartCsvFamily:
    """The smartcsv family implementor."""

    name = "smartcsv"

    def generate(self, rows: int, seed: int, out: Path) -> FamilyOutput:
        """Write the family's Parquet and CSV files under out."""
        refuse_bad_rows(rows)
        refuse_bad_seed(seed)
        refuse_repository_output(out)
        out.mkdir(parents=True, exist_ok=True)
        currencies = [_currency_text(index, seed) for index in range(rows)]
        quantities = [(index + seed) % 97 for index in range(rows)]
        notes = [_note_value(index, seed) for index in range(rows)]
        strict_flags = [STRICT_FLAG_SPELLINGS[(index + seed) % 4] for index in range(rows)]
        strict_bools = [(index + seed) % 2 == 0 for index in range(rows)]
        loose_flags = [LOOSE_FLAG_SPELLINGS[(index + seed) % 4] for index in range(rows)]
        smalls = [_small_value(index, seed) for index in range(rows)]
        wides = [_wide_value(index, seed) for index in range(rows)]
        overflows = [_overflow_value(index, seed) for index in range(rows)]
        table = pa.table(
            {
                "Order Total": pa.array(currencies, type=pa.string()),
                "Qty": pa.array(quantities, type=pa.int32()),
                "Note": pa.array(notes, type=pa.string()),
                "PureBlank": pa.array(["" for _ in range(rows)], type=pa.string()),
                "StrictFlag": pa.array(strict_bools, type=pa.bool_()),
                "LooseFlag": pa.array(loose_flags, type=pa.string()),
                "Small": pa.array(smalls, type=pa.int32()),
                "Wide": pa.array(wides, type=pa.int64()),
                "Overflow": pa.array(overflows, type=pa.decimal128(20, 0)),
            }
        )
        pq.write_table(table, out / PARQUET_NAME)
        with (out / CSV_NAME).open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(CSV_HEADER_NAMES)
            for index in range(rows):
                writer.writerow(
                    [
                        currencies[index],
                        quantities[index],
                        notes[index],
                        "",
                        strict_flags[index],
                        loose_flags[index],
                        smalls[index],
                        wides[index],
                        str(overflows[index]),
                    ]
                )
        return FamilyOutput(rows=rows, parquet_path=out / PARQUET_NAME, csv_path=out / CSV_NAME)

    def expected_read_schema(self, fmt: Literal["parquet", "csv"]) -> pa.Schema | None:
        """Return the declared expectation: resolved types on Parquet, Spark-inferred on CSV."""
        if fmt == "parquet" or fmt == "csv":
            return DECLARED_PARQUET_SCHEMA
        return None


SMARTCSV_FAMILY = SmartCsvFamily()
