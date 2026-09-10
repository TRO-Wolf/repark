"""The extreme_types torture family: high-precision decimals, UUIDs, paragraphs, HTML."""

from __future__ import annotations

import csv
import hashlib
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

PARQUET_COLUMN_NAMES = ("Precise", "Whole", "Uuid", "Paragraph", "Html")
CSV_COLUMN_NAMES = ("big38", "uuid", "paragraph", "html")
DECIMAL_PRECISION = 38
DECIMAL_SCALE = 18

DECLARED_PARQUET_SCHEMA = pa.schema(
    [
        pa.field("Precise", pa.decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)),
        pa.field("Whole", pa.decimal128(DECIMAL_PRECISION, 0)),
        pa.field("Uuid", pa.string()),
        pa.field("Paragraph", pa.string()),
        pa.field("Html", pa.string()),
    ]
)

DECLARED_CSV_SCHEMA = pa.schema(
    [
        pa.field("big38", pa.decimal128(DECIMAL_PRECISION, 0)),
        pa.field("uuid", pa.string()),
        pa.field("paragraph", pa.string()),
        pa.field("html", pa.string()),
    ]
)

_MAX_WHOLE = 10**DECIMAL_PRECISION - 1
_SENTENCE = "The quick brown fox jumps over the lazy dog while the engine reads every row."
_HTML_FRAGMENTS = (
    '<p class="lead">row {index} &amp; <b>bold</b> text</p>',
    '<a href="/x?a=1&amp;b=2">link {index}</a>',
    '<ul><li>item {index}</li><li>"quoted" &lt;tag&gt;</li></ul>',
)


def _precise_value(index: int, seed: int) -> Decimal:
    """One decimal(38,18) value: a 20-digit integer part and a full 18-digit fraction."""
    whole = (index * 6_679_099 + seed * 7919 + 1) % 10**20
    frac = (index * 104_729 + seed * 15_485_863 + 7) % 10**18
    text = f"{whole}.{frac:018d}" if index % 2 == 0 else f"-{whole}.{frac:018d}"
    return Decimal(text)


def _whole_value(index: int) -> Decimal:
    """One decimal(38,0) value near the 38-digit maximum, alternating sign."""
    magnitude = _MAX_WHOLE - index
    return Decimal(-magnitude) if index % 2 else Decimal(magnitude)


def _uuid_value(index: int, seed: int) -> str:
    """One deterministic UUID-shaped string."""
    digest = hashlib.sha256(f"{seed}:{index}".encode()).hexdigest()[:32]
    return f"{digest[:8]}-{digest[8:12]}-{digest[12:16]}-{digest[16:20]}-{digest[20:32]}"


def _paragraph_value(index: int) -> str:
    """One long deterministic paragraph string."""
    return " ".join([_SENTENCE] * (index % 4 + 2)) + f" Tail {index}."


def _html_value(index: int) -> str:
    """One deterministic embedded-HTML string."""
    return _HTML_FRAGMENTS[index % len(_HTML_FRAGMENTS)].format(index=index)


def _big38_value(index: int) -> str:
    """One 38-digit integer text value, alternating sign."""
    magnitude = _MAX_WHOLE - index
    return str(-magnitude) if index % 2 else str(magnitude)


class ExtremeTypesFamily:
    """The extreme_types family implementor."""

    name = "extreme_types"

    def generate(self, rows: int, seed: int, out: Path) -> FamilyOutput:
        """Write the family's Parquet and CSV files under out."""
        refuse_bad_rows(rows)
        refuse_bad_seed(seed)
        refuse_repository_output(out)
        out.mkdir(parents=True, exist_ok=True)
        uuids = [_uuid_value(index, seed) for index in range(rows)]
        paragraphs = [_paragraph_value(index) for index in range(rows)]
        htmls = [_html_value(index) for index in range(rows)]
        table = pa.table(
            {
                "Precise": pa.array(
                    [_precise_value(index, seed) for index in range(rows)],
                    type=pa.decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                ),
                "Whole": pa.array(
                    [_whole_value(index) for index in range(rows)],
                    type=pa.decimal128(DECIMAL_PRECISION, 0),
                ),
                "Uuid": pa.array(uuids, type=pa.string()),
                "Paragraph": pa.array(paragraphs, type=pa.string()),
                "Html": pa.array(htmls, type=pa.string()),
            }
        )
        pq.write_table(table, out / PARQUET_NAME)
        with (out / CSV_NAME).open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(CSV_COLUMN_NAMES)
            for index in range(rows):
                writer.writerow(
                    [
                        _big38_value(index),
                        uuids[index],
                        paragraphs[index],
                        htmls[index],
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


EXTREME_TYPES_FAMILY = ExtremeTypesFamily()
