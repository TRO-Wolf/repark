"""The inference torture family: CSV type-inference conflicts against the Spark oracle."""

from __future__ import annotations

import datetime
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

COLUMN_NAMES = ("growth", "halves", "boolish", "datish")
_GROWTH_HEAD_FACTOR = 7
_TEXT_PREFIX = "text-"
CSV_INFER_ROW_DIVISOR = 2

DECLARED_SCHEMA = pa.schema(
    [
        pa.field("growth", pa.int64()),
        pa.field("halves", pa.string()),
        pa.field("boolish", pa.int32()),
        pa.field("datish", pa.date32()),
    ]
)


def _growth_value(index: int, seed: int, conflict_at: int) -> int:
    """One growth value: int32-ranged before the conflict row, int64-ranged after."""
    if index < conflict_at:
        return (index * _GROWTH_HEAD_FACTOR + seed) % 1_000_000_000
    return 2_147_483_647 + index - conflict_at + seed


def _halves_value(index: int, seed: int) -> str:
    """One halves value: float-looking on even rows, text on odd rows."""
    if index % 2 == 0:
        return f"{((index * 13 + seed) % 10_000) / 8:.3f}"
    return f"{_TEXT_PREFIX}{(index + seed) % 100_000:05d}"


def _resolved_arrays(rows: int, seed: int, conflict_at: int) -> dict[str, pa.Array]:
    """Build every column once at its resolved type; the CSV text renders these values."""
    growth: list[int] = []
    halves: list[str] = []
    boolish: list[int] = []
    datish: list[datetime.date] = []
    for index in range(rows):
        growth.append(_growth_value(index, seed, conflict_at))
        halves.append(_halves_value(index, seed))
        boolish.append((index + seed) % 2)
        datish.append(datetime.date(2026, (index + seed) % 12 + 1, (index + seed) % 28 + 1))
    return {
        "growth": pa.array(growth, type=pa.int64()),
        "halves": pa.array(halves, type=pa.string()),
        "boolish": pa.array(boolish, type=pa.int32()),
        "datish": pa.array(datish, type=pa.date32()),
    }


def _csv_text(arrays: dict[str, pa.Array]) -> str:
    """Render the inference CSV from the resolved values, one plain line per row."""
    columns = {name: arrays[name].to_pylist() for name in COLUMN_NAMES}
    lines = [",".join(COLUMN_NAMES)]
    for index in range(len(columns["growth"])):
        lines.append(
            f"{columns['growth'][index]},{columns['halves'][index]},"
            f"{columns['boolish'][index]},{columns['datish'][index].isoformat()}"
        )
    return "\n".join(lines) + "\n"


class InferenceFamily:
    """The inference family implementor: per-column type expectations on the CSV door."""

    name = "inference"

    def generate(self, rows: int, seed: int, out: Path) -> FamilyOutput:
        """Write the conflict CSV and the resolved-type Parquet under out."""
        refuse_bad_rows(rows)
        refuse_bad_seed(seed)
        refuse_repository_output(out)
        out.mkdir(parents=True, exist_ok=True)
        arrays = _resolved_arrays(rows, seed, rows // CSV_INFER_ROW_DIVISOR)
        (out / CSV_NAME).write_text(_csv_text(arrays), encoding="utf-8")
        table = pa.table(arrays)
        pq.write_table(table, out / PARQUET_NAME)
        return FamilyOutput(rows=rows, parquet_path=out / PARQUET_NAME, csv_path=out / CSV_NAME)

    def expected_read_schema(self, fmt: Literal["parquet", "csv"]) -> pa.Schema | None:
        """Return the declared expectation; both formats carry the same resolved types."""
        if fmt == "parquet" or fmt == "csv":
            return DECLARED_SCHEMA
        return None


INFERENCE_FAMILY = InferenceFamily()
