"""Seeded schema-inference-conflict generator (family ``schema_inference``).

CSV is the inference battleground; parquet is typed truth. ``small()`` returns the
typed table. CLI default ``conflict_at=500_000`` is the honest 1M sampling-miss
(smartCsv infers from the first 10k). ``small()`` defaults ``conflict_at`` inside
the row budget so every labeled class is visible at test scale.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import random
import sys
import types
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pyarrow.parquet as pq

LOGGER = logging.getLogger(__name__)

FAMILY: Final[str] = "schema_inference"
DEFAULT_CLI_ROWS: Final[int] = 1_000_000
DEFAULT_SEED: Final[int] = 42
SMALL_ROWS: Final[int] = 64
DEFAULT_CONFLICT_AT: Final[int] = 500_000
MAX_ROWS: Final[int] = 10_000_000
BATCH_SIZE: Final[int] = 4_096
DATA_PARQUET: Final[str] = "data.parquet"
DATA_CSV: Final[str] = "data.csv"
MANIFEST_NAME: Final[str] = "manifest.json"
INT32_MAX: Final[int] = 2**31 - 1
#: Floor for the `leading_zero_id` pad width — keeps small runs at the historical shape.
LEADING_ZERO_MIN_WIDTH: Final[int] = 6

SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("int_widens", pa.int64(), nullable=False),
        pa.field("str_or_float", pa.string(), nullable=False),
        pa.field("boolish_int", pa.int32(), nullable=False),
        pa.field("dateish", pa.string(), nullable=False),
        pa.field("currency", pa.string(), nullable=False),
        pa.field("leading_zero_id", pa.string(), nullable=False),
        pa.field("empty_or_null", pa.string(), nullable=False),
        pa.field("euro_decimal", pa.string(), nullable=False),
        pa.field("scientific", pa.float64(), nullable=False),
        pa.field("ts_looking", pa.timestamp("us"), nullable=False),
        pa.field("bool_spelling", pa.bool_(), nullable=False),
    ]
)

_WORD_POOL: Final[tuple[str, ...]] = ("alpha", "beta", "gamma", "delta", "epsilon")
_NULL_TOKENS: Final[tuple[str, ...]] = ("", "null", "NA", "n/a", "none", "nan", "ok")
_CURRENCY_MARKS: Final[tuple[str, ...]] = ("$", "€", "£")
_BOOL_TRUE: Final[tuple[str, ...]] = ("true", "TRUE", "t", "T")
_BOOL_FALSE: Final[tuple[str, ...]] = ("false", "FALSE", "f", "F")
_CSV_COLUMNS: Final[tuple[str, ...]] = tuple(field.name for field in SCHEMA)


def _bootstrap_repark_datasets() -> None:
    if "repark_datasets" in sys.modules:
        return
    datasets_dir = Path(__file__).resolve().parent.parent
    package = types.ModuleType("repark_datasets")
    package.__path__ = [str(datasets_dir)]  # type: ignore[attr-defined]
    sys.modules["repark_datasets"] = package


def _cache_mod() -> Any:
    _bootstrap_repark_datasets()
    import importlib

    return importlib.import_module("repark_datasets._cache")


def load_manifest() -> dict[str, Any]:
    """Return the checked-in class manifest (tests read this file, not a copy)."""
    path = Path(__file__).resolve().parent / MANIFEST_NAME
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        msg = f"manifest must be a JSON object: {path}"
        raise ValueError(msg)
    return payload


def _validate(rows: int, seed: int, conflict_at: int) -> None:
    if not isinstance(rows, int) or isinstance(rows, bool) or rows < 1:
        msg = f"rows must be an integer >= 1; got {rows!r}"
        raise ValueError(msg)
    if rows > MAX_ROWS:
        msg = f"rows must be <= {MAX_ROWS}; got {rows}"
        raise ValueError(msg)
    if not isinstance(seed, int) or isinstance(seed, bool) or seed < 0:
        msg = f"seed must be an integer >= 0; got {seed!r}"
        raise ValueError(msg)
    if not isinstance(conflict_at, int) or isinstance(conflict_at, bool) or conflict_at < 0:
        msg = f"conflict_at must be an integer >= 0; got {conflict_at!r}"
        raise ValueError(msg)


def leading_zero_width(rows: int) -> int:
    """Zero-pad width that keeps a leading zero on EVERY id of a ``rows``-row run.

    A fixed ``06d`` silently retires the class once ``row_index >= 1_000_000``
    (``1000000`` has no leading zero) and :data:`MAX_ROWS` allows that, so the
    width is one digit wider than the largest index, floored at
    :data:`LEADING_ZERO_MIN_WIDTH` for small runs.
    """
    if rows < 1:
        return LEADING_ZERO_MIN_WIDTH
    return max(LEADING_ZERO_MIN_WIDTH, len(str(rows - 1)) + 1)


def format_leading_zero_id(row_index: int, width: int) -> str:
    """Format one id at an explicit pad width."""
    return f"{row_index:0{width}d}"


def leading_zero_id(row_index: int, rows: int) -> str:
    """The id emitted for ``row_index`` in a ``rows``-row run (always leading-zeroed)."""
    return format_leading_zero_id(row_index, leading_zero_width(rows))


def _resolve_small_conflict_at(rows: int, conflict_at: int | None) -> int:
    if conflict_at is not None:
        return conflict_at
    return max(1, rows // 2)


def _build_row(
    rng: random.Random,
    row_index: int,
    conflict_at: int,
    id_width: int,
) -> tuple[dict[str, Any], dict[str, str]]:
    """One row. RNG draws are consumed on every row so later rows stay seed-stable."""
    if row_index < conflict_at:
        widens = int(row_index % 1000)
        str_or_float = rng.choice(_WORD_POOL)
    else:
        widens = int(INT32_MAX + 1 + (row_index - conflict_at))
        str_or_float = f"{rng.randint(1, 9)}.{rng.randint(0, 99):02d}"

    boolish = int(rng.choice((0, 1)))
    if rng.choice((True, False)):
        dateish = f"2020-01-{(row_index % 28) + 1:02d}"
    else:
        dateish = f"not-a-date-{row_index % 10}"

    mark = rng.choice(_CURRENCY_MARKS)
    currency = f"{mark}{rng.randint(1, 99)}.{rng.randint(0, 99):02d}"
    leading = format_leading_zero_id(row_index, id_width)
    empty_or_null = _NULL_TOKENS[row_index % len(_NULL_TOKENS)]
    euro = f"{rng.randint(1, 99)},{rng.randint(0, 99):02d}"

    mantissa = rng.randint(1, 9)
    exponent = rng.randint(1, 4)
    scientific_token = f"{mantissa}.{rng.randint(0, 9)}e{exponent}"
    scientific_value = float(scientific_token)

    hours = rng.randint(0, 23)
    minutes = rng.choice((0, 15, 30, 45))
    timestamp = datetime(2020, 1, 1) + timedelta(days=row_index % 400, hours=hours, minutes=minutes)
    timestamp_token = timestamp.strftime("%Y-%m-%dT%H:%M:%S")

    flag = bool(rng.choice((True, False)))
    bool_token = rng.choice(_BOOL_TRUE if flag else _BOOL_FALSE)

    typed: dict[str, Any] = {
        "id": int(row_index),
        "int_widens": widens,
        "str_or_float": str_or_float,
        "boolish_int": boolish,
        "dateish": dateish,
        "currency": currency,
        "leading_zero_id": leading,
        "empty_or_null": empty_or_null,
        "euro_decimal": euro,
        "scientific": scientific_value,
        "ts_looking": timestamp,
        "bool_spelling": flag,
    }
    csv_row: dict[str, str] = {
        "id": str(row_index),
        "int_widens": str(widens),
        "str_or_float": str_or_float,
        "boolish_int": str(boolish),
        "dateish": dateish,
        "currency": currency,
        "leading_zero_id": leading,
        "empty_or_null": empty_or_null,
        "euro_decimal": euro,
        "scientific": scientific_token,
        "ts_looking": timestamp_token,
        "bool_spelling": bool_token,
    }
    return typed, csv_row


def _iter_batches(
    rows: int,
    seed: int,
    conflict_at: int,
    *,
    batch_size: int = BATCH_SIZE,
) -> Iterator[tuple[pa.RecordBatch, list[dict[str, str]]]]:
    rng = random.Random(seed)
    id_width = leading_zero_width(rows)
    start = 0
    while start < rows:
        end = min(start + batch_size, rows)
        typed_rows: list[dict[str, Any]] = []
        csv_rows: list[dict[str, str]] = []
        for row_index in range(start, end):
            typed, csv_row = _build_row(rng, row_index, conflict_at, id_width)
            typed_rows.append(typed)
            csv_rows.append(csv_row)
        yield pa.RecordBatch.from_pylist(typed_rows, schema=SCHEMA), csv_rows
        start = end


def generate(rows: int, seed: int, *, conflict_at: int) -> pa.Table:
    """Build the typed-truth table. ``conflict_at`` is required (no silent CLI default)."""
    _validate(rows, seed, conflict_at)
    batches = [batch for batch, _csv in _iter_batches(rows, seed, conflict_at)]
    return pa.Table.from_batches(batches, schema=SCHEMA)


def small(
    rows: int = SMALL_ROWS,
    seed: int = DEFAULT_SEED,
    *,
    conflict_at: int | None = None,
) -> pa.Table:
    """CI / test door. Default ``conflict_at`` is ``rows // 2`` so every class is visible."""
    resolved = _resolve_small_conflict_at(rows, conflict_at)
    return generate(rows, seed, conflict_at=resolved)


def read_parquet(path: Path) -> pa.Table:
    """Read a generator parquet and cast to :data:`SCHEMA` (table-identity pin)."""
    return pq.read_table(path).cast(SCHEMA)


def write_files(
    *,
    rows: int = DEFAULT_CLI_ROWS,
    seed: int = DEFAULT_SEED,
    out: Path | None = None,
    conflict_at: int = DEFAULT_CONFLICT_AT,
) -> Path:
    """Write ``data.parquet`` (typed truth) + ``data.csv`` (inference text)."""
    _validate(rows, seed, conflict_at)
    cache = _cache_mod()
    if out is None:
        out_dir = cache.family_cache_dir(FAMILY)
        root = cache.default_datasets_root()
    else:
        out_dir = Path(out)
        root = out_dir
    prepared = cache.prepare_output_dir(out_dir, root=root)
    parquet_path = prepared / DATA_PARQUET
    csv_path = prepared / DATA_CSV
    cache.refuse_symlink_file(parquet_path)
    cache.refuse_symlink_file(csv_path)

    LOGGER.info(
        "schema_inference: writing %s rows (seed=%s conflict_at=%s) → %s",
        rows,
        seed,
        conflict_at,
        prepared,
    )
    with (
        parquet_path.open("wb") as parquet_handle,
        csv_path.open("w", encoding="utf-8", newline="") as csv_handle,
    ):
        writer = pq.ParquetWriter(parquet_handle, SCHEMA)
        csv_writer = csv.DictWriter(csv_handle, fieldnames=list(_CSV_COLUMNS), lineterminator="\n")
        csv_writer.writeheader()
        try:
            for batch, csv_rows in _iter_batches(rows, seed, conflict_at):
                writer.write_batch(batch)
                csv_writer.writerows(csv_rows)
        finally:
            writer.close()
    return prepared


def main(argv: list[str] | None = None) -> int:
    """CLI: ``--rows`` (default 1_000_000), ``--seed``, ``--out``, ``--conflict-at``."""
    parser = argparse.ArgumentParser(description="Generate the schema-inference torture dataset")
    parser.add_argument("--rows", type=int, default=DEFAULT_CLI_ROWS)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--out", type=Path, default=None)
    parser.add_argument(
        "--conflict-at",
        type=int,
        default=DEFAULT_CONFLICT_AT,
        help=f"row index of the int32→int64 / string→float shift (default {DEFAULT_CONFLICT_AT})",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        written = write_files(
            rows=args.rows,
            seed=args.seed,
            out=args.out,
            conflict_at=args.conflict_at,
        )
    except ValueError as error:
        print(f"usage error: {error}", file=sys.stderr)
        return 2
    print(written)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
