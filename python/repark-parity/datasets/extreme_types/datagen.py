"""Seeded extreme-types generator (family ``extreme_types``).

Typed truth lives in parquet; CSV carries the same values as text. ``small()``
returns the typed table. Values beyond 38 digits are stored as strings — the
smartCsv p>38 → float64 demotion is a documented POLICY pin for DS-4, not a
generator fix.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import random
import sys
import types
import uuid
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pyarrow.parquet as pq

LOGGER = logging.getLogger(__name__)

FAMILY: Final[str] = "extreme_types"
DEFAULT_CLI_ROWS: Final[int] = 1_000_000
DEFAULT_SEED: Final[int] = 42
SMALL_ROWS: Final[int] = 64
MAX_ROWS: Final[int] = 10_000_000
BATCH_SIZE: Final[int] = 4_096
DATA_PARQUET: Final[str] = "data.parquet"
DATA_CSV: Final[str] = "data.csv"
MANIFEST_NAME: Final[str] = "manifest.json"
DECIMAL_PRECISION: Final[int] = 24
DECIMAL_SCALE: Final[int] = 21
# 40 integer digits + 1 fractional — precision 41, above the smartCsv decimal128 cap of 38.
BEYOND_38_PREFIX: Final[str] = "1" + ("0" * 39)
DECIMAL_BASE: Final[Decimal] = Decimal("102.102334252345232345233")
DECIMAL_STEP: Final[Decimal] = Decimal("0.000000000000000000001")
UUID_NAMESPACE: Final[uuid.UUID] = uuid.uuid5(
    uuid.NAMESPACE_URL, "https://example.com/repark-datasets"
)
WORD_POOL: Final[tuple[str, ...]] = (
    "alpha",
    "river",
    "stone",
    "cloud",
    "table",
    "green",
    "north",
    "value",
    "record",
    "field",
    "sample",
    "token",
    "range",
    "index",
    "layer",
)

SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("decimal_hi", pa.decimal128(DECIMAL_PRECISION, DECIMAL_SCALE), nullable=False),
        pa.field("beyond_38", pa.string(), nullable=False),
        pa.field("uuid_col", pa.string(), nullable=False),
        pa.field("paragraph", pa.string(), nullable=False),
        pa.field("html_fragment", pa.string(), nullable=False),
    ]
)

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


def _validate(rows: int, seed: int) -> None:
    if not isinstance(rows, int) or isinstance(rows, bool) or rows < 1:
        msg = f"rows must be an integer >= 1; got {rows!r}"
        raise ValueError(msg)
    if rows > MAX_ROWS:
        msg = f"rows must be <= {MAX_ROWS}; got {rows}"
        raise ValueError(msg)
    if not isinstance(seed, int) or isinstance(seed, bool) or seed < 0:
        msg = f"seed must be an integer >= 0; got {seed!r}"
        raise ValueError(msg)


def _paragraph(rng: random.Random, row_index: int) -> str:
    word_count = 80 + rng.randint(0, 40)
    words = [rng.choice(WORD_POOL) for _ in range(word_count)]
    words[0] = f"row{row_index}"
    return " ".join(words)


def _build_row(
    rng: random.Random,
    row_index: int,
    seed: int,
) -> tuple[dict[str, Any], dict[str, str]]:
    decimal_value = DECIMAL_BASE + (DECIMAL_STEP * rng.randint(0, 9))
    beyond = f"{BEYOND_38_PREFIX}.{row_index % 10}"
    uuid_value = str(uuid.uuid5(UUID_NAMESPACE, f"{seed}:{row_index}"))
    paragraph = _paragraph(rng, row_index)
    html = (
        f'<div class="row"><span>{row_index}</span>'
        f'<a href="https://example.com/item/{row_index}">item</a></div>'
    )
    typed: dict[str, Any] = {
        "id": int(row_index),
        "decimal_hi": decimal_value,
        "beyond_38": beyond,
        "uuid_col": uuid_value,
        "paragraph": paragraph,
        "html_fragment": html,
    }
    csv_row: dict[str, str] = {
        "id": str(row_index),
        "decimal_hi": format(decimal_value, "f"),
        "beyond_38": beyond,
        "uuid_col": uuid_value,
        "paragraph": paragraph,
        "html_fragment": html,
    }
    return typed, csv_row


def _iter_batches(
    rows: int,
    seed: int,
    *,
    batch_size: int = BATCH_SIZE,
) -> Iterator[tuple[pa.RecordBatch, list[dict[str, str]]]]:
    rng = random.Random(seed)
    start = 0
    while start < rows:
        end = min(start + batch_size, rows)
        typed_rows: list[dict[str, Any]] = []
        csv_rows: list[dict[str, str]] = []
        for row_index in range(start, end):
            typed, csv_row = _build_row(rng, row_index, seed)
            typed_rows.append(typed)
            csv_rows.append(csv_row)
        yield pa.RecordBatch.from_pylist(typed_rows, schema=SCHEMA), csv_rows
        start = end


def generate(rows: int, seed: int) -> pa.Table:
    """Build the typed-truth table for ``seed`` (no wall-clock entropy)."""
    _validate(rows, seed)
    batches = [batch for batch, _csv in _iter_batches(rows, seed)]
    return pa.Table.from_batches(batches, schema=SCHEMA)


def small(rows: int = SMALL_ROWS, seed: int = DEFAULT_SEED) -> pa.Table:
    """CI / test door. Defaults are the bound A9 values (64 rows, seed 42)."""
    return generate(rows, seed)


def read_parquet(path: Path) -> pa.Table:
    """Read a generator parquet and cast to :data:`SCHEMA` (table-identity pin)."""
    return pq.read_table(path).cast(SCHEMA)


def write_files(
    *,
    rows: int = DEFAULT_CLI_ROWS,
    seed: int = DEFAULT_SEED,
    out: Path | None = None,
) -> Path:
    """Write ``data.parquet`` (typed truth) + ``data.csv`` (text)."""
    _validate(rows, seed)
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

    LOGGER.info("extreme_types: writing %s rows (seed=%s) → %s", rows, seed, prepared)
    with (
        parquet_path.open("wb") as parquet_handle,
        csv_path.open("w", encoding="utf-8", newline="") as csv_handle,
    ):
        writer = pq.ParquetWriter(parquet_handle, SCHEMA)
        csv_writer = csv.DictWriter(csv_handle, fieldnames=list(_CSV_COLUMNS), lineterminator="\n")
        csv_writer.writeheader()
        try:
            for batch, csv_rows in _iter_batches(rows, seed):
                writer.write_batch(batch)
                csv_writer.writerows(csv_rows)
        finally:
            writer.close()
    return prepared


def main(argv: list[str] | None = None) -> int:
    """CLI: ``--rows`` (default 1_000_000), ``--seed``, ``--out``."""
    parser = argparse.ArgumentParser(description="Generate the extreme-types torture dataset")
    parser.add_argument("--rows", type=int, default=DEFAULT_CLI_ROWS)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        written = write_files(rows=args.rows, seed=args.seed, out=args.out)
    except ValueError as error:
        print(f"usage error: {error}", file=sys.stderr)
        return 2
    print(written)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
