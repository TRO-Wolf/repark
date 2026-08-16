"""Seeded nested / dynamicFlatten torture generator (family ``nested``).

Two doors: ``small(rows=64, seed=42)`` returns an in-memory pyarrow table; the CLI
writes ``data.parquet`` + ``data.jsonl`` under the cache root (never the repo).
Determinism is table identity (schema + values), not raw file bytes.
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import sys
import types
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pyarrow.json as paj
import pyarrow.parquet as pq

LOGGER = logging.getLogger(__name__)

FAMILY: Final[str] = "nested"
DEFAULT_CLI_ROWS: Final[int] = 1_000_000
DEFAULT_SEED: Final[int] = 42
SMALL_ROWS: Final[int] = 64
MAX_ROWS: Final[int] = 10_000_000
BATCH_SIZE: Final[int] = 4_096
DATA_PARQUET: Final[str] = "data.parquet"
DATA_JSONL: Final[str] = "data.jsonl"

# Schema nesting of ``Legs`` is 7 (list → struct → list → struct → struct → struct → struct).
_DEEP = pa.struct(
    [
        pa.field("level", pa.int32(), nullable=True),
        pa.field("note", pa.string(), nullable=True),
    ]
)
_EXTRA = pa.struct(
    [
        pa.field("Deep", _DEEP, nullable=True),
        pa.field("Flags", pa.list_(pa.bool_()), nullable=True),
    ]
)
_META = pa.struct(
    [
        pa.field("venue", pa.string(), nullable=True),
        pa.field("Tags", pa.list_(pa.string()), nullable=True),
        pa.field("Extra", _EXTRA, nullable=True),
    ]
)
_FILL = pa.struct(
    [
        pa.field("fill_id", pa.int64(), nullable=True),
        pa.field("px", pa.float64(), nullable=True),
        pa.field("qty", pa.int64(), nullable=True),
        pa.field("Meta", _META, nullable=True),
    ]
)
_LEG = pa.struct(
    [
        pa.field("leg_id", pa.int64(), nullable=True),
        pa.field("side", pa.string(), nullable=True),
        pa.field("Fills", pa.list_(_FILL), nullable=True),
    ]
)

SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("Legs", pa.list_(_LEG), nullable=True),
        pa.field("Tags", pa.list_(pa.string()), nullable=True),
        pa.field("Scores", pa.list_(pa.int32()), nullable=True),
        pa.field("user_properties", pa.list_(pa.null()), nullable=True),
    ]
)

CLASSES: Final[tuple[str, ...]] = (
    "deep_nesting",
    "list_of_struct",
    "capitalized_legs",
    "mixed_element_types",
    "null_typed_list",
    "empty_list_row",
    "null_list_row",
)

_VENUES: Final[tuple[str, ...]] = ("XNYS", "XNAS", "ARCX")
_SIDES: Final[tuple[str, ...]] = ("Buy", "Sell")
_TAG_POOL: Final[tuple[str, ...]] = ("alpha", "beta", "gamma")


def _bootstrap_repark_datasets() -> None:
    """Register ``repark_datasets`` so script invocation can import ``_cache``."""
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


def schema_nesting_depth(arrow_type: pa.DataType) -> int:
    """Return the max list/struct nesting depth of ``arrow_type`` (scalars are 0)."""
    if pa.types.is_struct(arrow_type):
        if arrow_type.num_fields == 0:
            return 1
        return 1 + max(schema_nesting_depth(field.type) for field in arrow_type)
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return 1 + schema_nesting_depth(arrow_type.value_type)
    return 0


def _make_fill(rng: random.Random, fill_id: int) -> dict[str, Any]:
    flag_count = rng.randint(0, 2)
    tag_count = rng.randint(0, 2)
    return {
        "fill_id": int(fill_id),
        "px": float(rng.randint(1, 100)),
        "qty": int(rng.randint(1, 20)),
        "Meta": {
            "venue": rng.choice(_VENUES),
            "Tags": list(_TAG_POOL[:tag_count]),
            "Extra": {
                "Deep": {
                    "level": int(rng.randint(0, 6)),
                    "note": "example.com/nested",
                },
                "Flags": [bool(rng.choice((True, False))) for _ in range(flag_count)],
            },
        },
    }


def _make_leg(rng: random.Random, leg_id: int) -> dict[str, Any]:
    fill_count = rng.randint(0, 2)
    fills = [_make_fill(rng, fill_id=leg_id * 10 + fill_index) for fill_index in range(fill_count)]
    return {
        "leg_id": int(leg_id),
        "side": rng.choice(_SIDES),
        "Fills": fills,
    }


def _build_row(rng: random.Random, row_index: int) -> dict[str, Any]:
    """One row. RNG draws are consumed on every row so later rows stay seed-stable."""
    first_leg = _make_leg(rng, leg_id=row_index * 2 + 1)
    second_leg = _make_leg(rng, leg_id=row_index * 2 + 2)
    tag_count = rng.randint(0, 3)
    score_count = rng.randint(0, 3)
    tags: list[str] | None = list(_TAG_POOL[:tag_count])
    scores: list[int] | None = [int(rng.randint(0, 100)) for _ in range(score_count)]
    properties: list[None] | None = []

    pattern = row_index % 8
    if pattern == 0:
        legs: list[dict[str, Any]] | None = [first_leg, second_leg]
    elif pattern == 1:
        legs = []
    elif pattern == 2:
        legs = None
    elif pattern == 3:
        legs = [first_leg]
        properties = None
        tags = None
    elif pattern == 4:
        legs = [first_leg]
        scores = None
    else:
        legs = [first_leg]

    return {
        "id": int(row_index),
        "Legs": legs,
        "Tags": tags,
        "Scores": scores,
        "user_properties": properties,
    }


def _iter_batches(
    rows: int,
    seed: int,
    *,
    batch_size: int = BATCH_SIZE,
) -> Iterator[pa.RecordBatch]:
    rng = random.Random(seed)
    start = 0
    while start < rows:
        end = min(start + batch_size, rows)
        batch_rows = [_build_row(rng, row_index) for row_index in range(start, end)]
        yield pa.RecordBatch.from_pylist(batch_rows, schema=SCHEMA)
        start = end


def generate(rows: int, seed: int) -> pa.Table:
    """Build the nested table for ``seed`` (no wall-clock entropy)."""
    _validate(rows, seed)
    batches = list(_iter_batches(rows, seed))
    return pa.Table.from_batches(batches, schema=SCHEMA)


def small(rows: int = SMALL_ROWS, seed: int = DEFAULT_SEED) -> pa.Table:
    """CI / test door. Defaults are the bound A9 values (64 rows, seed 42)."""
    return generate(rows, seed)


def read_parquet(path: Path) -> pa.Table:
    """Read a generator parquet and cast to :data:`SCHEMA` (table-identity pin)."""
    table = pq.read_table(path)
    return table.cast(SCHEMA)


def read_jsonl(path: Path) -> pa.Table:
    """Read a generator JSON-lines file under :data:`SCHEMA` (table-identity pin)."""
    table = paj.read_json(path, parse_options=paj.ParseOptions(explicit_schema=SCHEMA))
    return table.cast(SCHEMA)


def write_files(
    *,
    rows: int = DEFAULT_CLI_ROWS,
    seed: int = DEFAULT_SEED,
    out: Path | None = None,
) -> Path:
    """Write ``data.parquet`` + ``data.jsonl`` under ``out`` (or the family cache dir)."""
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
    jsonl_path = prepared / DATA_JSONL
    cache.refuse_symlink_file(parquet_path)
    cache.refuse_symlink_file(jsonl_path)

    LOGGER.info("nested family: writing %s rows (seed=%s) → %s", rows, seed, prepared)
    with (
        parquet_path.open("wb") as parquet_handle,
        jsonl_path.open("w", encoding="utf-8", newline="\n") as jsonl_handle,
    ):
        writer = pq.ParquetWriter(parquet_handle, SCHEMA)
        try:
            for batch in _iter_batches(rows, seed):
                writer.write_batch(batch)
                for row in batch.to_pylist():
                    jsonl_handle.write(
                        json.dumps(row, separators=(",", ":"), sort_keys=True, ensure_ascii=True)
                    )
                    jsonl_handle.write("\n")
        finally:
            writer.close()
    return prepared


def main(argv: list[str] | None = None) -> int:
    """CLI: ``--rows`` (default 1_000_000), ``--seed``, ``--out``."""
    parser = argparse.ArgumentParser(description="Generate the nested torture dataset")
    parser.add_argument(
        "--rows",
        type=int,
        default=DEFAULT_CLI_ROWS,
        help=f"row count (default {DEFAULT_CLI_ROWS}; max {MAX_ROWS})",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"RNG seed (default {DEFAULT_SEED})",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="output directory (default $XDG_CACHE_HOME/repark-datasets/nested)",
    )
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
