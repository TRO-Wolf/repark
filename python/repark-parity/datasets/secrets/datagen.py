"""Seeded credential-named-column generator (family ``secrets``).

Every secret-shaped column carries an **obviously fake** synthetic value: each one
starts with the literal ``repark-fake-`` marker and never imitates a real credential
format. That is a hard hygiene fence, not a style preference — a fixture that looks
like a live key is a fixture that eventually gets reported as a leak.

``manifest.json`` labels each column with the needle class it stands for (the needle
inventory lives in the facade's ``prop_key_is_secret`` mirror). Two columns are
deliberate NEGATIVE controls: ``id`` matches nothing, and ``bucket_key`` ends with
``_key`` yet is excluded by the documented ``bucket`` carve-out.

Reads of this family behave NORMALLY today. Opt-in secret flagging on data columns is
a roadmap feature this fixture predates; facade-level pins land in DS-4.
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
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pyarrow.parquet as pq

LOGGER = logging.getLogger(__name__)

FAMILY: Final[str] = "secrets"
DEFAULT_CLI_ROWS: Final[int] = 1_000_000
DEFAULT_SEED: Final[int] = 42
SMALL_ROWS: Final[int] = 64
MAX_ROWS: Final[int] = 10_000_000
BATCH_SIZE: Final[int] = 4_096
DATA_PARQUET: Final[str] = "data.parquet"
DATA_CSV: Final[str] = "data.csv"
MANIFEST_NAME: Final[str] = "manifest.json"

#: The marker every synthetic credential value starts with. Tests pin it.
FAKE_PREFIX: Final[str] = "repark-fake-"
#: Prefixes that must NEVER appear: they are the shapes real credential scanners hunt.
FORBIDDEN_VALUE_PREFIXES: Final[tuple[str, ...]] = ("AKIA", "ASIA", "ghp_", "gho_", "sk-", "xoxb-")

#: ``(column, class_id)`` for every credential-named column. Order is the schema order.
SECRET_COLUMNS: Final[tuple[tuple[str, str], ...]] = (
    ("apiKey", "apikey_camel"),
    ("api_key", "apikey_snake"),
    ("api_token", "token_api"),
    ("access_token", "token_access"),
    ("password", "password"),
    ("session_token", "token_session"),
    ("accessKey", "accesskey_camel"),
    ("client_secret", "secret_client"),
    ("private_key", "privatekey_snake"),
    ("credential_id", "credential_ref"),
)
#: The one nullable credential column — a null secret is still a secret-shaped column.
NULLABLE_SECRET_COLUMN: Final[str] = "session_token"
NULL_EVERY: Final[int] = 7
#: Negative control: ends with ``_key`` but the ``bucket`` carve-out excludes it.
CARVE_OUT_COLUMN: Final[str] = "bucket_key"
MAX_FILLER: Final[int] = 12

SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        *(
            pa.field(column, pa.string(), nullable=column == NULLABLE_SECRET_COLUMN)
            for column, _class_id in SECRET_COLUMNS
        ),
        pa.field(CARVE_OUT_COLUMN, pa.string(), nullable=False),
    ]
)

_CSV_COLUMNS: Final[tuple[str, ...]] = tuple(field.name for field in SCHEMA)
_SLUG_BY_COLUMN: Final[dict[str, str]] = {
    column: class_id.replace("_", "-") for column, class_id in SECRET_COLUMNS
}


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


def fake_secret(slug: str, row_index: int, filler: int = 0) -> str:
    """Build one obviously-fake credential value.

    ``filler`` only varies the length (short and long secrets in one column); it never
    changes the shape. The value always begins with :data:`FAKE_PREFIX`.
    """
    return f"{FAKE_PREFIX}{slug}-{row_index:06d}{'x' * filler}"


def _build_row(rng: random.Random, row_index: int) -> tuple[dict[str, Any], dict[str, str]]:
    """One row. Every column draws from ``rng`` so later rows stay seed-stable."""
    typed: dict[str, Any] = {"id": int(row_index)}
    csv_row: dict[str, str] = {"id": str(row_index)}
    for column, _class_id in SECRET_COLUMNS:
        filler = rng.randint(0, MAX_FILLER)
        value = fake_secret(_SLUG_BY_COLUMN[column], row_index, filler)
        if column == NULLABLE_SECRET_COLUMN and row_index % NULL_EVERY == 0:
            typed[column] = None
            csv_row[column] = ""
        else:
            typed[column] = value
            csv_row[column] = value
    object_key = f"warehouse/table/part-{row_index:05d}.parquet"
    typed[CARVE_OUT_COLUMN] = object_key
    csv_row[CARVE_OUT_COLUMN] = object_key
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
            typed, csv_row = _build_row(rng, row_index)
            typed_rows.append(typed)
            csv_rows.append(csv_row)
        yield pa.RecordBatch.from_pylist(typed_rows, schema=SCHEMA), csv_rows
        start = end


def generate(rows: int, seed: int) -> pa.Table:
    """Build the typed table for ``seed`` (no wall-clock entropy)."""
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

    LOGGER.info("secrets: writing %s rows (seed=%s) → %s", rows, seed, prepared)
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
    parser = argparse.ArgumentParser(description="Generate the secrets-fixture dataset")
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
