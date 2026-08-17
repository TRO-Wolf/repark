# map — python/repark-parity/datasets/extreme_types

## Purpose

Extreme-types family: decimal128-scale values, digit strings beyond 38 digits
(the smartCsv ladder demotes those — documented here, facade POLICY pin in
DS-4), deterministic UUIDs, paragraph-length strings, embedded HTML fragments
(example.com only).

CSV is text; parquet is typed truth. `small()` returns the typed table.

## Contents

- `datagen.py` — `generate` / `small` / `write_files` / `read_parquet` / CLI
  (`--rows` default 1_000_000, `--seed` default 42, `--out`).
- `manifest.json` — class id → column name + parquet type. Tests read this file.
- `__init__.py` — public door.
- `map.md` — this file.

## Classes

See `manifest.json`. `beyond_38` is stored as string (cannot be decimal128).
`decimal_hi` is `decimal128(24,21)` around `102.102334252345232345233`.

## I want to…

| I want to… | Go to |
|---|---|
| Build 64 deterministic typed rows | `small()` |
| Write CSV + parquet | `write_files(...)` or the CLI |

## Pointers

- Up: [../map.md](../map.md)
- Tests: [../../tests/map.md](../../tests/map.md) `test_datasets_extreme_types.py`

## Debug

| Symptom | First check |
|---|---|
| decimal128 overflow on `beyond_38` | that column is string on purpose |
| UUID not stable across runs | must be `uuid5` over seed+row, never `uuid4` |
| HTML hygiene red | fragments may only mention example.com |
