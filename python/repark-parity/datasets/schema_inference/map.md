# map — python/repark-parity/datasets/schema_inference

## Purpose

Schema-inference-conflict family. CSV is the inference battleground; parquet is
typed truth. One column per labeled class (`manifest.json`). The CLI default
`conflict_at=500_000` is the honest 1M sampling-miss versus smartCsv's 10k
inference cap. `small()` defaults `conflict_at` to `rows // 2` so every class is
visible at test scale.

## Contents

- `datagen.py` — `generate` / `small` / `write_files` / `read_parquet` / CLI
  (`--rows`, `--seed`, `--out`, `--conflict-at`).
- `manifest.json` — class id → column name + parquet type. Tests read this file.
- `__init__.py` — public door.
- `map.md` — this file.

## Classes

See `manifest.json`. Includes the charter set (int32→int64 at `conflict_at`,
string-vs-float halves, bool-looking ints, date-looking strings, currency
symbols, leading-zero ids, empty/null tokens) plus honestly generatable extras
(euro-comma decimals, scientific notation, ISO timestamps, mixed-case bool
spellings).

## I want to…

| I want to… | Go to |
|---|---|
| Build 64 deterministic typed rows | `small(rows=64, seed=42)` |
| Force the mid-file shift | `small(..., conflict_at=32)` / CLI `--conflict-at` |
| Write CSV + parquet | `write_files(...)` or the CLI |

## Pointers

- Up: [../map.md](../map.md)
- Tests: [../../tests/map.md](../../tests/map.md) `test_datasets_schema_inference.py`

## Debug

| Symptom | First check |
|---|---|
| No int64 values in `int_widens` | `conflict_at` is past `rows` (CLI default 500_000 on a small run) |
| CSV re-read types differ from `small()` | Expected — CSV is the battleground; identity pin is parquet |
| smartCsv still sees int32 on the 1M corpus | POLICY: inference samples 10k; facade pin is DS-4 |
