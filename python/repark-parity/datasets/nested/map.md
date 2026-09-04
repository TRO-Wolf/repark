# map — python/repark-parity/datasets/nested

CC-2 slice complete: comments and docstrings condensed; oracle discriminators, pins, mutation payloads, and safety contracts kept byte-exact; history narration deleted.

## Purpose

Nested-reading + `dynamicFlatten` torture family (U-DF-1 capitalized-`Legs` corpus).
Deep struct/list nesting (depth ≥ 6), mixed list element types, lists of structs,
capitalized field names, null-typed lists, empty and null list rows.

Two doors: `small(rows=64, seed=42)` (in-memory) and a CLI that writes `data.parquet`
+ `data.jsonl` to the cache root. Determinism = identical pyarrow tables (schema +
values) from `small()` and from files re-read under `SCHEMA` — not raw file bytes.

## Contents

- `datagen.py` — schema, `generate` / `small` / `write_files` / `read_parquet` /
  `read_jsonl`, CLI (`--rows` default 1_000_000, `--seed` default 42, `--out`).
- `bed.py` — **PERF-DYNFLATTEN-1** measurement bed: named shapes (struct depth 3/6,
  list-of-struct 1/8/64, cartesian sibling lists, null-typed list), 30 % null
  parents, dictionary-encoded `Name`, capitalized fields. CLI `--scale
  gate|quick|full --out DIR`. Real-dataset flags/env refused.
  pins: perf-dynflatten-1-measure/C-001
- `__init__.py` — re-exports the public door.
- `map.md` — this file.

## Classes (`CLASSES` in datagen.py)

| Label | What `small()` must exhibit |
|---|---|
| `deep_nesting` | `Legs` field nesting depth ≥ 6 |
| `list_of_struct` | `Legs` is `list<struct<…>>` |
| `capitalized_legs` | field name is exactly `Legs` |
| `mixed_element_types` | `Tags` list<string>, `Scores` list<int32>, `Legs` list<struct>, `user_properties` list<null> |
| `null_typed_list` | `user_properties` is `list<null>` (empty and null values) |
| `empty_list_row` | at least one row with `Legs == []` |
| `null_list_row` | at least one row with `Legs is None` |

## I want to…

| I want to… | Go to |
|---|---|
| Build 64 deterministic rows | `small()` |
| Write parquet + JSON-lines | `write_files(rows=…, seed=…, out=…)` or the CLI |
| Re-read for the identity pin | `read_parquet` / `read_jsonl` (both cast to `SCHEMA`) |
| Build a measurement-bed shape | `bed.small("struct_d3")` / `bed.write_bed(scale="gate", out=…)` |

## Pointers

- Up: [../map.md](../map.md)
- Tests: [../../tests/map.md](../../tests/map.md) `test_datasets_nested.py`,
  `test_dynflatten_bed.py`

## Debug

| Symptom | First check |
|---|---|
| JSON re-read schema drift | always go through `read_jsonl` (explicit schema + cast); raw `pyarrow.json.read_json` makes `id` nullable |
| CLI `ImportError` | run `python python/repark-parity/datasets/nested/datagen.py` — the file bootstraps `repark_datasets` |
| Depth pin red | `schema_nesting_depth(SCHEMA.field("Legs").type)` must stay ≥ 6 |
