# map — python/repark-parity/datasets/smartcsv

## Purpose

Messy-CSV torture family at generator scale — the growth of the three-row inline
messy-CSV example in the facade's smartCsv suite. CSV is the battleground;
parquet is typed truth. `small()` at the default 64 rows emits **every** labeled
class.

## Two class scopes

- **column** — visible in the typed table (`small()` / `data.parquet`): currency
  amounts with thousands separators, decimal width variants (pure fraction, bare
  int, uniform scale, wide int digits, signs, leading zeros), euro comma
  decimals, bool spellings (`true`/`TRUE`/`t`/`T` and the false half), yes/no
  spellings that look boolean but are **not** bool tokens, null-token variants, a value
  embedding all four candidate delimiters, the duplicate-header pair, and the two
  ragged tail columns.
- **file** — visible only in the CSV text: UTF-8 BOM, preamble lines, the
  duplicate header row, ragged rows (short **and** long), the delimiter zoo, and
  quoting forced by the embedded delimiters.

The CSV deliberately does **not** naively round-trip to the parquet — that gap is
the torture. The A3 determinism pin is the parquet table, never raw file bytes.

## Contents

- `datagen.py` — `generate` / `small` / `write_files` / `read_parquet` /
  `render_csv` / `csv_file_name` / `is_short_row` / `is_long_row` /
  `null_token_for` / `typed_note` / CLI (`--rows` default 1_000_000, `--seed`
  default 42, `--out`).
- `manifest.json` — class id → scope, column, parquet type; plus the delimiter
  schemes and both null-token groups. Tests read this file.
- `__init__.py` — public door.
- `map.md` — this file.

## Emitted files

`data.parquet` (typed truth) plus one CSV per delimiter scheme:
`data_comma.csv`, `data_semicolon.csv`, `data_tab.csv`, `data_pipe.csv`. Each
carries the same logical rows, the same BOM and preamble, and the same duplicate
header row.

## Ragged rows

Short rows (`row_index % 11 == 5`) omit the two trailing cells; typed truth is
null for `ragged_tail_1` / `ragged_tail_2`. Long rows (`row_index % 13 == 7`)
carry one unlabeled overflow cell that no typed column claims. Short wins when a
row qualifies for both — the first such row is 137, which is why the tie is coded
explicitly rather than left to evaluation order.

## Null tokens

`RECOGNIZED_NULL_TOKENS` mirrors the smartCsv default token set case-insensitively
(`""`, `null`, `none`, `na`, `n/a`, `nan`) and yields null typed truth.
`UNRECOGNIZED_NULL_TOKENS` (`-`, `\N`, `(null)`, `NIL`, `missing`, `ok`) survive
as literal strings. The mirror is a generator-side convenience; binding it to the
engine is a DS-4 facade pin, not a claim made here.

## I want to…

| I want to… | Go to |
|---|---|
| Build 64 deterministic typed rows | `small(rows=64, seed=42)` |
| Get one scheme's CSV text in memory | `render_csv(rows, seed, "pipe")` |
| Write parquet + all four CSVs | `write_files(...)` or the CLI |
| Add a torture class | extend `datagen.py` **and** `manifest.json` — the type cross-check test binds the two |

## Pointers

- Up: [../map.md](../map.md)
- Tests: [../../tests/map.md](../../tests/map.md) `test_datasets_smartcsv.py`,
  `test_datasets_manifest_types.py`

## Debug

| Symptom | First check |
|---|---|
| CSV re-read types differ from `small()` | Expected — CSV is the battleground; identity pin is parquet |
| A scheme's file parses to one column | The wrong delimiter was passed to `csv.reader`; use `DELIMITERS[scheme]` |
| No quoted fields in a scheme | `embedded_delims` must carry all four candidate delimiters |
| `small()` misses a class | Every cycle must be ≤ 64 (bool 8, null tokens 17, decimals 7, marks 3, yes/no 6) |
