# map — python/repark-parity/fixtures/torture

## Purpose

The TORTURE-1 generator package (roadmap 1.2, card TORTURE-1): seeded, deterministic
torture datasets for the repark readers, generated as `repark_parity.torture` and never
committed. `python -m repark_parity.torture generate <family> --rows N --seed S --out DIR`
writes Parquet AND CSV per family. The tier switch is `TORTURE_TIER`: `ci` (default)
generates 10k rows into a temp dir at test time; `full` generates 1M rows once under
`/tmp/torture/` and reuses them through a rows+seed manifest. Suite:
[../../tests/torture/map.md](../../tests/torture/map.md).

## Contents

- `family.py` — the one `Family` protocol every family implementor provides (`name`,
  `generate(rows, seed, out) -> FamilyOutput`, `expected_read_schema(fmt)`), the
  `FamilyOutput` record, the shared data-file names, and the loud refusals (rows, seed,
  repository-internal output).
- `tiers.py` — tier names, the `TORTURE_TIER` read (unknown names refuse), row budgets
  (ci 10k, full 1M), the shared seed default 7, and the `/tmp/torture/` full root.
- `nested.py` — the `nested` family (D-2): deep struct/list chains (struct levels =
  `--depth`, default 6), `--width` scalar fields per struct level (default 3, D-6 bed
  scaling), mixed list element types (string/int32/struct/null), lists of structs,
  capitalised field names (`Legs`, `Tags`, `Scores`, `Mixed`, `Value*`), null-typed lists
  (`user_properties`), empty and null list rows. Row classes cycle by index, so shapes are
  stable across row counts. Its CSV leg renders nested columns as JSON text: pyarrow
  refuses to write nested types as CSV (measured `ArrowInvalid`), and the suite pins that
  leg to completes-or-refuses-loud plus row count only — the schema contract rides the
  Parquet leg. Declared Parquet read expectation: the write schema with `user_properties`
  widened to `list<int32>`, because that is the measured Spark answer for a parquet
  `list<null>` and repark matches it (registry `DYNFLATTEN-LISTNULL-1`); pyarrow's own
  read-back keeps `null`, so the expectation is the parity answer, not the file's type.
- `inference.py` — the `inference` family (D-2): `growth` is int32-ranged before row
  rows//2 and int64-ranged after (row 5k at the CI tier's 10k rows, row 500k at full),
  `halves` alternates float-looking and text rows, `boolish` is a 0/1 column, `datish` is
  date-looking text. One value source renders both the CSV and the resolved-type Parquet,
  so the two legs never disagree. Declared per-column CSV expectation: `int64`, `string`,
  `int32`, `date32`. The `boolish` expectation is `int32` (Spark narrows all-int columns);
  repark answers `int64`, filed as registry `CSV-INFER-INT32-WIDTH` with a strict xfail.
- `__main__.py` — the `generate` CLI; `--depth`/`--width` are refused for non-nested
  families.
- `map.md` — this file.

## I want to…

| I want to… | Go to |
|---|---|
| Generate a family by hand | `python -m repark_parity.torture generate <family> --rows N --seed 7 --out DIR` |
| Scale the dynamicFlatten bed | `... generate nested ... --depth D --width W` |
| Run the suite | `make py-test-torture` (`TORTURE_TIER=full` for the big tier) |
| Read the door pins | [../../tests/torture/map.md](../../tests/torture/map.md) |

## Pointers

- Up: [../map.md](../map.md)
- Card: [../../../../briefs/next-sequence.md](../../../../briefs/next-sequence.md) TORTURE-1
- Registry rows this package filed: [../../../../docs/spark-sql-iceberg-parity.md](../../../../docs/spark-sql-iceberg-parity.md) `CSV-INFER-INT32-WIDTH`

## Constraints

- Zero new Python dependencies (pyarrow + stdlib + the harness's pydantic).
- Data files never enter the repository: generators refuse repository-internal outputs.
- No comments in code (owner ruling); the reasons above live here.

## Debug

| Symptom | First check |
|---|---|
| `No module named 'repark_parity.torture'` | Import `repark_parity` first — the graft in its `__init__` exposes this directory only when it exists beside `src/` |
| `torture output must stay outside the repository` | Pass `--out` under `/tmp/` or another path outside the checkout |
| Full tier regenerates every run | The manifest in `/tmp/torture/<family>/manifest.json` must record the same family, rows, and seed |
