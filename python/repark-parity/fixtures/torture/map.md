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
- `extreme_types.py` — the `extreme_types` family (D-2, step 2): `Precise`
  `decimal(38,18)` with 20-digit integer parts and full 18-digit fractions, `Whole`
  `decimal(38,0)` at the 38-digit maximum, UUID-shaped strings, repeated-sentence
  paragraphs, embedded HTML with quotes, entities and tags. The CSV leg swaps the two
  decimal columns for one 38-digit integer text column (`big38`): Spark's answer for
  overflow-long integer CSV text is measured territory (`CSV-INFER-20DIGIT`,
  `decimal(20,0)`-class), while scale-18 decimal CSV inference is unmeasured, so the
  high-precision decimals ride the Parquet leg where the type is file-carried. Every
  decimal is built from exact int/string forms — Python's `Decimal` context rounds unary
  minus, multiplication and addition to 28 significant digits, which silently corrupts
  38-digit values. The `big38` cell is a strict xfail citing `CSV-INFER-20DIGIT`.
- `smartcsv.py` — the `smartcsv` family (D-2, step 2): capitalised and space-bearing CSV
  headers (`Order Total`, `Qty`, ...), blank cells mixed with text, an all-blank column,
  currency-formatted text, an int-width ladder (int32-fitting, int64-fitting, 20-digit),
  and two bool-spelling columns (strict `true/FALSE/True/false`, loose `yes/no/Y/N`). The
  Parquet leg carries the same names and resolved types. Repark refuses the capitalised
  CSV leg loud on both doors — registry `CSV-INFER-HEADER-CASE`, strict xfail on the CSV
  schema cell, whose contract rides the leg's declared Spark answer; the loud-plus-row
  count cell stays green through the refusal arm.
- `temporal.py` — the `temporal` family (D-2, step 2): `ts_us` `timestamp(µs)` at the
  epoch edges and the ±i64-microsecond bounds, `day` `date32` at the epoch edges and the
  whole-day edge 106751991 (the largest whole day count an i64 microsecond count holds),
  `duration_us` written as Arrow `duration(µs)` at the same bounds, and the MonthDayNano
  decomposition columns (`interval_months`/`interval_days`/`interval_nanos`, mixed tuples
  and the zero unit). Measured: pyarrow writes the Duration logical annotation (repark
  answers `duration[us]`) but refuses `month_day_nano_interval` for Parquet, so the
  interval values ride the decomposition; the interval TYPE contract is carried by the
  registry's `BL-13`/`BL-14` rows, not by a file. The `duration_us` declaration is
  repark's measured answer; Spark's answer for the parquet Duration annotation is
  unmeasured until a live run. The CSV leg keeps its timestamp text inside the
  nanosecond-representable window (repark's CSV timestamp inference casts through ns and
  refuses year-9999 text loud, measured; Spark's CSV window is µs-wide, unmeasured), so
  the far-instant extremes ride the Parquet leg. Cells: the two strict-xfail SQL cells
  cite `BL-14` (zero-sub-day promotion) and `DATE-INTERVAL-NSBOUND-1` (the whole-day-edge
  raise), and the `months`/`days`/`nanos` cells cite `CSV-INFER-INT32-WIDTH`.
- `decimal_overflow.py` — the `decimal_overflow` family (D-2, step 2): two `decimal(38,0)`
  columns at 9.5e37/9.9e37 scale so the true column sum (~9.5e41) overflows both the
  `decimal(38,0)` aggregate boundary and the i128 accumulator, and the true average
  (~9.5e37) overflows the `decimal(38,4)` average boundary. Repark's `SUM` answers the
  wrapped i128 value (registry `SUM-DEC-I128WRAP-1`, strict xfail) and its `AVG` refuses
  loud on both doors, matching the recorded `AVG-DEC-SUMWRAP-1` Spark class. The CSV leg's
  38-digit integer text infers `double` on repark against Spark's measured
  `decimal(38,0)`-class answer — strict xfails citing `CSV-INFER-20DIGIT`.
- `secrets.py` — the `secrets` family (D-2, step 3): thirteen credential-shaped column
  names, each one tripping a distinct needle in `prop_key_is_secret`
  (`password`, `*token`, `*secret*`, `privatekey`, `access_key_id`, `access_key`,
  `connection_string`, `credential`, `user_info`, `bearer`, `apikey`), beside the ordinary
  `id`/`name`/`note` and the `bucket_key` negative control (`_key` suffix rescued by the
  `bucket` carve-out). Every flagged value carries the `repark-fake-` prefix and no
  real-looking key-id shape (`AKIA[0-9A-Z]{16}` is forbidden); `session_token` is the one
  nullable credential column (every 7th row null). `id` is int64, the rest string, so
  Parquet and CSV share one declared schema (measured: repark's CSV inference answers
  `id`→int64, all text→string, all-nullable). pins: torture-1/C-017
- `v3_dv.py` — the `v3_dv` family (D-2, D-5, step 4): a **live-Spark-only** generator —
  `generate` refuses loud naming `REPARK_PARITY_LIVE` unless it is `1`, because only Spark
  writes the Puffin deletion vectors the family exists to exercise. It builds (or adopts)
  an Iceberg-armed local session — module-private catalog `torture_v3dv`, never `local`;
  the Iceberg runtime GAV is restated here because this package cannot import the
  `python/repark/tests/` pin module — creates `<out>/ns/v3dv` as a format-v3,
  all-merge-on-read table partitioned by `part = id % 2`, seeds ids 1..rows in twelve
  contiguous insert batches (many small files), then deletes `id % 5 = seed % 5`
  (falling back to `id = rows` when a tiny row count empties the modulo set), measures
  Spark's own post-delete count, and writes `truth.json` at the table root. It returns a
  `V3DvResult`, not a `FamilyOutput`, and stays OUT of `FAMILIES`: it produces an Iceberg
  table, not a Parquet+CSV pair, and Spark's UUID file names and timestamps are not
  byte-deterministic, so the determinism pins stay scoped to the file families.
  pins: torture-1/C-024, C-029
- `data/` — the card's one committed-data exception (D-5): the ≤ 1 MB checked-in
  `v3_dv` table instance the JVM-free cells read. See [data/map.md](data/map.md).
- `__main__.py` — the `generate` CLI; `--depth`/`--width` are refused for non-nested
  families; `v3_dv` is a listed family choice with its own dispatch branch and print
  line (live rows and the table root, not the parquet/csv pair).
- `map.md` — this file.

## I want to…

| I want to… | Go to |
|---|---|
| Generate a family by hand | `python -m repark_parity.torture generate <family> --rows N --seed 7 --out DIR` |
| Generate the v3_dv table | same CLI with family `v3_dv`, under `REPARK_PARITY_LIVE=1` + a Java 17 `JAVA_HOME`; `<out>/ns/v3dv` is the table root |
| Regenerate the committed fixture | generate with `--out /tmp/repark-torture-v3dv`, copy `ns/v3dv` over [data/v3_dv/](data/v3_dv/map.md), strip `*.crc` |
| Scale the dynamicFlatten bed | `... generate nested ... --depth D --width W` |
| Run the suite | `make py-test-torture` (`TORTURE_TIER=full` for the big tier) |
| Read the door pins | [../../tests/torture/map.md](../../tests/torture/map.md) |

## Pointers

- Up: [../map.md](../map.md)
- Card: [../../../../briefs/next-sequence.md](../../../../briefs/next-sequence.md) TORTURE-1
- Registry rows this package filed:
  [../../../../docs/spark-sql-iceberg-parity.md](../../../../docs/spark-sql-iceberg-parity.md)
  `CSV-INFER-INT32-WIDTH`, `CSV-INFER-HEADER-CASE`, `SUM-DEC-I128WRAP-1`,
  `DATE-INTERVAL-NSBOUND-1` (cited, not re-filed: `CSV-INFER-20DIGIT`, `BL-14`,
  `AVG-DEC-SUMWRAP-1`, `DYNFLATTEN-LISTNULL-1`)

## Constraints

- Zero new Python dependencies (pyarrow + stdlib + the harness's pydantic; `v3_dv`
  imports pyspark lazily inside `generate`, which the `record` extra already carries).
- Data files never enter the repository: generators refuse repository-internal outputs.
  The one exception is D-5's checked-in `v3_dv` instance under `data/` — written by the
  generator outside the repo and copied in by hand, never a `--out` target.
- No comments in code (owner ruling); the reasons above live here.

## Debug

| Symptom | First check |
|---|---|
| `No module named 'repark_parity.torture'` | Import `repark_parity` first — the graft in its `__init__` exposes this directory only when it exists beside `src/` |
| `torture output must stay outside the repository` | Pass `--out` under `/tmp/` or another path outside the checkout |
| Full tier regenerates every run | The manifest in `/tmp/torture/<family>/manifest.json` must record the same family, rows, and seed |
