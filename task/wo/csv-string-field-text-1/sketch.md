# CSV-STRING-FIELD-TEXT-1 — step 0: a measured design sketch (2026-11-20)

By Claude Opus 5.5 (`claude-opus-5-5`), effort xhigh. No product code, no build. Marks: **[M]** measured, **[R]**
read, **[Rec]** recommendation. The cells: [domain-table.md](domain-table.md). The sites:
[inventory.md](inventory.md). The probes and raw output: [evidence/](evidence/map.md).

**The rulings carried.** The owner, 2026-11-20, about 00:22 UTC, verbatim: "Let's go with defaults across the board"
(the window's terms; the questions in §7 go to a Fable medium consult). The owner, 2026-11-19, about 14:00 UTC,
verbatim: "go with recommendations" (Fable's §9: N1 folds into this card; the short-row cost rule: C-012 a hard
1.05x, a row under 100 ms over 1.05x a flag attributed by swap or instruction counts, a build-attributed flag does not
block, nothing over 1.15x without the owner). Standing: Rust-first (2026-09-14), the insertion-inventory rule
(2026-11-09), and `reader.py`'s 1,000-line ceiling.

## 0. In short

1. **Spark's rule [R, M].** A CSV read parses each field's text by the declared type, per field, with its own
   converters, never `CAST`, and never under the ANSI flag (0 of 3,852 Spark cell pairs move with ANSI). A `STRING`
   field and every column of a read with no schema or `inferSchema=false` is the file's text; a misfit is NULL under
   `PERMISSIVE`.
2. **The defect [M].** RePark lets DataFusion infer each column, then `CAST`s the typed column to the declared type
   (or to `STRING`). 1,908 of 7,704 cells are WRONG and every one is silent; 2,670 more raise where Spark answers.
   The label path stores every WRONG read. Main and unit 1 are byte-equal in all 7,704.
3. **The seat [Rec].** Seat A: one all-text scan for every read that does not infer (Rust, `read_options.rs`), and a
   Rust kernel that parses text by Spark's CSV converters with NULL on misfit, wired into the facade's four cast
   sites. An emulation on unit 1's package measures the scan half: the card's three reads are EQUAL in every cell,
   and all-text + `TRY_CAST` already answers 1,336 of 1,376 schema cells. `TRY_CAST` (seat B) is not Spark's rule:
   it loses 24 EQUAL cells.
4. **It removes unit 0's second inference** (no inference at all on these reads), so the 1 ms cost line retires.
5. **Split [Rec]:** two units under the one-round cap: the kernel, then the wiring.

## 1. Spark's rule

From the jar (`javap -c -p` on `spark-catalyst_2.13-4.1.2.jar`; [inventory.md](inventory.md) §2) and from live runs
(the table in [domain-table.md](domain-table.md) §2):

- `UnivocityParser.makeConverter` per type: `INT`/`BIGINT` Java `parseInt`/`parseLong` (sign and leading zeros
  accepted; no spaces, fraction or exponent; overflow is a misfit); `DOUBLE` Java `parseDouble` after the
  `NaN`/`Inf`/`-Inf` tokens (it trims and takes `1d`); `DECIMAL` `new BigDecimal(text.replaceAll(",", ""))` set to
  the declared precision and scale (exponents parse, spaces do not); `BOOLEAN` `true`/`false` in any case, nothing
  else; `DATE`, `TIMESTAMP`, `TIMESTAMP_NTZ` the formatter with the lenient fallback (`2024-01-15 10:30:00` is a
  `DATE` of `2024-01-15`); `STRING` the text.
- `nullSafeDatum`: the `nullValue` token (default `""`) is NULL before any converter.
- A converter that throws marks the record bad. `PERMISSIVE` keeps the row with that field NULL; `DROPMALFORMED`
  drops the row; `FAILFAST` raises `FAILED_READ_FILE.NO_HINT` [M].
- No schema and no options: every column is `STRING` (`_c0`, `_c1`, …), the text. `inferSchema=true` runs
  `CSVInferSchema` (`int` before `bigint`, then `decimal`, `double`, dates and timestamps, `boolean`, `string`).

## 2. The defect and its reach [M]

| read | cells | EQUAL / WRONG / RAISE / BOTHERR | the class |
|---|---|---|---|
| user schema (S) | 4,128 | 1,590 / 1,068 / 1,470 / 0 | WRONG for a typed inference cast to the declared type (`STRING` over `007` is `7`; `INT` over `1.50` is `1`; `BOOLEAN` over `42` is `true`); RAISE for a misfit cast (`CAST` raises in both ANSI modes, STRING-CAST-LENIENT-1's class) |
| no options (N) | 556 | 264 / 216 / 76 / 0 | WRONG: the typed column cast to text; RAISE: ``csv.`path` `` has no route |
| `inferSchema=false` (F) | 480 | 264 / 216 / 0 / 0 | as N |
| `inferSchema=true` (T) | 480 | 288 / 192 / 0 / 0 | another class (§7 Q5) |
| `DROPMALFORMED`, `FAILFAST` (M) | 1,376 | 0 / 0 / 956 / 420 | RePark refuses both modes (§7 Q4) |
| the label path (L) | 684 | 320 / 216 / 148 / 0 | each WRONG read stored WRONG, by `writeTo().create()` and `INSERT INTO … SELECT` |

- **By door:** `spark.read.csv`, `format("csv").load` and a temp view over the frame answer alike in every cell.
- **By type and shape:** [domain-table.md](domain-table.md) §3. The `STRING` field is WRONG for 8 of 19 shapes
  (`007`, `1.50`, `1e3`, `1E-2`, `5.`, `TRUE`, `True`, `"007"`), alone and beside a `TIMESTAMP` field alike.
- **N1 [M]:** 24 S cells, 24 N, 24 F, WRONG on both builds, with and without a `TIMESTAMP` field: Spark answers
  (NULL, NULL), (NULL, 7), (3, 4); RePark (1000, 2), (1, 7), (3, 4).

## 3. Main against unit 1 [M]

0 of 7,704 records differ, so no verdict moves, and the seven `EXPLAIN`s of `domain.py E` are equal. A seat built on
unit 1 starts from main's cells. Unit 1 changes `reader.py` (1,000 → 987 lines) and moves
`_session_timestamp_text_columns` into `reader_support.py` (618 → 635); a seat edits both.

## 4. The seats

Both seats share the scan: a read that does not infer types (a user schema, or no schema with `inferSchema` false
or unset) reads every column as Utf8 with no inference, once. They differ in the parse.

### Seat A [Rec]: an all-text scan, and a Rust Spark-CSV field kernel

- **Product files and caps.**
  - `crates/repark-core/src/read_options.rs` (657 of 1,000): a facade-only option, never taken from the user
    (`test_csv_infer_perf_1.py` pins `utf8_columns` the same way), that sets
    `CsvReadOptions::schema_infer_max_records(0)`. **[R]** DataFusion then reads only the first chunk for the header
    and types every column Utf8 (`disable_inference`, `datafusion-datasource-csv-54.1.0/src/file_format.rs:620-624`,
    `:654-660`) on every store, local, `file://` and S3 alike. About 10 lines.
  - a new `crates/repark-functions/src/spark_csv_field.rs` (with tests; ceiling 1,000), registered in `lib.rs`
    (186 lines): Utf8 to the declared type by §1's converters, NULL on misfit. `TIMESTAMP` reuses the Spark-rule
    text kernel unit 0 already reaches (`spark_string_timestamp.rs`, the `to_timestamp` in its plans).
  - the Python-reachable builder: `crates/repark-python/src/column/mod.rs` is at 984 of 1,000, so a sibling file.
  - `reader.py` (unit 1: 987): `_load_csv` sets the option unless `inferSchema=true` and drops the re-read block
    (`:512-518`, 7 lines); the four `.cast(field["dataType"])` sites (`:722`, `:737`, `:740`, `:745`) call the
    kernel through one helper. **About 982 lines, under 1,000; no ceiling raise.**
  - `reader_support.py` (unit 1: 635): `_session_timestamp_text_columns` goes (17 lines) and the helper comes (about
    6).
- **Cells it moves [R, by construction; the scan half M].** Every S, N and F cell whose difference is the field
  parse: S 1,068 WRONG and 1,470 RAISE, N 216, F 216, and the L cells over them (216 WRONG, 148 RAISE, predicted:
  the writes store the read). It leaves N's 76 SQL-door cells, M's 1,376 and T's 192. The measured floor: the
  all-text scan with the facade's own `TRY_CAST` answers 1,336 of 1,376 S cells and all 320 N and F cells on the
  `csv` door ([domain-table.md](domain-table.md) §5); the kernel's own job is the 40 cells and the 24 of
  `kernel.py`'s 88 that `TRY_CAST` misses.
- **EQUAL cells it could lose.** None by design. The risk cells, each to be pinned: `DECIMAL` over `1e3`/`1E-2`
  (unit 0's F1: main EQUAL through the inferred `Float64`; the existing text casts NULL or raise), `DOUBLE` over
  ` 7 `, and the `DATE`/`TIMESTAMP`/`TIMESTAMP_NTZ` cells the typed scan answers today (36, 72, 72 per side), which
  move to a text parse.
- **`EXPLAIN` changes** for every CSV read that does not infer: the scan's schema is all Utf8 and the projection
  calls the kernel where it now reads `CAST(column_1 AS Int32)`; a `STRING` field and the no-schema read keep the
  text with no cast. A filter's `__repark_cast_shield__` wraps casts, so the filter's text changes too [R].
  `inferSchema=true` and JSON reads keep their plans.
- **Unit 0's second inference: removed.** No inference runs on these reads, and the re-read goes. The cost line "a CSV
  read whose schema holds a `TIMESTAMP` field pays one extra schema inference, at most 1 ms per read" retires.

### Seat B: the same scan, and the facade's `TRY_CAST`

- **Files:** `read_options.rs` as seat A (or none, if the local first-record pre-scan is kept: then a URL or S3 path
  still infers first and re-reads the named columns); `reader.py`'s four casts become `try_cast` (0 lines), the
  re-read goes. No kernel; `TRY_CAST` is already a Rust kernel, so Rust-first holds.
- **Cells [M, emulated on unit 1's package]:** S 1,336 EQUAL / 40 WRONG / 0 RAISE; N and F all EQUAL. **It loses 24
  EQUAL cells** (`DECIMAL` over `1e3` and `1E-2`, reopening unit 0's F1; `DOUBLE` over ` 7 `), and on `kernel.py`'s
  wider shapes it matches Spark in 64 of 88: `CAST`'s boolean tokens (`t`, `1`, `y`), no comma stripping for
  `DECIMAL`, no trim for `DOUBLE` or `DATE`.
- **`EXPLAIN`:** `TRY_CAST(column_1 AS Int32)`. **Unit 0's second inference:** removed, as A.
- **Verdict [Rec]:** not Spark's rule, and it loses EQUAL cells; useful only as a stopgap the owner would rate.

### Cost (design estimates)

**Not yet sampled under the exclusive lock.** The five-round series (`cost.sh`: `cost.py` per case and variant,
`flock -x`, cores 32-47) waited behind lane B's shared hold of the measure lock (a cargo build) for the whole
window. The one functional check per variant ran on unit 1's package under the shared lock, one process each, five
repetitions after a warm-up. The seat's emulation is the native all-text read (the local pre-scan) and the facade's
casts, which is seat A's scan and about seat B's parse:

| case | rows | base ms | seat ms | ratio (one sample) |
|---|---|---|---|---|
| `k0_count` (unit 0's) | `n INT, ts TIMESTAMP`, `count()` | 18.78 | 18.35 | 0.977x |
| `k0_hour` (unit 0's) | `n > 1000000`, `max(hour(ts))` | 58.03 | 57.02 | 0.983x |
| `is_count` | 2M rows `n INT, s STRING`, no `TIMESTAMP` field | 12.73 | 12.32 | 0.967x |
| `is_sum` | the same, `sum(n)`, `max(s)` | 24.04 | 25.02 | 1.041x |
| `is_none` | the same file, no schema | 26.89 | 22.60 | 0.840x |
| `is_infer` | the same file, `inferSchema=true` (the seat leaves it) | 54.01 | 54.84 | 1.015x (same code) |
| C-012 | — | — | — | untouched [R]: `replay_work.py` and its directory read no CSV |

- **[R]** `k0_count` loses the second read (the 0.86 ms Fable measured); `k0_hour` keeps the text timestamp parse,
  which TEXT-TIMESTAMP-PARSE-KERNEL-1 owns; `is_sum` pays the text-to-integer parse that replaces the typed scan's
  (the card's 1.04x); `is_none` drops the integer-to-text cast.
- **Under the short-row rule** these rows are under 100 ms, so a build row over 1.05x is a flag to attribute, not a
  miss. The build must re-measure with its own kernel, five rounds, exclusive.

## 5. What the build must also hold

- Pins, red on main and green on the build: a `STRING` field over `007`, `1.50`, `1e3`, `TRUE`, the default read and
  `inferSchema=false` (the card's three reads), N1's three rows, a misfit NULL in `PERMISSIVE`, and the label path
  (`writeTo().create()` and `INSERT INTO`), each against Spark 4.1.2's recorded answers, both ANSI modes.
- The risk cells of §4 A, and the `nullValue` token over text (`NA` then `007`: Spark NULL, `007`).
- The bar: the 7,704 cells re-run (no EQUAL lost), the emulation's 1,696, `kernel.py`'s 88, and unit 0's bar files
  (its `csv_s`/`csv_f` cells and the glob residual: the residual's spelling takes the same all-text scan).

## 6. The unit split under the one-round cap [Rec]

1. **CSV-FIELD-KERNEL-1 (Rust only).** The kernel, its registration and its Python builder; unit tests on §1's rule
   with this step's recorded Spark answers (the 19 shapes × 9 types, N1 and `kernel.py`'s 44) as the oracle. No
   reader change, so no user-visible cell moves; small and reviewable on its own.
2. **CSV-STRING-FIELD-TEXT-1 (the wiring).** The scan option, the facade's casts, the re-read's removal, the pins and
   the bar of §5, `EXPLAIN` named, the cost series.

The alternative is one unit (six product files); its risk is a single round over a kernel and a reader change at once.

## 7. Questions for the Fable medium consult

Each can be ruled from [domain-table.md](domain-table.md) alone.

1. **The parse: seat A (a Rust Spark-CSV kernel) or seat B (`TRY_CAST`)?** Options: A; B; B now and A later.
   **Default: A.** Rests on: §5 of the domain table (B: S 1,336 / 40 / 0, 24 EQUAL lost; `kernel.py` 64 of 88).
2. **The scan: `schema_infer_max_records(0)` on every path, or the local first-record pre-scan with a named re-read
   elsewhere?** **Default: `schema_infer_max_records(0)`,** with a pin on header names (an empty header name must stay
   `_c<i>`: unit 0's bar lost 2 such cells on a named-only scan). Rests on: [R] `file_format.rs:620-624`; no URL or
   S3 cell is in this table, so the build measures them.
3. **The split: two units (the kernel, then the wiring) or one?** **Default: two** (§6). Rests on: the six product
   files of seat A.
4. **`DROPMALFORMED` and `FAILFAST`:** fold them into the wiring unit (the kernel can report a misfit), or card them?
   **Default: card, S3, loud.** Rests on: M, 956 RAISE and 420 BOTHERR, all a refusal before the read.
5. **`inferSchema=true`'s gaps:** card or fold? **Default: a new card, S2, not label-blocking.** Rests on: T, 120
   type-only cells (Spark `int`, RePark `bigint`) and 72 value cells (`5.`, ` 7 `, a 20-digit integer); not a
   default read.
6. **``SELECT * FROM csv.`path` ``:** RePark has no route (76 RAISE, loud). **Default: card, S3.** Rests on: N's
   `sql` door.
7. **READER-TYPED-SCAN-1's CSV half** (malformed and overflowing text raise where Spark answers NULL): fold its CSV
   cells into this unit's bar, since the kernel moves them? **Default: fold the CSV half; the JSON half stays.**
   Rests on: S's 1,470 RAISE (for example `INT` over `2147483648` or `abc`, `DATE` over `42`).

## 8. What I did not check

- No seat was built; seat A's cells are predicted from Spark's rule and the emulation's scan half.
- No URL, `file://`, S3, glob, compressed or multi-file path; no `multiLine`, `sep`, `quote` or `escape` option.
- `FLOAT`, `SMALLINT`, `TINYINT`, interval and binary fields; non-ASCII digits; locales other than `en-US`.
- The cost series under the exclusive lock (§4, cost).

Up: [map.md](map.md).
