# CSV-STRING-FIELD-TEXT-1 — step 0: a measured design sketch (2026-11-20, revised the same day)

By Claude Opus 5.5 (`claude-opus-5-5`), effort xhigh. No product code, no build. Marks: **[M]** measured, **[R]**
read, **[Rec]** recommendation. The cells: [domain-table.md](domain-table.md). The kernel's oracle:
[oracle.md](oracle.md). The sites: [inventory.md](inventory.md). The probes and raw output:
[evidence/](evidence/map.md).

**The revision.** The critic (Claude Opus 5.5 medium) returned GAPS: it re-measured 268 cells with 0 disagreements and
kept seat A, the split and the WRONG set. This revision closes its items: the scan condition (§4), positional
pairing (Q8), the full converter list and oracle (§1, [oracle.md](oracle.md)), the scan topology (domain table §6.3),
the inventory (rows 17-22), the card candidates (§8), the file count (§4) and the cost series (§4, now exclusive).

**The rulings carried.** The owner, 2026-11-20, about 00:22 UTC, verbatim: "Let's go with defaults across the board"
(the window's terms; the questions in §7 go to a Fable medium consult). The owner, 2026-11-19, about 14:00 UTC,
verbatim: "go with recommendations" (Fable's §9: N1 folds into this card; the short-row cost rule: C-012 a hard
1.05x, a row under 100 ms over 1.05x a flag attributed by swap or instruction counts, a build-attributed flag does not
block, nothing over 1.15x without the owner). Standing: Rust-first (2026-09-14), the insertion-inventory rule
(2026-11-09), and `reader.py`'s 1,000-line ceiling.

## 0. In short

1. **Spark's rule [R, M].** A CSV read parses each field's text by the declared type with its own converters
   (§1), never `CAST`, and never under the ANSI flag (0 of 3,852 domain and 0 of 1,184 oracle pairs move with ANSI).
   A `STRING` field, and every column of a read with no schema or `inferSchema=false`, is the file's text. A misfit
   is NULL under `PERMISSIVE`. A user schema pairs with the columns **by position** and overrides `inferSchema`.
2. **The defect [M].** RePark lets DataFusion infer each column, then `CAST`s the typed column to the declared type
   (or to `STRING`), pairing a schema with header names **by name**. 1,908 of 7,704 cells are WRONG and every one is
   silent; 2,670 more raise where Spark answers. The label path stores every WRONG read. Main and unit 1 are
   byte-equal (7,704 and 5,156 records).
3. **The seat [Rec].** Seat A:
   - one all-text scan in Rust whenever a user schema is given or `inferSchema` is not true;
   - a Rust kernel that parses text by Spark's converters with NULL on misfit, whose oracle is [oracle.md](oracle.md);
   - the facade's four cast sites calling it, pairing a CSV schema by position (Q8).

   The emulation answers the card's three reads in every cell. Seat B (`TRY_CAST`) loses 24 EQUAL cells and matches
   only 984 of 1,184 oracle cells.
4. **Unit 0's second inference goes,** so its 1 ms cost line retires. Measured, five exclusive rounds: the seat's
   emulation runs `k0_count` at 0.884x and `k0_hour` at 0.927x of unit 1. One row, `is_sum`, reads 1.055x and is a
   flag: it prices arrow's `CAST`, not the kernel.
5. **Split [Rec]:** two units under the one-round cap: the kernel (with the oracle), then the wiring (with the scan
   and positional pairing).

## 1. Spark's rule

From the jar (`javap -c -p` on `spark-catalyst_2.13-4.1.2.jar`; the critic's `javap` agrees) and from live runs:

- **Converters** (`UnivocityParser.makeConverter`, in full in [oracle.md](oracle.md)):
  - `TINYINT`/`SMALLINT`/`INT`/`BIGINT`: Java's radix-10 parse. It takes a sign and leading zeros; a space, tab,
    `_`, `0x`, fraction or exponent is a misfit, and so is overflow.
  - `FLOAT`/`DOUBLE`: the `NaN`, `Inf`, `-Inf` tokens, then Java's `parseFloat`/`parseDouble`. It trims and takes
    `Infinity`, `1d`, `1f` and `0x1p3`; `-0` keeps its sign; `inf`, `nan` and `+Inf` are NULL.
  - `DECIMAL`: `new BigDecimal(text.replaceAll(",", ""))`, HALF_UP to the scale. A precision overflow is NULL, an
    exponent parses, a space does not.
  - `BOOLEAN`: `true`/`false` in any case, nothing else.
  - `DATE`: the formatter, then the `stringToDate` fallback (`2024` is `2024-01-01`; a time suffix is dropped).
  - `TIMESTAMP`: the formatter, then `stringToTimestamp(s, sessionZone)`.
  - `TIMESTAMP_NTZ`: `parseWithoutTimeZone`. It never uses the session zone; text with an offset is NULL.
  - `STRING`: the text. `BINARY`: its bytes.
  - Intervals and `VARIANT` have converters of their own.
- **`nullSafeDatum`** makes the `nullValue` token (default `""`) NULL before any converter runs.
- **A misfit:** `PERMISSIVE` makes that field NULL, `DROPMALFORMED` drops the row, and `FAILFAST` raises
  `FAILED_READ_FILE.NO_HINT` [M].
- **The reads:**
  - No schema and no options: every column is `STRING` (`_c0`, …), holding the text.
  - `inferSchema=true` runs `CSVInferSchema`, **unless a schema is given, which wins** (domain table §6.1).
  - A schema pairs by position under `enforceSchema=true`, the default (§6.2). The header names only skip the header
    line.

## 2. The defect and its reach [M]

| read | cells | EQUAL / WRONG / RAISE / BOTHERR | the class |
|---|---|---|---|
| user schema (S) | 4,128 | 1,590 / 1,068 / 1,470 / 0 | **WRONG:** a typed inference cast to the declared type (`STRING` over `007` is `7`; `INT` over `1.50` is `1`; `BOOLEAN` over `42` is `true`). **RAISE:** a misfit cast (`CAST` raises in both ANSI modes, STRING-CAST-LENIENT-1's class) |
| no options (N) | 556 | 264 / 216 / 76 / 0 | **WRONG:** the typed column cast to text. **RAISE:** ``csv.`path` `` has no route |
| `inferSchema=false` (F) | 480 | 264 / 216 / 0 / 0 | as N |
| `inferSchema=true` (T) | 480 | 288 / 192 / 0 / 0 | another class (Q5) |
| `DROPMALFORMED`, `FAILFAST` (M) | 1,376 | 0 / 0 / 956 / 420 | RePark refuses both modes (Q4) |
| the label path (L) | 684 | 320 / 216 / 148 / 0 | each WRONG read is stored WRONG |
| revision cells (domain table §6) | 5,156 records | the pairing, scan-condition, topology and tokenizer cells; the oracle | below and §8 |

N1 is WRONG in 72 cells on both builds, with and without a `TIMESTAMP` field.

## 3. Main against unit 1 [M]

0 of 7,704 domain records and 0 of 5,156 revision records differ; the seven `EXPLAIN`s are equal. Unit 1 changes
`reader.py` (1,000 → 987 lines) and moves `_session_timestamp_text_columns` into `reader_support.py` (618 → 635).

## 4. The seats

### Seat A [Rec]: an all-text scan, a Rust Spark-CSV field kernel, positional pairing

- **The scan condition (corrected).** The scan applies **whenever a user schema is given, or whenever `inferSchema` is
  not true**. A user schema overrides `inferSchema=true` on Spark: `x STRING` under `inferSchema=true` answers `007`
  (domain table §6.1). An `inferSchema=true` read with no schema keeps today's inference and
  `_finish_csv_infer_schema`.
- **The scan.** A facade-only option in `read_options.rs`, never taken from the user (as `utf8_columns` is pinned),
  sets `CsvReadOptions::schema_infer_max_records(0)`.
  - [R] DataFusion then reads the first listed object's header alone and types every column Utf8
    (`file_format.rs:401-404`, `:620-624`).
  - [M] For one file, emulated through the `file://` route: the URL, `gzip`, `sep` and a BOM answer Spark's text
    (8 of 8 cells). An empty header name stays `''` where Spark says `_c0`, which must be pinned. Untrimmed names
    raise as on unit 1, and must be pinned too.
  - [R] Several files: the first-listed object names the columns and each file reads positionally. Local listing
    is readdir order and S3's is lexicographic; Spark takes the largest file. A glob of same-header files stops
    raising `Fail to merge schema`.
  - **S3 is reading only; the build's verifier must add moto cells** (inventory row 22).
- **The kernel.** A new `crates/repark-functions/src/spark_csv_field.rs` (with tests; ceiling 1,000), registered in
  `lib.rs` (186 lines). It parses Utf8 to the declared type by §1's converters, NULL on misfit, and is tested
  against [oracle.md](oracle.md).
  - `TIMESTAMP` may reuse the Spark-rule text kernel unit 0 reaches (`spark_string_timestamp.rs`).
  - **`TIMESTAMP_NTZ` must never go through the session zone:** `TRY_CAST` to NTZ moves the `2024-03-10 02:30:00`
    gap an hour in Los Angeles and New York, F2's 4 cells ([oracle.md](oracle.md), zones).
  - Intervals and `VARIANT` refuse by name.
- **Positional pairing (Q8, default: in this seat).** The four cast sites pair a CSV schema with the columns by
  position, as `enforceSchema=true` does. It must be CSV-only, because the same function serves JSON, which pairs by
  name. It moves the 9 WRONG pairing cells (domain table §6.2) and the two-file schema read.
- **Product files: seven touched, two of them new.**
  - **Kernel unit (4):** `spark_csv_field.rs` (new), `repark-functions/src/lib.rs`, a new sibling of
    `crates/repark-python/src/column/mod.rs` for the Python builder, and one `mod` line in `column/mod.rs`
    (984 → 985 of 1,000).
  - **Wiring unit (3):**
    - `read_options.rs` (657 of 1,000, about +10).
    - `reader.py` (unit 1: 987): the re-read block goes (`:512-518`, 7 lines); the four sites (`:722`, `:737`,
      `:740`, `:745`) call the kernel through one helper; the name-pairing branches collapse for CSV. **Under 1,000;
      no ceiling raise.**
    - `reader_support.py` (unit 1: 635): `_session_timestamp_text_columns` goes (17 lines) and the helper comes
      (about 6).
- **Cells it moves [R, by construction; scan half M].**
  - Every S, N and F cell whose difference is the field parse: S 1,068 WRONG and 1,470 RAISE, N 216, F 216, and the
    L cells over them.
  - The pairing cells.
  - The oracle's 825 unit-1 misses (ANSI on).
  - It leaves N's 76 SQL-door cells, M's 1,376, T's 192 and the §6.4 tokenizer cells.
- **EQUAL cells it could lose; each must be pinned.** By design, none. The risk cells:
  - `DECIMAL` over `1e3`/`1E-2` (unit 0's F1) and HALF_UP rounding (`1.555`);
  - `DOUBLE` over ` 7 `, `-Inf`, `Infinity`, `1d`, `1f`;
  - the `DATE`/`TIMESTAMP`/`TIMESTAMP_NTZ` cells the typed scan answers today;
  - F2's 4 NTZ gap cells;
  - the non-UTC zone cells (126 of 168 EQUAL on unit 1 today);
  - the header-name cells the pre-scan path answers (an empty name `_c<i>`).
- **`EXPLAIN` changes** for every CSV read the condition covers: an all-Utf8 scan, the kernel in place of `CAST`, and
  a filter's `__repark_cast_shield__` text with it. Reads with `inferSchema=true` and no schema keep their plans, as
  do JSON reads.
- **Unit 0's second inference: removed.** The cost line "a CSV read whose schema holds a `TIMESTAMP` field pays one
  extra schema inference, at most 1 ms per read" retires.

### Seat B: the same scan, the facade's `TRY_CAST`

- **Files:** `read_options.rs` and `reader.py`, with no kernel. `TRY_CAST` is already a Rust kernel.
- **Cells [M, emulated on unit 1]:**
  - S 1,336 EQUAL / 40 WRONG / 0 RAISE, and N and F all EQUAL.
  - **24 EQUAL cells lost:** F1's `DECIMAL` exponents, and `DOUBLE` over ` 7 `.
  - The oracle: 984 of 1,184 EQUAL.
  - It moves F2's NTZ gap.
- **Verdict [Rec]:** not Spark's rule. Rejected, and the critic agrees.

### Cost (design estimates) [M]

Five rounds of `cost.sh`, each sample alone under `flock -x <measure lock> nice -n 10 taskset -c 32-47`, one process,
a warm-up and five timed repetitions, the order alternating by round. The seat's emulation is the native all-text
read and the facade's casts on the same package. Medians in ms:

| case | main base | main seat | unit 1 base | unit 1 seat | seat / base on unit 1 | unit 1 / main base |
|---|---|---|---|---|---|---|
| `k0_count` (unit 0's) | 17.76 | 16.10 | 18.36 | 16.23 | **0.884x** | 1.034x |
| `k0_hour` (unit 0's) | 56.96 | 53.98 | 61.56 | 57.06 | **0.927x** | 1.081x |
| `is_count` (2M rows `n INT, s STRING`, no `TIMESTAMP`) | 12.25 | 12.02 | 12.35 | 12.20 | **0.988x** | 1.008x |
| `is_sum` (the same, `sum(n)`, `max(s)`) | 22.68 | 23.20 | 23.52 | 24.81 | **1.055x** (flag) | 1.037x |
| `is_none` (the same file, no schema) | 26.38 | 20.27 | 26.32 | 22.25 | **0.845x** | 0.998x |
| `is_infer` (`inferSchema=true`; the seat leaves it) | 52.74 | — | 54.89 | — | untouched | 1.041x |
| C-012 | — | — | — | — | untouched [R]: `replay_work.py` and its directory read no CSV | — |

- **The seat's gains.** `k0_count` and `k0_hour` lose the second inference and read; `is_none` drops the
  integer-to-text cast.
- **`is_sum` is a short-row flag** (under 100 ms, over 1.05x, under 1.15x). The text-to-integer parse replaces the
  typed scan's, priced here by **arrow's `CAST`, not the kernel**. **The build must measure its own kernel,
  exclusively, five rounds**, and attribute any row over 1.05x by swap or instruction counts.
- **Unit 1's own rows:** the last column is unit 1's package against main, not this seat. `k0_hour`'s 1.081x has a
  wide spread (unit 1 base 56.87 to 68.12 ms), noted for unit 1's records.
- Raw: [evidence/raw/](evidence/raw/map.md) (`cost-samples.jsonl`, `cost-table.md.txt`).

## 5. What the build must also hold

- **Pins,** red on main and green on the build, against Spark 4.1.2's recorded answers, both ANSI modes:
  - a `STRING` field over `007`, `1.50`, `1e3`, `TRUE`, the default read and `inferSchema=false`;
  - a schema with `inferSchema=true` (§6.1);
  - N1;
  - a misfit NULL;
  - the pairing cells (§6.2);
  - the label path.
- **The kernel unit's tests:** every cell of [oracle.md](oracle.md); a year-32768 or year-40000 value and the
  year-month intervals are checked through `CAST(x AS STRING)`.
- **Unit 0's bar, named:** F1's `DECIMAL` exponents, and F2's `TIMESTAMP_NTZ` gap cells in Los Angeles and New York.
  Also the non-UTC zone cells, its `csv_s`/`csv_f` cells, and the glob residual (whose spelling takes the same scan).
- **The scan topology:**
  - `file://`;
  - S3 with moto: one file, a prefix of several, a glob;
  - a glob, and two files with headers in different orders;
  - `.csv.gz` with `compression`, and `sep`;
  - an empty header name (`_c<i>`), untrimmed names, a BOM.
- **The bar:** the 7,704 cells re-run with no EQUAL lost, the emulation's 1,696, `kernel.py`'s 88, the oracle's
  2,368 and the revision cells.

## 6. The unit split under the one-round cap [Rec]

1. **CSV-FIELD-KERNEL-1 (Rust only, 4 files).** The kernel, its registration and its Python builder, tested on
   [oracle.md](oracle.md) and the NTZ zone cells. No reader change, so no user-visible cell moves.
2. **CSV-STRING-FIELD-TEXT-1, the wiring (3 files).** The scan option under the corrected condition, the facade's
   casts, positional pairing for CSV (Q8), removing the re-read, the pins and bar of §5, `EXPLAIN` named, and the
   cost series on the built kernel.

The alternative is one unit of seven files, a single round over a kernel and a reader change at once. The oracle is
complete for every declared type, so the wiring unit should never need a kernel delta.

## 7. Questions for the Fable medium consult

Each can be ruled from [domain-table.md](domain-table.md) and [oracle.md](oracle.md) alone. The scan condition is a
correction (§4), not a question.

1. **The parse: seat A (a Rust Spark-CSV kernel) or seat B (`TRY_CAST`)?** Options: A; B; B now and A later.
   **Default: A.**
   - *Cells:* domain table §5 (B: S 1,336 / 40 / 0, 24 EQUAL lost); the oracle (B: 984 of 1,184); F2's 4 NTZ cells.
2. **The scan: `schema_infer_max_records(0)` on every path, or the local first-record pre-scan with a named re-read
   elsewhere?** **Default: `schema_infer_max_records(0)`.** Pins: an empty header name becomes `_c<i>`, untrimmed
   names, and the first-object header rule. S3 moto cells in the build.
   - *Cells:* §6.3 (8 of 8 single-file cells EQUAL; `emptyname_none` WRONG; the untrimmed names RAISE); inventory
     rows 20-22.
3. **The split: two units (kernel, then wiring) or one?** **Default: two** (§6).
   - *Cells:* the seven files of §4 A; the oracle's 2,368 cells as the kernel unit's own bar.
4. **`DROPMALFORMED` and `FAILFAST`:** fold them into the wiring unit (the kernel can report a misfit), or card them?
   **Default: card, S3, loud.**
   - *Cells:* M, 956 RAISE and 420 BOTHERR, all a refusal before the read.
5. **`inferSchema=true`'s gaps:** card or fold, and do they block the label? Options:
   - (a) an S2 card, with the label text excluding CSV `inferSchema=true` reads;
   - (b) S1, label-blocking;
   - (c) fold them into this unit.

   **Default: (a).** I found no such exclusion in the label's text today (`docs/spark-sql-iceberg-parity.md`,
   `task/wo/ice-v2-flat-aws-label-1/design/`), so (a) needs that line. Without it, the critic's test makes the gaps
   label-blocking: a 20-digit integer read as `double` (`1e+20`) loses digits silently.
   - *Cells:* T, 192: 120 type-only (`int` against `bigint`) and 72 values (`5.`, ` 7 `, 20 digits).
6. **``SELECT * FROM csv.`path` `` and `CREATE TEMPORARY VIEW … USING csv`:** RePark has no route for either (loud).
   **Default: one card, S3.**
   - *Cells:* N's `sql` door, 76; §6.4's two `USING csv` cells.
7. **READER-TYPED-SCAN-1's CSV half** (malformed and overflowing text raise where Spark answers NULL): fold its CSV
   cells into this unit's bar? **Default: fold the CSV half; the JSON half stays.**
   - *Cells:* S's 1,470 RAISE (for example `INT` over `2147483648` or `abc`, `DATE` over `42`); the oracle's RAISE
     cells on unit 1.
8. **Positional pairing.** Spark pairs a CSV schema with the columns by position (`enforceSchema=true`); the facade
   pairs by name. Silent and stored; S1 by class and label-blocking by the test that made this card so. Options:
   - (a) fold into the wiring unit, which rewrites the same four sites, CSV only;
   - (b) a unit of its own after the wiring;
   - (c) a card.

   **Default: (a).**
   - *Cells:* §6.2, 9 WRONG that seat A as sketched keeps (`swap_int`, `swap_string`, `part_int`, `part_string` in
     both modes, and `emptyname_schema`); 5 EQUAL controls; the two-file schema read of §6.3 [R].

## 8. Card candidates, with proposed ratings (not filed here)

| candidate | cells | rating | label-blocking |
|---|---|---|---|
| the tokenizer's defaults: Spark's `escape` is `\` and `""` stays raw | §6.4: `"a\"b"` (Spark `a"b`, RePark `a\b"`), `"a""b"` (Spark `"a""b"`, RePark `a"b`) | S2, silent | no |
| `emptyValue` dropped; quoted empty text under a `nullValue` | §6.4: `emptyValue='E'` over `""` (Spark `E`, RePark NULL); `nullValue='NA'` over `""` (Spark `''`, RePark NULL) | S2, silent | no |
| Q5, `inferSchema=true` | T, 192 | S2, silent | only if the label does not exclude these reads |
| Q6, ``csv.`path` `` and `USING csv` | 76 + 2 | S3, loud | no |
| Q4, the two modes | 1,376 | S3, loud | no |
| header names with spaces or duplicates | §6.3's untrimmed names RAISE; the critic's duplicate-name cell | S3, loud | no |
| short rows, and several files of different widths | the critic's cells | S3, loud | no (may sit with READER-TYPED-SCAN-1's D10/D11) |

## 9. What I did not check

- No seat was built. Seat A's cells are predicted from Spark's rule, the oracle and the emulation's scan half.
- S3 (by the brief's rule, no external call), a real multi-file `schema_infer_max_records(0)` read, `multiLine`,
  `quote` and `escape` options set by the user.
- Locales other than `en-US`, non-ASCII digits, and UDT fields.

Up: [map.md](map.md).
