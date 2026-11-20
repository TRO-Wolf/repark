# CSV-STRING-FIELD-TEXT-1 — the domain table, measured (step 0, 2026-11-20)

**Builds.** Main `6d3739ab`, package `S/ddu-work/pkg-rp1base-8bc26cba` (its product tree equals `8bc26cba`'s).
Unit 1's head (PR #1373), package `S/ddu-work/pkg-rp1d-acd27d6e`. Spark 4.1.2 live, the Iceberg 1.11.0 runtime jar,
`local[1]`. Every RePark run printed `repark._native.__file__` from its package. Session zone UTC, `TZ=UTC` in
both processes. Raw cells and verdicts: [evidence/raw/](evidence/raw/map.md); probes:
[evidence/probes/](evidence/probes/map.md).

**Verdicts.** Against Spark: **EQUAL** (rows and types equal), **WRONG** (RePark answers and the rows or types
differ, or RePark answers where Spark raises: **silent**), **RAISE** (RePark raises where Spark answers: loud),
**BOTHERR** (both raise). Column names were compared too: no cell where both answer differs by name.

## 1. Size and the headline

**7,704 cells per build** (`domain.py`), each read collected whole:

| set | what | cells |
|---|---|---|
| S | a user schema `x <type>, <companion>`: 9 types × 19 shapes, and N1's `a INT, b BIGINT`; header on and off; alone (a `k STRING` companion) and beside a `TIMESTAMP` field; doors `spark.read.csv`, `format("csv").load`, a temp view over the frame | 4,128 |
| N | no schema and no options (header on is `header=true` only); the three doors and ``SELECT * FROM csv.`path` `` (header off only) | 556 |
| F | `inferSchema=false` | 480 |
| T | `inferSchema=true` | 480 |
| M | the S reads (`spark.read.csv`, header off) under `DROPMALFORMED` and `FAILFAST` | 1,376 |
| L | the label path: nine reads (`S` with `STRING`, `INT`, `DECIMAL(10,2)`, `DOUBLE`, `BOOLEAN`, `DATE`; `STRING` beside a `TIMESTAMP` field; N; F), header on, 19 shapes, written by `writeTo().tableProperty("format-version","2").create()` and by `CREATE TABLE … USING iceberg` + `INSERT INTO … SELECT`, read back | 684 |

Each set covers both ANSI modes, half each. The shapes are `42` (`plain`), `007`, `+5`, `1.50`, `1e3`, `1E-2`, `5.`,
`TRUE`, `True`, `yes`, ` 7 `, an empty field, the `nullValue` token (`NA` then `007`, with `nullValue=NA`), `"007"`
quoted, `abc`, `2147483648`, `99999999999999999999`, `2024-01-15` and `2024-01-15 10:30:00`; N1's rows are
`1e3,2.0` / `1.5,7` / `3,4`.

| set | main EQUAL / WRONG / RAISE / BOTHERR | unit 1 |
|---|---|---|
| S | 1,590 / 1,068 / 1,470 / 0 | the same |
| N | 264 / 216 / 76 / 0 | the same |
| F | 264 / 216 / 0 / 0 | the same |
| T | 288 / 192 / 0 / 0 | the same |
| M | 0 / 0 / 956 / 420 | the same |
| L | 320 / 216 / 148 / 0 | the same |
| **all** | **2,726 / 1,908 / 2,670 / 420** | **the same** |

**Main against unit 1 [M]: 0 of 7,704 records differ** (after the scratch paths are normalised), so 0 verdicts
move. Unit 1's `_reader_parse` wrapper and its helper move change no CSV answer, and its `EXPLAIN` text for the
seven plans of `domain.py E` equals main's.

**The card's finding holds, wider [M].** Every one of the 1,908 WRONG cells is silent. The three reads of the card
answer `7`, `1.5`, `1000.0`, `true` on both builds where Spark answers `007`, `1.50`, `1e3`, `TRUE`, in every door
and both ANSI modes, and the label path stores those values.

## 2. Spark's rule, live [M] (and the jar, [inventory.md](inventory.md) §2)

`spark.read.csv`, header off, alone. **0 of 3,852 Spark cell pairs differ between ANSI on and off**: the CSV parse
never reads the ANSI flag.

| shape | STRING | INT | BIGINT | DECIMAL(10,2) | DOUBLE | BOOLEAN | DATE | TIMESTAMP | TIMESTAMP_NTZ |
|---|---|---|---|---|---|---|---|---|---|
| `42` | 42 | 42 | 42 | 42.00 | 42.0 | NULL | NULL | NULL | NULL |
| `007` | 007 | 7 | 7 | 7.00 | 7.0 | NULL | NULL | NULL | NULL |
| `+5` | +5 | 5 | 5 | 5.00 | 5.0 | NULL | NULL | NULL | NULL |
| `1.50` | 1.50 | NULL | NULL | 1.50 | 1.5 | NULL | NULL | NULL | NULL |
| `1e3` | 1e3 | NULL | NULL | 1000.00 | 1000.0 | NULL | NULL | NULL | NULL |
| `1E-2` | 1E-2 | NULL | NULL | 0.01 | 0.01 | NULL | NULL | NULL | NULL |
| `5.` | 5. | NULL | NULL | 5.00 | 5.0 | NULL | NULL | NULL | NULL |
| `TRUE`, `True` | the text | NULL | NULL | NULL | NULL | true | NULL | NULL | NULL |
| `yes` | yes | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL |
| ` 7 ` | ` 7 ` | NULL | NULL | NULL | 7.0 | NULL | NULL | NULL | NULL |
| empty | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL |
| `NA` / `007` (`nullValue=NA`) | NULL / 007 | NULL / 7 | NULL / 7 | NULL / 7.00 | NULL / 7.0 | NULL | NULL | NULL | NULL |
| `"007"` | 007 | 7 | 7 | 7.00 | 7.0 | NULL | NULL | NULL | NULL |
| `abc` | abc | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL |
| `2147483648` | the text | NULL | 2147483648 | NULL | 2147483648.0 | NULL | NULL | NULL | NULL |
| `99999999999999999999` | the text | NULL | NULL | NULL | 1e+20 | NULL | NULL | NULL | NULL |
| `2024-01-15` | the text | NULL | NULL | NULL | NULL | NULL | 2024-01-15 | 2024-01-15 00:00:00 | 2024-01-15 00:00:00 |
| `2024-01-15 10:30:00` | the text | NULL | NULL | NULL | NULL | NULL | 2024-01-15 | 2024-01-15 10:30:00 | 2024-01-15 10:30:00 |
| N1 `a INT, b BIGINT` | — | (NULL, NULL), (NULL, 7), (3, 4) | | | | | | | |

- **A `STRING` field is the file's text,** and so is every column of a read with no schema (`_c0`, `_c1`, all
  `string`) or with `inferSchema=false`. Only an empty field (the default `nullValue`, `""`) is NULL.
- **A misfit is NULL** under `PERMISSIVE`, for the field alone: the row keeps its other fields.
- **`DROPMALFORMED`** drops the row (`abc` in an `INT` answers no row; N1 keeps only `(3, 4)`). **`FAILFAST`** raises
  `FAILED_READ_FILE.NO_HINT` on the first misfit and answers the rest (an empty field is not a misfit).
- **`inferSchema=true`** infers `int` before `bigint`, a decimal for `5.` (`decimal(1,0)`) and for a 20-digit integer
  (`decimal(20,0)`), and `double` for ` 7 `.

## 3. The WRONG set [M] (main; unit 1 is identical)

### By read and door

| read | csv WRONG / RAISE | load | view | `csv.`path`` |
|---|---|---|---|---|
| user schema (S) | 356 / 490 | 356 / 490 | 356 / 490 | — (no schema door) |
| no options (N) | 72 / 0 | 72 / 0 | 72 / 0 | 0 / **76** (no route: `table 'datafusion.csv./…' not found`) |
| `inferSchema=false` (F) | 72 / 0 | 72 / 0 | 72 / 0 | — |
| `inferSchema=true` (T) | 64 / 0 | 64 / 0 | 64 / 0 | — |

The three frame doors answer alike in every cell, header on and off alike.

### By declared type (S, all doors, both modes; EQUAL / WRONG / RAISE)

| type | alone | beside a `TIMESTAMP` field |
|---|---|---|
| `STRING` | 132 / **96** / 0 | 132 / **96** / 0 |
| `INT` | 72 / 84 / 72 | 72 / 72 / 84 |
| `BIGINT` | 84 / 84 / 60 | 84 / 72 / 72 |
| `DECIMAL(10,2)` | 120 / 36 / 72 | 120 / 36 / 72 |
| `DOUBLE` | 168 / 36 / 24 | 174 / 24 / 30 |
| `BOOLEAN` | 36 / 108 / 84 | 36 / 108 / 84 |
| `DATE` | 36 / 0 / 192 | 36 / 0 / 192 |
| `TIMESTAMP` | 72 / 96 / 60 | 72 / 96 / 60 |
| `TIMESTAMP_NTZ` | 72 / 0 / 156 | 72 / 0 / 156 |
| N1 | 0 / **12** / 0 | 0 / **12** / 0 |

**A `STRING` field answers the same alone and beside a `TIMESTAMP` field in every one of the 19 shapes:** WRONG for
`007`, `1.50`, `1e3`, `1E-2`, `5.`, `TRUE`, `True`, `"007"` (8 shapes × 12 cells each side); EQUAL for the other 11,
including `2024-01-15 10:30:00`, whose cast back to text round-trips. The side gain the card names is not visible
in these shapes (§5).

### By shape and read (WRONG cells; all doors, both modes)

| shape | S | N | F | T | L |
|---|---|---|---|---|---|
| `42` | 48 | 0 | 0 | 24 | 4 |
| `007` | 72 | 24 | 24 | 24 | 20 |
| `+5` | 0 | 0 | 0 | 24 | 0 |
| `1.50`, `1e3`, `1E-2` | 120 each | 24 each | 24 each | 0 | 24 each |
| `5.` | 120 | 24 | 24 | 24 | 24 |
| `TRUE`, `True` | 120 each | 24 each | 24 each | 0 | 28 each |
| `yes` | 24 | 0 | 0 | 0 | 4 |
| ` 7 ` | 24 | 0 | 0 | 24 | 4 |
| `nullValue` | 0 | 0 | 0 | 24 | 0 |
| `"007"` | 72 | 24 | 24 | 24 | 20 |
| `2147483648` | 48 | 0 | 0 | 0 | 4 |
| `99999999999999999999` | 0 | 0 | 0 | 24 | 0 |
| `2024-01-15 10:30:00` | 36 | 0 | 0 | 0 | 8 |
| N1 | 24 | 24 | 24 | 0 | 0 |

The worst cells, each silent: a `STRING` field and the default read rewrite `007`, `1.50`, `1e3`, `TRUE`; `INT`
over `1.50` answers `1` and N1's `1e3` answers `1000`; `BOOLEAN` over `42` answers `true`; `TIMESTAMP` over `42`
answers `1970-01-01 00:00:42`; `DOUBLE` over a timestamp text answers its epoch second.

### The loud half (RAISE, S and N)

Main's raise texts: `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION` (240) and `CAST_WITH_FUNC_SUGGESTION` (144), `CAST_INVALID_INPUT` (174, ANSI on), `Unsupported CAST
from Float64 to Date` (96), Arrow's `Cannot cast string … to value of Int32` (ANSI on **and off**: STRING-CAST-LENIENT-1's
class, measured by `castrule.py`), and the 76 SQL-door planning errors. The S set raises 822 cells under ANSI on and
648 under ANSI off; Spark answers NULL in all of them.

### By ANSI mode

| set | ANSI on WRONG / RAISE | ANSI off WRONG / RAISE |
|---|---|---|
| S | 534 / 822 | 534 / 648 |
| N, F | 108 / 38, 108 / 0 | the same |
| T | 96 / 0 | 96 / 0 |
| L | 108 / 76 | 108 / 72 |
| M | 0 / 478 | 0 / 478 |

### The modes (M)

RePark refuses both modes before it reads: `reader option mode='DROPMALFORMED' is not supported by repark yet`
(688) and `mode='FAILFAST'` (688; 420 of them BOTHERR, where Spark's `FAILFAST` also raises). Loud; not this card's
defect, but a seat that parses each field by Spark's rule can carry a misfit flag (sketch Q4).

### `inferSchema=true` (T): a separate class

192 WRONG, none in this card's reads: 120 are type only (Spark `int`, RePark `bigint` for `42`, `007`, `+5`,
`"007"`, the `nullValue` file), and 72 are values (`5.`: Spark `decimal(1,0)` 5, RePark `double` 5.0; ` 7 `: Spark
`double` 7.0, RePark the text; the 20-digit integer: Spark `decimal(20,0)`, RePark `double`). Sketch Q5.

## 4. The label path (L) [M]

| read written | EQUAL / WRONG / RAISE (19 shapes × 2 writes × 2 modes) |
|---|---|
| user schema `STRING`, alone | 44 / **32** / 0 per side (beside a `TIMESTAMP` field the same) |
| user schema `INT` | 24 / 28 / 24 |
| `DECIMAL(10,2)` | 40 / 12 / 24 |
| `DOUBLE` | 56 / 12 / 8 |
| `BOOLEAN` | 12 / 36 / 28 |
| `DATE` | 12 / 0 / 64 |
| no options | 44 / **32** / 0 |
| `inferSchema=false` | 44 / **32** / 0 |

`writeTo().create()` and `INSERT INTO … SELECT` answer alike. **Every WRONG read is stored WRONG** (a table over
`007` holds `7`), and the RAISE cells raise at the write (`write: … Cannot cast string 'abc' to value of Int32`).

## 5. What a seat can reach: two emulations on unit 1's package [M]

`seat_emu.py` builds the seat's frame on the existing package: the native read with `utf8_columns` (on a local path
this takes the all-text first-record pre-scan, so no inference and one scan), then either the facade's own casts
(`text_cast`) or `TRY_CAST` by the declared type (`text_try`). The `csv` door, both headers, both companions, both
modes (1,696 cells):

| set | unit 1 EQUAL / WRONG / RAISE | all-text + `CAST` | all-text + `TRY_CAST` |
|---|---|---|---|
| S (1,376) | 530 / 356 / 490 | 672 / 16 / 688 | **1,336 / 40 / 0** |
| N (160) | 88 / 72 / 0 | 160 / 0 / 0 | 160 / 0 / 0 |
| F (160) | 88 / 72 / 0 | 160 / 0 / 0 | 160 / 0 / 0 |
| EQUAL lost against unit 1 | — | 16 (`DECIMAL` over `1e3`, `1E-2`: unit 0's F1) | **24** (the same 16, and `DOUBLE` over ` 7 `) |

- **The all-text scan alone fixes the card's three reads:** every N and F cell, and every `STRING` field.
- **`TRY_CAST` is not Spark's CSV rule.** Its 40 WRONG are five pairs, each in all 8 header × companion × mode
  cells: `BOOLEAN` over `yes` (`true`, Spark NULL), `DECIMAL` over `1e3` and `1E-2` (NULL, Spark `1000.00`,
  `0.01`), `DECIMAL` over ` 7 ` (`7.00`, Spark NULL), `DOUBLE` over ` 7 ` (NULL, Spark `7.0`).
- **Wider shapes** (`kernel.py`, 44 texts × 2 modes, Spark live): unit 1 matches Spark in 49 of 88, all-text +
  `TRY_CAST` in 64 of 88. `TRY_CAST` still differs on `BOOLEAN` over `t`, `1`, `0`, `y`, ` true`, `false `
  (`CAST`'s tokens; Spark's CSV accepts only `true`/`false`, any case, no spaces), `DOUBLE` over `1d` and ` 1e3`
  (Java's `parseDouble` takes a suffix and trims), `DECIMAL` over `1,000.5` (Spark strips the comma: `1000.50`),
  `DATE` over `20240115` (Spark NULL) and ` 2024-01-15` (Spark trims), `TIMESTAMP_NTZ` over
  `2024-01-15 10:30:00+02:00` (Spark NULL). [evidence/raw/kernel-summary.txt](evidence/raw/kernel-summary.txt).

**Why the side gain is not visible [R, M].** Unit 0's final re-read names only the scan's zone-less timestamp
columns ([inventory.md](inventory.md) §3), so a `STRING` field keeps the typed scan unless its own text has a
timestamp shape, and `2024-01-15 10:30:00` round-trips through the cast. The bar's delta 1 records the shapes where
it still shows (ISO `T` text).

Up: [map.md](map.md).
