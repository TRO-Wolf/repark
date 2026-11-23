# Card STRING-CAST-LENIENT-1: string casts refuse loudly where Spark answers, on CAST, on the write statements and on the readers

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), on the owner's rulings of 2026-11-16. **Sources:** the 2026-11-16 string-cast sizing report, sections 2 and 5 (held outside the repository, so its raw cells are not committed), and question 3 of the owner questions in the step 0 sketch of CAST-LITERAL-UNWRAP-1, [sketch.md](../../wo/cast-literal-unwrap-1/sketch.md).

**Status:** ruled 2026-11-16; the INT-family slice is fixed on branch `fix/string-cast-lenient-int-1` (2026-11-22, not merged; see the status section at the end), the other targets are not dispatched. **Not label-blocking.** The owner, 2026-11-16, about 17:05, verbatim: "go with your recommendation, merge 1343", on question 3. The recommendation it adopts: the loud refusals do not block the label, and they are carded S2. The label text lists them as exclusions until the shared build lands. The shared build is [STRING-CAST-SILENT-1](string-cast-silent-1-card-2026-11-16.md)'s build, queued at the end of the blocker list. If it lands before the label, the exclusions drop.

**Severity: S2, a loud refusal on an ordinary cast, not silent.**

## What refuses loudly

Report section 5, "Which classes refuse loudly?"

- **ANSI off.** Whitespace, fractions, exponents, specials and junk to the INT family (5 + 6 + 4 + about 7 texts, times 4 types); exponents and fullwidth digits to DECIMAL; most junk to BOOLEAN; DATE and NTZ trim, pad and year forms. Each one aborts the enclosing INSERT, MERGE or writeTo statement, and the whole CSV or JSON read.
- **ANSI on.** The same classes, minus fractions to INT, where both engines refuse. The dense grid counts 43 CAST cells that refuse where Spark answers. Every write statement on all 12 targets refuses on both engines; the first bad text differs, and the stored rows agree.

## Ingestion, ANSI off

Report section 2, the sample of eight texts into an Iceberg v2 table, one column per target.

- `INSERT ... SELECT CAST(s AS T)` refuses on 8 targets (INT four times, DECIMAL twice, BOOLEAN, DATE), where Spark stores 8 rows with NULLs for the bad texts. FLOAT, DOUBLE, TIMESTAMP and NTZ succeed on both engines.
- MERGE with `UPDATE SET c = CAST(src.s AS T)` refuses on the same 8 targets. A `writeTo(...).append()` of a cast frame shows the same split.
- The CSV reader with an explicit schema aborts the whole read on the first bad row on main (INT family on `' 1'`, DECIMAL, DATE, BOOLEAN and NTZ forms). Spark returns the column with NULLs.
- The CSV options `ignoreLeadingWhiteSpace` and `ignoreTrailingWhiteSpace`: Spark trims and answers. Main refuses the option itself: `reader option 'ignoreLeadingWhiteSpace' is not supported by repark yet`.
- The JSON reader, PERMISSIVE: INT family, DECIMAL, DATE, BOOLEAN and NTZ refuse on the first bad row, where Spark NULLs it.
- `mode=FAILFAST` on CSV or JSON: both refuse when the file holds a bad row for the target, with different error classes. Main's refusal is the unsupported-option error, which coincides with Spark's read failure on these files.

## Ordinary inputs that refuse where Spark answers

Report section 2, the bullet list under the ingestion table.

- Whitespace-padded integers, such as `' 1'`, to the INT family: INSERT, MERGE, writeTo, and the CSV and JSON readers, in both modes.
- `'1.0'`-style fractions to the INT family, with ANSI off.
- `'1e3'`-style exponents to DECIMAL, in both modes. The DECIMAL exponent cells are covered by [STRING-DECIMAL-EXPONENT-1](string-decimal-exponent-1-card-2026-11-15.md), which covers exactly the dense exponent-to-DECIMAL CAST and TRY_CAST cells: 4 texts, 2 targets, 2 functions and 2 modes, 32 cells in all. On this measurement every one of them refuses on CAST and gives a silent NULL on TRY_CAST.
- DATE trim, pad and year forms, in both modes, on every write path.
- NTZ trim and pad forms refuse with ANSI on. With ANSI off they give silent NULLs wherever they reach the shim; that half is in STRING-CAST-SILENT-1.

## Measured

The orchestrator's spot-check agrees with the report on `CAST(' 1' AS INT)`: Spark 1, main refuses. The other three spot-check cells belong to STRING-CAST-SILENT-1. The spot-check probe is held outside the repository.

## Seats

The seats are in [STRING-CAST-SILENT-1](string-cast-silent-1-card-2026-11-16.md#seats). This card adds none. The same build covers them.

## Order and exclusions

One build with STRING-CAST-SILENT-1, queued at the end of the current blocker list, after SIGNED-ZERO-UPDATE-IN-LIST-1. Until that build lands, these refusals are exclusions in the scoped label ICE-V2-FLAT-AWS-LABEL-1. If the build lands before the label, the exclusions drop.

## Append (2026-11-16): the comparison positions, from Fable's sweep

Fable's type-pair sweep of 2026-11-16 (family 11) found the same loud refusal in comparisons, which the list above (CAST, the write statements and the readers) does not name. With ANSI off, `int_col = '1.25'`, `int_col IN ('1.25')` and `int_col < '1.25'` refuse on main with `Cannot cast string '1.25' to value of Int32 type`, where Spark answers (cells `f:eq:INT:STR_NUM`, `f:in:INT:STR_NUM`, `f:lt:INT:STR_NUM`, class R). With ANSI on both engines refuse (class B). The cited cells use INT; other pairs in the sweep are not quoted here. The silent cells stay on STRING-CAST-SILENT-1. Measured before #1341 and #1345, on main's package `pkg-ddu1-7b21bde3` against Spark 4.1.2. Reproduction: `python3 <scratchpad>/sweep/v.py f:eq:INT:STR_NUM f:in:INT:STR_NUM`, with `<scratchpad>` the Fable sweep scratch folder (not committed).

## Status (2026-11-22): the INT-family slice is fixed on branch `fix/string-cast-lenient-int-1` (not merged)

The first slice of this card: a STRING cast to TINYINT, SMALLINT, INT or BIGINT on the Spark door. Measured on
main `5caea2a5` against Spark 4.1.2, both ANSI modes, 82 texts per target in 15 classes, on the SQL literal,
`VALUES`, column, DataFrame `.cast`, `F.expr` and `selectExpr` seats, `TRY_CAST`, a hundred-row frame with one bad
row, and `INSERT ... SELECT CAST(s AS T)`, `MERGE ... UPDATE SET c = CAST(u.s AS T)` and `writeTo(...).append()` of
a cast frame into a local Iceberg table read back cold. Registry entry
[STRING-CAST-LENIENT-INT-1](../../../docs/spark-sql-iceberg-parity.md#string-cast-lenient-int-1--a-string-cast-to-tinyint-smallint-int-or-bigint-trims-spark-whitespace-and-answers-null-with-ansi-off--fixed-2026-11-22-branch-fixstring-cast-lenient-int-1-not-merged).

**Fixed (every measured cell answers Spark's value, type and nullability, or Spark's `[CAST_INVALID_INPUT]` and
SQLSTATE 22018):**

- Whitespace-padded integers (the bytes Spark trims: U+0000 to U+0020 and U+007F) answer the number in both
  modes, where main refused with Arrow's cast error.
- With ANSI off, malformed, exponent, special, empty, blank, Unicode-space and out-of-range texts answer NULL for
  the row, and fractions answer the truncated integer (`'1.9'` 1, `'-1.9'` -1, `'.5'`, `'.'`, `'+.'` and `'-.'`
  0), where main aborted the query.
- With ANSI on, the refusals carry Spark's class and message (an out-of-range text is `CAST_INVALID_INPUT` too, as
  Spark's `toIntExact` raises it).
- The three write statements store Spark's rows (ANSI off: every row, NULL for the bad texts; ANSI on: the padded
  numbers) and refuse as Spark does on a bad text with ANSI on.
- A NUL in the text no longer aborts the process: `'\x003'` answers 3, and a refusal renders the NUL as `\u0000`
  (main's Arrow error carried the NUL into Arrow's FFI stream, which panicked in a non-unwinding function).

**Not changed, by design or out of scope (recorded, not fixed):**

- `TRY_CAST` keeps its answers: its whitespace cells stay NULL where Spark answers the number; they are
  STRING-CAST-SILENT-1's cells.
- The readers: the JSON reader's user-schema integer casts and `smartCsv`'s integer rungs keep Arrow's parse
  through an identity operand the new seat skips; the CSV reader's schema path runs its own field parser and was
  never on this seat.
- A cast planned inside a `VALUES` node keeps Arrow's parse, because `INSERT ... VALUES` places its
  store-assignment cast there. Main and this branch store `INSERT INTO t VALUES (1, '42')` into an INT column
  as 42, where Spark refuses `[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]` in both modes (silent, the
  store-assignment family of STRING-CAST-SILENT-1); padded and malformed texts there still refuse.
- The native door (`repark.sql()`) keeps DataFusion's cast.
- An `IN` list keeps Arrow's parse and still refuses `CAST(n AS INT) IN (' 2', '3 ')` in both modes: Spark
  compares that list as text with ANSI off (no row) and as numbers with ANSI on (rows 2 and 3). Plain comparisons
  that reach the cast (`= ' 1 '`, `= '1.25'`, `< 'abc'`) answer Spark's rows with ANSI off.
- DECIMAL, BOOLEAN, DATE and NTZ targets, and the comparison cells of the 2026-11-16 append beyond those that
  reach this cast, remain for the later slices.

Up: [map.md](map.md).
