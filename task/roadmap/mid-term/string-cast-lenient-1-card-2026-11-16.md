# Card STRING-CAST-LENIENT-1: string casts refuse loudly where Spark answers, on CAST, on the write statements and on the readers

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), on the owner's rulings of 2026-11-16. **Sources:** the 2026-11-16 string-cast sizing report, sections 2 and 5 (held outside the repository, so its raw cells are not committed), and question 3 of the owner questions in the step 0 sketch of CAST-LITERAL-UNWRAP-1, [sketch.md](../../wo/cast-literal-unwrap-1/sketch.md).

**Status:** ruled 2026-11-16, not dispatched. **Not label-blocking.** The owner, 2026-11-16, about 17:05, verbatim: "go with your recommendation, merge 1343", on question 3. The recommendation it adopts: the loud refusals do not block the label, and they are carded S2. The label text lists them as exclusions until the shared build lands. The shared build is [STRING-CAST-SILENT-1](string-cast-silent-1-card-2026-11-16.md)'s build, queued at the end of the blocker list. If it lands before the label, the exclusions drop.

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

Up: [map.md](map.md).
