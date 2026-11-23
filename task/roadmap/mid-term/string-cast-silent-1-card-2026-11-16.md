# Card STRING-CAST-SILENT-1: a string cast answers a silent zero, a silent NULL or a stored wrong value where Spark answers or refuses

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), on the owner's rulings of 2026-11-16. **Sources:** the 2026-11-16 string-cast sizing report, its sections 0 to 5 (held outside the repository, so its raw cells are not committed), and question 3 of the owner questions in the step 0 sketch of CAST-LITERAL-UNWRAP-1, [sketch.md](../../wo/cast-literal-unwrap-1/sketch.md).

**Status:** ruled 2026-11-16, not dispatched. **Label-blocking.** The owner, 2026-11-16, about 16:25, verbatim: "adopt Fable's, run the string-cast sizing", which adopted the four side cards of CAST-LITERAL-UNWRAP-1 step 0 (question 6). The owner, 2026-11-16, about 17:05, verbatim: "go with your recommendation, merge 1343", on question 3. The recommendation it adopts:
1. the silent half blocks the scoped label ICE-V2-FLAT-AWS-LABEL-1, at S1;
2. the loud refusals do not block it: [STRING-CAST-LENIENT-1](string-cast-lenient-1-card-2026-11-16.md) is carded S2, and the label text lists those refusals as exclusions;
3. one build covers both, queued at the end of the current blocker list, after [SIGNED-ZERO-UPDATE-IN-LIST-1](signed-zero-update-in-list-1-card-2026-11-15.md). If it lands before the label, the exclusions drop.

**Severity: S1, label-blocking (the silent half).**

## Silent cells

Report section 5, "Which give a wrong value or NULL silently?", with Spark's answer and main's answer for each. Spark 4.1.2 is the oracle. Main's product tree at `cb7d3943` is the measured base; main at `6eea676d` has the same crate and Python trees.

| Cell | Spark 4.1.2 | RePark main |
|---|---|---|
| `CAST('' AS DECIMAL(10,2))`, and `TRY_CAST` of the same text | NULL with ANSI off; raises with ANSI on | `0.00` in both modes (`0E-18` at scale 18). Observed four times: the dense grid in both modes, the DataFrame door and the native literal. |
| `CAST('20260105' AS DATE)` and `TRY_CAST` of the same text | NULL with ANSI off; raises with ANSI on | `2026-01-05` in both modes |
| `TRY_CAST` of a whitespace text to TINYINT, SMALLINT, INT or BIGINT, for example `' 1'` (five texts in the class) | `1` | NULL in both modes. `CAST` of the same text refuses instead (see STRING-CAST-LENIENT-1). |
| `TRY_CAST` of a whitespace text to FLOAT or DOUBLE | `1.0` | NULL in both modes |
| `TRY_CAST` of a padded DATE text with ANSI off: `' 2026-01-05'`, `'2026-01-05 '`, `'2026'`, `'2026-01'`, `' 2026-01-05 10:00:00'`, `'2026-01-05 10:00'` | the date | NULL, silently |
| `CAST` and `TRY_CAST` of a padded TIMESTAMP_NTZ text with ANSI off: `' 2026-01-05 10:00:00'`, `'2026-1-5'`, `'2026-01-05 10:00'`, `'2026-01-05 '`, `'2026'`, `'2026-01'` | answers | NULL, silently. With ANSI on, main refuses instead. |
| `INSERT ... SELECT s` of a text into a FLOAT or DOUBLE column, with ANSI off | refuses (`CANNOT_SAFELY_CAST`) in both modes | stores every row of the eight-text sample: the parsed values, and NULLs for the junk texts |
| `INSERT INTO t VALUES (1, '42')` into an INT column (added 2026-11-23 from the STRING-CAST-LENIENT-1 INT-slice builder, PR #1413; measured on main `5caea2a5`) | refuses (`INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST`) in both modes | stores 42 in both modes |
| The JSON reader, PERMISSIVE, DOUBLE or FLOAT from a JSON string value `'1.0'`, `'1e3'` or `'nan'` | NULL for every JSON string value except `'Infinity'`, which Spark returns as inf | parses them: 1.0, 1000.0 and NaN |
| The CSV reader, explicit schema, DOUBLE, from `'nan'` | NULL | NaN |

Not counted as silent cells: the TIMESTAMP wrong values in the dense grid (30 per mode) came from the driver-zone rendering of a collected timestamp, not from the cast. Rendered as strings, all 34 TIMESTAMP date and timestamp cells agree in both modes (report section 0, artifact A1).

## Seats

From report section 3, with file:line on main `cb7d3943`.

- **S1, the cast kernel.** The Spark door has no STRING-to-INT, DECIMAL, BOOLEAN or DATE parse shim. Those plans keep a raw `Cast`, and `arrow::compute::cast_with_options` decides. That gives Arrow's verbatim errors and Arrow's accept set (`+1`, `01`, `-0`, `''` to a zero decimal, `'20260105'` to a date). General CAST routing is `crates/repark-functions/src/analyzer.rs:98-120`. The integer parser `string_to_integral` (`crates/repark-functions/src/cast_map/leaf.rs:294`, Spark trim and legacy fraction parse) is wired only into the map-cast UDF (`crates/repark-functions/src/cast_map.rs:273-310`). The shipped TIMESTAMP seat is `crates/repark-functions/src/spark_string_timestamp.rs:27-36`. The report sizes the seat as one analyzer rewrite that routes STRING-source `Cast` and `TryCast` to a parse function per target family, plus per-row decimal and date parsers, comparable to the TIMESTAMP seat.
- **S2, the NTZ shim.** `crates/repark-functions/src/timestamp_ntz_cast.rs:1-40` parses through Arrow's `string_to_datetime`, with no trim and no pad forms, and its Legacy and TRY path NULLs. Size (report): small; extend the pre-parse with Spark's trim and the date, year and month forms, mirroring the LTZ grammar.
- **S3, the implicit float store.** `INSERT ... SELECT s` into FLOAT or DOUBLE with ANSI off bypasses the refusal. The gate `crates/repark-iceberg/src/write/insert_gate.rs:44-81` judges only `CAST(bare column)` projections, and the matrix in `crates/repark-iceberg/src/write/store_assign.rs:27-85` (the shared `Cast.canANSIStoreAssign` translation) never sees the pair. Spark refuses in both modes. Size (report): small.
- **S4, the readers.** CSV and JSON schema application aborts the read on the first bad row instead of NULLing it under PERMISSIVE, and the whitespace-trim and FAILFAST options refuse loudly. Seat: the reader option and plumbing layer, plus the S1 kernel per row. The report gives no file:line for S4. Size (report): medium (PERMISSIVE-null plumbing per row, plus two options). The JSON DOUBLE parse and the NTZ `Z` accept show that the reader parsers are already RePark's own, so one seat covers both readers.

## What already covers or agrees

From report section 4 and section 5.

- `string-decimal-exponent-1-card-2026-11-15.md` covers the exponent-to-DECIMAL cells and nothing else in this list.
- `ansi-off-legacy-cast-1-card-2026-11-02.md` covers none of these cells: it concerns map-key array elements.
- `string-to-date-truncate-1-card-2026-10-01.md` covers none of these cells: no trailing-junk text was measured.
- `string-numeric-parse-perf-1-card-2026-10-01.md` is performance work and asserts no correctness cell.
- No card covers the DECIMAL `''` zero, the DATE `'20260105'` leniency, the TRY_CAST silent NULLs, the implicit float store, the reader PERMISSIVE, trim and FAILFAST paths, or the JSON string parsing.
- Agreeing cells that should not be re-litigated: sign and zero spellings to integers; the full FLOAT and DOUBLE CAST surface, with exponents, specials and case variants; single-letter booleans; every TIMESTAMP text, as strings; overflow texts (refuse or NULL on both engines).

## Spot-check

The orchestrator's spot-check agrees with the report on four cells: `CAST(' 1' AS INT)` (Spark 1, main refuses); `CAST('' AS DECIMAL(10,2))` (Spark NULL with ANSI off and raises with ANSI on; main `0.00` in both modes); `CAST('20260105' AS DATE)` (Spark NULL or raises; main `2026-01-05`); and a CSV read with an INT schema over the lines `1`, ` 2` and `abc` (Spark gives 1, NULL, NULL; main aborts the read). The spot-check probe is held outside the repository.

## Order

One build, shared with STRING-CAST-LENIENT-1, queued at the end of the current blocker list, after SIGNED-ZERO-UPDATE-IN-LIST-1. The build is not dispatched.

## Append (2026-11-16): ruling A1, the ANSI-on split

The owner, 2026-11-16, about 23:40 UTC, verbatim "Merge those and defaults.", ruled A1: the scoped label ICE-V2-FLAT-AWS-LABEL-1 is scoped to ANSI on, which is Spark 4's default. A defect seen only with ANSI off no longer blocks the label. The record is [rulings-2026-11-16.md](../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-16.md), section A1. The table above is unchanged. Each cell's mode below is read from the card's own rows and text.

**Still block (silent with ANSI on):**

- `CAST('' AS DECIMAL(10,2))` and `TRY_CAST` of the same text: main `0.00` in both modes.
- `CAST('20260105' AS DATE)` and `TRY_CAST` of the same text: main `2026-01-05` in both modes.
- `TRY_CAST` of a whitespace text to TINYINT, SMALLINT, INT or BIGINT: main NULL in both modes.
- `TRY_CAST` of a whitespace text to FLOAT or DOUBLE: main NULL in both modes.

**No longer block (ANSI off only):**

- `TRY_CAST` of a padded DATE text: the row states ANSI off.
- `CAST` of a padded TIMESTAMP_NTZ text: a silent NULL with ANSI off; the card says "With ANSI on, main refuses instead", and that is the CAST answer.
- `INSERT ... SELECT s` of a text into FLOAT or DOUBLE: the row states ANSI off. The table gives no ANSI-on cell for main on this row.

**Mode not stated:**

- `TRY_CAST` of a padded TIMESTAMP_NTZ text: the card does not give its ANSI-on answer, and TRY_CAST does not refuse. Its half of the row is not shown to be ANSI off only.
- The JSON reader, PERMISSIVE, DOUBLE or FLOAT from a JSON string value: the card states no mode.
- The CSV reader, explicit schema, DOUBLE from `'nan'`: the card states no mode.

A cell whose mode is not stated is not shown to be ANSI off only, so it stays in the blocking half until its mode is measured. This reading of A1 is the clerk's, for the owner to confirm. The card's status and severity lines are unchanged by this append.

Up: [map.md](map.md).
