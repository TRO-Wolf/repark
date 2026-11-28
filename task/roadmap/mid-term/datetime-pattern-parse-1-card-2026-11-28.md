# DATETIME-PATTERN-PARSE-1 — the shared Java-pattern parser reads Spark's fraction runs and optional sections

**Rating:** S1 (owner, 2026-11-28). **Base:** main `862d7155a` (product tree of `9df269acb`). **Parent:** the
FNP-11B parser card D-1 ([fnp-11b ledger](../../ledgers/staging/fnp-11b-ledger.md)), the `java_datetime.rs` row of
[crates/repark-functions/src/map.md](../../../crates/repark-functions/src/map.md). Scout family F02 of the
2026-11-28 `dates` lane audit. Evidence: [task/wo/datetime-pattern-parse-1/](../../wo/datetime-pattern-parse-1/map.md).

## Defect

Every function that parses with a format (`to_timestamp`, `to_date`, `unix_timestamp`, `to_unix_timestamp`,
`to_timestamp_ltz`, `to_timestamp_ntz`, `try_to_timestamp`, `try_to_date`, and the `from_csv` / `from_json`
`timestampFormat` / `dateFormat` options) refused text Spark 4.1.2 reads:

- an `S` run of width *n* took exactly *n* digits (Spark takes 1 to *n*);
- `[` / `]` were literals, not optional sections (Spark nests them, runs an unclosed `[` to the end, and refuses a
  stray `]` with `DATETIME_PATTERN_RECOGNITION`, `try_` forms included, for a non-NULL row);
- `try_to_date` with a format had its own date-only tokenizer, NULL for any pattern with a time field.

Main answered NULL, or `CANNOT_PARSE_TIMESTAMP` under ANSI, and stored NULL rows: `INSERT … SELECT
to_timestamp(x, 'yyyy-MM-dd[ HH:mm:ss]')` into Iceberg stored `[NULL, NULL, NULL]` where Spark stores
`[2024-02-29 00:00:00, 2024-02-29 03:04:05, NULL]`.

## Fix

`java_datetime/walk.rs` (new; the parent file is at its 1000-line ceiling) walks the tokens with a section stack,
takes 1 to *n* fraction digits, rolls a failed section back, fails a section that re-parses a field with a
different value, and keeps main's literal brackets and exact-width fractions for a pattern with a token the
parser cannot read (the zone letters) or a digit run Spark reads greedily across a section boundary.
`parse_wall_or_null` raises a stray `]` on the first non-NULL row. `try_to_date`'s two-argument arm is
`to_date_with_format` with ANSI off; its 207-line tokenizer is deleted. 309 product lines added, 290 deleted (the
owner accepted the 309 and the `parse_wall_or_null` seat on 2026-11-28).

## Measured (2026-11-28, Spark 4.1.2)

- Step 0, 71,532 cells (8 functions × 7 doors, `from_csv` / `from_json`, NULL formats and inputs, the `S`×1-9 by
  0-10-digit grid, 110 named pairs, New York / Kolkata / UTC, both ANSI modes, LTZ and NTZ sessions, four string
  encodings, mixed columns, Iceberg and Parquet writes read back on Spark): EQUAL 22,898 → 52,182; moved on 31,819;
  **moved off 0**. [counts](../../wo/datetime-pattern-parse-1/evidence/step0_counts.json).
- Spark-door scoreboard, 51 lanes: moved on 190, **moved off 0**; the 198 F02 cells: 186 EQUAL, 12
  NULLABILITY-ONLY with Spark's value. [comparison](../../wo/datetime-pattern-parse-1/evidence/scoreboard_main_to_head.md).
- Prior corpora (unit 2's step 0, the ltzcast / ntzwiden / fexpr replay suites, fexprn, csvjson, dim, the scout
  pattern probe): 0 moved off.
- Cost, `to_timestamp(s, 'yyyy-MM-dd HH:mm:ss.SSSSSS')` over 1,000,000 rows: 1.01× main (bar 1.15×).
  [cost](../../wo/datetime-pattern-parse-1/evidence/cost.json).

## The NTZ-session exception (owner's words)

The owner's word (2026-11-28): "Except by family: in a session built with NTZ, `to_timestamp` and
`try_to_timestamp` with a pattern keep main's type and main's shifted store for newly parsed text. Anything else
moved or wrongly stored fails." Widened the same day (the owner said "I agree"): "Widen DATETIME-PATTERN-PARSE-1's
exception to every pattern form in any NTZ session, built or runtime, if all 2,924 cells match main's answer for
the same value."

29,568 cells (8 forms × 22 texts × selects, `INSERT … SELECT`, `INSERT VALUES` into `TIMESTAMP_NTZ` and
`TIMESTAMP`, CTAS × built NTZ, built LTZ, runtime NTZ × New York, Kolkata, Los Angeles, UTC × both ANSI modes, read
back on Spark): EQUAL 25,060, SAME-AS-MAIN 1,584, NEW-WRONG 2,924, OTHER-MOVED 0; no NEW-WRONG cell in an LTZ
session. [classes](../../wo/datetime-pattern-parse-1/evidence/ntz_session_store_classes.md).

| Session | Form | NEW-WRONG | SAME-AS-MAIN | Twin check pass |
| --- | --- | ---: | ---: | ---: |
| built NTZ | `to_timestamp` | 816 | 240 | 816 of 816 |
| built NTZ | `try_to_timestamp` | 816 | 240 | 816 of 816 |
| built NTZ | `to_timestamp_ltz` | 408 | 120 | 408 of 408 |
| runtime NTZ | `to_timestamp` | 432 | 136 | 432 of 432 |
| runtime NTZ | `try_to_timestamp` | 432 | 136 | 432 of 432 |
| runtime NTZ | `to_timestamp_ntz` | 12 | 8 | 12 of 12 |
| runtime NTZ | `to_date` / `try_to_date` | 8 | 0 | 8 of 8 |
| runtime NTZ | unix forms | 0 | 352 | — |
| built LTZ | unix forms | 0 | 352 | — |

The twin check ran each NEW-WRONG cell's operation on main with Spark's value written as a text main already
parses (exact-width fraction): **2,924 of 2,924** equal head's answer, in result type, CTAS column type and the
stored value read back on Spark. Ids and twins:
[ntz_session_twin_cells.tsv.gz](../../wo/datetime-pattern-parse-1/evidence/ntz_session_twin_cells.tsv.gz);
table: [ntz_session_twin_check.md](../../wo/datetime-pattern-parse-1/evidence/ntz_session_twin_check.md).

**Owner of those cells:** unit 2's NTZ batch (the NTZ-session type work: `to_timestamp`'s result type and the
`TIMESTAMP` column type under a built or runtime `timestampType=TIMESTAMP_NTZ`, and the LTZ-to-NTZ store).

## Pins

[test_datetime_pattern_parse_1.py](../../../python/repark/tests/timestamps/test_datetime_pattern_parse_1.py) with
its Spark oracle: 99 TARGET cells (red on main, green on head), 148 PROTECTED cells (green on both; the §1 controls,
and one default-session `TIMESTAMP_NTZ` store per form), and two twin pins (built NTZ, runtime NTZ) asserting head
equals main's twin answer. Rust: `java_datetime::walk::tests::*`, `try_invert::convert::tests::
try_to_date_parses_time_bearing_and_year_less_patterns`; seven mutants, each killed
([mutants.json](../../wo/datetime-pattern-parse-1/evidence/mutants.json)).

## Out of scope, unchanged

- Zone letters in a parse pattern (`X`, `XXX`, `Z`, `z`, `VV`, `O`): byte-identical to main. Next unit on this seat.
- A Spark-greedy digit run across a section boundary (`yyyyMMdd[HHmmss]`, `[yyyy][MM]`, `yyyy-M-d[HH]`) keeps
  main's behaviour; `yyyyMMdd[HHmmss]` over `20240229` stays unfixed.
- `to_timestamp_ntz` and NTZ sessions name a stray `]` `DATETIME_PATTERN_RECOGNITION` where Spark says
  `INVALID_DATETIME_PATTERN.WITH_SUGGESTION` (both refuse; `timestamp_ltz_ntz.rs`).
- `YYYY`, an unterminated quote or an illegal letter with an all-NULL input still raises on main's `to_timestamp`
  family (Spark answers NULL).
- A dictionary-encoded string column ignores the format in `to_timestamp` / `to_date` / `unix_timestamp` and fails
  in `to_timestamp_ntz`; `from_json` with an options map refuses.
- The formatter (`date_format`, F04), `spark.sql.legacy.timeParserPolicy` (F06), the typed `F.to_unix_timestamp`
  `NOT_ITERABLE` (F05; its three cells stay REFUSED), default no-format parsing, week-based `Y`, the CSV/JSON
  reader options.
