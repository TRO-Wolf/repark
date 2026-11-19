# Card READER-TYPED-SCAN-1: a JSON or CSV typed parse raises where Spark answers, the seat (b) typed scan

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under Fable's guidance of 2026-11-19 (the owner's delegation, about 01:28 UTC). **Source:** Fable's reply of 2026-11-19 (reader-parse step 0), §2 rows Q1, Q3 and Q10, kept outside the repository; the step 0 [domain table](../../../wo/temp-view-frame-reader-parse-1/domain-table.md), §7 row D2 and §8.5 (D9–D11); and the step 0 [sketch](../../../wo/temp-view-frame-reader-parse-1/sketch.md), §4 (seat (b)).

**Status:** filed 2026-11-19, not dispatched. Seat (b) is a typed parse inside a Rust scan (Rust-first; carded). Not label-blocking, because each direct gap is loud.

**Severity: S3, not label-blocking** (Fable's guidance under the owner's delegation of 2026-11-19, about 01:28 UTC; Fable's Q3 and Q10 defaults, card D2 and D9–D11 as S3).

## Finding

- Domain table D2: "malformed timestamp text and overflowing integer text in a JSON or CSV parse raise where Spark's PERMISSIVE parser answers `NULL`"; the A set's direct cells; loud.
- Domain table D9 (`json_ml`: `d`, `eb_hour`, `sb_hour`): `multiLine` JSON, a pretty top-level array; 6 cells; RAISE when the frame is made: `Not valid JSON: EOF while parsing`; Spark answers.
- Domain table D10 (`csv_gz`, `csv_gzi`: `d`, `eb_hour`): a directory of `*.csv.gz`, with a schema and with `inferSchema`; 8 cells; RAISE when the frame is made: `No files found`; Spark answers.
- Domain table D11 (`csv_2i`: `d`, `eb_hour`): two CSV files with `inferSchema` whose `n` is `1` and `1.5`; 4 cells; RAISE when the frame is made: `Fail to merge schema field 'n'`; Spark answers, `n` `DOUBLE` (`1.0`, `1.5`).
- Fable's Q3 row, [M]: "Loud in all 34 direct cells."
- Fable's Q10 row, [M]: "18 of 18 loud."
- Fable's Q1 row, [M]: "`emu_r` and `emu_live` predict (a) in every door; (b) moves matching cells and `EXPLAIN`."

**Main:** raises in each case. D2's malformed timestamp and overflowing integer text raise where Spark's PERMISSIVE parser answers `NULL`; D9–D11 raise when the frame is made, where Spark answers.

Up: [map.md](map.md).
