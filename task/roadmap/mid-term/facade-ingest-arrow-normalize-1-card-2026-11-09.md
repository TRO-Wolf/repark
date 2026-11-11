# Card FACADE-INGEST-ARROW-NORMALIZE-1: narrow the Arrow list kinds at createDataFrame to the plain types Spark has

**Date:** 2026-11-09. **Filed by:** the records clerk (`claude-haiku-5-5`), from the Unit C park memo of 2026-11-09 (Q3, the normalise option) and the owner's ruling of 2026-11-09 ("defaults"), which adopted the card unscheduled.

**Status:** filed 2026-11-09, unscheduled. It needs a sketch and a measurement before any ruling on scheduling.

**What it would do:** at `createDataFrame`, narrow incoming DataFrame Arrow types (`LargeList`, `FixedSizeList`, the list views, `RunEndEncoded`) to the plain types Spark has. Spark has one array type. Today the facade passes five list kinds and run-end encoding straight through. A narrowing at the door would remove those variants for every rule behind the Spark door at once.

**Cost:** offset narrowing, and a batch over 2^31 elements cannot narrow. Both are costs of the option and are measured in the sketch, not assumed here.

**Why it does not replace the rule-level fix:** the native door has `arrow_cast`, so a frame built there reaches a rule with its own types. Each rule still needs its type-variant inventory (the 2026-11-09 rule in `task/lessons.md`).

**Close:** a sketch that measures the offset-narrowing cost and the 2^31 limit, then an owner ruling on whether the door narrows.

**2026-11-10, measured reach** (from the scoped review of STRUCT-SETOP-TYPE-FAMILY-1, PR #1290, findings F-5 and F-6; both pre-existing, on `b8ad7a9c` and on #1290's head; Spark answers both):
- F-5: a SQL array union a polars `Array` fails at execution on a list field-name mismatch (`element` against `item`). `union`, `unionByName`, `subtract` and `intersect` refuse with "column types must match schema types" when the polars `Array` is second; the `Array`-first orientation answers.
- F-6: a conforming INSERT of a SQL array union a polars list hits a DataFusion internal error, "cannot convert LargeList into List", on v2 copy-on-write, v2 merge-on-read and v3. The polars-first orientation inserts correctly.
- **2026-11-11, F-5 over-refusal (PR #1295, Opus 5.5 round 1).** Q1954 to Q1957 moved from EQUAL to refused where Spark answers, under the list-kind family of STRUCT-COMPARE-SEAT-FAMILY-1. The cells are over-refusals that fail closed; this card is their home. Not dispatched.
