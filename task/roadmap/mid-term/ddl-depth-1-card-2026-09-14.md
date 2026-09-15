# Card DDL-DEPTH-1 — a nesting-depth cap on the DDL type parser

**Date:** 2026-09-14 · **Filed by:** run 14b under owner ruling Q-R13-13 (card only; no new refusal in run 14b) ·
**Source:** FACADE-4 step 1 S2-21 Rust review finding P3-PARSE-DEPTH
([facade-4-ledger.md](../../ledgers/staging/facade-4-ledger.md)).

## Why

`crates/repark-spark/src/type_table/parse.rs` recurses once per nesting level with no depth bound, while the
`typeName` name surface already bounds at `SPARK_TYPE_NAME_MAX_DEPTH` (32). Neither the base Python parser nor the
step-1 Rust parser caps DDL nesting: both raise `RecursionError` near depth 1,000 instead of a typed refusal.

## Decisions (proposed)

| id | decision |
|---|---|
| D-1 | Measure first: PySpark 4.1.2's answer for `fromDDL` / `_parse_datatype_string` / `spark.sql("CAST(... AS <deep type>)")` at depths 32, 33, 100 and 1,000, pinned before the change. |
| D-2 | Cap parse depth at `SPARK_TYPE_NAME_MAX_DEPTH` with a loud typed refusal whose class and message follow D-1's measurement; if Spark accepts depth 1,000, the cap moves to the smallest depth that keeps the parser off the native stack and the refusal says so. |
| D-3 | The new refusal is a declared delta in the census pins and the changelog. |

## Steps

| step | worker | what |
|---|---|---|
| 0 | Devin | D-1 oracle cells and red-first pins (both doors). |
| 1 | Devin | D-2 cap, D-3 deltas, pins green, S2-21 Rust reviewer. |

**Home:** `crates/repark-spark/src/type_table/parse.rs`, `python/repark/tests/test_facade_4_census_pins.py` or a sibling
pin file. **Gates:** `make verify`, `make preflight`; Grok critic-logic before the PR.

## Pointers

- Up: [map.md](map.md)
