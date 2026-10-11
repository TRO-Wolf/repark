# Card ICE-TSNS-NARROW-REFUSE-1: a narrowing the analyzer inserted refuses by name; a narrowing the user wrote stores

**Date:** 2026-10-10. **Filed by:** the v1.5.4 docs lane (Claude Haiku 5.5), from the owner's ruling of 2026-10-10 on parity row R-017.

**Status:** open. Target v1.5.5. One Opus unit.

**Sibling:** card ICE-TSNS-COERCION-1 (the real fix, no release assigned):
[ice-tsns-coercion-1-card-2026-10-10.md](ice-tsns-coercion-1-card-2026-10-10.md).
Parity row: ICE-TSNS-SQL-1-R-017 in [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).

## Why

Owner rule, 2026-10-10, verbatim:

> narrowing the user wrote stores, narrowing the analyzer inserted refuses. A CAST, TRY_CAST or date_trunc written in the statement is deliberate and stores. A microsecond type that only exists because type coercion widened a NULL branch (coalesce, nvl, array, CASE ELSE NULL, if) is accidental and refuses by name.

A `timestamp_ns` value beside an untyped NULL is typed as a microsecond instant on main, and the
value is cut to microseconds before the store. The refusal of this card is the narrow fix: it
refuses the accidental narrowing and leaves the written casts alone. The typing defect itself is
card ICE-TSNS-COERCION-1.

## Starting evidence

- At main `58bf67e3`, 480 of 650 top-level cells store a value cut to microseconds: 360 cut
  only, and 120 cut with the wall moved as well. The count is the third verify's (2026-10-10,
  five zones) and is recorded in parity row R-017.
- A lineage test that refuses every one of those 480 also refuses a written
  `CAST(ns AS TIMESTAMP)` and a written `date_trunc('second', ns)`. Both store on main and must
  keep storing. A test on the lineage of the value does not separate the written narrowing from
  the inserted one.
- Today one door refuses: `UPDATE` with no `WHERE` for an untyped-NULL spelling (pin
  `an_update_with_no_where_refuses_a_value_narrowed_from_nanoseconds` in
  `crates/repark-spark/tests/timestamp_ns_nested_shapes.rs`, parity row R-017).

## Recorded disagreement (for the unit to resolve)

With no `WHERE`, three written casts on that door disagree:

| Statement | Stored value on main |
|---|---|
| `UPDATE t SET v = CAST(c AS TIMESTAMP)` | all nine digits (the INSERT conform peels the cast) |
| `UPDATE t SET v = CAST(c AS TIMESTAMP_NTZ)` | the value cut to microseconds |
| `UPDATE t SET v = CASE WHEN id > 0 THEN CAST(c AS TIMESTAMP) ELSE NULL END` | the value cut to microseconds; a New York gap wall moves to 03:30 |

Main raised a raw Arrow error for all three. None is refused today.

Open question for the unit. The owner rule says a written CAST stores, so the first row is the
rule's answer. The second and third rows are written casts too, yet they store a cut value. Lean:
a written CAST stores the value it was written to produce; a cut the analyzer makes beside an
untyped NULL refuses. The unit confirms or overrides this lean against the owner rule, and the
verdict is recorded in the ledger.

## The ask

- Refuse the analyzer-inserted narrowing on every door, not only the `UPDATE` with no `WHERE`:
  the coalesce family (`coalesce`, `nvl`, `array`, `CASE ... ELSE NULL`, `if`) when the value
  is `timestamp_ns` and the NULL branch carries no nanosecond type.
- Store every written narrowing (`CAST`, `TRY_CAST`, `date_trunc`) as main does today, with the
  values the statement writes.
- Resolve the recorded disagreement above, under the owner rule.
- The refusal uses the text the one refusing door carries today (`… The value was narrowed from
  nanoseconds to microseconds before the store; give the NULL beside it the type timestamp_ns`)
  unless the unit measures a Spark class to match.

## Acceptance

- Five session zones, every write door (INSERT, INSERT OVERWRITE, `replaceWhere`, MERGE arms,
  UPDATE with and without `WHERE`, DataFrame writers), values read from the Parquet files.
- Never worse than main: no cell that stores the right value on main stores a wrong one, and no
  written cast that stores on main refuses.
- Every refusal is pinned by its own statement and reads the value back to prove the store
  did not happen.
- An Opus verifier on the product pull request.

## Out of scope

- The nanosecond-aware typing that would turn these refusals back into stores: card
  ICE-TSNS-COERCION-1.
- The nested leaves: card ICE-TSNS-NESTED-1 (parity row R-015).
- The `timestamptz_ns` overwrite and MERGE wall (parity row R-008).
