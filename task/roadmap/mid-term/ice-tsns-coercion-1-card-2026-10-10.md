# Card ICE-TSNS-COERCION-1: nanosecond-aware type coercion keeps nanoseconds beside an untyped NULL

**Date:** 2026-10-10. **Filed by:** the v1.5.4 docs lane (Claude Haiku 5.5), from the owner's ruling of 2026-10-10 on parity row R-017.

**Status:** open. No release assigned.

**Sibling:** card ICE-TSNS-NARROW-REFUSE-1, which refuses these narrowings until this card lands:
[ice-tsns-narrow-refuse-1-card-2026-10-10.md](ice-tsns-narrow-refuse-1-card-2026-10-10.md).
Parity row: ICE-TSNS-SQL-1-R-017 in [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).

## Why

On main `58bf67e3`, and at head, a nanosecond value beside an untyped NULL is typed as a
microsecond instant with the UTC zone. The five spellings are `coalesce(ns, NULL)`,
`nvl(ns, NULL)`, `array(ns, NULL)`, `CASE WHEN … THEN ns ELSE NULL END` and `if(…, ns, NULL)`.
The planner casts the NULL to `Timestamp(ns)`, the session's rule reads that cast as the SQL
`TIMESTAMP`, and the nanosecond argument is narrowed to match. A top-level `timestamp_ns` column
then stores the value floored to microseconds: 480 of 650 measured cells, 360 cut only and 120 cut
with the wall moved.

Spark has no nanosecond type. A NULL takes the type of its sibling; RePark's NULL should take
`timestamp_ns` in the same way.

Card ICE-TSNS-NARROW-REFUSE-1 refuses these spellings by name. This card is the real fix: it
keeps the nanoseconds through the coercion, so the spellings store the value they carry, and the
refusals of the narrow card turn back into stores.

## The ask

- Nanosecond-aware coercion for the five spellings when an operand is `timestamp_ns`: the NULL
  takes `timestamp_ns`, and the value keeps all nine digits through the store.
- The session wall rule applies to the result exactly as it applies to a `timestamp_ns` column
  written directly, on every write door.
- The refusals of ICE-TSNS-NARROW-REFUSE-1 for these spellings are removed in the same change.
  The written casts (`CAST`, `TRY_CAST`, `date_trunc`) are not touched by this card.
- Controls do not move: `timestamptz_ns` values and microsecond spellings store byte-identical
  values to main.

## What ICE-TSNS-NARROW-REFUSE-1 leaves for this card (2026-10-10)

- The refusal is a mark and a guard: `repark-functions` `null_narrowing.rs` (tagged before
  type coercion, settled in the session's timestamp rule) and `repark-iceberg`
  `write/narrowed_store.rs`. Keeping the nanoseconds through the coercion removes the mark at
  its source; the guard, its five call sites and the pins that expect a refusal go with it.
- Four typings the refusal does not cover and this card decides: a NULL typed `TIMESTAMP` or
  `TIMESTAMP_NTZ` beside a nanosecond value, `nvl` and `ifnull` over a nanosecond value
  (typed `STRING` today), a nanosecond value beside a microsecond column or literal, and a
  frame materialised from a narrowed value (the narrow card's ledger, questions Q2 to Q5).
- The base matrix of 89,600 cells
  (`python/repark/tests/ice_tsns_narrow_refuse_1_base.json`, recorder beside it) is the
  never-worse reference for the doors and spellings above.

## Out of scope

- The nested leaves (`struct`, `array`, `map` holding a `timestamp_ns` leaf): card
  ICE-TSNS-NESTED-1 (parity row R-015). Their refusal stays until that card lands.
- The `timestamptz_ns` overwrite and MERGE wall: parity row R-008.

## Gates

- Every spelling in the ask, every write door, five session zones, values read from the Parquet
  files, digits below the microsecond pinned.
- The Spark oracle for each spelling measured live on Spark 4.1.2 before the pin is written.
- Never worse than main; the controls above byte-identical to main.
- An Opus verifier on the product pull request.
