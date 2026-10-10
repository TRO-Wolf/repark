# Card ICE-TSNS-UPDATE-CAST-1: an UPDATE with no WHERE stores the microsecond value a written CAST produces

**Date:** 2026-10-10. **Filed by:** the ICE-TSNS-NARROW-REFUSE-1 build lane (Claude Opus 5.5), from the owner's ruling of 2026-10-10 on question Q6 of that unit's ledger.

**Status:** open. Target v1.5.5. Its own unit, after the verify of ICE-TSNS-NARROW-REFUSE-1.

**Sibling:** card ICE-TSNS-NARROW-REFUSE-1:
[ice-tsns-narrow-refuse-1-card-2026-10-10.md](ice-tsns-narrow-refuse-1-card-2026-10-10.md).
Parity row: ICE-TSNS-SQL-1-R-017 in [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).

## Why

`UPDATE t SET v = CAST(c AS TIMESTAMP)` with no `WHERE`, over a `timestamp_ns` or
`timestamptz_ns` column `v`, stores all nine digits of `c`. Every other door stores the value
the cast was written to produce: `c` cut to microseconds. The owner rule says a narrowing the
user wrote stores, and what it stores is the narrowed value.

## Starting evidence

Measured on the base `8d1c4f49` and unchanged on the head of ICE-TSNS-NARROW-REFUSE-1
(spelling `cast_ts` of `python/repark/tests/ice_tsns_narrow_refuse_1_base.json`):

| Door | Zones | `timestamp_ns` target | `timestamptz_ns` target |
|---|---|---|---|
| `UPDATE` with no `WHERE`, copy-on-write (`update_nowhere`) | 5 | nine digits | nine digits |
| `UPDATE` with no `WHERE`, merge-on-read (`update_nowhere_mor`) | 5 | nine digits | nine digits |
| `UPDATE` with no `WHERE` on a branch (`branch_update`) | 5 | nine digits | nine digits |

That is 15 cells per target for each source column type (`timestamp_ns` and `timestamptz_ns`),
60 cells in all. The same statement with a `WHERE`, and the 61 other doors, store the
microsecond value.

The narrow card records the cause as the INSERT conform of the unfiltered `UPDATE` peeling
the cast. That site is not measured here; the unit measures it before it changes anything.

## The ask

- The unfiltered `UPDATE` stores the value the written cast produces, on the three doors above,
  for both targets and both source types.
- `CAST(c AS TIMESTAMP_NTZ)` and a written cast inside a `CASE` branch on the same door already
  store the microsecond value; they do not move.
- No refusal is added or removed.

## Acceptance

- Five session zones, the three doors, both targets, both sources; values read from the Parquet
  files; the stored value equals what `INSERT … SELECT CAST(c AS TIMESTAMP)` stores.
- Never worse: the 60 cells are the only ones that move.
- The release note says an unfiltered `UPDATE` written before the fix kept nanoseconds under a
  written `CAST(c AS TIMESTAMP)`.
