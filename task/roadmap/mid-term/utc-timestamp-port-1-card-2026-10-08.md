# Card UTC-TIMESTAMP-PORT-1: `from_utc_timestamp` and `to_utc_timestamp` on the zone-horizon helper

**Date:** 2026-10-08. **Filed by:** Claude (Haiku 5.5), docs lane, from the orchestrator's brief.

**Status:** open. Not scheduled. The owner ruled on 2026-10-08 that this is a follow-up unit to
ZONE-HORIZON-RENDER-1, not part of it.

**Retires:** when the two functions answer Spark's values and types in every cell of the grid
below, and the cast cost in R-12 is measured against main.

## Why

ZONE-HORIZON-RENDER-1 made every instant and wall-clock render read the final DST rule after
2099, through the shared helper `repark_common::zone_horizon`. `from_utc_timestamp` and
`to_utc_timestamp` were left out. They are `datafusion-spark` kernels, outside the workspace, so
they cannot call the helper. They still read standard time after 2099.

Source: [task/ledgers/staging/zone-horizon-render-1-ledger.md](../../ledgers/staging/zone-horizon-render-1-ledger.md),
clause C-012 (OPEN, carried here by the owner's ruling), residue class R-1, and residue R-12.

## Measured

Each number below is the ledger's. None is re-measured here.

- **C-012.** `from_utc_timestamp` and `to_utc_timestamp` read the final rule after 2099. Its
  evidence is the eight `*_utc_timestamp*` rows of the ledger grid. The clause also names 13
  control cells before 2100 where the upstream kernel differs from Spark: its implicit
  `TIMESTAMP_NTZ` → `TIMESTAMP` cast and its gap rule.
- **R-1 residue.** Class R-1 holds 223 residue cells: `from_utc_timestamp`, `to_utc_timestamp`,
  and both over a `TIMESTAMP_NTZ`. The row's "At 2099 too" column gives 13. The brief called the
  223 cells "after 2099" and the 13 "control cells before 2100". The ledger does not say the 223
  are only after 2099. This card uses the ledger's numbers: 223 residue cells, of which 13 also
  differ at 2099.
- **R-12, a cost row.** `CAST(… AS DATE)` and `CAST(… AS TIMESTAMP_NTZ)` on data before 2100 read
  1.043 and 1.041 of main, on 20,000,000 instants before 2100 (session zone `America/New_York`,
  four pinned cores, five interleaved process pairs, median; base `156be81c`, head `d0db2b5e`).
  No cell changes its answer. The ledger's §Perf records the figures; its C-013 guard covers
  `hour()` and `date_format` only.
- **Ruling, 2026-10-08.** The remedy for R-12, hoisting the helper's year check to once per batch,
  is folded into this unit by the owner's ruling.

The brief's optional items. The orchestrator's brief states them; the ZONE-HORIZON-RENDER-1
ledger does not record these two S3s:

- In `crates/repark-common/src/zone_horizon.rs`, the predicate that decides whether a zone has a
  final rule reads a `static HAS_FINAL_RULE_CACHE: Mutex<Vec<(String, bool)>>` (line 74). Its
  lookup is a linear scan of that vector under the lock (lines 137–154), keyed by the zone's
  `Display` name. The brief says the cache key is unpinned. This card did not check the tests.

## Step 0

1. Re-measure the 223 residue cells and the 13 control cells on live Spark 4.1.2 against the
   current helper. If the counts differ from the ledger, say so here and use the new count.
2. Re-measure R-12 on main `6c04b218` and on the branch that carries the port.

## The ask

1. Own `from_utc_timestamp` and `to_utc_timestamp` in `crates/repark-functions`, calling the
   zone-horizon helper. Their answers after 2099 follow the final rule, as the other sites do.
2. Port the two functions' edges before 2100 as well. The ledger says their implicit
   `TIMESTAMP_NTZ` → `TIMESTAMP` cast and gap rule differ from the upstream kernel there too
   (13 control cells). This changes answers before 2100, to Spark's. The change must say so, and
   the 13 cells must be pinned.
3. Hoist the helper's year check to once per batch, so `CAST(… AS DATE)` and
   `CAST(… AS TIMESTAMP_NTZ)` before 2100 cost what they cost on main (R-12).

Optional scope, not required for the ask: the two S3s above. The cache key and the
scan under the mutex after 2099 belong to the same helper. Take them only if the port
already changes that code, and say so in the change.

The port is wider than a fix. [AGENTS.md](../../../AGENTS.md) "Change discipline" says a
semantic-adjacent rewrite is a separate change. The ledger's own note calls it "wider than the
horizon." Keep the port's diff to the two functions and the helper hoist.

## Gates

- The Step 0 grid: each of the eight `*_utc_timestamp*` rows, the 223 residue cells and the 13
  control cells, equal to Spark's recorded answer, value and type, or are named in the change as
  still differing for a reason the card records.
- No cell outside those classes changes its answer. The grid diff shows it.
- The two casts in R-12 read at main's cost, on the same measurement as the ledger's §Perf.
- Each pin fails for its reason under a mutation.
- The `map.md` of every directory the change touches is current.

## Pointers

- The ZONE-HORIZON-RENDER-1 ledger: [zone-horizon-render-1-ledger.md](../../ledgers/staging/zone-horizon-render-1-ledger.md),
  clauses C-012 and C-013, residue R-1 and R-12, §Perf and Q1.
- The ZONE-HORIZON-RENDER-1 card: [zone-horizon-render-1-card-2026-10-07.md](zone-horizon-render-1-card-2026-10-07.md).
- The transition-day card, a sibling that carries R-4: [transition-day-arithmetic-1-card-2026-10-08.md](transition-day-arithmetic-1-card-2026-10-08.md).
- The shared helper: `crates/repark-common/src/zone_horizon.rs`.
