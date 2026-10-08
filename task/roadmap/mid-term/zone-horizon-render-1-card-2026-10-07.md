# Card ZONE-HORIZON-RENDER-1: instant-to-wall-clock rendering past 2099 in a DST zone

**Date:** 2026-10-07. **Filed by:** Claude (Haiku 5.5), docs lane, from the orchestrator's brief. **Source:** the C-2d fold 1 re-verify verdict (PR #991), finding "Engine-wide: an instant past 2099 in a DST zone renders at standard time".

**Status:** filed, not scheduled. The verdict grades it S2.

## Why

**The repro.** With the session zone `America/New_York` and no Postgres needed:

```sql
SELECT CAST(TIMESTAMP '2100-07-01 12:00:00' AS STRING), hour(TIMESTAMP '2100-07-01 12:00:00'), date_format(TIMESTAMP '2100-07-01 12:00:00', 'HH:mm')
```

The same wall clock read through `pg.<schema>.c` (New York row 2100-06-01) shows the same fault: the instant is now right, and the engine's instant-to-wall rendering is the part that is off.

**Observed.** `('2100-07-01 11:00:00', 11, '11:00')`, with unix_micros `4118140800000000` (the right instant). The 2099-07-01 control gives `12:00 / 12`. `collect()` returns `datetime(2100, 7, 1, 12, 0)`.

**Expected.** `12:00:00 / 12`, as Spark.

**Scope.** The verdict says this predates C-2. It is not in PR #991's diff. It belongs to the engine's rendering path, which does not read `zone_horizon::proxy_year`. The localiser agrees exactly with the literal, so the C-2d ruling is met. The fault is in the rendering of the instant to a wall clock.

## The ask

Make instant-to-wall-clock rendering (the cast to string, `hour`, `date_format` and their family) read the same horizon as the localiser, `zone_horizon::proxy_year`. The C-2d rulings accepted the new crate edge `repark-functions → repark-common` as the way to share `proxy_year` without a second copy.

## Gates

- the verdict's three expressions answer as Spark at 2100 in New York, in a Southern-hemisphere zone, and in a zone with no DST;
- a 2099 control stays unchanged;
- an Opus verifier on the product PR.

## Pointers

- The C-2d fold 1 re-verify verdict (PR #991), finding S2 "Engine-wide: an instant past 2099 in a DST zone renders at standard time".
- The orchestrator's C-2d fold 1 rulings (2026-10-07), which accept the crate edge `repark-functions → repark-common`.
- The localiser's pin named in the verdict: `zone_localiser/tests.rs::the_2099_and_2100_sides_of_the_horizon_agree`.
- The code the finding names: `crates/repark-common/src/zone_horizon.rs`.
