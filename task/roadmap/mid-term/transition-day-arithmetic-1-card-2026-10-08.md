# Card TRANSITION-DAY-ARITHMETIC-1: wall clocks and elapsed time on DST transition days

**Date:** 2026-10-08. **Filed by:** Claude (Haiku 5.5), docs lane, from the orchestrator's brief.
**Ruled:** 2026-10-08 (owner's PM delegate, relayed by the owner): accept and card the ledger's
C-016 cells, with residue class R-4 of ZONE-HORIZON-RENDER-1.

**Status:** open. Not scheduled. Source: the ZONE-HORIZON-RENDER-1 ledger, fold 1, finding 3 (S1)
and the declared 16 (its R-4 and R-5 rows):
[zone-horizon-render-1-ledger.md](../../ledgers/staging/zone-horizon-render-1-ledger.md).
The unit that found the cells is [zone-horizon-render-1-card-2026-10-07.md](zone-horizon-render-1-card-2026-10-07.md).

## Why

ZONE-HORIZON-RENDER-1 made every instant and wall-clock render read the final DST rule after 2099,
through `repark_common::zone_horizon`. Main had no zone transitions after 2099, so the defects in
this card answered Spark there by accident. With the real transition days in place they show after
2099 as well. They already show at 2099, so the defect exists at every year. It was hidden only by
the absence of transitions.

**The defects** (R-4 of the ledger, measured on its grid):

- `unix_timestamp('<gap wall>')` raises `CANNOT_PARSE_TIMESTAMP` where Spark shifts the wall clock
  forward.
- `timestamp ± INTERVAL` and `months_between` read the wall clock on a transition day where Spark
  reads elapsed time.

**The count.** The ledger's R-4 class holds 104 cells on its grid, and its row gives 20 of them at
2099, the control year. This card uses the ledger's 20. The verifier's 27-zone grid counts 1,208
cells after 2099: 629 `months_between`, 150 `unix_timestamp` in a gap, 279 `TIMESTAMP_NTZ` of a gap
wall and 150 `from_json` of a gap wall. The verifier's evidence sits under this unit's verify
record.

## The ask

Make the transition-day answers match Spark, in three parts:

- `unix_timestamp` (and `to_unix_timestamp`) of a wall clock inside a gap shifts forward as Spark
  does.
- A gap wall cast to `TIMESTAMP_NTZ` and `from_json` of a gap wall answer Spark. (An NTZ cast
  should not consult the session zone.)
- `months_between` and interval arithmetic read elapsed time on transition days.

The ledger's grid does not show a residue cell for `to_unix_timestamp` (R-4 names only
`unix_timestamp`). Step 0 measures whether `to_unix_timestamp` of a gap wall differs from Spark
before any code changes. If it does not, the card drops it from the ask and says so.

## Out of scope

- `from_utc_timestamp` and `to_utc_timestamp`: the upstream kernel, carried by the ledger's C-012
  to its own follow-up unit.
- The 2099 control and every answer that already matches Spark: the card changes only the cells
  it names.
- R-2 (`extract` and `date_part` read the `UTC` label) and R-3 (nanosecond range): not horizon
  defects, not carded here.

## Gates

- The verifier's 1,208 cells after 2099 and the ledger's 20 R-4 cells at 2099 answer Spark on the
  27-zone grid and on the unit's grid.
- No cell outside those classes changes its answer. The grid diff shows it.
- The residue of ZONE-HORIZON-RENDER-1 (832 cells) loses the R-4 cells and gains none.
- Each pin fails for its reason under a mutation, as the ledger's C-014 requires.
- An Opus verifier on the product PR.

## Pointers

- The ZONE-HORIZON-RENDER-1 ledger: C-016, R-4, R-5 and fold 1 finding 3.
- The ZONE-HORIZON-RENDER-1 card: [zone-horizon-render-1-card-2026-10-07.md](zone-horizon-render-1-card-2026-10-07.md).
- The shared helper: `crates/repark-common/src/zone_horizon.rs`.
- The residue and oracle fixtures: `python/repark/tests/zone_horizon_render_1_residue.json` and
  `python/repark/tests/zone_horizon_render_1_spark_oracle.json`.
- The residue check that names the classes: `python/repark/tests/test_zone_horizon_render_1.py`
  (`test_every_residue_cell_is_explained`).
