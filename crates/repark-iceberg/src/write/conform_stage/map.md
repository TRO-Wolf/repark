# map — repark-iceberg/src/write/conform_stage/

The directory holds the unit pins of [conform_stage.rs](../conform_stage.rs), the one plan-time conform stage (WRITE-SPINE-1 step 2, 2026-10-25).

- [tests.rs](tests.rs) — the stage's unit pins: the partial type converts only wall date and instant leaves, a text leaf never enters the seam, an already stored or top-level value has no partial type, a map value converts and its key keeps its field, table targets resolve listed names and refuse an unknown column, a nested zone pair without the seam refuses and records nothing, the stage record holds each site once until cleared, and the staging sink pairs on every branch.

- **The cost gate (WRITE-SPINE-1 step 2, verification round 2, 2026-10-26):** no unit test measures cost. The head's cost against base on the ZONE-SEAM-1 perf set, including the planned-plan reuse of fix round 2, is recorded in [step-2-cost-2026-10-26.md](../../../../../task/wo/write-spine-1/evidence/step-2-cost-2026-10-26.md) under alternated base and head order with thirty-run medians over twelve or more rounds (WS2-36).

- [seam_pins.rs](seam_pins.rs) — **NESTED-LITERAL-SEAM-1 (2026-10-27):** the seven seam pins of the
  zone rule R1 to R12: F-2 under the row-merge CASE, F-3 on a constant struct, P25 on a partitioned
  target, F-5 map keys from a literal and a column, TD1 and TW on D10, the per-leaf `TRY_CAST`
  exclusion, and the F1 field-id read-back. Their value pins assert Spark's recorded values in New
  York and Kolkata through a test-local mirror seam (`MirrorSeam`, registered under
  `ZONE_SEAM_NAME`), so they check the stage's partial type, not the product seam. The product seam
  is pinned by [zone_seam_pairs.rs](../../../../repark-functions/src/tests/zone_seam_pairs.rs) and by
  [map_timestamp_key_zone_seam.rs](../../../../repark-spark/tests/map_timestamp_key_zone_seam.rs).
  The file is 1000 lines, at the default ceiling: the next pin splits it.

- **The cost gate (NESTED-LITERAL-SEAM-1, 2026-10-27):** no unit test measures cost. The measured
  tables are in [the unit ledger](../../../../../task/ledgers/staging/nested-literal-seam-1-ledger.md),
  section "Cost gate".
