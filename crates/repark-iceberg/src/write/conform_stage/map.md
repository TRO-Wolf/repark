# map — repark-iceberg/src/write/conform_stage/

The directory holds the unit pins of [conform_stage.rs](../conform_stage.rs), the one plan-time conform stage (WRITE-SPINE-1 step 2, 2026-10-25).

- [tests.rs](tests.rs) — the stage's unit pins: the partial type converts only wall date and instant leaves, a text leaf never enters the seam, an already stored or top-level value has no partial type, a map value converts and its key keeps its field, table targets resolve listed names and refuse an unknown column, a nested zone pair without the seam refuses and records nothing, the stage record holds each site once until cleared, and the staging sink pairs on every branch. pins: write-spine-1/C-007, C-008, C-017, C-021

- **The cost gate (WRITE-SPINE-1 step 2, verification round 2, 2026-10-26):** no unit test measures cost. The head's cost against base on the ZONE-SEAM-1 perf set, including the planned-plan reuse of fix round 2, is recorded in [step-2-cost-2026-10-26.md](../../../../../task/wo/write-spine-1/evidence/step-2-cost-2026-10-26.md) under alternated base and head order with thirty-run medians over twelve or more rounds (WS2-36). pins: write-spine-1/C-030
