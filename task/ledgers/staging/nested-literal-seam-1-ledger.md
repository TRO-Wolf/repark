# Unit ledger — NESTED-LITERAL-SEAM-1 · nested date and wall literals, constant nested values, a partitioned target and a MAP timestamp key reach the zone seam

**Date:** 2026-10-27 · **Branch:** `feat/nested-literal-seam-1` · **Base:** `59d104d9` (`origin/main`) · **Product head:** `5076bdfb` (fix round 1, the builder's last commit; the build ended at `ce7ec21d`, and the outcome and record commits after it are docs only)
**Models:** builder `muse-spark-1.3-contributor` (Muse MAX, commits `f5e41910` to `ce7ec21d`, and fix round 1 `6f44f0f9` to `5076bdfb`); clerk `claude-haiku-5-5` (step 3 and the records of fix round 1). **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** Target: release 1.5.6.

**Order:** [the order](../../wo/nested-literal-seam-1/order.md), its step 0 and step 1 outcomes and rulings NLS-1 and NLS-2; the step 0 sketch [sketch-step-0.md](../../wo/nested-literal-seam-1/sketch-step-0.md), section 5. Cards retired by this ledger: [NESTED-LITERAL-SEAM-1](../../roadmap/mid-term/nested-literal-seam-1-card-2026-10-25.md) and [NESTED-MAP-KEY-ZONE-1](../../roadmap/mid-term/nested-map-key-zone-1-card-2026-10-25.md). Source findings: [WRITE-SPINE-1 step (2) ledger](write-spine-1-ledger.md), F-2, F-3, P25 and F-5 (C-026).
**This ledger closes when the pull request of NESTED-LITERAL-SEAM-1 merges.**

## What the unit changes

WRITE-SPINE-1 step (2) seamed only an `arrow_cast` whose argument reads a column, so four shapes stayed on main's UTC reading. The seam rule R1 to R12 (`zone_partial_for` in `conform_stage.rs`) now converts a nested date literal under the row-merge CASE (F-2); a constant nested value outside a CASE, partitioned or not, through its date leaves (F-3 and P25); a date literal or column into a `MAP<TIMESTAMP, …>` key on D3 (F-5, through the Map arms of `zone_seam.rs`); and, on D10, the `TRY_CAST` date and wall leaves (TD1 and TW). A wall literal under the D8 CASE or in a D10 constant stays ZS-9-refused (R3), and D3 and D8 refuse the `TRY_CAST` leaves. An instant `TRY_CAST` to a wall leaf keeps its UTC wall by a per-leaf exclusion. The CASE that holds a scalar-foldable seamed argument is wrapped in a CAST to the metadata-free container type (ruling NLS-2). The Map arms also drive the read-path CAST of a map with a timestamp key, which now answers the session zone (C-015). Door routing and refusals are unchanged, and ZS-9 stays.

## PROPOSITION LEDGER — NESTED-LITERAL-SEAM-1 — 2026-10-27

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | F-2: a nested date literal under the row-merge CASE of a D8 MERGE (whole-struct and `SET *`) stores the session midnight, for struct, array, map and array-of-struct targets, in New York and Kolkata. A wall literal under that CASE stays ZS-9-refused (R3). | `case_literals_under_the_row_merge_case_store_the_session_midnight`, the four D8 shapes in both zones against the pinned Spark values. | **PROVEN** | The pin asserts `NEW_YORK_MIDNIGHT` `1767330000000000` and `KOLKATA_MIDNIGHT` `1767292200000000`, the values Spark 4.1.2 stores for `DATE '2026-01-02'`; main stored the UTC midnight `1767312000000000`. The 32 D8 cells of the moves file are this mechanism. pins: nested-literal-seam-1/C-001 |
| C-002 | F-3: a constant nested value outside a CASE on a D10 `UPDATE … WHERE` stores the session midnight: a date literal inside `named_struct`, into a struct target. A wall literal in a D10 constant stays ZS-9-refused (R3). | `a_constant_struct_stores_the_session_midnight`, both zones. | **PROVEN** | The same pinned values. The first attempt without the per-leaf exclusion of C-006 broke the existing TRY_CAST pin, and main stored the UTC midnight. The 8 D10 `update_whole_where` cells of the moves file are this mechanism. pins: nested-literal-seam-1/C-002 |
| C-003 | P25: the F-3 mechanism on a partitioned UPDATE target stores the session midnight: `UPDATE t SET st = named_struct('a', DATE '2026-01-02') WHERE k = 0` on a partitioned `(id INT, st STRUCT<a: TIMESTAMP>, k INT)`. | `a_constant_struct_on_a_partitioned_target_stores_the_session_midnight`, both zones. | **PROVEN** | Found in verification round 1 of WRITE-SPINE-1 step (2) (S2, F-3 widened, probe P25). The partitioned census spelling `update_whole_where_part` carries its form as 8 moved cells (C-013). pins: nested-literal-seam-1/C-003 |
| C-004 | F-5: a date into a `MAP<TIMESTAMP, …>` key on D3 stores the session midnight, from a literal (`map(DATE '2026-01-02', 'x')`) and from a column (`map(c_date, 'x')`), in New York and Kolkata. | `map_keys_store_the_session_midnight_from_a_literal_and_a_column`, both zones. | **PROVEN** | The key pair reaches the seam through the Map arms of `converted_type` and `convert` in `zone_seam.rs` (ruling NLS-1 Q1). That stage pin asserts Spark's values through a test-local mirror seam, so the product seam is pinned by `a_container_cast_of_a_date_key_to_timestamp_starts_at_the_session_midnight`, `a_container_cast_of_a_wall_key_to_timestamp_reads_it_in_the_session_zone` and `a_container_cast_of_an_instant_key_to_timestamp_ntz_takes_the_session_wall` (`crates/repark-functions/src/tests/zone_seam_pairs.rs`), and by `map_date_keys_store_the_session_midnight_through_insert_by_name` (`crates/repark-spark/tests/map_timestamp_key_zone_seam.rs`, a literal and a column key through `ReparkSession`, both zones). The 4 `map_ltz_key` cells of the moves file are the census form. Closes WRITE-SPINE-1 C-026 by citation, with a closure line at the end of that ledger. pins: nested-literal-seam-1/C-004 |
| C-005 | TD1 and TW (ruling NLS-1 Q2): on D10, `TRY_CAST(date …)` and `TRY_CAST(wall …)` leaves inside a nested TIMESTAMP store Spark's values; D3 and D8 refuse them (ZS-9 and C-012). | `try_cast_leaves_store_spark_values_and_an_instant_leaf_keeps_utc`, both zones. | **PROVEN** | No census cell: the census has no TRY_CAST source vocabulary (sketch, section 5). On D10, before this unit, these leaves stored the UTC reading silently on main. pins: nested-literal-seam-1/C-005 |
| C-006 | The exclusion is per leaf (ruling NLS-1 Q6): an instant `TRY_CAST` to a wall leaf keeps its source type in the seam carrier, so a mixed struct converts its date leaves and keeps the instant leaf's UTC reading. | `a_try_cast_instant_leaf_keeps_its_source_type_in_the_seam_carrier`, New York only, asserting the carrier's type and not a value; the existing `a_plain_update_whose_only_naive_leaf_is_a_try_cast_stores_spark_instant` (`crates/repark-spark/tests/ntz_into_ltz_nested.rs`) stays green and asserts the stored value. | **PROVEN** | A whole-node skip broke the existing pin (value `1767341045123456` against Spark's `1767323045123456`); the per-leaf rule keeps it green. pins: nested-literal-seam-1/C-006 |
| C-007 | The F1 mechanism (ruling NLS-1 Q4, as built under NLS-2): a CASE holding a scalar-foldable seamed argument is wrapped in a CAST to the metadata-free container type, and the sink re-attaches `PARQUET:field_id` on the nested leaf. | `a_stripped_array_case_writes_field_ids_through_the_sink`, which reads the written Parquet schema back. | **PROVEN** | The read-back finds field id 3 on the stripped array, so the sink's re-attachment is proven, not assumed. The wrap is `wrap_scalar_seam_case` in `conform_stage.rs`. pins: nested-literal-seam-1/C-007 |
| C-008 | The census grows to 43 spellings and 12,720 cells. The two new spellings are `update_whole_where_part` (D10, partitioned, 480 cells) and `insert_map_key` (D3 on the key-packed targets `map_ltz_key` and `map_ntz_key`, 48 cells). The base's red set is 2,649 (F1 240, F2 228, F3 596, F4 428, F5 52, F6 351, F7 318, F8 436); the head's is 2,597, with no F5 cells. | `test_the_spelling_table_is_exactly_the_forty_three_ruled_names` and `test_fixture_cells_are_the_census`, both green on the builder's run. | **PROVEN** | The spelling test pins the 43 ruled names on their doors. The fixture test reads the 2026-10-27 recount section of the census addendum (fix round 1) and pins 12,720 cells, the status counts, the 2,649 base red set and the families. The ZS9 count (1,048) and the D10-STORE count (672) are the `cells` of their rows in `test_write_spine_1_doors.py`. pins: nested-literal-seam-1/C-008 |
| C-009 | The 52 moves: every move is DIFFERENT to EQUAL, and no other cell moves. By shape: `merge_update_whole` 16, `merge_update_whole_literal` 16, `update_whole_where` 8, `update_whole_where_part` 8, `insert_map_key` 4. The frozen ids are the `moves` array of the frozen moves file. | `test_head_replay_moves_exactly_the_nested_literal_seam_1_cells`, which asserts the head replay's difference from the base fixture equals the frozen set and that every move is EQUAL. | **PROVEN** | The committed moves file holds 52 unique ids in the five shapes above (checked by the clerk). The test skips when no head replay document is present. None is committed (the WRITE-SPINE-1 C-022 precedent), so the replay was run on the lane and its counts are in the tables below. pins: nested-literal-seam-1/C-009 |
| C-010 | Zero regressions: no EQUAL cell regresses, and every cell outside the 52 is byte-identical to the base fixture (12,668 cells). | The same test, whose second assertion is that every cell outside the frozen set equals main's answer. | **PROVEN** | The builder's replay diff: 12,668 cells byte-identical, zero regressions. The skip caveat of C-009 applies. pins: nested-literal-seam-1/C-010 |
| C-011 | Cost gate (order section 1): the census replay wall and the C-012 cost set (eight write cells, five interleaved sets per side) stay within 1.05x of main. | No unit test measures cost; the measured tables below are the record. | **PROVEN** | Worst head/base median 1.034 (INSERT OVERWRITE into TIMESTAMP, 215.2 ms against 208.2 ms); all eight within 1.05. Replay wall 130.06 s on the head against 130.80 s on the base, ratio 0.994. The lane-local harness is not in the repository. pins: nested-literal-seam-1/C-011 |
| C-012 | The CAST and TRY_CAST forms under the D8 row-merge CASE (the sketch's C1m, T1m, TD1m and CD1m) stay ZS-9-refused, so D8 and D10 refuse these forms differently. The asymmetry is recorded as measured, not fixed. | Ruling NLS-1 Q3 (lean: stays refused) and sketch section 6, Q3. | OPEN | Closing question for WRITE-SPINE-1 step (4): does the ZS-9 retirement take these D8 forms? That needs the seam to read a CAST inside the CASE, which this unit does not do. |
| C-013 | The count correction (ruling NLS-1 Q5): the ruling expected 76 moves (the 40 listed, 32 partitioned F5 cells and 4 `map_ltz_key` cells). The measured set is 52, because the partitioned spelling moves 8 struct cells, not 32. Its array, map and array-of-struct date cells were already EQUAL on the base, as in both `update_whole_where` spellings. | The base fixture statuses (for example `update_whole_where_part.cow.array_ltz.date.new_york` is EQUAL on `59d104d9`) and the moves test of C-009. | **PROVEN** | The base fixture records those cells as EQUAL. The sketch extrapolated the full target matrix onto the partitioned spelling. Recorded as a sketch correction in the order, not a halt; no unlisted cell moved. pins: nested-literal-seam-1/C-013 |
| C-014 | Ruling NLS-2 (retro under Q4): the ruled strip of the outer cast's metadata fails at execution, so the F1 mechanism is the CAST wrap of C-007, and the Q4 field-id read-back holds on that wrap. | `a_stripped_array_case_writes_field_ids_through_the_sink`, as C-007. | **PROVEN** | The strip's failure is the builder's measured record in the order: the physical planner re-coerces the CASE branches, and the ScalarValue round trip drops nested field metadata. The read-back pin proves the mechanism that replaced it. pins: nested-literal-seam-1/C-014 |
| C-015 | The read-path `CAST` of a map with a date, wall or instant key answers Spark's session-zone value: `CAST(map(…) AS MAP<TIMESTAMP, STRING>)` and `CAST(map(…) AS MAP<TIMESTAMP_NTZ, STRING>)` in New York and Kolkata, where main answered the UTC reading. It is a read value, not a stored one. | `map_key_casts_on_the_read_path_answer_spark_values`, both zones and the four CAST shapes, and the three `zone_seam_pairs.rs` key pins named under C-016. | **PROVEN** | The ten measured values are in the order's verification round 1 section (S2-1); every head value equals Spark's, and the base stored the UTC reading on the read path too. The move is the Q1 key arm of C-004 doing on the read path what the value arm already did. pins: nested-literal-seam-1/C-015 |
| C-016 | The key pass-through mutant (the key left unconverted in `zone_seam.rs` `convert`) is caught by the Rust tests: red on each of the three product-seam pair pins and on each of the two end-to-end key pins. Before fix round 1 it survived every Rust test (S1-1). | `a_container_cast_of_a_date_key_to_timestamp_starts_at_the_session_midnight`, `a_container_cast_of_a_wall_key_to_timestamp_reads_it_in_the_session_zone` and `a_container_cast_of_an_instant_key_to_timestamp_ntz_takes_the_session_wall` (`zone_seam_pairs.rs`), and `map_date_keys_store_the_session_midnight_through_insert_by_name` and `map_key_casts_on_the_read_path_answer_spark_values` (`map_timestamp_key_zone_seam.rs`). | **PROVEN** | Fix round 1 outcome (order): the orchestrator reproduced mutant (c) red on 3 of 3 pair pins and 2 of 2 end-to-end pins, with clean runs of 23 of 23 and 2 of 2. pins: nested-literal-seam-1/C-016 |

## Verification round 1 (2026-10-27)

The Opus 5.5 verifier (`claude-opus-5-5`) returned **FAIL** on head `3984c26d` (CI green, 24 pass). Claims 1, 2, 3, 5 and 6 passed as measured: the seven seam pins assert the oracle's values; the head replay reproduces the recount below with exactly the 52 frozen moves and zero regressions; the re-recorded base fixture is identical cell by cell; the ZS-9 and C-012 refusals hold on head and base; the release-module cost worst is 1.022; mutants (a) and (b) are killed. The five findings, each reproduced by the orchestrator before ruling NLS-3 (order, "Verification round 1 and ruling NLS-3"):

- S1-1: mutant (c), the key passed through unconverted in `zone_seam.rs` `convert`, survived every Rust test, because the `seam_pins.rs` value pins check the stage against a test-local mirror seam; answered by C-016 and fix round 1.
- S2-1: the read-path CAST of a map with a timestamp key moved with no oracle cell, pin or record; ruled correct on the measured values and recorded as C-015.
- S2-2: the ledger, the registry status line, the release-notes bullet and the write map said a wall literal under the D8 CASE converts and that `TRY_CAST` of a date or wall converts everywhere; corrected under C-001 and C-005 and by the records of this round.
- S3-1: the recount named the head red set as 2,649 with F5 52; that is the base, and the head's is 2,597 with no F5 cells; corrected under C-008.
- S3-2: C-006 said both zones, but the carrier pin runs in New York only and asserts a type; corrected under C-006, and the `seam_pins` map row now says its value pins run through the mirror seam.

Ruling NLS-3 is in the order by pointer. Fix round 1 (Muse MAX, commits `6f44f0f9` to `5076bdfb`) added the pins of C-015 and C-016 and the census-addendum parse of C-008; its outcome is in the order.

## Census recount (C-008 to C-010, C-013)

Base and head replay counts over the 12,720 cells, zero UNRECORDED on both sides:

| Side | EQUAL | DIFFERENT | REFUSED | ERROR | Total |
|---|---|---|---|---|---|
| base `59d104d9` | 7,843 | 1,479 | 2,452 | 946 | 12,720 |
| head `ce7ec21d` | 7,895 | 1,427 | 2,452 | 946 | 12,720 |

The 52 moves, all DIFFERENT to EQUAL:

| Spelling | Door | Cells | Shape |
|---|---|---|---|
| `merge_update_whole` | D8 | 16 | cow and mor, struct, array, map and array-of-struct, date, New York and Kolkata |
| `merge_update_whole_literal` | D8 | 16 | the same shape as `merge_update_whole` |
| `update_whole_where` | D10 | 8 | cow and mor, struct, date and col_date, New York and Kolkata |
| `update_whole_where_part` (`WHERE k = 0`, partitioned) | D10 | 8 | the same shape as `update_whole_where` |
| `insert_map_key` (`map_ltz_key`) | D3 | 4 | append, date and col_date, New York and Kolkata |

The census families of the base's red set (RED 2,649): F1 240, F2 228, F3 596, F4 428, F5 52, F6 351, F7 318 and F8 436. The head's red set is 2,597 with no F5 cells: every move is F5, so F5 falls to zero and no other family moves. The row counts are ZS9 1,048 and D10-STORE 672, as in the census addendum of 2026-10-27.

## Cost gate (C-011)

Five interleaved sets per side, each the median of five timed writes after one warm run; 2M rows, New York, v3, release modules on both sides, the same CPU set.

| Write | Base ms | Head ms | Ratio |
|---|---|---|---|
| INSERT into TIMESTAMP | 47.7 | 46.3 | 0.970 |
| INSERT OVERWRITE into TIMESTAMP | 208.2 | 215.2 | 1.034 |
| INSERT BY NAME into TIMESTAMP | 207.0 | 207.1 | 1.000 |
| MERGE insert into TIMESTAMP | 261.7 | 262.8 | 1.004 |
| INSERT into TIMESTAMP_NTZ | 49.6 | 49.6 | 1.000 |
| INSERT OVERWRITE into TIMESTAMP_NTZ | 214.7 | 214.8 | 1.000 |
| INSERT OVERWRITE into TIMESTAMP from TIMESTAMP_NTZ | 220.9 | 219.1 | 0.992 |
| writeTo.overwritePartitions into TIMESTAMP | 210.3 | 214.2 | 1.019 |

The full replay wall (12,720 cells, release modules, interleaved): base median 130.80 s, head median 130.06 s, ratio 0.994.

## Clerk record (2026-10-27)

- The ledger is listed in [the staging map](map.md).
- The cards NESTED-LITERAL-SEAM-1 and NESTED-MAP-KEY-ZONE-1 are closed in place, and their rows in the mid-term map name this ledger.
- The WRITE-SPINE-1 step (2) ledger gets dated closure lines for its C-026 (by C-004 here) and for the P25 gap under its C-019 row (by C-003 here). No row text is rewritten.
- The census addendum of 2026-10-27 is at the end of [census-2026-10-24.md](../../wo/write-spine-1/census-2026-10-24.md).
- The registry status line of 2026-10-27 sits under the conform-stage row of [the parity registry](../../../docs/spark-sql-iceberg-parity.md).
- The v1.5.6 notes draft carries two bullets under "Stored values that change": the nested-literal bullet and the read-path CAST bullet (C-015).
