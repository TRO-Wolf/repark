# NESTED-LITERAL-SEAM-1 — nested date and wall literals, constant nested values and MAP timestamp keys reach the zone seam     grade: B   engine band: Muse MAX builder (sketch and build); Opus 5.5 verifier; Muse MAX second verifier   release: 1.5.6

Opened by the orchestrator (Claude Fable 5.1) on 2026-10-27 under the owner ruling of the same day ("adopt all three leans"): NESTED-MAP-KEY-ZONE-1 is fixed first, folded here, and the format-v2 label (ICE-V2-LABEL-1, PR #1195) re-dates on the main that carries this unit. Cards: [NESTED-LITERAL-SEAM-1](../../roadmap/mid-term/nested-literal-seam-1-card-2026-10-25.md), [NESTED-MAP-KEY-ZONE-1](../../roadmap/mid-term/nested-map-key-zone-1-card-2026-10-25.md). Source findings: WRITE-SPINE-1 step (2) ledger F-2, F-3, F-3 widened (P25) and F-5 (C-026), ruling WS2-25 and WS2-30.

## 0. The gap, measured

WRITE-SPINE-1 step (2) put one plan-time conform stage on every routed write door and seams every nested `arrow_cast` whose argument reads a column (site B, ruling B2). Four shapes stay on main's UTC reading because the column-reference guard excludes them:

- F-2: on D8 (MERGE whole-struct or `SET *`), a nested date or wall literal under the row-merge CASE. 32 census cells. The first attempt to seam literals failed with `column types must match schema types`: the projected list lost its `PARQUET:field_id` metadata under the volatile UDF.
- F-3: a constant nested value outside a CASE. 8 D10 `update_whole_where` struct cells (a date literal, or a scalar-subquery date source). The first attempt broke the pin `a_plain_update_whose_only_naive_leaf_is_a_try_cast_stores_spark_instant`: `TRY_CAST(instant AS TIMESTAMP_NTZ)` inside a literal struct was already the UTC wall when the seam read it in the session zone (`1767341045123456` against Spark's `1767323045123456`), a double conversion.
- P25: the F-3 mechanism on a partitioned UPDATE target (`UPDATE t SET st = named_struct('a', DATE '2026-01-02') WHERE k = 0`), outside the census.
- F-5: a date into a `MAP<TIMESTAMP, …>` key on D3 (`map(DATE '2026-01-02', 'x')` and `map(c_date, 'x')`): `zone_partial` in `crates/repark-iceberg/src/write/conform_stage.rs` clones the key field and partials only the value, so the key pair never reaches the seam. Spark: New York `1767330000000000`, Kolkata `1767292200000000`; main stores `1767312000000000` in both.

Silent values all four; the v2 label cannot carry them.

## 1. Rulings already made

- Dialect and door classes are WRITE-SPINE-1's; this unit changes no door routing and retires no refusal (ZS-9 stays).
- A value Spark refuses stays refused; a value Spark stores is stored as Spark stores it; nothing new is silent (owner, standing).
- Cost gate: the write census replay wall and the C-012 set within 1.05x of main (coercion ruling 2026-10-12).
- The census is the measurement: `python/repark/tests/writes/_record_write_spine_1.py --main --base <sha>` replays 12,192 cells; this unit adds cells for the four shapes so the axis is measured from now on.

## 2. Steps

0. **Sketch (hand back before step 1; the orchestrator rules, then step 1 starts).** Reproduce the four shapes on this head and on Spark 4.1.2 (the oracle on this box; one JVM) in New York and Kolkata; record the six values per shape. Find the F-2 cause (why the projected list under the row-merge CASE loses `PARQUET:field_id` when a literal is seamed, when a plain SELECT with the same CASE does not) and the F-3 cause (where the literal struct's `TRY_CAST` is folded before the seam sees it). Propose the seam rule that replaces the column-reference guard: which nested expressions carry a zone pair the seam must convert (a date or wall literal, a `named_struct`/`map`/`array` constant, a scalar subquery of date type) and which already carry the converted instant (an instant cast to a wall by an explicit cast). Propose the map-key arm: the key pair reaches the seam (or the key is converted in the stage) for `MAP<TIMESTAMP, …>` and `MAP<TIMESTAMP_NTZ, …>` keys, literal and column. State the expected census moves exactly (the 40 cells by id; the new cells) and the pins. Questions, with a lean each. No product change in this step.
1. **Build.** The seam rule and the map-key arm, in `crates/repark-iceberg/src/write/conform_stage.rs` and the seam modules under `conform_stage/` only, within the file-size ratchets. Unit pins per mechanism (F-2 CASE literal on D8, F-3 constant struct on D10, P25 partitioned target, map key literal and column on D3, each asserted against Spark's recorded values in both zones, and the `TRY_CAST` double-conversion pin stays green). New census cells for the four shapes in the fixture, re-recorded with the recorder; the replay on the head shows the 40 F-2/F-3 cells and the new cells DIFFERENT to EQUAL and no other cell moving.
2. **Measure.** `pytest python/repark/tests/writes -q -n 8` never worse; `cargo test -p repark-iceberg` and `cargo test -p repark-spark` green; the replay counts and wall; the C-012 set median within 1.05x; the 842-cell matrix untouched (the orchestrator runs it on the merged main).
3. **Records** (clerk, Haiku 5.5 xhigh): ledger `task/ledgers/staging/nested-literal-seam-1-ledger.md`, the two cards retire, registry rows, the write-spine-1 ledger's C-026 closes by citation, maps, release-notes line.

## 3. Gates

`make ci` surface locally for the touched crates, `make rust-panic-ban`, `make check-rust-file-size`, the comment gate (no comments in code from Anthropic models; Muse writes none either under this unit), `bash scripts/check_map_md.sh --base origin/main` before the PR. CI green before any verifier; Opus 5.5 verifier (value-moving) and Muse MAX second verifier, merge on the union of PASS and the owner's word.

## 4. Halts

Any census cell moving that the sketch did not list; any EQUAL cell regressing; the cost gate; a need to touch a door's routing or a refusal; the file-size ratchet with no honest split.

## Step 0 outcome and ruling NLS-1 (orchestrator, 2026-10-27)

The sketch (`sketch-step-0.md`, Muse MAX, commit `f9453e25`) reproduced the four shapes on this head and on Spark 4.1.2 in both zones (head stores `1767312000000000` for every shape; Spark stores `1767330000000000` in New York and `1767292200000000` in Kolkata), found both causes (F-2: the analyzer's type coercion wraps the CASE branch in a cast carrying the field-id metadata, the optimizer folds the seamed literal to a scalar list that carries none, and the projection's schema check fails; F-3: `TRY_CAST(instant AS TIMESTAMP_NTZ)` folds to the UTC wall only in the optimizer, after the stage, so an unguarded seam converts it twice), and drew the seam rule R1 to R12, the map-key arm, the exact 40 census moves and the new cells.

**Ruling NLS-1 (Q1 to Q6, every lean adopted).**
- Q1: step 1 may touch `crates/repark-functions/src/zone_seam.rs` for the Map arms of `converted_type` and `convert`, mirroring the value arms; nothing else outside `conform_stage.rs`, `conform_stage/`, the fixture and the recorder.
- Q2: TD1 (`TRY_CAST(date …)`) and TW (`TRY_CAST(wall …)`) are seamed; each gets a Rust pin against Spark's recorded value in both zones; no census cell.
- Q3: the CAST and TRY_CAST forms under the D8 CASE stay ZS-9-refused; the D8/D10 asymmetry is recorded as measured; step (4) scope.
- Q4: F1 adopted (strip the outer cast's metadata on a scalar-foldable seamed argument), with one added pin: for one F-2 cell the written Parquet file's schema carries the `PARQUET:field_id` on the nested leaf (read the file back), so the sink's re-attachment is proven, not assumed.
- Q5: the new census shapes are approved as drawn (`update_whole_where_part` with `WHERE k = 0`, 480 cells; `insert_map_key` on the key-packed targets, 48 cells); the census grows to 12,720 cells and the recount pins move with it; the expected moves are exactly the 40 listed, the 32 partitioned F5 cells and the 4 `map_ltz_key` cells, and nothing else.
- Q6: the TRY_CAST-instant exclusion is per leaf.
- Budget: no split; if `conform_stage.rs` passes 900 lines the zone half moves pure into `conform_stage/seam.rs` with a map row.

Step 1 starts on this ruling. Verification: Opus 5.5 verifier (value-moving) and Muse MAX second verifier after CI.

## Step 1 outcome and ruling NLS-2 (orchestrator, 2026-10-27)

The build (Muse MAX, commits `f5e41910` to `ce7ec21d`) delivered the seam rule R1 to R12 as `zone_partial_for` in `conform_stage.rs` (615 to 827 lines; no split), the per-leaf `TRY_CAST` exclusion, the map-key arms in `zone_seam.rs` (Q1), the two census spellings (`update_whole_where_part`, 480 cells; `insert_map_key`, 48 cells; 43 spellings, 12,720 cells), the re-recorded base fixture `write_spine_1_main_59d104d9.json`, the frozen moves `write_spine_1_nls1_moves.json` and seven Rust pins in `conform_stage/seam_pins.rs` (1000 lines, at the default ceiling: the next pin splits the file).

Reproduced on the lane by the orchestrator: the 15 `conform_stage` pins green; the head replay re-recorded with the recorder (`--main --base ce7ec21d`) gives EQUAL 7895 / DIFFERENT 1427 / REFUSED 2452 / ERROR 946, differs from the base fixture on exactly the 52 frozen cells, every one EQUAL on the head, zero regressions, 12,668 cells byte-identical; `make check-rust-file-size rust-fmt-check check-model-ban` clean. Builder gates as handed back: `cargo test` green on repark-iceberg (1047), repark-functions (967, 1 ignored) and repark-spark (33); `pytest python/repark/tests/writes -q -n 8` 5543 passed, 57 skipped, 3 xfailed; cost set worst 1.034 (INSERT OVERWRITE into TIMESTAMP), all eight within 1.05; replay wall ratio 0.994.

**Count correction (Q5).** Ruling NLS-1 expected 40 + 32 + 4 = 76 moves. The measured set is 52: the 40 listed, 8 `update_whole_where_part` cells (struct target only) and the 4 `map_ltz_key` cells. The sketch's "32 partitioned F5 cells" extrapolated the full target matrix onto the partitioned spelling, but the array, map and array-of-struct date cells of `update_whole_where` were already EQUAL on `59d104d9` (its own D10 list in section 5 names 8 struct cells, not 32), and the partitioned sibling measures the same way. The expected set is therefore 52, and the build matches it exactly; no unlisted cell moved and no halt applies.

**Ruling NLS-2 (F1 mechanism, retro under Q4).** The ruled strip (remove the outer cast's field metadata on a scalar-foldable seamed argument) fails at execution: the physical planner re-coerces the CASE branches and the `ScalarValue` round trip drops nested field metadata. The build instead wraps the CASE that holds the scalar-foldable seam in a `CAST` to the metadata-free container type (`wrap_scalar_seam_case`), so the projection's schema check sees one type on both sides. Accepted: same intent, same scope, and the Q4 pin `a_stripped_array_case_writes_field_ids_through_the_sink` reads the written Parquet schema back and finds field id 3 on the stripped array, so the sink's re-attachment is proven. Step (2) is satisfied by the figures above; step (3) records follow on the Haiku clerk, then CI, the Opus 5.5 verifier and the Muse MAX second verifier.
