# map — repark-spark/src/call/plan_partitioning

## Purpose

The `plan_partitioning` procedure's file-backed test module
(`../plan_partitioning.rs`): the pin fixtures that need the measured AP-0 bed
shapes live here so the procedure file stays under its size ceiling.

## Contents

- `tests.rs` — the in-crate unit tests: the `plan_id` stability pins and the
  extra-branch refusal (AP-1 step 1), and **AP-1-CLOSE-1 (2026-09-12)** the
  RP-18 three-bed pins — `rp18_beds_rank_like_the_live_frames` reconstructs the
  measured bed shapes (200 batches split at the 65 536-boundaries into 206
  items per synthetic bed with the recorded per-piece uncompressed sums; the
  futures bed's rated columns verbatim) and asserts the RP-18 frame order and
  (partition, projected-file) counts; `rp18_beds_projection_bounds_the_live_actual`
  asserts the unpartitioned `projected_files_at_target` × target bounds the
  recorded live actuals within 2× (the upper-bound reading of the projection,
  red-first by doctoring the bound).
  pins: ap-1-close-1/C-001, C-002

## Pointers

- Up: [../map.md](../map.md)
- The procedure: [../plan_partitioning.rs](../plan_partitioning.rs)
