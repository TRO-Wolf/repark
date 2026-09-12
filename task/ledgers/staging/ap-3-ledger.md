# Unit ledger — AP-3 step 1 · `projected_files_at_target` multiplies the uncompressed footer sum (S2-23)

**Unit:** AP-3 step 1 · **Date:** 2026-09-12 · **Branch:** `feat/ap-3` · **Base:** `f6d5466f` (RP-17)
**Model:** swe-2-high
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** Three AP-1 measurements agree: `projected_files_at_target` multiplies the
stored (already-compressed) `file_size_in_bytes` sum by the footer `byte_ratio`,
applying compression twice. RP-17's re-measure under one codec
([docs/perf/adapt-part-ap1-remeasure-2-2026-09-12.md](../../../docs/perf/adapt-part-ap1-remeasure-2-2026-09-12.md))
reads −74.2 % / −72.0 % against the live rewrite; the corrected form
`Σ uncompressed footer bytes × byte_ratio` — which equals the inputs' own compressed
sum, the honest floor for a rewrite of the same rows — reads −36.7 % / −31.5 %. The
remaining gap is S2-24's (the rewrite writes ~1.5× its input's compressed bytes under
one codec; fork card F-REWRITE-SIZE-1). AP-1-R-001 stays OPEN until both land.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Not in this step:** `projected_partitions` (untouched, measured exact — D-3), any
formula tune beyond D-1, any dependency file, `STATUS.md`,
`briefs/next-sequence.md`, any fork change (D-4), any JVM.

**Decisions.** D-1 `projected_files_at_target` = ceil(Σ_uncompressed(item) ×
byte_ratio / target) per partition value, where Σ_uncompressed(item) is that file's
footer `total_uncompressed_size` column-chunk sum; an unreadable footer keeps the
documented 0.55 fallback ratio but each file's byte basis becomes the uncompressed
estimate `file_size_in_bytes / 0.55`. D-2 red first: the AP-1 byte pins move from the
stored-bytes projection to the uncompressed one with the RP-17 numbers as evidence
(1 156 376 → 2 831 692 on uniform/skewed); every other plan pin stays green.

## PROPOSITION LEDGER — AP-3 step 1 — 2026-09-12

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `uncompressed_footer_basis` (D-1): `projected_files_at_target` equals `ceil(Σ_uncompressed(value) × byte_ratio / target)` summed over partition values, where each file's byte basis is its footer `total_uncompressed_size` column-chunk sum — compression applied once. | Pin asserts the uncompressed-derived count against independently recomputed footer sums on a compressible CTAS fixture; the RP-17 bed-shape pin asserts the 2 831 692-derived count. | OPEN | pending |
| C-002 | `fallback_estimates_uncompressed` (D-1): with any footer unreadable the frame keeps `byte_ratio=0.55 (fallback)` and each file's byte basis is the uncompressed estimate `file_size_in_bytes / 0.55`, so the projection folds back to stored bytes at target — never compressed twice. | Corrupt one data file; pin asserts `0.55 (fallback)` on every row and the `unpartitioned` count equals `ceil(Σ (stored_i / 0.55) × 0.55 / target)`. | OPEN | pending |
| C-003 | `red_first_rp17_bed` (D-2): a pin on the RP-17 uniform bed shape (206 items, Σ stored 3 074 844, Σ uncompressed 7 529 566, ratio 0.376076, target 524 288) asserts the 2 831 692-derived file count and FAILS on the base tree. | Red output pasted below. | OPEN | Red pasted in "Red first": `rp17_bed_shape_projects_the_uncompressed_sum_once` reads 3 vs the required 6 on the base tree; the two CALL pins red beside it (252 vs 142; 50 vs 90). |
| C-004 | `other_plan_pins_green` (D-2): every other `plan_partitioning`/`apply_partitioning` pin passes unchanged. | `cargo test -p repark-spark plan_partitioning` — only the moved pins red on base; all green after. | OPEN | pending |
| C-005 | `projected_partitions_identical` (D-3): `projected_partitions` is byte-identical before and after on every pin (it counts distinct values, never bytes). | The pins asserting partition counts (90 days, 4 regions, 1 for the RP-17 shape) are green on both runs. | OPEN | pending |
| C-006 | `notes_and_guide`: the residue note and the frame `notes` text state the new basis; `docs/guide/maintenance-policy.md` gains the S2-24 known-issues line (compaction is a net-size loss on zstd tables until F-REWRITE-SIZE-1 lands) where it lists the AP-0-R-001 caveat. | `plan_every_row_carries_the_r001_file_count_caveat` green; the guide paragraph names the uncompressed basis and the S2-24 line. | OPEN | pending |

VERDICT: 6 clauses, 0 PROVEN, 6 OPEN, 0 REJECTED.

## Red first

The three moved pins ran against the base tree (`cargo test -p repark-spark
plan_partitioning`, 26 tests) before any production edit. Two assert the
uncompressed derivation on live footers and one is the RP-17 uniform bed shape
at the score seam; on the base tree every one of them reads the stored-bytes
projection instead:

```text
---- call::plan_partitioning_score::tests::rp17_bed_shape_projects_the_uncompressed_sum_once stdout ----
thread 'call::plan_partitioning_score::tests::rp17_bed_shape_projects_the_uncompressed_sum_once' (1930261) panicked at crates/repark-spark/src/call/plan_partitioning_score.rs:454:9:
RP-17 uniform shape projects ceil(2 831 692 / 524 288) = 6, got 3

---- tests::plan_partitioning::plan_byte_ratio_falls_back_when_a_footer_is_unreadable stdout ----
thread 'tests::plan_partitioning::plan_byte_ratio_falls_back_when_a_footer_is_unreadable' (1930267) panicked at crates/repark-spark/src/tests/plan_partitioning.rs:644:5:
assertion `left == right` failed: projected files fold the per-file stored/0.55 uncompressed estimate by the fallback ratio
  left: 50
 right: 90

---- tests::plan_partitioning::plan_byte_ratio_is_measured_from_parquet_footers stdout ----
thread 'tests::plan_partitioning::plan_byte_ratio_is_measured_from_parquet_footers' (1937454) panicked at crates/repark-spark/src/tests/plan_partitioning.rs:604:5:
assertion `left == right` failed: at target 1 the projection is the footers' uncompressed sum scaled once by the ratio — the compressed sum
  left: 252
 right: 142

test result: FAILED. 23 passed; 3 failed; 0 ignored; 0 measured; 913 filtered out
```

The score pin's red run fed the bed's stored bytes (3 074 844 over 206 items) —
the only basis the base signature can express — and read 3 against the required
6. The committed pin feeds the uncompressed basis the change adds (7 529 566
over the same 206 items). The compressible-fixture pin's `target` moved to 1 so
the stored-byte form (252) cannot share a ceiling with the uncompressed form
(142 = the footers' compressed sum).

## Gates

Pending.
