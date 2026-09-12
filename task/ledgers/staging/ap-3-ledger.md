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
| C-001 | `uncompressed_footer_basis` (D-1): `projected_files_at_target` equals `ceil(Σ_uncompressed(value) × byte_ratio / target)` summed over partition values, where each file's byte basis is its footer `total_uncompressed_size` column-chunk sum — compression applied once. | Pin asserts the uncompressed-derived count against independently recomputed footer sums on a compressible CTAS fixture; the RP-17 bed-shape pin asserts the 2 831 692-derived count. | **PROVEN** | `plan_byte_ratio_is_measured_from_parquet_footers` — at `target => 1` the projection is exactly the footers' compressed sum (142), i.e. uncompressed×ratio once; `rp17_bed_shape_projects_the_uncompressed_sum_once` — ceil(2 831 692 / 524 288) = 6 |
| C-002 | `fallback_estimates_uncompressed` (D-1): with any footer unreadable the frame keeps `byte_ratio=0.55 (fallback)` and each file's byte basis is the uncompressed estimate `file_size_in_bytes / 0.55`, so the projection folds back to stored bytes at target — never compressed twice. | Corrupt one data file; pin asserts `0.55 (fallback)` on every row and the `unpartitioned` count equals `ceil(Σ (stored_i / 0.55) × 0.55 / target)`. | **PROVEN** | `plan_byte_ratio_falls_back_when_a_footer_is_unreadable` — one file truncated; every row's notes say `0.55 (fallback)`; the projected count 90 = ceil(Σ stored/0.55 × 0.55 / target), equal to the stored-byte estimate |
| C-003 | `red_first_rp17_bed` (D-2): a pin on the RP-17 uniform bed shape (206 items, Σ stored 3 074 844, Σ uncompressed 7 529 566, ratio 0.376076, target 524 288) asserts the 2 831 692-derived file count and FAILS on the base tree. | Red output pasted below. | **PROVEN** | Red pasted in "Red first": `rp17_bed_shape_projects_the_uncompressed_sum_once` read 3 vs the required 6 on the base tree; the two CALL pins red beside it (252 vs 142; 50 vs 90); committed red at `dd999a2a`, all green after the change |
| C-004 | `other_plan_pins_green` (D-2): every other `plan_partitioning`/`apply_partitioning` pin passes unchanged. | `cargo test -p repark-spark plan_partitioning` — only the moved pins red on base; all green after. | **PROVEN** | `cargo test -p repark-spark plan_partitioning`: 26 passed, 0 failed; `cargo test -p repark-spark apply_partitioning`: 12 passed, 0 failed. Two rank pins' fixture targets moved to the uncompressed basis (uncompressed/90 and uncompressed/8) because the score bands uncompressed bytes now — the asserted outcomes are unchanged |
| C-005 | `projected_partitions_identical` (D-3): `projected_partitions` is byte-identical before and after on every pin (it counts distinct values, never bytes). | The pins asserting partition counts (90 days, 4 regions, 1 for the RP-17 shape) are green on both runs. | **PROVEN** | `projected_partitions` is a distinct-value count untouched by the byte basis; the same pins assert 90 (days90), 4 (regions), 1 (RP-17 shape) on both trees — only the file-count column moved (252→142, 50→90, 3→6) |
| C-006 | `notes_and_guide`: the residue note and the frame `notes` text state the new basis; `docs/guide/maintenance-policy.md` gains the S2-24 known-issues line (compaction is a net-size loss on zstd tables until F-REWRITE-SIZE-1 lands) where it lists the AP-0-R-001 caveat. | `plan_every_row_carries_the_r001_file_count_caveat` green; the guide paragraph names the uncompressed basis and the S2-24 line. | **PROVEN** | `RESIDUE_NOTE` now reads "applies byte_ratio to the footers' uncompressed byte sum, counting compression once (the stored-byte form measured -74%/-72% low against the RP-17 same-codec rewrite; the remaining gap is S2-24)"; `plan_every_row_carries_the_r001_file_count_caveat` and `plan_byte_ratio_*` pins assert the `byte_ratio=<r> (footers|fallback)` spelling; the guide's byte paragraph names the uncompressed basis and the `stored / 0.55` fallback estimate, followed by the S2-24 known-issues line |

VERDICT: 6 clauses, 6 PROVEN, 0 OPEN, 0 REJECTED.

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

- `make develop`: green (native module rebuilt and installed editable).
- `cargo test -p repark-spark plan_partitioning`: green (26 passed; 0 failed).
- `cargo test -p repark-spark apply_partitioning`: green (12 passed; 0 failed).
- `.venv/bin/python -m pytest python/repark/tests -q -k "plan_partitioning or adapt or apply_partitioning"`: green (1 passed, 5 skipped, 6299 deselected).
- `make py-test` (whole parity suite): green (744 passed, 1 skipped, 11 xfailed).
- `make check-docs-links`: green (777 files, 4990 links).
- `make check-ledger-grammar`: green.
- `make verify`: green.
- Comment fence (`git diff --cached -- '*.rs' '*.py' '*.toml' '*.sh' '*.yml' | grep -P '^\+\s*(//|#(?! noqa))'`): prints nothing.
- D-5 facade seat: unchanged — `plan_partitioning` stays a `CALL` only; no Python session method added.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ap-3
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: All three moved pins failed red on the base tree (pasted above) and pass green after the change; the score pin feeds the RP-17 bed's exact measured sums (206 items, stored 3 074 844, uncompressed 7 529 566, ratio 0.376076) so a wrong basis cannot share the answer.
      artifacts: [crates/repark-spark/src/call/plan_partitioning_score.rs, crates/repark-spark/src/tests/plan_partitioning.rs, crates/repark-spark/src/call/plan_partitioning_bytes.rs]
    - id: AT-2
      status: ATTACKED
      evidence: End-to-end on real parquet footers — the compressible-fixture pin recomputes the footer sums independently (parquet-rs metadata, not the production measure) and asserts the projection is the compressed sum at target 1; the corrupted-footer pin walks the fallback path on a live table.
      artifacts: [crates/repark-spark/src/tests/plan_partitioning.rs, crates/repark-spark/src/call/plan_partitioning.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The unreadable-footer path is pinned loud — `0.55 (fallback)` on every row plus the count 90 = ceil(Σ stored/0.55 × 0.55 / target), proving the fallback basis is the uncompressed estimate and not the stored bytes folded a second time.
      artifacts: [crates/repark-spark/src/tests/plan_partitioning.rs, crates/repark-spark/src/call/plan_partitioning_bytes.rs]
    - id: AT-4
      status: N/A
      justification: No shared or mutable state, no concurrency — the measure reads footers sequentially over the table's FileIO and the scorer is a pure function over its inputs.
    - id: AT-5
      status: N/A
      justification: No privileged action, no credential, no network — parquet footer reads through the existing FileIO and local memory-catalog fixtures only.
    - id: AT-6
      status: ATTACKED
      evidence: The integrity claim is the formula itself — compression is applied exactly once (the uncompressed share folded by the ratio equals the inputs' own compressed sum, the floor for a same-rows rewrite); the residue note keeps the measured-error record and names the remaining S2-24 gap instead of claiming exactness.
      artifacts: [crates/repark-spark/src/call/plan_partitioning.rs, crates/repark-spark/src/call/plan_partitioning_score.rs]
    - id: AT-7
      status: ATTACKED
      evidence: No new work per row — the uncompressed sums come from the footer metadata the measure already decodes; fallback constructs the basis with one division per file.
      artifacts: [crates/repark-spark/src/call/plan_partitioning_bytes.rs]
    - id: AT-8
      status: ATTACKED
      evidence: No second byte path — `PlanInputs.sizes` takes the measure's per-file uncompressed sums in `paths` order; the fallback estimate lives in the same measure, so scoring sees one basis either way. `projected_partitions` is untouched (D-3).
      artifacts: [crates/repark-spark/src/call/plan_partitioning.rs, crates/repark-spark/src/call/plan_partitioning_score.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The public spelling is pinned — `byte_ratio=<r rounded 2 places> (footers|fallback)` on every row's notes, the reworded AP-0-R-001 residue note asserted by `plan_every_row_carries_the_r001_file_count_caveat`, and the guide's known-issues line states the S2-24 residual.
      artifacts: [crates/repark-spark/src/call/plan_partitioning.rs, docs/guide/maintenance-policy.md, crates/repark-spark/src/tests/plan_partitioning.rs]
    - id: AT-10
      status: ATTACKED
      evidence: The pins are the reproduce record — 3 of 26 failed red on the base tree (3 vs 6; 252 vs 142; 50 vs 90, pasted above) and all 26 pass after; the two rank pins that changed targets document the basis move in their assert text.
      artifacts: [task/ledgers/staging/ap-3-ledger.md, crates/repark-spark/src/tests/plan_partitioning.rs]
  complete: true
```

## Perf review (S2-21, Grok, read-only, 2026-09-12)

No P1, no P2: footer I/O stays one pass per file; the 5,000-file scoring shows no measurable regression against the copied base; `u64` → `f64` is exact below 2^53. One P3 recorded, not remediated: `files.sizes: Vec<u64>` stays live beside the new `uncompressed: Vec<f64>` through scoring (8 N B, 40 KB at 5,000 files) — the fallback is the only remaining reader of stored bytes; reuse or drop it if the walk is ever tightened. Report: the orchestrator's review log for this unit.
