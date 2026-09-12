# Errata — RP-16 re-measure (2026-09-11)

**Model:** grok-4.6. **Branch:** `feat/ap-1-remeasure`. **Base:** RP-16
`864e3483` (fork pin `090bc821`, F-WRITE-COMPRESS-1 / fork `#276`).
Measurement only: no Rust or Python source changed. Clause verdicts below are
untouched. This note sits at the top because `completed/` ledgers are frozen
except a prepended errata.

The residue's named mechanism is gone. Independent `pyarrow.parquet` over
`/tmp/ap1r-bed/warehouse/repark_ctas/ap/ns/<bed>/data/*.parquet` on the release
module (`__debug_assertions__ == False`) reads **ZSTD** on every column chunk of
every bed, including the two INSERT-grown synthetics:

| Bed | Codec (AP-1 step 2) | Codec (this run) | byte_ratio (AP-1) | byte_ratio (this run) | On-disk bytes (this run) |
|---|---|---|---|---|---|
| futures | zstd (CTAS) | zstd | 0.462675 | 0.462675 | 26 729 684 |
| uniform | uncompressed (INSERT) | **zstd** | 1.000000 | 0.376076 | 3 074 844 |
| skewed | uncompressed (INSERT) | **zstd** | 1.000000 | 0.376076 | 3 074 844 |

Frame notes say `byte_ratio=0.46 (footers)` on futures and `byte_ratio=0.38
(footers)` on both synthetics. The 0.55 fallback did not fire.

`run_adaptpart.py` has `--plan` and no rewrite flag, so the O-run rewrite was
not re-run. The 20 percent check compares `Σ file_size × byte_ratio` against
AP-0's recorded O-run actuals (those actuals predate the INSERT codec change):

| Bed | Projected bytes | O-run actual | New error | AP-1 step-2 error |
|---|---|---|---|---|
| uniform | 3 074 844 × 0.376076 = 1 156 376 | 4 413 222 | **-73.8 %** | +76.2 % |
| skewed | 3 074 844 × 0.376076 = 1 156 376 | 4 134 457 | **-72.0 %** | +88.0 % |

**AP-1-R-001 still OPEN.** The projection is still more than 20 percent wrong
on both beds; the sign flipped from over-predict to under-predict because the
ratio now multiplies already-zstd stored bytes. Full frames, codecs and
reproduce commands:
[docs/perf/adapt-part-ap1-remeasure-2026-09-11.md](../../../docs/perf/adapt-part-ap1-remeasure-2026-09-11.md).
This unit's clause table:
[task/ledgers/staging/ap-1-remeasure-ledger.md](../staging/ap-1-remeasure-ledger.md).

**Owner question (not decided here):** should `byte_ratio` multiply the
footers' *uncompressed* sum rather than the stored file bytes, i.e. predict the
rewrite's own codec?

---

# Unit ledger — AP-1 step 1 (plan) · `CALL plan_partitioning()`

**Unit:** AP-1 step 1 (plan) · **Date:** 2026-09-10 · **Branch:** `feat/ap-1` · **Base:** `origin/main`
**Model:** Muse Spark (muse-spark-1.3-contributor)
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** AP-0 measured P-2/P-3 from manifest bounds and filed AP-0-R-001 (byte
projection 76–88 % high) and AP-0-R-002 (file-count column untested). This step builds the
plan half: `CALL <catalog>.system.plan_partitioning(table => …, target_file_size_bytes => …)`
returning the D-1 frame, P-2 candidates scored with exactly P-3, the R-001 caveat visible in
the output, P-5 branch refusal plus the multi-spec note. No apply, no guide section (step 2).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Not in this step:** `apply_partitioning` (AP-2), the `adaptive_partitioning` hook (AP-3),
any release-build run over the AP-0 tables, `docs/perf/adapt-part-ap1-*.md`, the
`docs/guide/maintenance-policy.md` section, any dependency file, `STATUS.md`,
`briefs/next-sequence.md`.

**Decision (no data scan).** D-2 allows no data scan in this card, while AP-0 used a bounded
head sample for the P-2 distinct-count branch. The procedure therefore derives the
identity/bucket branch from the union of manifest bound endpoints per column (≤ 1000 → identity,
else buckets): manifest-only, no data read. Recorded here so step 2 reads the same rule.

## PROPOSITION LEDGER — AP-1 step 1 — 2026-09-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `plan_days_ts_wins`: on a synthetic table whose `ts` spans 90 days at about the target per day, `days(ts)` ranks first. | CALL the procedure on the 9-file fixture; row 0 candidate is `days(ts)`. | **PROVEN** | `plan_days_ts_ranks_first_on_ninety_day_fixture`: 9 ten-day files, target = total/90; row 0 `days(ts)` at 0.0 over 90 partitions, ddl carries `days(ts)`, frame sorted best-first, winner `projected_files_at_target` > 0, `calls` chains `rewrite_data_files`. |
| C-002 | `plan_frame_shape`: the frame carries exactly `candidate`, `score`, `projected_partitions`, `projected_files_at_target`, `ddl`, `calls`, `plan_id`, `notes` with Arrow types Utf8, Float64, Int64, Int64, Utf8, Utf8, Utf8, Utf8. | Schema assertion on the collected batch. | **PROVEN** | `plan_frame_carries_d1_columns_with_arrow_types` plus the shared `plan_rows` helper asserting names and Arrow types on every collected frame. |
| C-003 | `plan_boundless_column_excluded`: a column with no readable bounds is not a candidate and the last row's `notes` names it. | Fixture `note` column (all NULL); no candidate mentions it; last-row notes contain `note`. | **PROVEN** | `plan_boundless_column_is_excluded_and_named_on_last_row`: no candidate contains `note`; last-row notes name it. |
| C-004 | `plan_unpartitioned_present`: `unpartitioned` is a candidate row. | Frame contains a row with candidate `unpartitioned`. | **PROVEN** | `plan_unpartitioned_is_a_candidate`. |
| C-005 | `plan_id_stable`: `plan_id` repeats for the same (snapshot, candidate) across two CALLs and differs across candidates. | Two CALLs, per-candidate comparison plus intra-frame uniqueness. | **PROVEN** | `plan_id_repeats_per_candidate_and_differs_across_candidates`: two CALLs agree per candidate, ids non-empty, all distinct within the frame; in-module `plan_id_is_stable_per_pair_and_unique_per_candidate`. |
| C-006 | `plan_r001_note_everywhere`: every row's `notes` names AP-0-R-001 and states the pre-rewrite-byte caveat. | Substring assertion on all rows. | **PROVEN** | `plan_every_row_carries_the_r001_file_count_caveat`: every row's notes contain `AP-0-R-001` and `projected_files_at_target`. |
| C-007 | `plan_branch_refuses`: a table with a branch besides `main` refuses the CALL. | CREATE BRANCH then CALL errors naming the branch. | **PROVEN** | `plan_branch_besides_main_refuses`: `CREATE BRANCH feat` then the CALL errors naming `feat`, `branch`, `main`. |
| C-008 | `plan_multispec_noted`: a table with more than one partition spec plans with a note that apply would rewrite to one spec. | ADD PARTITION FIELD then CALL succeeds; notes contain `one spec`. | **PROVEN** | `plan_multi_spec_table_reports_the_single_spec_rewrite`: after `ADD PARTITION FIELD days(ts)` the CALL succeeds and a row's notes contain `one spec` (spec count read from table metadata, not from files, so a new spec with no files yet still reports). |
| C-009 | `plan_identity_wins`: on a 4-region table at ~2x target per region, `identity(region)` ranks first with score 0. | CALL on the region fixture; row 0 is `identity(region)` at 0.0. | **PROVEN** | `plan_identity_region_ranks_first_at_twice_target`: 4 single-region files, target = total/8; row 0 `identity(region)` at 0.0 over 4 partitions. |

VERDICT: 9 clauses, 9 PROVEN, 0 OPEN, 0 REJECTED.

## Red first

All 9 pins were written before the procedure existed and run against the base tree.
They fail because there is nothing to run — the honest red:

```text
test result: FAILED. 0 passed; 9 failed; 0 ignored; 0 measured; 895 filtered out; finished in 0.17s
plan_partitioning runs: NotImplemented("CALL system.plan_partitioning is not supported. Supported procedures: expire_snapshots, register_table, rewrite_data_files, rewrite_manifests, remove_orphan_files, rewrite_position_delete_files, rollback_to_snapshot, run_maintenance.")
```

Two fixture corrections landed before green, both in test code only: the first red
also caught a weak C-007 pin (DataFusion renders `NotImplemented` as "This feature is
not implemented", which matched the `feat`/`main` substrings), so the pin now requires
the word `branch`; and the door exposes no SQL `date_add`, so the 90-day fixture renders
its days as `TIMESTAMP '…'` literals in Rust instead of date arithmetic in SQL. No
production file was edited to make the red pass except adding the procedure the pins name.

## Gates

- `cargo test -p repark-spark --lib plan_partitioning`: green (23 passed).
- `make verify`: green.
- Comment fence (`git diff --cached` grep for added `//`/`#` lines): prints nothing.

# Step 2 — the footer `byte_ratio`, the three-bed run, and residue AP-1-R-001

**Model:** swe-2-high
**Date:** 2026-09-11 · **Branch:** `feat/ap-1-step-2` · **Base:** step 1 merged on `main`

**Scope.** Card AP-1 step 2 as amended by ruling S2-10 with orchestrator decisions
D-4/D-5 (run 7): replace the pre-rewrite byte sum behind `projected_files_at_target`
with `byte_ratio = Σ total_compressed_size / Σ total_uncompressed_size` over every
column chunk of every live data file's parquet footer — ranged tail reads through the
table's own `FileIO`, never row-group data — falling back to the measured constant
0.55 (AP-0's O-run, 0.53–0.57×) when any footer is unreadable. Every row's `notes`
carries `byte_ratio=<r rounded to 2 places> (footers|fallback)`; the AP-0-R-001 caveat
stays, reworded to say the projection applies the ratio. `score` and
`projected_partitions` keep the raw P-3 model — D-4 amends the file-count projection
only. Then: the three AP-0 beds on the release module into
`docs/perf/adapt-part-ap1-2026-09-11.md`, the guide section, maps in lockstep. AP-2
apply and AP-3 remain out of scope.

**Discovered while seeding the pins:** repark's two write paths emit different
codecs — `INSERT INTO` passes through the fork's `iceberg-datafusion` task writer at
parquet-rs's uncompressed default, while CTAS writes through `repark-iceberg`'s
`writer_properties_for` (zstd). The footer pin's fixture is CTAS-built for that
reason (an INSERT-grown table measures 1.0), and the same split is what the
synthetic beds show below.

## PROPOSITION LEDGER — AP-1 step 2 — 2026-09-11

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-010 | `plan_footer_byte_ratio`: every row's `notes` carries `byte_ratio=<r> (footers)` where r equals the independently recomputed Σ compressed / Σ uncompressed over the fixture's footers and is strictly inside (0, 1); `projected_files_at_target` derives from bytes × r. | CALL on a CTAS-built compressible memory-catalog fixture; recompute the footer sums in the test through `datafusion::parquet`; compare every row's notes and the `unpartitioned` row's count to `ceil(Σ bytes × r / target)`. | PROVEN | pin `plan_byte_ratio_is_measured_from_parquet_footers` — red first (pasted below); green after `plan_partitioning_bytes.rs` |
| C-011 | `plan_byte_ratio_fallback`: one unreadable footer flips the whole CALL to `FALLBACK_BYTE_RATIO = 0.55` (named constant beside the other penalties) with source `fallback` on every row, and the projection folds by 0.55. | Overwrite one data file with non-parquet bytes, CALL, assert `byte_ratio=0.55 (fallback)` on every row and the `unpartitioned` count equals `ceil(Σ bytes × 0.55 / target)`. | PROVEN | pin `plan_byte_ratio_falls_back_when_a_footer_is_unreadable` — red first (pasted below); green after the same change |
| C-012 | `plan_release_bed_run`: the three AP-0 beds under `run_adaptpart.py --plan` on the release module answer the full D-1 frame; the perf doc records the top three rows verbatim per bed, the measured `byte_ratio` and its source, and the new projection's error against the O-run actuals (uniform 4,413,222; skewed 4,134,457), filing AP-1-R-001 if the error stays above 20 %. | Build the beds at the script's defaults, run the CALL per bed, paste the frames, compute the errors. | PROVEN | docs: docs/perf/adapt-part-ap1-2026-09-11.md — futures `byte_ratio=0.46 (footers)` (independent recompute 0.462675), uniform/skewed `1.00 (footers)`; projected bytes unchanged at 7,773,590 vs actuals 4,413,222/4,134,457 → +76.2 % / +88.0 %, above 20 % → residue filed |
| C-013 | `plan_guide_and_maps`: `docs/guide/maintenance-policy.md` documents the CALL, the frame columns, the byte-ratio note and its fallback, and the plan-only boundary; every affected `map.md` updated in the same commits. | Section present and links resolve; maps carry the new file and the changed entries. | PROVEN | docs: docs/guide/maintenance-policy.md#planning-a-partition-spec--call-plan_partitioning; map edits committed beside the code |

## Step 2 red first

The two pins were written against the step-1 tree (implementation absent) and run
before `plan_partitioning_bytes.rs` existed. An earlier draft seeded the fixture by
`INSERT` and red-flagged on the *premise* (`got 1` for the strictly-inside-(0,1)
assert) — which is how the INSERT/CTAS codec split above was measured; the fixture
moved to CTAS and the pins were re-run on the base tree:

```text
running 2 tests
test tests::plan_partitioning::plan_byte_ratio_is_measured_from_parquet_footers ... FAILED
test tests::plan_partitioning::plan_byte_ratio_falls_back_when_a_footer_is_unreadable ... FAILED

failures:

---- tests::plan_partitioning::plan_byte_ratio_is_measured_from_parquet_footers stdout ----

thread 'tests::plan_partitioning::plan_byte_ratio_is_measured_from_parquet_footers' (1618258) panicked at crates/repark-spark/src/tests/plan_partitioning.rs:565:9:
every row carries the footer-measured ratio, got: AP-0-R-001: projected_files_at_target derives from pre-rewrite file bytes and measured 76-88% high on the AP-0 beds; projected_partitions measured exact

---- tests::plan_partitioning::plan_byte_ratio_falls_back_when_a_footer_is_unreadable stdout ----

thread 'tests::plan_partitioning::plan_byte_ratio_falls_back_when_a_footer_is_unreadable' (1618257) panicked at crates/repark-spark/src/tests/plan_partitioning.rs:599:9:
every row reports the fallback ratio, got: AP-0-R-001: projected_files_at_target derives from pre-rewrite file bytes and measured 76-88% high on the AP-0 beds; projected_partitions measured exact

failures:
    tests::plan_partitioning::plan_byte_ratio_falls_back_when_a_footer_is_unreadable
    tests::plan_partitioning::plan_byte_ratio_is_measured_from_parquet_footers

test result: FAILED. 0 passed; 2 failed; 0 ignored; 0 measured; 924 filtered out; finished in 1.72s
```

## Step 2 gates

- `cargo test -p repark-spark --lib plan_partitioning`: green (25 passed, incl. the two new pins).
- `cargo clippy --locked -p repark-spark --all-targets -- -D warnings -A clippy::disallowed_methods`: clean.
- Bed run: `run_adaptpart.py --scratch /tmp/ap1-bed --futures-parquet /tmp/ap1-src/test_futures.parquet --plan` on the release module — frames pasted in the perf doc.
- `make verify`: green (workspace clippy `-D warnings`, panic ban, all static gates, full Rust suite).
- `python3 scripts/check_docs_links.py`: clean (753 files, 4 825 links).
- `python3 scripts/check_ledger_grammar.py`: clean (94 ledgers, 590 clauses).
- Parity suite `uv run --no-project python -m pytest python/repark-parity/tests -q`: 719 passed, 11 xfailed.
- Comment fence (`git diff --cached` grep for added `//`/`#` lines, attributes excepted): prints nothing.

## Step 2 residue

- **AP-1-R-001** — `projected_files_at_target` is still more than 20 % high on the
  two synthetic beds (+76.2 % uniform, +88.0 % skewed against the O-run actuals),
  unchanged from step 1's raw sum. Mechanism: `byte_ratio` measures the *input*
  files' stored codec ratio, and these beds' files are uncompressed (INSERT path),
  so the ratio is 1.0 while the O-run rewrite emitted zstd at 0.53–0.57×. Where
  input and rewrite share a codec the footer signal is real (futures: 0.462675 —
  the `unpartitioned` projection drops from 51 raw-sum files to 24). The 0.55
  fallback never fired on these beds. Follow-up lever (not taken here): project at
  the *rewrite's* codec or carry a learned per-table compaction factor — AP-0's own
  "what is wrong" note points the same way.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ap-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause was re-derived from the pins rather than read off the card — the 9 pins failed red on the base tree (0 passed, exact NotImplemented text pasted above) and pass green after, 23 tests total including the red-caught C-007 strengthening. Step 2 adds two more door pins, also red first — `plan_byte_ratio_is_measured_from_parquet_footers` (notes carry the independently recomputed footer-sum ratio, strictly inside (0,1) on the CTAS fixture, and the unpartitioned count equals `ceil(bytes × ratio / target)`) and `plan_byte_ratio_falls_back_when_a_footer_is_unreadable` (one corrupted file flips every row to `byte_ratio=0.55 (fallback)` and folds the projection by 0.55) — 25 tests total.
      artifacts: [crates/repark-spark/src/tests/plan_partitioning.rs, crates/repark-spark/src/call/plan_partitioning.rs, crates/repark-spark/src/call/plan_partitioning_bytes.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Both temporal and identity winners pinned end to end (90-day ts table, 4-region table), plus the boundless-column exclusion, the unpartitioned candidate, and the branch/multi-spec tables; bucket and pair paths pinned at unit level (even N split, 50-cell cross product) since no fixture spans them. Step 2's footer reads go through the table's FileIO as ranged tail reads — the last 8 bytes, then the extent the parquet NeedMoreData hint names — and the three-bed run exercised the real CALL end to end on the release module via `run_adaptpart.py --plan`.
      artifacts: [crates/repark-spark/src/tests/plan_partitioning.rs, crates/repark-spark/src/call/plan_partitioning_score.rs, crates/repark-spark/src/call/plan_partitioning_bytes.rs, python/repark-parity/bench/adaptpart/run_adaptpart.py, docs/perf/adapt-part-ap1-2026-09-11.md]
    - id: AT-3
      status: ATTACKED
      evidence: Branch refusal, missing/zero/negative target and unknown-key refusals all pinned loud with the key named; the multi-spec case plans with a note instead of refusing, pinned too. Step 2 degrades footer failures to the 0.55 fallback with a `(fallback)` tag in notes rather than refusing the plan.
      artifacts: [crates/repark-spark/src/tests/plan_partitioning.rs]
    - id: AT-4
      status: N/A
      justification: No shared or mutable state, no concurrency: one CALL collects its metadata batches, scores them single-threaded, and answers one frame; the fixtures run in isolated memory catalogs.
    - id: AT-5
      status: N/A
      justification: No privileged action, no credential, no network. All reads are local memory-catalog metadata tables; step 2 adds ranged footer reads of the live data files through FileIO — still local paths under the table's own warehouse, still no credential or network.
    - id: AT-6
      status: ATTACKED
      evidence: The P-1 integrity claim (manifests first, no data scan) was attacked by construction and by pin: the procedure issues SQL only against the `files` and `refs` metadata tables and reads the schema from loaded table metadata, and the all-NULL column test proves a boundless column is excluded without any data read. Step 2 keeps the bound — the footer reader consumes only each file's parquet tail through FileIO and never decodes a row group; the bed run confirms it reports the codecs as stored (futures 0.46 zstd, synthetics 1.00 uncompressed).
      artifacts: [crates/repark-spark/src/call/plan_partitioning.rs, crates/repark-spark/src/call/plan_partitioning_bytes.rs]
    - id: AT-7
      status: ATTACKED
      evidence: The work scales with the manifest row count plus the truncated-range expansion, both bounded by the table's own file layout; the whole battery (a 9-commit and a 4-commit fixture plus eight small tables) runs in under two seconds.
      artifacts: [crates/repark-spark/src/tests/plan_partitioning.rs]
    - id: AT-8
      status: ATTACKED
      evidence: The two upstream shapes the procedure could have presumed were checked against the fork source, not presumed: `refs` type strings (`BRANCH`/`TAG` in `inspect/refs.rs`) and the `files` columns (`content`, `file_size_in_bytes`, `readable_metrics` in `inspect/data_file.rs`). The first multi-spec attempt read spec ids from `files` and missed a spec with no files yet; detection moved to table metadata, pinned by C-008.
      artifacts: [crates/repark-spark/src/call/plan_partitioning.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Every refusal names the table, the key and the offending value; every plan row carries its DDL, its follow-up CALLs, its plan id and the R-001 caveat, and the last row names each skipped column with its reason. Step 2 keeps the D-1 frame shape — the ratio lands in `notes` as `byte_ratio=<r> (footers|fallback)` per D-5, no new column.
      artifacts: [crates/repark-spark/src/call/plan_partitioning.rs]
    - id: AT-10
      status: ATTACKED
      evidence: The pins are the reproduce record — 9 of 9 failed red before the procedure existed and pass after, with the red output pasted verbatim above; adequacy was attacked once already (the weak C-007 substring the base tree satisfied for the wrong reason) and the pin was strengthened to require the word `branch`. Step 2 repeated the discipline — both byte-ratio pins failed red on the step-1 tree (0 passed, pasted in the Step 2 section) and the failed premise assert on the first draft is what surfaced the INSERT/CTAS codec split.
      artifacts: [task/ledgers/staging/ap-1-ledger.md, crates/repark-spark/src/tests/plan_partitioning.rs]
  complete: true
```
