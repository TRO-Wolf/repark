# Unit ledger — RDF-SCHEMA-EVO-1 · `rewrite_data_files` after schema evolution

**Date:** 2026-09-06 · **Branch:** `fix/rdf-schema-evo-1` · **Base:** `origin/main` `1883968b` ·
**Model:** muse-spark-1.3 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Rubric:** STANDARD. `risk_tier: standard`.
**Registry:** `RDF-SCHEMA-EVO-1` **FIXED** at RP-15 (`85db42f2`, fork `#272`, merged 2026-09-06; pins green on the bumped pin — §8).

**Retires:** this ledger moves to `../completed/` when the pin-bump unit lands.

**Why now.** The owner hit a hard refusal in production-shaped compaction: after `ADD COLUMN`
(7 → 8 columns) and `ADD PARTITION FIELD`, `CALL rewrite_data_files` raised `DataInvalid,
context: { batch_columns: 7, expected_columns: 8 }`. Compaction plans its read tasks under
the snapshot-pinned old schema while the partition calculator and writer build on the current
schema. The fix is fork-side; this unit reproduces every evolution shape on a local warehouse,
measures the Spark oracle, and pins the fixed behavior so the pin bump is a pure version move.

**Not in this unit:** any RePark production code (none changes); the fork pin (`make
bump-fork-pin` is the orchestrator's, after the fork PR merges); `STATUS.md`;
`briefs/next-sequence.md`; sort/zOrder strategies (`RDF-SORT-1`).

**Writable paths:** `python/repark/tests/{test_rdf_schema_evo_1.py,map.md}`,
`docs/spark-sql-iceberg-parity.md` (one row), this ledger and its `staging/map.md` row.
Closed: `Cargo.toml`, `Cargo.lock` (the measurement patch is reverted before hand-back),
every dependency, `.github/`, every other ledger.

## Plan

- [x] Reproduce on the pinned fork (`8bc325a3`): the owner shape and every step-1 shape.
- [x] Measure the same shapes on live PySpark 4.1.2 + Iceberg 1.11.0 (re-run, not carried).
- [x] Verify end to end against the local fork (uncommitted path patch, reverted after).
- [x] Pins red on the pin, green on the fix; registry row; maps; gates.

## PROPOSITION LEDGER — RDF-SCHEMA-EVO-1 — 2026-09-06

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | After `ADD COLUMN note` + `ADD PARTITION FIELD bucket(4, id)` with no later write, `rewrite_data_files` succeeds and old rows survive with NULL `note`, Spark-equal. | The owner pin, red on `8bc325a3` with the owner's exact error, green on the fix with Spark's 6→3 layout. | **PROVEN** | `test_rewrite_after_add_column_and_partition_field_matches_spark`: red on the pin — `DataInvalid, { batch_columns: 7, expected_columns: 8 }`; green on the fix — rewritten 6, added 3, ids 0..5, `note` all NULL, files `[(1,1),(1,2),(1,3)]`. Spark oracle: 6→3, buckets 0:3 / 3:2 / 2:1, same rows. |
| C-002 | Add-column-only on a partitioned table compacts 6→1; the unpartitioned add-only shape is unaffected (control). | The partitioned pin red→green; the unpartitioned pin green on both sides. | **PROVEN** | `test_rewrite_after_add_column_only_partitioned`: red 7v8, green 6→1, one `(0,6)` file. Spark partitioned oracle: 6→1, one 6-record `d=True` file. `test_rewrite_after_add_column_only_unpartitioned_is_unaffected`: green on both sides, 6→1 (no calculator on the unpartitioned path). |
| C-003 | After `DROP COLUMN`, kept columns survive and output matches the current spec. | The drop pin red→green with Spark's 6→3 layout. | **PROVEN** | `test_rewrite_after_drop_column_matches_spark`: red 7v6, green 6→3, schema `[id,a,b,c,d,e]`, files `[(1,1),(1,2),(1,3)]`. Spark: 6→3, same rows minus `f`. |
| C-004 | After `RENAME COLUMN`, values survive under the new name. | The rename pin red→green with Spark's 6→3 layout. | **PROVEN** | `test_rewrite_after_rename_column_matches_spark`: red (`f` vs `f2` at position 6), green 6→3, schema ends `f2`, files `[(1,1),(1,2),(1,3)]`. Spark: 6→3, same rows. |
| C-005 | After INT→BIGINT promotion of a data column and of a partition source, values survive widened. | Both promote pins red→green with Spark's 6→1 layouts. | **PROVEN** | `test_rewrite_after_promote_column_matches_spark`: red (parquet writer Int64-vs-Int32), green 6→1, `id` int64. `test_rewrite_after_promote_partition_source_matches_spark`: red (partition struct Int64-vs-Int32), green 6→1. Spark: 6→1 on both shapes, `bigint` schema. |
| C-006 | A v3 table with a deletion vector compacts after schema+spec evolution; the deleted row stays gone and the DV leaves with its file. | The v3 pin red→green; rows and surviving files equal Spark's; the count delta recorded, not hidden. | **PROVEN** | `test_rewrite_v3_deletion_vectors_after_evolution_matches_spark`: red 4v5, green 6/3/1, rows `[0,2,3,4,5]`, files `[(1,1),(1,2),(1,2)]`, zero delete files. Spark: 5/3/0 — it skips the fully-deleted file where the fork rewrites it and drops its DV (standing F-16 semantic); rows and the surviving multiset are equal. |
| C-007 | A post-evolution write self-heals: the same CALL succeeds with and without the fix. | The with-write pin green on both sides, with the mechanism stated. | **PROVEN** | `test_rewrite_with_post_evolution_write_is_unaffected`: green on both sides (rewritten ≥ 6, 8 rows, new rows keep their notes). Mechanism: the write commits a new snapshot whose schema is current, so the planned tasks already carry it. |
| C-008 | `rewrite_position_delete_files` and `rewrite_manifests` succeed on the evolved table with Spark's zeros and intact rows. | Both characterization pins green; zeros asserted, not just success. | **PROVEN** | `test_rewrite_position_delete_files_on_evolved_table`: 0/0, rows `[0,2,3,4,5]`. `test_rewrite_manifests_on_evolved_table`: 0/0, 6 rows. Spark returns 0/0 and 0/0 on the same shapes. Green on both sides by measurement (neither procedure hits the compaction read path). |
| C-009 | No dependency moves: `Cargo.toml` and `Cargo.lock` are byte-identical to the base at hand-back. | The diff; the patch revert. | **PROVEN** | `git diff origin/main -- Cargo.toml Cargo.lock` empty after the measurement patch was reverted (§8). The pin bump is the orchestrator's next step, not this unit's. |

VERDICT: 9 clauses, 9 PROVEN, 0 OPEN, 0 REJECTED.

```
COVERAGE_ATTESTATION:
  pr_unit: rdf-schema-evo-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause is stated against a red-then-green pin or a measured both-sides control. Counts are asserted, not just row identity: a no-op CALL (rewritten 0) reds every load-bearing pin.
      artifacts: [python/repark/tests/test_rdf_schema_evo_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Add (+spec), add-only partitioned and unpartitioned, drop, rename, promote of a data column and of a partition source, v3 with a real DV, a post-evolution-write control, and both sibling procedures. Spark door only: MAINTENANCE_CALL and ALTER_TABLE_PARTITION_FIELDS are DeliberatelyAbsent on the ANSI door (matrix.rs).
      artifacts: [python/repark/tests/test_rdf_schema_evo_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The v3 pin asserts the DV exists before evolving (a missing DV fails the pin, not the proof); the promote pins assert the widened Arrow type, not just values; the rename pin asserts the full schema name list.
      artifacts: [python/repark/tests/test_rdf_schema_evo_1.py]
    - id: AT-4
      status: N/A
      justification: No RePark production code changes; no new shared state or concurrency. The fix is fork-side and single-threaded per task stream.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM or secret handling. No .github change. Cargo.toml and Cargo.lock byte-identical to origin/main at hand-back.
      artifacts: [Cargo.toml, Cargo.lock]
    - id: AT-6
      status: ATTACKED
      evidence: No public API change. test_maintenance_call.py 15/15 green on the patched native alongside the 11 new pins.
      artifacts: [python/repark/tests/test_maintenance_call.py]
    - id: AT-7
      status: ATTACKED
      evidence: Every load-bearing pin reds on the pinned fork with the production error and greens on the fix (§6). The v3 count delta (6/3/1 vs Spark 5/3/0) is a recorded standing semantic, not a hidden divergence.
      artifacts: [task/ledgers/staging/rdf-schema-evo-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: Spark cells re-measured by this session (three JVM runs, each stopped; multiset-equal to the prior session's two runs). RePark cells measured on the pinned native (red) and the patched release native (green).
      artifacts: [task/ledgers/staging/rdf-schema-evo-1-ledger.md]
    - id: AT-9
      status: N/A
      justification: No AWS surface touched; local warehouse only. The owner's S3 Tables report is the trigger, never a touched system.
    - id: AT-10
      status: ATTACKED
      evidence: The fork pin does not move in this unit; STATUS.md, briefs/next-sequence.md and .github untouched.
      artifacts: [docs/spark-sql-iceberg-parity.md, Cargo.toml]
  complete: true
```

## 6. Red-first and mutation (docs/testing.md "Gate provocation proofs")

| # | provocation | pins that redden |
|---|---|---|
| R1 | the pinned release native (fork `8bc325a3`) under the 11 pins | 7 red out of 11: owner 7v8, add-only-partitioned 7v8, drop 7v6, rename `f`-vs-`f2`, promote parquet Int64-vs-Int32, promote-partition-source struct Int64-vs-Int32, v3 4v5 (verbatim in C-001..C-006). The 4 greens are the two controls and the two characterizations, green by design. |
| R2 | the patched release native (fork `fix/rdf-schema-evo-1` `8ef7ef5b`) under the 11 pins + `test_maintenance_call.py` | 0 red out of 26: 11/11 green, 15/15 green. The `.so` mtime guard confirms R1 ran fully on the pre-fix binary. |
| M1 | fork-side: the read-path re-pointing reverted (`task.schema` / `project_field_ids` back to snapshot-pinned) | 7 red out of 10 fork pins (`rewrite_data_files_evolved_schema_tests`), the 3 unpartitioned controls green. Re-run by this session via `git stash`. |
| M2 | fork-side: the manifest-stats widening reverted (`promote_to` out of `PartitionFieldStats::update`) | the partition-source promotion pin reds at commit (`value is not compatible with type`); the other 9 stay green. |

None is committed. The measurement patch (`Cargo.toml` path patch + lock re-resolve) is reverted
before hand-back (§8); `Cargo.lock` was never committed changed.

## 7. Spark oracle cells (all re-measured 2026-09-06, PySpark 4.1.2 + Iceberg 1.11.0)

Seed: 7 columns `(id INT, a STRING, b BIGINT, c DOUBLE, d BOOLEAN, e DATE, f TIMESTAMP)`,
6 single-row files ids 0..5, no later write unless noted. Every shape succeeds on Spark.

| shape | CALL result | rows after | `.files` after |
|---|---|---|---|
| owner add + `bucket(4,id)` | 6 / 3 / 0 | 6 rows, `note` NULL | 3 files, spec 1: bucket 0:3, 3:2, 2:1 |
| add-only, unpartitioned | 6 / 1 / 0 | 6 rows, `note` NULL | 1 file, spec 0, 6 records |
| add-only, `PARTITIONED BY (d)` | 6 / 1 / 0 | 6 rows, `note` NULL | 1 file, spec 0, 6 records, `d=True` |
| drop `f` + `bucket(4,id)` | 6 / 3 / 0 | 6 rows, 6 columns | 3 files, spec 1: 3/2/1 |
| rename `f`→`f2` + `bucket(4,id)` | 6 / 3 / 0 | 6 rows, `f2` kept | 3 files, spec 1: 3/2/1 |
| promote `id`→BIGINT, `identity(v)` | 6 / 1 / 0 | 6 rows, `bigint` | 1 file, spec 1, `v=100` |
| promote partition source (all `id`=7) | 6 / 1 / 0 | 6 rows, `bigint` | 1 file, spec 1, `id=7` |
| v3 + DV, add + `bucket(4,id)` | 5 / 3 / 0 | 5 rows (`id` 1 gone), `note` NULL | 3 files, spec 1: 2/2/1 |
| `rewrite_position_delete_files`, evolved | 0 / 0 | rows intact | unchanged |
| `rewrite_manifests`, evolved | 0 / 0 | rows intact | unchanged |

Counts are `rewritten / added / removed-or-failed` as each procedure reports them. Byte counts
and path tails are run-varying and are not pinned. The RePark pins assert the same row sets,
schemas and file multisets; the two recorded deltas are the v3 rewrite count (C-006) and —
resolved during pinning, kept here as the trail — that the brief's literal with-write sequence
self-heals on the pin (C-007).

## 8. F-trigger (what the pin bump flips)

- **Fork branch:** `fix/rdf-schema-evo-1` (owner's iceberg-rust fork).
- **Fork commit:** `8ef7ef5b` — `[fork] fix: F-RDF-EVO-1 — rewrite_data_files projects
  old-file batches to the current schema (row R135)`.
- **Merged as:** fork `#272`, squash `85db42f2` on the fork's `main` (2026-09-06); fork critic PASS
  (10/10 pins, 7 red under the batch revert, 12 attack shapes with no lost value). RePark pin bump
  RP-15 `8bc325a3` → `85db42f2` by the orchestrator (`make bump-fork-pin`), rows in
  `docs/fork-sync.md` and the root `map.md`; the eleven pins re-run green on the bumped native
  (see §10).
- **What it changes:** `crates/iceberg/src/maintenance/rewrite_data_files_write.rs` re-points
  each compaction read task at the current schema with the full current projection, so the
  Arrow reader evolves every file's batches before the splitter and writer see them;
  `crates/iceberg/src/spec/values/primitive.rs` gains `PrimitiveLiteral::promote_to`
  (int→long, float→double) applied in `PartitionFieldStats::update`, so old carried tuples
  widen against the current partition type at manifest commit. Fork tests:
  `maintenance/rewrite_data_files_evolved_schema_tests.rs` (10 pins) + the `promote_to`
  unit test; maintenance battery 332/332, `make check` green.
- **RePark trigger:** when the fork PR merges, the orchestrator runs `make bump-fork-pin` to
  this commit. The 11 pins in `test_rdf_schema_evo_1.py` flip from red to green with no other
  change, and registry row `RDF-SCHEMA-EVO-1` flips BACKLOG → FIXED. No RePark source change
  is part of that bump.

## 9. Gates

- `make verify` exit 0 (with the patch in place; tree verified clean of it after §8's revert).
- `pytest python/repark/tests/test_rdf_schema_evo_1.py
  python/repark/tests/test_maintenance_call.py` — 26 passed.
- `make py-test-dbt`, `make check-map-sync`, `make check-ledger-grammar`, `make check-ledgers`,
  `make check-docs-compaction`, `ledger_lifecycle.py check --base origin/main`, `typos` — exit 0.
