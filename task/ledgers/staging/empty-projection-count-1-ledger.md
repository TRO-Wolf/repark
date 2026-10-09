# Unit ledger — EMPTY-PROJECTION-COUNT-1 · `COUNT(*)` over the changelog, incremental and lineage readers

**Date:** 2026-10-09 · **Branch:** `fix/empty-projection-count-1` · **Base:** `291b39f4`
**Model:** Muse Spark (muse-spark-1.3-contributor) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Card EMPTY-PROJECTION-COUNT-1 (filed 2026-10-09 by the MB-4 lane): `COUNT(*)`
plans an empty projection, and `crates/repark-iceberg/src/catalog/scan_batches.rs::conform_batch`
rebuilds the batch with `RecordBatch::try_new`, which cannot infer a row count from zero
columns — every reader that reaches an empty scan projection fails with the engine-internal
`iceberg scan could not rebuild batch: ... must either specify a row count or at least one
column`. Step 1 measured all four callers on the base tree plus the live Spark oracle; step 2
fixes the shared site with an explicit row count.

**Not in this unit:** `STATUS.md`, `briefs/`, `.github/`, `Cargo.toml`/`Cargo.lock`,
`pyproject.toml`, `uv.lock`, any fork (`iceberg-rust`) change, and the MB-4 branch's local
`zero_column_batch` fix (commit `a3426ee5` on that branch; the shared fix here makes it
redundant — the later merge must reconcile, see D-1). The MB-4 branch also carries an open copy
of this card; step 4 files a closed copy under `task/roadmap/mid-term/` here, and the merge
must reconcile the two.

## PROPOSITION LEDGER — EMPTY-PROJECTION-COUNT-1 — 2026-10-09

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The caller audit is complete: `scan_batches::conform_batch` has exactly four callers — `catalog/changelog.rs`, `catalog/incremental_append.rs`, `catalog/lineage_columns.rs`, `microbatch/provider.rs` — and `catalog/metadata_columns.rs` owns a separate copy that already serves empty projections. | `grep conform_batch` over `crates/repark-iceberg/src` plus the base measurement below. | PROVEN | §1 audit, 2026-10-09: four `use ...scan_batches::{conform_batch...}` call sites, one local already-fixed copy, zero other rebuild sites on the read path. |
| C-002 | The changelog reader answers every empty-projection spelling with the changelog row count: `COUNT(*)`/`COUNT(1)`/`SELECT 1`/`df.count()`/`EXISTS` over v2 (5), v3 (4), single-append (2), ten-append (20) and MOR-delete (7) histories. | Facade pins in `test_empty_projection_count_1.py`, red on base, green on head. | OPEN | Red pins committed step 1; base fails all 20 empty-projection cells (§2). |
| C-003 | The incremental reader answers every empty-projection spelling with the window row count: `(S0,S2]` (3), ten-snapshot window (18), v3 window (2), delete-holding window (4). | Facade pins, red on base, green on head. | OPEN | Same red-pin commit (§2). |
| C-004 | The lineage reader answers `SELECT COUNT(*) AS _row_id` (4) and `df.count()` over `_row_id` (4) on the v3 DV fixture, and the v2 `COUNT(*) AS _row_id` still answers (1). | Facade pins, red on base except the v2 guard, green on head. | OPEN | Same red-pin commit (§2). |
| C-005 | The micro-batch provider answers `COUNT(*)` (3) over a three-file window holding an empty file, equal to its `SELECT id` row count, with per-batch counts kept through a direct empty-projection scan. | Rust provider pin, red on base, green on head. | OPEN | `provider_counts_rows_through_an_empty_projection`, committed step 1. |
| C-006 | The shared site is pinned directly: zero columns over one batch (4 rows), an empty batch (0 rows), and a three-batch stream (3, 0, 1). | Rust unit pins beside `conform_batch`, red on base, green on head. | OPEN | Three `empty_projection_*` tests in `scan_batches.rs`, committed step 1. |
| C-007 | Every neighbouring answer is unchanged: filtered counts, empty results, plain/v3/snapshot/metadata counts, the changelog view count, and the lineage `EXISTS` refusal. | Guard pins green on base and head plus the §2/§4 before/after tables. | OPEN | Base answers recorded §2; guards committed step 1. |
| C-008 | Head counts equal live Spark 4.1.2 counts for the same table histories on both shared doors. | The §3 Spark cells against the head re-run. | OPEN | Spark recorded §3, 2026-10-09: incremental 3, changes 5, v3 incremental 2, v3 changes 4. |
| C-009 | Gates are green and the card is filed closed with its map row. | The gate list in §5 plus the mid-term card copy. | OPEN | Lands with the last commit. |

## 1. Caller audit (C-001, 2026-10-09)

`grep -n conform_batch crates/repark-iceberg/src` (base `291b39f4`): the shared
`scan_batches.rs::conform_batch` is imported by `catalog/changelog.rs:21`,
`catalog/incremental_append.rs:21`, `catalog/lineage_columns.rs:29` and
`microbatch/provider.rs:19` — four readers, no other importers. (`write/conform.rs` owns a
same-named write-side helper over a different signature; unrelated.) `catalog/metadata_columns.rs`
owns a local `conform_batch` that already early-returns through
`RecordBatch::try_new_with_options` with `Some(batch.num_rows())` on an empty schema
(lines 303–314) — the in-tree precedent for the shared fix, and a neighbour that must not
change. `snapshot_metadata_table.rs` projects with `batch.project` and `changelog_view.rs`
scans its source unprojected; both answer `COUNT(*)` on base (§2 neighbours).

Filtering happens before `conform_batch` in all four readers: the changelog's
`ChangelogReader` tasks, the incremental and lineage `select` + `with_filter` scans, and the
micro-batch planned file tasks all emit post-filter batches, so the incoming batch's row count
is the true count in every case. No fork change is needed: the base failure is ours (the fork
produced the batches; our rebuild refused them).

## 2. Base measurement, facade (2026-10-09, unmodified tree, release `make develop` build)

Fixtures: v2 three-append `(1,'a','x'),(2,'b','y') / (3,'c','x') / (4,'d','y'),(5,'e','x')`
(oracleshape); v2 single-append (2 rows, one file); v2 ten-append (20 rows, ten files); v3
two-append (4 rows); v2 MOR three-append + `DELETE id = 3` + `(6,'f','x')`; the v3
`v3-spark-part-dv` fixture via `register_table` (4 live rows); a v2 one-row twin.

Failing on base — 20 cells, all with the same engine-internal text
(`iceberg scan could not rebuild batch: ... must either specify a row count or at least one
column`; the `df.count()` spelling wraps it as `datafusion engine error`):

| Reader | Failing cells |
|---|---|
| Changelog v2 | `COUNT(*)`, `COUNT(1)`, `SELECT 1`, `EXISTS`, `df.count()` |
| Incremental v2 | `df.count()`, `COUNT(*)`, `COUNT(1)`, `SELECT 1`, `EXISTS` (temp-view SQL) |
| Changelog single-file | `COUNT(*)` |
| Changelog + incremental, ten-file | `COUNT(*)` each |
| Changelog + incremental, v3 | `COUNT(*)` each |
| Incremental + changelog, deletes present | `COUNT(*)` each |
| Lineage v3 | `COUNT(*) AS _row_id`, `COUNT(1) AS _row_id`, `df.count()` over `_row_id` |

Answering on base — references and must-not-change cells:

| Cell | Base answer |
|---|---|
| Changelog refs `COUNT(id)` / row-form length | 5 / 5 |
| Incremental `(S0,S2]` refs `COUNT(id)` / row-form length | 3 / 3 |
| Single-file changelog row-form length | 2 |
| Ten-file changelog / incremental row-form lengths | 20 / 18 |
| v3 changelog / incremental row-form lengths | 4 / 2 |
| MOR-delete changelog / incremental row-form lengths | 7 / 4 |
| Filtered counts (answer: the filter forces a non-empty scan projection) | changelog `id > 2` → 3; incremental `id > 3` → 2; lineage `_row_id IS NOT NULL` → 4; lineage `id > 2` → 3 |
| Empty results (`WHERE 1 = 0`) | 0 on all three readers |
| Plain `COUNT(*)` v2 / v3 | 5 / 4 |
| `COUNT(*)` over `.snapshots` | 3 |
| `_file` row-form length / `COUNT(_file)` | 5 / 5 |
| Changelog view (`create_changelog_view` + `COUNT(*)`) | 5 (row form 5) |
| Lineage plain `COUNT(*)` (no lineage reference) | 4 |
| Lineage refs `COUNT(id)` / row-form length | 4 / 4 |
| v2 `COUNT(*) AS _row_id` / plain `COUNT(*)` | 1 / 1 |
| Lineage `EXISTS` over `_row_id` | refuses `[V3-ROWID-2]` (composed-statement rule, must keep refusing) |

The first view attempt used a wrong `CALL` spelling (`table => 'ns.t2'` outside the recorded
`short` form) and is superseded by the corrected run above. Full per-cell log:
`/tmp/epc1_measure_base.log` (54 cells) plus `/tmp/epc1_view_base.log` (4 cells).

## 3. Spark oracle (live PySpark 4.1.2, 2026-10-09)

Banner: `version=4.1.2 tz=UTC` (`local[1]`, Hadoop catalog, same logical histories as §2).
`/tmp/sparkenv/bin/python` with `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`.

| Cell | Spark answer |
|---|---|
| Incremental `(S0,S2]` `.count()` / row-form length | 3 / 3 |
| Incremental open-end `(S0,head]` `.count()` | 3 |
| `SELECT COUNT(*) FROM t.changes` / row-form length / `COUNT(id)` | 5 / 5 / 5 |
| v3 incremental `(S0,S1]` `.count()` | 2 |
| v3 `SELECT COUNT(*) FROM t.changes` | 4 |

The `.changes` door exists in Spark 4.1.2 + Iceberg 1.11. Every Spark count equals repark's
row-returning form for the same history, so the pins' expected values are Spark's values.

## 4. Head re-run (lands with the fix)

## 5. Gates (lands with the last commit)

## D-1. Merge note: the MB-4 local fix becomes redundant

MB-4 commit `a3426ee5` (`zero_column_batch` in `microbatch/provider.rs`, that branch only)
serves the provider's empty projection locally. The shared `conform_batch` fix here covers the
same path (C-005 pins it with the provider reading through the shared site), so the later merge
must drop the local branch in favour of the shared site and keep one pin family. This unit does
not touch that branch.
