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
| C-002 | The changelog reader answers every empty-projection spelling with the changelog row count: `COUNT(*)`/`COUNT(1)`/`SELECT 1`/`df.count()`/`EXISTS` over v2 (5), v3 (4), single-append (2), ten-append (20) and MOR-delete (7) histories. | Facade pins in `test_empty_projection_count_1.py`, red on base, green on head. | PROVEN | §4: 7 pins red on `028de351`, green on head; every value equals its row-returning form and Spark §3. |
| C-003 | The incremental reader answers every empty-projection spelling with the window row count: `(S0,S2]` (3), ten-snapshot window (18), v3 window (2), delete-holding window (4). | Facade pins, red on base, green on head. | PROVEN | §4: 6 pins red on `028de351`, green on head; values equal row forms and Spark §3. |
| C-004 | The lineage reader answers `SELECT COUNT(*) AS _row_id` (4) and `df.count()` over `_row_id` (4) on the v3 DV fixture, and the v2 `COUNT(*) AS _row_id` still answers (1). | Facade pins, red on base except the v2 guard, green on head. | PROVEN | §4: 2 pins red on `028de351`, green on head at 4 = row form; v2 guard green both sides at 1. |
| C-005 | The micro-batch provider answers `COUNT(*)` (3) over a three-file window holding an empty file, equal to its `SELECT id` row count, with per-batch counts kept through a direct empty-projection scan. | Rust provider pin, red on base, green on head. | PROVEN | `provider_counts_rows_through_an_empty_projection`: red on `028de351` with the rebuild error, green after the shared fix with zero product change in `provider.rs`. |
| C-006 | The shared site is pinned directly: zero columns over one batch (4 rows), an empty batch (0 rows), and a three-batch stream (3, 0, 1). | Rust unit pins beside `conform_batch`, red on base, green on head. | PROVEN | Three `empty_projection_*` tests in `scan_batches.rs`: red on `028de351`, green after the fix. |
| C-007 | Every neighbouring answer is unchanged: filtered counts, empty results, plain/v3/snapshot/metadata counts, the changelog view count, and the lineage `EXISTS` refusal. | Guard pins green on base and head plus the §2/§4 before/after tables. | PROVEN | §4: 33/33 answering cells byte-identical, 5 guard pins green both sides, 0 other changes. |
| C-008 | Head counts equal live Spark 4.1.2 counts for the same table histories on both shared doors. | The §3 Spark cells against the head re-run. | PROVEN | §4: head 5/3/4/2 equal Spark 5/3/4/2 on the same histories. |
| C-009 | Gates are green and the card is filed closed with its map row. | The gate list in §5 plus the mid-term card copy. | PROVEN | §5 all green; card filed `task/roadmap/mid-term/empty-projection-count-1-card-2026-10-09.md` with its map row. |

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

## 4. Head re-run (2026-10-09, after `ab1d7f55`, rebuilt native module)

The §2 matrix re-run verbatim: 54 cells, of which 33 previously-answering cells are
byte-identical (values, types, and the `[V3-ROWID-2]` refusal), all 20 previously-failing
cells answer, and 0 cells changed in any other way. The 4 view cells are identical too
(`COUNT(*)` 5). Full logs: `/tmp/epc1_measure_head.log`, `/tmp/epc1_view_head.log`.

| Fixed cell | Head answer | Row-returning form | Spark §3 |
|---|---|---|---|
| Changelog v2 `COUNT(*)` / `COUNT(1)` / `df.count()` | 5 | 5 | 5 |
| Changelog v2 `SELECT 1` rows / `EXISTS` | 5 rows / 1 | 5 / 1 (derived) | 5 / — |
| Incremental `(S0,S2]` `COUNT(*)` / `COUNT(1)` / `df.count()` | 3 | 3 | 3 |
| Incremental `SELECT 1` rows / `EXISTS` | 3 rows / 1 | 3 / 1 (derived) | 3 / — |
| Changelog single-file `COUNT(*)` | 2 | 2 | — (shape-covered) |
| Changelog ten-file / incremental ten-window `COUNT(*)` | 20 / 18 | 20 / 18 | — (shape-covered) |
| Changelog v3 / incremental v3 `COUNT(*)` | 4 / 2 | 4 / 2 | 4 / 2 |
| Changelog MOR-delete / incremental over-delete `COUNT(*)` | 7 / 4 | 7 / 4 | — (row-form-proven) |
| Lineage `COUNT(*) AS _row_id` / `COUNT(1)` / `df.count()` | 4 | 4 | — (no Spark v3-lineage door on this box) |

No wrong count, so no halt: every fixed value equals its row-returning form, and every
Spark-shared value equals Spark.

## 5. Gates (2026-10-09, all on the head)

| Gate | Result |
|---|---|
| `cargo fmt --check` | clean |
| `make rust-clippy` | clean after one remediation round (3 test-only lints in the provider pin: `too_many_lines`, 2 × `redundant_closure`; fixed by extracting `counted_table`) |
| `make rust-panic-ban` | clean |
| `cargo test --locked -p repark-iceberg --lib` | 966 passed, 0 failed |
| `make develop` | clean (rebuilt after the fix) |
| Unit pins `test_empty_projection_count_1.py` | 20 passed |
| `pytest python/repark/tests -q -n 8 -k "changelog or incremental or lineage or changes"` | 124 passed, 12 skipped (all environmental: no pyspark in venv, live tier off, moto venv absent) |
| `uvx ruff@0.15.22 check .` / `format --check .` | clean / clean |
| `python3 scripts/sync_map_md.py --check` | 374 maps clean |
| `bash scripts/check_map_md.sh --base 291b39f4` | clean (`origin/main` does not exist in this clone; the brief's base commit used) |
| `make check-ledger-grammar` | clean |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: empty-projection-count-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause carries red-first pins: 15 facade pins plus 4 Rust pins failed on 028de351 with the rebuild error and pass on the head; 5 guard pins pass on both.
      artifacts: [python/repark/tests/test_empty_projection_count_1.py, crates/repark-iceberg/src/catalog/scan_batches.rs, crates/repark-iceberg/src/microbatch/provider.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Empty schema over 0-row, N-row and mixed (3, 0, 1) streams, an empty parquet file, v2 and v3 tables, and delete-holding histories are all exercised with exact counts.
      artifacts: [crates/repark-iceberg/src/catalog/scan_batches.rs, crates/repark-iceberg/src/microbatch/provider.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The errors that must stay still stay: the V3-ROWID-2 composed-statement refusal, the WHERE-false zeros, and the untouched non-empty rebuild error arm.
      artifacts: [python/repark/tests/test_empty_projection_count_1.py]
    - id: AT-4
      status: N/A
      justification: The fix is a pure function of the incoming batch and schema; the empty path returns before the projection cache, so no shared or cached state is touched.
    - id: AT-5
      status: N/A
      justification: No privileged action, no secret, no deserialization, no path handling; a batch rebuild only.
    - id: AT-6
      status: ATTACKED
      evidence: The wrong-count halt condition was checked cell by cell: all 20 fixed counts equal their row-returning forms, and the 4 Spark-shared values equal live Spark 4.1.2 (§4 table).
      artifacts: [task/ledgers/completed/empty-projection-count-1-ledger.md]
    - id: AT-7
      status: N/A
      justification: A zero-column batch allocates no column arrays; the change replaces an error return with an O(1) rebuild, with no loop or growth.
    - id: AT-8
      status: ATTACKED
      evidence: No fork change: the fork produced the batches on the base tree and only our rebuild refused them. The Arrow explicit-count contract is honored exactly as the metadata-columns copy does. No Cargo.toml change.
      artifacts: [crates/repark-iceberg/src/catalog/scan_batches.rs]
    - id: AT-9
      status: N/A
      justification: No new failure path; the one error text the new arm can emit keeps the existing wording verbatim.
    - id: AT-10
      status: ATTACKED
      evidence: Red-first runs are recorded in §4/§5 (15 facade + 4 Rust red on 028de351, all green on head). Branch liveness: the is_empty arm turns COUNT(*) from error to count (pinned), and the non-empty arm is held by the 966-test crate suite plus the guard pins.
      artifacts: [python/repark/tests/test_empty_projection_count_1.py, task/ledgers/completed/empty-projection-count-1-ledger.md]
  reattested: [AT-10]
  complete: true
```

## D-1. Merge note: the MB-4 local fix becomes redundant

MB-4 commit `a3426ee5` (`zero_column_batch` in `microbatch/provider.rs`, that branch only)
serves the provider's empty projection locally. The shared `conform_batch` fix here covers the
same path (C-005 pins it with the provider reading through the shared site), so the later merge
must drop the local branch in favour of the shared site and keep one pin family. This unit does
not touch that branch.
