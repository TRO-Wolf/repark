# map — repark-spark/src/call

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

Per-procedure bodies for the maintenance `CALL` router (`../call.rs`). The router keeps argument
parsing, table-ident resolution, and the other procedures; a procedure moves here when its body
and measured-parity contract would grow `call.rs` beyond its exact
`check_rust_file_size` baseline. This directory contains
`apply_partitioning`, `rewrite_manifests`, `rewrite_data_files`, and `rewrite_where`; `call.rs` keeps
`expire_snapshots`, `rewrite_position_delete_files`, `remove_orphan_files`,
`rollback_to_snapshot`, `register_table`).

## Contents

- `apply_partitioning.rs` — **AP-2 step 1 (2026-09-11):** `CALL
  <catalog>.system.apply_partitioning(table => …, plan_id => … [, dry_run => …])`.
  `dry_run` defaults true (nothing commits; every row `status` `dry_run`). `dry_run => false`
  runs each `ALTER TABLE … ADD PARTITION FIELD …` from the matching plan row (one commit
  each; the `unpartitioned` candidate has no DDL step), then `rewrite_data_files`,
  `rewrite_manifests`, `expire_snapshots` through the existing procedure bodies. The plan id
  is re-derived: the procedure re-plans at the current snapshot via `collect_plan_rows` and
  matches `plan_id`; no match refuses naming the table, the id, and that the snapshot moved
  or the id is not from this table. A step failure stops the chain and names the step number
  and statement; earlier steps stay committed (not a transaction). Frame columns: `step`
  Int32, `procedure` / `arguments` / `status` / `result` / `plan_id` Utf8. P-5: extra
  branches refuse (same helper as plan); sort order is unchanged after a real apply; a
  multi-spec table is rewritten so live data files share one spec. AP-1 is SQL-only, so this
  door is SQL-only too (no Python session method). **D-8 (2026-09-11):** optional
  `target_file_size_bytes`, spelled and parsed as in `plan_partitioning` (positive
  integer). When present the lookup re-plans at that target so a two-field `plan_id` is
  found; when absent the lookup stays at target 1 and a miss tells the caller to pass
  the planning target. Guide: `docs/guide/maintenance-policy.md`.
  pins: ap-2/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- `rewrite_data_files.rs` — **rewrite_data_files options (2026-08-31):** v2 `where` is wired
  through the fork's `RewriteDataFiles::filter` (file-selection, no residual). `strategy`
  `binpack` runs; `sort` and `sort_order` refuse (fork R135 / registry `RDF-SORT-1`). Unknown
  strategy and bad `where` use Spark 4.1.2 + Iceberg 1.11.0 text. v3 rewrite
  preserves lineage (`V3-LINEAGE-1` FIXED, RP-4 / fork #243) and drops
  in-scope Puffin DVs with a true `removed_delete_files_count` (`V3-DANGLE-1`
  FIXED, V3-5). `options` stays refused. **MAINT-POLICY-1 step 3 (2026-09-10):** the fork
  invocation is the shared `run_rewrite` core (door passes `None` for the size after its
  refusals; the apply path passes the policy size). **MAINT-POLICY-1 step 4 (2026-09-10):**
  the door loads the table once up front and passes the loaded table (or its ident) into
  `run_rewrite`, so a `where` CALL loads once and a missing table reports before a
  malformed `remove-dangling-deletes` value — the pre-step-3 precedence, pinned. The
  eighth parameter (ident plus table) trips pedantic `too_many_arguments`, held by the
  item-scoped allow on `run_rewrite`; bundling the action config into a struct was
  rejected as heavier than the two-caller shared core it would serve.
  pins: maint-rewrite-data-files-options/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
  pins: rp-4-fork-repin/C-003
  pins: v3-5-dv-compaction/C-002, C-004
- `rewrite_where.rs` — SQL `where` string → Iceberg `Predicate` (eq/cmp/AND/OR/NOT/IS NULL/IN/
  BETWEEN on primitives). Failures wrap as Spark's `Cannot parse predicates in where option`.
  In-module unit tests pin each convertible operator's Predicate shape.
  pins: maint-rewrite-data-files-options/C-007
- `run_maintenance.rs` — **MAINT-POLICY-1 steps 2–3 (2026-09-10):** `CALL
  <catalog>.system.run_maintenance(table => … [, dry_run => …] [, <D-1 key> => …])`.
  Inline keys overlay the stamped file policy (per-table entry, then profile) through
  step 1's `resolve`; no stamped policy and no inline keys refuse with the D-6 text.
  `plan_steps` admits each D-4 step by its gate (delete ratio from `files WHERE content = 0`
  against `delete_files` byte sums, `rewrite_manifests = true`, set cutoffs) with stable D-4
  ordinals and renders each step's CALL string; the dry-run frame answers `step` Int32 plus
  `procedure` / `arguments` / `status` / `result` Utf8, every `status` `planned`.
  Each `PlannedStep` also carries its typed `StepAction` (the plan-time cutoffs, so apply
  reuses the rendered values bit-for-bit); `dry_run => false` delegates to
  `run_maintenance_apply.rs` behind one `Box::pin` (the apply future would push the router
  future past the 16 KiB `large_futures` lint otherwise). In-module unit tests
  pin the gates, the renderings, and the saturating cutoff math.
  **ORPHAN-S3TABLES-1 (2026-09-12):** a `PlannedStep` carries an optional `skip_reason`;
  on a `ServiceManagedLocation` catalog (the `s3tables` kind) the orphan step keeps its
  D-4 ordinal but is marked skipped with the service's `unreferencedFileRemoval` remedy as
  the reason — table buckets answer `ListObjectsV2` 405 — so the dry-run frame shows
  `skipped`, never `planned`.
  pins: maint-policy-1/C-007, C-008, C-009, C-010, C-011, C-012
  pins: orphan-s3tables-1/C-003
- `run_maintenance_apply.rs` — **MAINT-POLICY-1 step 3 (2026-09-10):** the apply path. Each
  planned step runs through the same procedure body the CALL door dispatches to (built
  `CallArgs`, no SQL-text re-entry): position-delete, manifests, expire and orphan steps
  through their `execute_*` entries, the rewrite step through the shared `run_rewrite` core
  with the policy's `target_file_size_bytes` (the door keeps its v1 options-map refusal, so
  the dry-run options rendering stays Spark-spelling documentation while apply passes the
  parsed size). The frame keeps the dry-run shape with `ran` / `failed` / `skipped`: the
  first failure stops the chain, its row carries the error text, later rows are `skipped`
  with empty results; gate-skipped steps stay absent exactly as on a dry run. Each
  `result` is the step's own frame rendered as JSON by a
  small local renderer (no JSON dependency: `Cargo.toml` is frozen this card); unknown
  column types refuse loud rather than guessing. Orphan steps pass `dry_run => false`
  explicitly (the door defaults it true). **ORPHAN-S3TABLES-1 (2026-09-12):** a step whose
  `skip_reason` is set never reaches `run_step` — its row is `skipped` with the reason and
  the chain continues (a service-managed orphan sweep on `s3tables`), unlike a
  chain-stopped `skipped` row, which carries an empty result.
  pins: maint-policy-1/C-013, C-014, C-015, C-016, C-017
- `rewrite_manifests.rs` — **MW-6**: `CALL <catalog>.system.rewrite_manifests(table => …)` over
  the fork's `RewriteManifestsAction` (`transaction/rewrite_manifests.rs`). The action returns no
  counts, so Spark's two columns are read from the new snapshot's summary
  (`manifests-replaced` → `rewritten_manifests_count`, `manifests-created` →
  `added_manifests_count`). Three guards make the answer Spark's rather than the fork's: a table
  with no snapshot returns zeros where the action errors; Spark's no-op rule (one matching
  manifest already at target size) returns zeros and commits nothing; and a zero answer refuses
  while two or more delete manifests stay uncompacted, because the fork rewrites data manifests
  only (registry `MANIFEST-1`). `rewrite_if` pins Java's default current-spec filter; `spec_id`
  refuses and `use_caching` is an accepted no-op (registry `MANIFEST-2`). Above
  `commit.manifest.target-size-bytes` the two engines write a different NUMBER of manifests, so
  `added_manifests_count` diverges there (registry `MANIFEST-3`); `rewritten_manifests_count`
  agrees at every size measured.
- `plan_partitioning.rs` (+ `plan_partitioning/`) — **AP-1 step 1 (2026-09-10):** `CALL
  <catalog>.system.plan_partitioning(table => …, target_file_size_bytes => …)` (both required,
  target positive). Statistics come from one `files WHERE content = 0` read
  (`file_size_in_bytes`, `file_path`, plus the `readable_metrics` bound pairs) and the `refs`
  table; no data scan. P-2 per column: timestamp/date → `years`/`months`/`days`/`hours`;
  int/string → `identity` when the bound-endpoint union holds at most 1000 values else
  `bucket(N)`; plus `unpartitioned`; pairs cross the best single of the top three columns.
  P-3 scores each projected value against the 0.25×–4× target band (a file spanning k values
  contributes 1/k to each) and ranks by score, projected files, name. The scoring constants
  live in one place, `plan_partitioning_score.rs`: band 0.25 and 4.0, distinct limit 1000,
  bucket widths 8/16/32/64/128, and `FALLBACK_BYTE_RATIO = 0.55` (S2-10/D-4: the value AP-0's
  O-run measured post-rewrite bytes at, 0.53–0.57× of the pre-rewrite sum —
  `docs/perf/ap-0-partition-candidates-2026-09-10.md` §"The O-run"). The frame answers D-1's
  `candidate`/`score`/`projected_partitions`/`projected_files_at_target`/`ddl`/`calls`/`plan_id`
  plus `notes`; `plan_id` hashes snapshot id plus candidate. **AP-1 step 2 (2026-09-11):**
  `projected_files_at_target` now derives from post-rewrite bytes — each partition value's
  raw byte share times `byte_ratio` (per `plan_partitioning_bytes.rs`); `score` keeps the raw
  P-3 band model, D-4 amends the file-count projection only. Every row's `notes` carries
  `byte_ratio=<r rounded 2 places> (footers|fallback)` and the reworded AP-0-R-001 caveat
  (the projection now applies the ratio); the last row additionally names boundless columns;
  more than one partition spec in metadata adds the one-spec rewrite note. A branch besides
  `main` refuses (P-5). Timestamps truncate in UTC through a dependency-free civil calendar.
  **AP-3 (2026-09-12, S2-23):** the scoring byte basis is each file's footer
  `total_uncompressed_size` sum — `projected_files_at_target` multiplies the uncompressed
  share by `byte_ratio` once (the stored-byte form compressed twice and read −74 %/−72 %
  low against the RP-17 same-codec rewrite; the residue note says so and names S2-24 as the
  remaining gap). `score` bands the uncompressed share; `projected_partitions` is untouched.
  On an unreadable footer each file's basis is the estimate `file_size_in_bytes / 0.55`.
  **AP-1-CLOSE-1 (2026-09-12, S2-27):** no formula change — the residue note is re-read as
  an upper bound from the inputs' compressed bytes (a same-codec rewrite into fewer, larger
  files does not compress worse), AP-1-R-001 closes, and the pins in
  [plan_partitioning/tests.rs](plan_partitioning/map.md) reproduce the
  three AP-0 beds' RP-18 frame: `projected_files_at_target` × target sits at or above the
  live actual and at or below 2× it, and the candidate ranking equals RP-18's exactly.
  The `mod tests` block moved to `plan_partitioning/tests.rs` in this change so the file
  stays under its size ceiling.
  pins: ap-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011
  pins: ap-3/C-001, C-006
  pins: ap-1-close-1/C-001, C-002
- `plan_partitioning_bytes.rs` — **AP-1 step 2 (2026-09-11):** the `byte_ratio` measurement
  behind the step-2 byte model. For every live data file's `file_path` it opens the table's
  own `FileIO`, takes the file size from `metadata()`, and range-reads only the parquet tail
  (last 8 bytes, then the footer extent the `NeedMoreData` hint names — never row-group data)
  through `datafusion::parquet`'s `ParquetMetaDataReader` (the `datafusion` re-export; no new
  dependency). `byte_ratio` is Σ `total_compressed_size` / Σ `total_uncompressed_size` over
  every column chunk of every row group; **AP-3 (2026-09-12):** the measure also returns each
  file's uncompressed sum in `paths` order — the byte basis the score folds by the ratio.
  Any unreadable footer (bad magic, missing file, non-parquet data file, thrift decode
  failure, zero uncompressed sum) yields `FALLBACK_BYTE_RATIO` and the `fallback` source tag
  with each file's basis estimated as `file_size_in_bytes / 0.55`, else `footers`. Since
  RP-16 both write paths honour `write.parquet.compression-codec` (CTAS and INSERT land zstd
  by default); measured on the AP-0 beds in
  `docs/perf/adapt-part-ap1-2026-09-11.md` and re-measured in
  `docs/perf/adapt-part-ap1-remeasure-2-2026-09-12.md`.
  pins: ap-1/C-010, C-011
  pins: ap-3/C-002
- `plan_partitioning_score.rs` — the pure P-2/P-3 engine behind the procedure above:
  the civil calendar, the grains, the Spark DDL labels, the 1/k byte spread, the band
  penalty, the single/pair scoring and the best-first ranking, plus the in-module unit tests
  for each. No SQL, no catalog reads; the caller feeds it rated bound pairs. Step 2 threads
  `byte_ratio` through `accumulate`/`score_single`/`score_pair`: the projected file count
  folds each value's byte share by the ratio before `ceil(…/target)`. **AP-3 (2026-09-12):**
  the item byte basis is `f64` and carries the footers' uncompressed sum per file (so the
  fallback's `stored / 0.55` estimate stays exact); `score` bands that uncompressed share
  against the target and `projected_partitions` stays a pure value count.
  pins: ap-1/C-001, C-009, C-010
  pins: ap-3/C-003

## Pointers

- Up: [../map.md](../map.md)
- Pins: [../tests/call_manifests.rs](../tests/call_manifests.rs),
  [../tests/call_rewrite_options.rs](../tests/call_rewrite_options.rs),
  `python/repark/tests/test_maintenance_call.py`,
  `python/repark/tests/test_rewrite_data_files_options.py`
- Divergences: [../../../../docs/spark-sql-iceberg-parity.md](../../../../docs/spark-sql-iceberg-parity.md)
  rows `MANIFEST-1`, `MANIFEST-2`, `RDF-SORT-1`

## Debug

| Symptom | First check |
|---|---|
| A rewrite answered `1, 1` on an already-compacted table | The no-op guard: Spark's rule is `targetNumManifests == 1 && matching.size() == 1` |
| A rewrite refused on a merge-on-read table | The delete-manifest guard — compact the delete FILES first with `rewrite_position_delete_files` |
| The counts disagree with Spark on a table whose spec evolved | `rewrite_if` must filter to `default_partition_spec_id` |
| `added_manifests_count` disagrees with Spark and the table is above the manifest target size | Expected — registry `MANIFEST-3`; the fork rolls on an estimate where Java repartitions into `ceil(total / target)` |
| The commit succeeded but the counts errored | The fork stopped writing `manifests-replaced` / `manifests-created`; the summary is the only source |

First checks: `cargo test -p repark-spark call_manifests::`.
