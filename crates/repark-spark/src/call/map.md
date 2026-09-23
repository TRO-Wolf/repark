# map — repark-spark/src/call

U1-MEM-LAYOUT-1 (2026-09-23): `remove_orphan_files.rs` adds fail-closed refusals for foreign metadata and scans inside another registered table. pins: u1-mem-layout-1/C-011, C-012, C-013, C-014, C-020

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

Per-procedure bodies for the maintenance `CALL` router (`../call.rs`). The router keeps argument
parsing, table-ident resolution, and the other procedures; a procedure moves here when its body
and measured-parity contract would grow `call.rs` beyond its exact
`check_rust_file_size` baseline. This directory contains
`ancestors_of`, `apply_partitioning`, `branch_ops`, `compute_partition_stats`,
`compute_table_stats`, `rewrite_manifests`, `rewrite_data_files`, `rewrite_table_path`, and
`rewrite_where`; `call.rs` keeps
`apply_partitioning`, `rewrite_manifests`, `rewrite_data_files`, `rewrite_options`, and
`rewrite_where`; `call.rs` keeps
`expire_snapshots`, `rewrite_position_delete_files`,
`rollback_to_snapshot`, `register_table`). `remove_orphan_files` moved here on 2026-09-22
(`remove_orphan_files.rs`, `orphan_file_list.rs`).

## Contents

- `apply_partitioning.rs` — **AP-2 step 1 (2026-09-11):** `CALL
  <catalog>.system.apply_partitioning(table => …, plan_id => … [, dry_run => …])`.
  **RP-34 (2026-09-19):** the rewrite step and each `run_step` call are boxed (`Box::pin`), as
  `clippy::large_futures` flags them at fork `43fcd243`; the same boxing holds for
  `run_maintenance_apply.rs`'s `run_step`.
  pins: rp-34-fork-pin/C-003
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
- `create_changelog_view.rs` — **ICE-CHANGELOG-1 (2026-09-20):** the procedure. Java's
  parameter list verbatim (`table`, `changelog_view`, `options`, `compute_updates`,
  `identifier_columns`, `net_changes`) — `remove_carryovers` is NOT an argument in Iceberg
  1.11, so it is behaviour here and not an argument either. The default view name is
  `` `<table>_changes` `` WITH backticks in the returned row and without them in the registered
  name, which is what `QC-DEFAULT-NAME` records. pins: ice-changelog-1/C-014
- `changelog.rs` (+ `changelog/`) — **ICE-CHANGELOG-1 (2026-09-20):** the row transforms
  `create_changelog_view` applies to the raw changelog relation, ported from Iceberg 1.11's
  `ChangelogIterator` family (read off the runtime jar's bytecode, not from docs) and held here
  because they are the PROCEDURE's, never the relation's — `t.changes` stays raw INSERT/DELETE.
  Java is an if/else, not a chain: `compute_updates` runs `RemoveCarryoverIterator` then
  `ComputeUpdateIterator` (repartition by the identifier columns + `_change_ordinal`, sort by
  those + `_change_type`, so a DELETE pairs only with the INSERT that follows it); otherwise
  `removeCarryoverRows(net_changes)` runs either `RemoveCarryoverIterator` (equality over every
  column but `_change_type`, so a carryover pair must share one snapshot) or
  `RemoveNetCarryoverIterator` (equality over the data columns only, a running net count that
  restarts the group at a zero crossing — which is what yields the recorded LAST-touching
  ordinal). Sorting and equality both go through one Arrow `RowConverter`, whose default
  ascending / nulls-first order is Spark's `sortWithinPartitions`.
  pins: ice-changelog-1/C-011, C-012, C-013
- `branch_ops.rs` — **IPI-05 (2026-09-21):** `execute_publish_changes` publishes a staged WAP
  snapshot: it looks the snapshot up with the fork's `staged_snapshot_for_wap_id`, re-raising that
  error's **bare message** (never `to_string()`, which would prefix the error kind and diverge from
  Java's `Cannot apply unknown WAP ID '…'`), commits the fork's `publish_changes` action, and
  answers one row `(source_snapshot_id, current_snapshot_id)`.
- `branch_ops.rs` — **ICE-BRANCH-OPS-1 (2026-09-17):** `fast_forward`, `cherrypick_snapshot`,
  `set_current_snapshot` and `rollback_to_timestamp` over the fork's `ManageSnapshots` /
  `Transaction::cherry_pick` (no fork change). Ref-kind and ancestry pre-checks shape
  Spark's procedure-layer messages (`IllegalArgumentException` via
  `DataFusionError::Configuration`, newly mapped in `repark-core` `error_map.rs`);
  commit-time fork errors pass through with their Java-identical text as the base
  `PySparkException`. Naive timestamp walls read as UTC in every session zone, matching
  Spark's measured procedure-path rule (registry REF-8). Ref targets come from
  `repark-iceberg` `list_snapshot_refs` (the fork's refs-map field is crate-private).
  **Round 2:** empty/whitespace `branch` auto-creates (Spark does; trim refusal gone);
  missing routine args refuse `REQUIRED_PARAMETER_NOT_FOUND`, integer timestamps refuse
  `DATATYPE_MISMATCH`, malformed strings refuse `CAST_INVALID_INPUT`, malformed
  `TIMESTAMP` literals refuse `INVALID_TYPED_LITERAL` — all with Spark's measured text;
  rollback commits the pre-check's selected id via `rollback_to` (fork re-validates
  ancestry at commit). A duplicate WAP cherry-pick refuses with Java's
  `Duplicate request ...` text since RP-26 (fork #293 reordered its cherry-pick
  validation WAP-first; FIXED residue `ICE-BRANCH-OPS-1-R-001` beside registry REF-6).
  pins: ice-branch-ops-1/C-001, C-002, C-003, C-004, C-007, C-010
  pins: ice-branch-ops-1/C-012, C-013, C-014, C-015, C-016, C-017, C-018, C-019, C-020
- `rewrite_data_files.rs` — **rewrite_data_files options (2026-08-31):** v2 `where` is wired
  through the fork's `RewriteDataFiles::filter` (file-selection, no residual). `strategy`
  `binpack` runs. Unknown
  strategy and bad `where` use Spark 4.1.2 + Iceberg 1.11.0 text. v3 rewrite
  preserves lineage (`V3-LINEAGE-1` FIXED, RP-4 / fork #243) and drops
  in-scope Puffin DVs with a true `removed_delete_files_count` (`V3-DANGLE-1`
  FIXED, V3-5). **ICE-RDF-OPTIONS-1 round 2 (2026-09-17):** `options => map(…)` parses
  through `rewrite_options.rs` (Spark's 16 keys, Spark's class and text) and every
  fork-owned key wires into the fork builders in `run_rewrite` (`rewrite-all`,
  `partial-progress.*`, `output-spec-id`, `rewrite-job-order`, sequential
  `max-concurrent-file-group-rewrites`; `max-failed-commits` accepted without effect).
  **MAINT-POLICY-1 step 3 (2026-09-10):** the fork
  invocation is the shared `run_rewrite` core (the door passes the parsed options struct
  by value; the apply path passes a struct carrying only the policy size).
  **MAINT-POLICY-1 step 4 (2026-09-10):**
  the door loads the table once up front and passes the loaded table (or its ident) into
  `run_rewrite`, so a `where` CALL loads once and a missing table reports before a
  malformed `remove-dangling-deletes` value — the pre-step-3 precedence, pinned. The
  options struct parameter keeps the two-caller core at eight arguments (PR2a threads
  the branch name; the `run_maintenance` caller passes `None`); both CALL arms
  dispatch behind `Box::pin` (the options state would push the router future past the
  16 KiB `large_futures` lint otherwise — same remedy as the `run_maintenance` plan path).
  pins: maint-rewrite-data-files-options/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
  pins: rp-4-fork-repin/C-003
  pins: v3-5-dv-compaction/C-002, C-004
  pins: ice-rdf-options-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-011
  **ICE-RDF-SORT-PARSE-1 (2026-09-20):** `strategy` / `sort_order` route onto the fork's
  `RewriteStrategy`, retiring registry `RDF-SORT-1`. `resolve_strategy` is a transcription of
  Java `RewriteDataFilesProcedure.checkAndApplyStrategy` read from the 1.11.0 bytecode, and
  three of its arms are not what the surface suggests: both parameters default to **null**, so
  the procedure skips the strategy check entirely only when *both* are absent — which is why an
  omitted `strategy` with a `sort_order` still sorts rather than bin-packing; `binpack` with any
  `sort_order` reaches Java's `ensureRunnerNotSet` and must raise
  `Cannot set rewrite mode, it has already been set to BIN-PACK`, z-order strings included; and
  `strategy => 'zorder'` is an *unknown* strategy, not a z-order, so the existing
  `unsupported strategy: …` arm stays. The `Cannot mix …` check runs before the strategy
  dispatch, as it does in Java. Everything past the parse is the fork's: the sort execution, the
  Z-order interleave and every refusal text are surfaced unchanged rather than re-authored, so
  there is no second copy of a rule to drift. The procedure's `sort_order` is a one-shot rewrite
  instruction and is never committed to the table — all three cells measure `md.sort-order`
  unchanged. **ICE-PROCEDURES-1 PR2a (2026-09-21):** `branch` wires through
  to the fork's `RewriteDataFiles::branch` (sixth/last, StringType, optional, per the
  jar order): the rewrite plans from the named ref's head and commits only that ref.
  `branch` beside `remove-dangling-deletes` (top-level extra or options-map key)
  refuses loud, since the fork's dangling-delete pass reads main's head; an unknown
  ref passes the fork's `snapshot ref '…' not found` text through
  `illegal_argument_error` via a fourth NEEDLES entry, with no Spark-parity claim.
  The fork's
  sort/z-order validation refusals are re-raised through `illegal_argument_error` with the
  fork text verbatim, so the door reports Java's class as well as Java's text. Round 3
  (2026-09-20, V-001): the remap reads `error.message()`, not `error.to_string()` — the
  latter renders the `DataInvalid => ` kind prefix Java never sends.
  pins: ice-rdf-sort-parse-1/C-004, C-005, C-006, C-007, C-008, C-009
  pins: ice-procedures-1/C-021
- `rewrite_options.rs` — **ICE-RDF-OPTIONS-1 round 1 (2026-09-17):** `options => map(k, v, …)`
  extraction and validation for both rewrite procedures. String/number/boolean/NULL scalar
  rendering, duplicate-key `[DUPLICATED_MAP_KEY]`, unknown-key listing in map order named for
  the **resolved rewriter** (`BIN-PACK`, `SORT` or `Z-ORDER` — ICE-RDF-SORT-PARSE-1,
  2026-09-20; the accepted set is `RDF_ACCEPTED` plus the fork's
  `RewriteStrategy::valid_option_names()`, so the four layout options are accepted exactly
  where Java accepts them and refused with Java's text everywhere else), Java `Long.parseLong` / `parseBoolean` / `Double.parseDouble` value rules,
  case-insensitive `rewrite-job-order` names, `output-spec-id` membership against the table
  specs, and the size-band cross-checks against the table-property (or 512 MiB) default.
  Sizes store as signed longs: `min-file-size-bytes` below 0 refuses `>= 0`, the band
  compares in `i128` so `max-file-size-bytes` `-1` renders signed in the IAE text, and a
  negative reaching a fork builder clamps to 0 (Java's no-floor equivalent).
  The delete procedure accepts its measured subset; semantic-invalid values on its four
  unwired keys report Spark's IAE text before the `UnsupportedOperationException` refusal
  (`rewrite-job-order`, `partial-progress.*`, `max-concurrent-file-group-rewrites`), which
  names the key and the registry row. A present NULL `remove-dangling-deletes` map key
  wins over the legacy top-level flag (Java's default, false). Errors return the
  `IllegalArgument` marker so Python raises `IllegalArgumentException`.
  pins: ice-rdf-options-1/C-001, C-002, C-005
  pins: ice-rdf-sort-parse-1/C-009

- `rewrite_where.rs` — SQL `where` string → Iceberg `Predicate` (eq/cmp/AND/OR/NOT/IS NULL/IN/
  BETWEEN on primitives). Failures wrap as Spark's `Cannot parse predicates in where option`.
  In-module unit tests pin each convertible operator's Predicate shape.
  pins: maint-rewrite-data-files-options/C-007
- `remove_orphan_files.rs` — **IPI-30 guard narrowed (2026-09-22, owner ruling Q-55-6):** the
  procedure body, moved out of `call.rs` with its comments shed. The scan path is `location`,
  or the table location when `location` is absent. On a `TempFallbackAllowed` catalog
  `refuse_shared_temp_fallback_location` refuses a scan path that is `<root>/repark_ctas` or
  `<root>/repark_ansi_ctas`, or a parent of either, after the lexical and `file:` normalisation
  (a table's own directory under the root is sweepable). `refuse_scan_over_other_tables`
  walks every namespace and table through the catalog API and refuses a scan path that
  equals or contains another table's location, naming it. A `ServiceManagedLocation`
  catalog still refuses before any IO. Without `file_list_view` the fork's
  `DeleteOrphanFiles` lists and deletes; with it, `orphan_file_list.rs` answers and the
  same partial-delete refusal applies. Registry row ORPHAN-3.
  pins: ipi-30-orphan-guard-narrow-1/C-001, C-002, C-003, C-004, C-008, C-010, C-016
  U1-MEM-LAYOUT-1 (2026-09-23): fail-closed foreign-metadata and inside-another-table refusals. pins: u1-mem-layout-1/C-011, C-012, C-013, C-014, C-020
- `orphan_file_list.rs` — **IPI-30 (2026-09-22):** `file_list_view`, ported from Java
  `compareToFileList`. The view must carry `file_path` (a string) and `last_modified` (a
  timestamp); a missing view answers `TABLE_OR_VIEW_NOT_FOUND`. Candidates are the non-null
  rows with `last_modified < older_than` whose normalised path lies under the scan path.
  The referenced set is the fork's `DeleteReachableFiles` walk with a collecting
  `delete_with`, which matches the set `DeleteOrphanFiles` uses. The join runs on the URI
  path under `equal_schemes` (Spark's `s3n`/`s3a` defaults merged), `equal_authorities` and
  `prefix_mismatch_mode`. Orphans come back verbatim, sorted and deduplicated. `gc.enabled =
  false` refuses with the fork's text. An ERROR-mode prefix conflict builds the fork's
  `prefix_conflict_error` pairs and text and goes through the same `iceberg_err`, so both
  paths fail with one string; the `gc.enabled` refusals are built the same way.
  pins: ipi-30-orphan-guard-narrow-1/C-005, C-006, C-007, C-011, C-012, C-013, C-014, C-015
- `run_maintenance.rs` — **MAINT-POLICY-1 steps 2–3 (2026-09-10):** `CALL
  <catalog>.system.run_maintenance(table => … [, dry_run => …] [, <D-1 key> => …])`.
  Inline keys overlay the stamped file policy (per-table entry, then profile) through
  step 1's `resolve`; no stamped policy and no inline keys refuse with the D-6 text.
  `plan_steps` admits each D-4 step by its gate (delete ratio from `files WHERE content = 0`
  against `delete_files` byte sums, `rewrite_manifests = true`, set cutoffs) with stable D-4
  ordinals and renders each step's CALL string — the orphan step carries `dry_run => false`,
  the same spelling `run_maintenance_apply` passes, so a copied CALL cannot arm differently
  than the plan shows; the dry-run frame answers `step` Int32 plus
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
  **FNP-4B (2026-09-15):** the local `quote_ident` emits backticks (embedded doubled);
  fixed engine-internal names in the metadata reads stay bare. pins: fnp-4b/C-002
  (slice-3: needless raw-string-hash lint only, no behavior change).
- `rewrite_data_files.rs` / `run_maintenance_apply.rs` — **ICE-FOOTER-CACHE-1 (2026-09-19):**
  both calls to `run_rewrite` are behind `Box::pin`. At fork PR #316's pin the fork `Table` the
  future holds across its awaits carries a footer-cache field, and the two futures reached
  clippy's 16 KiB `large_futures` bound; boxing is behaviour-neutral (the rewrite pins pass
  unchanged). **RP-40 (2026-09-20):** the action's own `execute` future joined them at 23,184
  bytes once fork #323 added the sort and zorder strategies; it is boxed the same way.
- `run_maintenance_apply.rs` — **MAINT-POLICY-1 step 3 (2026-09-10):** the apply path. Each
  planned step runs through the same procedure body the CALL door dispatches to (built
  `CallArgs`, no SQL-text re-entry): position-delete, manifests, expire and orphan steps
  through their `execute_*` entries, the rewrite step through the shared `run_rewrite` core
  with the policy's `target_file_size_bytes` (the door parses the same options map, so
  the dry-run options rendering stays Spark-spelling documentation while apply passes the
  parsed size). The frame keeps the dry-run shape with `ran` / `failed` / `skipped`: the
  first failure stops the chain, its row carries the error text, later rows are `skipped`
  with empty results; gate-skipped steps stay absent exactly as on a dry run. Each
  `result` is the step's own frame rendered as JSON by a
  small local renderer (no JSON dependency: `Cargo.toml` is frozen this card); unknown
  column types refuse loud rather than guessing. Orphan steps pass `dry_run => false`
  explicitly — the door's own default is deleting (Spark parity, IPI-30), so the argument
  is documentation, not the safeguard. **ORPHAN-S3TABLES-1 (2026-09-12):** a step whose
  `skip_reason` is set never reaches `run_step` — its row is `skipped` with the reason and
  the chain continues (a service-managed orphan sweep on `s3tables`), unlike a
  chain-stopped `skipped` row, which carries an empty result.
  pins: maint-policy-1/C-013, C-014, C-015, C-016, C-017
- `ancestors_of.rs` — **ICE-PROCS-ROUTE-1 (2026-09-19):** `CALL
  <catalog>.system.ancestors_of(table [, snapshot_id])` walks the snapshot
  parent chain newest-first, answering `snapshot_id bigint, timestamp bigint`
  (the snapshot's own timestamp-ms). No current snapshot refuses `Cannot find
  snapshot: -1`; an unknown id refuses `Cannot find snapshot: <id>` — both as
  `IllegalArgumentException`, matching Spark's procedure-layer class.
  pins: ice-procs-route-1/C-004, C-005
- `compute_table_stats.rs` — **ICE-PROCS-ROUTE-1 (2026-09-19):** `CALL
  <catalog>.system.compute_table_stats(table [, snapshot_id] [, columns])`
  over the fork's `ComputeTableStats`, answering the single `statistics_file`
  string column with the registered path. The router keeps Spark's guards:
  an empty table answers zero rows and commits nothing; an empty `columns`
  array refuses `Columns cannot be null/empty`; `columns` keeps caller order
  after dedup (a reversed input registers `[data]` blobs first); an unknown
  column refuses `Can't find column <name> in table <schema>`; a
  non-primitive column refuses `Can't compute stats on non-primitive type
  column: <name> (<type>)` with Spark's struct rendering — all as
  `IllegalArgumentException`. The schema dump trims the fork `NestedField`
  rendering's trailing spaces to Spark's measured text. Nested names pass
  through to the fork, which has
  no nested scan projection yet (fork ask R-005 in the unit ledger), so they
  fail loud instead of collapsing to empty stats. The `columns` argument
  parses both `array(…)` call and `ARRAY[…]` literal spellings.
  pins: ice-procs-route-1/C-006, C-007, C-008, C-009, C-010
  Round 3 (2026-09-19, run 25c) measured the refusal texts, the nested field
  id, the dedup, and the caller-order blobs on live Spark 4.1.2.
- `compute_partition_stats.rs` — **ICE-PROCS-ROUTE-1 (2026-09-19):** `CALL
  <catalog>.system.compute_partition_stats(table [, snapshot_id])` over the
  fork's `ComputePartitionStats`, answering the single
  `partition_statistics_file` string column. An unpartitioned default spec
  refuses `Table must be partitioned` as `IllegalArgumentException`.
  pins: ice-procs-route-1/C-011, C-012
- `rewrite_table_path.rs` — **ICE-PROCS-ROUTE-1 (2026-09-19):** `CALL
  <catalog>.system.rewrite_table_path(table, source_prefix, target_prefix [,
  staging_location] [, create_file_list] [, start_version] [, end_version])`
  over the fork's `RewriteTablePath`, answering Spark's four columns
  (`latest_version` names the current metadata file; `file_list_location` is
  the staged `file-list` CSV or `N/A`; the manifest count is the covered
  snapshots; the delete count is the staged parquet position-delete files).
  Default staging is `<table>/metadata/copy-table-staging-<nanos>-<pid>/` (no
  `uuid` dependency in this crate). A source prefix the table is not under
  refuses `Path …/ does not start with …/` as `IllegalArgumentException`;
  `start_version` / `end_version` refuse naming the fork's missing incremental
  range. The file list is the fork copy plan plus the staged rewritten
  metadata entry (the fork stages it but plans no copy for it; Spark's list
  carries staged metadata files), written through
  `repark_iceberg::catalog::write_text_file`, the one fork-`Bytes` call this
  door needs.
  pins: ice-procs-route-1/C-013, C-014, C-015, C-016
- `rewrite_manifests.rs` — **MW-6**: `CALL <catalog>.system.rewrite_manifests(table => …)` over
  the fork's `RewriteManifestsAction` (`transaction/rewrite_manifests.rs`). The action returns no
  counts, so Spark's two columns are read from the new snapshot's summary
  (`manifests-replaced` → `rewritten_manifests_count`, `manifests-created` →
  `added_manifests_count`). **ICE-RM-DELETES-1 (2026-09-20):** the fork's
  `rewrite_delete_manifests(true)` opt-in joins Spark's second leg in the same commit, and
  `rewrite_if` matches a leg only while that leg has work (more than one manifest, or over
  `commit.manifest.target-size-bytes`) — a quiet leg is kept, never rewritten one to one —
  so a delete-only table compacts its deletes and a table quiet on both legs answers zeros
  and commits nothing. A table with no snapshot answers zeros where the action errors.
  `spec_id` selects the rewritten spec (unknown ids raise Spark's `Invalid spec id`
  refusal) and `use_caching` is an accepted no-op. Above
  `commit.manifest.target-size-bytes` the two engines write a different NUMBER of manifests, so
  `added_manifests_count` diverges there (registry `MANIFEST-3`); `rewritten_manifests_count`
  agrees at every size measured. **RM-SORTBY-1 (2026-09-21):** `sort_by` wires
  through, named and fourth-positional per the jar `PARAMETERS` order, parsed as a
  string array. A non-empty list calls the fork's `sort_by_columns` sort-then-pack
  instead of the single-key `cluster_by`; a missing or NULL list keeps the legacy
  path. The empty-list and non-partition-column refusals surface the fork text
  bare through `illegal_argument_error`.
  pins: ice-rm-deletes-1/C-001, C-002, C-003, C-004, C-005, C-006
  pins: ice-procedures-1/C-022
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
  **FNP-4B (2026-09-15):** the local `quote_ident` emits backticks (embedded doubled);
  fixed engine-internal names in the metadata reads stay bare. pins: fnp-4b/C-002
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
- `params.rs` — **ICE-PROCEDURES-1 (2026-09-20):** the per-procedure declared
  parameter lists `bind` in `../call_args.rs` binds against — name, jar type
  and required flag transcribed from `procedure-params.txt` (`javap`-derived
  from the 1.11.0 jar), in jar order. Positional arguments bind in this order;
  the jar type rides along for the rounds that switch the remaining handlers
  (the array and map coercions read it when a handler wires an array or map
  parameter). `params_for` resolves all twenty jar procedures and answers an
  empty list for anything else, which `bind` then refuses loud. One deliberate
  gap stays: `remove-dangling-deletes` is not declared here because
  it is a RePark-only extra the RDF handler passes to `bind` separately, which
  keeps it named-only. **PR2a (2026-09-21):** the RDF list gains `branch`
  sixth/last (StringType, optional), closing the second gap; the binder's
  unknown-argument refusal now names the six-name list.
  pins: ice-procedures-1/C-001, C-002, C-005, C-011, C-021
- `add_files.rs` — **ICE-PROCEDURES-1 PR1b (2026-09-21):** `CALL
  <catalog>.system.add_files(table => …, source_table => … [, partition_filter] [,
  check_duplicate_files] [, parallelism])` over the fork's `AddFiles` action.
  `source_table` takes the `` `parquet`.`<directory>` `` spelling only: any other format
  refuses naming the format, a catalog-table reference refuses as out of scope, and any
  other shape refuses with the expected spelling. `partition_filter` reuses the options
  map parser with its own argument name in the messages; `check_duplicate_files` defaults
  true; `parallelism` validates positive and rides the fork's reader. When the table
  properties lack `schema.name-mapping.default` the handler commits Java's mapping JSON
  in Spark's pretty-print first, so the fork's own ensure stays a no-op and the import
  binds source columns by name. The output is Spark's two columns with a NULL
  `changed_partition_count` on every import; the fork's merge-append writes a
  `changed-partition-count` summary key Java's add_files path does not, so `md.snapshots`
  stays a fork residue (ledger C-015).
  pins: ice-procedures-1/C-015, C-016, C-017, C-018, C-019

## Pointers

- Up: [../map.md](../map.md)
- Pins: [../tests/call_manifests.rs](../tests/call_manifests.rs),
  [../tests/call_procedures_1.rs](../tests/call_procedures_1.rs),
  [../tests/call_procedures_2.rs](../tests/call_procedures_2.rs),
  [../tests/call_rdf_branch.rs](../tests/call_rdf_branch.rs),
  [../tests/call_rewrite_options.rs](../tests/call_rewrite_options.rs),
  [../tests/call_rdf_options.rs](../tests/call_rdf_options.rs),
  `python/repark/tests/test_maintenance_call.py`,
  `python/repark/tests/test_rewrite_data_files_options.py`,
  `python/repark/tests/test_ice_rdf_options_1.py`,
  `python/repark/tests/test_ice_procedures_1.py`
- Divergences: [../../../../docs/spark-sql-iceberg-parity.md](../../../../docs/spark-sql-iceberg-parity.md)
  rows `MANIFEST-1`, `MANIFEST-2`, `RDF-SORT-1`, `ICE-RDF-OPTIONS-1`, `RDF-DANGLING-1`

## Debug

| Symptom | First check |
|---|---|
| A rewrite answered `1, 1` on an already-compacted table | The no-op guard: Spark's rule is `targetNumManifests == 1 && matching.size() == 1` |
| A rewrite refused on a merge-on-read table | The delete-manifest guard — compact the delete FILES first with `rewrite_position_delete_files` |
| The counts disagree with Spark on a table whose spec evolved | `rewrite_if` must filter to `default_partition_spec_id` |
| `added_manifests_count` disagrees with Spark and the table is above the manifest target size | Expected — registry `MANIFEST-3`; the fork rolls on an estimate where Java repartitions into `ceil(total / target)` |
| The commit succeeded but the counts errored | The fork stopped writing `manifests-replaced` / `manifests-created`; the summary is the only source |

First checks: `cargo test -p repark-spark call_manifests::`.
