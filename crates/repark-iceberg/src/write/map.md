# map — repark-iceberg/src/write

ICE-MIXED-CASE-1 (2026-09-17): the write path resolves target columns through the shared case-insensitive scope helpers (`name_resolution.rs`, `predicate_dml.rs`); identity DELETE/UPDATE take the flag at the execution site. pins: ice-mixed-case-1/C-003, C-005

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001). Wrapped-line fragments rewritten as complete sentences (D-002). Clippy doc_markdown backticks added.

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

## Purpose

The **thin Spark-semantics write adapter** over the owned iceberg-rust fork (v1 `repark-write`,
ported byte-faithful). The heavy table-format machinery (`OverwriteFiles` / `RowDelta` /
`RewriteFiles` actions, position-delete writers, `UpdateSchema`, snapshot management) lives in
the fork; this tree only translates Spark write semantics onto the fork's native actions plus an
OCC retry loop. `DELETE`/`UPDATE`/`INSERT` need no adapter — DataFusion plans them onto the
fork's `iceberg-datafusion` `TableProvider`.
Source documentation may retain model provenance; code-quality grade tags stay outside code.
Source comments are condensed to API and safety contracts; executable behavior is unchanged.

**The gap WI-1 named, closed by WI-2 (2026-08-15):** plain `INSERT` still has no adapter here —
DataFusion's own `insert_to_plan` injects the `CAST` and hands a schema-conformed plan straight to
the fork's `IcebergTableProvider::insert_into` — so the gate could not be a call site on a write
lowering. It is an `AnalyzerRule` instead (`insert_gate.rs`), one stage EARLIER, where the
pre-cast source type is still in the plan. `INSERT INTO … SELECT`, `writeTo().append()` and
`write.insertInto()` now refuse the `Date32 → Int32` reinterpretation (`18262`) that Spark
refuses. Named residual: a literal `INSERT INTO … VALUES` row — see `insert_gate.rs`.

**Error boundary:** re-exports `repark_common::{Error, Result}` for MERGE/append, but the
`alter` and `snapshot_refs` primitives still return `iceberg::Result` — the fold lives in
repark-core's error map.

## Contents

- `mod.rs` (v1 `lib.rs`) — module decls + the public re-export list (names unchanged from v1):
  `Error`/`Result`, write/scan concurrency knobs, `writer_props`, the `write_data_files*` +
  `write_partitioned_data_files*` families (bounded-memory stream variants; K concurrent file
  writers, default 4, K=1 serial), `append`, the overwrite stage-then-swap surface, and the
  snapshot-ref helpers. `store_assign` is declared `pub(crate)` — an internal predicate, never
  a public surface. **ICE-SESSION-WRITE-CONF-1 (2026-09-19):** declares
  `session_write_conf` and re-exports its resolver surface.
- `session_write_conf.rs` — **ICE-SESSION-WRITE-CONF-1 (2026-09-19):** the
  session write-conf carrier (`SessionWriteView`: session codec/level plus the
  `spark.sql.iceberg.snapshot-property.*` map) and `resolve_write_for_session`,
  which folds writer option over session conf over table property at every owned
  commit; a bogus codec refuses naming the codec (comment-free per the owner ban).
  **Round 3 (2026-09-19):** the `snapshot-property.` SUFFIX is now stored VERBATIM, not
  lowercased. Measured Spark 4.1.2 (cells `QK-*`, run 25c): the key's PREFIX is
  case-SENSITIVE — `Spark.sql.iceberg.snapshot-property.team` and
  `Spark.SQL.Iceberg.Compression-Codec` are silently ignored, which refutes the second
  verification critic's `P2-CONF-KEY-CASE` premise that SQLConf folds them — while
  `spark.sql.iceberg.snapshot-property.TEAM` stamps `TEAM`, the suffix untouched. The
  writer-option suffix still lowercases, which is a different measured rule
  (ICE-WRITE-OPTIONS-1's `suffix_lower_cases_like_spark`): Spark reaches a writer option
  through a case-insensitive option map and a session conf through SQLConf's verbatim
  settings map. Two suffixes that differ only in case are two properties, as they are in
  Spark, so `unset` clears the exact spelling.
  pins: ice-session-write-conf-1/C-048
- `output_spec.rs` — **U7 PR1 (2026-09-24):** the `output-spec-id` write option.
  `parse_output_spec_id` is Java's `Integer.parseInt` (a non-integer refuses
  `NumberFormatException` `For input string: "<v>"` through `number_format_error`);
  `validate_output_spec_id` refuses an
  id the table lacks through the fork's `resolve_output_spec` (`Output spec id <n> is not a
  valid spec id for table`); `staging_table` returns the table itself when the option is absent
  or names the current spec, else a read-only view whose metadata default spec is the requested
  one (`TableMetadataBuilder::set_default_partition_spec`). Only STAGING sees the view: every
  staging entry in `write_options.rs` (`append_with_statement_options`,
  `stage_unpartitioned_stream_with_overrides`, `stage_partitioned_stream_with_overrides`,
  `stage_overwrite_files_with`, `append_staged_with_options`) swaps it in, idempotently, while
  the commit stays on the real table; the fork routes the files into per-spec manifests.
  `WriterStagingOverrides.output_spec_id` carries the value and `merged_staging`
  (`session_write_conf.rs`) keeps it. **Round 2 (2026-09-24):** the two stream stagers pick
  the writer from the STAGED view's spec, not the caller's table (an unpartitioned output spec
  under a partitioned table, or a partitioned one under an unpartitioned table, used to reach
  the wrong writer and leak `DataInvalid => Cannot create partition calculator…`), so an RTAS
  or replace resolves the id in the replacement metadata; `staged_spec_is_partitioned` tells
  the dynamic overwrite whether the staged spec replaces partitions or the whole table. Pins:
  `../tests/output_spec.rs`.
  pins: u7-write-df/C-010, C-011, C-014, C-016
- `writer_partitioning.rs` — **U7 PR1 (2026-09-24):** the DataFrameWriter layout and `save()`
  kernels. `provided_transforms` renders Spark's `partitioningAsV2` (`identity(c)`, then
  `bucket(n, c…)` or `sorted_bucket(c…, n, s…)`, parts quoted as Spark's `quoteIfNeeded`);
  `table_transforms` renders Iceberg's `SparkTable.partitioning()` (`identity`, `bucket(n, c)`,
  `truncate(w, c)`, `years`/`months`/`days`/`hours`, void dropped);
  `check_layout_matches_table` refuses a difference with Spark's `requirement failed: The
  provided partitioning or clustering columns do not match the existing table's.` text
  (case-sensitive, as Spark's `sameElements`), and `check_layout_matches_catalog_table` loads
  the table first. `decide_save_target` maps an explicit-iceberg `save(target)` plus mode and
  existence onto create / append / overwrite / skip, raising `TABLE_OR_VIEW_NOT_FOUND`
  (`<ns>.<name>`, or `` `<parent>`.<leaf> `` for a path) and `TABLE_OR_VIEW_ALREADY_EXISTS`
  (`` `<ns>`.`<name>` ``, shared with `saveAsTable` as `already_exists_error`) from
  `repark_common::spark_error`, and the declared default-format / path-create refusal. Round 2:
  the path relation splits at the last `/` keeping every other character (Iceberg's
  `PathIdentifier`: `` `file:///r/a`.b ``, `` `/r/a/b`.`` ``), and `save_target_names_table`
  is the one path-or-name test. Pins: `../tests/writer_partitioning.rs`.
  pins: u7-write-df/C-005, C-006, C-009, C-014
- `writer_plan.rs` — **U7 PR1 round 2 (2026-09-24):** `plan_writer`, the statement kernel for
  `saveAsTable` and `save()`. It maps the action, mode, existence and layout to a
  `WriterStatement` (`ctas`, `rtas`, `append`, `overwrite`, `skip`) plus whether the caller
  checks the layout against the table: `save()` goes through `decide_save_target`; `saveAsTable`
  appends to an existing table with the check, replaces on every overwrite (U7 PR2,
  2026-09-24: Spark's `ReplaceTableAsSelect(orCreate)`, bucketed or not, existing or not;
  it was a static `INSERT OVERWRITE` for an unbucketed existing table), creates a missing
  one, skips on ignore and refuses the error mode with Spark's already-exists text. On every create-or-replace arm a bucket column,
  then a `sortBy` column (round 4), absent from the frame (case-folded unless the session is
  case-sensitive) is
  `WriterRefusal::MissingBucketColumn`, rendered as `_LEGACY_ERROR_TEMP_3060` by
  `missing_column_message` before the existence refusal; `missing_column_name` is Spark's
  rendering of the name (backticks when it contains a `.`, no escaping), also the `i`
  parameter. Pins: `../tests/writer_plan.rs`.
  pins: u7-write-df/C-015, C-018
  pins: u7-write-df-2/C-002
- `replace_schema.rs` — **U7 PR2 slice-1 round 2 (2026-09-25):** `replacement_schema` gives a
  replace's schema the field ids Java's `TableMetadata.buildReplacement` gives it: the fork's
  `assign_fresh_ids_with_base` (the port of `TypeUtil.assignFreshIds(schema, base, nextId)`)
  against the table's current schema, the counter starting at its `last-column-id`. A kept
  name, nested ones by dotted name, keeps its id; a new name takes the next id. The fork's
  `StagedTableTransaction::begin_replace` takes the caller's ids as given, so every
  `begin_replace` caller calls this first and builds the partition spec from the result:
  `repark-spark` `ctas.rs` and `create_table.rs`, `repark-sql` `create_table.rs` (round 3,
  2026-09-25). Pins: `../tests/replace_schema.rs`.
  pins: u7-write-df-2/C-011
- `set_location.rs` — **IPI-26/27 round 4 (2026-09-21, cell `D-SET-LOCATION`):**
  `set_table_location` applies the fork's `update_location` action
  (`TableUpdate::SetLocation`) in one transaction: the move commit itself and every
  commit after it write the next table metadata under the NEW location, and existing
  data and metadata files are not moved. A sibling of `alter.rs`, which sits at its
  exact file-size baseline. 2 in-module pins (the move writes the new metadata file
  under the new location and advances the catalog pointer; the next property commit
  lands under the new location while the old metadata file stays).
- `writer_props.rs`, `write_options.rs` — **ICE-SESSION-WRITE-CONF-1 round 8 (2026-09-20):**
  `writer_properties_with` takes Java's `parquet.enable.dictionary` default — absent = ON
  (`ParquetProperties.DEFAULT_IS_DICTIONARY_ENABLED = true`, measured by javap on the
  Iceberg 1.11.0 Spark runtime and confirmed by dictionary pages in the checked-in
  Spark-written fixtures) — and only an explicit `false` turns dictionary pages off. Round 3
  had copied the fork insert exec's opposite rule into this shared function, which changed the
  bytes of EVERY file RePark writes and moved the MW-7 / MW-8 bin-pack bands. That rule now
  travels on `WriterStagingOverrides::fork_insert_dictionary_rule`, set only by the owned
  append that stands in for the fork's insert exec, so the owned and unowned INSERT routes
  still write one layout. `staged_writer_properties` is the single staging-to-properties
  bridge the three writer builders call. pins: ice-session-write-conf-1/C-064
- `writer_props.rs` — **ICE-SESSION-WRITE-CONF-1 round 3 (2026-09-19):**
  `position_delete_codec_resolves_over_the_data_file_property`, the critic's
  `P2-POSDEL-CODEC-PYTHON-ONLY`. The round-1 footer pin set only
  `write.parquet.compression-codec` and so stayed green on the OLD data-property-only
  rule; this one walks three rows where the answer can only come from the resolved
  order — a delete-codec property beating a data-codec property, a staging override
  beating both, and an override beating a gzip data property — and reverting
  `delete_compression_with` to the data property alone reds it in-crate.
  pins: ice-session-write-conf-1/C-050
- `data_format.rs` — **IPI-41 WO1 (2026-09-22):** the format-resolution seam both write
  doors route through. `resolve_data_format(staging_format, table_default)` returns the
  per-write `write-format` option when present, else the `write.format.default` table
  property, else parquet. `resolve_delete_format(staging_format, table_property,
  data_format, format_version)` returns PUFFIN on format-version 3 before consulting any
  setting, else the per-write `delete-format` option, else the
  `write.delete.format.default` property, else the resolved DATA format. The vocabulary is
  parquet/orc/avro case-insensitively; any other string refuses with
  `Invalid file format: {name}` (IllegalArgumentException), the same shape
  `validate_write_format` uses. Callers pass
  `staging.write_format` / `staging.delete_format` (`WriterStagingOverrides`) with the raw
  property values from `table.metadata().properties()`; WO2 routed the five builder
  sites through them (unpartitioned staging, partitioned fanout, partitioned lineage,
  unpartitioned MERGE, position deletes), and WO3a opened `validate_write_format` to
  orc/avro. pins: ice-orc-avro-1/C-001, C-002, C-003, C-004, C-005, C-006, C-025
- `merge/` — the RePark-owned `MERGE INTO` executor (copy-on-write AND merge-on-read per
  `write.merge.mode`, fork ENGINE_CONTRACT §6). DML-A adds `WHEN NOT MATCHED BY SOURCE`.
  See [merge/map.md](merge/map.md).
- `meta_delete.rs` — **ICE-META-DELETE-1 (2026-09-19):** Spark's decision, above
  `write.delete.mode`, whether a DELETE can be answered by REMOVING whole data files.
  `try_meta_delete_target` reads a three-part `Statement::Delete` (a four-part branch selector
  and every non-identity clause decline); `delete_predicate` translates its `WHERE` EXACTLY —
  `AND`/`OR`, `=`/`<`/`<=`/`>`/`>=`, `IS [NOT] NULL`, a positive `IN` list, `LIKE 'prefix%'`
  (Spark's `STARTS_WITH`), literal `TRUE`, and a missing `WHERE`; anything else declines and the
  statement keeps the row-level route. Negations (`NOT`, `<>`, `NOT IN`, `NOT LIKE`) decline on
  purpose: Iceberg's negated predicates MATCH a null where SQL's three-valued logic does not,
  and the recorded `not_in_whole_*_mor` cells show Spark taking the row-level route for them.
  The decision splits into `plan_metadata_delete` and `commit_metadata_delete` so a door can
  run its own refusals between them (the ANSI door runs the MoR multi-spec guard only once the
  predicate has translated, which keeps the cheap G3-E8 subquery valve first).
  A column reference binds the way the calling door's planner binds it: a case-insensitive
  door folds, an exact door lower-cases an UNQUOTED reference (DataFusion's default ident
  normalization) and binds a QUOTED one verbatim.
  `try_metadata_delete` then asks the fork's `Table::can_delete_using_metadata` (partition
  selection, else strict metrics on every planned file) and, only when it answers true, commits
  the fork's `DeleteFilesAction::delete_from_row_filter`. The decision precedes the commit, as
  Java's does, because the action fails a PARTIAL match non-retryably. A predicate that plans no
  file is vacuously true, which is where Spark's empty `delete` snapshot on a no-match comes
  from. Both doors call this one seat.
  pins: ice-meta-delete-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- `predicate_dml.rs` — **ICE-CATALOG-SESSION-1 S9 (2026-09-20):** the identity-collector
  scratch refs quote through the split-aware scratch quoter (982 → 983, under the default ceiling).
  **WO U5 PR2b round 2 (2026-09-25):** merge-on-read DELETE/UPDATE on a v1 table raise Spark's
  IllegalArgumentException `Deletes are supported in V2 and above` (MERGE: `merge/mod.rs`).
  pins: ice-nested-evo-1/C-057
- `predicate_dml.rs` — **ICE-OCC-SCOPED-1 (2026-09-17):** the identity DELETE / UPDATE builds a
  `CommitScope` from its isolation property and `conflict_filter::for_identity_dml` over its own
  `WHERE`, and hands it to the COW overwrite or the MoR row delta, so a concurrent commit that
  cannot touch the rows the statement reads no longer aborts it. The two four-line
  `RowDeltaPolicy` literals became `scope.row_delta(kind)` (net 0 lines; baseline 1142 held).
  pins: ice-occ-scoped-1/C-005, C-007, C-008
- `predicate_dml.rs` — **V3-8 (2026-09-02):** the COW rewrite carries stored `_row_id` /
  `_last_updated_sequence_number` on format-v3 (scratch from `merge::row_lineage`, survivors and
  updated rows projected through `predicate_dml/lineage.rs`), so `row_lineage_guard.rs` lost its
  last caller and was deleted with it — registry `V3-COW-1` FIXED.
  **V3-9 (2026-09-02):** `resolve_write_mode`'s merge-on-read format gate went from
  `!= FormatVersion::V2` to `< FormatVersion::V2` (the shape `resolve_merge_mode` already
  used), so v3 predicate DML falls through to `commit_row_delta_kind` →
  `merge::dv_close::prepare_row_delta_deletes`, which already branches V2 parquet position
  deletes / V3 `close_touched_dv_containers`. No new deletion-vector code — registry
  `V3-MOR-1` FIXED. The `write.delete.granularity` parse stays as a validation gate on both
  versions even though a v3 deletion vector is file-scoped by construction. The per-row
  `Arc<str>` for a matched row's data-file path is reused when the path is unchanged
  (`predicate_dml/lineage.rs::push_identity_pair`), so a single-file DELETE allocates once
  rather than once per row.
  pins: v3-8-subquery-where-lineage/C-002; v3-9-mor-predicate-dml-dv/C-003, C-009
- `predicate_dml.rs` — **RP-7 (2026-09-02):** the identity scratch scan is no longer built with
  `filter: None`. `identity_scan_residual` re-parses `selection_sql` and, for a POSITIVE
  uncorrelated `IN` or a positive `EXISTS` whose correlation is one bare equality, derives the
  source key's min/max through the same `residual_bounds_predicate` MERGE uses (PERF-04) and
  pushes it onto the target scan. `NOT IN` / `NOT EXISTS` keep the unfiltered scan, and
  `repark.merge.scan-pruning=false` turns it off.
  **Safety.** Two conditions, and both are load-bearing. (1) Ownership must be EXACT:
  `predicate_dml/residual.rs` classifies each side of the correlation ONCE, the way
  `scan_prune::parse_column_ref` does, and derives NO residual when a qualifier resolves to
  neither owner or to BOTH. A target alias that shadows the subquery relation's alias or bare
  table name is rejected outright, because Spark resolves the inner name to the subquery's own
  relation: `DELETE FROM t s WHERE EXISTS (SELECT 1 FROM src s WHERE s.id = s.id)` is
  UNCORRELATED and deletes every row, and an independent per-side classification read it as a
  correlation and pruned. That is a wrong answer, not a slow one — it was measured on Spark
  4.1.2 and it is why the classification is one resolution per side.
  (2) Given exact ownership the push cannot drop a matching row: the identity scan is
  match-discovery only (the COW arm re-reads survivors through its own allowlisted scan) and a
  min/max range is a superset of the key set.
  `collect_identity_pairs` / `collect_identity_update_rows` consume `execute_stream()` and
  `reserve(batch.num_rows())` per batch instead of collecting the whole result first.
  pins: rp-7-f18-repin/C-005
- `predicate_dml.rs` — **G3-E8 A1-identity** (`execute_predicate_dml`): evaluate the original
  `WHERE` as a SELECT over the pinned `(_file, _pos)` streaming target, then commit through the
  MERGE COW/MoR write arms honoring `write.delete.mode` / `write.update.mode` / isolation —
  **never** `write.merge.mode`. Product hole is the valve allow-list (uncorrelated
  `DELETE … IN` / `NOT IN (SELECT …)`, including the NULL 3VL trap, `[NOT] EXISTS` ±
  correlation, correlated IN, identity `UPDATE … SET <scalar> WHERE col IN`, and
  **RP-9 r2:** a three-part `DELETE … WHERE <scalar comparison>` via
  `predicate_dml/plain.rs` so the production partition map reaches F-23; UPDATE,
  literal `IN`, and branch selectors stay on the fork). ANY/ALL
  stay refused (Spark 4.1.2 parse-fails quantified comparisons). Pins:
  [predicate_dml/tests/predicate_dml.rs](predicate_dml/tests/predicate_dml.rs) +
  [predicate_dml/tests/update.rs](predicate_dml/tests/update.rs) +
  [predicate_dml/tests/plain.rs](predicate_dml/tests/plain.rs)
  pins: rp-9-repin-f23/C-005
  **ICE-LIST-NULL-2 (2026-09-19):** the three-part comparison claim additionally
  declines selections over non-primitive columns (`plain::selection_refs_non_primitive`
  against the loaded table's schema, via the shared `conflict_filter::top_level_field`
  rule) to the fork DELETE path; primitive-only selections are unchanged.
  pins: ice-list-null-2/C-003
  — **LRS-5 (2026-08-20):** moved into the canonical module tree, `#[path]` gone. Isolation
  property pins (M19 / A10: no trim, `to_ascii_lowercase`, default serializable,
  garbage ⇒ Plan `Invalid isolation level: {name}`) live in those two test
  files. **MW-9:** `resolve_write_mode` parses `write.delete.granularity` on the
  MoR arm before identity UPDATE/DELETE writes parquet (same refuse-before-IO
  class as `resolve_merge_mode`). Ledger:
  [`../../../../task/r1-g3e8-pr4-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-14-r1-g3e8-pr4-ledger.md).
- `file_order.rs` — **V3-11 (2026-09-02):** `ascending_partition_order` stable-sorts one
  commit's `Vec<DataFile>` by partition value ascending (spec-field order, nulls first,
  primitive literals ascending) before the files reach the manifest, so `first_row_id`
  assignment is deterministic. Each write path sorts exactly **once**: the serial fanout entry
  `append.rs::fanout_conformed_stream_serial` sorts what its single writer closed, the
  concurrent path sorts only where the worker vectors are concatenated (sized from their
  summed lengths), and `merge/row_lineage.rs` sorts its own fanout close. Unpartitioned
  commits sort to a no-op. Cost is file-count work, not per-row work (1e6 rows / 8 partitions:
  2.810/2.850/2.875 s with, 2.973/2.943/3.010 s without). The name is the rule, **not** a Spark
  claim: Spark's own order is the Java `HashMap` bucket index of the partition struct, decoded
  in registry `V3-FILEORDER-1`, and the two coincide only on collision-free monotonic sets —
  `{0,1}`, `{0,1,2}`, `{0,1,2,3}`, `bucket(4, ·)` — not on five or more int partitions,
  strings, multi-field specs, `truncate`/`days`, or a null slot arriving after a non-null.
  Plain `INSERT INTO` on a partitioned table never reaches this module: the fork's `TaskWriter`
  owns it. **RP-8 (2026-09-03):** fork ask **F-20** landed (`#261`), so `FanoutWriter::close`
  drains ascending too and `F-v3-10-partition-file-order` is FIXED — one ordering rule now holds
  on every writer that reaches a repark table, the fork's included, and `V3-FILEORDER-1` covers
  that path as well.
  pins: v3-11-row-id-determinism/C-001, C-003, C-006, C-007
  pins: rp-8-repin-f21-f22/C-004
- `conform.rs` — batch conforming for the append write path (name resolution, WI-1 store
  assignment, strict casts), split from `append.rs` (file-size ratchet, 2026-09-01;
  append.rs baseline 1886). A missing
  column whose Iceberg field carries a `write-default` builds against the reduced schema so the
  fork's `DataFileWriter::write` fills it (**V3-6 C-005**).
  **CTAS-VIEW-1 (2026-09-03):** `conform_batch_retaining_unmapped_columns` is the unpartitioned
  stream-writer map; it calls `conform_batch` then keeps MERGE lineage extras (`_row_id`).
  Matching types skip `try_new` so CAST-NULL empty overwrite keeps source nullability.
  pins: ctas-view-1-conform-stream/C-002
  **ICE-PROMOTE-READ-1 (2026-09-16):** `promoted_scan_column` widens a scanned column written
  before a legal Iceberg promotion (`Int32 → Int64`, `Float32 → Float64`,
  `Decimal128(p,s) → Decimal128(p',s)`) to the current type with a strict cast and returns every
  other column unchanged, so a non-promotion mismatch still fails in `RecordBatch::try_new`.
  Caller: `merge/mod.rs` `conform_scan_batch`.
  pins: ice-promote-read-1/C-011
- `append_fanout_serial.rs` — **ICE-WRITE-OPTIONS-1 round 3 (2026-09-17):** the serial
  conformed fanout (`fanout_conformed_stream_serial[_with_abort]`), split out of
  `append.rs` under the file-size gate; re-exported there so callers keep their paths.
  Run 22b rebase (2026-09-18): the fanout builder is wrapped in `distribution::stamp`, the
  ICE-SORTED-INSERT-1 change main made to the pre-split `append.rs` body, so both the
  option-free and the option-carrying fanout stamp the default sort order id.
  pins: ice-write-options-1/C-017
- `append.rs` — `append(catalog, ident, batches)`: public bulk append — conform
  ([conform.rs](conform.rs): missing /
  extra / duplicate column = loud error, except a missing column whose Iceberg field carries a
  `write-default`: conform builds that batch against the reduced schema and the fork's
  `DataFileWriter::write` fills it — **V3-6 C-005**; **WI-1** ANSI store-assignment gate then
  strict casts, overflow never NULLs) → identity-partition fanout write → ONE stamped
  `merge_append` commit
  (append×append commutes via the fork's refresh-and-re-apply retry; empty input commits an
  empty stamped snapshot). Also `write_partitioned_data_files(_from_stream)` — the partitioned
  staged-write core. **V3-1 / RP-3 C-008:** `iceberg_err` goes through
  `catalog::iceberg_to_datafusion`; Hadoop `vN.metadata.json` writes bump to `v(N+1)`
  (registry `V3-ADOPT-1` FIXED).
  **WRITE-DISTRIBUTION-2 (2026-09-06):** the concurrent dispatcher no longer deals whole
  batches round-robin. It routes each batch through `distribution/router.rs::PartitionRouter`, which
  splits the batch's rows by hash of the writer's partition values, and sends each part to its
  slot's worker — one partition value lands in one writer on every partitioned stream write
  (INSERT OVERWRITE, MERGE inserts, staged appends). The serial path and the unpartitioned
  path are untouched by this unit.
  pins: write-distribution-2/C-001
  **WRITE-ORDER-DIST-1 (2026-09-06):** the concurrency entry delegates to the distribution
  module's sorted drivers, so a declared default sort order sorts every partitioned staged
  write at no behaviour change when no order is declared.
  pins: write-order-dist-1/C-008
- `truncate.rs` — whole-table `TRUNCATE TABLE` (DML-C): `commit_truncate` is
  `commit_overwrite_replace_all` with no added files (fork stamps `Operation::Delete`).
  `commit_truncate_to` commits onto a named branch.
  pins: dml-c-truncate/C-001, C-005
  pins: rp-5-fork-repin/C-004
- `update_cast.rs` — **U8 WRITE-SQL PR2 (2026-09-25):** `store_assignment_cast_sql` is the one
  home of the `arrow_cast((expr), '<type>')` store-assignment cast, with the field ids
  stripped from the type name. MERGE's `insert.rs` and the Spark door's nested-assignment fold
  both call it. pins: u8-write-sql/C-030
- `update_cast.rs` — **IPI-51 PR10 (2026-09-22):** `incompatible_update_message` renders
  `[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]`/`KD000` through the shared
  `ansi_store_assignable` predicate (never a second matrix) with uppercase Spark type
  names; an unlisted type falls through to `None`. Both doors call it before executing
  a positional UPDATE. pins: ipi-51/W-UPDATE-TYPE-ERR
- `conflict_filter.rs` — **ICE-OCC-SCOPED-1 (2026-09-17):** the conflict-detection filter a DML
  commit hands the fork's serializable validation (Java `SparkScan.filterExpression()` threaded into
  `RowDelta` / `OverwriteFiles.conflictDetectionFilter` by `SparkPositionDeltaWrite` /
  `SparkCopyOnWriteOperation`). `from_merge_on` keeps only the `ON` conjuncts whose every column is
  qualified by the target alias (`t.k = 'a'`) — never a join equality (`t.id = s.id`), a
  source-only conjunct, or a bare identifier, and nothing at all when the source alias shadows the
  target's. `from_selection` / `for_identity_dml` take the identity DML's `WHERE` (bare or
  target-qualified columns). Conversion is SOUND BY WIDENING: under `AND` an unconvertible side is
  dropped (the filter only grows); under `OR` / `NOT` / `IN` / `BETWEEN` every part must convert or
  the whole node is dropped; a parse failure, an unknown or nested column, a literal that does not
  fit the column's primitive type, a subquery, or an empty result is `AlwaysTrue`. Columns resolve
  against the table's top-level fields, exact name first (the struct's own name index, never
  `Schema::field_by_name`, which also answers dotted nested names), then a unique
  case-insensitive match
  (the schema's spelling is emitted so `case_sensitive(true)` binds). Literal typing mirrors
  `repark-spark`'s `call/rewrite_where.rs` (the `rewrite_data_files` `where` parser); the two stay
  separate because that one is a maintenance procedure with a strict all-or-nothing contract, and
  unifying them is left to a later unit (hand-back of run 21a).
  A FLOAT / DOUBLE literal converts only when it is EXACT: its decimal text and the parsed value's
  full `{:.800e}` expansion normalize to the same digits and exponent, so an underflow
  (`f < 1e-50` → `0.0`), an overflow, a rounding (`0.1`) and a zero (the fork orders floats by
  `total_cmp`, `-0.0 < 0.0`, while SQL equates them) leave the node unconverted. A float column
  also converts only under `=`, `<>`, `IN` and `NOT IN`: the fork's metrics evaluator skips a
  nans-only file (and bounds exclude NaN) under `<` / `>` / `BETWEEN`, while SQL ranges order NaN
  as a value, so a float range stays unscoped (ruling Q-21a-OCC-1).
  pins: ice-occ-scoped-1/C-001, C-002, C-003, C-018
- `commit_target.rs` — `maybe_to_branch` / `snapshot_id_for_commit` for named-ref commits.
  **U7 PR2 (2026-09-24):** `FilterValidation` carries the `isolation-level` and
  `validate-from-snapshot-id` writer options into `commit_overwrite_by_filter_with_summary`.
  `isolation` resolves the level as before (the option, else the table property); `start` is
  where the conflict validation begins: the requested snapshot when an explicit level is
  set, else the snapshot the table was loaded at (the old behaviour; Spark
  validates the whole history there, residue R-6). The requested id parses like Java's
  `Long.parseLong` (`NumberFormatMarker` `For input string: "<v>"`) and must be an ancestor of
  the commit's snapshot, else `DataInvalid` `Cannot determine history between starting
  snapshot <id> and the last known ancestor <oldest id>` (Java's text; the fork walks the whole
  history for an unknown start). Pins: `../tests/filter_validation.rs`.
  **U7 PR2 slice-2 round 2 (2026-09-25, critic r4 V-001..V-007):** `write_options.rs::isolation_with_override` no longer
  maps an `isolation-level=none` option to "no validation": only `serializable` and
  `snapshot` parse, anything else refuses `Invalid isolation level: <raw>` (Spark's
  `IsolationLevel.fromName`; the table property parser in `overwrite.rs` keeps the fork's
  `none` sentinel). Pin: `writer_props.rs::isolation_override_none_refuses_like_spark`
  (was `isolation_override_none_disables_validations`). pins: u7-write-df-2/C-014
  pins: u7-write-df-2/C-009
  `commit_append_to` (ICE-RTAS-BYNAME-1, 2026-09-17): `commit_append` with an
  optional named branch, mirroring `commit_overwrite_replace_all_to`; the Spark door's
  `INSERT … BY NAME` staged append commits through it. Like its sibling it carries
  `#[allow(clippy::missing_errors_doc)]` rather than a doc comment.
- **ICE-MERGE-APPEND-1 (2026-09-19):** every append commit site in this directory —
  `append.rs::commit_append`, `commit_target.rs::commit_append_to`,
  `write_options.rs::commit_append_with_summary` — commits through the fork's
  `merge_append()` (Java `MergeAppend`, what `Table.newAppend()` returns), not
  `fast_append()`. The 1.11.0 bytecode of `SparkWrite$BatchAppend.commit` calls
  `Table.newAppend()`; only `SparkWrite$StreamingAppend` calls `newFastAppend()`, and RePark
  has no streaming writer, so none of these sites is a fast-append site. The three
  `commit.manifest*` table properties (`-merge.enabled`, `.min-count-to-merge`,
  `.target-size-bytes`) therefore take effect as they do in Spark: with defaults the
  hundredth append replaces 99 manifests with 1. The overwrite, replace-partitions and
  row-delta sites are untouched — Spark never merges there. A bare `INSERT INTO` does NOT
  reach these functions: it plans on the fork's `IcebergCommitExec`, which is
  `pub(crate)` and still calls `fast_append` (DECLARED, `ICE-MERGE-APPEND-INSERT-1`).
  `append.rs` carries no module banner and `commit_append` no summary doc line under the
  comment ban: the merge contract is stated here instead.
  The routing needs no dependency movement: `Transaction::merge_append()` is already in the
  pinned fork `44834673`, and `Cargo.toml` / `Cargo.lock` are untouched by the unit.
  pins: ice-merge-append-1/C-001, C-002, C-003, C-004, C-005, C-007, C-010
  pins: rp-5-fork-repin/C-004
  pins: ice-rtas-byname-1/C-001
- `commit_error.rs` — **ICE-COMMIT-UNKNOWN-1 (2026-09-14):** `CommitStateUnknownError`, the
  `std::error::Error` wrapper a RePark commit site stamps with the `engine.operation-id` it
  minted; `operation_id_and_summary` mints the id and the snapshot summary together;
  `commit_err` / `commit_result` wrap only `ErrorKind::CommitStateUnknown` — every other kind
  passes through the plain `DataFusionError::External` fold unchanged. repark-core's
  `error_map` downcasts the wrapper BEFORE the bare `iceberg::Error`, which is how the id
  reaches `Error::CommitStateUnknown`'s `operation_id` field. `is_commit_state_unknown`
  is the same detection for callers that must decide BEFORE wrapping (both doors'
  service-managed CTAS abort arms skip `drop_table` on it). The mint-site set is the six
  call sites: `commit_append` (service-managed CTAS + `append()`) plus its branch twin
  `commit_append_to`, `commit_overwrite` /
  `commit_row_delta_kind` (MERGE + predicate DML), `commit_overwrite_replace_all_to`
  (`INSERT OVERWRITE` + `TRUNCATE`), and the two `partition_overwrite.rs` commits; the
  staged publish path (`StagedTableTransaction` — Glue, warehouse catalogs) mints none at
  fork `edc38c6a`. The
  registry row `ICE-COMMIT-UNKNOWN-1` in `docs/spark-sql-iceberg-parity.md` §2.3 and the
  `docs/cutover/inventory.md` §8 ruling-8 cell carry the same shape table and the Airflow
  alert/retry guidance. `commit_append_to` mints its own id the same way.
  pins: ice-commit-unknown-1/C-001, C-003, C-005, C-006
- `overwrite_commit.rs` — full-table overwrite commit, optional `to_branch`.
  pins: rp-5-fork-repin/C-004
  **ICE-RTAS-OPS-2 round 2 (2026-09-18):** `commit_replace_write` is the RTAS commit for
  a table the service just created: the fork's public
  `overwrite_files().overwrite_by_row_filter(AlwaysTrue).add_files(…).allow_empty_commit()`,
  stamped like `commit_append`. It records `overwrite` with files and `delete` with none —
  the same snapshot the fork's `StagedTableTransaction::with_replace_write(true)` stages.
  Only the two service-managed create-first arms call it, and only for `OR REPLACE … AS
  SELECT`; no fork semantics are patched here.
  pins: ice-rtas-ops-2/C-018
- `hadoop_stale_commit.rs` (test-only) — **ICE-HADOOP-VN-1 (2026-09-17):** two memory
  catalogs over one tempdir warehouse adopt the same Hadoop `v2` file; the first
  `append()` lands `v3`, and the stale catalog's append burns the fork's bounded
  retry budget and surfaces `ErrorKind::CatalogCommitConflicts` with the winner's
  bytes and rows intact, then stays wedged-loud on the next stale append. Red-first:
  both pins fail on a temporary local revert to the pre-#286 pin (the stale append
  returns `Ok`), green on `75da2b58`; the revert never reached a commit.
  pins: ice-hadoop-vn-1/C-001
- `overwrite.rs` — exclusive full-table `INSERT OVERWRITE` stage-then-swap:
  `write_overwrite_staged_files_from_stream` (positional map + **WI-1** store-assignment gate +
  stream stage) + `commit_overwrite_replace_all` + `parse_overwrite_isolation`
  (absent→snapshot | snapshot | serializable | none | invalid-loud).
- `conform.rs` — **DATE-FN-1 (2026-09-04):** the identity arm of
  `conform_batch_retaining_unmapped_columns` rebuilds the batch against the write schema so
  leaked Iceberg `PARQUET:field_id` metadata from a multi-table join cannot scramble CTAS
  columns. pins: date-fn-1-spark-date-spelling/C-002
  **V3-COV (2026-09-03):** the `SourceMatch::Unique` arm returns the source array
  unchanged when its Arrow type already equals the target field's, before the store-assignment
  check and the cast kernel. This is the bulk-append hot path and the identity case is the common
  one; the guard and the strict cast still run for every pair that actually differs.
  pins: v3-cov-statement-coverage/C-004
- `concurrency.rs` — **PERF-ICE-WRITEPATH-1 round 2 (2026-09-05):** on the CTAS write node
  `repark.write.max-concurrent-files` is **binary, not a cap** — 1 writes one data file through a
  `CoalescePartitionsExec`, 2 or more writes one data file per DataFusion partition. Measured at
  cap 1/2/4/8 on one 1e6-row seed: 1 / 8 / 8 / 8 data files. It still bounds the stream write
  paths (INSERT, MERGE, overwrite, predicate DML) at the worker count it names, which is the
  meaning the module's own doc comments carry and which this unit did not change. The reason it
  cannot be a cap on the node is in `partition_write.rs` below.

- `partition_write.rs` — **PERF-ICE-WRITEPATH-1 (2026-09-05):** `IcebergPartitionWriteExec`, the
  CTAS write node. One output partition per input partition, each draining exactly that partition
  through the existing serial writer; `execute_stream` coalesces the node and the coalesce spawns
  one task per partition, which is where the parquet encode and zstd of the writers stop sharing
  a task. RePark spawns nothing and gains no dependency: the parallelism is the DataFusion
  executor's, which is why this is a node and not a `tokio::spawn` (`clippy.toml` bans that, the
  rust-code-quality scan bans routing around it through `JoinSet` or a helper crate, and `tokio`
  is a dev-dependency of this crate).
  **One writer per input partition, not `min(cap, partitions)`.** A writer that drained several
  input partitions in sequence measured 738 ms against 547 ms for one-each on the partitioned 1e6
  CTAS, and worse, it is unbounded in memory: DataFusion's repartition channels are unbounded
  per output partition and only gate when EVERY channel is non-empty, so the partitions a writer
  has not reached yet buffer whole. `repark.write.max-concurrent-files` therefore selects between
  one writer over a `CoalescePartitionsExec` (cap 1, one data file) and one writer per partition
  (cap 2 or more); it still bounds the stream write paths that INSERT, MERGE, overwrite and
  predicate DML use, which this node does not touch. The knob is read from the session
  configuration, so it is a builder `.config(...)`, not a post-build `conf.set`.
  **Determinism is content-derived, because the DataFusion partition index is NOT stable.**
  Round 1 ordered the committed files by the writer index and claimed reproducibility; the round-2
  critic refuted it, and the instrumented measurement says why: over eight UNEQUAL source files,
  six identical v3 CTAS gave six different partition-index-to-source-file assignments (partition 1
  read the 3,000-row file in one run and the 40,000-row file in the next), so the writer index is
  a property of that execution, not of the statement. `stable_commit_order`
  ([file_order.rs](file_order.rs)) therefore sorts the committed files by partition value first
  (V3-11 unchanged), then by each field's lower bound in field-id order, then the upper bounds,
  then record count, file size and path — a total order that is a function of the DATA. Six runs
  of the refuting fixture commit ONE manifest record-count sequence and ONE `first_row_id` map at
  16 partitions.
  **Round 3 crossed that boundary and narrowed the claim.** The scan's row-to-file GROUPING is not
  stable either: on four cores the same eight-file source is packed into four writers differently
  from run to run inside ONE process — measured 4 to 6 distinct groupings in 10 runs at
  `target_partitions = 4`, against 1 in 10 at 16, which is why CI's 4-core runner reddened the
  round-2 pin and this box never did. So the committed LAYOUT and the `_row_id` a given row
  receives are **not** reproducible across groupings, and no writer-side ordering can make them
  so — the rows themselves land in different files. What IS true at every partition count, and is
  what the pin now asserts at 3, 4, 8 and 16: the manifest ascends by content, `_row_id` tiles it
  contiguously from zero, the committed row set is the expected digest of ids, and two runs that
  produce the SAME grouping produce the same id-to-`_row_id` map — keyed by a hash of that map,
  since keying it by the record-count sequence made the conjunct unfalsifiable (round-3 critic
  G3). The residual is filed as `WRITE-GROUPING-CTAS-1`.
  An input error raises a shared flag: siblings stop taking `Ok` batches and close what they hold,
  and the failure sweep deletes **every data file the attempt created** — the completed files
  plus every parquet that appeared under the table's data root since the attempt began, which is
  how the failing writer's own rolled files are reclaimed (round-2 S2-2: a 64 KiB target file size
  left 9 of them behind before this).
  **The sweep's precondition is checked, not assumed** (round-2 F7): the census-and-sweep arm runs
  only when the table has NO current snapshot — the CTAS case, where the table is staged or freshly
  created and this statement is its only writer. Against a table that already has a snapshot the
  sweep falls back to deleting just the files its own writers completed, so a concurrent or
  pre-existing data file is never touched. Pinned both ways:
  `a_failed_partition_deletes_every_completed_data_file` (fresh table, nothing survives) and
  `a_failed_write_into_a_committed_table_sweeps_only_its_own_files` (seeded commit, its files
  survive, AND so does a file another writer drops into the data root during the attempt — which
  is the case the gate actually guards, since the census by itself already protects everything
  that existed before it).
  In-module pins: every input partition gets its own writer and its own data file, and the files
  come back in CONTENT order (the mock's partitions carry ascending row ranges, so content order
  and partition order coincide there — the assertion says so), and a one-task drive of the same
  four partitions answers identically; the returned files carry writer-index order over three runs; and a late
  failure in one partition leaves no parquet file in the warehouse. There is deliberately no
  wall-clock assertion in the unit suite — under `cargo test` on a loaded box the fixed cost of
  four small parquet writes swamped the injected delay (a 6.4 s floor against a 6.0 s delayed
  run), so the timing evidence lives in
  [../../../../docs/perf/iceberg-write-baseline.md](../../../../docs/perf/iceberg-write-baseline.md)
  instead, where it is measured on a release module. `make verify` is green with the pin in place.
  The vectorized partition splitter this path calls on every partitioned write is fork ask
  **F-28** on `f-28-vectorized-partition-splitter`: the splitter lexsorts the partition-value
  columns, reads group boundaries with `arrow_ord::partition` and materializes one
  `Literal::Struct` per group instead of one per row, keeping the row-wise path for Float,
  Double, Unknown and empty partition types, where Arrow total-order equality is not Iceberg
  `Struct` equality (`-0.0` and `0.0` are one group under `OrderedFloat` and two under total
  order). It is NOT consumed here: the pin bump is its own PR
  ([../../../../docs/fork-sync.md](../../../../docs/fork-sync.md)), so the fork half is measured
  through a temporary, never-committed path override.
  pins: perf-ice-writepath-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-011
- `distribution.rs` — **WRITE-DISTRIBUTION-1 (2026-09-06):** the hash distribution rule before a
  partitioned write, Spark's Iceberg default `write.distribution-mode = hash`. `hash_distribution`
  wraps the CTAS node's input in DataFusion's `RepartitionExec` under `Partitioning::Hash` over one
  `PartitionTransformExpr` per partition field, so every row of one partition value reaches exactly
  one writer and the commit holds one data file per value present — 8 where the node alone wrote
  64 at `shuffle.partitions = 8`. The key is the TRANSFORM value, computed the way the writer's
  own `PartitionValueCalculator` computes it: the source column cast to the Iceberg field's Arrow
  type, then the fork's `create_transform_function` — so `bucket(4, id)` keys on the bucket, not
  on `id`, and a NULL is one value. The hash is DataFusion's seeded `REPARTITION_RANDOM_STATE`, so
  the row-to-writer map is a function of the data, not the shared counter of the round-robin
  PERF-ICE-WRITEPATH-1 rejected. The rule is skipped when the table is unpartitioned or there is
  one writer: the unpartitioned CTAS keeps one file per input partition, by decision — Spark's 2
  at 8 partitions is its scan split count, not a distribution rule, and a coalesce below the
  partition count is the writers-below-partitions shape measured at 738 ms against 547 ms with
  unbounded buffering. Buffering here is bounded in practice because every output partition is
  consumed concurrently by its own writer task (RSS peak 760–785 → 842–861 MB on the 1e6
  partitioned cell). What the rule changes on the partitioned path: a data file's row order
  follows the channel interleaving of the input partitions, so a row's `_row_id` is not
  reproducible across runs; the manifest still ascends by partition value and `_row_id` still
  tiles it. A partition source column the input plan lacks is a planning error, not a silent
  skip. In-module pins: one value → one writer; the same layout across two runs; input partitions
  without rows and an empty input; NULL partition values; `bucket(4, id)` + `day(ts)` keyed on the
  transform (mutation: hashing the raw source column reds this pin alone); an unpartitioned table
  bypasses the rule; a plan lacking a partition source column errors. Numbers:
  [docs/perf/iceberg-write-baseline.md](../../../../docs/perf/iceberg-write-baseline.md) §8.
  pins: write-distribution-1/C-001, C-002, C-003, C-004, C-005, C-006, C-008
  **WRITE-DISTRIBUTION-2 (2026-09-06):** the same rule on the stream write paths, which have no
  physical plan to hang a `RepartitionExec` on — the input is a one-shot
  `SendableRecordBatchStream`, and a repartition node would re-drive it once per output. So
  `PartitionRouter` applies the rule in the dispatcher instead: it hashes each row's partition
  values with the writer's own `PartitionValueCalculator` plus `create_hashes` under DataFusion's
  seeded `REPARTITION_RANDOM_STATE`, splits the batch with `take`, and each part goes to its
  slot's worker. Same key family as the plan node, same seed, one pass over the input, no spawn.
  The MERGE serial lineage writer (V3 tables) already runs one writer and is untouched; plain
  `INSERT INTO` never reaches this module (the fork's `TaskWriter` owns it — still 32 files
  where Spark writes 8, an open fork ask). In-module pins: the stream path lands one value in
  one writer; determinism across two runs; NULL values; a two-field spec; MERGE inserts through
  the MERGE entry; `truncate(3, s)` over a view-typed string keys on the cast value (mutation:
  dropping the cast fails with the fork's `Unsupported data type for truncate transform:
  Utf8View`); a late failure into a partitioned table leaves no data file. Numbers:
  [docs/perf/iceberg-write-baseline.md](../../../../docs/perf/iceberg-write-baseline.md) §6–§8.
  pins: write-distribution-2/C-001, C-002, C-004, C-005, C-006, C-008
  **WRITE-ORDER-DIST-1 (2026-09-06):** the rule reads `write.distribution-mode` — `none`
  skips it (the CTAS falls back to writers × values and the stream dispatcher deals whole
  batches round-robin), unset and `hash` keep one file per value, `range` takes the hash shape
  plus the per-writer sort below, and anything else is a planning error. When the table declares
  a default sort order, each writer sorts its own stream through DataFusion's `SortExec` over an
  in-memory source before the funnel writes it — `fanout_sorted_serial` / `fanout_sorted_stream`
  for the partitioned funnel, `drive_unpartitioned` for the unpartitioned one — so CTAS, INSERT
  OVERWRITE, and MERGE all commit monotone files with no new dependency and no spawned task. A
  sort field on a non-identity transform refuses loud; only identity fields sort. A dotted sort
  field resolves to the nested field id and sorts on the nested value through a struct-field
  expression with parent-null masking (round 2, 2026-09-06). In-module pins:
  the `none`/`hash`/`range` layouts, the unknown-mode planning error, cross-batch sorting, the
  identity return without an order, monotone committed files on both funnel entries, the
  `none` round-robin stream layout, the nested sort, and the transform refusal.
  pins: write-order-dist-1/C-007, C-008, C-010
  **ICE-SORTED-INSERT-1 (2026-09-17):** plain `INSERT INTO` sorts inside the
  fork's `insert_into` (F-SORTED-INSERT-1, RP-22), but the three RePark-owned
  writer sites never stamped the files they wrote, so `{t}.files` read NULL.
  `stamp` wraps a built `DataFileWriterBuilder` with the fork's
  `with_sort_order_id` carrying the table's default order id (0 when unordered,
  like the fork), called at the fanout close in `append.rs`, the lineage fanout
  in `merge/row_lineage.rs`, and the unpartitioned MERGE writer in
  `merge/mod.rs`. No sort is re-implemented here; the sort stays where
  WRITE-ORDER-DIST-1 put it.
  pins: ice-sorted-insert-1/C-003
  **Round 3 (2026-09-17):** `default_sort_lex_ordering` wraps every `Float32` /
  `Float64` sort key in `CanonicalFloatExpr` (`distribution/canonical_float.rs`),
  so the owned sort places NaN the way the fork's INSERT path does: every NaN,
  including a negative one, lands in one block above every value. The lineage
  fanout now calls `sort_batches_by_default_order` too
  (`merge/row_lineage.rs`), so each of the three stamp sites stamps only bytes
  that went through this sort.
  pins: ice-sorted-insert-1/C-006, C-008
  See [distribution/map.md](distribution/map.md).
- `partition_overwrite.rs` — **V3-COV (2026-09-03):** the module-private `StaticPartitionPlan`
  resolves the spec
  bindings and the `PARTITION (k=v)` map ONCE per commit and `stage_static_partition_overwrite_files`
  streams the batches through it instead of resolving per batch and collecting them all first;
  `inject_static_partition_columns` stays as the one-batch wrapper. `store_assign_source_column` runs the
  append path's `refuse_unless_write_store_assignable` and then a strict cast when a source
  column's Arrow type differs from its target field's, so a `SELECT` source producing
  DataFusion's view string representation writes instead of failing
  (`column types must match schema types, expected Utf8 but found Utf8View`); the `VALUES`
  spelling always worked, which is why DML-B never saw it. Registry `V3-COV-1` FIXED.
  Streaming moved the injection failure point: a batch whose column will not store-assign now
  refuses mid-stream, after earlier batches have already been written, where the collect-first
  shape refused before any file was staged. What that leaves behind is staged data files no
  commit ever references — the `OverwriteFiles` commit is still all-or-nothing and the table
  state is untouched either way — so the residue is orphaned files for
  `remove_orphan_files`, not a partial overwrite.
  pins: v3-cov-statement-coverage/C-004
  **DML-B:** static `PARTITION (k=v)` via
  `overwrite_files` / `overwrite_by_row_filter` + `validate_added_files_match_overwrite_filter`
  (pin `commit_rejects_added_file_outside_overwrite_filter`);
  dynamic `PARTITION (k)` / empty `PARTITION ()` via `replace_partitions`; empty-input
  dynamic guard names the three empty-dynamic surfaces (STATIC wipe, writeTo no-op, RePark
  refuse) in its rustdoc and error. `commit_*_to` variants pass `.to_branch`.
  pins: dml-b-insert-overwrite/C-001, C-002, C-004
  pins: rp-5-fork-repin/C-004 V3-COV pins in this file: a view-string source conforms to its Utf8 target instead of failing the rebuild (V3-COV-1); the identity arm hands the same buffer back while a non-assignable pair still refuses.
  **ICE-V3-WRITE-DEFAULT-1 round 5 (2026-09-17, ruling Q-21b-3):**
  `stage_static_partition_overwrite_files` takes the statement column list; a
  non-empty list maps the source by name (static columns from the clause, listed
  columns from the source, the rest NULL or `CANNOT_FIND_DATA` when required) and a
  listed static column refuses `STATIC_PARTITION_COLUMN_IN_INSERT_COLUMN_LIST`.
  `static_partition_source_columns` names the columns the clause assigns so the
  default fill skips them. pins: ice-v3-write-default-1/C-015
  **ICE-WRITE-OPTIONS-1 run 22b (2026-09-18):** the plan-then-inject prelude of
  `stage_static_partition_overwrite_files` is `pub(crate) static_injected_stream`, shared
  with the options variant in `write_options.rs`; the file stays under the default ceiling.
  pins: ice-write-options-1/C-014
  **ICE-OVERWRITE-MODE-1 (2026-09-19):** `PartitionOverwriteRequest` is a struct —
  `equalities`, `dynamic_names`, and every key in clause order (`names`) — so a mixed
  `PARTITION (k='v', k2)` parses; the static/dynamic plan enum and `plan_partition_overwrite`
  moved to `overwrite_scope.rs`. A clause key that binds no partition field refuses Spark's
  `[NON_PARTITION_COLUMN] … SQLSTATE: 42000` (`non_partition_column`).
  pins: ice-overwrite-mode-1/C-004, C-005
  **Round 2 (2026-09-19):** the empty-dynamic refusal (`refuse_empty_dynamic_overwrite`,
  `EMPTY_DYNAMIC_OVERWRITE_NEEDLE`) is gone: `commit_replace_partitions_to` returns the table
  unchanged when `overwrite_scope::replace_partitions_is_noop` says the stage holds no rows,
  as Spark skips the commit. A static value whose literal kind does not match its partition
  source is cast (`static_value.rs`) for both the row-filter datum and the injected column.
  A value that is not a plain literal (`TIMESTAMP '…'`) is kept as `refused_values` so the
  key is checked first. pins: ice-overwrite-mode-1/C-011, C-013, C-014
- `static_value.rs` — **ICE-SESSION-WRITE-CONF-1 round 5 (2026-09-20):** `cast_datum` answers
  an identity DECIMAL partition column. The arrow cast already lands the literal at the column's
  own precision and scale, so `decimal_datum` hands that mantissa to `Datum::try_from_bytes` with
  the column's `PrimitiveType` rather than re-deriving a precision — the datum the row filter and
  the removed-set lookup both compare with. Spark takes `PARTITION (amt = '1.50')` on
  `DECIMAL(10,2)` (`QD-TYPE-DECIMAL-*`); RePark used to refuse it `not assignable`, so the
  removed-set resolver never ran there. DOUBLE and BOOLEAN already cast and are pinned beside it.
  pins: ice-session-write-conf-1/C-060
- `static_value.rs` — **ICE-OVERWRITE-MODE-1 round 2 (2026-09-19):** casts a static
  `PARTITION` value to its partition source type with Arrow's cast (`safe: false`), the
  cast the engine's `CAST` runs, so `PARTITION (d = '2024-01-01')` on a `DATE` column
  replaces that partition and an invalid value refuses with the same `Cast error` text as
  `SELECT CAST(…)`. `cast_datum` turns the cast value into an Iceberg `Datum` (boolean, int,
  long, float, double, date, string, timestamp and timestamptz micros); other targets refuse.
  A string value on a TIMESTAMP / TIMESTAMPTZ source refuses (`CAST-TS-STRING-1`): Arrow reads
  it in UTC where Spark casts in the session zone (verification critic, 2026-09-19).
  pins: ice-overwrite-mode-1/C-019
  pins: ice-overwrite-mode-1/C-013
- `overwrite_scope.rs` — **ICE-OVERWRITE-MODE-1 (2026-09-19):** the one overwrite decision
  for every door. `OverwriteMode` (session `partitionOverwriteMode`, the typed
  `OverwriteIntent` — `Static` for `saveAsTable`, `Dynamic` for `writeTo.overwritePartitions`
  — and the `overwrite-mode` writer option) plus "has static values" picks the scope:
  dynamic → `ReplacePartitions`; static with values → `RowFilter` over the static values
  (Spark's `OverwriteByExpression`); static without values → the whole table, unless the
  writer option says `dynamic` (Iceberg's `SparkWriteBuilder.overwrite` turns an
  `alwaysTrue` filter dynamic; the static intent ignores it). `plan_overwrite` validates the
  clause first: an unpartitioned table refuses the first key `NON_PARTITION_COLUMN`, and
  (round 2) any key that is not an identity partition column — a transform source or a
  transform field name, static or dynamic — refuses `NON_PARTITION_COLUMN` too, as Spark's
  identity-only `partitionColumnNames` does (PIN O5's `NotImplemented` retired); a deferred
  literal refusal surfaces only after every key binds. `replace_partitions_is_noop` is the
  empty-dynamic rule. `validated_static_equalities` serves the `BY NAME` path.
  Tests: [../tests/overwrite_scope.rs](../tests/overwrite_scope.rs).
  pins: ice-overwrite-mode-1/C-002, C-004, C-005, C-006, C-007
- `insert_gate.rs` — **WI-2 (2026-08-15):** `InsertStoreAssignment`, an `AnalyzerRule` over
  `LogicalPlan::Dml(WriteOp::Insert(_))` that runs `store_assign.rs`'s matrix — imported, never
  duplicated — against the pre-cast types in the synthesized projection's INPUT schema. Registered
  by `repark_spark::SparkExtension::register`, BEFORE `repark_functions::analyzer_rules()`, so a
  `DATE → INT` insert cites Spark's WRITE class rather than the CAST class. Judges exactly
  `Alias(Cast(Column(c), target))`: that shape is provably the conform cast DataFusion
  synthesized, while a user-written explicit `CAST` (legal Spark — the user's stated intent)
  reaches this projection already conformed, as a bare column, and is invisible to the rule.
  Named residual: `Cast(Literal, …)` inside a `Values` node, where the synthesized and explicit
  forms are byte-identical. Ledger:
  [`../../../../task/wi2-g6-cast-integrity-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-16-wi2-g6-cast-integrity-ledger.md).
- `insert_defaults.rs` — **ICE-V3-WRITE-DEFAULT-1 (2026-09-17):** the ONE home for
  filling omitted columns from `write_default` on every write path: `column_defaults`
  reads the table defaults, `fill_insert_plan` rewrites a short INSERT plan, an
  explicit NULL stays NULL, and a missing required column keeps Spark's error.
  `rewrite_insert_markers` passes a missing table through unloaded, so the door's
  standard missing-table error fires instead of a leaked `TableNotFound`. An unloadable
  table, an unrenderable default literal, and a default that does not fit its column type
  all surface as plan errors. Its entry points carry
  `#[allow(clippy::missing_errors_doc)]` in place of the `# Errors` doc comment the
  no-code-comments ruling forbids. The marker pass probes the AST for `DEFAULT`
  first and loads nothing without one; the loaded table travels in `MarkerRewrite`
  into `fill_insert_plan`, so an INSERT pays at most one catalog load. Unit tests
  (including the load-count pins over a counting test catalog) live in
  `insert_defaults/tests/mod.rs` — a `tests/` directory so the C-009 setter guard
  (`test_rp3_c009_write_default.py`, needles `with_write_default` / `write_default(`,
  `tests` path parts exempt) reads the test-only `with_write_default` builder as test
  code; the pre-scan is named `schema_has_primitive_fill` for the same guard (run 21b
  round 2, 2026-09-18).
  **Run 21b round 2 (2026-09-18, ruling Q-21b-9):** `refuse_default_marker_under_with`
  refuses a `DEFAULT` marker in the outer VALUES / SELECT list of an INSERT whose query
  carries `WITH` — `UNRESOLVED_COLUMN.WITHOUT_SUGGESTION` naming `DEFAULT`, SQLSTATE
  42703 — before any table load, on `INSERT INTO` and `INSERT OVERWRITE`, both doors
  (`rewrite_insert_markers` and `rewrite_markers_with_table` both call it). Spark 4.1.2
  resolves `DEFAULT` only in the top-level INSERT's own list and refuses it under
  `WITH` (its text carries `WITH_SUGGESTION` and the CTE's columns; RePark names none).
  `DEFAULT` inside a CTE body or derived table is never rewritten and refuses in
  planning (`No field named default`), as Spark refuses it.
  pins: ice-v3-write-default-1/C-021
  pins: ice-v3-write-default-1/C-004, C-005, C-006, C-007
  **Round 5 (2026-09-17):** `overwrite_source_with_defaults` is the one
  `INSERT OVERWRITE` fill both doors share — whole-table and both PARTITION arms —
  appending `(CAST(default)) AS col` for every omitted defaulted column not listed
  and not assigned by a static clause, and returning the extended column list.
  `schema_has_primitive_fill` is the cheap pre-scan that skips the Arrow conversion
  and the `ColumnDefaults` map on tables with no defaults (R-04).
  pins: ice-v3-write-default-1/C-015, C-019
  `rewrite_markers_with_table` is the DEFAULT-marker pass over an already-loaded table
  (`rewrite_insert_markers` loads, then calls it); `query_has_default_marker` is the
  public AST probe. The Spark door's `INSERT OVERWRITE` calls both (ruling Q-21b-4).
  pins: ice-v3-write-default-1/C-016
- `store_assign.rs` — **U8 WRITE-SQL PR2 (2026-09-25):** `ansi_store_assignable` judges a
  struct pair field by field, as Spark's `canANSIStoreAssign` does. The two structs must have
  the same length and the same names in the same order (case-insensitive), and each field
  must be assignable. The field metadata (Iceberg's `PARQUET:field_id`) is ignored. Before, a
  struct passed only by identity, so every MERGE struct assignment refused against a table
  schema that carries field ids. The gate stays positional: the Spark door rebuilds a
  reordered struct by name before it reaches the gate (u8-write-sql C-030, C-032).
  `without_field_metadata` strips those ids at every depth for the cast type name and for
  both types in the refusal text, so no `PARQUET:field_id` reaches a user. pins: u8-write-sql/C-030
  **WO U9-TYPES-1 r2 (2026-09-25):** a map pair is assignable when its key and its value
  types each are (Spark's `MapType` arm), so a MERGE assigns `map()` (`Map<Null, Null>`) to a
  typed map column; `a_map_assigns_when_its_key_and_value_assign`. pins: u9-types-1/C-006
- `store_assign.rs` (crate-private) — **WI-1 (2026-08-15):** the ONE home for Spark's ANSI
  store-assignment matrix (`Cast.canANSIStoreAssign` → Arrow):
  `ansi_store_assignable` / `normalize_for_assignment` /
  `refuse_unless_ansi_store_assignable` (`MERGE `-labelled callers, class
  `INCOMPATIBLE_DATA_FOR_TABLE` — byte-identical #111/#135 text) and
  `refuse_unless_write_store_assignable` (non-MERGE write paths, sub-class
  `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST`). Hoisted out of `merge/insert.rs`, which
  had the only two call sites in the tree, so `append.rs` / `overwrite.rs` share the predicate
  instead of forking a second one. Needle `not ANSI-store-assignable`. Named narrowing: the
  write-path entry point excuses NESTED pairs (the v1 matrix judges them by identity, which
  would be a NEW refusal on paths that conform `List<Utf8View>` → `List<Utf8>` correctly today).
  **Not** a CAST-legality matrix — see `planning/hardening/G63-DATE-INT-DESIGN.md` §3.3.
  Ledger: [`../../../../task/wi1-insert-store-gate-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-15-wi1-insert-store-gate-ledger.md).
  **CTAS-VIEW-1 (2026-09-03):** `BinaryView` is a binary-width variant with `Binary`/`LargeBinary`
  (same class as `Utf8View` among string widths), so parquet-read binary columns store-assign.
  pins: ctas-view-1-conform-stream/C-002
- `alter.rs` — `ALTER TABLE` primitives on iceberg-rust public API: SET/UNSET TBLPROPERTIES
  (**V3-10:** the combined `alter_table_properties` seat moved to `format_version.rs`; the three
  atomicity tests stay here beside the `CommitFaultCatalog` harness they need and now drive
  `set_properties_and_format_version` — one action, no half-applied state),
  `rename_table`, schema evolution (`apply_schema_changes` / `SchemaChange` → fork
  `UpdateSchema`), partition-spec evolution (`apply_partition_spec_changes` /
  `PartitionSpecChange` → fork `UpdatePartitionSpec`). Return `iceberg::Result`.
  **ICE-COLUMN-REORDER-1 (2026-09-17, round 2 Q-20b-5):** `SchemaChange::MoveColumn`
  (top-level and nested paths via the fork's standalone `move_first` / `move_after`); every
  move commits through one `UpdateSchema` transaction, with batch-added names known to the
  resolver. `apply_schema_changes_on_table` runs the commit on an already-loaded table so
  doors load once. The partition-spec family moved to `partition_spec.rs` in the same change
  (the size ratchet), behaviour-identical.
  pins: ice-column-reorder-1/C-001, C-002, C-003, C-004, C-005, C-008, C-010, C-011
- `column_move.rs` — **ICE-COLUMN-REORDER-1 (2026-09-17, round 2 Q-20b-5):**
  `resolve_move_names` (pure fork-index resolution: the fork's own
  `field_by_name_case_insensitive`, bare `AFTER` references qualified into the mover's
  struct, top-level suggestions with Spark's `UNRESOLVED_COLUMN` framing) plus
  `starts_with_alter` (the zero-alloc `ALTER`-prefix scan gating both doors' intercepts).
  Split out of `alter.rs`, which sits at its exact ceiling.
  pins: ice-column-reorder-1/C-001, C-002, C-003, C-006, C-007, C-008, C-014
  **WO U5 PR2a round 3 (2026-09-24):** `unresolved_column_parts` renders the unresolved name
  from its parsed parts, so a backquoted `` `st.x` `` stays one part. Each suggestion splits on
  `.`, so a top-level `p.q` renders `` `p`.`q` `` as Spark does. `unresolved_column` splits a
  dotted name and delegates to it.
  pins: ice-nested-evo-1/C-048
- `nested_column.rs` — **ICE-NESTED-EVO-1 (2026-09-17):** `ColumnPathChange` (`Add` under an
  optional dotted parent — a struct, or a list or map whose element or value struct the fork
  resolves — with `FIRST` / `AFTER` sibling positions; `Rename` and `Drop` by dotted path) and
  `apply_column_path_changes`, which folds them into ONE case-insensitive fork `UpdateSchema`
  on an already-loaded table. Both doors' nested `ALTER TABLE` intercepts commit through it.
  A sibling of `alter.rs` rather than a new `SchemaChange` arm because `alter.rs` sits at its
  exact file-size ceiling. 2 in-module tests (children evolve by field id; a required child
  without a default refuses and the schema id stays).
  pins: ice-nested-evo-1/C-007, C-010, C-011, C-012
  **Round 2 (2026-09-18, run 22b):** `nested_add_refusal` is the Spark-shaped pre-check both
  doors run before the commit: an unknown parent answers Spark's
  `[UNRESOLVED_COLUMN.WITH_SUGGESTION] … SQLSTATE: 42703`, an existing child (case-insensitive)
  Spark's `[FIELD_ALREADY_EXISTS] Cannot add column, because `s`.`a` already exists in
  "STRUCT<…>". SQLSTATE: 42710`, the table schema rendered in Spark's `DataType.sql` form
  (`NOT NULL`, `COMMENT`, backticked names that need it). It reuses `column_move.rs`'s
  `unresolved_column` / `top_level_names` (now `pub(super)`). 2 more in-module tests.
  pins: ice-nested-evo-1/C-019
  **WO U5 PR1 round 2 (2026-09-24):** `resolve_nested_type_change` is the nested `ALTER COLUMN
  … TYPE` pre-check. It walks the path the way Spark does: struct steps case-insensitive,
  `key` / `value` / `element` exact, and anything else `INVALID_FIELD_NAME` or
  `UNRESOLVED_COLUMN`. It then answers the from→to pair in Spark's order, returning a
  `NestedTypeRefusal::Analysis` (`not_supported_change_column`, Spark SQL type names) when
  `canUpCast` fails. An `Unsupported` refusal carries Iceberg Java's `Cannot change column
  type: …` when the pair is not an Iceberg promotion, or `Cannot update map keys: map<…>` for a
  promotion on a map key. Otherwise it returns the resolved dotted path. `iceberg_type_name`
  renders Java's `Type.toString` (`decimal(9, 2)`, `map<int, int>`), which the fork's Display
  does not.
  pins: ice-nested-evo-1/C-027, C-029
  **Round 3 (2026-09-24):** `nested_spark_only_type_refusal` resolves the same path and
  answers `NOT_SUPPORTED_CHANGE_COLUMN` with a caller-supplied Spark target name. It serves
  targets Iceberg has no type for (TINYINT, SMALLINT, CHAR(n), VARCHAR(n)), which Spark never
  up-casts to.
  pins: ice-nested-evo-1/C-032
  **WO U5 PR2a (2026-09-24):** `resolve_column_path` resolves a column path of any depth the
  same way for `ALTER COLUMN … COMMENT` and returns the schema-cased dotted name. A map `key`
  step refuses with the shared `map_key_refusal` (`Unsupported table change: Cannot update map
  keys: map<…>`), which `resolve_nested_type_change` now also uses.
  pins: ice-nested-evo-1/C-038
  **Round 2 (2026-09-24):** `resolve_column_path` returns the schema-cased parts, not a dotted
  name. The Spark door compares them to refuse a repeated column and joins them for
  `UpdateColumnDoc`.
  pins: ice-nested-evo-1/C-040
  **Round 3 (2026-09-24):** `resolve_nested_path` keeps a `MapKeyTouch`. A path ending at a
  map `key` gives `Cannot update map keys: <map>`, and a path strictly under one gives `Cannot
  alter map keys: <map>`, naming the innermost map. The nested TYPE route raises it where it
  raised the key refusal. `resolve_column_path` returns a `ResolvedColumnPath` that holds the
  refusal back, so the Spark door can resolve and check repeats first.
  `column_paths_commit_refusal` then picks the refusal of the first map in the post-order
  schema visit that Iceberg's `ApplyChanges` makes. `doc_lands` is false for a list `element`
  or map `value`, whose doc `ApplyChanges` drops. Unresolved names render from the parsed
  parts through `column_move::unresolved_column_parts`.
  pins: ice-nested-evo-1/C-044, C-045, C-046, C-048
- `nested_type_sql.rs` — **ICE-NESTED-EVO-1 round 2 (2026-09-18, run 22b):** the one token
  rewrite both doors run on a nested column type: a struct child's `NOT NULL` becomes the
  struct-field option `OPTIONS(repark_not_null=TRUE)` (the only struct-field suffix
  sqlparser models), which `struct_field_required` reads back as an Iceberg required child;
  a hand-written struct-field `OPTIONS` refuses Spark's `[PARSE_SYNTAX_ERROR] … near
  'OPTIONS'. SQLSTATE: 42601`; with `map_parens` (the ANSI door, `GenericDialect`) `MAP<K, V>`
  becomes `MAP(K, V)`, the same AST. A `>>` closing two brackets splits. Comparisons and
  `ARRAY<… NOT NULL>` are left alone. 4 in-module tests.
  pins: ice-nested-evo-1/C-016, C-017
  **Round 3 (2026-09-18, run 22b, V-002):** both doors call `rewrite_create_column_types`,
  which finds the column-definition list (the first parenthesized group after `TABLE [IF NOT
  EXISTS] <name>`) and rewrites only that range; the `AS SELECT` query and every other token
  pass through verbatim, so a column named `map` / `struct` compared with `<` is no longer
  rewritten into `MAP(` / `OPTIONS(…)`. `create_column_list_has_nested_type_opener` is the ANSI
  gate over the same range. 2 more in-module tests (mutation-checked: rewriting the whole
  statement reds both).
  pins: ice-nested-evo-1/C-022
- `partition_spec.rs` — the partition-spec evolution family, split out of `alter.rs`
  behaviour-identical (the size ratchet): one `PartitionSpecChange` transaction through
  `apply_partition_spec_changes`. `AddField` carries a source column, a transform
  (`identity` / `bucket[N]` / `truncate[W]` / `year` / `month` / `day` / `hour`) and an
  optional `AS` name; `RemoveFieldByName` drops by partition name;
  `RemoveFieldByTransform` drops by source-plus-transform pair; `ReplaceField` drops by old
  name and adds source plus transform with an optional new name; `RenameField` renames by
  current name. Errors propagate the load, validation, or commit failure unchanged.
  **IPI-26/27 round 3 (2026-09-21, cell `D-REPLACE-PART-FIELD`):** `ReplaceFieldByTransform`
  names the replaced field by `(source column, transform)` — resolved against the CURRENT
  default spec (source column case-insensitive, like the fork's own transform resolution)
  into the by-name `ReplaceField` flow, and a pair matching no current field refuses loud
  `DataInvalid` before anything commits. Measured Spark answer for
  `REPLACE PARTITION FIELD days(ts) WITH hours(ts)`: spec `[["ts_hour","hour","ts"]]`,
  spec-count 2.
- `sort_order.rs` — **WRITE-ORDER-DIST-1 (2026-09-06):** `apply_write_order`, the one-transaction
  write-layout primitive over the fork's `Transaction::replace_sort_order` plus an optional
  `write.distribution-mode` property set: column names resolve case-insensitively against the
  table schema, dotted paths through struct types included (an unknown column is a loud
  `DataInvalid` and commits nothing), an empty field
  list resets the default to the unsorted order 0 (the fork dedups it, so no order is appended),
  and an identical order reuses its id the way Spark's sequence does. Return `iceberg::Result`.
  pins: write-order-dist-1/C-001, C-002, C-003, C-004, C-005, C-006
  **WO U5 PR2b (2026-09-24):** `WriteSortField` carries a `Transform`, and every field goes
  through the fork's `ReplaceSortOrderAction::sort_by` (RP-46), so a transform term lands with
  the fork's void, width and bind checks. Identity fields pass `Transform::Identity`.
- `format_version.rs` — **V3-10:** `set_properties_and_format_version` folds the fork's
  `UpgradeFormatVersionAction` and `UpdatePropertiesAction` into ONE transaction, so an ALTER
  carrying `format-version` beside another key is one metadata commit as it is on Spark; nothing
  is committed when there is neither an upgrade nor a property to write, which is why requesting
  the version a table already has writes no metadata file. It takes the table the door already
  loaded, so an upgrading ALTER loads once rather than twice, and takes `sets` by value (still
  generic over the hasher, which `clippy::implicit_hasher` requires of an exported signature)
  because both doors own theirs. `format_version_number` reads the resolver's SIGNED version off a loaded
  table and `format_version_from_number` errors rather than falling back, so an out-of-domain
  number can never be silently taken as v2 and a negative request reaches the downgrade branch
  rather than the parse branch. It is also the seat the old `alter::alter_table_properties` folded
  into (`target: None`); that function had no production caller left. Its entry points carry
  `#[allow(clippy::missing_errors_doc)]` in place of the `# Errors` doc comment the
  no-code-comments ruling forbids; every error they raise comes from the fork.
  pins: v3-10-upgrade-v2-to-v3/C-003, C-005
- `snapshot_refs.rs` — product CREATE/DROP/REPLACE BRANCH|TAG helpers over fork
  `ManageSnapshots` (+ retention setters). Write-to-branch routing lives in the Spark
  door (`repark-spark` `write_to_branch.rs`) and the `to_branch` / `with_commit_branch`
  commit seats. **ICE-BRANCH-OPS-1 (2026-09-17):** `list_snapshot_refs` reads every ref as
  `(name, kind, snapshot_id)` through the fork's refs inspect table for the branch-procedure
  pre-checks (the fork's refs-map field is crate-private). **Round 2:** the inspect batch
  schema is gated (strict Utf8/Utf8/Int64) and a mistyped refs schema refuses typed instead
  of panicking. **WO U5 PR2b (2026-09-24):** `create_branch_on_empty_table` is Java's
  `SnapshotManager.createBranch(name)` on a snapshot-less table: an empty fast append
  `to_branch(name)`. The `engine.operation-id` snapshot property is what lets the fork's
  empty-commit guard pass, so no data file is needed. Retention is a second commit, because
  the fork checks a retention update against the base table, where the branch is still absent.
  **WO U5 PR2b round 2 (2026-09-25):** `refuse_ref_write_on_format_v1` is the one kernel that
  keeps a non-main ref out of format v1 metadata, which the fork writes without refs (fork unit
  F-V1-REFS-1). It returns UnsupportedOperationException `<KIND> on the format v1 table
  <ns>.<table> is not supported: the Iceberg fork writes v1 metadata without its refs, so the new
  ref would be lost` for a branch or tag other than `main`, and for `main` with retention. The
  ref-writing helpers (`create_snapshot_ref[_with_retention]`, `replace_snapshot_ref`,
  `create_or_replace_snapshot_ref`, `create_branch_on_empty_table`) call it after they load the
  table and now return DataFusion `Result`, so both doors inherit the refusal;
  `testing_create_ref` keeps its `iceberg::Result` by folding the error back.
  `commit_target::maybe_to_branch` takes the table and calls it for every RePark branch commit
  (append, overwrite, partition overwrite, write options, MERGE).
  pins: ice-nested-evo-1/C-053
- `testing_support.rs` — `testing_create_ref` (wraps `create_snapshot_ref`) for fixtures only;
  product SQL routes via `snapshot_refs`.
- `concurrency.rs` — `repark.write.max-concurrent-files` (default 4, ≥1 or loud): DataFusion
  `ConfigExtension` (`ReparkWriteConfig`) + builder-map parse (hyphen + underscore). Parallel
  drivers share an abort flag so source/worker errors skip `finish`/`close`.
- `scan_concurrency.rs` — `repark.scan.concurrency-limit` (optional; unset = fork default) for
  the MERGE target scan's `with_concurrency_limit`.
- `scan_prune.rs` — MERGE target-scan pruning + ON bare-equality parser + residual bounds
  (`repark.merge.scan-pruning`, default true); `ReparkMergeConfig` also carries
  `file_scoped_rewrite`. **MG-1 (2026-08-15):** char-boundary ON scanners (`char_indices`);
  skip-conjunct helpers (`identical_int_key_width`, `unique_schema_field`,
  `residual_bounds_predicate`) — identical Int32/Int64 only, probe failures skip, source
  column resolved case-insensitively then quoted. Ledger:
  [`../../../../task/mg1-scanprune-hardening-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-15-mg1-scanprune-hardening-ledger.md).
- `file_scoped_rewrite.rs` — filter `FileScanTask`s by affected-path allowlist
  (`repark.merge.file-scoped-rewrite`); refuses a non-empty allowlist matching zero or partial
  path set (survivor-loss guard). Test helper `dummy_task` constructs `#183` Arc innards
  (`data_file_path: Arc<str>`, `project_field_ids: Arc<[i32]>`, `deletes: Arc<[…]>`), and at RP-39
  fork #317's `file_record_count` field.
- `name_resolution.rs` (crate-private) — the shared case-insensitive by-name column resolver
  (Spark `spark.sql.caseSensitive=false` conform semantics); used by both `append` conform and
  merge star expansion so the two surfaces cannot drift. `resolve_write_column` is the single
  resolve-or-refuse entry for write target lists (case-twin collisions refuse
  `[AMBIGUOUS_REFERENCE]` / `42704`, one option per twin in the requested spelling — run 21b
  Q-21b-1 / Q-21b-2 from the measured Spark cells); `arrow_field_twins` /
  `ambiguous_write_message` are its pieces. Twin targets only exist on non-fork schemas: the
  fork refuses to load a twin Iceberg schema (round 21b step 5 applies the requested spelling and
  42704 in `ambiguous_write_message`). pins: ice-mixed-case-1/C-004, C-016
- `predicate_dml.rs` — **ICE-SESSION-WRITE-CONF-1 round 1 (2026-09-19):** `PredicateDmlSpec`
  carries the target `branch` (`Some(name)` when the statement named `<table>.branch_<name>`), and the executor scans that ref's snapshot and commits on it;
  and its executor is comment-free per the ban (it plans the identity SELECT over the pinned
  scratch, then COW-rewrites or writes MoR deletes; errors are planning, write or commit
  failures, plus `NotImplemented` for non-V2 MoR; data files follow the write-format option
  over `write.format.default`, v2 MoR writes position deletes per `write.delete.format.default`
  over the data format, v3 writes deletion vectors).
  `try_allowed_plain_update` joins `plain::try_allowed_plain_identity` as an owned identity
  route (see `predicate_dml/map.md`). pins: ice-session-write-conf-1/C-038
- `position_delete.rs` — **ICE-SESSION-WRITE-CONF-1 round 1 (2026-09-19):**
  `write_position_deletes` takes the resolved `WriterStagingOverrides`, so a
  position-delete file takes the writer option / session codec its commit resolved.
  pins: ice-session-write-conf-1/C-040
- `writer_props.rs` — **ICE-SESSION-WRITE-CONF-1 round 1 (2026-09-19):**
  `position_delete_writer_properties_for(table, staging)` resolves the delete-file codec
  in Iceberg's `SparkWriteConf` order: writer option (already merged over the session conf)
  > `write.delete.parquet.compression-codec` > `write.parquet.compression-codec` > default.
  Before the round it read the data-file property only, so the delete-codec property was
  silently ignored too; the resolver is comment-free per the owner ban, and this row is where
  its order is written down. pins: ice-session-write-conf-1/C-040
- `position_delete.rs` (crate-private; two `pub` re-exports via `mod.rs`) — merge-on-read
  WRITE primitive: turn `(_file, _pos)` pairs into committable position-delete `DataFile`s by
  driving the fork's production `PositionDeleteFileWriter`. Owns sort order (ascending
  `(file_path, pos)`), `write.delete.granularity` grouping (**MW-9:** unset → Spark `file`;
  `'partition'` → one file per `(spec_id, partition)`), and partition stamping (each delete
  file carries the `(spec_id, partition)` of the data file it deletes from, resolved from the
  snapshot's DATA manifests — never the table's current default spec). Unpartitioned groups keep `partition_key = None`;
  fork #239 (`d408da42`) errors on `build(None)` with no spec, so that path chains `.unpartitioned()`.
  An evolved unpartitioned spec whose id is not 0 also chains `.with_partition_spec(spec)`
  so the fork does not fall back to stamping spec 0 (**M16**,
  [`../../../../task/m16-posdelete-specid-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-15-m16-posdelete-specid-ledger.md)).
  pins: rp-3-fork-repin/C-002
  **RDF-1 (2026-09-02):** the Parquet properties come from
  `writer_props::position_delete_writer_properties_for`, not the plain data-file builder, so the
  `file_path` and `pos` bounds are exact and a delete file naming ONE data file is file-scoped.
  pins: rdf-1-position-delete-bounds/C-002
  `#182` `PartitionKey::new` is fallible (`validate_partition_data`); this module maps
  `iceberg::Error` through `iceberg_err`. Also hosts the BUG-001 P0 valve
  (`MorDmlKind` + `refuse_mor_unpartitioned_multi_spec_dml`, hoisted from the v1 SQL crate in
  phase-2 PR-3b): refuse merge-on-read SQL DELETE/UPDATE when the current default spec is
  unpartitioned and multi-spec history exists — the fork position-delete fast-path under-delete
  hazard this file's stamping discipline exists to avoid. The SQL door resolves the target and
  calls it; the door's `bug001_*` battery pins it end to end. MERGE is never gated here.
- `idents.rs` — shared Spark/DF `quote_ident_spark` + path-escape needles + `probes` tables
  (single source; MERGE `quote_ident` delegates here). **FNP-4B (2026-09-15):** quoting
  emits backticks (embedded doubled). pins: fnp-4b/C-002
- `writer_props.rs` — Parquet `WriterProperties` from Iceberg
  `write.parquet.compression-codec` (+ optional level). Default **zstd** when absent (Java
  Iceberg 1.4+ parity); accepted `zstd|snappy|gzip|lz4|uncompressed`; unknown = loud error.
  Shared by append / MERGE data files / position deletes. **ICE-WRITER-METRICS-1 (2026-09-19):**
  `metrics_config_for` resolves the fork's `MetricsConfig::for_table`, and
  `name_matched_parquet_builder` returns the name-matched `ParquetWriterBuilder` already carrying
  it, so the MERGE executor asks for a configured builder instead of assembling one (that move
  is why `merge/mod.rs` shrank to 1,756 lines and its size exception ratcheted down).
  pins: ice-writer-metrics-1/C-001
  **RDF-1 (2026-09-02):** position deletes take a second builder,
  `position_delete_writer_properties_for`, which adds the fork's own
  `position_delete_writer_properties()` truncation setting
  (`set_statistics_truncate_length(None)`) to that codec. parquet-rs truncates statistics at 64
  bytes by default; a truncated statistic is not `min_is_exact`, and the fork's metrics
  aggregator drops an inexact bound — so every RePark-written position delete reached the
  manifest with NO `file_path` bound, was never file-scoped, and was invisible to
  `tooHighDeleteRatio`. The setting is read from the fork rather than restated, so a fork
  policy change carries. Registry `RDF-1`.
  pins: rdf-1-position-delete-bounds/C-002
- `write_options.rs` — **ICE-SESSION-WRITE-CONF-1 round 4 (2026-09-20):**
  `commit_overwrite_by_row_filter_with_summary` takes the plan's `StaticPartitionOverwrite`
  rather than its bare predicate, so `engine_summary_for_row_filter` can resolve the
  removal set from the same equalities the filter was built from.
  pins: ice-session-write-conf-1/C-054
  **Round 4, the collision lookup (2026-09-20):** `summary_with_extras` asks
  `refuse_collision` about the VERBATIM extra key — the spelling it is about to insert — not an
  ascii-lowered copy. Spark 4.1.2 measured (cells `QC-*`, 2026-09-20): a session
  `snapshot-property.Deleted-Records=5` on a CoW DELETE commits BOTH `Deleted-Records=5` and the
  engine's own `deleted-records=3`, because the producer's `ImmutableMap` is case-sensitive; only
  the exact spelling refuses. Folding stays the WRITER-OPTION rule, applied at
  `StatementWriteOptions::validate`, so an option still arrives here already lower-cased.
  pins: ice-session-write-conf-1/C-056
  **ICE-WRITER-METRICS-1 (2026-09-20):** every Parquet data-file builder applies
  `MetricsConfig::for_table` of the table it writes — `write_options.rs` (INSERT
  stage), `append_fanout_serial.rs` (fanout), `merge/mod.rs` (CoW rewrite),
  `merge/row_lineage.rs` (lineage rewrite) — and the position-delete builder applies
  `for_position_delete_table`. `position_delete_writer_properties_for` keeps RePark's
  `parse_compression` validation (the gzip-with-level refusal stays) and builds through
  the fork's `position_delete_writer_properties_for`, gaining the
  `delete-type=position` key/value; the unset-zstd default moves from level 1 to the
  fork's level 3 (both valid zstd, readers cannot tell). Pins in
  `merge/tests/writer_metrics.rs` (+ `writer_metrics_truth.json` fixture).
  pins: ice-writer-metrics-1/C-001
  pins: ice-writer-metrics-1/C-002
  pins: ice-writer-metrics-1/C-003
  pins: ice-writer-metrics-1/C-004
  pins: ice-writer-metrics-1/C-005
  **ICE-SESSION-WRITE-CONF-1 round 6, the merge with ICE-WRITER-METRICS-1 (2026-09-20):**
  the two rules compose in `writer_props::position_delete_writer_properties_for`. The
  fork's `position_delete_writer_properties_for` is asked first and its answer is the
  BASE — statistics-truncate length, the `delete-type=position` key/value and, when
  nothing delete-specific is configured, its compression (so the unset-zstd default
  stays the fork's level 3 that ICE-WRITER-METRICS-1 measured). `delete_compression_with`
  now answers `None` in exactly that case and `Some` whenever a delete-specific input
  exists — a writer option or session conf (`staging.codec` / `staging.level`) or
  `write.delete.parquet.compression-{codec,level}` — and only then does RePark's
  `parse_compression` decide, still falling back to `write.parquet.compression-*` for
  the half the caller left unset. So the round-1 order (writer option > session conf >
  delete property > data property) holds where the caller asked for it, and main's
  fork-derived defaults hold where it did not.
  pins: ice-session-write-conf-1/C-050
  pins: ice-writer-metrics-1/C-002
- `write_options.rs` — **ICE-WRITE-OPTIONS-1 (2026-09-17):** per-statement DataFrame
  write-option staging and commits. `WriterStagingOverrides` (codec/level/target-size,
  option over table property) feeds override-capable builders that mirror the
  `merge/mod.rs` unpartitioned and `append.rs` fanout constructions against the same
  fork actions; the four `*_with_summary` commits merge validated `snapshot-property.*`
  extras into the summary (empty extras are behaviour-identical to the `_to`
  canonicals, so the overwrite family commits through them unconditionally);
  `isolation_with_override` shares the table-property grammar. The mirror exists
  because the canonicals live in size-capped files the gate holds exact — a fork bump
  re-verifies both copies (see `writer_props.rs` duties). Only option-carrying
  statements reach the override staging; option-free staging keeps the canonicals.
  Fallible fns carry `#[allow(clippy::missing_errors_doc)]`, never `# Errors` sections
  (owner comment ban; round-2 purge 2026-09-17). Round 3 (2026-09-17): staging
  is stream-in with session concurrency (the override threads through the
  canonical concurrent fanout; canonical callers pass `none()`); gzip refuses
  on the merged level whatever side it came from (Q-20c-6); `summary_with_extras`
  drops user `operation`/`engine.operation-id` and refuses engine metric keys
  as Spark does (Q-20c-5). Round 4 (2026-09-17): the metric-key prefix sweep is
  gone; `summary_with_extras` takes the `EngineSummary` of the commit in hand
  (`summary_collision.rs`) and refuses only a key that summary contains (V-04).
  Round 5 (2026-09-18, Q-21c-7): the target-size override takes effect at the
  RP-23 fork granularity, 1000-row slices once the bytes reach the target, not
  per batch. The `writer_props.rs` units stage 3,500 rows: exactly 4 files at a
  1-byte option or table property, and 1 at the default or a 512 MB option over
  the property.
  pins: ice-write-options-1/C-008, C-012
  Run 22b rebase (2026-09-18): the unpartitioned mirror builder takes main's
  `distribution::stamp` like `merge/mod.rs` (C-017); `commit_replace_write_with_summary`
  is the options twin of main's RTAS `commit_replace_write` (overwrite by `AlwaysTrue`,
  empty allowed, collision rule against `EngineSummary::for_overwrite`, C-016);
  `stage_static_partition_overwrite_files_with` moves here from `partition_overwrite.rs`
  (which only exposes `static_injected_stream`), takes main's column list and an
  `Option` of the overrides, and hands `None` to the canonical untouched.
  IPI-05 (2026-09-21): `append_staged_with_options` stages a plain INSERT
  carrying only session snapshot properties under `wap.id` instead of
  committing on main; re-exported through `write/mod.rs`.
  pins: ice-write-options-1/C-014, C-016, C-017
- `summary_collision.rs` — **ICE-SESSION-WRITE-CONF-1 round 1 (2026-09-19):**
  `for_changes(table, added, removed, branch)` is the general shape — the fork's
  collector over BOTH sides plus the fork's own total arithmetic
  (`previous + added - removed`, dropped when it cannot resolve or goes negative) —
  and `for_append` is its add-only case. The MERGE / DML commit sites build it from
  the files they already hold, so the ONE rule (refuse an actual collision, stamp
  otherwise) is now shared by the writer option and the session conf on every
  owned commit site. `extras_need_removed_files` + `live_data_files` let a
  whole-table overwrite resolve its removal set when an extra names a
  removal-fed key, so the refusal names Spark's value instead of
  `<resolved at commit>`. The refusal is raised through
  `illegal_argument.rs`, so it reaches Python as `IllegalArgumentException`
  carrying Spark's message verbatim — the class Spark 4.1.2 raises
  (`QR-*`, and ICE-WRITE-OPTIONS-1's `COLL-*` cells record the same).
  pins: ice-session-write-conf-1/C-041
  **Round 5 (2026-09-20):** `live_files` is the one manifest walk and `live_delete_files` its
  delete-manifest case, so `row_filter_removed_files` resolves BOTH sides of what a static
  partition overwrite drops — the live data files whose partition tuple matches the PARTITION
  equalities AND the position/equality delete files in that same tuple. Spark's own producer
  counts them (`QD-MOR-*`), so a merge-on-read partition that already holds deletes refuses
  `removed-delete-files`, `removed-position-deletes` and `total-delete-files` instead of
  stamping the session extra beside the engine's own key.
  pins: ice-session-write-conf-1/C-059
  **Round 3 (2026-09-19):** `replaced_data_files` gives `replace_partitions` the same
  resolution the whole-table arm already had. `for_overwrite` marks every removal key
  `<resolved at commit>` the moment a previous snapshot exists, which is wrong for a
  dynamic overwrite: Spark 4.1.2 measured (cells `QP-*`, recorded 2026-09-19 run 25c)
  emits NO `deleted-records` when the overwrite lands only in partitions the table did
  not have, so a `snapshot-property.deleted-records` there is a free key that stamps and
  feeds the totals (`total-records` 3 + 1 - 5 is negative and is dropped, exactly as
  `QR-INSERT-DELETED-RECORDS` records). Where the overwrite DOES replace a live
  partition Spark refuses naming its computed value — `deleted-records=2`, not
  `<resolved at commit>`. `replaced_data_files` filters the live files to those whose
  partition value matches a staged file's, comparing by partition-field NAME so an
  evolved spec still matches, and the commit site falls back to `for_changes` over that
  set only when an extra names a removal-fed key. An empty dynamic overwrite still
  commits nothing, which is what Spark does.
  pins: ice-session-write-conf-1/C-047
  **Round 4 (2026-09-20):** `row_filter_removed_files` does the same for the STATIC arm.
  Spark 4.1.2 measured (cells `QO-*`, recorded 2026-09-20 run 25c) answers
  `INSERT OVERWRITE t PARTITION (cat = 'x')` the same way it answers the dynamic
  overwrite: overwriting the live partition refuses naming the engine value
  (`deleted-records=2`, `deleted-data-files=1`, `total-records=2`), while overwriting a
  never-written partition stamps `deleted-records=5` / `deleted-data-files=9` as free
  keys and only the totals collide (`total-records=4`). The removed set here cannot come
  from the staged files — an empty source stages nothing yet still clears the partition —
  so it is resolved from the PARTITION equalities: the live files whose partition value
  equals the equality literal, matched by partition-field name.
  `partition_overwrite.rs`'s `equality_literal` is the one place a `PartitionLiteral`
  becomes the `Literal` a data file carries, so the row filter and the removal set agree
  on type coercion.
  pins: ice-session-write-conf-1/C-054
- `illegal_argument.rs` — **ICE-SESSION-WRITE-CONF-1 round 1 (2026-09-19):**
  `IllegalArgumentMarker` and `illegal_argument_error`, moved here from
  repark-core's `error_map.rs` (which re-exports them unchanged) so a
  repark-iceberg refusal can carry the `IllegalArgumentException` class with an
  untouched message. **U7 PR1 round 2 (2026-09-24):** `NumberFormatMarker` and
  `number_format_error(input)` (`For input string: "<input>"`) are its leaf, classified to
  `Error::NumberFormat` and raised as `NumberFormatException`.
  pins: u7-write-df/C-011
- `unsupported.rs` — **ICE-VIEWS-1 R2 (2026-09-21):** `UnsupportedMarker` and
  `unsupported_error`, the `IllegalArgumentMarker` twin for the `External`
  classifier arm: a refusal that must reach Python as
  `UnsupportedOperationException` with byte-exact text (the A-9 viewless
  CREATE/REPLACE wording, no `NotImplemented` wrapper).
  pins: ice-views-1/C-005
  **PR-B hadoop naming (2026-09-24):** `unsupported_message_error` maps a fork
  `FeatureUnsupported` error to `unsupported_error(err.message())`, so the user sees the bare
  fork message and not `FeatureUnsupported => …`. Every other kind stays
  `DataFusionError::External`, which is what both doors' `iceberg_err` did. Both doors use it at
  their `RENAME TO` call site, where the fork's hadoop-naming MemoryCatalog refuses with
  "Cannot rename Hadoop tables" (Java `HadoopCatalog` 1.11.0).
- `unsupported_tests.rs` — **PR-B r3 (2026-09-24, critic V-004):** declared in `mod.rs` as
  `#[cfg(test)] mod unsupported_tests;`. `FeatureUnsupported` becomes an `External`
  `UnsupportedMarker` whose Display is the bare message and that is not an `iceberg::Error`.
  `Unexpected`, `TableNotFound` and `TableAlreadyExists` stay an `External` `iceberg::Error`
  with kind and message kept, never an `UnsupportedMarker`. Mutation: map every error to
  `unsupported_error(error.to_string())` and both pins go red.
- `summary_collision.rs` — **ICE-WRITE-OPTIONS-1 round 4 (2026-09-17):**
  `EngineSummary`, the snapshot-summary keys the engine computes for the commit
  in hand, which a user `snapshot-property.<k>` may not collide with (Spark's
  measured rule: refuse iff the engine computed that exact key, message
  `Multiple entries with same key: <k>=<engine> and <k>=<user>`). The fork merges
  extras inside its own `SnapshotProducer::summary`, where a user value would
  silently win, so the map is rebuilt ahead of the commit: `for_append`
  runs the fork's public `SnapshotSummaryCollector` over the staged files and
  applies the fork's add-only total arithmetic to the branch head (exact
  values); `for_overwrite` keeps the exact added side and marks totals,
  `changed-partition-count` and — with a parent — the data-removal keys as
  `<resolved at commit>`, since the fork resolves the removal set inside the
  commit. `engine-name` / `engine-version` are reserved (Spark stamps both on
  every write; RePark writes neither): they refuse with `<engine-reserved>` as
  the engine half (R-21c-1).
  pins: ice-write-options-1/C-008
  pins: ice-write-options-1/C-001, C-002, C-003

## I want to...

| ...do this | go to |
|---|---|
| ALTER TABLE properties / rename / schema / partition evolution | `alter.rs` |
| Bulk-append batches through the sanctioned commit path | `append.rs` (`append`) |
| Stream a SELECT into a staged (CTAS) write with bounded memory | `write_data_files_from_stream` (`merge/mod.rs`) / `write_partitioned_data_files_from_stream` (`append.rs`) |
| Stage + commit full-table INSERT OVERWRITE | `overwrite.rs` |
| Stage + commit partition-scoped INSERT OVERWRITE | `partition_overwrite.rs` |
| Decide the overwrite scope (whole table / row filter / replace partitions) | `overwrite_scope.rs` |
| Stage static-overwrite batches with statement levers | `partition_overwrite.rs` (`stage_static_partition_overwrite_files_with`, ICE-WRITE-OPTIONS-1) |
| Cap concurrent Iceberg file writers (session conf) | `repark.write.max-concurrent-files` via `concurrency.rs` |
| Send one partition value to one writer before a CTAS write (Spark's `hash` distribution) | `distribution.rs` (`hash_distribution`) |
| Parquet compression codec (table property) | `writer_props.rs` |
| Stage/commit an option-carrying write (summary extras + overrides) | `write_options.rs` |
| Parquet statistics properties for a position-delete file | `writer_props.rs` (`position_delete_writer_properties_for`) |
| Change MERGE INTO semantics | [merge/map.md](merge/map.md) |
| Identity DELETE/UPDATE (subquery `WHERE` and RP-9 r2 plain `WHERE`) | `predicate_dml.rs` (`execute_predicate_dml`) |
| Change which concurrent commits a MERGE / UPDATE / DELETE conflicts with | `conflict_filter.rs` + [merge/map.md](merge/map.md) `snapshot_commit.rs` |
| Wire ordinary DELETE/UPDATE/INSERT OVERWRITE | DataFusion → fork `TableProvider` (non-subquery) |
| Ask whether a `(source, target)` type pair may be written | `store_assign.rs` (`ansi_store_assignable`) |
| CREATE/DROP BRANCH or TAG | `snapshot_refs.rs` |

## Pointers

- Up: [../map.md](../map.md)
- Fork contract: `docs/ENGINE_CONTRACT.md` in the owned fork.

## Debug

| Symptom | First check |
|---|---|
| SET/UNSET TBLPROPERTIES not landing | the action must be `.apply(tx)`'d and `tx.commit(catalog)` awaited; empty-action commit no-ops |
| `append` rows land in one partition | fanout must pass EACH split group's own `PartitionKey` to `FanoutWriter::write`; inspect `DataFile.partition` in committed manifests |
| UNSET errors "present in both removal and update set" | a key was both set and removed in one action — the router only passes disjoint keys |
| Streaming CTAS OOMs / collects the whole SELECT | must use the `_from_stream` writers over `execute_stream()`, never `collect()` |
| A partitioned CTAS writes writers × values data files | `hash_distribution` must wrap the input when the spec is partitioned and `writers > 1`; check `IcebergPartitionWriteExec`'s child is a `RepartitionExec` with `Partitioning::Hash` |
| Parallel write left partial files after a failed MERGE | abort flag must skip `finish()`/`close()` |
| A DML aborts on a concurrent write to another partition | the commit's conflict filter printed `TRUE`: see `conflict_filter.rs`'s widening rules and [merge/map.md](merge/map.md) Debug |
| Rejected MERGE OCC commit left new Parquet files in the warehouse | commit-error abort must `FileIO::delete` writer-result paths only (`merge/abort.rs`); never re-derive from manifests; never delete `affected` |
| MERGE OOMs on a large target | target must register as a `StreamingTable` (`(_file, _pos)` identity), never a full-target `MemTable` |
| MERGE produces duplicates | multiple-source-match must **error** (like Spark); serializable (default) commit arms carry `validate_no_conflicting_data`; snapshot isolation drops it (`write.merge.isolation-level`) |
| Conflict-retry corrupts data | on commit conflicts re-read the target; don't cache stale file lists |
| MoR MERGE on a spec-evolved unpartitioned table loud-fails `Partition value is not compatible` | position-delete writer must `.with_partition_spec` the resolved unpartitioned spec when it is not spec 0; `partition_key` stays `None` |

First checks: `cargo test -p repark-iceberg write::` (all on `MemoryCatalog`). Escalate to:
[../../map.md#debug](../../map.md).

- **EC-9 scrub (2026-08-08, phase-3 PR-5):** pre-existing private fixture/doc literals
  (a team/bucket name fragment) replaced with `example-team` equivalents — outcome-neutral
  (fixtures and their oracles changed together); enumerated in docs/history/port-v2/p3e-facade-ledger.md.

- **Neutral-fixture scrub (2026-08-10, hardening-prep):** an owner-approved, forward-only,
  comment-and-fixture-only pass moved this directory's doc text and example literals to
  neutral placeholders — the upstream job the acceptance shape mirrors is named generically
  ("the source publish job"), and example table/view/entity names are placeholders carrying no
  domain vocabulary. Outcome-neutral: every renamed fixture moved together with the assertions
  that read it. Sites here: `append.rs` — the example table literal in
  `append_a1_acceptance_identity_partitioned_end_to_end`, now `"t"` like every other
  `create_table` call in the file.
- **FNP-4B remediation (2026-09-15):** `idents.rs` keeps the backtick `quote_ident_spark`; the unit's added code comments were removed under the 2026-08-26 ruling (facts stay in this map).

## IPI-19 + IPI-37 (2026-09-20) — schema evolution on write

- `schema_evolution.rs` — the one seam every schema-evolving write goes
  through. `ACCEPT_ANY_SCHEMA_PROP` / `accepts_any_schema` read Spark's table
  property `write.spark.accept-any-schema`; `incoming_schema` turns the
  incoming Arrow schema into an Iceberg one with auto-assigned ids;
  `evolve_schema` commits the fork's `UpdateSchemaAction::union_by_name_with`
  (Java `unionByNameWith`) **case-insensitively** — Spark's default resolution —
  and returns the table at the result.

  The merge rule is the fork's, not a second one written here: a new column is
  added **last and optional**, keeping the source's own type; a column missing
  from the source stays (union, not replace); a narrowing incoming type is
  ignored and a widening one promotes. Re-deriving any of that in RePark would
  diverge on the first nested or promotion case.

  **Why the schema commits before the data, not with it.** The fork's
  `TransactionAction` trait is `pub(crate)`, so an external engine cannot ask an
  `UpdateSchemaAction` what schema it *would* produce without committing it —
  and the data files must carry the evolved schema's field ids before they are
  written. The schema update therefore commits first and the write runs against
  the table it returns. That costs nothing observable: a schema update emits
  only `AddSchema` + `SetCurrentSchema`, never `AddSnapshot`, so the data commit
  is still the only new snapshot (pinned in
  [`schema_evolution/`](schema_evolution/map.md)), and it is what Spark itself
  does — its `MERGE WITH SCHEMA EVOLUTION` runs the schema changes as commands
  before the merge. The residue is atomicity: a failure between the two leaves a
  widened schema and no rows, which is Spark's behaviour too.
  pins: ipi-19-56-37-schema-evolution-write/C-001, C-003, C-005

## U6 WRITE-REFUSALS (2026-09-24) — MERGE type changes

- `schema_evolution.rs` — `evolve_merge_schema` is the MERGE seam. For each
  source column whose primitive type differs from the matched table column, it
  issues the fork's `update_column(<table name>, <source type>)`, then the union.
  This is what Spark's `MERGE WITH SCHEMA EVOLUTION` asks Iceberg to do, so the
  fork's promotion rule answers: `int → long` widens, `long → int` refuses.
  `type_change_err` maps the fork's `DataInvalid` `Cannot change column type: …`
  to `IllegalArgumentMarker`, on apply and on commit, for both seams.
  pins: u6-write-refusals/C-006, C-007

## U8 WRITE-SQL PR1 (2026-09-24) — overwrite by a Spark filter

- `overwrite_filter.rs` — Spark's `OverwriteByFilter` for `INSERT INTO … REPLACE WHERE`.
  `spark_overwrite_filter` converts a sqlparser predicate to an Iceberg row filter by Spark's
  optimizer and V2-filter rules (`=`, `<>` as `NOT (=)`, `<=>`, ranges, `BETWEEN`, `IN`,
  `NOT IN`, `IS [NOT] NULL`, a wildcard-free or prefix `LIKE`, `AND`/`OR`/`NOT`). **Round 3
  (2026-09-25, critic r2):** `lower` builds Spark's optimized predicate as a `Folded` tree
  (convertible leaves with their Spark rendering, opaque leaves, `null`, `(c IS NULL) AND
  (null)`, `(c IS NOT NULL) OR (null)`, constants simplified through `AND`/`OR`), and
  `spark_overwrite_filter` splits it into conjuncts and refuses on the first one it cannot
  convert, rendering that conjunct as Spark does. `NOT` pushes down as Spark's optimizer
  pushes it; `<` and `<=` carry a `notNull` conjunct because Java Iceberg's `lt` never
  matches a NULL. An `IN` list is deduplicated with its NULL counted, and a one-element list
  folds to `=` / `<>`. A literal an integer column cannot hold follows Spark's unwrap-cast: a
  BIGINT-typed literal on INT folds `=` / range comparisons to `(c IS NULL) AND (null)` or
  `(c IS NOT NULL) OR (null)`; a literal beyond i64 folds range comparisons to TRUE / FALSE;
  a fractional literal selects the rows of `<= floor` / `>= ceil` and renders in a refusal as
  Spark's unwrap-cast renders it (`<` → `< ceil`, `<=` → `<= floor`, `>` → `> floor`, `>=` →
  `>= ceil`; round 5, verifier V-001); `<=>` against any of them is FALSE; an `IN` drops them. A string literal is coerced to the column
  type. A column reference binds by its exact name; another case refuses Iceberg's
  `Cannot find field '<name>' in struct: …`. pins: u8-write-sql/C-015, C-017, C-019, C-020
  **Round 4 (2026-09-25, critic r3):** `number_literal` types a decimal literal on an INT
  column as Spark does: outside the INT range but inside i64 it is `OutOfRange` with the INT
  folds (never a constant), and a `.0` literal exactly at the INT maximum or minimum is a
  `Boundary`, which `boundary` folds by Spark's rules (`> max` → `null`, `<= max` →
  `(c IS NOT NULL) OR (null)`, `< max` → `NOT (c = max)`, `>= max` → `c = max`, mirrored at the
  minimum). pins: u8-write-sql/C-022
  `commit_overwrite_by_filter_with_summary` commits `overwrite_files().overwrite_by_row_filter`
  with the staged files and no added-file validation (Spark adds none), with the isolation,
  `validate_from_snapshot` and branch handling of its siblings in `write_options.rs` (U7 PR2,
  2026-09-24: the level and the start come from `commit_target::FilterValidation`, so a
  `validate-from-snapshot-id` writer option beside an `isolation-level` starts the
  validation there; pins: u7-write-df-2/C-009). A set
  row filter already counts as a change in the fork, so an empty source commits `delete`
  without `allow_empty_commit`. A snapshot property that names an engine summary field
  refuses, because the removed-file set of a general filter is not computed.
  Tests: [overwrite_filter/](overwrite_filter/map.md).
  U7's `writeTo(t).overwrite(condition)` reuses these two entry points (the claims line is in
  `task/ledgers/staging/u8-write-sql-ledger.md`).
  pins: u8-write-sql/C-013
- `mod.rs` — declares `overwrite_filter` and re-exports its two entry points.
