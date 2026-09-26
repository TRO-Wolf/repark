# map — repark-iceberg/src/tests

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

Crate-root test modules. `lib.rs` declares `#[cfg(test)] mod tests;`.

## Contents

- `mod.rs` — thin index (rustfmt module order).
- `session_write_conf.rs` — **ICE-SESSION-WRITE-CONF-1 round 8 (2026-09-20):** the two
  staging literals take `..WriterStagingOverrides::none()`, so the struct's new
  `fork_insert_dictionary_rule` field defaults there instead of being restated.
  pins: ice-session-write-conf-1/C-064
- `session_write_conf.rs` — **ICE-SESSION-WRITE-CONF-1 (2026-09-19):** the
  resolver precedence pins (writer option over session conf over table property, bogus
  codec refuses naming the codec) plus the `SessionWriteView` carrier shape
  (comment-free per the owner ban).
- `overwrite_scope.rs` — **ICE-OVERWRITE-MODE-1 (2026-09-19):** the decision table (session
  mode × intent × `overwrite-mode` option × static values → scope), the option's
  case-insensitive `dynamic` reading, `plan_overwrite` on a two-level identity table (mixed
  list: row filter `cat = "x"` in static mode, replace partitions in dynamic mode; both keys
  dynamic: whole table / replace), and `NON_PARTITION_COLUMN` on an unpartitioned table and
  for a non-partition column. pins: ice-overwrite-mode-1/C-002, C-004, C-005, C-006, C-007
  Round 2 (2026-09-19): a key naming a `bucket` source (static, dynamic, by spec field name,
  or with a typed literal) refuses `NON_PARTITION_COLUMN` in both modes, while a typed literal
  on an identity key keeps its literal refusal; a static `'2024-01-01'` on a `DATE` partition
  plans the row filter `d = 2024-01-01` and `'2024-13-45'` refuses with the Arrow cast text;
  an empty dynamic stage skips the commit (`replace_partitions_is_noop`).
  pins: ice-overwrite-mode-1/C-011, C-013, C-014
- `v1_ref_writes.rs` — **WO RP50-A (2026-09-26):** the v1 ref-commit pin for C-060 (it
  replaces the round-2 C-053 kernel pin). On a format v1 table, `create_branch_on_empty_table`
  commits `b0` with no `main`, `commit_append_to(…, Some("main"))` seeds main,
  `create_snapshot_ref` commits branch `b1` and tag `t1` at the seed, and
  `commit_append_to(…, Some("b1"))` moves only the branch; a reload holds every ref on
  format-version 1. Red-first: the pin failed on the unfixed tree with the removed guard's text.
  pins: ice-nested-evo-1/C-060
- `merge_append_series.rs` — **ICE-MERGE-APPEND-1 (2026-09-19):** replays the recorded Spark
  4.1.2 / Iceberg 1.11.0 manifest series (`../../../../python/repark/tests/ice_merge_append_1_truth.json`)
  through `write::commit_append` — 120 sequential single-file appends per variant, probed at
  1, 5, 20, 50, 99, 100, 101, 110, 120, asserting the manifest count and the data-file count.
  `defaults` collapses 99 manifests into 1 at the hundredth append,
  `commit.manifest.min-count-to-merge=5` holds the table between 1 and 4 manifests, and
  `commit.manifest-merge.enabled=false` keeps one manifest per append. Also the regression
  battery a merging commit could break: a two-spec table (Java never merges across spec ids),
  a MoR table whose delete manifests must carry forward, row lineage and sequence numbers
  across a merge, and a branch-targeted merging append.
  Mutations M1/M2/M3 each red their named subset of these pins and nothing else (ledger §3).
  All seven pins green after the routing change; the branch leg merges at the hundredth
  manifest on the branch (the carried seed plus 99 branch appends), not the hundredth branch
  append, and main's pointer never moves.
  pins: ice-merge-append-1/C-002, C-003, C-004, C-005, C-008
- `output_spec.rs` — **U7 PR1 (2026-09-24):** `output-spec-id` pins over a memory catalog
  table evolved with `ADD PARTITION FIELD cat`: Java int parsing and its refusal text, the
  staging view's swapped default spec (the real table untouched), Spark's unknown-id text for
  `7` and `-1`, an append with spec 0 committing only spec-0 files, and the near miss without
  the option committing two spec-1 files. Round 2 (2026-09-24): the refusal is a
  `NumberFormatMarker`, the partitioned stager follows an unpartitioned output spec (one
  spec-0 file), and `staged_spec_is_partitioned` answers per id and refuses `7`.
  pins: u7-write-df/C-014, C-016
- `writer_partitioning.rs` — **U7 PR1 (2026-09-24):** the layout and save-target kernels:
  provided and table transform rendering (quoting, `sorted_bucket`, `days`/`truncate`), the
  mismatch text with an empty and a three-field table side, count and case mismatches, a
  matching catalog table passing, and `decide_save_target` across every mode, existence,
  path and default-format cell. Round 2: the path relation for URI, trailing-slash,
  doubled-slash, `s3://`, relative and one-segment paths, and `save_target_names_table`.
  Round 3: a void field dropped from the table side and `years`/`months`/`hours` named, as
  Spark measured them (`bucket_existing_void_append`, `bucket_existing_time_append`).
  pins: u7-write-df/C-005, C-006, C-014
- `replace_schema.rs` — **U7 PR2 slice-1 round 2 (2026-09-25):** `replacement_schema` keeps
  ids by name over reordered, renamed and added columns (fresh ids above `last-column-id`),
  keeps a type-changed column's id, does not reuse a dropped id for a new name, and keeps
  nested struct ids by dotted name; round 3 (2026-09-25): after a column drop, where
  `last-column-id` exceeds the current highest id, a new name takes `last-column-id + 1`.
  pins: u7-write-df-2/C-011
- `writer_plan.rs` — **U7 PR1 round 2 (2026-09-24):** `plan_writer` pins: the `saveAsTable`
  statement for every mode and existence with and without buckets, Spark's already-exists
  text, the missing bucket column on every create-or-replace arm (and not on an existing
  append), a case-sensitive session keeping `ID`, the `_LEGACY_ERROR_TEMP_3060` text (round 3:
  the name backticked when it contains a `.`, and only then), a missing `sortBy` column after
  the bucket columns (round 4), and the `save()` statements with their layout check.
  pins: u7-write-df/C-015, C-018
  U7 PR2 (2026-09-24): a `saveAsTable` overwrite plans `rtas` whether or not the table
  exists or is bucketed. pins: u7-write-df-2/C-002
- `filter_validation.rs` — **U7 PR2 (2026-09-24):** `FilterValidation` through a real
  overwrite-by-filter commit on a memory catalog seeded with one file per row: an explicit
  `serializable` or `snapshot` level validates from the requested snapshot (a later delete of
  a matching file refuses, a later snapshot passes), no level ignores the request, an explicit
  level without a start validates from the loaded snapshot (residue R-6), `serializable`
  refuses a matching append and `snapshot` does not, a change to other rows never conflicts,
  and a start that is not a long or not an ancestor refuses with Java's text.
  pins: u7-write-df-2/C-009
- `tracing.rs` — shared tracing harness: one global subscriber, both capture layers
  (forced-edit class 6). Accessors used by `catalog/tests/catalog.rs` and
  `write/merge/tests/streaming_scan.rs`.
- `fork_pin.rs` — ADR-0001 fork-pin proof: names and exercises fork-only public API
  (`iceberg::plan_commit_base_load` / `CommitBaseLoadPlan`).
- `v3_types.rs` — **V3-6 C-001:** fork pin `00cdde0` (RP-5) read/write measurement for
  `timestamp_ns` / `timestamptz_ns` (parquet round-trip), `unknown` (Arrow Null;
  since RP-51 the parquet write omits the column and the scan reads NULL —
  `fork_unknown_write_omits_the_column_and_reads_null`; RP-5 C-006 / R91 `#246` recorded
  the refusal the fork `#356` retired), and binary `variant` (parquet builder
  refuse). **C-002:** `fork_variant_scan_refuses_naming_the_type` — a real data file plus a
  variant projection refuses at the fork's reader guard (empty table streams cleanly);
  the §4 registry row `V3-VARIANT-SHRED-1` cites these pins and the STATUS v3 block
  truth-up rides the same landings (pins: v3-6-v3-types/C-007).
  pins: rp-5-fork-repin/C-006
  **C-005 (2026-09-01):** `write_default` fills an omitted column on append
  (red-first vs the old refuse pin), a supplied column is kept, and `initial_default`
  reads into files missing the column. pins: v3-6-v3-types/C-001, C-002, C-005

## Pointers

- Up: [../map.md](../map.md)
