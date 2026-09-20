# map — repark-iceberg/src/tests

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

Crate-root test modules. `lib.rs` declares `#[cfg(test)] mod tests;`.

## Contents

- `mod.rs` — thin index (rustfmt module order).
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
  pins: ice-merge-append-1/C-002, C-003, C-004, C-005, C-008
- `tracing.rs` — shared tracing harness: one global subscriber, both capture layers
  (forced-edit class 6). Accessors used by `catalog/tests/catalog.rs` and
  `write/merge/tests/streaming_scan.rs`.
- `fork_pin.rs` — ADR-0001 fork-pin proof: names and exercises fork-only public API
  (`iceberg::plan_commit_base_load` / `CommitBaseLoadPlan`).
- `v3_types.rs` — **V3-6 C-001:** fork pin `00cdde0` (RP-5) read/write measurement for
  `timestamp_ns` / `timestamptz_ns` (parquet round-trip), `unknown` (Arrow Null;
  parquet write refuses `Writing the unknown column 'u' is not supported yet` —
  `fork_unknown_write_refuses_naming_the_column`; RP-5 C-006 / R91 `#246`), and binary `variant` (parquet builder
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
