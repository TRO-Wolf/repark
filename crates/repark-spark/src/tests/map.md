# map — repark-spark/src/tests

## Purpose

Lib-root unit battery for the Spark SQL door (the former `src/tests.rs` monolith, split in
G-4 as a **declared-rename** unit under `docs/testing.md` "Relocation discipline"). Production
code is not here — only tests, shared fixtures, and the module manifest.

## Contents

- `mod.rs` — pure module manifest (`mod common;` + one `mod` per leaf).
- `common.rs` — shared fixtures (`setup`, `rows`, `run`, `register_source`, `table_rows`, …)
  and the cross-cutting helpers that more than one leaf needs (`time_travel_id_multiset`,
  `execute_without_collecting`, unsafe-cast walk helpers). Bodies moved byte-identically from
  the monolith; `pub(super)` visibility + type re-exports for leaf `use super::common::*;`.
- **Production-aligned leaves** (flat tests gained one path segment `tests::<leaf>::…`):
  `ctas`, `create_table`, `namespace_ddl`, `catalog_ops`, `describe_show`, `alter`, `dml`
  (DELETE/UPDATE + BUG-001 valve; no production `delete`/`update` module), `insert_overwrite`,
  `merge`, `call`, `ref_ddl`, `time_travel`, `metadata_tables`, `normalize`, `local_fs_ddl`,
  `router` (multi-statement, F-BR-2 eager DML, TRUNCATE refuse), `decimal` (G-7b bit-exact
  `Decimal128` i128 pins — literal / division / 38-clamp / avg+promotion / overflow+div-zero /
  nullability; cites Python corpus row names, no Python edits).
- **Path-preserving sibling lifts** (former nested `mod`s; cargo paths unchanged):
  `partitioned_ctas`, `partitioned_merge`, `transform_overwrite` (still nests
  `provider_partition_correctness`), `service_managed_ctas`.
- **Added after the split** — `time_travel.rs` gained the three H-1b ephemeral-view pins
  (2026-08-11), `time_travel_temp_views_do_not_survive_a_{successful,failed}_statement` and
  `time_travel_statement_pins_never_collide_with_a_reader_options_view` (the fix-pass collision
  pin: a reader-options registration made through `repark_core::read_table_at` must SURVIVE a
  Spark-door `VERSION AS OF` statement, and the door must mint from repark-core's counter — the
  second assertion is the one that reds whatever the numbers happen to be), beside the twin
  `time_travel_version_timestamp_branch_tag_and_errors`, plus their three leaf-private helpers
  (`leftover_time_travel_views`, `setup_time_travel_leak_table`, `temp_view_sequence` — used by
  that leaf only, so they stay out of `common.rs`). They read the default catalog/schema directly
  rather than `information_schema`, which this door's `setup` does not enable. Not a relocation:
  new tests, new names, and the G-4 identity artifacts are unaffected.
- **N-2b / G3 deferred MERGE pins (2026-08-11)** — `merge.rs` gains four Spark-door SQL pins
  that mirror the N-2 Python differential corpus shapes G-4's file ban deferred:
  `merge_duplicate_source_keys_with_matched_raises`,
  `merge_duplicate_source_keys_insert_only_commits_both`,
  `merge_matched_and_arm_order_update_then_delete`,
  `merge_matched_and_threshold_update_or_delete`, plus the leaf-private `score_table_rows`
  helper for the two score-arm pins. Not a relocation: new tests, new names.

## Mapping rule

1. Production-module alignment by name / primary assertion.
2. Two-module tests follow the primary assertion; genuine cross-cutting residue gets a small
   shared home (`dml`, `common`).
3. Nested mods lift to sibling files of the same name (path-preserving).
4. Helpers move byte-identically; only new lines are `mod` decls and `use` adjustments.

Authoritative membership + margin rulings: unit ledger `docs/history/hardening-h1/g4-tests-split-ledger.md` and
planning cut map `G4-CUT-MAP.md`. Generated name map: `docs/history/hardening-h1/g4-artifacts/name-map.md`.

## Pointers

- Up: [../map.md](../map.md)
- Registry pins: [../../../../docs/spark-sql-iceberg-parity.md](../../../../docs/spark-sql-iceberg-parity.md)
- Surface matrix (also `#[cfg(test)]`): [../matrix.rs](../matrix.rs)

## Debug

| Symptom | First check |
|---|---|
| Identity / rename doubt | `cargo test -p repark-spark --lib -- --list` vs `docs/history/hardening-h1/g4-artifacts/`; leaf multiset must match; full paths follow the name map |
| Matrix pin string red / stale | `matrix.rs` strings are `--list` names; update with renames only — never re-point a surface |
| Registry pin path stale | filesystem form `crates/repark-spark/src/tests/<file>.rs::leaf` |
| Missing `TempDir` / Arrow types in a leaf | `use super::common::*;` (shared re-exports live in `common.rs`) |
| Cross-leaf helper not found | should be `pub(super)` in `common.rs` (or wrongly left private in one leaf) |
| Nested partition pin fails | `partitioned_ctas` / `partitioned_merge` / `transform_overwrite` — manifest-level `DataFile.partition` oracles |

First checks: `cargo test -p repark-spark tests::<module>::`. Escalate to: [../map.md#debug](../map.md).
