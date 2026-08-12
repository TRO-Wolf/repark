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
  nullability; cites Python corpus row names, no Python edits), `float_agg` (G7 float
  aggregation determinism — catastrophic-cancellation fixture; `sum`/`avg` `f64::to_bits` at
  `target_partitions` 1/2/8; per-count stability + cross-count spread disclosure).
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
- **G5b temporal-`RANGE` pins (2026-08-11)** — `window_temporal_range.rs`, a NEW leaf (not a
  relocation): five tests over the Spark door's `RANGE` frames on datetime order keys, holding
  the two arms of `../window_range.rs` plus the paths it must NOT disturb —
  `temporal_range_bare_offset_over_timestamp_key_refuses_like_spark` (Spark's error class),
  `temporal_range_bare_offset_over_date_key_means_days` (one day vs thirty days, so a
  one-month reading cannot pass), `temporal_range_interval_bounds_still_match_spark` (asc /
  desc / ties / HOUR-not-DAY), `temporal_range_null_order_keys_match_spark`, and
  `temporal_range_numeric_order_keys_are_untouched` (scope, incl. the mixed-statement
  fallback). Goldens are the live PySpark 4.1.2 halves recorded in the unit's section-0 recon,
  the same oracle the `temporal_range` family in
  `python/repark/tests/test_window_parity.py` pins — one oracle, two halves. Leaf-private
  fixtures (`register_timestamp_seed`, `register_date_seed`, `seed_micros`,
  `days_from_civil`) stay out of `common.rs`: only this leaf uses them. Ledger:
  [`../../../../task/g5b-temporal-range-ledger.md`](../../../../task/g5b-temporal-range-ledger.md).
- **N-2b / G3 deferred MERGE pins (2026-08-11)** — `merge.rs` gains four Spark-door SQL pins
  that mirror the N-2 Python differential corpus shapes G-4's file ban deferred:
  `merge_duplicate_source_keys_with_matched_raises`,
  `merge_duplicate_source_keys_insert_only_commits_both`,
  `merge_matched_and_arm_order_update_then_delete`,
  `merge_matched_and_threshold_update_or_delete`, plus the leaf-private `score_table_rows`
  helper for the two score-arm pins. Not a relocation: new tests, new names.
- **Added after the split** — `dml.rs` gained **10** `g3e8_*` subquery-predicate valve pins
  (2026-08-11): the refuse family for both verbs, the adjacent negatives that prove the valve did
  not widen (non-subquery DML, `INSERT … SELECT` with a subquery, MERGE over a subquery source,
  `UPDATE … SET col = (SELECT …)` with and without a `WHERE`), the guard-ORDER pin against the
  BUG-001 valve, the **FROM-less** `DELETE <table> WHERE …` family + its negative (the panel's
  live bypass — that spelling fails the router's Databricks parse and reaches the executor
  through the passthrough's own parse), and the CTE-prefixed `WITH … DELETE` loud-today pin.
  `normalize.rs` gained **2**: the detector unit pin
  (`g3e8_subquery_detector_fires_on_every_spelling_and_no_other`) and the statement-level valve
  pin (`g3e8_statement_valve_covers_both_verbs_and_renders_the_parsed_target`, the entry point
  `spark_ast::execute_passthrough` calls) — production-module alignment: both live in
  `../normalize.rs`. Leaf-private helpers (`g3e8_setup`, `g3e8_seed`, `assert_g3e8_message`) stay
  in `dml.rs`; only that leaf uses them. Counts here are the file's real `#[tokio::test]` /
  `#[test]` totals — re-derive with
  `grep -cE '^(async )?fn g3e8_' crates/repark-spark/src/tests/{dml,normalize}.rs` minus the two
  helper fns in `dml.rs`. See `task/g3e8-guard-ledger.md`.

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
