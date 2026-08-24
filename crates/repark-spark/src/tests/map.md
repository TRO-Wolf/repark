# map — repark-spark/src/tests

## Purpose

Lib-root unit battery for the Spark SQL door (the former `src/tests.rs` monolith, split in
G-4 as a **declared-rename** unit under `docs/testing.md` "Relocation discipline"). Production
code is not here — only tests, shared fixtures, and the module manifest.

## Contents

- `mod.rs` — pure module manifest (`mod common;` + one `mod` per leaf).
- `common.rs` — shared fixtures (`setup`, `rows`, `run`, `register_source`, `table_rows`, …)
  and the cross-cutting helpers that more than one leaf needs (`time_travel_id_multiset`,
  `execute_without_collecting`, unsafe-cast walk helpers). **V3-2:**
  `setup_allow_create_format_version_3` (`Model: Grok 4.6 xHigh`). **U2:** `setup` /
  `setup_allow_local_fs_ddl` / `setup_strict_catalog` call
  `crate::extension::apply_spark_float_as_decimal` so Spark-door unit fixtures match
  production `configure`. **R-2:** those fixtures plus `setup_with_ansi` also call
  `register_spark_decimal_planner` (`extension.rs` is closed). Bodies moved
  byte-identically from
  the monolith; `pub(super)` visibility + type re-exports for leaf `use super::common::*;`.
- **TZ-4 PR-1 (2026-08-13)** — `create_table.rs` pin `ts TIMESTAMP` → `Timestamptz`;
  `ctas_of_instant_producers_stores_timestamptz` (SQL `current_timestamp` / `to_timestamp(Z)`
  / identity-partitioned CTAS).
- **Production-aligned leaves** (flat tests gained one path segment `tests::<leaf>::…`):
  `ctas` (**A13:** `register_memory_catalog` location-less CTAS lands under the warehouse, not
  `<temp>/repark_ctas`), `create_table`, `namespace_ddl` (R-6 / G-6 Q1: `IF NOT EXISTS` create-new /
  same / conflicting / no-location twins; the old silent-adopt fixture now matches
  location), `catalog_ops`, `describe_show`, `alter`, `dml`
  (DELETE/UPDATE + BUG-001 valve; no production `delete`/`update` module), `insert_overwrite`,
  `merge`, `call` / `call_orphan` (**A13:**
  `call_remove_orphan_files_refuses_a_location_arg_under_the_fallback_root` is the
  execute-path CALL `location` refuse in `call_orphan.rs`;
  `call_orphan_shared_ctas_root_rule` is the helper table). **MW-1:** the LOCAL-only fence is gone — both remote catalog policies
  execute, an unknown catalog still refuses, and expire pins Spark's six-column result from
  the fork's typed `CleanupReport` views (RP-1 / F-2); the split pin strands two
  MERGE deletes plus two post-MERGE appends so data≠position; it strands its
  position deletes by ROLLBACK, because compaction keeps them until MW-2. **MW-2:**
  `rewrite_position_delete_files` is wired and pinned against a live Spark 4.0.1 oracle —
  8 delete files compact to 1 with the row set unchanged, nothing-to-do returns four zeros,
  and `rewrite_data_files` grew Spark's fifth column. **RP-1** flipped `call_mor1_…` to
  equality at floor 5 (row retired); `call_rpdf_compacts_at_sparks_min_input_files_floor` pins
  the exact floor. `call_mor2_…` still holds the partition-granularity
  writer, which is what makes the parity pin's comparison legitimate.
  The deletion-vector guard is pinned as a rule table plus both no-false-positive paths; the
  vector-present path is the Spark-written fixture under `fixtures/v3-spark-mor/`, adopted by
  `call_register` / V3-1 — the MW-2 rule-table pin's rustdoc says so too),
  `call_v3` (**V3-0**, split from `call` on subject the way `call_orphan` was: every test is about
  one table property rather than one procedure. Holds the `rewrite_data_files` row-lineage
  refusal, its v2 control, and a fixture assertion — the fixture is built by upgrading an
  engine-created table through the fork's own `Transaction::upgrade_table_version` so tests that
  must not depend on V3-2 CREATE still run. **V3-2** adds
  `opt_in_create_produces_v3_and_rewrite_still_refuses` (engine CREATE with the session
  opt-in is V3 and still hits V3-LINEAGE-1; `Model: Grok 4.6 xHigh`). A fourth pin holds the guard's **default-session**
  blast-radius claim — all four doors to a v3 table refuse without the opt-in — and it lives here
  rather than with the CREATE tests because the claim is what makes a refusal stricter than Spark
  defensible; its `ALTER` half is an UPSTREAM behaviour, so the pin doubles as the detector for
  the fork changing it),
  `call_manifests` (**MW-6**: `CALL system.rewrite_manifests` — Spark's two non-nullable `int`
  columns; five data manifests → one with the row set unchanged; the no-op answers two zeros and
  commits NO snapshot; a table with no snapshot answers zeros where the fork action errors; the
  current-spec filter is pinned through `ALTER TABLE … ADD PARTITION FIELD`, which is the only
  fixture that makes that branch live; and both sides of the delete-manifest divergence —
  a refusal when the answer would be zeros, the data-leg counts when it would not. **Critic
  remediation:** a 4 KB-target fixture pins the ENGINE's `added_manifests_count` where it diverges
  from Spark's (registry `MANIFEST-3`), and the no-op pin now asserts its SEEDING call's `5, 1`
  first — without that, inverting the guard under test left the pin green),
  `call_register` (**V3-1**: `CALL system.register_table` — Spark's two arguments and three
  nullable BIGINT columns; engine-written adopt + read-back; occupied ident refuses and keeps
  the original rows; Hadoop `vN.metadata.json` error text; Spark-written format-v3 fixture under
  `fixtures/v3-spark-mor/` which is what promotes `B-MOR-3`),
  `fixtures/` (Spark-written on-disk Iceberg tables CI can adopt with no JVM),
  `call_orphan` (**MW-3**, split out of `call` when that module crossed the 1500-line ceiling —
  `remove_orphan_files` and nothing else, because every test in it is about the blast radius of a
  deletion rather than a shared mechanism, which is also why its fixture helpers live there and
  not in `common`. The armed pin compares the WHOLE table directory before and after, so it proves
  the run deleted the orphans AND not one live file; `plant_orphans` back-dates its files through
  `std::fs::FileTimes` because the fork cuts on the listed file's `last_modified` and the 24-hour
  floor forbids a cutoff young enough for a freshly written one),
  `ref_ddl`,
  `time_travel`, `metadata_tables` (**RP-1:** projection battery iterates
  `MetadataTableType::all_types`; `position_deletes` rewrites then scan-refuses.
  **MW-4b:** Glue-shaped `table_exists` — 4-part
  `.snapshots`/`.files` rewrites to `$` despite hierarchical `DataInvalid`; Unexpected
  and single-level DataInvalid stay fatal), `normalize`, `local_fs_ddl`,
  `router` (multi-statement, F-BR-2 eager DML, TRUNCATE refuse), `decimal` (G-7b bit-exact
  `Decimal128` i128 pins — literal / division / 38-clamp / avg+promotion / overflow+div-zero /
  nullability; cites Python corpus row names. **U2 (2026-08-13):**
  `pin_literal_1_23_infers_decimal128_3_2_i128` (was float64) and overflow wrap `10^38`
  at (38,0). **V-2 U3+U4a (2026-08-13):** `pin_int_times_decimal_is_12_2_i128` (was 31,2),
  `pin_cast_int_times_decimal_stays_21_2_i128`, clamp pins `(38,6)` / `(38,17)`,
  `pin_mul_38_20_still_refuses_at_plan` (DEC-8 now plans `(38,6)` — name kept);
  fixtures go through `apply_spark_float_as_decimal` via `common::setup` plus the DEC-8
  `ExprPlanner`. **U5 (2026-08-14):** `setup` installs ANSI ON; `setup_with_ansi(false)`
  for legacy NULL `/0`;
  `pin_div_by_zero_decimal38_raises_under_default_ansi` +
  `pin_div_by_zero_decimal38_returns_null_at_38_4_when_ansi_false` (type now Spark
  `(38,6)`). **R-2 (2026-08-14):** `/` i128 at `(23,13)`; DEC-6 raise / ANSI-OFF NULL;
  DEC-8 plans), `float_agg` (G7 float
  aggregation determinism — catastrophic-cancellation fixture; `sum`/`avg` `f64::to_bits` at
  `target_partitions` 1/2/8; per-count stability + cross-count spread disclosure),
  `join_null_keys` (R-3 / G8 — Spark-door NULL-key join value pin: INNER / LEFT /
  LEFT SEMI / LEFT ANTI; goldens = G4 live Spark 4.1.2, re-verified under the
  JVM lock; cites
  `spark_door_null_keys_never_match_inner_left_semi_anti`).
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
- **G15 collation refuse pins (2026-08-12)** — `collation.rs`, a NEW leaf: Spark-door
  parse-altitude refusals for expression `COLLATE`, `ORDER BY COLLATE` (two names),
  `CREATE TABLE` column `COLLATE`, `CAST AS STRING COLLATE`, SET/RESET of a collation
  `SQLConf` key (helper + `execute` + parenthesized SET), `execute_passthrough` +
  spark_ast source attach (Q-001), a string-literal negative (incl. CAST-in-literal),
  and a default (non-COLLATE) `ORDER BY` untouched pin. Ledger:
  [`../../../../task/y7-collation-refuse-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-13-y7-collation-refuse-ledger.md).
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
  [`../../../../task/g5b-temporal-range-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-12-g5b-temporal-range-ledger.md).
- **G5b-R residual pins (Y-1, 2026-08-12)** — same leaf, five added tests:
  `temporal_range_negative_offset_is_spark_empty_frame` (R3 HIGH — TIMESTAMP
  `INTERVAL '-1' DAY` is Spark's empty frame; DATE stays empty and is not refused),
  `temporal_range_day_to_second_literal_matches_spark` (R2 — `'1 12:00:00'` and `'1 0:0:0'`),
  plus the residual recordings. **W-4 (2026-08-13)** flips R1 to
  `temporal_range_unquoted_interval_literal_matches_quoted` and R5 to
  `temporal_range_interval_bound_over_int_key_is_numeric_n` (+ magnitude /
  unit-ignored pin `temporal_range_interval_bound_over_int_key_uses_numeric_magnitude`);
  R4 stays `temporal_range_following_to_following_still_includes_current_row` (120 vs 90);
  adds `temporal_range_timestamp_seed_is_microseconds` (µs type pin after #79) and
  `temporal_range_mixed_datetime_and_numeric_interval_leaves_numeric_loud` (mixed-statement
  R5 stays Arrow-cast).
  **Half-B (2026-08-12)** adds two: `temporal_range_value_inverted_frames_do_not_wrap`
  (same-kind magnitude invert after sign-normalize: `-2 PRECEDING AND -1 PRECEDING`,
  `-1 PRECEDING AND 0 FOLLOWING`, direct `2 FOLLOWING AND 1 FOLLOWING` — Spark
  `WRONG_COMPARISON`, wrapping `-1` is gone; no `10000 YEAR` pair) and
  `temporal_range_mixed_negative_timestamp_and_numeric_bare_refuses` (Q-003). Ledger:
  [`../../../../task/g5br-range-residuals-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-13-g5br-range-residuals-ledger.md),
  [`../../../../task/z4-residuals-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-13-z4-residuals-ledger.md),
  [`../../../../task/w4-z-residuals-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-13-w4-z-residuals-ledger.md).
- **MG-2 MERGE lowering strictness (2026-08-15)** — `merge.rs` gains execute-path
  pins for M2 (`merge_oracle_style_update_where_refuses` / `_delete_where_` /
  `_insert_where_`), M3 (`merge_source_qualified_set_target_refuses`,
  `merge_nested_field_set_target_refuses`,
  `merge_target_qualified_and_bare_set_targets_execute`), M8
  (`merge_insert_without_column_list_refuses`; `INSERT *` stays on
  `merge_star_forms_upsert`), M10 (`merge_non_last_unconditional_matched_refuses`,
  `merge_non_last_unconditional_not_matched_refuses`; first-match-wins stays on
  `merge_clause_order_first_match_wins` / `merge_matched_and_arm_order_update_then_delete`).
  Lowering twins live in `../merge.rs`. Ledger: `task/mg2-lowering-strictness-ledger.md`.
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
- **Z-1 / G3-E8 PR-1 (2026-08-13)** — IN-DELETE flips from refuse to execute
  (`g3e8_delete_in_subquery_deletes_exactly_the_matching_row`, quoted + temp-view,
  FROM-less IN). Residual refuse family + valve-ORDER pin restated over NOT IN / EXISTS.
  Ledger: [`../../../../task/z1-g3e8-pr1-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-13-z1-g3e8-pr1-ledger.md).
- **W-3 / G3-E8 PR-2 (2026-08-13)** — NOT IN + NULL trap execute
  (`g3e8_delete_not_in_subquery_*`, empty subquery, quoted + FROM-less). Residual refuse
  family + valve-ORDER pin restated over EXISTS / nested / UPDATE. Ledger:
  [`../../../../task/w3-g3e8-pr2-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-13-w3-g3e8-pr2-ledger.md).
- **V-1 / G3-E8 PR-3 (2026-08-13)** — `[NOT] EXISTS` ± correlation execute
  (`g3e8_delete_exists_uncorrelated_and_correlated_execute`). Residual refuse family +
  valve-ORDER pin restated over correlated IN / nested / scalar / UPDATE. Ledger:
  [`../../../../task/v1-g3e8-pr3-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-13-v1-g3e8-pr3-ledger.md).
- **R-1 / G3-E8 PR-4 (2026-08-14)** — correlated IN + identity UPDATE IN execute
  (`g3e8_delete_correlated_in_deletes_exactly_the_matching_row`,
  `g3e8_update_in_subquery_rewrites_only_the_matching_row`). Residual refuse family
  restated as the permanent v1 valve (ANY/ALL / nested / scalar / mixed / UPDATE NOT IN).
  Ledger: [`../../../../task/r1-g3e8-pr4-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-14-r1-g3e8-pr4-ledger.md).
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
| `spark_door_null_keys_never_match_inner_left_semi_anti` RED | 3VL on JOIN keys moved, or `LEFT SEMI`/`ANTI` stopped parsing. Goldens are G4 live Spark 4.1.2 — do not absorb a match-on-NULL |
| Cross-leaf helper not found | should be `pub(super)` in `common.rs` (or wrongly left private in one leaf) |
| Nested partition pin fails | `partitioned_ctas` / `partitioned_merge` / `transform_overwrite` — manifest-level `DataFile.partition` oracles |

First checks: `cargo test -p repark-spark tests::<module>::`. Escalate to: [../map.md#debug](../map.md).
