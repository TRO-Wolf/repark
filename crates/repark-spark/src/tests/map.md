# map — repark-spark/src/tests

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001). Wrapped-line fragments rewritten as complete sentences (D-002).

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

## Purpose

Lib-root unit battery for the Spark SQL door. Production code is not here: only tests, shared
fixtures, and the module manifest.
Test documentation may retain model provenance; code-quality grade tags stay outside code.

## Contents

- `mod.rs` — pure module manifest (`mod common;` + one `mod` per leaf).
- `declared_refuse.rs` — **FNP-15/16:** Spark-door parse-altitude refusals for the six
  unreachable names and the sketch family; passthrough attach pin.
  pins: fnp-15-16/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- `spark_string_literals.rs` — **SQP-1:** the string-literal escape pins (C-001..C-008, C-010,
  C-012): the escape domain, `\'`/unpaired-backslash lexing, adjacency + the DataFusion-native
  `OPTIONS` carve-out, quote-runs-are-not-triple-quotes, raw strings, LIKE/RLIKE/backtick controls,
  exactly-once-on-every-path, the one-caller grep pin, the Generic-dialect honesty pin.
- `cast_binary.rs` — **SQP-1 (C-009):** `CAST … AS BINARY` plans to Arrow `Binary` (B1/B8–B10/B13/
  B15), refuses illegal sources (`DATATYPE_MISMATCH`, B2–B7), keeps `VARBINARY` refusing (B12),
  leaves a `BINARY` DDL column untouched; `TRY_CAST(<int>)` refuses without the ANSI-off suggestion.
- `v3_cow.rs` — v3 UPDATE / MERGE
  refuse (`V3-COW-1`, both seats; V3-3 measured keep-refusal: Spark preserves `_row_id`,
  the engine rewrite reassigns), the plain-`WHERE` DELETE commits on a DV-free table (COW
  keeps first-snapshot lineage then refuses the unsafe COW second DELETE, MOR commits a Puffin DV) and a second MOR DELETE merges into the
  live vector (pins: rp-2-fork-repin/C-003, C-005; rp-3-fork-repin/C-004; v3-3-dml/C-001, C-002); short-name,
  padded merge-on-read, and v2-control cases keep `V3_MAINTENANCE_ORACLE` and ENC-1's pin.
- `create_table.rs` — also the V3R-1 type pin: `GEOMETRY` / `GEOGRAPHY` / `VARIANT` refuse at
  CREATE (`V3-GEO-1`).
- `v3_types.rs` — **V3-6:** C-001 ledger matrix + refuse of `UNKNOWN` / `VARIANT` /
  ADD COLUMN DEFAULT; C-003 opt-in v3 `timestamp_ns` / `timestamptz_ns` CREATE,
  ns Arrow round-trip, v2 refuse (asserts the fork's exact
  "timestamp_ns is not supported until v3" phrase); C-005 `ALTER COLUMN SET DEFAULT`
  refuse.
  pins: v3-6-v3-types/C-001, C-003, C-005
- `v3e4.rs` — **V3E-4:** snapshot refs, `VERSION AS OF` over DVs, expire with
  real work, orphan 24h floor on the partitioned-DV fixture after a RePark
  append, and the live-DV UPDATE pre-write refusal with snapshot, rows, and fixture bytes unchanged;
  rustdoc cites C-001..C-016 (`Model: Grok 4.6 xHigh`; rp-3-fork-repin/C-004).
- `v3_lineage.rs` — **V3-4:** Spark-door `_row_id` / `_last_updated_sequence_number` on the
  V3E-3 fixtures (MOR+DV surviving rows), created v3 derivation, v2/v1 unresolved (`No field
  named _row_id`), `SELECT *, _row_id` expands user columns only, qualified/aliased forms,
  unquoted case-fold, JOIN/CTE/subquery/`VERSION AS OF` refuse `V3-ROWID-2`, V3-COW-1 files
  byte-untouched (content-hash pin; RP-4 re-records `test_v3_cow_dml.py` after the rewrite
  CALL lift); the C-001 matrix pin finds the ledger anywhere under `task/ledgers/` so
  lifecycle moves keep it green.
  pins: rp-4-fork-repin/C-003
  pins: v3-4-serve-lineage-columns/C-001, C-002, C-005, C-006, C-007, C-008, C-009, C-010,
  C-011, C-012, C-013, C-014, C-015, C-016, C-018, C-020
- `v3e3.rs` — **V3E-3:** Spark-written partitioned v3 DV fixture and equality-delete
  + DV fixture (`fixtures/v3-spark-part-dv/`, `fixtures/v3-spark-eq-dv/`); live
  rows, partition prune, `.delete_files` content 1/2, B-MOR-3 refuse, RP-3 cells 3–6
  MOR DELETE on the partitioned DV (pins: rp-3-fork-repin/C-004);
  C-007 keeps `B-MOR-3` after measuring fork R136 as a parquet-to-DV conversion no-op;
  the measurement pin does not write outside the crate
  (pins: rp-3-fork-repin/C-007, C-011)
  (`Model: Grok 4.6 xHigh`; rustdoc cites C-013).
  **V3-5:** `rewrite_data_files` on the partitioned DV fixture drops both
  vectors (delete-ratio admits each one-file group);
  `where => 'part = 0'` drops only that vector and keeps the sibling live.
  pins: v3-5-dv-compaction/C-002, C-003, C-005
- `delete_granularity.rs` — **MW-9:** Spark-door `write.delete.granularity` (explicit
  file/partition, unknown refuse on MERGE and identity UPDATE, fork DELETE/UPDATE
  residual, ALTER-then-MERGE).
- `call_rewrite_dangling.rs` — the CALL's
  `'remove-dangling-deletes' => true` reaches the fork's composed GC and reports a true
  `removed_delete_files_count` on a partitioned v2 fixture (C-006).
- `call_rewrite_options.rs` — **rewrite_data_files options:** `where => 'part = 0'` (and `IN (0)`)
  keeps the **part=1** pre-image paths byte-identical and rewrites part=0 away; unknown strategy
  and bad where use Spark's text; `sort_order` refuses without compacting; named `BINPACK` still
  compacts v2.
  pins: maint-rewrite-data-files-options/C-002, C-003, C-004, C-005, C-006, C-007, C-008
- `write_to_branch.rs` — RP-5 C-004 family pins: INSERT VALUES/SELECT, UPDATE, DELETE,
  MERGE, INSERT OVERWRITE on a diverged branch; tag and missing-branch Spark-shaped refuse.
  pins: rp-5-fork-repin/C-004
- `common.rs` — shared fixtures (`setup`, `rows`, `run`, `register_source`, `table_rows`, …)
  and the cross-cutting helpers that more than one leaf needs (`time_travel_id_multiset`,
  `execute_without_collecting`, unsafe-cast walk helpers). **V3-2:**
  `setup_allow_create_format_version_3` (`Model: Grok 4.6 xHigh`). **U2:** `setup` /
  `setup_allow_local_fs_ddl` / `setup_strict_catalog` call
  `crate::extension::apply_spark_float_as_decimal` so Spark-door unit fixtures match
  production `configure`. **R-2:** those fixtures plus `setup_with_ansi` also call
  `register_spark_decimal_planner`; shared helpers use `pub(super)` visibility and re-exports.
- `create_table.rs` pins `ts TIMESTAMP` → `Timestamptz`;
  `ctas_of_instant_producers_stores_timestamptz` (SQL `current_timestamp` / `to_timestamp(Z)`
  / identity-partitioned CTAS).
- **Production-aligned leaves:**
  `ctas` (`register_memory_catalog` location-less CTAS lands under the warehouse), `create_table`,
  `namespace_ddl` (`IF NOT EXISTS` create-new / same / conflicting / no-location behavior),
  `catalog_ops`, `describe_show`, `alter`, `dml`
  (DELETE/UPDATE + BUG-001 valve; no production `delete`/`update` module), `insert_overwrite`,
  `partition_overwrite` (DML-B dynamic/static snapshot stamps, empty-static `delete`,
  sibling file-path stability, two-key AND + incomplete-static, string/NULL partitions,
  Hive too-many-columns refuse, empty-dynamic guard;
  pins: dml-b-insert-overwrite/C-001, C-002, C-004, C-005),
  `truncate` (DML-C: wipe summary keys, equal empty-overwrite keys, time travel,
  missing-table / view / `INVALID_PARTITION_OPERATION` / IF EXISTS parse refuse;
  pins: dml-c-truncate/C-001, C-002, C-005, C-006, C-007),
  `merge`, `merge_nmbs` (DML-A NMBS COW+MOR, Arrow types, hunt cells: NULL keys,
  MATCHED-predicate miss, extra file, source-empty UPDATE, NMBS-only dup source;
  pins: dml-a-merge-not-matched-by-source/C-001, C-002, C-003, C-004, C-005, C-006, C-007),
  `call`, and `call_orphan`. `call_remove_orphan_files_refuses_a_location_arg_under_the_fallback_root`
  and `call_orphan_shared_ctas_root_rule` pin the fallback-root safety contract. Maintenance tests
  pin Spark's full schemas, typed count sources, deletion-vector refusal, and file-granularity rules.
  `call_v3` (**V3-0 / RP-4**): v3 rewrite preserves lineage, v2 control, and
  `opt_in_create_produces_v3_and_rewrite_runs` (six-file CALL, lineage equal;
  `V3-LINEAGE-1` FIXED; `Model: Grok 4.6 xHigh`); the battery
  `the_engine_still_cannot_produce_a_v3_table` is V3-6 C-006's identity check —
  byte-untouched through V3-6.
  Pin `call_rewrite_data_files_on_v3_preserves_row_lineage`
  (`pins: rp-3-fork-repin/C-005; rp-4-fork-repin/C-003`).
  `call_v3_dv` (**V3-5**): six-file v3 MOR with live Puffin DVs;
  `rewrite_data_files` drops all six (`removed_delete_files_count = 6`,
  count columns Arrow Int32); `rewrite_position_delete_files` still refuses
  (`B-MOR-3`).
  pins: v3-5-dv-compaction/C-001, C-002, C-003, C-004, C-007
  `call_manifests` (**MW-6**) pins the two non-nullable `int` columns, no-op zero result, current
  spec filter, delete-manifest refusal, and `MANIFEST-3` count divergence.
  `call_register` (**V3-1 / RP-3 C-008**): `CALL system.register_table` arguments, three nullable BIGINT columns,
  adoption/read-back, occupied-ident refusal, Hadoop `vN.metadata.json` write bumps to `v(N+1)`,
  S3 Tables register names R126, and the Spark-written `fixtures/v3-spark-mor/`
  fixture (`B-MOR-3`),
  `fixtures/` (Spark-written on-disk Iceberg tables CI can adopt with no JVM),
  `call_orphan` (**MW-3**): full-directory before/after orphan safety and 24-hour cutoff fixtures,
  `ref_ddl` (**REF:** the write-to-branch/tag refusal names the `iceberg-datafusion`
  commit-target gap at fork pin `33be9a0`, not the superseded pin; pins:
  ref-branch-tag-wap/C-004),
  `refs_and_wap` (**REF:** both `WITH SNAPSHOT RETENTION` halves at the oracle's values and the
  reversed order refusing; the `branch_`/`tag_` READ selectors resolving the ref, joining
  against the live table, refusing loud on a missing ref, and claiming neither a
  metadata-table suffix nor a real table whose own name starts with `branch_`; and WAP
  declared — the three publish procedures and the `spark.wap.*` confs all fail closed and leave
  the branch where it was; and the read-vs-write boundary — a selector in a DML statement's
  source, `USING` operand or predicate subquery reads the ref (four classes plus CTAS, each
  asserting the ref's ids and not `main`'s), while a ref-named write TARGET still refuses even
  when the source is another selector. The oracle stamp the registry rows §2.2
  `REF-1`/`REF-3`/`REF-4` cite lives in the REF ledger C-001 (2026-09-01, live PySpark
  4.1.2 + Iceberg 1.11.0; retention values are the oracle's own `refs` rows).
  pins: ref-branch-tag-wap/C-001, C-002, C-003, C-005, C-007),
  `time_travel`, `metadata_tables` (**RP-1:** projection battery iterates
  `MetadataTableType::all_types`; `position_deletes` rewrites then scan-refuses.
  **MW-4b:** Glue-shaped `table_exists` — 4-part
  `.snapshots`/`.files` rewrites to `$` despite hierarchical `DataInvalid`; Unexpected
  and single-level DataInvalid stay fatal), `normalize`, `local_fs_ddl`,
  `router` (multi-statement, F-BR-2 eager DML), `decimal` (G-7b bit-exact
  `Decimal128` i128 pins — literal / division / 38-clamp / avg+promotion / overflow+div-zero /
  nullability; cites Python corpus row names.
  `pin_literal_1_23_infers_decimal128_3_2_i128` and overflow wrap `10^38` at (38,0).
  `pin_int_times_decimal_is_12_2_i128`,
  `pin_cast_int_times_decimal_stays_21_2_i128`, clamp pins `(38,6)` / `(38,17)`,
  `pin_mul_38_20_still_refuses_at_plan` (DEC-8 plans `(38,6)`; name retained);
  fixtures go through `apply_spark_float_as_decimal` via `common::setup` plus the DEC-8
  `ExprPlanner`. `setup` installs ANSI ON; `setup_with_ansi(false)` enables legacy NULL `/0`;
  `pin_div_by_zero_decimal38_raises_under_default_ansi` +
  `pin_div_by_zero_decimal38_returns_null_at_38_4_when_ansi_false` (Spark type `(38,6)`);
  `/` uses i128 at `(23,13)`; DEC-6 raises or returns ANSI-OFF NULL;
  DEC-8 plans), `float_agg` (G7 float
  aggregation determinism — catastrophic-cancellation fixture; `sum`/`avg` `f64::to_bits` at
  `target_partitions` 1/2/8; per-count stability + cross-count spread disclosure),
  `join_null_keys` (R-3 / G8 — Spark-door NULL-key join value pin: INNER / LEFT /
  LEFT SEMI / LEFT ANTI; goldens = G4 live Spark 4.1.2, re-verified under the
  JVM lock; cites
  `spark_door_null_keys_never_match_inner_left_semi_anti`).
- **Sibling test modules:**
  `partitioned_ctas`, `partitioned_merge`, `transform_overwrite` (still nests
  `provider_partition_correctness`), `service_managed_ctas`.
- [call_orphan.rs](call_orphan.rs) — orphan safety, cutoff, and fallback-root refusal pins.
- **Time-travel pins:**
  `time_travel_temp_views_do_not_survive_a_{successful,failed}_statement` and
  `time_travel_statement_pins_never_collide_with_a_reader_options_view` (the fix-pass collision
  pin: a reader-options registration made through `repark_core::read_table_at` must SURVIVE a
  Spark-door `VERSION AS OF` statement, and the door must mint from repark-core's counter — the
  second assertion is the one that reds whatever the numbers happen to be), beside the twin
  `time_travel_version_timestamp_branch_tag_and_errors`, plus their three leaf-private helpers
  (`leftover_time_travel_views`, `setup_time_travel_leak_table`, `temp_view_sequence` — used by
  that leaf only, so they stay out of `common.rs`). They read the default catalog/schema directly
  rather than `information_schema`, which this door's `setup` does not enable.
- [time_travel.rs](time_travel.rs) — statement-owned pinned-view cleanup and collision pins.
- `collation.rs` pins parse-altitude refusals for expression `COLLATE`, `ORDER BY COLLATE` (two names),
  `CREATE TABLE` column `COLLATE`, `CAST AS STRING COLLATE`, SET/RESET of a collation
  `SQLConf` key (helper + `execute` + parenthesized SET), `execute_passthrough` +
  spark_ast source attach (Q-001), a string-literal negative (incl. CAST-in-literal),
  and a default (non-COLLATE) `ORDER BY` untouched pin. Ledger:
  [`../../../../task/y7-collation-refuse-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-13-y7-collation-refuse-ledger.md).
- `window_temporal_range.rs` pins the Spark door's `RANGE` frames on datetime order keys and the
  paths that must remain unchanged:
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
- The same leaf also pins:
  `temporal_range_negative_offset_is_spark_empty_frame` (R3 HIGH — TIMESTAMP
  `INTERVAL '-1' DAY` is Spark's empty frame; DATE stays empty and is not refused),
  `temporal_range_day_to_second_literal_matches_spark` (R2 — `'1 12:00:00'` and `'1 0:0:0'`),
  plus the residual cases:
  `temporal_range_unquoted_interval_literal_matches_quoted` and R5 to
  `temporal_range_interval_bound_over_int_key_is_numeric_n` (+ magnitude /
  unit-ignored pin `temporal_range_interval_bound_over_int_key_uses_numeric_magnitude`);
  R4 stays `temporal_range_following_to_following_still_includes_current_row` (120 vs 90);
  adds `temporal_range_timestamp_seed_is_microseconds` (µs type pin after #79) and
  `temporal_range_mixed_datetime_and_numeric_interval_leaves_numeric_loud` (mixed-statement
  R5 stays Arrow-cast).
  It also pins `temporal_range_value_inverted_frames_do_not_wrap`
  (same-kind magnitude invert after sign-normalize: `-2 PRECEDING AND -1 PRECEDING`,
  `-1 PRECEDING AND 0 FOLLOWING`, direct `2 FOLLOWING AND 1 FOLLOWING` — Spark
  `WRONG_COMPARISON`, wrapping `-1` is gone; no `10000 YEAR` pair) and
  `temporal_range_mixed_negative_timestamp_and_numeric_bare_refuses` (Q-003). Ledger:
  [`../../../../task/g5br-range-residuals-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-13-g5br-range-residuals-ledger.md),
  [`../../../../task/z4-residuals-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-13-z4-residuals-ledger.md),
  [`../../../../task/w4-z-residuals-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-13-w4-z-residuals-ledger.md).
- `merge.rs` pins execute-path strictness: `merge_oracle_style_update_where_refuses` / `_delete_where_` /
  `_insert_where_`), M3 (`merge_source_qualified_set_target_refuses`,
  `merge_nested_field_set_target_refuses`,
  `merge_target_qualified_and_bare_set_targets_execute`), M8
  (`merge_insert_without_column_list_refuses`; `INSERT *` stays on
  `merge_star_forms_upsert`), M10 (`merge_non_last_unconditional_matched_refuses`,
  `merge_non_last_unconditional_not_matched_refuses`; first-match-wins stays on
  `merge_clause_order_first_match_wins` / `merge_matched_and_arm_order_update_then_delete`).
  Lowering twins live in `../merge.rs`; ledger: `task/mg2-lowering-strictness-ledger.md`.
- `merge.rs` also pins:
  `merge_duplicate_source_keys_with_matched_raises`,
  `merge_duplicate_source_keys_insert_only_commits_both`,
  `merge_matched_and_arm_order_update_then_delete`,
  `merge_matched_and_threshold_update_or_delete`, plus the leaf-private `score_table_rows`
  helper for the two score-arm pins.
- `dml.rs` pins the `g3e8_*` subquery-predicate valve: the refuse family for both verbs and
  adjacent negatives that prove the valve did
  not widen (non-subquery DML, `INSERT … SELECT` with a subquery, MERGE over a subquery source,
  `UPDATE … SET col = (SELECT …)` with and without a `WHERE`), the guard-ORDER pin against the
  BUG-001 valve, the **FROM-less** `DELETE <table> WHERE …` family + its negative (the panel's
  live bypass — that spelling fails the router's Databricks parse and reaches the executor
  through the passthrough's own parse), and the CTE-prefixed `WITH … DELETE` refusal.
- `g3e8_delete_in_subquery_deletes_exactly_the_matching_row` pins executable IN-DELETE forms and
  the residual refusal family.
  Ledger: [`../../../../task/z1-g3e8-pr1-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-13-z1-g3e8-pr1-ledger.md).
- `g3e8_delete_not_in_subquery_*` pins NOT IN, NULL, empty, quoted, and FROM-less forms; the
  residual refusal family remains covered.
  Ledger:
  [`../../../../task/w3-g3e8-pr2-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-13-w3-g3e8-pr2-ledger.md).
- `g3e8_delete_exists_uncorrelated_and_correlated_execute` pins `[NOT] EXISTS` with and without
  correlation and the residual refusal family.
  Ledger:
  [`../../../../task/v1-g3e8-pr3-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-13-v1-g3e8-pr3-ledger.md).
- `g3e8_delete_correlated_in_deletes_exactly_the_matching_row` and
  `g3e8_update_in_subquery_rewrites_only_the_matching_row` pin correlated IN and identity UPDATE IN.
  Ledger: [`../../../../task/r1-g3e8-pr4-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-14-r1-g3e8-pr4-ledger.md).
  `normalize.rs` pins the detector
  (`g3e8_subquery_detector_fires_on_every_spelling_and_no_other`) and the statement-level valve
  pin (`g3e8_statement_valve_covers_both_verbs_and_renders_the_parsed_target`, the entry point
  `spark_ast::execute_passthrough` calls) — production-module alignment: both live in
  `../normalize.rs`. Leaf-private helpers (`g3e8_setup`, `g3e8_seed`, `assert_g3e8_message`) stay
  in `dml.rs`; only that leaf uses them.
  See `task/g3e8-guard-ledger.md`.

## Mapping rule

1. Production-module alignment by name / primary assertion.
2. Two-module tests follow the primary assertion; genuine cross-cutting residue gets a small
   shared home (`dml`, `common`).
3. Nested mods lift to sibling files of the same name (path-preserving).
4. Helpers move byte-identically; only new lines are `mod` decls and `use` adjustments.

The test modules follow production ownership. Archived ledgers remain available from the pointers
above.

## Pointers

- Up: [../map.md](../map.md)
- Registry pins: [../../../../docs/spark-sql-iceberg-parity.md](../../../../docs/spark-sql-iceberg-parity.md)
- Surface matrix (also `#[cfg(test)]`): [../matrix.rs](../matrix.rs)

## Debug

- `spark_string_literals.rs` and `cast_binary.rs` are byte-frozen (sha256) by
  `python/repark-parity/tests/test_pr_245_revalidation_record.py`; any edit, a comment rewrap
  included, reds that record — revert, never re-hash.
| Symptom | First check |
|---|---|
| Test identity doubt | `cargo test -p repark-spark --lib -- --list`; compare the production-aligned leaf names |
| Matrix pin string red / stale | `matrix.rs` strings are `--list` names; update with renames only — never re-point a surface |
| Registry pin path stale | filesystem form `crates/repark-spark/src/tests/<file>.rs::leaf` |
| Missing `TempDir` / Arrow types in a leaf | `use super::common::*;` (shared re-exports live in `common.rs`) |
| `spark_door_null_keys_never_match_inner_left_semi_anti` RED | 3VL on JOIN keys moved, or `LEFT SEMI`/`ANTI` stopped parsing. Goldens are G4 live Spark 4.1.2 — do not absorb a match-on-NULL |
| Cross-leaf helper not found | should be `pub(super)` in `common.rs` (or wrongly left private in one leaf) |
| Nested partition pin fails | `partitioned_ctas` / `partitioned_merge` / `transform_overwrite` — manifest-level `DataFile.partition` oracles |

First checks: `cargo test -p repark-spark tests::<module>::`. Escalate to: [../map.md#debug](../map.md).
