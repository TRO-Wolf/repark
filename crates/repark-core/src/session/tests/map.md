# map — repark-core/src/session/tests

- `session.rs` — **U1-MEM-LAYOUT-1 (2026-09-23):** direct and configured memory-catalog registration preserve the warehouse fallback root and record the layout root. pins: u1-mem-layout-1/C-001

## Purpose

Session test modules. `session.rs` declares `#[cfg(test)] mod tests;`.

## Contents

- `mod.rs` — thin index (rustfmt module order).
- `read_postgres.rs` — **C-3 fold 1 (2026-10-08):** a count of `3000000000` from the door's
  argument refuses as `NumberFormat` before any connection. pins: c-3/C-010
- `read_postgres.rs` — **C-3 fold 1 (2026-10-08):** the pre-connection number refusal is pinned
  on `numPartitions`; a bound is no longer parsed before the relation resolves, so a date bound
  beside a bad count still names the count. pins: c-3/C-009
- `read_postgres.rs` — **C-3 (2026-10-07):** `partition_options_refuse_as_spark_does_before_any_connection`
  (the all-or-none sentence from arguments and from properties, a bound that is not an `i64`
  as `NumberFormat` without its value, `query` with a column, `predicates` declared from the
  argument and from a property, a spelling given twice) and
  `num_partitions_alone_is_no_partitioning_and_never_a_setting`. pins: c-3/C-006
- `read_postgres.rs` — **C-2d fold 1 (2026-10-07), N6:** behind `postgres`.
  `a_dbtable_property_is_the_target_never_a_setting`: a `dbtable` property, in any case, is
  dropped before the settings check, so a read without `user` answers that refusal and never
  names `dbtable` (the verifier's V2). pins: c-2/C-113
- `attr_id.rs` — **ATTR-ID-1 S1 (2026-09-30):** the propagation pins for the `repark.attr`
  field-metadata key, measured on DataFusion 54.1.0 alone (a frame tagged by
  `alias_with_metadata`, ids read by position after each node). A bare column and an alias of a
  column inherit the id; a cast copies the source id (so the stamp must override it);
  arithmetic, a literal, `abs`, `upper`, a one-argument `coalesce` and `CASE` carry none;
  Filter, Limit, Sort, Distinct and SubqueryAlias keep every id; a self-join carries the same
  ids on both sides; Union and `union_by_name` keep an id only where every input carrying the
  column has the same one (a self-union keeps all, a union of distinct ids keeps none, and a
  column the first input lacks keeps the second input's id); an Aggregate key keeps
  its id and the aggregate values carry none; a Window keeps its input ids and its value
  carries none; a temp view registered through `create_or_replace_temp_view_from` and read back
  by SQL keeps the ids, and the facade's join shape over two views of one frame repeats them.
  pins: attr-id-1/C-001
  The same file pins the core over `frame_names`: `stamp` mints one fresh id per source field
  and is idempotent on a stamped source and a stamped projection, and a temp view of a stamped
  frame reads back the same ids; it keeps column ids and mints distinct fresh ids for a cast,
  arithmetic and a literal; it leaves a stamped Filter/Limit/Sort/Distinct/SubqueryAlias chain
  unchanged; it keeps an Aggregate key's id and mints the values and a Window output; a
  Union, `union_by_name` and `union_by_folded_name` take the first input's ids, a union of two
  unstamped sources mints three distinct ids, and a column the first input lacks gets a fresh
  id, not the second input's. Over the
  facade's join shape (two temp views, `requalify_join_sides`, then
  `remint_join_collisions`) the left keeps its ids and each colliding right id is re-minted
  once, so right-side twins stay twins and a written `id` over both sides is ambiguous; a join
  of distinct attributes is returned unchanged. `resolve` over `(id, Data, s)` under both rules:
  one hit, twins of one attribute (same and case-folded displays), two attributes under one
  name (same and case-folded displays), a written qualifier's hit and miss, a cast twin that
  reuses the name, and a missing id or a display-count mismatch as an error. Mutations M1–M4
  (the ledger's record) red these pins. pins: attr-id-1/C-002, C-003, C-004, C-006
  **ATTR-ID-1 S1b (2026-09-30, VA1-4):** the join no-op pin first asserts the six joined ids
  are present and distinct, so it cannot pass with every id missing. pins: attr-id-1/C-020
  **ATTR-ID-1 S3a timing cut (2026-09-30):** `plan_is_stamped_matches_what_stamp_would_change`
  asserts both directions of the mirror on every root shape the facade spawns: a stamped
  source, a bare source, a pure-column projection, a cast/literal projection, a
  Filter/Limit/Sort/Distinct/SubqueryAlias chain, a fresh Aggregate, a fresh `lag` Window
  whose copied ids are all native, a Filter over a bare Aggregate, a self-union, a union of
  two distinctly stamped frames, and a field-less `EmptyRelation`. Each shape asserts the
  predicate's answer and that `stamp` returns the plan unchanged exactly when the predicate
  reads stamped. pins: attr-id-1/C-023
  **ATTR-ID-1 S3e (2026-10-01):** the six `resolve_*` tests move unchanged to
  `attr_id_resolve.rs` (the file sat at the ceiling after the facade-qualifier
  argument joined every `resolve` call), and every remaining `resolve` call
  passes `None` for it. pins: attr-id-1/C-039
  **ATTR-ID-1 S4 (2026-10-02):** two `projection_source_ids` pins: alias,
  single-column `coalesce`, the `IS NOT NULL` fill `CASE` and the equality
  replace `CASE` read their input id; multi-column `coalesce`, arithmetic and
  `upper` read none. pins: attr-id-1/C-040
- `attr_id_resolve.rs` — **ATTR-ID-1 S3e (2026-10-01):** the moved `resolve_*`
  pins (one hit, twins of one attribute, two attributes under one name, a
  written qualifier's hit and miss, the cast twin, the missing-id and
  display-count errors), with local copies of the `source`/`stamped`/`strings`
  helpers, as `attr_id_s3e.rs` already does. pins: attr-id-1/C-039
- `attr_id_s3e.rs` — **ATTR-ID-1 S3e (2026-10-01):** 14 pins for the
  qualified-name family: facade-over-plan matching, the using-key union, the
  free-ref rewriter (bind, pass-through, ambiguity), the grandchild key, the
  join source map (using, condition, semi, non-join), and the star positions
  over plan and facade qualifiers. pins: attr-id-1/C-039
- `case_bind.rs` — **ATTR-ID-1 S3e (2026-10-01):** the 17 binder pins moved
  unchanged out of the inline `case_bind.rs` test module (the file sat at its
  ceiling); imports switch from `super::` to `crate::frame_names`, with
  `bind_names` through its widened `pub(crate)` path. pins: attr-id-1/C-039
  **ATTR-ID-1 SJ-5 F1 (2026-10-03):** three `free_sql_names` pins (verbatim
  spellings, alias/function/qualified skips, nested-scope and guard
  declines).
  **ATTR-ID-1 SJ-5 F2 (2026-10-03):** qualified idents collect with head
  parts; six `sql_mentions_duplicate` pins (present, absent, literal-only,
  backquoted, case-fold, non-word dup).
  **CASESENS-2 port (2026-10-03):** `folded_with_columns_keys_refuse_only_under_ignore_case`
  pins `refuse_folded_duplicate_keys` (the charter's inline pin, moved with the
  surviving function; a Greek final-sigma pair proves the Java lowering).
  pins: casesens-2/C-010
- `attr_id_s3b.rs` — **ATTR-ID-1 S3b (2026-10-01):** the filter/sort binder
  pins, 15 tests on tagged `MemTable` frames (`alias_with_metadata` ids, no
  facade). `sort_shape` over a union, a join, an aggregate, a select through a
  filter, a stamp passthrough, and a scratch-relation join-select (which skips
  to the join); `grandchild_key` binding a shared using-key id, refusing
  folded rivals, missing an absent name, and declining a non-Projection root;
  `bind_free_names` rewriting a display to its engine field, refusing an
  ambiguity with an exact spelling present on the filter path, routing a
  Project sort through the projection input (SM-2c round B; the S3b oldest-id
  pin is rewritten), refusing unresolved on a join sort, leaving a
  miss and a qualified token untouched, and binding the outer query of
  `Exists`/`InSubquery` without entering the subplan. Mutation: skipping
  `resolve` on the filter path reds the 4 filter pins and leaves the sort/shape
  pins green. pins: attr-id-1/C-025
  **Fold SM-2c round B (2026-10-06):** the Project-sort pin now asserts the
  input spelling (`v`, not the oldest engine), plus a case-mismatched respell
  pin, a pass-through pin for a non-unique input, an unresolved pin for twins
  meeting at a join, and a sourced-twin pin over a reminted input.
  pins: attr-id-1/C-069, C-070, C-071
  **Fold SM-2d (2026-10-07):** new pins leave the key unbound when no twin
  lineage runs through the nearest visible column, refuse
  `UNRESOLVED_COLUMN` when a join input carries the visible name twice, and
  keep the written spelling only when the output engines are twins (the
  respell pin still asserts the input casing). pins: attr-id-1/C-071
  **STAMP-2-R5P6-1 (2026-10-07):** the lazy-trace pin binds a unique sort key
  with the lineage trace counter (`SORT_TRACES`, `#[cfg(test)]` only) unmoved
  and, as its positive control, moves it by exactly 4 on an ambiguous Project
  key, one per lineage door. Mutation: forcing any door into the bound arm
  leaves every binding answer equal and reds only the counter assertion;
  uncounting a door reds the exact count. pins: stamp-2-r5p6-1/C-004, C-010
- `attr_id_seam.rs` — **ATTR-ID-1 S2b (2026-09-30):** the logical/physical seam. A core
  session's optimizer starts with `repark_strip_attribute_ids` and its analyzer does not carry
  it, so an analyzed plan (the Spark SQL door's eager analysis) keeps its ids. An Aggregate
  over a stamped clean source (the facade's `inferSchema` shape: `sum(CASE … TRY_CAST …)`)
  plans and runs, and over stamped Window, Union, `union_by_name`, semi-join, self-join,
  scalar-subquery and `EXISTS` frames too; each optimized plan carries no id at any node while
  the frame's own schema keeps every id. A source keyed outside the session under a
  re-labelling alias still runs, since the schema is recomputed rather than blanked. With the
  rule removed four pins fail, three with DataFusion's "Physical input schema should be the
  same" error (the ledger's M-A1). A stamped frame (a struct field access, an aggregate)
  optimizes to exactly its unstamped twin's plan; without the unalias step DataFusion's
  `push_down_leaf_projections` fails on the stamp's `s AS s` alias (M-A2). `df_guard.rs`'s
  rule-order pin now expects the strip first.
  pins: attr-id-1/C-015, C-016
  **PERF-ATTR-STAMP-2 O-1 (2026-10-03):**
  `a_stamped_plan_reaches_the_optimizer_backstop_and_its_collapsed_twin_plans_the_same`: for a
  stamped source, a stamped aggregate and a stamped failed-cast count, the optimizer backstop
  leaves no id, `strip_for_execution` leaves no id, and the twin optimizes to the backstop's
  plan text with schema. It goes red if the backstop leaves the optimizer list, and if the twin
  uses the non-collapsing `strip`. pins: attr-id-1/C-052
- `attr_id_fresh.rs` — **ATTR-ID-1 S1b (2026-09-30):** fresh ids for computed outputs.
  Measured on DataFusion alone: a negated group key, `first_value`, `last_value`, `lag`, `lead`
  and `nth_value` copy their argument's id. After `stamp`, every aggregate value the facade
  emits (`first`, `last`, `first` ignoring nulls, `min`, `max`, `sum`, `avg`, `count`,
  `collect_list`, `collect_set`, `nth_value`) and every cast, negated or arithmetic group key
  gets its own fresh id while a bare-column key keeps its id; every window output (`lag`,
  `lead`, `nth_value`, and `first_value`/`last_value`/`max` over a window) is fresh while the
  input passes through; a `select` over a same-op Window or Aggregate mints what it computes
  and keeps what it passes; SQL `GROUP BY` and `HAVING` over a view do the same. Each shape
  re-stamps unchanged. A parquet file whose footer carries `repark.attr` (read with
  `skip_metadata(false)`; the default read drops field metadata) gets fresh native ids once,
  two reads share none, and the frame aggregates and runs. Mutations M5, M6, M7 and M8 (the
  ledger's record) red these pins. pins: attr-id-1/C-017, C-018, C-019, C-021
- `attr_id_verify.rs` — **ATTR-ID-1 S1b (2026-09-30):** the S1 verifier's regression
  pins, ported with only `rustfmt` and one split (the self-join pin's semi-join and
  qualified-resolve tail is its own test, for clippy's length limit): `first_value`/`last_value`
  values, cast and negated keys, `lag`/`lead` roots, `lag` through `select` resolving
  ambiguous, `first_value` through SQL over a view, a union of unions, three-way and nested
  self-joins, a semi join and qualified names over a re-minted join, and an alias over an
  aliased cast that carries an id. pins: attr-id-1/C-017, C-018
  **ATTR-ID-1 S2 (2026-09-30):** a USING self-join keeps the key and left ids and re-mints
  the two colliding right ids distinctly; `strip` removes every id from a Projection root
  keeping names, types, nullability, and qualifiers, returns an unstamped plan unchanged,
  and clears a non-Projection (Aggregate) root.
  pins: attr-id-1/C-011, C-009
  **ATTR-ID-1 S2 recursive (2026-09-30):** a stripped deep plan and a stripped USING
  self-join collect clean batches (the optimizer cannot resurrect the key); strict and
  by-name unions rebuild with names preserved and clean output; a keyed `TableScan`
  fails loud; `strip_record_batches` cleans a keyed schema and its batches keeping
  values, and returns clean input untouched.
  pins: attr-id-1/C-009
  **ATTR-ID-1 S2 fix (2026-09-30):** `EXPLAIN`, `EXPLAIN ANALYZE`, `DESCRIBE`, `INSERT`,
  `COPY`, and `DROP TABLE` plan to statement roots that `stamp` returns unchanged while a
  `SELECT` root stays a relation; a field-less `EmptyRelation` is a statement, one with
  fields a relation. pins: attr-id-1/C-014
- `frame_lineage.rs` — **ATTR-ID-1 SJ-1a (2026-10-02):** 16 pins for the self-join
  lineage core over hand-built DAGs (`FrameNode::{root, derived, set_op, join}`, with each
  join's `remint` minted from `shared_ids`, the way SJ-3 will build them). Each Spark cell
  is named in its test: refusals `A_inner_sel_f_v`, `A_sel_only_id_left_sel_d_v`,
  `A_wc_sel_w_v`, `K_on_union_first_input`, `K_on_union_named_first_input`,
  `G_eq3_cross_tok`; answers `A_rev_left_sel_f`, `A_sibling_absent`, `A_wc_sel_w_id`,
  `F_left_semi_sel_f`, `K_on_union_second_input`, `K_on_union_named_second_input`; and
  `K_off_shared_sel` through `renewed_absent`. Three more pins: lineage reach, frame-id
  order and `renews`; an unstamped output is an internal error; and `remint_shared`
  renews a shared id that is not an output collision, returning its map. Mutations,
  each reverted: the walk skips right sides (M-A1) reds 7 tests; `shared_ids` limited
  to the left's outputs (M-A2) reds `a_sel_only_id_left_sel_d_v_refuses` and
  `k_off_shared_sel_ref_is_renewed_absent`; `SetOp.others` walked (M-A6) reds
  `k_on_union_named_second_input_answers`; `emits_right` ignored (M-A7) reds none (see
  `../df_guards/map.md`). `attr_id.rs` and `attr_id_verify.rs` now call
  `remint_shared` with `join_collisions`, the old behaviour.
- `join_qualifiers.rs` — **CASESENS-2 port (2026-10-03):** six pins for
  `predicate_names::bind_condition_qualifiers` through `prepare_join_condition`
  on stamped frames with facade qualifiers: alias-qualified names bind per side
  under both rules (and in compound arithmetic); a wrong-case, unknown or
  missing qualifier keeps its text; a qualifier both sides hold stays unbound;
  a lambda parameter shadows the qualifier inside its own body only, by the
  session rule; two ids under one qualifier refuse `AMBIGUOUS_REFERENCE`; a
  struct field after the bound column is kept. pins: casesens-2/C-009
  **Phase 2 (2026-10-03):** `sort_twins_from_two_join_positions_meet_at_the_join`
  pins `sort_hits_meet_at_join` on SQL-built plans: join twins meet, also under
  `DISTINCT`/`LIMIT`; a single position, a computed twin and a join-free
  projection do not. pins: casesens-2/C-012
  **R-CS2P-1 (2026-10-03):**
  `filter_qualifiers_fold_to_the_alias_spelling_only_under_ignore_case` pins
  `fold_frame_qualifiers` on a `Tb`-qualified schema: unquoted, upper-case,
  backticked and struct-path roots fold to `` `Tb` `` under `IgnoreCase`; the
  `Exact` rule, an already-backticked exact root, an unknown qualifier, a
  missing next name, a lambda parameter and a subquery stay as written.
  pins: casesens-2/C-013
- `self_join.rs` — **ATTR-ID-1 SJ-1b (2026-10-02):** 62 pins for `df_guards/self_join.rs`
  over hand-built DAGs (the probes' fixtures: `f`, `g`, `a`, `b` share `d`'s ids, `w`
  re-mints `id`, `r` re-mints `v` as `z`, `s` keeps `id`, `e` is unrelated). Each test is
  named after its measured Spark cell (`selfjoin/sj1.spark.json`, `sj2`, `sj4`, `sj5`, and
  the lane's `sj-1b/probe/sj1b.spark.json` and `sj1b2.spark.json` for the `X_*` cells):
  every `B_*`, `D_*`, `G_eq3_*` (`G_eq3_str` and `G_eq3_right_nested` through the
  token-free and inner conditions), `I_rewrite_name_missing` and `F_left_anti_gt`
  verdict, plus `E_parent_alias_gt`, the `P_h2_*`/`P_s4_*` condition cells,
  `K_off_cond_missing`, the `H_off_*` condition cells, the measured `X_*` shapes (nested
  and half casts, `!=`, reversed and foldable literals, `rand()`, `IN`, the
  `DeduplicateRelations` condition rewrite), and the post-join `check_refs` cells
  `D_self_eq_sel_d`, `A_inner_sel_f_v`, `P_v_left_join_right_parent` and `I_window_f`.
  Refusals assert the names Spark prints with its `#<n>L` suffixes stripped; rewrite
  errors assert Spark's message byte for byte. Token pins: quoted spans are text,
  qualifiers parse, and a token without a frame field is loud. Mutations, each reverted
  (records in the SJ-1b hand-back): the equality exemption removed (M-A3), the rewrite
  resolving by id (M-A4), right-first binding (M-A5).
  **ATTR-ID-1 SJ-2 (2026-10-02):** `x_f0_frameless_token_binds_by_id_and_never_flags`
  pins the `F0` contract — a frameless token parses, never flags ambiguity, binds
  by id, and still refuses loud when its id is missing.
  **ATTR-ID-1 SJ-2 R-SJ2-3 (2026-10-02):**
  `x_empty_root_never_renews_and_skips_refusal_checks` pins the inert empty
  `Root`: it never renews, and `check_refs` returns early on it even when the
  display count cannot match the empty outputs.
  **ATTR-ID-1 SJ-3 R-SJ3-2 (2026-10-02):**
  `tokens_with_a_leaf_display_field_parse_and_span_to_its_close` pins that the
  parser skips the token's `__D<hex>__` leaf field (the `rfind("__")` close
  already covers it; no production change).
  **ATTR-ID-1 SJ-4 (2026-10-02):** `tok_leaf` renders tokens with the `__D`
  field; `k_off_shared_sel_missing` pins conf-off MISSING APPEAR,
  `k_on_shared_sel_prefers_1182` pins ambiguity-first ordering, and
  `k_drop_sel_missing_from_input` pins MISSING_FROM_INPUT under both confs;
  `i_engine_display_falls_back_to_token_leaf` pins the leaf fallback for
  engine-flavored displays.
  **CASESENS-2 port (2026-10-03):** the `side` helper sets `qualifiers: None`.
- `session.rs` — ported v1 session battery plus P2G R2 / A13 / metadata-enumeration pins. RP-5: the bare-session half of the metadata-table enumeration contract (fork F-8 listing); mutation — make `information_schema` expect a `$snapshots` twin and the pin reds. pins: rp-5-fork-repin/C-003
  Child: [session/catalog_registration.rs](session/map.md).
  RP-5: `information_schema` hide pin now cites fork F-8 listing (no engine shim).
  pins: rp-5-fork-repin/C-003
- `aws_gate.rs` — E-2 offline AWS-gate pins.
- `path_write.rs` — **S3-PATH-WRITE-1 round 1 (2026-09-28):** save-mode protocol
  pins on an in-memory store (no AWS): mode matrix, marker-last commit order,
  exists-any-object, overwrite delete-then-write, bucket-root refusal.
  pins: s3-path-write-1/C-007, C-008, C-009, C-015
  **TEXT-WRITE-TIMESTAMP-ZONE-1 (2026-09-29):** the csv `dateFormat` refusal
  leg moved to `temporal_write_options_are_honored_on_csv_path_write`: the
  option now succeeds instead of refusing.
  pins: text-write-timestamp-zone-1/C-002
  **TEXT-WRITE-TIMESTAMP-ZONE-1 re-verify 3 fold (2026-09-30):** the
  failed-write rollback pins: a 400k-row JSON write with zone letters on NTZ
  leaves the destination key set unchanged for overwrite-into-empty, append
  (existing bytes also unchanged) and partitioned overwrite.
  pins: text-write-timestamp-zone-1/C-007
  **TEXT-WRITE-TIMESTAMP-ZONE-1 re-verify 4 fold (2026-09-30):** the
  concurrent-writer pins: keys PUT under the same prefix, a sibling prefix
  and an unrelated prefix while a 400k-row append fails all survive with
  their bytes, and a failing append at the bucket root keeps a foreign
  prefix intact while its own parts are gone.
  pins: text-write-timestamp-zone-1/C-009
- `text_write_format.rs` — **TEXT-WRITE-TIMESTAMP-ZONE-1 (2026-09-29):**
  compiler/validator/renderer pins against the Spark oracle: every error
  class per pattern kind, DST-gap and LMT-seconds default renders, the
  offset-letter matrix, and the SELECT builder (star fast path, wrapping,
  partition skip, eager rejection).
  pins: text-write-timestamp-zone-1/C-004
  **TEXT-WRITE-TIMESTAMP-ZONE-1 sink-format round (2026-09-30):** the SELECT
  builder pins are now COPY-parts pins (plain format keep, sink format
  resolution with zone, partition skip, eager rejection, quote-only escaping,
  case twins); the wrapping-shape pins left with the UDF.
  **TEXT-WRITE-TIMESTAMP-ZONE-1 verifier fold (2026-09-29):** quote-run,
  year-width, proleptic-year, trailing-`]`-class, backslash-escape, and
  case-twin pins (52 total).
  **Re-verify (2026-09-29):** `g`-padding, no-LEGACY-clause, offset-cache,
  and scalar-vs-fast differential pins (56 in-module; the two
  temporary release probes left with the re-verify fold).
- `text_write_format_cache.rs` — **TEXT-WRITE-TIMESTAMP-ZONE-1 re-verify 2
  (2026-09-30):** the zone-offset cache proves itself against direct
  chrono-tz lookups over all 597 bundled zones: every transition found by a
  3-day scan of 1840-2100 (exact instants plus microsecond/second/hour/day
  neighbours, each queried after warming the cache days away on both sides),
  sampled years 1-9999, and a fixed-offset battery. Every resolve also holds
  the cached window at or under 15 minutes, so a reintroduced step search
  reds the suite structurally.
- `text_write_sink_spike.rs` — **TEXT-WRITE-TIMESTAMP-ZONE-1 sink-format
  round (2026-09-30):** step-0 spike for wiring route (A). A test-only
  wrapping format factory, format, sink and serializer over CSV/JSON prove
  in DataFusion 54.1.0 that a custom `STORED AS` name resolves, custom
  OPTIONS keys reach `create` verbatim, part files keep the `.csv`/`.json`
  extension, and a serializer error surfaces with its message intact; a
  no-strip control shows unstripped custom keys fail the inner factory.
- `text_write_sink.rs` — **TEXT-WRITE-TIMESTAMP-ZONE-1 sink-format round
  (2026-09-30):** end-to-end pins for the sink serializer: zone-correct
  CSV/JSON bytes, user patterns, raw partition directory names, lazy and
  eager error identity, empty and all-null frames, the
  keep-partition-columns native pass-through against plain CSV, and the
  signed year-10000 render.
- `s3_prefix_read.rs` — **S3-PATH-WRITE-1 round 2 (2026-09-28):** slashless S3
  prefix reads on an in-memory store (no AWS): a written prefix reads back
  slashless for parquet, csv and json; exact part URLs keep single-file reads
  for all three; the trailing-slash and `s3a://` spellings read the same rows.
  pins: s3-path-write-1/C-013
- `hadoop_naming.rs` — **PR-B hadoop naming (2026-09-24):** a catalog configured through the
  config map with `type=hadoop` over a LocalFs tempdir warehouse: create through the registered
  handle, then two SQL `INSERT`s. The `*.metadata.json` names in `db/t/metadata/` are exactly
  `v1`, `v2` and `v3`, and `version-hint.text` reads `3`. The core default dialect routes no
  Iceberg DDL, so the table is created through the catalog handle. Mutation: pass an empty map
  from the session Memory arm and the hadoop pin goes red.
  **CATALOG-1 (2026-09-26):** the shared helper builds with the `catalogExtensions` opt-in
  so the memory-type naming tests keep working catalogs.
  **PR-B r3 (2026-09-24, critic V-002/V-003):** `assert_uuid_metadata_names` holds the fork's
  uuid contract (`{version:0>5}-{uuid}.metadata.json`, Display of `MetadataLocation`): three
  names, versions `00000`..`00002`, lowercase 8-4-4-4-12 hex, distinct uuids, and no hint. It
  runs on `type=hadoop` plus an explicit `metadata-naming=uuid`, on `type=memory`, on
  `catalog-impl=org.apache.iceberg.inmemory.InMemoryCatalog`, and on direct
  `register_memory_catalog`. `uuid_metadata_name_check_rejects_near_misses` shows the checker
  refuses `v1`, gzip, short-version, upper-case, short and unhyphenated names. Mutation: force
  `hadoop` in the Memory arm whenever `metadata-naming` is present and the explicit-uuid pin
  goes red.
  **Class sweep (2026-09-24):** `memory_arm_forwards_only_metadata_naming_to_the_fork_builder`
  reads `Catalog::properties()` on the registered handle. It is exactly
  `{metadata-naming: hadoop}` for `type=hadoop` and empty for `type=memory`, even with extra
  passthrough props (red when the arm forwards every spec prop).
  `memory_registration_keeps_fallback_root_and_local_write_root` pins the moved registration
  body on the props path and the direct path: `TempFallbackAllowed` at the warehouse, the
  layout root, and the SEC-02 local write root.
  `rename_error_mapping_keeps_the_engine_error_class`: after `unsupported_message_error`,
  `engine_err` still answers `NotImplemented` with the bare message, `Analysis` for
  `TableNotFound`, and `Iceberg` for `Unexpected`.
- `conf_unread.rs` — **CONF-UNREAD-1 step 1 (2026-09-11):** the four
  accepted-but-unread keys. `coalesce_batches` refuses loud at build and at
  runtime `SET`, naming the key and the reason (DataFusion 54.1.0 defines the
  option but no engine path reads it); `enable_page_index` and
  `bloom_filter_on_read` set to `false` reach both `SessionConfig` and the
  session table options the scan source reads (control leg: the default session
  reads `true`); `write_batch_size` set to `1000` reaches both structures
  (control leg: the default reads `1024`).
  pins: conf-unread-1/C-001, C-002, C-003, C-004
  Step 2 (2026-09-11): the refusal pins hold the message's load-bearing tokens
  (key named, `cannot take effect`) that `docs/guide/session-and-conf.md`
  quotes verbatim in its `datafusion.*` paragraph.
  pins: conf-unread-1/C-007
- `conf_dump_redaction.rs` — **SOURCE-URL-REDACT-1 (2026-10-06):** `conf_dump()` masks a password inside a builder
  conf URL and an S3 endpoint, while `resolve_endpoint_from_dump` over the stored rows still
  sees the configured endpoint. pins: source-url-redact-1/C-008
- `df_guard.rs` — nine DataFusion 54.1 guard pins (the eighth, 2026-09-25: the leaf-pushdown alias-collision decline on a LEFT JOIN projection; the ninth, fix round 5: `BoomOnProjection`, a non-collision inner error on a Projection stays loud, so the decline cannot widen to every error). pins: u8-write-sql/C-030
- `io_stats.rs` — **ICE-READ-PERF-0 (2026-09-19):** a session-level read through a registered
  memory catalog counts data-file ranged reads into `iceberg_io_stats()`, and
  `reset_iceberg_io_stats()` zeroes the set. pins: ice-read-perf-0/C-003
  It builds its table through the registered handle (`catalogs_snapshot().get`) and
  `refresh_catalog_provider`, because repark-core has no SQL door of its own.
- `metadata_cache_report.rs` — **ICE-CATALOG-CACHE-1 (2026-09-19):** `iceberg_metadata_cache_report()`
  carries evictions (three 40 KiB-property tables under `metadataCacheEntries=1`, loads checked
  for their own property) and the legacy `iceberg_metadata_cache_stats()` triple agrees with it;
  a disabled cache reports `None` on both; two built sessions hold distinct caches; a source pin
  reads `session.rs::register_catalog_spec` and requires both AWS builders to receive
  `&iceberg_caches::caches_of(&self.catalogs)`. pins: ice-catalog-cache-1/C-002, C-004, C-006,
  C-007
- `footer_cache_report.rs` — **ICE-FOOTER-CACHE-1 (2026-09-19):** the session door for the footer
  cache. A default session reports zeroed stats, then after a cold and a warm scan of a
  memory-catalog table: zero warm data-file footer reads, hits and misses counted, `fetches`
  equal to the cold scan's footer reads. `footerCacheBytes = 0` reports `None` and re-reads every
  footer; a bad value on the alias fails `build()` naming both spellings. Two sessions hold
  distinct caches (`Arc::ptr_eq`) and a second session adopting the first one's warm table still
  reads footers with zero hits. pins: ice-footer-cache-1/C-001, C-003, C-006, C-007
- `namespace_create.rs` — `create_namespace` location-guard pins (G-6 Q1 / R-6).
- `nlj_tight_pool.rs` — **NEVER-OOM-PANIC-1 (2026-09-16):** the tight-pool nested-loop-join
  loop pin. The plan shape is guarded (`NestedLoopJoinExec` in `EXPLAIN`), every iteration
  runs under an 8 MiB pool with 4 partitions and proves its own tightness (the pool recorded
  a refusal).
  **Round 2 (2026-09-16):** two pins of three iterations — the INNER join asserts spilled
  values (`count(*)` 2016, `sum(id)` 41664, or the typed refusal), the LEFT join asserts
  the typed `Resources exhausted … fair(` refusal (the fallback DataFusion documents as
  unsafe must not emit rows), never a panic payload. The shape guard and the refusal-shape
  assertion are helpers shared by both pins.
  pins: never-oom-panic-1/C-003, C-006, C-012
- `a13.rs` — `file://` warehouse fallback-root pin.
- `pool_refusals.rs` — **H3-SPILL-RESIDUE-1 (2026-09-06):** the wiring pins. A bounded
  `build()` installs a pool that still reports `MemoryLimit::Finite` and now carries a refusal
  log that starts at zero and counts the session's own refusal; `memory_limit_bytes(0)` installs
  no log, so the containment cannot fire on an unbounded session. Two more hold the SET path:
  a runtime resize keeps the very same log (`Arc::ptr_eq`) and the new pool records into it,
  and a runtime `= '0'` drops the log with the pool.
  pins: h3-spill-residue-1/C-002
- `cache_budget.rs` — **EAGER-BUDGET-1 step 1 (2026-09-13):** D-2 pins. Two cache views
  registered over the same `RecordBatch` count one buffer set; a sliced array counts its
  parent's buffer once (dedupe is `Buffer::data_ptr()`, the allocation base — `as_ptr()`
  double-counts because arrow-58 slices the `Buffer` itself); no view answers 0; a dropped
  view stops counting; `user_view` and `__repark_ckpt_*` names are ignored. One function-level
  pin holds the reuse contract: a second `distinct_buffer_bytes` call over the same batches
  with the same pointer set returns 0.
  pins: eager-budget-1/C-002, C-003
  **EAGER-BUDGET-1 step 2 (2026-09-13):** D-1/D-3/D-4 admission pins. A three-batch source
  under a one-batch budget refuses at batch two with the tag, budget, `retained`, and
  `admitted` in the message — `admitted` lands strictly below the unbudgeted result, so the
  stream was dropped mid-collection and nothing registered. A scan over a live cache view
  admits zero new bytes (shared buffers seed `seen`) and leaves `retained` unchanged, while a
  fresh-buffered frame under `budget = retained` refuses naming that retained figure.
  `max_bytes` keeps its legacy message and registers nothing; an admitted cache registers and
  its `retained` equals the admitted distinct bytes.
  **Review round (2026-09-13, R12b-D-4):** `max_bytes_measures_this_result_even_when_buffers_are_shared`
  pins the per-result metric — a scan of a live cache view under `(max_bytes=1,
  max_total_bytes=u64::MAX)` refuses with this result's `get_array_memory_size` integer even
  though every buffer is shared.
  pins: eager-budget-1/C-005, C-007, C-008
- `ordered_cache.rs` — **TA-SERIES S1 (2026-10-04):** ordered-cache registration pins.
  A sorted plan registers one batch with the declared key (`ORDER BY id` asc,
  `ORDER BY id DESC NULLS LAST` desc); a declared-sorted multi-batch source concats
  to one sorted batch with the declared key; an unsorted plan keeps its batch count
  with no declared order; an exact-`max_bytes` budget still materializes but keeps
  the split batches with no declared order; an empty sorted plan stores no rows; a
  sort-then-filter plan that fans out to 16 partitions (the sort kept with a
  pass-through LIMIT, else the optimizer drops a limit-less sort) keeps split batches
  with no declared order and its window ranks still follow key order.
  pins: ta-series-s1/P-S1-3, P-S1-4, P-S1-6
  **S1 FOLD V944-1/V944-2 (2026-10-05):** `sorted_cache_rematerialised_large_keeps_order`
  re-materialises a sorted 1M-row cache (`ORDER BY` then `SELECT`, `batch_size=8192`, so
  the second plan streams 123 slices of one buffer in one partition) and pins one stored
  batch with the declared ascending key; restoring the per-batch size sum reds it.
  `sorted_cache_over_session_total_keeps_todays_path` pins that a session-total budget of
  `2.5 * retained` still admits the materialize but keeps split batches with no declared
  order; dropping the session-total check reds it.
  pins: ta-series-s1/P-S1-7, P-S1-8
- `commit_unknown.rs` — **ICE-COMMIT-UNKNOWN-1 (2026-09-14):** `engine_err` classification
  pins for the ambiguous-commit path — the stamped `CommitStateUnknownError` wrapper maps to
  `Error::CommitStateUnknown` carrying the minted `operation_id`, a bare iceberg
  `CommitStateUnknown` kind maps to the same variant with `None`, and the definite kinds
  (`CatalogCommitConflicts` included) stay in the `Error::Iceberg` base bucket. Mutation:
  fold the stamped arm and the kind arm back to `Error::Iceberg` and both classification
  pins red; the repark-common routing pin and the repark-python `to_py_err` pin red under
  the matching `exception_class` → `Base` leg of the same mutant.
  pins: ice-commit-unknown-1/C-001, C-004, C-007
- `window_rescan.rs` — **WIN-SLIDE-1 (2026-09-04):** six capability pins for the
  `sliding_frame_rescan` rule in [../df_guards/window_rescan.rs](../df_guards/window_rescan.rs). The throwaway
  `winslide_probe_sum` UDAF exists only here: it has no `retract_batch`, so it proves the fallback
  fires on an aggregate the rule has never heard of, and its `default_value` is the sentinel
  `-1.0`, so the empty-frame pin distinguishes "fresh accumulator" from "aggregate default".
  Mutation: make the rule probe `accumulator` instead of `create_sliding_accumulator` and
  `a_retractable_aggregate_keeps_datafusions_sliding_accumulator` reds.
  pins: win-slide-1/C-005, C-006
- `subquery.rs` — **DF-SUBQUERY-1 (2026-09-15):** plan-level pins for the three
  [../df_guards/subquery.rs](../df_guards/subquery.rs) rules — the scalar guard wraps a
  non-singleton subplan in `__repark_single_row` and leaves a zero-group `count(*)`
  aggregate untouched (count-bug compensation stays native), the lateral hoist lifts a
  `Subquery`-wrapped right projection onto the left input and refuses a correlated
  `Unnest`, and the EXISTS-in-projection rewrite keeps the plan's field name.
  **Round-3 (2026-09-16):** the hoist keeps a `SubqueryAlias` qualifier on its
  lifted outputs (`t.dbl` resolves in the rewritten schema, a still-correlated
  right keeps its `Subquery` marker under the alias), and the scalar guard
  strips a correlated `LIMIT 1` into `__repark_any_row` while an uncorrelated
  `LIMIT 1` stays untouched.
  pins: df-subquery-1/C-001, C-002, C-004
- `session_catalog.rs` — **CATALOG-1 (2026-09-26):** the auto-catalog decision, the fresh
  current catalog with a configured block, `spark.sql.defaultCatalog` at build, the missing
  default's `CATALOG_NOT_FOUND`, the USE pin against `apply_default_catalog`, the refused
  kinds on both doors (builds quietly, `table_exists` raises, the long form replaces), and
  the opt-in making the memory type a catalog on both doors.
  pins: catalog-1/C-001, C-002, C-003, C-004, C-006, C-007, C-008

## Pointers

- Up: [../map.md](../map.md)
