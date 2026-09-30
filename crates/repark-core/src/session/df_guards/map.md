# map — repark-core/src/session/df_guards

## Purpose

DataFusion 54.1 guards that are too large to live inside
[../df_guards.rs](../df_guards.rs). That file owns the two small guards (a config default and a
wrapped optimizer rule) and declares this directory.

## Contents

- [window_rescan.rs](window_rescan.rs) — **WIN-SLIDE-1 (2026-09-04):** the `sliding_frame_rescan` analyzer rule.
  Its design note, the DataFusion contracts it reads, and the routes it does not take are in
  [../map.md](../map.md); its pins are `../tests/window_rescan.rs` and
  `python/repark/tests/test_win_slide_1.py`.
  pins: win-slide-1/C-001, C-005
- `attr_id.rs` — **ATTR-ID-1 S1 (2026-09-30):** attribute identity as a function of the plan.
  Every output field of a DataFrame plan carries one attribute id in its field metadata under
  `repark.attr`; the work order is `/tmp/oc-worker/direct/wo/attr-id-1-design.md` §3.
  `AttrId` is `a` plus 12 hex digits from a per-process counter (`NEXT_ATTR`, the
  `TEMP_VIEW_SEQ` pattern of `metadata_columns.rs`), never derived from a name, a position or a
  plan id. `stamp(plan)` looks only at the root and is idempotent: a Projection root keeps a
  column (or an alias chain over a column) whose field already carries an id, keeps any alias
  whose own metadata carries one, and gives every other expression a fresh id through
  `Alias::with_metadata` (a bare expression gets an alias of its own qualified name) — so a cast,
  which copies its source id in DataFusion, still gets a fresh one. Any other root keeps a fully
  stamped schema as it is and otherwise adds one pass-through Projection that sets an id only
  where one is missing, and every other root mints. A Union root instead takes the first
  input's id at every position, Spark's rule: DataFusion intersects the ids of the inputs that
  carry a column, so it drops an id where the inputs differ and keeps a later input's id where
  the first lacks the column (`union_by_name` with a missing column). The first input's ids
  are read from its expressions when it is a Projection (an alias's own id, else the id of the
  column under the aliases), because DataFusion's by-name wrapper Projection carries the
  union's schema; a position with no such id (the wrapper's `NULL AS c`) mints. `attribute_ids(schema)` reads the ids by position.
  `remint_join_collisions(plan, left_width)` gives every right-side id that also appears on the
  left one fresh id (right-side twins of one attribute stay twins; a left width past the field
  count is an internal error). `resolve(schema, written, qualifier, rule, displays)` collects
  the positions whose display name matches `written` under `rule` (and whose relation matches
  a written qualifier through `same_relation`, which moved here from `case_bind.rs` so both
  files share it) and answers `Bound(hits)` for one distinct id, `Ambiguous(hits)` for more and
  `Missing` for none; a hit without an id and a display count that differs from the field count
  are internal errors, never a wildcard. The facade does not call any of it yet (S2 stamps
  every spawned frame). Pins: `../tests/attr_id.rs`. pins: attr-id-1/C-002, C-003, C-004
  **ATTR-ID-1 S2 (2026-09-30):** `strip(plan)` returns the plan unchanged when no root
  output field carries the key, else rebuilds the root with the key removed from every
  field (a Projection root keeps its expressions under an explicit cleaned schema via
  `try_new_with_schema`; any other root gains one pass-through Projection with a
  cleaned schema). Names, types, nullability, qualifiers, and schema-level metadata are
  preserved. Pins: `../tests/attr_id.rs`. pins: attr-id-1/C-009
  **ATTR-ID-1 S2 fix (2026-09-30):** `stamp` only touches relation roots
  (`plan_is_relation`: the 16 relation variants, plus an `EmptyRelation` with fields —
  a field-less one is a planned `DROP TABLE`). Statement roots (`Explain`, `Analyze`,
  `Ddl`, `Dml`, `Copy`, `DescribeTable`, `Statement`, `Extension`) pass through
  unchanged, since wrapping one breaks it (`Explain` must stay root, DML must stay a
  write). Pins: `../tests/attr_id.rs`. pins: attr-id-1/C-007, C-014
- `case_bind.rs` — **U11-EDGE-1 (2026-09-26):** `bind_case_insensitive`, run first by
  `subquery.rs`'s `resolve_bound_expr` (the DataFrame door's one binding hook). An
  unqualified column the frame schema does not hold exactly binds to the single field that
  matches it case-insensitively; an exact hit, a case twin, or a qualified column stays as
  it is. DataFusion's `col()` folds `F.col("ID")` to `id`, so without it a spelled SQL
  output (`SELECT ID` → `ID`, `SELECT id AS Id` → `Id`) or a `createDataFrame` column `Id`
  could not be filtered or projected by `F.col`. Live Spark answers all of these
  (`target/probe-u11-edge-1/spark_r3.json`). Rust pins in the file's own test module.
  pins: u11-edge-1/C-015
  **Round 2 (2026-09-26, V-001..V-004):** the file is the DataFrame door's one name binder
  (`pub mod`, re-exported as `repark_core::frame_names`). Rule, under the default
  `caseSensitive=false` (the door does not read the setting, like the round-1 hook): an exact
  name wins; otherwise the single case-insensitive match binds. A qualified column binds the
  same way within the fields whose relation matches the written qualifier part by part from
  the right (`t.id` → `(t, ID)`, `x.id` stays unbound). An alias that spells the qualified
  column (`F.col("t.ID")` arrives as `t.id AS "t.ID"`) is renamed to the written segment
  (`ID`), as Spark names it. `bind_projection_expr` (the select path) keeps the written
  spelling of a bare column it rebinds (`F.col("t.id")` on `ID` → `id`). `drop_named_columns`
  drops every case-insensitive match of each name (a name with none falls back to
  DataFusion's parse, so an absent name stays a no-op); `join_on_named_keys` binds each key on
  each side, joins on the bound columns and keeps one key column per folded key (semi/anti
  keep the left columns); `union_by_folded_name` respells the right frame's fields to the
  left spelling, refuses a strict mismatch with the facade's former text (`Union can only be
  performed … mismatched columns: [...]`, now listing folded mismatches only) and unions by
  name. Spark shapes: `target/probe-u11-edge-1/vx-spark.json`, `vx2-spark.json` (the
  verifier's probes). Rust pins in the file's test module:
  `qualified_reference_binds_through_its_relation`, `qualified_alias_names_the_written_segment`,
  `projection_keeps_the_written_spelling`, `drop_removes_every_folded_match`,
  `join_binds_each_side_and_keeps_one_key`, `union_respells_the_right_and_refuses_a_mismatch`;
  the round-1 `exact_ambiguous_and_qualified_references_stay` became
  `exact_and_ambiguous_references_stay` (a qualified exact hit stays).
  pins: u11-edge-1/C-017, C-018, C-019, C-020
  **Round 4 (2026-09-26, verifier round 2 V-001/V-003):** `drop_named_columns(frame, names,
  references)` takes the string targets and the Column targets apart. A string matches a field
  by its whole text ignoring case (`drop("t.ID")` matches no field); a Column target parses with
  `Column::from_qualified_name_ignore_case` and binds through `case_hits`, the one match rule
  select and filter use (a qualified target narrows by `same_relation` from the right, so
  `F.col("t.ID")` drops `(t, ID)` and `F.col("b.id")` drops `(b, ID)`). A target matching no
  field is a no-op on both, as Spark answers (`drop("t.ID")`, `drop("u.id")`,
  `drop(F.col("u.id"))`). Spark shapes: `target/probe-u11-edge-1/vz-spark.json` (`q_drop_*`,
  `j_drop_*`). Rust pin `qualified_drop_binds_through_its_relation`. pins: u11-edge-1/C-022
  **Round 4 (2026-09-26, verifier round 2 V-002/V-008):** the binder's final rule, under
  `caseSensitive=false`: a column reference collects every field whose name matches it ignoring
  case (`case_hits`; a qualified reference first narrows to the fields whose relation matches the
  written qualifier part by part from the right). Two or more hits refuse Spark's
  `[AMBIGUOUS_REFERENCE] Reference <ref> is ambiguous, could be: [<candidates>]. SQLSTATE: 42704`
  — an exact spelling among them does not win; one hit binds (an exact one stays as written);
  none leaves the column for DataFusion's own error. `ambiguous_reference` renders the reference
  as written and each candidate as its relation parts plus the reference's spelling, backticked
  and sorted the way Spark sorts the list (measured: `[`a`.`id`, `b`.`id`]` for either frame
  order, `[`id`, `sc`.`ns`.`t_vz_1`.`id`]` with the unqualified side first); a RePark scratch
  relation (`_repark_*`, `__repark_*`) renders unqualified, as Spark has no relation there
  (`createDataFrame` twins → `[`id`, `id`]`). `refuse_ambiguous_condition` applies the rule to the
  bare identifiers of a condition join's rewritten ON text across both sides (origin-qualified
  tokens are compound and skipped), and `requalify_join_sides` re-projects a condition join's
  output under each side's own relations so later references see Spark's candidates, not the
  scratch views; it keeps the join as is when the field counts differ or the re-projection does
  not plan. The error is a DataFusion `Plan` error, so the facade raises `AnalysisException` with
  the `Error during planning: ` prefix the SQL door's L-08 refusal carries. The kept
  `exact_and_ambiguous_references_stay` now asserts the three refusals (V-008). Rust pins:
  `exact_and_ambiguous_references_stay`, `ambiguous_candidates_render_sorted_like_spark`,
  `unqualified_and_catalog_candidates_render_like_spark`,
  `requalified_join_carries_each_side_relation`. Spark shapes:
  `target/probe-u11-edge-1/vz-spark.json`, `vz2-spark.json`. pins: u11-edge-1/C-023
  **Round 5 (2026-09-26, verifier round 3 V-001..V-004):** the ambiguity rule is for references
  the user wrote; Spark resolves the facade's own re-projections and origin Columns by
  attribute. `attribute_reference(name)` builds an unqualified `Expr::Column` for an exact engine
  field and marks it with a sentinel span (`ATTRIBUTE_MARK`, line and column `u64::MAX`, a
  location the SQL parser never produces; `Spans` takes no part in `Column` equality or hashing,
  so the mark changes no plan). `bind_case_insensitive` leaves a marked column exactly as held —
  never folded, never refused — and the mark survives aliases, casts and compounds because it
  rides the column node. `drop_named_columns(frame, names, references, attributes)` drops
  `attributes` by exact field name, and a Column target (`references`) with two or more hits
  refuses with the same `ambiguous_reference` text select uses (`drop(F.col("id"))` on twins →
  `[`id`, `id`]`, on the self-join → `[`a`.`id`, `b`.`id`]`); string targets keep dropping every
  folded twin. Rust pins `attribute_reference_binds_exactly_where_a_written_one_refuses`,
  `attribute_drop_is_exact_and_a_two_hit_reference_refuses`. Spark shapes:
  `target/probe-u11-edge-1/vw/fold-spark.json`, `fold2-spark.json`. pins: u11-edge-1/C-024,
  C-025, C-026
  Round 6 (2026-09-26, V-001): `with_attribute_copies(frame)` re-projects every held column and
  adds one exact copy per uniquely named field, named `attribute_copy_name(field)`
  (`__repark_attr_` plus the name's bytes in hex, so case twins never collide); the facade's SQL
  select route reads origin Columns through those copies, so a compound over an origin Column
  keeps its exact binding through arithmetic, cast, alias, `when`, `isin` and comparison.
  `is_scratch_relation` names the `_repark_*` / `__repark_*` relations every ambiguity renderer
  leaves unqualified. Rust pin (in `../../column_resolution/tests.rs`)
  `attribute_copies_bind_case_twins_exactly_and_scratch_relations_render_unqualified`.
  pins: u11-edge-1/C-027
  Round 7 (2026-09-26, V-003): `attribute_copy_name_in(schema, name)` spells the copy
  `attribute_copy_name(name)` and appends `_` until no field of `schema` carries it; the hex
  spelling holds no `_`, so two copies never meet. `with_attribute_copies` names every copy
  through it, so a user field literally named `__repark_attr_6964` keeps its value beside the
  `id` copy (`__repark_attr_6964_`) instead of refusing `Projections require unique expression
  names`. `is_scratch_relation` also filters the SQL door's `UNRESOLVED_COLUMN` suggestions
  (field names, V-002). Rust pin (in `../../column_resolution/tests.rs`)
  `attribute_copies_never_collide_and_suggestions_hide_scratch_names`. pins: u11-edge-1/C-029,
  C-030
  **CASESENS-1 S3 (2026-09-27):** the binder reads the session rule. `bind_case_insensitive`
  becomes `bind_names(expr, schema, rule)`: under `Exact` an exact hit binds, a case-only hit
  refuses Spark's `[UNRESOLVED_COLUMN.WITH_SUGGESTION]` naming the written reference with the
  frame's presented fields (scratch relations hidden), and anything else falls through to
  DataFusion; `IgnoreCase` keeps the round-4 rule. `resolve_written_names(schema, names, rule)`
  resolves describe's explicit columns to `(written, engine)` pairs with the same refusals
  (`Many` keeps the ambiguous text). `drop_named_columns` / `join_on_named_keys` /
  `union_by_folded_name` take the rule: a drop miss is a no-op, a join-key miss refuses
  Spark's `UNRESOLVED_USING_COLUMN_FOR_JOIN` text (side columns backticked, sorted by name),
  and a union name missing on the right refuses Spark's legacy
  `Cannot resolve column name …` text. `NameRule`/`NameHit` re-export through `frame_names`
  (the `repark-python` door's `repark-common` edge is dev-only, so the literal path does not
  resolve there — no new edge). Rust pins `exact_rule_refuses_a_case_only_match`,
  `ignore_case_rule_is_unchanged`, `frame_functions_follow_the_rule`.
  pins: casesens-1/C-009, C-010
  **Verifier fold (2026-09-28, VC-3):** `bind_projection_expr` aliases a bare
  top-level `Cast`/`TryCast` over a direct column child to the written child
  name, so `F.col(x).cast(...)` keeps the child name instead of leaking the
  qualified engine name (the all-lowercase leak goes with it). Pinned through
  the facade (`test_cast_of_a_column_keeps_the_written_child_name`) — the file
  sits 12 lines under the size ceiling, so no unit test lands here.
  **Re-verify (2026-09-28, RC-3):** the child-name lookup recurses through
  nested casts (`test_nested_cast_of_a_column_keeps_the_written_child_name`).
  pins: casesens-1/C-009
  **ATTR-ID-1 S1 (2026-09-30):** one `pub use` block re-exports `AttrId`, `Resolution`,
  `attribute_ids`, `remint_join_collisions`, `resolve` and `stamp` from `attr_id.rs`, so they
  leave through `frame_names`; `same_relation` moved to `attr_id.rs` and is imported back, so
  the file shrinks 1000 → 990. pins: attr-id-1/C-002
  **ATTR-ID-1 S2 (2026-09-30):** `join_on_named_keys` (USING) counts the kept right-side
  output positions while building the key-dedup projection and runs the S1
  `remint_join_collisions` over the projected join split at that count, so a self-join
  keeps the left ids and re-mints every colliding right id (semi/anti return before the
  projection and need none). `strip` joins the `frame_names` re-export. 990 → 997.
  pins: attr-id-1/C-011, C-009
- `subquery.rs` — **DF-SUBQUERY-1 (2026-09-15):** the subquery machinery — outer-reference
  scope resolution (`resolve_bound_expr` / `resolve_scoped_expr` /
  `resolve_subquery_plan`, innermost-first so an unqualified `col.outer()` binds inside
  the subquery like Spark classic), the `__repark_single_row` guard UDAF that turns a
  multi-row scalar subplan into the `SCALAR_SUBQUERY_TOO_MANY_ROWS` execution error
  and its `__repark_any_row` sibling (strict=false — first-row pick for a correlated
  `LIMIT 1`, whose `Limit` is stripped before wrapping because `ScalarSubqueryToJoin`
  cannot pull one up; uncorrelated `LIMIT 1` stays untouched as already-singleton),
  and three optimizer rules: `repark_scalar_subquery_guard` (wraps non-singleton scalar
  subplans in the guard aggregate so `ScalarSubqueryToJoin`'s count-bug compensation
  reads a non-`count` aggregate and emits NULL), `repark_lateral_projection_hoist`
  (lifts `LateralJoin` right-side projections of outer refs onto the left input,
  refusing `CORRELATED_REFERENCE` under generators/non-Filter parents, and unwraps
  uncorrelated `Subquery` arms — every right-input mutation rebuilds through
  `Join::try_new` so the cached join schema stays honest; a `SubqueryAlias` on the
  right is carried through — hoisted outputs are requalified to it, inner-qualifier
  column refs are rewritten `d.x`→`t.x`, join `on`/`filter` refs to hoisted columns
  are inlined, and a still-correlated right keeps its `Subquery` marker under the
  alias), and
  `repark_projection_exists` (a projection `Exists`/`Not(Exists)` becomes a
  `count(*)`-comparison boolean that keeps the plan's field name).
  Pins: `../tests/subquery.rs` and `python/repark/tests/test_df_subquery_1.py`.
  This module is the ledger's rust-first roll-call home: all scope resolution, the
  single-row guard, the exists rewrite, the projection hoist, and the Unnest
  refusal live here in repark-core.
  pins: df-subquery-1/C-001, C-002, C-003, C-004, C-008, C-009
  **CASESENS-1 S3 (2026-09-27):** `resolve_bound_expr` keeps its signature (it binds
  `IgnoreCase`) and gains the sibling `resolve_bound_expr_with(expr, schema, rule)`,
  re-exported through `frame_names` (`session.rs` sits exactly at its ceiling, so the root
  re-export cannot grow). pins: casesens-1/C-009

## Pointers

- Up: [../map.md](../map.md)
