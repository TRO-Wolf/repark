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
  `repark.attr`; the design and its rulings are recorded in the unit ledger,
  `task/ledgers/staging/attr-id-1-ledger.md`.
  `AttrId` is `a` plus 12 hex digits from a per-process counter (`NEXT_ATTR`, the
  `TEMP_VIEW_SEQ` pattern of `metadata_columns.rs`), never derived from a name, a position or a
  plan id. `stamp(plan)` looks only at the root and is idempotent: a Projection root keeps a
  column (or an alias chain over a column) whose field already carries an id, keeps any alias
  whose own metadata carries one, and gives every other expression a fresh id through
  `Alias::with_metadata` (a bare expression gets an alias of its own qualified name) — so a cast,
  which copies its source id in DataFusion, still gets a fresh one. Any other root keeps a fully
  stamped schema as it is and otherwise adds one pass-through Projection that sets an id only
  where one is missing (S1b below narrows "missing" for Aggregate and Window roots). A Union root instead takes the first
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
  **ATTR-ID-1 S2 (2026-09-30):** `strip(plan)` rebuilds the plan bottom-up with the key
  removed from every stored schema and every embedded `Alias` (nodes rebuild through
  their own `try_new`, so the optimizer cannot resurrect the key by recomputing; unions
  retry strict, by-name, then loose because the node does not record its constructor).
  `Values`/`EmptyRelation`/`TableScan` leaves are clean by construction and fail loud
  when keyed; statements and `Extension` pass through. `strip_record_batches` gives
  every `MemTable` temp view a clean schema and clean batches at creation. Pins:
  `../tests/attr_id.rs`. pins: attr-id-1/C-009
  **ATTR-ID-1 S2 fix (2026-09-30):** `stamp` only touches relation roots
  (`plan_is_relation`: the 16 relation variants, plus an `EmptyRelation` with fields —
  a field-less one is a planned `DROP TABLE`). Statement roots (`Explain`, `Analyze`,
  `Ddl`, `Dml`, `Copy`, `DescribeTable`, `Statement`, `Extension`) pass through
  unchanged, since wrapping one breaks it (`Explain` must stay root, DML must stay a
  write). Pins: `../tests/attr_id.rs`. pins: attr-id-1/C-007, C-014
  **ATTR-ID-1 S2b (2026-09-30):** ids live on unoptimized and analyzed plans only.
  `StripAttributeIds` (`repark_strip_attribute_ids`) is an optimizer rule that owns its
  recursion: bottom-up, subqueries included, on every node whose schema or expressions carry
  the key it drops the key from each `Alias`'s metadata and each outer-reference field (an
  alias left with no metadata over a column of its own name and relation becomes that bare
  column again, so the stamp's pass-through Projections fall to `OptimizeProjections` and the
  optimized plan is the unstamped twin's), then
  recomputes the node's schema through DataFusion's own `recompute_schema` (a `Union` takes
  the intersection of its inputs' field metadata, DataFusion's union rule, because
  `recompute_schema` keeps a same-width union's schema). Why: DataFusion 54.1's physical
  planner drops an `Alias`'s metadata unless the aliased expression is a literal, so a stamped
  pass-through Projection over a clean source gives a keyed logical field a clean physical
  twin, and the Aggregate planner's schema check (it compares field metadata) fails. After the
  rule, logical metadata again derives only from the sources, as the physical plan's does, so
  the check holds unweakened; a source that carries a foreign `repark.attr` keeps it on both
  sides. It is not an analyzer rule because the Spark SQL door analyzes eagerly
  (`repark_functions::analyze_eagerly`) and keeps the analyzed plan as the frame, whose ids the
  facade's join shape needs. Name resolution reads `DataFrame::schema()`, which the optimizer
  never touches. Pins: `../tests/attr_id_seam.rs`. pins: attr-id-1/C-015, C-016
  **PERF-ATTR-STAMP-2 O-1 (2026-10-03, ruling R-O1-1, option A):** `strip_for_execution(plan)`
  is the rule's own collapsing walk (`drop_node_ids`, bottom-up, subqueries included) as a
  plain function, re-exported through `frame_names`. The binding's per-handle twin
  (`PyDataFrame::executable`) calls it, so the native doors that execute analyze and optimize
  an id-free plan. It is not the non-collapsing `strip`, which keeps `col AS col` wrappers in
  the optimized plan. `StripAttributeIds` stays the first optimizer rule as the backstop for
  every other path (the SQL door, view registration, the join door's planning), and is still
  never an analyzer rule: the O-1 sketch's analyzer placement (E1) was measured to strip the
  eager SQL door's frames and break every condition join and `crossJoin` with the frame-lineage
  internal error (`/tmp/oc-worker/direct/wo/attr-id-1/stamp2-o1/probes/`). Pins:
  `../tests/attr_id_seam.rs`
  (`a_stamped_plan_reaches_the_optimizer_backstop_and_its_collapsed_twin_plans_the_same`,
  `every_session_strips_ids_first_in_its_optimizer_and_never_in_its_analyzer`,
  `an_analyzed_plan_keeps_its_ids_for_the_eager_sql_door`). pins: attr-id-1/C-052
  **O-1 fold V-1 (2026-10-04):** `strip_schema_ids(schema)` drops the key from every top-level
  field of an Arrow schema and returns the same `Arc` when no field carries it;
  `strip_record_batches` now uses it. The binding applies it to the analyzed export schema, so a
  provider schema that carries ids (a SQL-defined temp view, which neither the backstop nor
  `drop_node_ids` can clean) never reaches an export. pins: attr-id-1/C-053
  **ATTR-ID-1 S1b (2026-09-30):** identity is decided by structure, never by copied metadata
  (DataFusion copies the argument's metadata onto `first_value`, `last_value`, `lag`, `lead`,
  `nth_value`, a cast and a negation). `computed_outputs(node)` answers, by position, which
  outputs a same-op node computed: it skips a Filter/Sort/Limit chain (HAVING, ORDER BY), then
  a Window's outputs past its input width are computed and its input positions take the
  input's answer, and an Aggregate's group keys are computed unless the key (aliases stripped)
  is a `Column` over a non-computed input position, while the grouping id and every aggregate
  value are computed; any other node computes nothing. An Aggregate or Window root (or a
  Filter/Sort/Limit root over one) mints every computed position; a Projection root keeps a
  column only when it references a non-computed input position. The classification reads a
  same-op node below the root but never rewrites it: the ids are set on the root Projection or
  the pass-through Projection `stamp` adds. It needs no metadata: a Window or Aggregate that
  was itself a stamped frame's root sits under that stamp's pass-through Projection, except an
  Aggregate with no computed output, where both readings agree. A second `stamp` keeps every
  alias whose own metadata carries a native id, so the stamp stays idempotent. Ids are
  `a` + a 16-hex per-process prefix (a `RandomState` hash of the process id) + a 12-hex
  counter; `AttrId::is_native` tells them from ids another process wrote, and `stamp` never
  keeps a foreign id: a source that carries one is re-minted once. Pins:
  `../tests/attr_id_fresh.rs`, `../tests/attr_id_verify.rs`.
  pins: attr-id-1/C-017, C-018, C-019, C-020
  **ATTR-ID-1 S3a timing cut (2026-09-30):** `plan_is_stamped(plan)` answers by reference
  whether `stamp` would return the plan unchanged, so the binding can keep an already-stamped
  `PyDataFrame` handle (and its cached analyzed schema) instead of cloning the plan on every
  spawn. It mirrors the mint conditions exactly: a Projection root whose every expression
  carries its own native id or inherits one over a non-computed input position, a Union root
  whose every position already carries the first input's id, any other relation root with no
  computed position and no missing or foreign id; a statement root reads stamped, and a
  classification error reads unstamped so `stamp` surfaces it. A bare presence check would
  misread a fresh computed node whose copied ids are all native (a `lag` output keeps its
  argument's native id until `stamp` mints it), so the predicate reuses `computed_outputs`,
  `own_id` and `first_input_ids` rather than re-deciding. Pins: `../tests/attr_id.rs`.
  pins: attr-id-1/C-023
  **ATTR-ID-1 S3a (2026-09-30):** `alias_with_fresh_id(expr, name)` builds the `Alias`
  with a freshly minted id in its own metadata, the constructor a user `alias()` uses;
  a second stamp keeps it (the S1b idempotence rule). It joins the `frame_names`
  re-export. `first_input_ids` reads a Projection input's ids through `chain_id`
  (outermost-with-metadata down the alias chain): a user alias under a plain machinery
  alias keeps its own id instead of the source column's, so a union of aliased twins
  keeps distinct ids. The output schema is not the source: DataFusion's `union_by_name`
  pad projection grafts the other side's field (with its id) into the padded side's
  schema. Pins: `../tests/attr_id.rs` `union_reads_inner_alias_ids_through_plain_machinery_aliases`.
  pins: attr-id-1/C-024
  **ATTR-ID-1 S3e (2026-10-01):** `resolve` takes the facade-held qualifiers
  (attribute id to qualifier names) and matches a written qualifier against the
  plan relation or a facade entry under the live rule, so join sides and
  `alias()` frames bind through the one id rule; a using-key union binds
  either side. Pins: `../tests/attr_id_s3e.rs`. pins: attr-id-1/C-039
  **ATTR-ID-1 V-4 (2026-10-02):** `copy_attribute_ids(plan, source)` carries the
  source schema's ids onto the plan by position (a width mismatch is an
  internal error; an already-carried plan returns unchanged), so cache and
  checkpoint scans keep the live frame's identity. Pins: `../tests/attr_id.rs`.
  pins: attr-id-1/C-045
  **ATTR-ID-1 SJ-1a (2026-10-02):** `remint_join_collisions` becomes
  `remint_shared(plan, left_width, shared)`, which re-mints every right-side id in
  `shared` (one fresh id per shared id, so right-side twins stay twins) and returns the
  plan with the old-to-new map; an empty map leaves the plan unchanged. The shared set
  is the caller's: self-join option A passes the lineage set (`frame_lineage::shared_ids`)
  from SJ-3, which also renews an id that is on the right but only in the left's
  lineage, not its output (Spark's `DeduplicateRelations`: `s = d.select('id');
  s.join(d, s.id == d.id).select(d.v)` refuses, `A_sel_only_id_left_sel_d_v`).
  `join_collisions(plan, left_width)` returns the right-side ids that also appear on the
  left, the old collision set, and every caller passes it today (the USING path here,
  `requalify_join_sides`, `remint_cross_collisions` and `lateral_join` in repark-python),
  so nothing user-visible moves. A left width past the field count stays an internal
  error in both. Pins: `../tests/attr_id.rs`, `../tests/attr_id_verify.rs`,
  `../tests/frame_lineage.rs`.
  **ATTR-ID-1 SJ-1b (2026-10-02):** `AttrId::from_token(raw)` wraps the id text a
  reference token carries; it validates nothing, and an id no schema holds stays unbound
  (`self_join.rs` refuses it loudly).
  **ATTR-ID-1 SJ-3 (2026-10-02):** `remint_with_map(plan, left_width, remint)`
  applies exactly a caller-given map (`remint_shared` mints its map, then
  delegates); `join_on_named_keys` takes the two lineage nodes, re-mints over
  `shared_ids(left_node, right_outputs)`, and returns the plan with its `Join`
  node (semi/anti record the map with `emits_right=false` and skip the plan
  re-mint). Only `remint_cross_collisions` and `lateral_join` still pass the
  collision set. Pins: `../tests/frame_lineage.rs`.
- `attr_lineage.rs` — **ATTR-ID-1 S4 (2026-10-02):** projection-output lineage,
  a pure move out of `attr_id.rs` when that file passed the 1000-line ceiling.
  `projection_source_ids` maps each `Projection` output to its input attribute id
  through alias chains, single-column `coalesce` (`fillna`, both the built form and
  the optimizer's `CASE WHEN col IS NOT NULL THEN col ELSE lit` form), and
  single-column searched `CASE` (`replace`'s `WHEN col = lit THEN lit ELSE col`,
  with the type-group cast and const-foldable literal casts). Only these strict
  shapes resolve; anything else reads `None` so the caller keeps the old miss
  behavior. Re-exported through `frame_names`. Pins:
  `python/repark/tests/test_attr_id_1_s4.py` (fillna/replace/eqNullSafe/expression
  pins). pins: attr-id-1/C-040
  **CASESENS-2 port phase 2 (2026-10-03):** `sort_hits_meet_at_join(plan,
  positions)` follows output positions down through `Filter`, `Sort`, `Limit`,
  `Repartition`, `Distinct::All`, `SubqueryAlias` and plain column projections
  (a `Column` under any aliases), and answers whether two distinct positions
  reach one `Join`'s output. A computed expression or any other node answers
  `false`. Spark resolves a sort key that is ambiguous in the output through
  the unary children, so twins that meet at one join cannot resolve. Pin:
  `../tests/join_qualifiers.rs` (`sort_twins_from_two_join_positions_meet_at_the_join`).
  pins: casesens-2/C-012
  **Fold SM-2c round B (2026-10-06):** `project_input_is_join(plan)` answers
  whether the projection below the transparent wrappers reads directly from a
  join, sharing `sort_names::below_transparent`; the facade's sort route uses
  it to keep main's `AMBIGUOUS_REFERENCE` refusal for string keys over join
  inputs. pins: attr-id-1/C-070
- `frame_lineage.rs` — **ATTR-ID-1 SJ-1a (2026-10-02):** the lineage core for refusing
  ambiguous self-join references the way Spark Classic does (owner ruling 2026-10-02,
  option A; design sketch `attr-id-1-selfjoin-design.md` §2.1–§2.2). Spark tags every
  Dataset's plan with a dataset id and, after `DeduplicateRelations` renews the right
  side of a join, refuses a Column reference whose dataset occurs where its attribute
  was renewed into another id that the new operator can see
  (`DetectAmbiguousSelfJoin`, `_LEGACY_ERROR_TEMP_1182`). RePark has no exprIds, so it
  keeps the lineage itself:
  - `FrameId::mint()` is a per-process counter, Spark's `__dataset_id`. A node mints
    its id when it is built, after its inputs, so an ancestor's id is always smaller.
  - `FrameNode` is an immutable node in an `Arc` DAG: its id, its output attribute ids
    read from the frame's stamped schema (`FrameNode::{root, derived, set_op, join}`;
    an output without an id is an internal error, never a wildcard), its `FrameKind`
    (`Root`, `Derived(parent)`, `SetOp { first, others }`,
    `Join { left, right, remint, emits_right }`), and `renews`, true when this node or
    any node below it (set-operation inputs and join right sides included) is a join
    with a non-empty `remint`. It is computed once at construction, so a frame with no
    self-join below it skips every check on one branch.
  - `all_ids(node)` is the union of the outputs of every reachable node.
    `shared_ids(left, right_outputs)` is the right outputs found in `all_ids(left)`:
    the set `remint_shared` renews.
  - `AttrRef { attr, frame }` is one Column reference: the attribute id it holds and
    the frame it was bound on. SJ-1b's token parser will produce them.
  - `ambiguous(target, visible, refs)` is the detector, an explicit-stack depth-first
    walk of the target's lineage. It carries the chain of `remint` maps crossed on
    join right-side descents (an arena of links, so the walk allocates per join, not
    per node). At each node whose id is a reference's frame it applies the chain
    innermost first and records the reference when the image is a different id that
    `visible` holds. `SetOp.others` and the right side of a join that does not emit it
    (`emits_right` false: semi and anti) are not walked; a set operation outputs its
    first input's ids, so a frame met only in a later input is not ambiguous. The walk
    skips every node older than the oldest referenced frame and every node with no
    renewing join below it while the chain is empty, and memoises on (node, chain).
    It returns the reference indices in order, one per reference.
  - `renewed_absent(target, refs, outputs)` returns the references whose id is a key of
    some `remint` in the target's lineage and is not in `outputs`: the ids the re-mint
    removed, which §1.4 of the sketch refuses with `MISSING_ATTRIBUTES` under
    `failAmbiguousSelfJoin=false` (`K_off_shared_*`).

  Measured Spark verdicts the unit tests reproduce on hand-built DAGs
  (`/tmp/oc-worker/direct/wo/attr-id-1/selfjoin/sj1.spark.json` and `sj4.spark.json`):
  refusals `A_inner_sel_f_v`, `A_sel_only_id_left_sel_d_v`, `A_wc_sel_w_v`,
  `K_on_union_first_input`, `G_eq3_cross_tok` (the condition's `f.id`, not `e.id`);
  answers `A_rev_left_sel_f`, `A_sibling_absent`, `A_wc_sel_w_id`, `F_left_semi_sel_f`,
  `K_on_union_second_input`. `K_on_union_second_input` unions an anonymous
  `d.filter(...)`, so it answers by sibling absence; the lane measured the named form on
  live Spark 4.1.2 (`/tmp/oc-worker/direct/wo/attr-id-1/sj-1a/probe/sj1a.spark.json`):
  `d.join(d.union(f), …).select(f.v)` answers `[[10],[20],[20],[30],[30]]`
  (`K_on_union_named_second_input`), while `.select(un.v)`, `.select(d.v)` and
  `d.join(f.union(d), …).select(f.v)` refuse with 1182. That cell is the one that pins
  the opaque `SetOp.others`.

  `emits_right` changes no verdict on its own: a semi or anti join's renewed right ids
  never reach any frame's outputs, so `visible` (the target's outputs) cannot hold an
  image reached only through that side. It prunes the walk. Its mutation (M-A7) turns
  no test red, which the SJ-1a hand-back records. SJ-1b's condition check must build
  its provisional join node with the right side walked, since Spark checks a semi or
  anti join's own condition against the renewed right side (`F_left_anti_gt`).

  Nothing calls this module yet: SJ-1b adds the condition preparer and the bindings,
  and SJ-2 to SJ-4 wire the facade. Pins: `../tests/frame_lineage.rs`.
  **ATTR-ID-1 SJ-1b (2026-10-02):** `ambiguous_images(target, visible, refs)` is the same
  walk returning each hit with the first visible image it reached, so a refusal can name
  the column by the display at that image's position (Spark's `ambiguousAttrs`);
  `ambiguous` now maps it to indices, its verdicts unchanged. `FrameId::from_raw` reads a
  frame id back from a reference token (crate-private).
- `predicate_names.rs` — **CASESENS-2 port (2026-10-03):** the join-condition
  half of #881's `predicate_names.rs` (folds `15a7bb9d`, `b89a7d7f`, `3ac80920`),
  re-pointed at `attr_id::resolve`. `bind_condition_qualifiers(condition, left,
  right, rule, placeholder)` walks the parsed condition with #881's scope stack:
  a lambda pushes its parameters and pops them on leaving, subqueries are
  skipped, and a compound whose root is an in-scope parameter (by the session
  rule) or a reference placeholder is left alone (RC4-1, RC4-6, RC5-2). Every
  other `q.name[.field…]` (a compound identifier, or an identifier root with a
  dot chain) tries the widest qualifier first (up to three parts) and resolves
  `name` on each side with `resolve(schema, name, Some(q), rule, displays,
  qualifiers)`. One side `Bound` rewrites the qualifier and name to
  `` <side alias>.`<engine field>` `` and keeps the field chain; `Ambiguous` on a
  side refuses `AMBIGUOUS_REFERENCE` naming `` `q`.`name` `` and the qualified
  displays; both sides bound or neither leaves the text to the engine, as on
  main. `restore_placeholders` puts each side's rendered reference back into the
  rewritten tree. The filter-string half is not ported: the stack's
  `filter_quote.py` already owns alias qualifiers and lambda scope (S3b H-1).
  Pins: `../tests/join_qualifiers.rs`. pins: casesens-2/C-009
  **R-CS2P-1 (2026-10-03):** `fold_frame_qualifiers(predicate, schema, rule)`
  (re-exported from `case_bind.rs`) is the filter-string half for the
  insensitive door. It walks a parsed predicate with the same scope stack
  (lambda parameters shadow, subqueries are skipped) and rewrites each compound
  root, unquoted or backticked, that `same_relation` under `rule` matches to
  exactly one bare held qualifier holding the next name: the root becomes that
  held spelling, backticked, so DataFusion's identifier normalization keeps it.
  A root matching no qualifier, or two spellings, is left for the engine (halt
  rule 3). It returns whether anything changed; under `Exact` only a backticked
  exact root is a no-op and an unquoted exact root is re-quoted, which the
  facade never asks for. Pins: `../tests/join_qualifiers.rs`.
  pins: casesens-2/C-013
- `self_join.rs` — **ATTR-ID-1 SJ-1b (2026-10-02):** the self-join condition preparer, the
  post-join reference check and the refusal texts, all Rust (owner ruling 2026-10-02,
  option A: Spark Classic is the oracle, no "left wins" tie-break; sketch
  `attr-id-1-selfjoin-design.md` §1.1–§1.4, §2.2). No facade calls it yet: SJ-2 wires the
  token and the seam, SJ-3 the condition join, SJ-4 the post-join surfaces.
  - `parse_attr_refs(sql)` reads the reference tokens SJ-2 will render,
    `__REPARK_ATTR_<id>__F<frame>__<qualifiers>__` (the frame field sits before the
    greedy qualifier group, and the qualifier group ends at its last `__`, as the
    facade's regex does). Each token becomes the placeholder `__rp_ref_<n>` so the text
    parses, and its byte span on the original text is kept. Quoted spans (`'…'`, `"…"`,
    `` `…` ``, doubled-quote and backslash escapes) are text. A token without the frame
    field, a malformed token, or text already holding `__rp_ref_` is an internal error,
    never a guess (halt rule 5).
  - The condition is parsed with `sqlparser`'s `DatabricksDialect`, the parser
    `case_bind.rs` uses, and walked with `visit_expressions`. The walk records the
    placeholders inside a window function (`Function { over: Some(_) }`, never checked),
    and Spark's two exemptions from `DetectAmbiguousSelfJoin`'s root-`Join` branch: an
    equality (`=`, `<=>`, `IS NOT DISTINCT FROM`) of two references to the same id, each
    wrapped in any number of casts (Spark's recursive `AttrWithCast`; measured
    `X_self_eq_castcast` = 9, so the sketch's "one cast" is widened to Spark's), and, when
    the two inputs are the same frame, an equality of a reference and a foldable operand
    (no reference, no column name, no subquery, no nondeterministic function: `rand()`
    refuses, `1 + 1` and `upper('a')` do not; measured `X_self_eq_rand`,
    `X_self_eq_litexpr`, `X_self_lit_rev`).
  - `prepare_join_condition(cond_sql, left, right, names, rule, rules)` runs Spark's
    order over two `JoinSide`s (node, stamped schema, display names, view alias):
    1. `shared_ids` and one fresh id per shared id: the `remint` map returned with the
       condition, which SJ-3 must apply to the joined plan unchanged.
    2. `DeduplicateRelations`' own condition rewrite, read from the 4.1.2 bytecode (the
       `attrMap` filtered by `DONT_DEDUPLICATE_EXPRESSION_IF_EXPR_ID_IN_OUTPUT`, then
       `rewriteAttrs` on the join): a reference whose id the left does not output but
       the right outputs and renews takes the renewed id, so it binds right and is never
       ambiguous. Measured: `s.join(d, d.v > 15)` answers 6 with the check on and off
       (`X_on_cond_shared_v`, `X_off_cond_shared_v`, `X_on_cond_shared_fv`,
       `X_on_cond_shared_dv_f`, `X_on_cond_shared_eq`). The sketch's §1.3 omits this step.
    3. Detect, when `fail_ambiguous`: every reference outside an exemption and a window is
       walked by `ambiguous_images` from a provisional `Join` node whose right side is
       always walked (SJ-1a ruling 1: `F_left_anti_gt` refuses), with `visible` the left
       outputs plus the renewed right outputs. Hits refuse with
       `Refusal::SelfJoin { names }`, one name per occurrence in text order (Spark's
       multiplicity: `D_derived_eq_plus0` names `id, id`).
    4. Bind by id: the left outputs first, else the renewed right outputs, else
       `Refusal::Missing` with Spark's subclass (`operation` lists the missing names that
       match an input display under the session rule: `APPEAR_IN_OPERATION`, else
       `MISSING_FROM_INPUT`). A condition id missing from both sides belongs to a third
       frame, whose name the token does not carry, so it comes from the caller's `names`
       map; no name is an internal error.
    5. Rewrite, when `auto_resolve` and the two inputs' output ids intersect: each
       exempt equality of two bare references becomes `left.<name> = right.<name>`, the
       first operand to the left whatever its frame (`D_derived_eq_rev`), each side
       resolved by name with `resolve` under the session rule. No hit is
       `UNRESOLVED_COLUMN.WITH_SUGGESTION` with the side's displays as suggestions
       (`I_rewrite_name_missing`); two ids is `AMBIGUOUS_REFERENCE` `` `id` `` /
       `` [`id`, `id`] `` (`G_eq3_*`), both through `spark_error` and byte-equal to Spark.
    The text is spliced by span on the original condition, never re-rendered from the
    syntax tree: each placeholder becomes `<alias>.` + the backtick-quoted engine field,
    the quoting today's facade rewriter uses.
  - `check_refs(target, displays, sql_parts, rules)` is the post-join check (SJ-4): the
    references of every part, deduplicated by (frame, id) as Spark's `ColumnReference`
    set is, minus window ones, walked with the target's outputs visible. It runs nothing
    when `fail_ambiguous` is off or the target does not renew.
  - **ATTR-ID-1 SJ-4 (2026-10-02):** `check_refs` wires `renewed_absent` after
    the ambiguity walk (SJ-1a ruling 3 discharged: live Spark refuses the
    K shapes and their filter/order variants on every surface under both
    conf settings, so no per-surface switch; conf-on ambiguity still wins).
    `attr_token` also decodes the token's `__D` leaf display, which names the
    missing attributes (via `missing_refusal`) and backs the ambiguous-image
    name when the positional display is engine-flavored (crossJoin frames
    carry no display overlay; SJ-5 owns that path).
  - `self_join_message(names, config)` is the verbatim `_LEGACY_ERROR_TEMP_1182` template;
    `missing_message`, `missing_condition` and `quoted_names` render `MISSING_ATTRIBUTES`
    in the facade's one-line shape plus Spark's `SQLSTATE: XX000`. The operator renders
    as `!Join` (Spark prints the plan node with exprIds).
  The join type is not an input: `emits_right` is never a verdict input (SJ-1a ruling 2).
  Pins: `../tests/self_join.rs`; the bindings in `crates/repark-python/src/frame_lineage.rs`.
  **CASESENS-2 port (2026-10-03):** `JoinSide` carries the facade's
  `qualifiers` (attribute id to alias names, `None` when the frame holds none),
  and `prepare_join_condition` first hands the placeholder text to
  `predicate_names::bind_condition_qualifiers` (only when it holds a `.`). When
  that rewrites a free alias-qualified name, the final SQL is the rewritten tree
  with each placeholder restored to its rendered side text; otherwise the
  byte-span splice runs unchanged. Pins: `../tests/join_qualifiers.rs`.
  pins: casesens-2/C-009
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
  **ATTR-ID-1 S3a (2026-09-30):** `alias_with_fresh_id` joins the `frame_names`
  re-export beside `strip`. pins: attr-id-1/C-024
  **ATTR-ID-1 S3b (2026-10-01):** re-exports `SortShape`, `sort_shape`,
  `grandchild_key` and `bind_free_names` from `sort_names.rs`; `Hit`,
  `ambiguous_reference` and `unresolved_column` widen to `pub(crate)` for the
  walker. 999 lines. pins: attr-id-1/C-025
  **ATTR-ID-1 S3b H-1 (2026-10-01):** also re-exports
  `engine_field_is_unique` and `join_dup_below_wrappers` from `sort_names.rs`
  (one brace line). 1000 lines, at ceiling.
  pins: attr-id-1/C-026
  **ATTR-ID-1 S3e (2026-10-01):** re-exports `bind_qualified_free_refs`,
  `grandchild_qualified_key`, `join_output_sources` and
  `qualifier_star_positions` from `sort_names.rs`; `bind_names` widens to
  `pub(crate)` for the moved tests. The inline test module moves unchanged to
  `../tests/case_bind.rs`, so the file drops 1000 → 552.
  pins: attr-id-1/C-039
  **ATTR-ID-1 V-4 (2026-10-02):** `copy_attribute_ids` joins the `frame_names`
  re-export. pins: attr-id-1/C-045
  **ATTR-ID-1 SJ-1a (2026-10-02):** the re-export swaps `remint_join_collisions` for
  `remint_shared` and `join_collisions`, and adds `frame_lineage.rs`'s `AttrRef`,
  `FrameId`, `FrameKind`, `FrameNode`, `all_ids`, `ambiguous`, `renewed_absent` and
  `shared_ids`. `join_on_named_keys` (USING) passes `join_collisions` as the shared set,
  so its ids are unchanged.
  **ATTR-ID-1 SJ-1b (2026-10-02):** the re-export adds `ambiguous_images` and
  `self_join.rs`'s `AttrRefText`, `JoinSide`, `Prepared`, `PreparedCondition`, `Refusal`,
  `SELF_JOIN_CONDITION`, `SelfJoinRules`, `check_refs`, `missing_condition`,
  `missing_message`, `parse_attr_refs`, `prepare_join_condition`, `quoted_names` and
  `self_join_message`.
  **Fold SM-2 R6 (2026-10-06, R-R6-1):** `join_on_named_keys` keeps the
  left key physically on every join type, as on main; the `Full`/`Right`
  `coalesce(left key, right key)` from `d66de2e3` is reverted in this
  commit (it removed the left key from the merged plan and forced
  refusals on left-side references main answers Spark-exact), and
  `attr_id::with_id` is private again. Coalesced star and per-side key
  fields are the USING-PER-SIDE-KEYS-1 v1.5.3 follow-up.
  pins: attr-id-1/C-066
  **Fold SM-2c C-3 (2026-10-06):** `rename_output_fields(frame, names)`
  re-projects the frame positionally under the given names (one `select`;
  a length mismatch is a loud plan error) for the write/view registration
  boundary. pins: attr-id-1/C-067, C-068
- `sort_names.rs` — **ATTR-ID-1 S3b (2026-10-01):** the filter/sort free-name
  binder over the S1 `resolve`. `sort_shape` descends Filter/Sort/Limit/
  Repartition/Distinct/SubqueryAlias and transparent Projections (a passthrough,
  or a join-select whose every column carries a scratch relation) and answers
  `Project`, `Aggregate` or `Other`. `grandchild_key` resolves one written name
  against the join schema below a Projection root (`None` on any other root or
  with no join below). `bind_free_names` rewrites each unqualified `Column`
  (qualified tokens and subquery plans pass through): one id binds the engine
  field as an attribute reference, several refuse — `AMBIGUOUS_REFERENCE` on the
  filter path, the projection-input route on a Project sort (SM-2c round B
  below; the S3b oldest-id rule was wrong), `UNRESOLVED_COLUMN` on any other
  sort — and a miss passes through for the engine. No exact-preference anywhere:
  Spark refuses an exact spelling among folded rivals on both filter doors
  (`test_filter_predicate_rewrite.py`), and the S3a inline exact bind stays a
  select-only rule. Pins: `../tests/attr_id_s3b.rs`. pins: attr-id-1/C-025
  **ATTR-ID-1 S3b H-1 (2026-10-01):** `engine_field_is_unique` (one shared
  guard for select and filter) and `join_dup_below_wrappers`, which descends
  single-input nodes and id-preserving Projections (DataFusion's
  `SubqueryAlias::try_new` dedup projection preserves ids without being
  transparent) and answers whether a join sits below. `bind_free_names`
  refuses a multi-hit bound free column over such a join with the shared
  `ambiguous_for_hits` error; the non-unique-engine arm still leaves the token
  unbound for the engine.
  pins: attr-id-1/C-026
  **Gate j_cross (2026-10-01):** the walk also descends id-extending
  Projections (above starts with below: `withColumn` appends a column).
  pins: attr-id-1/C-029
  **ATTR-ID-1 S3d follow-up (2026-10-01):** `union_below_wrappers` answers
  whether a Union sits below through transparent nodes and id-subset
  Projections (every above id appears below, so reorder, rename, and subset
  pass while new expressions and any unstamped side stop); joins and
  aggregates stop the walk. Four unit pins (filter, no-union, reorder,
  new-expression).
  pins: attr-id-1/C-034
  **ATTR-ID-1 S3d follow-up 2 (2026-10-01):** `union_dup_below_wrappers`
  replaces it with positional tracking: a Projection continues the walk only
  when it maps every bound position to itself (Spark re-resolves through any
  name/Column projection, so reorder and dup-creating selects fan out, while
  star, rename, and append keep union lineage), and the Union arm answers
  true only when two or more distinct positions reach it (a dup created above
  the union never trims). Ten unit pins.
  pins: attr-id-1/C-036
  **ATTR-ID-1 S3e (2026-10-01):** the qualified-name family over the S1
  `resolve`. `bind_qualified_free_refs` rewrites each qualified free column
  whose head names a plan relation or facade qualifier (struct heads pass
  through for the engine); `grandchild_qualified_key` resolves one qualified
  name against the join schema below a Projection root;
  `join_output_sources` maps each join output position to its side feeds,
  pairing using keys; `qualifier_star_positions` lists the positions under
  one qualifier with their held parts. Pins: `../tests/attr_id_s3e.rs`.
  **ATTR-ID-1 SJ-5 F1 (2026-10-03):** `free_expr_names` collects the free
  column leaves of an expression (qualified or plain, never inside a
  subquery); `refuse_free_names` resolves each against the output displays
  and reports the first name with two ids, unless two hit engines match and
  the engine raises itself. `case_bind.rs` gains `free_sql_names`, the same
  rule over SQL text through the Databricks-dialect parser (subqueries,
  CTEs and a top-level FROM decline to scan).
  Pins: the `sort_names` unit module, `../tests/case_bind.rs`,
  `python/repark/tests/test_attr_id_1_sj5.py`.
  **ATTR-ID-1 SJ-5 F2 (2026-10-03):** `free_sql_names` also collects
  compound identifiers as qualifier plus written name.
  `sql_mentions_duplicate` skips the parse when no word or backquoted token
  matches a duplicated display under the session case rule; a duplicated
  display with a non-word character always parses. Pins:
  `../tests/case_bind.rs`.
  pins: attr-id-1/C-039
  **Fold SM-2c round B (2026-10-06):** the Project-sort arm no longer binds the
  oldest id (`oldest_field` is deleted: an oldest twin that is not the source
  column sorts rows neither Spark nor main produces). `bind_free_column` takes
  the plan; on ambiguity it refuses `UNRESOLVED_COLUMN` when the hits meet at
  a join (the string route's check, shared), else returns the key unbound,
  respelled to the input's casing when the projection input carries exactly
  one column of the name under the session rule, so DataFusion's
  missing-sort-column pushdown sorts by that input column. Otherwise the
  unbound key flows to the engine, which refuses or pushes deeper exactly as
  on main. `project_input_spelling` exposes the same walk to the facade's
  string-sort route. Pins: `../tests/attr_id_s3b.rs`.
  pins: attr-id-1/C-069, C-070
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
- `written_names.rs` — **CASESENS-2 port (2026-10-03):** what the ATTR-ID-1
  stack keeps of the charter's written-name matchers (cherry-picks `e4be1feb`,
  `581f91b1`, `28f6d28a`, `111e95b3`): `refuse_folded_duplicate_keys(keys, rule)`
  alone. `Exact` passes; `IgnoreCase` lowers each key with
  `java_case::string_lowered` and refuses the first repeat with Spark's
  `COLUMN_ALREADY_EXISTS` (42711) naming the lowered key (live Spark 4.1.2,
  2026-10-03: `{"v", "V"}` refuses `` `v` ``). The charter's `resolve_df_names`,
  `match_display_names`, `match_subset_names`, `resolve_qualified_display_names`,
  `rewrite_join_condition_aliases`, `unresolved_subset_name` and `Disposition`
  are deleted with their inline tests: `attr_id::resolve` and the S3a–S3e binds
  cover every site they served, and join conditions bind through
  `predicate_names.rs`. `case_bind::unresolved_column` is `pub` so the binding
  renders an engine miss with Spark's text and every frame field as a
  suggestion. Pin: `../tests/case_bind.rs`
  (`folded_with_columns_keys_refuse_only_under_ignore_case`).
  pins: casesens-2/C-004, C-010

## Pointers

- Up: [../map.md](../map.md)
