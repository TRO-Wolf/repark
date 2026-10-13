# ICE-NTZ-NESTED-WALL-1 + NTZ-STRUCT-COERCE-1 · Q3 · the one structure (read at `5ea94e8c`; nothing built, run or edited)

## 1. Recommendation
**C's semantics carried by A's mechanism: every naive-to-zoned conversion is the session zone, and the store kernel decides
per leaf, at execution, from the type of the argument it actually receives, with leaf selection taken from the target schema
alone.** Then "localise" and "store" are two independent total functions whose composition is the identity by meaning, so no
door, probe, marker or spelling list has to make them agree; B's marker and D's "no localise call in the value" test are both
evaluated on an analyzed plan, which is exactly what `probe.rs` already tries and fails to reach on four paths. One more fold
can pass a hostile verifier **only if it is this one, and it deletes more than it adds** (three patches, the probe, the
decision table); a sixth guard on a sixth path will not, and narrowing (D) keeps the same two mechanisms on the mixed values it
would still touch (`SET st = CASE …` with no mixing is still a localise site tomorrow).

## 2. The invariant
**In the optimized plan of any write whose target has a nested microsecond `TIMESTAMP_NTZ` leaf, no `Cast` has an input with a
naive leaf (µs or ns) at a path where its target has a zoned leaf, and every zoned-typed value that reaches such a target leaf is
the argument of `__repark_cast_nested_ntz__` (or of `__repark_cast_timestamp_ntz__` at a bare leaf) whose path literal was
derived from the target type.** Two halves, two places:
- localise: `crates/repark-functions/src/nested_wall.rs` `rewrite_struct_wall_cast`, called unconditionally from
  `instant_ts.rs` `rewrite_cast` at both seats (the pre- and post-coercion seat, `registration.rs:35`, `extension.rs:126`).
- store: `crates/repark-iceberg/src/write/update_cast.rs` `nested_leaf_cast_sql` (text) and its `Expr` twin for
  `insert_timestamp_ns.rs` `wrap_nested_ntz` and `ntz_store.rs` `zone_store_frame`. Doors that end there today: the UPDATE
  fold and the MERGE fold (`nested_assign.rs` `leaf_sql` → `assignment_cast_sql`, plus `expand.rs`), the MERGE star
  (`write/merge/mod.rs` `expand_star_clauses`), the predicate-DML UPDATE projection (`predicate_dml/lineage.rs`
  `update_projection_sql`), INSERT OVERWRITE and BY NAME (`zone_stores`), the INSERT hook (`after_analysis`).

## 3. The build contract (for a guided executor)
- **Kernel** `__repark_cast_nested_ntz__(value, '<paths>')`: paths are every µs-NTZ leaf of the **target** (the walk
  `target_has_nested_micro_ntz` already does, made to return paths). Per path, by name through the argument: a zoned leaf →
  `ntz_single` (session wall, as now); a naive leaf, a missing name, a non-struct argument → unchanged. Return field = the
  argument's field with those leaves retyped and **field metadata dropped** (`PARQUET:field_id`, see F5). `simplify`
  returns the bare argument when no selected leaf of the argument type is zoned, so the optimized plan and the EXPLAIN text of a
  naive-only value are main's by construction, not by `keep_update_store_casts`.
- **Text doors** pass only `(expr_sql, target_type)`: `nested_leaf_cast_sql` emits `arrow_cast(K((expr), '<paths>'), '<T>')`
  whenever the target holds a nested µs-NTZ leaf; `store_assignment_cast_sql` and `nested_micro_ntz_wall_sql` lose their
  `source` argument. The by-name member rebuild and a path longer than one step need nothing: a bare NTZ leaf already takes
  `__repark_cast_timestamp_ntz__`, whose `Timestamp(_, Some(_))` arm is `unlocalize_to_walls` (`timestamp_ltz_ntz.rs:~435`).
- **Plan doors** pass `(expr, target_type)`: `wrap_nested_ntz` wraps whenever the target is eligible, no `get_type`, no
  `arrow_cast_rejects_not_null_member` skip (the kernel keeps nullability, Arrow then refuses exactly as main does);
  `after_analysis` descends through `Explain` and `Analyze` to the `Dml` alike; `zone_store_frame` drops `analyzed_types`.
- **Analyzer**: `rewrite_struct_wall_cast` runs always and also matches a target leaf `Timestamp(ns, None)` (the user-written
  `STRUCT<v: TIMESTAMP>`, planned as ns: coercion ledger §6), retyping that leaf of the cast field to µs-UTC, the mirror of the
  top-level `to_timestamp` arm main already has. Ruling to flip: reads of a user-written struct cast move onto Spark.
- **Default when it cannot decide**: there is no pre-analysis decision left. The kernel's default is pass-through (main's
  cast) for any leaf it cannot prove zoned; the arm's only blind spot is a `Cast` whose input type `get_type` cannot read,
  which in an analyzed plan is none (the empty-schema VALUES path carries typed literals).
- **Deleted**: `nested_assign/probe.rs`, `Typed.stored`/`written` split, the `value_holds_scalar_subquery` branch of
  `leaf_sql`, `NtzDecision`/`source_paths`/`members_align`/`LEAF_ROWS`, every `analyzed_types` caller (`source_columns`,
  `lineage.rs:~100`, `zone_store_frame`), `keep_update_store_casts` and `without_the_assignment_kernel`,
  `analyzed_insert_stores_a_struct_wall` and the `localize_struct_walls` flag, `explain_update_assignments`' kernel-count
  gate (EXPLAIN UPDATE folds always, the same fold as execution), `rewritten_append_explain` as a special case.
- **R2 (an analysis failure treated as zoned)**: the fallback arm should not exist. A "fall back to main's cast" for a value
  the analyzer may still localise is the defect class itself, and the verifier could not make analysis fail because an
  analyzable statement that then runs has a type; deleting `probe.rs` removes the arm and the mutant.

## 4. What stays at main's value (rough, from the verifier's numbers)
Nested **nanosecond** NTZ leaves (12 `s_nanos` steps), **list and map** leaves, member names holding `.` or `,`, the
zone-switch temp views (R-032, 3 rows), `spark.sql.timestampType=TIMESTAMP_NTZ` (6 coercion cells), the Python native door
and a session without the Spark extension, walls main already stored as UTC instants in zoned columns, the C-005 and F8
refusals. Moving onto Spark beyond today: F1 (36 of 46 ucast cells per zone), F2 (24 of 36), F3 (28 of 36), F4 (16 of 24), the
attack probe's 29 per non-UTC session, the F6 literal-in-CASE cells (15 per zone), the R-031 NOT NULL cells (4, through the
leaf kernel of the member rebuild), the 11 user-cast read cells of the coercion ledger. Of the attack probe's 59
"as main, not Spark" cells per New York session I expect under 10 to remain, all in the list above.

## 5. The plan-level property test the lane must add
A Rust test in `repark-spark` that plans (never executes) every statement the suite already carries (the EXPLAIN json
corpus, `wall_first` shapes, the verifier's `fam.py` and `ucast.py` families, both operand orders) through the Spark door to
the optimized logical plan, walks it with `apply_with_subqueries` + `apply_expressions`, and asserts per statement: (a) zero
`Expr::Cast` whose input type has a naive leaf at a path where the cast type has a zoned leaf; (b) for each target column
with a nested µs-NTZ leaf, every zoned-typed subexpression at a target path is under exactly one store kernel, and a
naive-only value is under none; (c) the number of localise calls is unconstrained. Run it in two zones and in
`timestampType=TIMESTAMP_NTZ`. Beside it, one two-batch pin: two one-row record batches with `PARQUET:field_id` metadata
through `IF(cond, CASE WHEN … THEN st ELSE cs END, cs)` (F5), asserting the kernel output type equals its declared field.

## 6. F5, three attacks, what I did not read
**F5 is not the pairing family.** By reading: DataFusion 54.1 `case.rs` `case_when_no_expr` forwards a branch's array
unfiltered when every row of a batch takes that branch; the kernels build their output `StructArray` from the **input
array's** fields (metadata kept) while the declared type is main's `arrow_cast` literal with metadata stripped; one file
(both rows in one batch) and six rows in two files go through the filtered path and pass, two rows in two files hit the
shortcut and fail (`verify3/out/reduce-ny-head.txt`). Fix in the kernel's return field and output fields, not in any door.
Attacks: (1) every F1–F4 family on a table of N files with one row each, the NULL-struct row included, New York and
Lord_Howe; (2) EXPLAIN and rows of naive-only, instant-only and mixed values into an NTZ-leaf target through all six doors,
byte-equal to main where nothing converts, Spark's rows where something does; (3) a user-written struct cast at depth 2,
inside a scalar subquery, inside `MERGE … UPDATE SET *` from a reordered UNION source, on a branch target.
Not read: `expand.rs`, `insert_overwrite.rs`, `insert_by_name.rs`, the Python writer lowering, the `IF` lowering, DataFusion's
`ResultBuilder`, `ntz_single`'s gap policy, whether EXPLAIN prints the post-`simplify` plan (assumed from DataFusion's
default `logical_plan` row), and the cost of an unconditional kernel on naive values (assumed zero after `simplify`).
