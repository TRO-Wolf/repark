# Unit ledger — STAMP-2-R5P6-2 · the remaining r5p6 construction cost

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move).

**Unit:** STAMP-2-R5P6-2 · **Date:** 2026-10-07 · **Model:** claude-opus-5-5 (high) · **Branch:** `perf/stamp-2-r5p6-2` · **Base:** `3fbcb2ca` (`origin/main`, which holds #980)

**Rubric:** STANDARD. `risk_tier: standard`.

**Card:** [stamp-2-r5p6-2-card-2026-10-07.md](../../roadmap/mid-term/stamp-2-r5p6-2-card-2026-10-07.md).

## Scope

0. Profile the r5p6 family on a release build of main and confirm or correct the card's three
   cost sources.
1. The join doors: `crossJoin` and the condition join plan natively when the join's column
   references are exact; otherwise the SQL route is unchanged. One commit per door.
2. The stamp: measure metadata sharing and a wider `plan_is_stamped` short-circuit; land only a
   measured gain that changes no answer.
3. Binding: profile only; change code only for a measured hot spot.
4. Answers gate, lane-side speed (indicative), mutations.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Attribution on main `3fbcb2ca` (release, cProfile, the 12,272 r5p6 like cells): the H1 join door carries 0.88 s over 466 calls, 0.71 s of it in `sql_built`; the stamp carries 0.30 s over 43,194 calls; the binding layer carries about 1.6 s spread thin. The card's sources stand, with one correction: main before the stack also planned both doors through `sql_built`, so the native route is new work, not a restoration. | The step 0 table below. | **PROVEN** |
| C-002 | `join_exact_sides` followed by `analyze_built_plan` builds the plan the SQL route builds: `LogicalPlan` equality and equal plan depths against `sql_built` on the H1 statement, for a cross join and for a one-equality join over 20 key-type pairs, six join types and both operand orders, and over a side whose own plan is not yet analyzed. | `exact_key_join_builds_the_sql_route_plan_for_every_key_type_and_how`, `cross_join_builds_the_sql_route_plan`, `unanalyzed_sides_are_analyzed_as_the_sql_route_analyzes_them`; mutations M3, M5 below. | **PROVEN** |
| C-003 | The cross door (`crossJoin`, `join` with no condition or no keys, `how="cross"`) plans natively when both sides are exact, with the SQL route's schema, column names, attribute ids, rows, session state and plan text. | `test_cross_join_plans_natively_with_the_sql_route_answers`. | **PROVEN** |
| C-004 | A side that is not exact keeps the SQL route: a field or output name outside `[A-Za-z_][A-Za-z0-9_]*`, two fields equal under case folding, an output source the side does not hold exactly once, or a scratch alias that is not the three-part home reference. | `inexact_joins_are_left_to_the_sql_route`, `test_cross_join_over_an_inexact_side_keeps_the_sql_route`; mutation M6 below. | **PROVEN** |
| C-005 | A native attempt that raises is a miss: the SQL route runs unchanged, so a refusal carries the SQL route's class and text. | `test_raising_native_cross_join_keeps_the_sql_route_answer`, `a_map_key_refuses_on_both_routes`. | **PROVEN** |
| C-006 | The native join carries the session state of the join call, as `sql_built` does, not the left frame's older snapshot: a `spark.sql.caseSensitive` change between the frame and the join shows on the joined frame on both routes. | `test_native_cross_join_carries_the_session_state_of_the_join_call`; mutation M4 below. | **PROVEN** |
| C-007 | The condition door plans natively when the prepared condition is exactly one equality between one left field and one right field, `(l.f = r.g)` in either operand order, with the SQL route's schema, column names, attribute ids, rows and plan text over six join types; every other condition keeps the SQL route unchanged, and a refusal carries the SQL route's class and text. | `test_exact_join_keys_are_one_equality_between_the_two_sides`, `test_exact_key_join_plans_natively_with_the_sql_route_answers`, `test_inexact_join_conditions_keep_the_sql_route`, `test_join_refusals_are_the_sql_route_refusals`; mutations M1, M2 below. | **PROVEN** |
| C-008 | Answers: the attr-id, sort, fill and self-join suites, the 394-cell sort grid and the replay corpus are unchanged. | Does every answer gate come back identical on the final code? Step 4. | **OPEN** |
| C-009 | The stamp is measured and left: 39,731 of the 43,194 stamp calls on the like cells find the plan stamped and cost 0.16 s together, so no short-circuit can gain more than 0.7 point; field metadata is Arrow's owned map and cannot be shared from this repository; and the hash-table clones the earlier attribution read as field metadata are mostly the session registries, which C-010 removes. | The step 2 table below. | **PROVEN** |
| C-010 | Binding has one measured hot spot, in the native binder: `frame_rule` built a DataFusion `TaskContext` (the session configuration and four function registries, about 12 µs) for every bound column, 72 % of a native `select`. `PyDataFrame` now reads the rule once per handle and hands it to children that keep the parent's state. The value is the one every call read, so no answer changes. | `name_rule_is_read_once_per_handle_and_children_inherit_their_parents`, `test_frames_keep_the_case_rule_of_their_own_session_state`; the debug assertion in `inherit_rule` under the binding's 158 tests; mutation M7 below; the step 3 table below. | **PROVEN** |
| C-011 | Speed, lane side: the r5p6 family and the whole like set, three runs each, head against main. | What are the indicative ratios? Step 5. | **OPEN** |

## Step 0: where the time goes on main `3fbcb2ca` (C-001)

Release build (`CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16`, `maturin develop --release`), cProfile
over the 12,272 like cells of the gate record (`cellsLike.json`), cores 48–63, 26.1 s under
the profiler.

| Source | Calls | Time (s) | Note |
|---|---|---|---|
| `_join_on_condition_h1`, cumulative | 466 | 0.881 | 1.89 ms each |
| — its `sql_built` | 466 | 0.709 | 1.52 ms each |
| — `crossJoin` share | 354 | 0.618 | 1.75 ms each |
| — condition-join share (`j_alias`) | 112 | 0.264 | 2.36 ms each |
| — `requalify_join_sides`, view registration, qualifiers, spawn | 466 | 0.028, 0.026, 0.021, 0.015 | the rest of the door |
| `stamp_attribute_ids` | 43,194 | 0.304 | 7 µs each |
| `_column_of`, cumulative | 60,778 | 1.564 | 26 µs each |
| — `_bind_resolved_name`, cumulative | 38,686 | 0.830 | 0.256 self |
| — `_frame_id_snapshot`, cumulative | 120,878 | 0.410 | the id-snapshot weak map |
| — `WeakKeyDictionary` get, remove | 420,564, 99,543 | 0.299, 0.196 | cumulative, self |
| — `bind_qualified_free_refs` | 24,225 | 0.159 | native |
| `session_table` → `sql_built` | 1,650 | 1.167 | `S.table("tv")`; same on main before the stack; out of scope |

**The correction.** The card says main built both doors natively. It did not: before the stack,
`crossJoin` ran `SELECT * FROM l CROSS JOIN r` through `sql_built` (354 calls, 0.36 s in the
STAMP-2-R5P6-1 attribution) and the condition join ran through the same H1 statement as now.
The stack moved `crossJoin` onto the H1 statement, which names every column. So a native route
is new work on both doors, and it can go below main before the stack on these constructors.

**What the SQL route does that a native builder must repeat.** `sql_built` parses the text,
plans it, refuses map ordering over the whole plan, and then runs the analyzer eagerly over the
whole plan, the two sides included (`execute_passthrough_inner`). That eager analysis is
visible: it fixes Spark-adjusted types, raises analysis errors at the join call, and takes the
session configuration of the join call. A native builder that skipped it would move those to
the action. So the native route keeps it: `analyze_built_plan` is the same two calls,
`refuse_map_ordering` then `analyze_eagerly`, on the live session state. The first differential
run showed why: with no analysis, a `TIMESTAMP` side differed from the SQL route by one cast.

**What that leaves to save.** The text, the parse, the SQL planner and the statement rewrites.
Measured on the two r5p6 constructors (mean of the fastest half of 200 runs, release):

| Constructor | SQL route | Native route | Saved |
|---|---|---|---|
| `crossJoin` alone | 1,452 µs | 967 µs | 485 µs |
| `j_cross` (with its two selects and filter) | 1,762 µs | 1,272 µs | 489 µs |
| condition join alone | 1,810 µs | 1,133 µs | 677 µs |
| `j_alias` (with its selects and aliases) | 2,326 µs | 1,656 µs | 670 µs |

The native call itself is 0.72 ms, and `perf` puts nearly all of it in
`Analyzer::execute_and_check`. The analyzer is what is left of the door.

## The exactness rule

The H1 statement has one shape: `SELECT l.f AS o, …, r.g AS p FROM l <how> JOIN r [ON c]` over
two scratch views. Its answer is settled without the SQL planner when every reference resolves
by exact name:

- each scratch alias is the three-part home reference;
- every field of each side, and every output name, matches `[A-Za-z_][A-Za-z0-9_]*`, and no two
  fields of a side are equal under ASCII case folding (the SQL door may resolve those
  differently from an exact lookup);
- every projected source is held exactly once by its side.

`join_exact_sides` builds the aliased sides, the join and the projection, with a bare column
where source and output name are equal, as DataFusion's SQL planner does. `analyze_built_plan`
then does what the SQL door does after planning. Key types carry no guard: coercion happens in
the shared analysis, and the Rust pins hold plan equality for mixed-type keys too.

**The condition door.** The native preparer has already bound every reference of the
condition to a side before the route is chosen, and its refusals (the self-join 1182, a missing
attribute, an ambiguous name, a USING key) are raised there on both routes. `_join_exact_keys`
reads the prepared text: it replaces the one left reference and the one right reference by a
placeholder and admits only `(\x00 = \x00)`. That is #980's exactness test moved to a
condition, a placeholder shape matched whole. #980's own function does not fit: it rewrites
tokens against one frame and compares the result with that column's `_sql_expr`, and here both
routes read the same prepared text, so there is no second spelling to compare. #980's CAST and
literal discipline exists because the native typing of a literal differed from the SQL door's.
This route has no such difference to guard: it admits no literal and no cast, and it runs the
SQL door's own analysis.

Scratch views are still registered on both routes. Registration costs 35 µs per join, it moves
the session's deep-view high-water mark that later statements read, and leaving it in place
keeps the SQL route byte-for-byte unchanged.

## Step 2: the stamp (C-009)

Measured on the 12,272 like cells, head build, with `_native.stamp_attribute_ids` wrapped by a
counter (`out is inner` tells a kept plan from a restamped one).

| Outcome | Calls | Time | Each |
|---|---|---|---|
| already stamped, handle returned | 39,731 | 0.161 s | 4.05 µs in the run; 0.27–0.33 µs warm on 2–3 columns |
| restamped (sources and computed outputs) | 3,463 | 0.164 s | 47 µs |

**`plan_is_stamped` short-circuit: left.** The check already returns on the first read of a
stamped plan. All 39,731 kept calls together are 0.16 s of a 23.9 s run, so skipping every one
of them from the facade could gain at most 0.7 point, and a provable rule (children of
identity-preserving spawns only) would reach a part of that. Under the 1-point bar, with a new
constructor path to keep right; not landed. One thing is recorded for a wide-frame unit: the
Projection arm calls `maybe_index_of_column` per output, a linear scan, so the check is
quadratic in width (14 µs at 40 columns). r5p6 frames are 2–6 columns wide.

**Metadata sharing: not ours to do.** A field's metadata is Arrow's owned
`HashMap<String, String>` inside `Field`. DataFusion rebuilds it in `Expr::to_field` and
`projection_schema` on every schema computation. There is no map in this repository to put
behind an `Arc`; the 47 µs of a restamp is DataFusion's `Projection::try_new` plus one
session-state clone for the new `DataFrame`.

**A correction to the earlier attribution.** STAMP-2-R5P6-1 read the +0.20 s of
`hashbrown::RawTable::clone` as field metadata maps. `perf` with DWARF call graphs on a
`select` loop puts the hash-table clones and drops under `TaskContext::from(&SessionState)`
and its drop. On the like cells, hash-table clone plus drop self time is 7.5 % of all samples
before C-010 and 5.8 % after. What remains is the one session-state clone DataFusion needs for
every child `DataFrame`, the state snapshots of `sql` and `sql_built`, and the real field maps.

## Step 3: binding (C-010)

The Python binding layer profiles as the card says, wide and thin: `_column_of` 1.56 s
cumulative over 60,778 calls, no single function above 0.26 s self. No Python change is made.

The hot spot is below it, in the native binder. Ranking every native call of the step 0
profile put `PyDataFrame.select` fourth (18,972 calls, 1.45 s, 77 µs each), and a
`select` loop under `perf` gave:

| Frame | Share of the loop's samples |
|---|---|
| `PyDataFrame.select` | 29.8 % |
| — `bound_projection` | 22.1 % |
| — — `TaskContext::from(&SessionState)` | 11.7 % |
| — — `drop_in_place<TaskContext>` | 9.8 % |
| — `grown_clone_frame` (the child's own state clone) | 7.5 % |

`frame_rule` asks a frame for one boolean, `spark.sql.caseSensitive`, through
`DataFrame::task_ctx()`, because DataFusion 54.1 offers no borrowed access to a frame's
configuration. `task_ctx()` clones the session configuration and the scalar, higher-order,
aggregate and window registries. It ran once per bound column.

| Native `select` | Before | After |
|---|---|---|
| 1 column | 26.7 µs | 17.3 µs |
| 2 columns | 39.1 µs | 19.8 µs |
| 3 columns | 51.3 µs | 22.3 µs |
| facade `select("id", "v")` | 91.1 µs | 75.0 µs |
| `frame_case_sensitive` | 10.2 µs | 0.09 µs |

**Why no answer changes.** A `DataFrame` owns a snapshot of its session state; nothing changes
it after the handle is made, so the first read is every read. A child inherits only in the six
builders that make it from a clone of the parent through the DataFrame API, and from
`grown_plan_rewrite`, which rebuilds on the frame's own state. `inherit_rule` asserts in debug
builds that the inherited rule equals the child's own; the binding's tests run with it. Frames
from `sql`, `sql_built` and the native join start unread and read their own state.

## Mutations (red-first)

Filed with the answers and the speed runs in the unit's last commit.

## Out of scope, observed

- Every native builder clones the parent `DataFrame`, and so its whole session state, to make
  a child (`grown_clone_frame`, 7.5 % of a `select` loop). DataFusion's `DataFrame` owns its
  state by value, so this is one clone per frame by construction. It is most of the 5.8 % of
  hash-table clone and drop time left on the like cells.
- `session_table` plans `S.table("tv")` through `sql_built`: 1,650 calls, 1.17 s on the like
  cells, the same on main before the stack.
- `df.join(other, condition, "cross")` ignores the condition on main and on this branch (12 rows
  where Spark 4.1 answers 2, measured live). The H1 door emits `CROSS JOIN` with no `ON`. The
  native route reproduces the SQL route here and the differential pin holds only their
  equality, not the row count.
