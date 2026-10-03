# Unit ledger — WO ATTR-ID-1 · attribute identity as a function of the plan

**Date:** 2026-09-30 · **Branch:** `feat/attr-id-1` · **Base:** `db3a1f37` (`origin/main`)
**Model:** claude-opus-5-5 (S1 executor, high) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Work order:** `/tmp/oc-worker/direct/wo/attr-id-1-design.md` (owner-adopted 2026-09-30, §7a),
slice S1 (the Rust core; no facade change).

**Retires:** this ledger moves to `../completed/` in the unit's last commit (S4).

**Why.** Six verifier passes on #881 found eight S1 findings that are one bug: the DataFrame
door rebuilds attribute identity after the fact from four encodings that must agree, and every
op that does not propagate them leaves identity "unknown". Spark decides both halves (twins of
one attribute bind; two attributes with one name refuse) with one fact, the attribute's
`exprId`. This unit gives every output field of every DataFrame plan one attribute id, carried
in field metadata under `repark.attr`, and resolves names by that id.

## Round S1 (2026-09-30)

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | On DataFusion 54.1.0 alone, the `repark.attr` field-metadata key propagates as measured: a bare column and an alias of a column inherit; a cast copies the source id; arithmetic, literal, function, one-argument `coalesce` and `CASE` carry none; Filter, Limit, Sort, Distinct and SubqueryAlias keep every id; a self-join repeats the ids on both sides; Union and `union_by_name` keep an id only where every input carrying the column has the same one (a column the first input lacks keeps a later input's id); Aggregate keys keep theirs and values carry none; Window passes its input ids and its value carries none; a temp view read back by SQL keeps them, and so does the facade's join shape over two views. | One pin per row of the work order's §3.5, ids asserted by position after the node. | PROVEN | `crates/repark-core/src/session/tests/attr_id.rs`, 10 passed at the first commit. |
| C-002 | `stamp(plan)` (`df_guards/attr_id.rs`) gives every output field of the root an id and is idempotent: a Projection root keeps a column's inherited id and an alias's own id, and mints a fresh id for every other expression through `Alias::with_metadata`, overriding the id a cast copies; any other root keeps a fully stamped schema and otherwise adds one pass-through Projection that sets only the missing ids by minting, except a Union root, whose every position takes the first input's id (read from the first input's expressions, through DataFusion's `union_by_name` wrapper) and mints where the first input has none. `attribute_ids` reads the ids by position. The ids are opaque (`a` plus 12 hex digits from a per-process counter), never derived from a name, a position or a plan id. | Pins per §3.5 row after `stamp`: source, column, cast, arithmetic, literal, pass-through chain, Aggregate, Window, Union (three doors), temp-view read-back, idempotence. | PROVEN | `crates/repark-core/src/session/tests/attr_id.rs` — `stamp_mints_a_fresh_id_per_source_field_and_is_idempotent`, `stamp_keeps_column_ids_and_mints_for_cast_arithmetic_and_literals`, `stamp_leaves_pass_through_nodes_unchanged`, `stamp_keeps_aggregate_keys_and_mints_values_and_window_outputs`, `stamp_gives_a_union_the_first_input_ids`, `stamp_mints_where_the_first_union_input_has_no_id_to_give`. |
| C-003 | `remint_join_collisions(plan, left_width)` keeps every left id and gives each right-side id that also appears on the left one fresh id (right-side twins of one attribute stay twins); a join of distinct attributes returns unchanged, and a left width past the field count is an internal error. Over the facade's join shape the ids survive `requalify_join_sides`. | A self-join over two temp views of one stamped frame, a right side with a twin, and a join of two distinct frames. | PROVEN | `the_join_re_mint_keeps_the_left_and_renames_each_colliding_right_id_once`, `the_join_re_mint_leaves_a_join_of_distinct_attributes_unchanged`. |
| C-004 | `resolve(schema, written, qualifier, rule, displays)` collects the positions whose display matches under the rule (and whose relation matches a written qualifier through `same_relation`), then answers `Bound(hits)` for one distinct id, `Ambiguous(hits)` for more, `Missing` for none; a hit without an id and a display count unequal to the field count are internal errors, never a wildcard. | Pins over `(id, Data, s)` under `Exact` and `IgnoreCase`: one hit; two hits with one id (same and case-folded displays); two hits with two ids (same and case-folded displays, the folded-duplicate keys); a qualified hit and miss; a cast twin; the two error paths. | PROVEN | `resolve_binds_a_single_hit_under_each_rule`, `resolve_binds_every_twin_of_one_attribute`, `resolve_refuses_two_attributes_under_one_name`, `resolve_narrows_by_a_written_qualifier`, `resolve_refuses_a_cast_twin_that_reuses_the_name`, `resolve_treats_a_missing_id_as_a_bug_and_checks_the_display_count`. |
| C-005 | `repark-python`'s `dataframe_names` binds the core without logic of its own: `stamp_attribute_ids(frame) -> PyDataFrame`, `attribute_ids(frame) -> list[str \| None]`, `resolve_display_name(frame, written, qualifier, displays, exact) -> (str, list[int])` with `"bound"`, `"ambiguous"` or `"missing"` first; `requalify_join_sides` re-mints the right side's colliding ids when a right side is given. No facade or Python file changes in S1. | A binding pin on a real session. | PROVEN | `crates/repark-python/src/tests.rs` `binding_stamps_resolves_and_re_mints_a_self_join`; `git diff --stat origin/main` lists no `python/` path. |
| C-006 | Mutations M1–M4 (work order §4 S1) each turn their named pins red and are reverted with a clean tree. | Run each, record the red tests, revert, `git status` clean. | PROVEN | The mutation record below, run on the round's third commit. |

**§2 "to verify" rows, measured (2026-09-30), none contradicting §2:** Union and
`union_by_name` intersect the ids of the inputs that carry a column (dropped where they
differ; a later input's id where the first input lacks the column), which is the case §2
foresaw, so RePark gives a Union root the first input's ids (C-002). A temp view's read-back
keeps the ids. Aggregate keys keep theirs. A Window passes its input ids through. Filter,
Limit, Sort, Distinct and SubqueryAlias pass every id through.

## Mutation record (2026-09-30)

Each mutation edited `crates/repark-core/src/session/df_guards/attr_id.rs`, ran
`cargo test -p repark-core --lib session::tests::attr_id` (M2 also
`cargo test -p repark-python --lib binding_stamps`), and was reverted with `git checkout`;
`git status --short` was empty after each.

| # | Mutation | Red |
|---|---|---|
| M1 | `AttrId::mint` returns one constant id, so `stamp` mints one id for every field | 9 of 24: the ambiguity pins `resolve_refuses_two_attributes_under_one_name` and `resolve_refuses_a_cast_twin_that_reuses_the_name`, the stamp pins (source, cast/arithmetic/literal, aggregate/window, both union pins) and both join re-mint pins |
| M2 | `remint_join_collisions` returns the plan unchanged | `the_join_re_mint_keeps_the_left_and_renames_each_colliding_right_id_once`, `the_join_re_mint_leaves_a_join_of_distinct_attributes_unchanged` and the binding pin `binding_stamps_resolves_and_re_mints_a_self_join` |
| M3 | `resolve` groups the hits by display name instead of by id | `resolve_binds_every_twin_of_one_attribute` (the case-folded twin), `resolve_refuses_two_attributes_under_one_name`, `resolve_refuses_a_cast_twin_that_reuses_the_name`, and the self-join's ambiguous `id` in `the_join_re_mint_keeps_the_left_and_renames_each_colliding_right_id_once` |
| M4 | `stamp` keeps any field that already carries an id, so the id a cast copies survives | `stamp_keeps_column_ids_and_mints_for_cast_arithmetic_and_literals`, `resolve_refuses_a_cast_twin_that_reuses_the_name` |

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: attr-id-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each §3.5 row has a DataFusion-only pin and a pin after stamp; resolve is pinned over the six named cases under both rules.
      artifacts: [crates/repark-core/src/session/tests/attr_id.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Unstamped roots, fully stamped roots, a union whose first input lacks a column, right-side twins, a join of distinct frames, an out-of-range left width, a display-count mismatch and a hit without an id are each exercised.
      artifacts: [crates/repark-core/src/session/tests/attr_id.rs]
    - id: AT-3
      status: ATTACKED
      evidence: A missing id and a display-count mismatch are internal errors in resolve, and an out-of-range left width is one in the re-mint; stamp is idempotent, so a repeated spawn changes nothing.
      artifacts: [crates/repark-core/src/session/df_guards/attr_id.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The one shared state is the per-process id counter, an AtomicU64 incremented with fetch_add; ids stay unique across threads and sessions in one process.
      artifacts: [crates/repark-core/src/session/df_guards/attr_id.rs]
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, deserialization or path handling; plan metadata only.
    - id: AT-6
      status: ATTACKED
      evidence: The ids ride in Arrow field metadata; S1 changes no frame the facade holds, and the metadata that may leave the process once S2 stamps every frame is residue R-2.
      artifacts: [task/ledgers/staging/attr-id-1-ledger.md]
    - id: AT-7
      status: N/A
      justification: One pass-through projection per unstamped non-projection root; the replay timing guard belongs to S2 (work order halt rule 4).
    - id: AT-8
      status: ATTACKED
      evidence: The DataFusion 54.1.0 propagation the design leans on is pinned rather than presumed, including the cast and union behaviours the stamp overrides.
      artifacts: [crates/repark-core/src/session/tests/attr_id.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Every internal error names the field position and name or the two counts it compared.
      artifacts: [crates/repark-core/src/session/df_guards/attr_id.rs]
    - id: AT-10
      status: ATTACKED
      evidence: M1–M4 each red their named pins; every added branch has a pinned input (idempotent roots, union mint and inherit, the by-name wrapper, the re-mint no-op and error, both resolve errors).
      artifacts: [crates/repark-core/src/session/tests/attr_id.rs, crates/repark-python/src/tests.rs]
  complete: true
```

## Residues

| # | Residue |
|---|---|
| R-1 | Dated 2026-09-30, open for S3: Spark's `Alias` creates a new attribute, so a user-written `Column.alias` over a column is a fresh `exprId` there, while §3.5 (and `stamp`) has an alias of a column inherit, which the facade's own renames need. `stamp` keeps an alias's own id, so the facade can give a user alias a fresh one; the S0 replay decides. |
| R-2 | Dated 2026-09-30, open for S2: `repark.attr` rides in Arrow field metadata, so once S2 stamps every frame it can reach written files, cached views and exported Arrow schemas, and a file written by one process can bring ids that collide with another process's counter. |
| R-3 | Dated 2026-09-30, open for S2/S3: only `requalify_join_sides` re-mints; a USING join (`join_on_keys`, keys then each side's other columns) of a frame with itself still repeats the right side's ids. |
| R-4 | Dated 2026-09-30, open for S3e: `resolve` matches a written qualifier against the field's relation only; the frame's Python-held join qualifiers (§3.4) are not in the §4 S1 signature. Closed 2026-10-01 by S3e (C-039): `resolve` takes `frame_qualifiers` and matches plan or facade names under the live rule at every qualified door. |
| R-5 | Dated 2026-09-30, open for S3: Q2 forces materialized providers born clean, so a cache/checkpoint read mints fresh ids instead of inheriting the cached frame's; S3 `resolve` must cope with the cache identity break (origin-token fallback or equivalent). `Extension` plan nodes (only `UnpivotNode`) pass the strip opaque; a SQL-`UNPIVOT` write source is unpinned until the facade probe lands. |

## Round S2 (2026-09-30)

**Model:** muse-spark-1.3-contributor (S2 executor, guided).
**Work order:** `/tmp/oc-worker/direct/wo/attr-id-1-design.md` §4 S2 with the orchestrator's
corrections (baseline `main.json`, stamp in `DataFrame.__init__`, `Column._attr_id` at the
bind sites that exist on main) plus §9 rulings Q2 (strip `repark.attr` at every write sink
and export) and Q3 (the USING-join re-mint).

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-007 | Every facade `DataFrame` with a relation root is stamped at construction: `DataFrame.__init__` wraps the native frame in `stamp_attribute_ids`, and every construction site calls the constructor. Statement roots (`Explain`, `Analyze`, `Ddl`, `Dml`, `Copy`, `DescribeTable`, `Statement`, `Extension`, a field-less `EmptyRelation`) pass through unstamped. | Grep over `python/repark/src/repark` showing every facade `DataFrame(` call runs `__init__` (no `__new__`/copy/pickle bypass, no subclass), plus runtime pins that frames from `createDataFrame`, `sql`, and a reader carry an id on every output field. | OPEN | Construction grep: 17 facade `DataFrame(` calls, all direct constructor calls (`core.py` `_spawn` and the streaming parent, `session_core.py` readers and `sql`, `reader_incremental.py`, `reader_orc.py`, `reader_text.py`, `reader_iceberg_path.py`, `catalog_surface.py`, the ANSI door); the `joins_columns.py`/`udf_bridge.py` hits are `pd.DataFrame`. Runtime pins land with the Block 2 pin file. |
| C-008 | `Column` carries `_attr_id` (default `None`), set at the frame-field bind sites behind `_column_of`, `__getitem__`, and `__getattr__` (`_bind_schema_column` via `column_fields._bound_attr_id`, which stamps on read and fails loud on a missing id); binds on statement frames yield `None`; nothing reads it yet. | Pins that `df["x"]`, `df.x`, and `_column_of` set the id at the field's position, that `F.col` stays `None`, and that an `EXPLAIN` frame binds `None` and still collects. `_bind_engine_display_column` exists on main but is intentionally unset (the ruling's S3 covers it); `alias`, `for_select`, and compound constructors do not propagate yet (S3). | OPEN | Runtime pins land with the Block 2 pin file. |
| C-009 | `repark.attr` never reaches a written file: `strip` rebuilds the registered plan bottom-up with the key removed from every stored schema and every embedded `Alias` (a root-only strip fails: the optimizer re-derives the key from kept expressions, pinned red before the fix), every `MemTable` temp view is born clean, and keyed leaves fail loud; the strip runs on the registered source in `run_through_temp_view` (local COPY for parquet/CSV/JSON, CTAS/RTAS, by-name append, static overwrite, `saveAsTable`, `insertInto`, V2 `writeTo`), on the s3a frame, and on the MERGE source. Text writes carry values only (no schema channel). | A parquet footer's field and file metadata (simple, deep, joined, cached, and subquery sources), an Iceberg table's stored metadata JSON plus its data-file footers, and MERGE data-file footers carry no key; written CSV/JSON bytes carry none; core `strip` pins. | OPEN | Core pins green; the facade pins run after the rebuild. |
| C-010 | `repark.attr` never reaches an export: `_apply_export_display_names` drops the key from every top-level Arrow field, covering `toArrow`/`to_arrow`, `to_arrow_batches`, and through them `toPandas` and `collect`; `schema`, `printSchema`, `show`, and `_repr` build from name/type/nullable triples and carry no field metadata. | Arrow-schema pins over the table and batch exports, `schema`-metadata and `printSchema`/`repr` pins, `collect` sanity. | OPEN | Pins land in the Block 2 pin file; they run after the Block 2 build. |
| C-011 | USING joins re-mint colliding right-side ids: `join_on_named_keys` counts the kept right-side output positions while building the key-dedup projection and runs the S1 re-mint split at that count (semi/anti return before the projection and need none). | A USING self-join keeps the key and left ids with distinctly re-minted non-key right ids, pinned at the core, the binding, and the facade. | OPEN | The core and binding pins run in the Block 2 gate; the facade pin runs after the Block 2 build. |
| C-012 | The S0 replay on this head is byte-identical to `main.json` on every judged cell (the 7 nondet cells excluded), and its median-of-3 wall clock is at most 597 s (1.2x of main's 498 s). | `replay.py main` into the S2 out-dir, `compare.py` against `main.json`: 0 changed judged cells. | OPEN | Runs after the Block 2 build; counts and timings land in the hand-back. |
| C-013 | The strip is load-bearing: with the parquet-sink strip disabled the footer pin goes red; reverted, the tree is clean. | The mutation record below. | OPEN | Runs after the Block 2 build. |
| C-014 | `stamp` only touches relation roots: `plan_is_relation` answers the 16 relation variants plus a field-bearing `EmptyRelation`, and `frame_is_relation` exposes it so statement-frame binds yield `None`. | The six statement roots plan unchanged with a `SELECT` positive, and the `EmptyRelation` split pin. | OPEN | Core pins green in the fix commit; the facade `EXPLAIN` pin runs after the rebuild. |

## Round S2b (2026-09-30)

**Model:** claude-opus-5-5 (S2b executor, high).
**Work order:** `/tmp/oc-worker/direct/wo/attr-id-1-s2b-s1b.md` Part A: S2 ended blocked on 23
neighbour failures ("Physical input schema should be the same as the one converted from
logical input schema", `repark.attr` on the logical side). The choice and its evidence are in
the design's §9a.

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-015 | Ids live on unoptimized and analyzed logical plans only. `StripAttributeIds` is the first optimizer rule of every core session (never an analyzer rule: the Spark SQL door keeps its eagerly analyzed plan as the frame, and the facade's join shape reads ids from it): bottom-up, subqueries included, it drops `repark.attr` from every `Alias`'s metadata and every outer-reference field of a node whose schema or expressions carry it, and recomputes that node's schema with DataFusion's `recompute_schema` (a Union takes `intersect_metadata_for_union` of its inputs); an alias left with no metadata over a column of its own name and relation becomes the bare column again, so the stamp's pass-through Projections fall to `OptimizeProjections` and a stamped frame optimizes to its unstamped twin's plan. DataFusion's physical planner drops alias metadata on every non-literal expression, so without the rule a stamped pass-through Projection over a clean source makes the logical and physical Aggregate inputs differ in field metadata; with it they agree without weakening DataFusion's check, and a foreign-keyed source keeps its key on both sides. Name resolution reads `DataFrame::schema()`, which the optimizer never rewrites. | A Rust repro of the failure; the rule is first in the optimizer and absent from the analyzer, and an analyzed plan keeps its ids; Aggregates over stamped source, Window, Union, `union_by_name`, semi-join, self-join, scalar-subquery and `EXISTS` frames plan and run with no id in any optimized node while the frame keeps its ids; a foreign-keyed source still runs; a stamped struct-field select and a stamped aggregate optimize to their unstamped twins' plans. | PROVEN | `crates/repark-core/src/session/tests/attr_id_seam.rs`, 6 pins; `tests/df_guard.rs`'s rule-order pin; M-A1 below. |
| C-016 | S2's neighbour regressions are gone and nothing else moved: the 23 failing tests pass, the neighbour sweep is green, S2's pins stay green, and the S0 replay is byte-identical to `main.json` on every judged cell within the 597 s median bar. | The sweep (`test_casesens_1*`, every `test_*join*`, `test_*csv*`, `test_*json*`, `test_*write*`, `test_*iceberg*`, `test_*cache*`, `test_attr_id*`, `test_e2_readwriter.py`; `-n 8`) and three replays compared with `compare.py`. | OPEN (4 judged cells change; median 597.27 s, 0.27 s over the bar) | Sweep: 2159 passed, 19 skipped, 2 xfailed, 0 failed. Replay (`replay.py main`, three runs, 602.82 / 595.09 / 597.27 s, median 597.27 s against main's 497.68 s): 43992 cells, identical across the three runs; 4 judged cells differ from `main.json`, 0 EQUAL cells move away from Spark, all four are errors on both builds and wrong against Spark on both. `r2.{F,T}_oj_twinjoin_filt`: the "No field named v" message lists the last four fields qualified (`l.__H`) where main lists them bare. `r3.{F,T}_ob_agg_max`: main fails in `type_coercion`; this build fails earlier, "Projections require unique expression names", because DataFusion's missing-sort-column step adds `__ID.v` next to the stamp's `__ID.v AS v`. Both come from the stamp's `Alias(Column)` wrappers in the unoptimized plan, which no optimizer step can reach. Evidence: `/tmp/oc-worker/direct/wo/attr-id-1/s2b/cmp-a*`. |

**Mutation M-A1 (2026-09-30).** `unnest_safe_optimizer_rules` starts from an empty list
instead of `StripAttributeIds`: four `attr_id_seam.rs` pins go red, three with DataFusion's
"Physical input schema should be the same as the one converted from logical input schema"
error at `collect` and one on the missing rule; the analyzed-plan pin stays green. Reverted
from a copy; `git diff` showed only the round's own edits.

**Mutation M-A2 (2026-09-30).** The strip keeps an emptied alias over a same-named column:
`a_stamped_frame_optimizes_to_the_plan_of_its_unstamped_twin` goes red with DataFusion's
"Optimizer rule 'push_down_leaf_projections' failed … duplicate qualified field name s" (the
same failure the replay's `cs2.T.select(T.s.a)` cell showed, and stock DataFusion shows on a
stamped frame with no strip at all). Reverted from a copy.

**Rejected first cut (2026-09-30).** The strip first ran as an analyzer rule. The core pins
were green, but `binding_stamps_resolves_and_re_mints_a_self_join` and
`binding_re_mints_using_join_collisions_of_a_self_join` went red: the Spark SQL door
(`repark_functions::analyze_eagerly`) returns the analyzed plan as the frame, so every
`spark.sql()` frame and every SQL-over-views join lost its ids. The optimizer runs only at
execution, so the rule moved there.

## Round S1b (2026-09-30)

**Model:** claude-opus-5-5 (S1b executor, high).
**Work order:** `/tmp/oc-worker/direct/wo/attr-id-1-s1b-fix.md` (VA1-1..VA1-6 from the S1
verifier), on top of S2b. The orchestrator's rulings R1 and R2 are applied as written, and
§3.2's "never rewrite below the root" is amended to "never rewrite a node that was the root of
a stamped frame": a same-op Window or Aggregate below the root is read to classify its outputs
and is not rewritten; the ids are set on the Projection above it.

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-017 | Identity is decided by structure, never by copied metadata (VA1-1). `computed_outputs` classifies a same-op node's outputs by position: a Window computes every output past its input width and passes the input's answers through; an Aggregate computes its grouping id, every aggregate value and every group key that is not, aliases stripped, a `Column` over a non-computed input position; a Filter/Sort/Limit chain is skipped; any other node computes nothing. An Aggregate or Window root (or a Filter/Sort/Limit root over one) mints a fresh id at every computed position through the pass-through Projection `stamp` adds, whatever DataFusion copied. | The verifier's VA1-1 repros and every aggregate and window function the facade emits, each asserting a fresh id distinct from its argument's and from every other output; M5 and M6. | PROVEN | `crates/repark-core/src/session/tests/attr_id_fresh.rs` (`every_aggregate_value_the_facade_emits_gets_a_fresh_id`, `cast_negated_and_arithmetic_group_keys_get_fresh_ids`, `every_window_output_gets_a_fresh_id_and_the_input_passes_through`); `crates/repark-core/src/session/tests/attr_id_verify.rs` (`va_first_and_last_value_aggregates_get_fresh_ids`, `va_cast_and_negative_group_keys_get_fresh_ids`, `va_lag_and_lead_window_roots_get_fresh_ids`). |
| C-018 | A Projection root keeps a column's id only when the column references a non-computed position of its input, so a `select` or SQL `GROUP BY`/`HAVING` over a same-op Window or Aggregate mints what it computes and keeps what it passes; `select(id, lag(id))` resolves `id` as ambiguous. The decision needs no metadata: a Window or Aggregate that was a stamped frame's root sits under that stamp's pass-through Projection, except an Aggregate with no computed output, where both readings agree. `stamp` stays idempotent (R2): a second stamp keeps every alias whose own metadata carries a native id. | Pins over `select`, a re-select of a stamped aggregate, SQL `GROUP BY` and `HAVING` over a view, the verifier's lag-through-select and SQL `first_value` repros; every new shape re-stamps unchanged; M7. | PROVEN | `attr_id_fresh.rs` (`a_projection_over_a_same_op_window_or_aggregate_mints_what_it_computes`, `sql_group_by_and_having_over_a_view_mint_the_computed_outputs`, the `assert_idempotent` calls); `attr_id_verify.rs` (`va_lag_through_select_is_ambiguous_with_its_source`, `va_first_value_through_sql_over_a_view_gets_a_fresh_id`, `va_union_of_unions_takes_the_outermost_first_input`, `va_alias_over_an_aliased_cast_that_carries_an_id_is_re_minted_once`). |
| C-019 | A `repark.attr` value another process wrote is never trusted (VA1-5). An id is `a`, a 16-hex per-process prefix (a `RandomState` hash of the process id, fixed once per process) and a 12-hex counter; `AttrId::is_native` accepts only this process's prefix and length. `stamp` keeps only native ids, so a source whose field carries a foreign id is re-minted once and a stamped frame keeps its ids across a re-stamp. DataFusion's default parquet read (`skip_metadata`) already drops field metadata; a read with `skip_metadata(false)` brings the footer's ids in, and `stamp` replaces them. | A parquet file whose footer carries `repark.attr`, read twice with `skip_metadata(false)`: fresh native ids, no id shared between the reads, idempotent, and the frame aggregates and runs; the default read carries no id; M8. | PROVEN | `attr_id_fresh.rs` `a_source_id_minted_by_another_process_is_re_minted_once`. |
| C-020 | `lateral_join` re-mints the right side's colliding ids at the left frame's width, like `requalify_join_sides` (VA1-3), and the join no-op pin asserts that the joined ids are present and distinct before it asserts the no-op (VA1-4). | A self lateral join (inner and left) at the binding; the strengthened S1 pin; the lateral pin red with the re-mint removed. | PROVEN | `crates/repark-python/src/tests.rs` `binding_re_mints_lateral_join_collisions_of_a_self_join`; `attr_id.rs` `the_join_re_mint_leaves_a_join_of_distinct_attributes_unchanged`. |
| C-021 | Measured on DataFusion 54.1.0 alone (VA1-2), correcting C-001's "values carry none" and §2's "fresh Field": a negated group key and the `first_value`, `last_value`, `lag`, `lead` and `nth_value` outputs copy their argument's id (a cast does too, C-001). C-017 is the rule that overrides them. | A DataFusion-only pin over a tagged frame. | PROVEN | `attr_id_fresh.rs` `datafusion_copies_the_argument_id_onto_these_computed_outputs`. |
| C-022 | After S1b the S0 replay stays byte-identical to `main.json` on every judged cell within the 597 s median bar, and the neighbour sweep stays green. | Rebuild, sweep, three replays, `compare.py`. | OPEN (the same 4 judged cells as C-016; median 606.48 s, over the bar) | Sweep: 2159 passed, 19 skipped, 2 xfailed, 0 failed. Replay on the S1b build: 601.54 / 610.58 / 606.48 s, median 606.48 s (load average about 8.5 against about 6.5 during the S2b runs); every run's cells equal the S2b runs' cells, so S1b changes no cell, and the 4 cells of C-016 still differ from `main.json` with 0 moved away from Spark. Evidence: `/tmp/oc-worker/direct/wo/attr-id-1/s2b/cmp-b*`. |

**Mutation record S1b (2026-09-30).** Each mutation edited the named file, ran
`cargo test -p repark-core --lib attr_id` (the lateral one `cargo test -p repark-python --lib
binding_re_mints_lateral`), and was reverted from a copy; `git diff --stat` then showed only the
round's own edits.

| # | Mutation | Red |
|---|---|---|
| M5 | A Window's outputs past its input width count as not computed, so they keep the ids DataFusion copied | `every_window_output_gets_a_fresh_id_and_the_input_passes_through`, `a_projection_over_a_same_op_window_or_aggregate_mints_what_it_computes`, `va_lag_and_lead_window_roots_get_fresh_ids`, `va_lag_through_select_is_ambiguous_with_its_source` |
| M6 | An Aggregate's grouping id and values count as not computed | `every_aggregate_value_the_facade_emits_gets_a_fresh_id`, `sql_group_by_and_having_over_a_view_mint_the_computed_outputs`, `va_first_and_last_value_aggregates_get_fresh_ids`, `va_first_value_through_sql_over_a_view_gets_a_fresh_id` |
| M7 | `own_id` never recognises an alias's own id, so a second stamp re-mints | 8 pins: the idempotence checks in five `attr_id_fresh.rs` pins, `stamp_gives_a_union_the_first_input_ids`, `va_union_of_unions_takes_the_outermost_first_input`, `va_alias_over_an_aliased_cast_that_carries_an_id_is_re_minted_once` |
| M8 | `AttrId::is_native` accepts any id | `a_source_id_minted_by_another_process_is_re_minted_once` |
| M9 | `lateral_join` skips the re-mint | `binding_re_mints_lateral_join_collisions_of_a_self_join` |

**VA1-6.** The `attr_id.rs` row of `df_guards/map.md` no longer cites a `/tmp` path (it cites
this ledger) and its duplicated "every other root mints" clause is gone.

## Round S3a (2026-09-30)

**Model:** muse-spark-1.3-contributor (S3a executor, guided).
**Work order:** `/tmp/oc-worker/direct/wo/attr-id-1-design.md` §4 S3a with §9c rulings Q1–Q3:
the `select` family (`select`, `__getitem__`, `__getattr__`, `_column_of`,
`_rebind_stable_name_column`) binds through `resolve` under the session's live
`spark.sql.caseSensitive`, parent Columns bind by `_attr_id`, a user `alias()` mints a
fresh id through the alias's own metadata, and the helpers only this family used are
deleted in the same commit.

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-023 | The §9c Q2 timing cut: `plan_is_stamped` answers by reference whether `stamp` would change the plan (the Projection, Union and other-root mint conditions mirrored; statement roots read stamped; a classification error reads unstamped), and `stamp_attribute_ids` returns the same `PyDataFrame` handle when stamped, keeping the cached analyzed schema. The S0 replay median returns to at or under 597 s with no cell moved away from Spark. | The mirror pin over every spawned root shape; three replays on the cut compared with `compare.py`. | PROVEN | `crates/repark-core/src/session/tests/attr_id.rs` `plan_is_stamped_matches_what_stamp_would_change`; three cut replays (`/tmp/oc-worker/direct/wo/attr-id-1/s3a/cut-1..3`): corpus-sum 590.4 / 594.7 / 586.7 s, every run 0 cells moved away from Spark, the same 4 judged diffs as C-016/C-022. |
| C-024 | The S3a cutover: `select`, `__getitem__`, `__getattr__`, `_column_of` and `_rebind_stable_name_column` bind through the one resolve rule in `column_fields.py` (`_bind_resolved_name`) under the session's live `spark.sql.caseSensitive` (native `session_case_sensitive`, read from the session the frame holds); parent Columns bind by `_attr_id` to the first held position through `attribute_column`, across plans and onto unique engine fields only; a user `alias()` mints a fresh id in the alias's own metadata (`alias_with_fresh_id`, kept by the S1b idempotence rule, read through nested aliases by `chain_id`); `DataFrame.alias` restores the parent display/engine overlay when the `SubqueryAlias` dedupes a display name. `_bind_schema_column` stays for the star/int, `_iter_bound_columns`, ordering and ambiguous-with-exact callers — no top-level helper is deleted because every family-only candidate still has a caller outside the family (origin helpers, `column_or_str_error`). | The 23 cutover pins under both case rules; the u11 C-024 twin re-projection pin (which caught the bare-ref rebuild stripping the mark); the resolve mutation (shifted position) killing 15 of 23 pins; the first replay's halt (464 moved away in 3 shapes, all fixed and re-pinned); three S0 replays with 0 cells moved away from Spark and the 44-file neighbour sweep green. | PROVEN | `python/repark/tests/test_attr_id_1_s3a.py`; `crates/repark-python/src/column/display.rs` `alias`; `crates/repark-python/src/session_runtime.rs` `session_case_sensitive`; `crates/repark-core/src/session/df_guards/attr_id.rs` `alias_with_fresh_id`, `chain_id`; `crates/repark-core/src/session/tests/attr_id.rs` `union_reads_inner_alias_ids_through_plain_machinery_aliases`; replays land in the hand-back. |

**S3a halt H-1 (2026-09-30, §5 rule 2).** The first S0 replay on the cutover
(`/tmp/oc-worker/direct/wo/attr-id-1/s3a/s3a-1`, corpus-sum 615.4 s) moved 464
EQUAL cells away (5973 diffs, 4048 FIXED, 1392 GAP-MOVED). All 464 regressions
fall in 3 shapes: 109 UNRESOLVED (the native rule folds ASCII-only while the old
facade used Python `casefold`: `ünï`/`Ünï`, `σ`/`Σ`, Kelvin/Angstrom), 330 bare
`Schema error` (a marked unqualified ref over duplicate engine names skips the
case-bind hook and dies in DataFusion analysis instead of the shaped refusal),
25 ROWS-INSTEAD (same-frame written binds converted to id binds; a union of two
user aliases sharing one id). Fixes in the same round: `_unqualified_hits`
computes unqualified hits in Python with `casefold`, grouped by held id
(qualified names keep the native relation narrowing); `_bind_stable_id_column`
fires only across plans and onto a unique engine field; `first_input_ids` reads
nested alias metadata through `chain_id` (a schema-first read was tried and
reverted: DataFusion's `union_by_name` pad projection grafts the other side's
field into the padded schema, which the `stamp_mints_where_the_first_union_input_has_no_id_to_give`
pin caught). The union pin `union_reads_inner_alias_ids_through_plain_machinery_aliases`
and the facade unicode pin `test_unicode_spelling_binds_by_casefold` pin the
fixes; both bite (the union pin fails under `own_id`, the resolve mutation kills
15 of 23). s3a-1 is kept as the halt evidence; the three gate replays run on the
fixed code.

**S3a bind fast path (2026-09-30).** s3a-1 was already 615.4 s on a quiet box
against the 597 s bar, so the resolve rule was restructured before the gate
replays: one exact hit with no folded rival binds without reading the session
rule, and the relation check plus the id-loudness check run only off that path
(`_unqualified_candidates` / `_group_candidates`; `held[position]` answers the
id inline instead of `_bound_attr_id`'s re-read). Micro-bench: 3000
`select("id", "Data", "v")` in 2.14 s vs 2.17 s on base. No behaviour change:
the neighbour batch and the mutation re-run green after it.

**S3a gap-moved taxonomy (2026-09-30, from s3a-2: 0 moved away, 3925 FIXED).**
All 1208 GAP-MOVED cells named: R1 (315) alias `:1`→dup rename with identical
rows, the S3b op still gaps; R2 (63) sensitive `F.col` unicode miss falls
through to a bare engine error (needs the unicode-case card's hook);
R3 (134) sensitive `withColumn` twin upstream replaces, so S3a correctly refuses
the absent name (S3c); R4 (42) S3b orderBy/filter on the restored dup refuses
AMBIGUOUS where Spark says UNRESOLVED (S3b owns the cond); R5 (11) sensitive
qualifier-case inversion on alias frames — DataFusion normalizes the held
qualifier at plan build, so `t` binds and `T` misses (S3e Q4 facade-held
qualifiers); R6 (12) SQL-door cross joins share one id across sides, so
qualified twins bind (S1 §8 SQL-door residue); R7 (1) `orderBy('x.id')` values
now match Spark, struct repr differs; R8 (1) oracle-`skip` harness noise;
R9 (534) qualified join-side misses now shaped UNRESOLVED (S3e Q4);
R10 (46) AMBIGUOUS text-only (bare→engine prefix/SQLSTATE); R11 (29) cond-less
miss now shaped AMBIGUOUS (orderBy cond S3b); R12 (20) the 4 S2b Q1 residues
plus 16 qualified-`F.col` engine fall-throughs (S3e). s3a-2's timing (758.8 s)
is contaminated (a reverify-deep campaign drove load to 67 mid-run); the gate
replays re-run on a quiet box.

**S3a halt H-2 (2026-09-30, §5 rule 4 / 597 s bar).** Three clean runs on the
fixed code (s3a-3/4/5, corpus-sum 636.7 / 635.6 / 616.6 s, median 635.6 s):
5252 diffs, **0 moved away**, 3925 FIXED, 1208 GAP-MOVED per the taxonomy above
(identical classification all three runs). Timing is 38.4 s over the bar
(1.277× main's 497.68 s). Root cause, measured: the gains execute. A
twin-select that raised in 0.005 ms now collects in ~9.5 ms; r5p6 (the whole
delta, +41.4 s over cut-1's 264.9 s) has 1699 newly-executing cells; a cProfile
slice shows 5.2 of 8.7 s inside native collect vs 0.9 s in select. The bind path
is at parity with base (3000×3-col select 2.14 s vs 2.17 s; twin binds 0.4 ms).
No bind-path cut can recover execution cost, so the round halts with the code
complete and all other gates green. A delegation shortcut (exact-1 ambiguous
binds inline, exact-dup raises the old text inline, no `_bind_schema_column`
re-entry) landed after the three runs; the first timed head runs caught it
changing 4 backticked-name cells (the old path matches the raw written text,
quotes intact, so it misses where the inline raise refuses), and the shortcut
now delegates quoted spellings to `_bind_schema_column` — the re-runs below
measure that final shape. Neighbour sweep on the final code:
2182 passed, 19 skipped, 2 xfailed, 0 failed (44 files, `-n 8`; +23 over S2b
are the new S3a pins).

**S3a ruling Q2 (2026-09-30).** R5 (sensitive qualifier-case inversion on alias
frames), R9 (qualified join-side misses) and the qualified half of R12 are
S3e-owned residues per §9 Q4 (facade-held qualifiers into `resolve`); S3a keeps
the shaped miss. R6 stays an S1 §8 SQL-door residue, R2 with the unicode-case
card, R3 with S3c, R4/R11-orderBy with S3b.

**S3a like-for-like verdict (2026-09-30, ruling Q1).** The timed driver
(`s3a/replay_timed.py`, a mechanical copy of `replay.py` that records per-cell
wall time; byte-identical answers on the oracle corpus) ran 3× on the cutover
(612.3 / 614.2 / 621.3 s corpus-sum) and 3× on the parent `9f372d94`
(589.1 / 586.5 / 587.8 s). (a) Over the 39,095 cells whose outcome class is
unchanged (EQUAL→EQUAL plus same error cond), head/base = 234.4/235.4 s
median-of-3, **ratio 0.9959** (pairs 0.9845 / 0.9919 / 1.0059) — under the 1.2×
bar on every shape (EQUAL 0.9948, GAP-ROWS 0.9913, nocond 0.9991,
UNRESOLVED 1.0026, AMBIGUOUS 0.9811). Timing coverage 99.99% both sides.
(b) The 3,925 FIXED cells cost 18.4 s total, no bar. Base choice: the parent
commit, which isolates this round's overhead (origin/main would conflate the
whole landed unit and needs a full separate build). All three head runs repeat
5252 diffs / 0 moved away / 3925 FIXED; the quoted-guard amend restored the
s3a-3 answers cell-for-cell (only nondet row-order/candidate-case noise differs
run to run).

## Round S3b (2026-10-01)

**Model:** muse-spark-1.3-contributor (S3b executor, guided).
**Work order:** `/tmp/oc-worker/direct/wo/attr-id-1-design.md` S3 with
`/tmp/oc-worker/direct/wo/attr-id-1-s3b-filter-sort.md`: `filter(str)` /
`where(str)`, `filter(Column)` and `orderBy` / `sort` (string names and parent
Columns) bind through the S1 `resolve` under the live session rule; the
family's helpers are deleted in the same commit.

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-025 | The S3b cutover: `filter` (both arms) and `_sort_specs` bind through `column_fields` (`_rebind_free_names`, `_quote_filter_sql_identifiers`, `_bind_sort_key`) over the new `frame_names::sort_names` (`sort_shape`, `grandchild_key`, `bind_free_names`, bound via `dataframe_names.rs`). One id binds; several refuse (`AMBIGUOUS_REFERENCE` on filter, oldest id on a Project sort, `UNRESOLVED_COLUMN` elsewhere); a miss passes through, a sort miss tries the join grandchild first. No exact-preference on either door: an exact spelling among folded rivals refuses, as live Spark does. Deleted in the same commit: `_quote_filter_ident_token`, `_quote_filter_idents_in_fragment`, `DataFrame._quote_filter_sql_identifiers`, `_SQL_LITERAL_KEYWORDS` (the set re-homed in `column_fields`). | The 22 facade pins under both case rules; the 15 Rust pins; the resolve-skip mutation killing the 4 filter pins; the pre-existing `test_filter_predicate_rewrite.py` suite green (it caught the first cut's exact-preference and the dropped literal skip); three S0 replays with 0 cells moved away from Spark, `r2` twinjoin_filt and `r3` ob_agg_max closed, and the 44-file neighbour sweep green. | PROVEN (pins + mutation; replay lands in the hand-back) | `python/repark/tests/test_attr_id_1_s3b.py`; `crates/repark-core/src/session/tests/attr_id_s3b.rs`; `crates/repark-core/src/session/df_guards/sort_names.rs`; `python/repark/src/repark/spark/column_fields.py` `_rebind_free_names`, `_resolve_sort_name`, `_bind_sort_key`, `_bind_filter_token`; replays land in the hand-back. |
| C-026 | Grep proof that each deleted helper had no caller outside the family: `_quote_filter_ident_token`, `_quote_filter_idents_in_fragment` and `_SQL_LITERAL_KEYWORDS` appear only in `core.py` (deleted), the frozen surface `_dfcore_1_expected.py` (mirror updated in the same commit), and `test_filter_predicate_rewrite.py` docstrings (homes repointed); `DataFrame._quote_filter_sql_identifiers` appears only in `core.py` (deleted method, one docstring repointed) and the same test docstrings. | The grep output recorded at commit time; `test_dfcore_1_exports.py` green. | PROVEN | Commit grep; `python/repark/tests/test_dfcore_1_exports.py` 10 passed. |
| C-027 | The folded-lambda fallback: a filter-string lambda reference matching its parameter only by folding (insensitive rule) raises `_FoldedLambdaFallbackError`, and `_quote_filter_sql_identifiers` reruns the whole predicate through `_main_path_filter_sql`, a verbatim port of main's fold-everything quoter. Exact-rule references never fold and never trigger. | The 4 nested-collision pins (insensitive fold, sensitive distinct, reverse nesting, single-level folded); the M5 mutation (re-raise kills the 3 insensitive pins, sensitive stays green); the t-head-2 re-replay with 0 moved. | PROVEN (pins + mutation; replay lands in the hand-back) | `python/repark/tests/test_attr_id_1_s3b.py`; `python/repark/src/repark/spark/filter_quote.py` `_main_path_filter_sql`; replays land in the hand-back. |
| C-028 | The fallback fires only on a nested parameter collision: `_scopes_have_folded_collision` reports an inner lambda parameter that folds to an enclosing parameter with different spelling, and `_bind_filter_token` raises `_FoldedLambdaFallbackError` on a folded reference only then. Single-level and collision-free nested shapes stay on the binder. | The 3 stays-on-binder pins plus the 4 C-027 pins; M6 (force fallback) and M7 (force binder) each red on its side; the t-head-2 re-replay with 0 moved and the 23 cells back to FIXED. | PROVEN (pins + mutations; replay lands in the hand-back) | `python/repark/tests/test_attr_id_1_s3b.py`; `python/repark/src/repark/spark/filter_quote.py` `_scopes_have_folded_collision`; replays land in the hand-back. |
| C-029 | The B1 join-dup guard covers the two alias shapes: `join_dup_below_wrappers` descends id-extending Projections (above starts with below), and the qualifier-bound filter arm refuses a one-id multi-hit over a join dup exactly like the unqualified arm. | The 2 alias-dup pins under both rules (4 red before the fix); the t-head-2 re-replay with 0 S3a gains lost. | PROVEN (pins; replay lands in the hand-back) | `python/repark/tests/test_attr_id_1_s3b.py`; `crates/repark-core/src/session/df_guards/sort_names.rs` `join_dup_below_wrappers`; replays land in the hand-back. |

**S3b first-cut halt (2026-10-01, self-caught before commit).** The first cut
carried an exact-preference on both filter doors (`exact_hits or folded_hits`
in `_bind_filter_token`, `exact_hit` in the Rust walker) and dropped the
`_SQL_LITERAL_KEYWORDS` skip and the actual-spelling ambiguity echo. The
pre-existing `test_filter_predicate_rewrite.py` suite went 13 red: exact
spellings among case twins bound instead of refusing (str and Column doors),
`true` / `false` / `null` bound to same-named columns, and the message shape
pin failed. The cited `wcV` / `caseF` cells never needed the preference: `caseF`
is `select("id", "v", F.col("V"))` (one attribute under two displays, so the
pure rule binds) and `wcV` carries a single `V`. Fix in the same round: the
pure resolve rule on both doors, the keyword skip and the actual-spelling echo
restored, the two pins that encoded the preference rewritten to the live rule.

**S3b gate halt H-1 (2026-10-01, brief rule 1: a cell EQUAL on main moves
away).** Head replay 1 (`s3b/t-head-1`, oracle `spark.json`/`main.json` copied
from `t-base-1` per the S3a procedure): 9805 diffs, **179 REGRESSION** (main ==
Spark, head != Spark), 7426 FIXED. All four §9c cells are FIXED
(`r2.{F,T}_oj_twinjoin_filt` cond AMBIGUOUS_REFERENCE, `r3.{F,T}_ob_agg_max`
byte-equal rows), so the named goal lands but the neighbours fail the round.
S3a no-regress also fails: 203 cells where S3a-head == Spark no longer match
(the 179 plus 24 S3a-only gains). No further gate runs (replays 2-3, timing,
sweep, `gate.sh`) — the signal is structural, not noise. Buckets, each with
the Spark-measured behaviour that falsifies the brief's stated rule:

- B1, 103 cells (r2/r5p6/r5p6t filter, str and Column doors): Spark+main
  AMBIGUOUS_REFERENCE, head bare `Schema error: Ambiguous reference to
  unqualified field v`. A cross/self join carries one id on both sides (probed:
  `crossJoin` ids repeat per side) under duplicate engine names, so the walker
  binds and the marked ref dies bare in DataFusion analysis past the shaped
  hook refusal. The S3a select path already learned this (bind onto unique
  engine fields only); the walker lacks the guard. 24 further `j_cross` filter
  cells (S3a-only gains) return rows from one side instead of refusing.
- B2-sort, 60 cells (`r4p4.*_so proj*`): Spark+main ROWS, head shaped
  UNRESOLVED_COLUMN. `select("id").orderBy(v...)`: Spark resolves the sort key
  against the input scope, not the output displays. The brief's "0 hits →
  UNRESOLVED_COLUMN" premise is false for sort keys outside the output.
- B2-lambda, 5 cells (r3lam4/r4p4/r5p7, all caseSensitive=true):
  `filter("exists(arr, X -> X > 4)")` — Spark+main ROWS, head native
  UNRESOLVED of the lambda variable. Under Exact the new quoter leaves the
  var token bare and the downstream native binder refuses it; the old
  always-fold quoter had quoted it (masking the tokenizer's lambda-blindness).
- B3, 8 cells (`q_filt`): Spark+main AMBIGUOUS_REFERENCE, head bare `Schema
  error: No field named q.v`. `filter("q.v > 15")` on an aliased twin frame:
  the qualifier is facade-held (lost from the plan by `withColumn`), so the
  qualifier-aware miss passes the token through to a bare DF error where the
  old qualifier-blind quoter raised shaped AMBIGUOUS on the bare name. S3e Q4
  owns facade-held qualifiers.
- B4, 3 cells (`r3.*bd_struct_named_*`): Spark+main ROWS, head bare `Schema
  error: No field named t.id`. `filter("T.id > 3")` on a `(T struct, id)`
  frame is struct field access, not qualifier+name; the new dotted
  tokenization broke it (the old pattern matched bare idents only — the brief
  said to keep the parsing as-is).

Recommended repairs, for the ruling: B1 — the S3a uniqueness guard in the
walker (leave the token for the hook when the engine name is not unique);
B2-sort — miss falls through to the engine instead of refusing; B2-lambda —
tokenizer skips lambda-bound variables, or the native binder does (needs an
owner decision on which layer owns lambda scope); B3 — qualified-miss
fallback ruling (old qualifier-blind raise vs a shaped miss); B4 — restore
bare-ident tokenization and resolve dots as struct access first. B2-sort, B3
and B4 each need a decision the brief did not make, so the round halts rather
than reworking the rule unruled.

**S3b H-1 fix (2026-10-01, §9e rulings, pins attr-id-1/C-026).** All four
rulings implemented; the 179 buckets re-measured cell by cell below (replay
`t-head-2` pending at commit time).

B1: the walker takes the shared `engine_field_is_unique` guard plus a
`join_dup_below_wrappers` walk for the shape the guard cannot see — an
aliased join whose `SubqueryAlias::try_new` dedup projection renames the
engines unique (`v`, `v:1`) while preserving the degenerate one-id ids.
The walk descends single-input nodes and id-preserving projections (the
dedup preserves ids without being transparent); a one-id multi-hit token
over such a join refuses `AMBIGUOUS_REFERENCE` on both doors. Same-frame
dups (cc-twins, whose facade-renamed engines are already unique) still
bind. The str-door echo lists every candidate, as main does.

B2-sort: a 0-hit key falls through to the engine (plain column, main's
path); the grandchild-ambiguous refusal stays. Spark-measured: a 2-id
project key binds oldest (ROWS, committed pin stands) and a join dup
refuses `UNRESOLVED_COLUMN` — sort never yields `AMBIGUOUS_REFERENCE`,
so the fix brief's "2-id AMBIGUOUS" pin wording is loose; the committed
`test_orderby_of_join_dup_is_unresolved` is the pin, with Spark's tag.

B2-lambda: the tokenizer owns lambda scope (RC4-1/RC4-6/RC5-2 port from
`pull/881/head`): decl sites quoted, bodies resolve params, outer columns
bind under both rules. All three pins measured verbatim against live
Spark, including the shadowed-outside shape (`v < 100 AND exists(arr, v
-> v > 4)` → `[[1], [5]]`).

B3/B4: a dotted token routes on plan qualifiers only. A plan-qualifier
tie with a struct column goes qualifier-first — Spark-measured
(`T.id > 5` on the disambiguating frame → `[[9]]`) — with a main's-path
fallback on a qualifier miss (struct access, alias3 shape); anything else
takes main's bare-ident path with the byte-identical collision raise.
Under Exact a tie still reads struct-first because DataFusion lowercases
the `SubqueryAlias` qualifier (`T` → `t`, so the live rule misses):
kept main-divergence, S3e Q4 owns facade case. Exact qualified-join
filters (`L.v`) likewise take main's path (plan carries folded `l`/`r`).

Mutations (each red, then reverted; tree verified identical after):
M1 drops the B1 bound-arm gate — alias pins (both doors, both rules) and
`jcross-col` go red; `jcross-str` stays green via the engine/probe
backstop and cc stays green by correct bind. M2 refuses on 0 sort hits —
the fallthrough and grandchild pins go red. M3 blinds the tokenizer —
`exists` goes red; `transform`/shadow lock outcomes (quoting coincidence).
M4 routes every dotted token through resolve with miss raising — struct,
facade-held and Exact-coincidence pins go red; qualifier hits stay green.

Known residues kept: `qalias_str` (`q.v` on the alias frame — one
degenerate id, unique engine — binds ROWS where Spark refuses; the
rulings' letter, main-equal); `alias_sort` (sort binds one id, main's
path); Exact ties and Exact qualified joins (above, S3e). The
`column_fields.py` split (`filter_quote.py`, pure move, entry stays) was
forced by the size gate: S3b left the file at 1209 with no exception row
and `gate.sh` never ran on that round. `case_bind.rs` sits at exactly
1000 (at ceiling, untouched).

## S3b H-1 follow-up: folded-lambda fallback (2026-10-01)

Post-reboot `t-head-2` compare on 6be8301f: 7666 FIXED, but 14 EQUAL-moved
(all nested-lambda case collision: `exists(arr, X -> exists(arr, x ->
X > x))` and 3 siblings, both nest orders, plus 6 RC6-4 reverse-nesting
cells). All 14 were EQUAL at 15c1ea17: the H-1 B2-lambda port introduced
them, so this is a port bug, not a new ruling. Cause: the skip leaves
`X`/`x` for the engine, which binds lambda variables case-sensitively
(error, then `[1, 5]`), while Spark folds them to one variable (`[]`).
Main folds every bare ident to display spelling, which reproduces Spark
there. Fix: a lambda reference matching its parameter only by folding
raises `_FoldedLambdaFallbackError`; the entry reruns the whole predicate
through `_main_path_filter_sql`, a verbatim port of main's quoter
(string-identical on 9 probes). Exact-rule references never fold and
never trigger. Mutation M5 re-raises instead of falling back: the 3
insensitive pins go red, the sensitive pin stays green; reverted.
Also fixed: the H-1 dotted-lambda subscript used a direct
quote-doubling that `check_python_conventions.py` refuses; it now uses
`escape_sql_single_quotes` (same bytes).

## Gate narrowing: collision-gated fallback (2026-10-01)

The post-fix `t-head-2` compare showed 0 moved but FIXED down 23
(7666 to 7643): the fallback fired on every folded lambda match and
rerouted 23 cells where the binder was Spark-right (single-level
folded references, sibling scopes, nested scopes without a parameter
collision) to main's quoter, which raises or misbinds on twin frames.
Fix: `_scopes_have_folded_collision` detects an inner parameter that
folds to an enclosing parameter with different spelling, and the raise
fires only then. The trigger is exact on 14 probe shapes. New pins:
backticked declaration plus folded reference, dotted folded head, and
nested no-collision on a twin frame, each expectation measured on live
Spark 4.1.2. Mutations M6 (force fallback: the 3 binder pins red) and
M7 (force binder: the 2 collision pins red with the physical
LambdaVariable error); both reverted. M6 caught a weak pin first: the
nested pin passed under both paths until the frame gained a true twin
column (`select` plus aliased literal; `withColumn` replaces
case-insensitively and made no twin).

## Gate j_cross: alias-dup refusals (2026-10-01)

`verify_gains.py` on the post-fix `t-head-2` lost 4 of the 203 S3a
gains (`r5p7` `j_cross` `alias_wc|filt` and `alias|q_filt`, both
rules; lost identically pre-fix, so an H-1 B1 gap, not a fallback
regression). Both join `v` fields share one provenance id (both
derive from the same source column), so the S3b rule binds; the B1
refusal then depends on `join_dup_below_wrappers`, which stopped at
the `withColumn` projection (above ids extend below) and never ran on
the qualifier-bound arm. Fixes: the walk descends id-extending
projections, and the qualified arm carries the symmetric guard with
Spark's qualifier-qualified echo. Pins written first (4 red), green
after. Also fixed in this commit: the H-1 `redundant_closure` clippy
lint in `logical_names.rs` (clippy's suggestion, same bytes), which
blocked `make rust-clippy`.

## S3b gate record (2026-10-01, head 1faedb48)

Replays `t-head-2/3/4` (foreground, `ulimit -v 67108864`): 43946
cells each, 0 EQUAL-moved on all three, 7672 FIXED each
(deterministic across runs; +20 over the pre-fix head: 14
nested-lambda, 4 S3a `j_cross`, 2 `j_cross` Column-door bonus),
0 of the 203 S3a gains lost, the four §9c cells FIXED. The
`setup J3/SJ2/TT failed AnalysisException` lines are the cs2 fixture
setup try/except outcomes, byte-identical on base and head.
Timing (`like_for_like_h1.py`, bases `t-base-1/2/3`): pair ratios
1.0139/1.0656/1.0366, median-of-3 1.0285x against the 1.2x bar;
7672 FIXED cells cost 37.5s reported separately; coverage 0.9999.
Sweep (`-n 8`, 45 files): 2236 passed, 19 skipped, 2 xfailed.
`gate.sh`: 15/15 GREEN, including the parity suite (green after the
orchestrator removed the torn `/tmp/muse-worker` snapshot) and both
Rust lib suites. Pin file: 52 green.

## Round S3c (2026-10-01)

**Model:** muse-spark-1.3-contributor (S3c executor, guided).
**Work order:** `/tmp/oc-worker/direct/wo/attr-id-1-design.md` §4 S3c with
`/tmp/oc-worker/direct/wo/attr-id-1-s3c-withcolumn.md` and the §9f ruling
(a rename mints one fresh id per renamed position): `withColumn(s)` and
`withColumn(s)Renamed` bind through the shared live-rule hit computation;
each replaced, appended or renamed position gets its own fresh id.

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-030 | The S3c cutover: `_live_rule_hits` (`column_fields.py`) computes a literal name's hit positions under the session's live `spark.sql.caseSensitive` — exact hits under the exact rule, exact plus Java-folded hits under the insensitive rule (`_java_case_equal` reproduces Java `String.equalsIgnoreCase`: same length plus per-character upper/lower/equal, U+0130 read as `I`; Python `casefold` matches `ß`/`SS` and misses `İ`/`i` where Spark does the opposite, measured live); no qualifier split, as before. `with_columns` replaces every hit of each key (last key wins a position two keys hit, as before) and appends hit-less keys in dict order; `with_column_renamed` renames every hit and no-ops a miss by returning the same frame; `with_columns_renamed` rewrites sequentially under the live rule instead of exact-only. Each replaced or appended position keeps its `.alias` fresh id; the singular rename keeps its `Column.alias` route and the plural rename moves its native plain alias to the fresh-id alias (`PyColumnParts.alias`, same `Column` construction otherwise). Colliding final names materialize through the display overlay; the Group F duplicate-finals refusal is lifted (EX-DF-18 FIXED). `_rebind_stable_name_column` re-raises an unqualified `AMBIGUOUS_REFERENCE` instead of falling through to a bare engine error; misses and qualified names still fall through (S3e owns qualified). No helper is deleted: `_iter_bound_columns`, `_resolve_getitem_column_name`, `_rebind_origin_column`, `_bind_engine_display_column` and `_engine_field_for_display` each keep a caller outside the family (commit grep). | The 29 pins (41 instances) under both case rules; the live-Spark probe series (banner 4.1.2 America/New_York); mutations M1–M4 each red on their pins and reverted; the S0 replay with 0 cells moved away from Spark, the 7672 S3a/S3b gains kept, and the neighbour sweep green. | PROVEN (pins + mutations; replay lands in the hand-back) | `python/repark/tests/test_attr_id_1_s3c.py`; `python/repark/src/repark/spark/column_fields.py` `_live_rule_hits`; `python/repark/src/repark/spark/dataframe/core.py` `with_columns`, `with_column_renamed`, `with_columns_renamed`; probes `s3c/spark_probes_s3c*.py` with `.out` files; replays land in the hand-back. |
| C-031 | The pins bite: 21 of 41 fail on the base tree (every live-rule, fan-out, renamed-twins, Column-door, materialize and Java-fold pin); the first commit fails the 6 gate-fix pins; M1 (live-rule bypass, unconditional fold) reds the 7 sensitive-rule pins; M2 (one alias shared across a key's hit positions) reds the 2 bare-value per-position pins — a literal keep-old-id variant reds 16 through broken values and is recorded as too coarse; M3 (both rename paths keep the id through the plain alias) reds the 4 renamed-twins pins; M4 (`casefold` for the Java fold) reds the 3 Java-divergent pins. Each mutation is reverted from a copy with the intended diff intact. | The fail-before runs and the four mutation runs below. | PROVEN | This ledger's mutation record; `git diff` after each revert. |

**S3c halt Q1 (2026-10-01, brief rule 5: Spark contradicts a bullet).**
The brief said "a renamed column keeps its id". Live Spark 4.1.2, measured
twice (executor probes `s3c3`/`s3c4`, orchestrator `rename_probe.py`), binds
`filter` on `select("v", "v")` twins and raises `AMBIGUOUS_REFERENCE` on
`filter`/`select(str)`/`select(F.col)` after either rename, under both case
rules: each renamed position is a distinct fresh attribute. The round halted
with no commit and resumed under the §9f ruling (rename mints fresh per
position, as an alias does). All other brief bullets were confirmed by the
same probes: replace and rename fan out to every hit with no ambiguity
refusal (twins of one attribute and two attributes sharing a display alike),
a rename miss is a no-op, a variant name replaces insensitive (taking the
written spelling) and appends sensitive, and folded `withColumns` keys raise
Spark's `COLUMN_ALREADY_EXISTS` insensitive — which stays a gap, since the
brief keeps today's last-wins/no-refusal key behaviour unchanged.

**S3c Column-door fix (2026-10-01).** The renamed-twins `select(F.col)` pin
caught a pre-existing S3a gap: `_rebind_stable_name_column` swallowed every
`AnalysisException` from `_bind_resolved_name`, so the Column door died bare
where Spark refuses shaped (established shape: 2-id twins `select(F.col)`
is bare on the base tree). The swallow is now precise: an unqualified
`AMBIGUOUS_REFERENCE` is re-raised (Spark refuses every unqualified 2-id
reference, so no EQUAL cell can depend on the fall-through); misses and
qualified names keep today's path. The replay guards the shared path.

**Observed, out of scope (2026-10-01).** Free `F.col` references in select
compounds and `withColumns` values against duplicate-display (overlay)
frames die with a bare engine error even where Spark binds (1-id twins
included); the `withColumns` pre-aliasing (`v AS v` display) skips the
stable-name rebind and the value path is untouched by S3c. Base behaves
identically. A corpus cell that moves only through this gap is named there.

**Mutation record S3c (2026-10-01).** Each mutation edited the named file,
ran `pytest python/repark/tests/test_attr_id_1_s3c.py`, and was reverted
from a copy; `diff` against the copies was empty afterwards and the file
is 36 green.

| # | Mutation | Red |
|---|---|---|
| M1 | `_live_rule_hits` always folds (`and False` on the exact arm) | The 7 sensitive-rule pins: variant append, folded-rival replace, folded-keys replace-and-append, rename-variant miss, plural exact-keep, the sensitive `İ` miss, insensitive-then-sensitive build |
| M2 | One `replacement.alias` per key shared across its hit positions | The 2 bare-value per-position pins (computed-value pins stay green: `stamp` mints per position whatever the alias shares) |
| M2-coarse | Literal keep-old-id: plain-alias `Column` construction for the replacement | 16 red through broken values (the thin construction drops facade state); rejected as too coarse, recorded here |
| M3 | Both rename paths keep the id (singular manual plain-alias construction, plural back to `bound._inner.alias`) | The 4 renamed-twins pins (singular and plural, both rules) |
| M4 | `_java_case_equal` replaced by `casefold` equality | The 3 Java-divergent pins (`STRASSE` append and rename no-op, the insensitive `İ` rename; the `ẞ` pin stays green: `casefold` matches `ẞ`/`ß` too) |

**S3c gate fixes (2026-10-01).** The first replay (`s3c/t-head-1`) showed
8517 FIXED and 4 REGRESSION cells. Three are `withColumnsRenamed` shapes
whose folded matches newly collide (`(a, A)` plus `{'A': 'z'}` and two
`tw` twins): main's exact-only matching returned wrong-named rows that the
`dtypes`+`rows` oracle shape counts as EQUAL (it ignores names), while the
new code hit the Group F duplicate-finals refusal. Spark answers rows, and
the `select` duplicate-display overlay already materializes the shape
(multi-name inputs skip the refusal today), so the fix lifts the refusal:
every map now matches Spark bit-for-bit (EX-DF-18 FIXED, with the two
pre-existing pins converted to row assertions). The fourth cell is
`r1.F_uni_nss0_wcrs` (`withColumnsRenamed({"STRASSE": "z"})` on
`[straße, v]` insensitive): Spark's Java resolver misses (`ß` never folds
to `SS`) while Python `casefold` matches. The fix gives the S3c family its
own Java fold (`_java_case_equal`: same length plus per-character
upper/lower/equal, U+0130 read as `I`, measured against live Spark on the
`ß`/`SS`, `ẞ`/`ß` and `İ`/`i` pairs); the S3a/S3b `casefold` computation
is untouched, and its convergence on Java belongs to the unicode-case card
(RC3-5), which owns version drift as well. The first commit fails the 6 new
gate-fix pins; the gate commit carries the fixes with 5 new unicode pins.
`gate.sh` then red on the CAP-1 mirror (`test_cap_1_source_file_line_cap`:
the `core.py` row still read 3846); a third commit ratchets the mirror row
to 3836 with its map row.

## Round S3d (2026-10-01)

**Model:** muse-spark-1.3-contributor (S3d executor, guided).
**Work order:** `/tmp/oc-worker/direct/wo/attr-id-1-design.md` §4 S3d with
`/tmp/oc-worker/direct/wo/attr-id-1-s3d-drop-na.md`: `drop`,
`dropDuplicates`, `fillna`/`na.fill` and `dropna`/`na.drop` bind through the
single resolve rule, fanning out to every hit position where Spark fans out
and refusing with Spark's error class where Spark refuses.

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-032 | The S3d cutover: `spark/subset_resolve.py` binds the family under the session's live rule over the shared `_live_rule_hits` plus id grouping (`_grouped`: one id binds every hit, two refuse) and the shared guard (multi-hit binds need unique engine fields and no join dup below the wrappers). `drop(str)` fans out to every hit and no-ops a miss; `drop(Column)` binds a parent Column by `_attr_id` to every id position (one side of a self-join), no-ops a resolved-but-absent Column (alias, other frame, compound, literal) and a miss, and refuses a two-attribute display; dotted or backticked free Columns keep the native qualified path. `dropDuplicates` fans out every key with no refusal and misses with `_LEGACY_ERROR_TEMP_1201`. `fillna` (subset and dict, last folded key wins) and `dropna` fan out one id positionally, refuse two ids past the guard, and miss with `UNRESOLVED_COLUMN.WITH_SUGGESTION`. A parent id position whose engine is shared with a kept position (a bare duplicate-engine frame, which cannot drop one position by name) takes the base-identical native path; bridge frames and desynced overlays keep their legacy paths. No helper is deleted: `_name_of`, `_resolve_getitem_column_name`, `_normalize_subset`, `_engine_field_for_display`, `_bind_engine_display_column` and `_iter_bound_columns` each keep a caller outside the family (commit grep); the family's own matching was inline and moved into the new module. | The 50 pins (84 instances) under both case rules, each expectation live-Spark 4.1.2 verbatim; mutations M1–M3 each red on their pins and reverted; the S0 replay with 0 cells moved away from Spark, the 8558 S3a/S3b/S3c gains kept, and the neighbour sweep green. | PROVEN (pins + mutations; replay lands in the hand-back) | `python/repark/tests/test_attr_id_1_s3d.py`; `python/repark/src/repark/spark/subset_resolve.py`; probes `s3d/spark_probes_s3d*.py` with `.out` files; replays land in the hand-back. |
| C-033 | The pins bite: 40 of 84 fail on the base tree (every ambiguity, miss-shape, live-rule, parent-id, compound, legacy-cond and join-dup pin); M1 (grouping binds every id count) reds the 14 two-attribute and join-dup refusal pins; M2 (parent Columns bind by display) reds the 10 id, sided, alias and other-frame pins; M3 (every fan-out stops at the first hit) reds the 14 fan-out pins except the `dropDuplicates` twins, which dedup identically by construction. Each mutation is reverted from a copy with the intended diff intact. | The fail-before run and the three mutation runs below. | PROVEN | This ledger's mutation record; `git diff` after each revert. |
| C-034 | The 7bc788dd corrections: the insensitive folds are codepoint Java (fold A is `Character.toLowerCase` per codepoint, fold B is codepoint `equalsIgnoreCase`: equal length plus per-codepoint equal/upper/lower) over int-version Java tables verified against a full-codepoint Zulu-17 dump — expansion pairs (`ß`/`SS`, U+0130/`i`+U+0307, U+FB00/`FF`) and newer-than-Java scripts (Vithkuqi, U+A7Cx, Georgian U+1C89/8A) miss, Deseret hits; one id closes over every same-id position in both case rules (a sensitive exact hit fans out too); a one-id multi-position bind under a union binds the positionally-first hit only for `fillna`/`dropna`/free-Column `drop` (native `union_below_wrappers` through transparent nodes and id-subset Projections, stopping at joins); `drop(str)` and `dropDuplicates` keep every-hit fan-out. | The 19 added pins plus the 9 fold pins (probes s3d8..11, every expectation live-Spark verbatim); the 4 `union_below_wrappers` Rust pins; the post-R-S3d-1 mutation re-run; the S0 replay with 0 cells moved away from Spark and the 8558 gains kept. | PROVEN (pins + mutations; replay lands in the hand-back) | `python/repark/tests/test_attr_id_1_s3d.py`; `python/repark/src/repark/spark/subset_resolve.py` `_close_positions`, `_trim_union_first`; `crates/repark-core/src/session/df_guards/sort_names.rs` `union_below_wrappers`; probes `s3d/spark_probes_s3d8*.py` with `.out` files; replays land in the hand-back. |
| C-035 | Ruling R-S3d-1: the folds move to `repark_common::java_case` (`fold_a_equal`, `fold_b_equal`, correction tables for the Java-17 divergence) with a full-codepoint unit test against the compacted dump; `repark-core` re-exports them and `crates/repark-python/src/dataframe_names.rs` exposes `java_fold_hits(written, displays, mode)`; `subset_resolve._hits_folded` takes a mode and calls it, `column_fields._live_rule_hits` calls mode `b`, and `_java_case_equal` plus the Python tables and helpers are deleted. The 41 S3c pins are byte-identical before and after (no halt); a 7249-pair native-vs-old sweep differs only on the intended drift corrections. | The 7 `java_case` Rust pins; the S3c (41) and S3d (111) suites green; the sweep record. | PROVEN | `crates/repark-common/src/java_case.rs` + `java_case_dump.txt`; `crates/repark-python/src/dataframe_names.rs` `java_fold_hits`; replays land in the hand-back. |

| C-036 | The union-trim correction: `union_dup_below_wrappers` tracks bound positions through the wrappers and a Projection continues only when it maps every position to itself (Spark re-resolves through any name/Column projection: reorder and dup-creating selects fan out, star/rename/append keep union lineage, probes s3d12..14); the Union arm fires only when two or more distinct positions reach it, so a dup created above the union (`unionbn`) never trims. | The 10 `sort_names` Rust pins; the S3c (41) and S3d (111) suites green; the S0 replay with 0 cells moved away from Spark. | PROVEN (pins; replay lands in the hand-back) | `crates/repark-core/src/session/df_guards/sort_names.rs` `union_dup_below_wrappers`; `crates/repark-python/src/dataframe_names.rs`; `python/repark/src/repark/spark/subset_resolve.py` `_trim_union_first`; probes `s3d/spark_probes_s3d12*.py`, `s3d13*.py`, `s3d14*.py` with `.out` files; replays land in the hand-back. |

| C-037 | The fold-truth correction: mode `b` is OpenJDK `String.equalsIgnoreCase` down to the lower-of-uppers step (the step that equates U+0130 with `i`/`ı`; verified against the `Analyzer.resolver` bytecode path and a JVM `equalsIgnoreCase` probe), restoring the four S3c `nti2` gains; mode `a` is equal length plus `String.toLowerCase` (U+0130 expands, U+03A3 takes final-sigma context) for `fillna`/`dropna`/free-Column `drop` (probes s3d16..17). Under a union, `fillna`/`dropna` refuse two or more exact spellings with `AMBIGUOUS_REFERENCE` and otherwise bind the positional-first hit even past an exact later hit, while free-Column `drop` trims silently; `drop(str)` and `dropDuplicates` fan out with no trim and no refusal. | The 7 `java_case` Rust pins (extended pairs); the S3c (43) and S3d (122) suites green; the S0 replay with 0 cells moved away from Spark. | PROVEN (pins; replay lands in the hand-back) | `crates/repark-common/src/java_case.rs` `fold_b_equal`, `string_lower_equal`; `crates/repark-python/src/dataframe_names.rs` `java_fold_hits`; `python/repark/src/repark/spark/subset_resolve.py` `_bound_subset_positions`; probes `s3d/spark_probes_s3d16*.py`, `s3d17*.py` with `.out` files; replays land in the hand-back. |

| C-038 | The multi-exact refusal is withdrawn: probe s3d18 untangles the two same-display union fixtures (a creation-dup union carries distinct attribute ids and refuses every subset spelling, a select-dup union shares one id and binds positional-first for either spelling), so the replay's 75 twin-union `dropna`/`fillna` cells are silent single-id trims, not refusals. The rule is id-only again: multi-id refuses, single-id under a union binds positional-first, single-id without a union fans out. C-037's fold clauses (OpenJDK `equalsIgnoreCase`, length-plus-`String.toLowerCase`) stand. | The S3c (43) and S3d (121) suites green; the S0 replay with 0 cells moved away from Spark. | PROVEN (pins; replay lands in the hand-back) | `python/repark/src/repark/spark/subset_resolve.py` `_bound_subset_positions`; probes `s3d/spark_probes_s3d18*.py` with `.out` files; replays land in the hand-back. |
| C-039 | Qualified names bind through `resolve` with the frame's facade-held qualifiers (id to names, set by `alias`, unioned onto join output, pairing using keys): one id binds under that qualifier only, several refuse `AMBIGUOUS_REFERENCE`, none misses to Spark's `UNRESOLVED_COLUMN`; a head matching no qualifier keeps its door's main path (struct tokens in filter, literal drop strings, engine fall-through for stars). | The 14 `attr_id_s3e` Rust pins and the 28 facade pins (56 instances, both rules) green; mutations M1–M3 red 19/3/18 and reverted; three replays 0 moved with 628 gains. | PROVEN | `crates/repark-core/src/session/df_guards/attr_id.rs` `resolve`; `sort_names.rs` `bind_qualified_free_refs`, `grandchild_qualified_key`, `join_output_sources`, `qualifier_star_positions`; `crates/repark-python/src/dataframe_names.rs`; `python/repark/src/repark/spark/qualified_names.py`; probes `s3e/spark_probes_s3e1*.py` through `s3e6*.py` with `.out` files; replays land in the hand-back. |

**S3d Spark measurements (2026-10-01, four batched probes, live Spark 4.1.2).**
`drop(str)` fans out over one id and over two with no refusal, and a miss is a
no-op; `drop(F.col)` fans out over one id, refuses two with
`AMBIGUOUS_REFERENCE`, and no-ops a miss; a parent Column drops every position
carrying its id (one side of a self-join, both of one-attribute twins);
compounds, literals, aliases, and other-frame Columns are no-ops, as is a
qualified `drop("l.v")` str (literal). `dropDuplicates` runs over twins and
two-attribute displays and misses with `_LEGACY_ERROR_TEMP_1201`.
`fillna`/`dropna` fan out one id, refuse two past the guard, and miss with
`UNRESOLVED_COLUMN.WITH_SUGGESTION`; folded dict keys are last-wins and
duplicate subset keys count each time under `thresh`. The live rule decides
after the frame was built. No halt: every shape agrees with fan-out per Bound
plus the per-API miss and refusal rules above.

**S3d residue R-6 (2026-10-01).** A parent-Column drop that splits a bare
duplicate-engine frame (a USING self-join: no display overlay, both engines
`v`) keeps the base `AMBIGUOUS_REFERENCE`: the drop mechanism addresses
fields by name and cannot name one of two same-named positions. Spark drops
the parent side. The fix is to give USING joins with duplicate outputs the
H1-style display/engine overlay at join time (then the id drop is exact, as
the condition-self-join pin proves); that is a join-path change, so a
follow-up card owns it, not S3e or S4. No replay cell covers the shape.

**Mutation record S3d (2026-10-01).** Each mutation edited
`python/repark/src/repark/spark/subset_resolve.py`, ran
`pytest python/repark/tests/test_attr_id_1_s3d.py`, and was reverted from a
copy; `diff` against the copy was empty afterwards and the file is 84 green.

| # | Mutation | Red |
|---|---|---|
| M1 | `_grouped` binds every id count (`== 1` to `>= 1`) | The 14 two-attribute and join-dup refusal pins (drop Column, fillna subset, fillna dict, dropna, each under both rules) |
| M2 | Parent Columns skip the id and origin-map branches and bind by display | The 10 id pins (parent-child, alias, other-frame, self-join side, sided values, each under both rules) |
| M3 | Every S3d fan-out stops at the first hit (five sites) | The 14 fan-out pins (drop str twins, drop Column twins, drop str join-dup, fillna subset twins, fillna dict twins, dropna thresh twins, each under both rules); the `dropDuplicates` twins stay green because dedup by one of identical twins yields the same rows |

**S3d echo rule (2026-10-01).** Live Spark echoes each ambiguity candidate as
the qualifier-qualified written name (measured: `[`Id`, `Id`]` for a folded
rival, `[`l`.`v`, `r`.`v`]` on a join), and the pre-existing u11 pin asserts
the same shape (`[`id`, `id`]` unqualified, `[`a`.`id`, `b`.`id`]` qualified).
The S3d refusal renders that shape from the plan qualifiers with the native
scratch-relation filter, so the u11 contract holds byte-for-byte with no test
change. The S3a/S3b display-echo sites are untouched.

**S3d follow-up measurements (2026-10-01, probes s3d8..11, live Spark 4.1.2).**
No probe before s3d8 ever tested an expansion pair under `drop`/`dropDuplicates`
against Spark: the upper-then-lower fold-B theory was Java-side inference from
same-length pairs plus a misread of probe7's (`ß`, U+1E9E), and Escape-exact
s3d8 refutes it — (`ß`,`SS`), (U+0130,`i`+U+0307) and (U+FB00,`FF`) miss under
both `drop(str)` and `fillna`, while (`ß`,U+1E9E) hits. What Spark does is
codepoint Java: fold A is `Character.toLowerCase` per codepoint (Deseret and
supplementary pairs included, so per-UTF-16-unit comparison is wrong too) and
fold B is codepoint `equalsIgnoreCase` (equal codepoint length plus per-point
equal/upper/lower with the int-version mappings, which never expand). Python
3.12 maps scripts Zulu-17 does not (Vithkuqi, U+A7C0/C1 and kin), so Python
ops alone over-hit; Rust 1.96 maps more still (Hanifi Rohingya, Medefaidrin,
Latin Extended-D additions, Georgian U+1C89/8A). Union output renumbers
colliding exprIds, so a same-id multi-position single-bind (`fillna`,
`dropna`, free-Column `drop`) under a union binds the positionally-first hit
only — including `fill("V")` filling the `v` position — while `drop(str)` and
`dropDuplicates` fan out to every hit with no refusal; a dict's keys each bind
first-only with last-key-wins per position. Sensitive hits close over every
same-id position (`flipFT` fill fills both), while sensitive `drop(str)` stays
hits-only. Multi-id refuses regardless of display order or exact-first.

**S3d residue R-7 (2026-10-01).** Single-bind ambiguity above a join that sits
above a union is unprobed (no replay cell covers join-above-union): the
`union_dup_below_wrappers` walk stops at joins, so a multi-id bind there refuses
`AMBIGUOUS_REFERENCE` while Spark's dedup-lineage answer is unknown. Owned by
the follow-up union-id card (the slice that implements union output id
separation), not S3e.

**S3d residue R-8 (2026-10-01).** `flipTF`/`T`-caseF cells: Spark errors
building `F.col("V")` where no `V` exists under the sensitive rule, while
RePark builds it (insensitive build binding). That is build-time
select/Column binding, owned by S3a/S3b, not S3d; S3d's call-time binding
cannot reach the Spark error.

**S3d residue R-9 (2026-10-01).** `NameRule::matches`/`lookup` stay ASCII-only
per ruling R-S3d-1: converging them on the Java folds belongs to the
unicode-case card (RC3-5), not to this round. Follow-up 3 measured the gap
exactly: `select` shares the fillna rule (equal length plus
`String.toLowerCase`, 12/12 r1 `sel` cells plus probe s3d16 `sel_sig_up`),
so ASCII-only misses non-ASCII folds (`σ`/`Σ`); the unicode-case card owns
the fix with those cells.

**Mutation re-run record S3d (2026-10-01, after R-S3d-1).** Same file and
command as above; diffs at `s3d/mutation_m1_rerun.diff`,
`s3d/mutation_m2_rerun.diff`, `s3d/mutation_m3_rerun.diff`; each reverted with
`git status` clean and the suite back to 111 green. M1 (grouping binds every
id count) reds 16: the 14 two-attribute and join-dup refusal pins plus the
distinct-id-union and reversed-order refusals. M2 (parent-Column drop binds by
display) reds 6: the self-join sided pins, the select-twins sided pins, and
the other-frame no-op pins, each under both rules; the two-frame join sided
pin stays green because the shared guard refuses the display-bound multi-hit
and the origin map recovers the left side. M3 (every fan-out stops at the
first hit, five sites) reds 16: the drop/fill/dropna twin, join-dup, union
`drop(str)` and sensitive-closure fan-out pins; the `dropDuplicates` twins
stay green by construction and the union first-only pins by design.

**Mutation re-run record S3d follow-up 3 (2026-10-01, after C-037).** Same
three mutations; diffs at `s3d/mutation_m1_c037.diff`,
`s3d/mutation_m2_c037.diff`, `s3d/mutation_m3_c037.diff`; each reverted with
`git status` clean and the suite back to 122 green. M1 reds 17 (the 16 plus
the multi-id same-display union refusal). M2 reds 6, unchanged. M3 reds 19
(the 16 plus the project-twins fill fan-out, the project-twins free-Column
fan-out, and the divergent-twins `dropDuplicates` fan-out). Follow-up 4
merges two union-twins pins into one (121 green); M1/M3 re-run post-C-038
still red 17/19 with `git status` clean (M2's area untouched).

**S3d gate record (2026-10-01, head 70001a27).** Three identical replays
(`s3d/t-head-2/3/4`, foreground, `timeout 3000`, `ulimit -v 67108864`): 0
cells moved away from Spark against both `main.json` and the S3c parent,
0 lost, all 8558 FIXED kept, 2676 gains. Like-for-like timing
(`s3d/like_for_like_s3d.py`, heads t-head-2/3/4, bases `s3c/t-base-c1/2/3`
since `main.json` predates S3a–S3c): median-of-3 ratio 0.9902 (bar 1.2x),
53.4s over the 11234 FIXED, coverage 0.9999. Neighbour sweep `-n 8` over
the S3c 46 files plus `test_attr_id_1_s3d.py`: 2400 passed, 19 skipped, 2
xfailed, 0 failed. `bash /tmp/xattr/gate.sh` prints GATE GREEN (second run;
the first run red on two `.typos.toml` comment lines only, deleted in
70001a27).

## Round S3e (2026-10-01)

R-4 closes: `resolve` takes the facade-held qualifiers and every qualified
door (`t.v`, `F.col`, `df[]`, filter/orderBy/selectExpr text, qualified refs
after `alias()`, join children) binds through it, closing the S3a Q2 residues
R5 (qualifier case on alias frames), R9 (qualified join-side misses) and the
qualified half of R12 (`F.col` fall-throughs). One id binds under its
qualifier only; several refuse `AMBIGUOUS_REFERENCE` (select, filter, drop),
`UNRESOLVED_COLUMN` in sort (Spark's sort-twin shape, probe swp_amb); a miss
raises Spark's `UNRESOLVED_COLUMN` echo. A head matching no qualifier keeps
its door's main path: struct tokens in filter, literal drop strings (Spark:
`drop("a.v")` is a no-op), engine fall-through for stars. Drop strings stay
literal while drop Columns resolve — the one behaviour the brief left open,
measured, not chosen. `DataFrame._frame_qualifiers` (id to names) is set by
`alias`, copied by `_spawn`, unioned onto join output at all four join sites
(using keys pair); cross-join duplicate ids re-mint only when a side carries
qualifiers, so unaliased crosses keep the S3d id assignment bit for bit (the
first replay's 2 twincross_fill losses, fixed). No helper is deleted: every
family-only candidate keeps an outside-family caller — `_bind_qualified_column`
(only caller `replace_expr.py:211`, the `replace()` positional path),
`_qualified_target` (the error raisers), `_assign_join_qualifiers` plus the
`_join_qualifiers` slot (join sites and `replace()`), `logical_column_qualifiers`
(the `_known_qualifiers` plan half), `requalify_join_sides` (condition joins).
For S4: `__REPARK_QCOL_` still has readers (`plan_collapse.py`, `core.py`,
`column.py`) and `_origin_plan_id`/`_origin_field` still have readers
(`functions.py`, `actions_export.py`, `plan_collapse.py`, `core.py`,
`replace_expr.py`, `subset_resolve.py`, `column_fields.py`,
`qualified_names.py`) — S3e is not their last reader.

**S3e Spark measurements (2026-10-01, six batched probes, live Spark 4.1.2
America/New_York).** Qualifier wins over a same-named struct in select,
filter, orderBy and star; re-alias drops old qualifiers on frames and joins;
the live rule decides after build; twin echo is `` [`q`.`v`, `q`.`v`] ``;
sort twins raise unresolved; drop Column refuses twins and no-ops a miss;
`selectExpr("a.v")` names `v`, compounds name bare (`(v + 1)`); `F.col("q.*")`
expands in select; `df["q.*"]`, unknown stars and star-Columns outside select
keep main's path (shaping is a follow-up). Divergences pinned honestly:
struct select/orderBy (main's resolve-miss raise), unknown-star engine error,
`selectExpr` over duplicate-field joins (the scratch-view scan fails on main
identically — view-layer card), `withColumn` over duplicate-display frames
(main-identical ambiguous bare bind), qualifier-keeping display names,
bare suggestion lists, single-id multi-hit binds (the settled S1/C-038
select-dup model; the S3b facade-held filter pin changes contract to bind and
is renamed, matching the select door on main). Owners: follow-up cards only.

Rust-first per R-S3d-1: all matching lives in `attr_id.rs`/`sort_names.rs`
behind five thin natives; `NameRule` untouched. Ceiling splits (pure moves):
`case_bind.rs` tests to `session/tests/case_bind.rs` (1000 → 552),
`resolve_*` tests to `session/tests/attr_id_resolve.rs`; the qualified family
to `spark/qualified_names.py`; the `selectExpr` body to
`filter_quote._select_expr_frame`; `core.py` 3803 → 3800 with the CAP-1
mirror. The 28 facade pins (56 instances) and the 14 Rust pins green;
mutations M1 (facade payload forced `None`) reds 19, M2 (rule forced exact in
`qualifier_matches_position`) reds the 3 fold pins, M3 (side names unioned
onto every id) reds 18, each reverted with `git status` clean.

**S3e gate record (2026-10-01, head ca53e0cc).** Three identical replays
(`s3e/t-head-1/2/3`, foreground, `timeout 1500`, `ulimit -v 67108864`):
43,946 cells, 0 moved away from Spark against both `main.json` and the S3d
parent, 0 lost, all 8558 S3a/S3b/S3c FIXED and all 2676 S3d gains kept, 628
S3e gains. Like-for-like timing (`s3e/like_for_like_s3e.py`, heads
t-head-1/2/3, bases `s3d/t-head-2/3/4`): median-of-3 ratio 1.0176 (bar 1.2x),
57.4s over the 11862 FIXED, coverage 0.9999. Neighbour sweep `-n 8` over the
C-016 globs (48 files: the S3d 47 plus `test_attr_id_1_s3e.py`): 2456 passed,
19 skipped, 2 xfailed, 0 failed. `bash /tmp/xattr/gate.sh` prints GATE GREEN.

## Round S4 (2026-10-02)

**Model:** muse-spark-1.3-contributor (S4 executor, guided).
**Work order:** `/tmp/oc-worker/direct/wo/attr-id-1-design.md` §4 S4 (delete the
encodings) plus §5 halt rules and §9–§9f rulings.

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-040 | S4 deletes the origin encodings after every reader moves to attribute ids: `_origin_map` (and its propagation sites), `_engine_origin`, `_NEW_ATTRIBUTE`, `_new_attribute`, `_fresh_outputs`, `_origin_not_emitted`, `twin_identities`, `same_source_fields`, `_shared_origin_column`, `_distinct_attributes`, `_twin_identities`, `__REPARK_QCOL_`, `_origin_plan_id` / `_origin_field`, `_thread_origin`, `_rebind_origin_column`, `_remember_unemitted_right_origins` and `_select_via_qcol_sql` have no reader. Join tokens carry (attribute id, qualifier names) and side exactly per token in `join_attr_tokens.py` (a pure-move CAP-1 split out of `plan_collapse.py`); unknown ids reach the engine unsided; positional alternation fires only for ids present on both sides under the multi-token-arm guard. Semi and anti refusals key on `_unemitted_attr_ids`. Select engine names shrink to `__repark_sel_{n}`. The `column.py`, `core.py` and `functions.py` CAP-1 rows ratchet down and the `plan_collapse.py` row leaves. | A deletion grep with 0 code hits per name; the 9 S4 pins (17 instances, both rules); the frozen surface green; the CAP-1 mirror green; three S0 replays outcome-neutral against 09d0971d. | PROVEN (deletion grep plus pins; the neutrality replay lands in the gate record) | Commits `ead8a3ca` and `16973386`; `python/repark/tests/test_attr_id_1_s4.py`; `python/repark/src/repark/spark/dataframe/join_attr_tokens.py`; registry row EX-DF-20; the S4 gate record below. |

**S4 verdicts on earlier OPEN clauses (2026-10-02, append-only update; the rows
above keep their original verdict cells).** C-007: PROVEN — the construction
grep re-run on this head finds the same 17 direct `DataFrame(` constructor
calls and no `__new__`, subclass, copy or pickle bypass, and
`test_attr_id_1_s2.py::test_every_spawned_frame_carries_an_id_on_every_output_field`
is green. C-008: PROVEN — the S2 bind pin is green and S4 sets `_attr_id` at
every remaining bind site (the S4 slot pin). C-009: PROVEN — the five S2
write-cleanliness pins plus `cargo test -p repark-core --lib` (974 passed)
and `-p repark-python --lib` (88 passed) are green. C-010: PROVEN — the S2
export pin is green. C-011: PROVEN — the S2 USING pin plus the core and
binding re-mint pins are green. C-013: REJECTED — the stated mutation (facade
write-path strip disabled) leaves all 13 S2 pins green, so the facade strip
is not load-bearing for any pinned cleanliness claim; the S2b optimizer rule
`StripAttributeIds` guarantees footer and byte cleanliness at execution.
C-014: PROVEN — the core statement-root pins and the S2 `EXPLAIN` pin are
green. C-012, C-016, C-022 stay OPEN by construction: their statements
(byte-identical to `main.json`) were overtaken on purpose by the S3 cutover
gains (S3b also closed the four §9c cells C-016 named); the S4 gate record
below is the live neutrality statement.

**S4 residue dispositions (2026-10-02).** R-1 closes: S3a gives a user alias
a fresh id (C-024) and the S0 replays decide the cells. R-2 closes: S1b
per-process id prefixes (C-019) plus the S2 strip and the S2 write pins.
R-3 closes: C-011 is PROVEN above. R-5 closes on the cache half (the S2
cache pin is green through every S3 sweep); the SQL-`UNPIVOT` write-source
half stays open under a follow-up card (no facade probe has landed).
R-6 stays with its follow-up card, R-7 with the follow-up union-id card,
R-8 moves to a follow-up card (build-time binding; S3a/S3b are closed),
R-9 stays with the unicode-case card. New: R-10 (2026-10-02) — the facade
write-path strip (`writer_layout.run_through_temp_view`,
`writer_s3`, `merge.py`) is redundant on every pinned path per the C-013
measurement; a follow-up card owns deleting it or pinning born-clean views.
R-11 (2026-10-02) — shared-lineage join siding runs where Spark reports
ambiguity; registry row EX-DF-20 (BACKLOG) with its pins. R-12
(2026-10-02) — brief-named, not S4-measured: 121 replay cells read a stale
session config; owned by the session-config card.

**S4 registry paragraph (2026-10-02).** Row EX-DF-20 in
`docs/spark-sql-iceberg-parity.md` §7 records the one divergence class S4
keeps: shared-lineage joins (`df.join(df, df.a == df.b)`, aliased and mixed
compound arms) side every token and answer rows where live Spark 4.1.2
raises ambiguity. Self-equi (`df.join(df, df.x == df.x)`) answers the
diagonal on both engines and is outside the row. The row lands with its
three pins in the same change.

**S4 positional-fallback ruling (2026-10-02, halt rule 3).** The fallback in
`_resolve_join_token_sides` fires only when a token's id is present on both
join sides (twins of one attribute: same-object frames, or distinct frames
whose lineage shares the id with no qualified sibling to complement); the
multi-token-arm guard refuses rather than mis-bind. A token whose id sits
on neither side is never sided — it reaches the engine unchanged, which
raises `UNRESOLVED_COLUMN`, base-identical. No branch treats a missing id
as anything but a loud error. Pinned by
`test_attr_id_1_s4.py::test_s4_third_frame_unknown_id_raises_engine_error`.

**S4 Spark measurements (2026-10-02, live Spark 4.1.2).** Same-object simple
cross-field (`frame.x == frame.y`), filter-compound, mixed-3token and
aliased-compound joins raise `AnalysisException` ambiguity; same-object
self-equi, unaliased-aliased, filter-simple, reversed-qualified and
select-lineage joins answer the diagonal, base-identical. One head cell
moves against 09d0971d: the filter-compound shape now refuses with the
multi-token-arm guard where base bound it — toward Spark, which raises.
Evidence: `/tmp/s4_spark_lineage.log`, `/tmp/s4_spark_selfequi.log`.

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-041 | S4 follow-up keeps the deletion outcome-neutral at the bind: a column bound against the target frame (`Column._birth_frame`, set at every `_attr_id` bind site and carried by every attr-preserving rewrap) stays verbatim, any other frame's held id rebinds by position, and the rebind gates on exact engine-name uniqueness so duplicate-engine parent refs keep the shaped `AMBIGUOUS_REFERENCE`; a lineage hit binds only when the output display still shows the written name (a rename that drops the name refuses, a case-only rename binds). The sort-marker family moves to `column_sort.py` and `_select_via_attr_sql` moves to `join_attr_tokens.py` behind a one-line delegate; the `column.py` and `core.py` CAP-1 rows ratchet down. | The 10 follow-up pins green; the 12-file pin set plus `casesens_1` green (616 passed); three S0 replays 0 moved against `main.json` and 09d0971d. | PROVEN (pins plus t-head-1; t-head-2/3 land in the gate record) | Commits `23458d49` and `b808460a`; `python/repark/tests/test_attr_id_1_s4.py`; `python/repark/src/repark/spark/column_sort.py`; the S4 gate record below. |

**S4 follow-up record (2026-10-02, append-only).** Replay t-head-1 at
`23458d49` moved 248 cells against `main.json`, all error-shape: marked
duplicate-engine refs failed with a bare engine error where base fails
shaped. `b808460a` restores the uniqueness gate with exact (not folded)
counting — J2 case twins still mark and bind, true duplicates stay written.
Replay t-head-1 at `b808460a`: 43992 cells, 0 EQUAL moved, FIXED 11872,
gains 10, lost 0, moved_vs_main 0.

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-042 | S4 alias fix keeps the deletion neutral for aliased refs: `alias` preserves the base column's `_attr_id` and `_birth_frame`, so an aliased side ref on a condition join binds its own side (the S4 deletion had dropped the alias-to-base link the origin pair carried) while a same-frame twin alias stays verbatim and refuses; `drop` takes only simple refs down the `_attr_id` path so an alias stays a no-op as at base. The string-predicate family moves to `column_string.py`; the `column.py` CAP-1 row ratchets down. | The alias pin green; the neighbour sweep green; three S0 replays 0 moved against `main.json`. | PROVEN (pin plus sweep; the neutrality replay lands in the gate record) | `python/repark/tests/test_attr_id_1_s4.py::test_s4_aliased_side_ref_binds_own_side`; `python/repark/src/repark/spark/column_string.py`; the S4 gate record below. |

**S4 gate record (2026-10-02, append-only).** Final head `e130016f`.
Product commits `23458d49` (birth-frame rule), `b808460a` (exact-unique
gate), `7f88914b` (alias binding pair, drop simple-refs, string split),
plus doc/ledger commits `1ac4d098` and `e130016f` (clippy borrow, CAP-1
mirror rows 1527 → 1378 and 3711 → 3653 in lockstep, two live `_origin_*`
prose mentions trued up). S0 replay ×3 at `7f88914b`, foreground with
`timeout` and `ulimit -v 67108864` into
`/tmp/oc-worker/direct/wo/attr-id-1/s4/t-head-N`: 43992 cells each, 0
EQUAL moved, FIXED 11872, gains 10, lost 0, moved_vs_main 0 on all three.
Like-for-like timing median-of-3 ratio 1.0289 (≤ 1.2x). Neighbour sweep
`-n 8` over the C-016 globs (49 files): 2496 passed, 19 skipped, 2
xfailed, 0 failed — including the eqNullSafe sweep case that base rows
and S4 had broken. `casesens_1` 70 passed with no EQUAL pin change;
`casesens_2`/`diffprobe` have no such facade files (`test -e` fails).
`cargo test -p repark-core --lib` 976 passed; `-p repark-python --lib`
88 passed. Deletion proof: every design name greps 0 live uses in
`python/` and `crates/` (6 remaining mentions are the absence assertions
in `test_attr_id_1_s4.py` and one removal docstring in
`test_dfcore_1_exports.py`); archived ledgers keep history lines, frozen
and foreign-unit records untouched. `bash /tmp/xattr/gate.sh` prints
GATE GREEN. C-041 and C-042 close to PROVEN unconditionally.

**Verifier-fold V-4/V-5/V-6 record (2026-10-02, append-only).** The Opus
end-of-stack verifier BLOCKed `b377de7e` with V-1 through V-8 (evidence
under `/tmp/oc-worker/direct/wo/attr-id-1/verify/`). This round folds V-4
and V-5; V-1, V-2 and V-3 are out of scope pending the owner join ruling,
and V-6 halts (C-043 OPEN) because its ruling premise is false as
measured. Product commits `4982931f` (V-5) and `b6b6aeb3` (V-4).

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-043 | V-6 HALTS: `crossJoin` re-minting the right side's colliding ids unconditionally (the ruling's mechanism) does not fix the `withColumnRenamed` pin — the rename dies in the name-based `_iter_bound_columns` duplicate path before ids matter — and the sufficient fix (a condition-join-style explicit projection with unique engines plus the display overlay) extends V-1's wrong-side binds to 14 base-refusing cross cells (`xj_sel_parent`, `xjf_sel_parent_f`, `xj_filter_parent` and kindred, both case rules). The ruling needs the owner V-1 decision first: refuse-like-Spark or carry frame lineage. | The owner ruling; then the three `p1_joins` pins (`xj_wcr`, `xj_wc`, `xj_dd`) green with the 14 cells explained. | OPEN (ruling question: V-6 rides on V-1) | `/tmp/oc-worker/direct/wo/attr-id-1/v456/p1_v6.json` (38 cells move against pre-V-6 head; the V-6 attempt is reverted, no commit). |

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-044 | V-5 keeps the case-twin refusal: under `caseSensitive=false` a folded-ambiguous name with one exact spelling births an id-less written-ref column (exact-engine spelling, birth frame kept) instead of binding the twin's id, so the engine refuses exactly as base and Spark on the birth frame and every pass-through child; `_born_ambiguous` returns those columns verbatim from `_bind_sort_key` because live Spark refuses Column sort keys as `AMBIGUOUS_REFERENCE` but string keys as `UNRESOLVED_COLUMN`. Under `caseSensitive=true` nothing changes. The S4 pass-through bind pin and the sort-marker `UNRESOLVED` pin flip to the Spark refusal. | The ported `test_v5` pin, the 13 remaining F_ cells refusing, all 16 T_ cells binding with Spark rows; the p5 grid 31 cells exact-base with 1 exact-Spark. | PROVEN | Commit `4982931f`; `python/repark/tests/test_attr_id_1_v456.py`; live-Spark `/tmp/oc-worker/direct/wo/attr-id-1/v456/v5_spark_order.json`; `/tmp/oc-worker/direct/wo/attr-id-1/v456/p5_v5.json`. |

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-045 | V-4 keeps a live frame's ids across materialization: native `copy_attribute_ids` carries the source schema's ids onto the fresh scan by position (a width mismatch is an internal error; an already-carried plan returns unchanged); `bind_registered_view` applies it to cache scans and `bind_checkpoint_scan` (moved out of `core`'s checkpoint arm, CAP-1 3653 → 3652) to checkpoint scans, while `unpersist` already restores the lineage plan untouched. `test_cache_keeps_frames_bindable` now asserts byte continuity. Known interaction: continuity exposes V-1 on `p7 child_before_materialize_join_parent` (base rows become a left-bind; Spark refuses), owned by the pending V-1 ruling. | The ported `test_v4` pins, the unpersist/checkpoint variants, the continuity pin, the Rust carry test; the p7 grid 7 cells exact-base-and-Spark. | PROVEN | Commit `b6b6aeb3`; `python/repark/tests/test_attr_id_1_v456.py`; `crates/repark-core/src/session/tests/attr_id.rs::the_id_carry_copies_source_ids_by_position_and_keeps_a_carried_plan`; `/tmp/oc-worker/direct/wo/attr-id-1/v456/p7_v4.json`. |

**Verifier-fold halt-rule-3 correction (2026-10-02).** The S4 ruling
sentence "No branch treats a missing id as anything but a loud error" is
false as measured: `_bind_stable_id_column`'s final `return column`
(`column_fields.py:722`) passes a column whose id no output holds, and it
then binds by display name (finding V-3, pre-existing and base-identical).
The sentence stands corrected to: every unknown id reaches a loud error,
except the pass-through `return column`, which stays open as V-3 pending
the owner ruling.

**Verifier-fold R-5 correction (2026-10-02).** The S4 residue disposition
closed R-5's cache half on bindability (`test_cache_keeps_frames_bindable`
asserted non-`None` ids only). That test now asserts byte continuity
across cache, unpersist and checkpoint, and C-045 proves the carry; the
cache half closes on continuity. The SQL-`UNPIVOT` half stays open as
recorded.

**Verifier-fold gate record (2026-10-02, append-only).** Product heads
`4982931f` (V-5) and `b6b6aeb3` (V-4) plus ledger `ec703941`. Cargo:
`repark-core --lib` 977 passed (978 with the carry test counted once more
under its filter), `repark-python --lib` 88 passed; clippy, fmt,
`rust-panic-ban`, `check_lib_rs`, `check_rust_file_size` clean.
`probes/test_verify_pins.py`: the 2 V-4 and 1 V-5 pins pass; the 5 V-1,
V-2 and V-3 pins fail byte-identical to `b377de7e`. S0 replay once into
`v456/t-head-1` (foreground, `timeout 1500`, `ulimit -v 67108864`):
43992 judged cells, 0 EQUAL moved, FIXED 11872 against `main.json`;
against `b377de7e`'s t-head-1, 9 raw diffs — 5 process-prefix noise, 3
engine-echo-case noise (all three flip run-to-run on one build), 1
`freqItems` order gain — 1 gain, 0 lost. Neighbour sweep `-n 8` over the
C-016 globs (50 files with the new pin file): 2532 passed, 19 skipped, 2
xfailed, 0 failed. `casesens_1` inside the sweep, no EQUAL pin change.
Known V-4/V-1 interaction: `p7 child_before_materialize_join_parent`
moves base-rows → left-bind (Spark refuses), owned by the pending V-1
ruling. `bash /tmp/xattr/gate.sh` prints GATE GREEN.

## Round SJ-6 (2026-10-03)

**Model:** muse-spark-1.3-contributor (SJ-6 clerk, guided).
**Work order:** `/tmp/oc-worker/direct/wo/attr-id-1-sj-6.md` (docs only): ledger clauses for the
self-join rule, the mechanism and the halt-rule-3 table; the dated 1182 registry row; the
`MISSING-REF-RESOLVE-1`, `VIEW-LINEAGE-SELFJOIN-1` and `LOCAL-CHECKPOINT-NEW-FRAME-1` cards;
the release note. Owner ruling 2026-10-02: option A — Spark Classic is the oracle.

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-046 | Option A is the rule: every self-join reference Spark Classic refuses now refuses with `_LEGACY_ERROR_TEMP_1182`, EQUAL on the error — the condition, the `config` parameter (`spark.sql.analyzer.failAmbiguousSelfJoin`), the verbatim template tail, and the display names sorted with exact multiplicity (RePark has no exprIds, so Spark's `id#0L, id#0L` renders `id, id`). No branch binds an ambiguous reference to a side: there is no tie-break, and with the conf off every reference binds left by id. | The condition refusals, the post-join refusals at every checking surface, the rewrite and missing cells, the over-fire guards that still answer, and the conf-off left-bind rows, each EQUAL to live Spark 4.1.2. | PROVEN | `python/repark/tests/test_attr_id_1_sj3.py` (the `B_*`, `D_*`, `E_parent_alias_*` and `G_eq3_*` rows, `I_rewrite_name_missing`, `P_s4_third_frame`, the `P_v_*` verifier pins), `test_attr_id_1_sj4.py` (the `A_*`, `F_*`, `I_*`, `J_*` and `K_*` rows, every `H_off_*`, the answer guards), `test_attr_id_1_sj5.py` (the `C_*` rows and the 14 p1 cross cells); error parameters measured in `selfjoin/sj2.params.json`. |
| C-047 | The mechanism is a frame-lineage DAG in Rust: every frame carries a `FrameId` and an immutable `FrameNode` (`Root`, `Derived`, `SetOp`, `Join` with its `remint` map); a join re-mints every right-output id found anywhere in the left lineage, not only output collisions; the condition preparer detects, rewrites exempt equalities by name, and binds left-first; the post-join funnel refuses at the 13 surfaces (select, filter/where, withColumn(s), repartition(+ByRange), orderBy/sort(+WithinPartitions), groupBy/cube/rollup, `DataFrame.agg`, `GroupedData.agg`) while name-based surfaces never check (`withColumnRenamed`, `dropDuplicates`, `fillna`, `drop(str)`, `toDF`, `drop(Column)`, window expressions); `renewed_absent` catches re-minted ids missing from the output; both confs read live; `crossJoin` runs through the join builder; positional siding and `remint_cross_collisions` are deleted. | The seam pins (token frame fields, `F0`, inert roots, conf forwarding), the condition-path pins, the funnel pins with the renewed-absent wiring evidence, and the cross-join pins with the over-fire guards. | PROVEN | `test_attr_id_1_sj2.py` (the seam pins, incl. frameless `F0` and inert empty roots), `test_attr_id_1_sj3.py` (the condition, rewrite and missing pins), `test_attr_id_1_sj4.py` (the funnel pins, incl. `K_on_*` preferring 1182 and `K_drop_*` missing), `test_attr_id_1_sj5.py` (the cross-join pins); the DAG and the preparer in `crates/repark-core/src/session/df_guards/`. |
| C-048 | Free names resolve against the output's display names (ruling R-SJ5-1): one matching display binds to its engine field; more than one raises `AMBIGUOUS_REFERENCE` with Spark's `name` and `referenceNames`; none falls through to the engine, which reports `UNRESOLVED_COLUMN` as before. The rule runs on SQL text (filter/where/selectExpr, behind a dup-word pre-filter), compound select and withColumn expressions, summary/describe, qualified `F.col("q.v")`, and backquoted `` `q`.`v` `` filter text; bound-key windows and unique-display frames skip the check. | One cross cell plus one inner twin per surface, each EQUAL to live Spark's class and parameters, with the corpus `bq`/`sexpr`/`wcz`/`summ`/`q_col` cells back to EQUAL. | PROVEN | `test_attr_id_1_sj5.py` F1/F2 pins (`f1_bq_*`, `f1_sexpr_*`, `f1_wcz_*`, `f1_summ_*`, `f1_qcol_*`, `f1_window_free_ref_refuses`, `f1_dropdup_alias_answers`, `f2_bqqual_cross`). |
| C-049 | Halt rule 3 holds as ruled: a `FrameNode` output without an id is an internal error, except a never-stamped plan, which gets an inert empty `Root` in the binding layer (R-SJ2-3); a frameless token renders `F0` and is never an ambiguity candidate (R-SJ2-2); the preparer answers `MISSING_ATTRIBUTES` (appear vs missing-from-input by the name rule) for an id on neither side, `UNRESOLVED_COLUMN`/`AMBIGUOUS_REFERENCE` for a rewrite miss/collision, and left-first otherwise; a reference whose frame is absent from the lineage is not ambiguous; an id outside every `remint` map passes unchanged; a re-minted id absent from the output raises `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION` under either conf. | The missing pins under both confs, the sibling-absent answer guard, and the dead-birth-frame naming pins. | PROVEN | `test_attr_id_1_sj3.py` (`P_s4_third_frame`, `K_off_cond_missing`, `r3_cp_*_join_parent`, `r3_cp_x_join_self`, `token_carries_leaf_display_hex`), `test_attr_id_1_sj4.py` (`K_off_shared_*`, `K_drop_*`, `A_sibling_absent`). |

**SJ-6 S4-statement correction (2026-10-03, append-only).** The sentence "No branch
treats a missing id as anything but a loud error" (the S4 positional-fallback ruling and
the verifier-fold correction) now holds for join-made absences: ids removed by the
lineage re-mint are refused by `renewed_absent` or the preparer's MISSING arms before
binding is reached. Ids made absent by non-join lineage — `withColumn` replace, alias
and rename swaps, `toDF` — stay open under card `MISSING-REF-RESOLVE-1` (V-3), which
owns the `_bind_stable_id_column` pass-through.

**SJ-6 residues (2026-10-03).** One section, each with its cell and Spark's answer.
Ledger residues R-13..R-18; the design's residue R-18 below is the work-order id, not
this ledger's R-18. New: R-13 — a self-join whose lineage passes through a
never-stamped plan, e.g. a `describe()` frame, is not detected; the lineage is cut at
the non-relation (R-SJ2-3). No probe cell in `selfjoin/` or `verify/probes/` takes a
describe/summary frame through a join, and Spark's answer for that shape is unmeasured;
the cut itself is pinned by `test_sj2_describe_and_summary_answer` and
`test_sj2_never_stamped_frames_get_inert_empty_roots`. R-14 — the MISSING refusal on
post-join surfaces names operator `!Join`; Spark names the surface (`sj4.spark.json`):
`!Project [v#1L]` (`K_off_shared_sel`), `!Filter (v#7L > cast(15 as bigint))`
(`K_off_shared_filter`), `!Sort [v#13L ASC NULLS FIRST]` (`K_off_shared_order`).
Unpinned precedent shared with the committed `K_off` pins (SJ-4 hand-back, ruled out
of SJ-5). R-15 — qualified free names do not resolve in join conditions (the design's
residue R-18, pre-existing): `test_h2_compound_alias_free_names_answer` is
strict-xfailed — `(F.col("l.x") + F.col("l.y")) == (F.col("r.x") + F.col("r.y"))` over
aliased frames, Spark 4.1.2 answers 3 under both case rules. The string-qualified
alias join conditions (`E_str_*`, `F_anti_idiom_str` answering `[[1]]`, `G_eq3_str`)
likewise keep their pre-option-A path. R-16 — `declare_sorted` (`core.py`) swaps
`self._inner` for the re-registered sorted view in place and keeps its
construction-time `_frame_node` (flagged 2026-10-02 as the 4th `_inner` swap).
Source frames only, so the node never renews and no check fires; no probe cell
covers a declared frame through a join, and Spark has no such API (the door is a
repark extension). R-17 — `dropDuplicates` over duplicate displays:
`r5p6.F|dd|j_left|w0|dd` (and the `star|dd` twin) — Spark answers `{"value": 4}`;
RePark raises `AMBIGUOUS_REFERENCE` naming `` `v` `` twice, on main and on this head
(a pre-existing main gap, R-SJ5-2 item 3). The aliased twin answers
(`test_sj5_f1_dropdup_alias_answers`, 3). Ledger R-18 — the order of names in
`ambiguousAttrs`: pins assert sorted names with exact multiplicity (R-SJ5-1 item 4).
Measured (`sj-5/ambiguous_order_residue.json`): `A_inner_agg_f` Spark `v, id` vs
RePark `id, v`; `SJ5_agg_two` Spark `id, id, v` vs RePark `id, v, id`; every other
measured 1182 cell matches Spark's order exactly.
