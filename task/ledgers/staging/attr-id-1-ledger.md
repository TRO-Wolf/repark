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
| R-4 | Dated 2026-09-30, open for S3e: `resolve` matches a written qualifier against the field's relation only; the frame's Python-held join qualifiers (§3.4) are not in the §4 S1 signature. |
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
| C-023 | The §9c Q2 timing cut: `plan_is_stamped` answers by reference whether `stamp` would change the plan (the Projection, Union and other-root mint conditions mirrored; statement roots read stamped; a classification error reads unstamped), and `stamp_attribute_ids` returns the same `PyDataFrame` handle when stamped, keeping the cached analyzed schema. The S0 replay median returns to at or under 597 s with no cell moved away from Spark. | The mirror pin over every spawned root shape; three replays on the cut compared with `compare.py`. | OPEN (replay pending at the cut commit) | `crates/repark-core/src/session/tests/attr_id.rs` `plan_is_stamped_matches_what_stamp_would_change`; replays land in the hand-back. |
