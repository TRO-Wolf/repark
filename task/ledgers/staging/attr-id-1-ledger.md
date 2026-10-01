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
