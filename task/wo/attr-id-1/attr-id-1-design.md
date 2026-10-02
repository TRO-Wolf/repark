# ATTR-ID-1 — attribute identity as a function of the plan: the design sketch and work order that finishes #881

Written 2026-09-30 by a Claude session (claude-fable-5-1) for the Opus 5.5 orchestrator, after reading the six verifier hand-backs on PR #881 (`verify-cs2-opus-handback.json`, `reverify-cs2` … `reverify5-cs2-opus-handback.json`), the PR head `121c4bd0` on `/tmp/xs-cs1`, and DataFusion 54.1.0 in the cargo registry. Every `path:line` below is at that head unless it names the registry. Facts are marked **verified** (read in the tree) or **to verify** (a test to write before relying on it).

## 0. Why a design and not a sixth fold

Six Opus passes on #881 found 8 S1s at a flat rate (0, 2, 2, 1, 1, 2 per pass). Every S1 is one bug: a name that matches two columns binds to one of them silently, or a name that matches one attribute twice is refused. Spark decides both with one fact, the attribute's `exprId`, assigned when the attribute is created and carried unchanged through every plan node that does not compute a new value. #881 has no such fact. It reconstructs identity after the fact from four encodings that must agree:

1. engine name strings, `__repark_sel_{plan_id}_{field}_{n}` and `__repark_sel_a_{pos}_{n}_…`, parsed back by regex (`python/repark/src/repark/spark/dataframe/written_names.py:228-233`, `:310-314`);
2. `DataFrame._origin_map`, a per-frame dict copied or rebuilt at 15 sites in `core.py`, `sampling.py` and `replace_expr.py`, and dropped by any op that does neither;
3. `twin_identities` (`crates/repark-core/src/session/df_guards/sort_names.rs:110-131`), which walks one `Projection` deep through Filter/Limit/Sort and stops, with `fresh` flags computed by a Python regex over encoding 1;
4. `same_source_fields`, for frames without a display overlay.

Any op that does not propagate leaves identity "unknown", and the folds ruled on unknown both ways: as a wildcard in fold 3 (RC5-1, silent bind) and as never-shared in fold 4 (RC6-2, spurious refusals) while `_bind_engine_display_column` still fabricated a shared origin (RC6-1, silent bind). No rule on "unknown" is correct because in Spark's model it does not exist. This unit makes it not exist here.

## 1. The rule

Every output field of every DataFrame plan carries one attribute id. Name resolution on the DataFrame door is then a single function:

> hits = the output positions whose display name matches the written name under the session's `NameRule` (and whose qualifier matches, when one is written);
> distinct = the set of attribute ids over the hits;
> 0 hits → UNRESOLVED_COLUMN; 1 distinct id → bound (every hit position, for the APIs that fan out); more than 1 → AMBIGUOUS_REFERENCE.

Nothing else in the facade decides "same attribute or not".

## 2. Facts the design leans on

**Verified in DataFusion 54.1.0 (`datafusion-expr-54.1.0/src/expr_schema.rs`):**
- `Expr::Column` → `schema.field_from_column(c)` returns the input `Field` `Arc` unchanged (`:497`). Field metadata survives a bare column projection.
- `Expr::Alias` → the inner expression's field with the inner metadata plus `Alias.metadata` merged in (`:481-496`). `Alias::with_metadata(Option<FieldMetadata>)` exists (`expr.rs:742`). So an alias can *carry* an id and can *set* one.
- `Expr::Cast` / `TryCast` → `cast_output_field` copies the source metadata (`:79-83`). **So a cast keeps the id it should not keep**; Spark treats `d.v.cast('int').alias('v')` as a new attribute. The stamp must override, not trust.
- `Expr::Literal` and the arithmetic, function, case, window and aggregate arms build a fresh `Field` with no metadata.
- `build_join_schema` concatenates the two sides' fields, metadata included (`logical_plan/builder.rs:1663`). A self-join therefore carries the *same* id on both sides until something re-stamps one side.
- `FieldMetadata::add_to_field` / `add_to_field_ref` exist (`datafusion-common-54.1.0/src/metadata.rs:320-329`).

**To verify by a pinned test in S1 before anything depends on it:**
- `Union` / `union_by_name` output fields: whether metadata is kept, and from which input. If dropped or taken from the wrong side, RePark stamps union outputs from the first input in its own union path (Spark's rule).
- A temp view over a stamped plan, read back through `SELECT … FROM view` (the facade's join path builds joins as SQL over two temp views, `core.py:2792-2799`): whether the view's `TableScan` fields keep metadata.
- `Aggregate` group-key fields and `Window` pass-through fields.
- `Distinct`, `Limit`, `Sort`, `Filter`, `SubqueryAlias`: expected pass-through; pin anyway.

**Verified in the facade:** every child frame is constructed by `DataFrame._spawn` (`core.py:345-364`); `DataFrame(` is called in seven places in total (five in `core.py`, one each in `joins_columns.py`, `udf_bridge.py`), all reachable from `_spawn` or trivially routed through it. `replace_expr._inherit_plan_metadata` (`replace_expr.py:236-247`) is the copy site for the overlay, the origin map and `_fresh_outputs`. `Column` stores `_origin_plan_id` / `_origin_field` (`column.py:206-207`) and renders them into `__REPARK_QCOL_…` tokens (`:225-229`). `requalify_join_sides` (`crates/repark-core/src/session/df_guards/case_bind.rs:258-276`) re-projects every joined field with `alias_qualified` and receives both sides' schemas: that is the join re-stamp site.

## 3. The design

### 3.1 The id and its carrier
- `AttrId`: an opaque string, `a` plus 12 hex chars from a per-process counter or a UUID prefix. Never derived from a name, a position or a plan id.
- Carrier: field metadata under one key, `repark.attr`. One key, one value, on every output field of every frame the facade holds.

### 3.2 `stamp(plan) -> Result<LogicalPlan>` (Rust, `crates/repark-core/src/session/df_guards/attr_id.rs`)
Looks only at the root node. Idempotent: stamping a fully stamped plan returns it unchanged.
- Root is a `Projection`: for each expression, strip `Alias` layers; if what remains is `Expr::Column`, the field already carries the input's id (inherit; if the input field somehow has none, mint); otherwise **mint a fresh id and set it through `Alias::with_metadata`, overriding whatever `to_field` copied** (this is the Cast rule). A bare, un-aliased non-column expression gets wrapped in an alias of its own display name to carry the id.
- Root is anything else (TableScan, Values, Filter, Join, Union, Aggregate, SubqueryAlias, …): if every output field carries an id, return the plan; otherwise add a pass-through `Projection` whose expressions are `Column(field).alias_qualified(qualifier, name).with_metadata(id)`, minting only for fields that lack one.
- Never rewrite below the root. Identity below the root was stamped when that frame was spawned.

### 3.3 `attribute_ids(plan) -> Vec<Option<AttrId>>`
Reads the root schema's `repark.attr` values by position. `None` is a bug after `stamp`, and the facade treats it as a HALT condition in tests (never as a wildcard, never as "distinct").

### 3.4 `resolve(plan, written, qualifier, rule, displays) -> Resolution`
- `displays`: the Spark-visible column names by position (today's `frame.columns`, the `_display_names` overlay where one exists), exactly as `match_display_names(plan, names, columns)` receives them today (`written_names.rs`, via `dataframe_names.rs:301`).
- `qualifier`: `Some("q")` for `q.v`, `L.id`, `t.ID`; matched against the field's `TableReference` and the frame's join qualifiers under the same `NameRule` (`same_relation` exists and is rule-aware).
- Returns `Bound(Vec<usize>)` (every hit position; they all share one id), `Ambiguous(Vec<usize>)`, or `Missing`.
- This single function replaces, at the facade: `_distinct_attributes`, `_shared_origin_column`, `_twin_identities`, `_engine_origin`, `_new_attribute`, and the origin-dedupe branches inside `_bind_written_column`, `_match_lenient_subset`, and the qualified-name path (`written_names.py:17-39`, `:66-127`, `:236-314`).

### 3.5 Where RePark enforces identity that DataFusion cannot
| Plan shape | Rule | Enforced by |
|---|---|---|
| Source (scan, `createDataFrame` values, `spark.sql` result) | fresh per field | `stamp` at first spawn |
| Bare column, alias of a column | inherits | DataFusion (verified) |
| Cast, arithmetic, function, literal, case, window, aggregate value | fresh | `stamp` overrides (Cast copies metadata otherwise) |
| Filter, Limit, Sort, Distinct, `alias()` | pass through | DataFusion (pin in S1) |
| Join | concatenate; then **any right-side id that also appears on the left is re-minted** | `requalify_join_sides`, which already sees both sides |
| Union | first input's id per position | RePark union path, once the S1 test says what DataFusion does |
| Aggregate | keys inherit, aggregates fresh | DataFusion for keys (pin), `stamp` for the rest |

### 3.6 Columns and parent references
`Column` gains `_attr_id: str | None`, set wherever `_origin_plan_id`/`_origin_field` are set today (`_bind_engine_display_column` `core.py:1827`, `_column_of`, `_shared_origin_column`'s replacement). A parent Column used against a child frame (`x.filter(d.v > 15)`, `x.drop(d.v)`) binds by finding the child position whose id equals the Column's; no position → the existing missing-attribute refusal. `_origin_plan_id`/`_origin_field` and the `__REPARK_QCOL_` token are deleted in S4, after every reader has moved.

### 3.7 The session rule is read live
`resolve` takes `rule` as an argument. The facade passes the session's *current* `spark.sql.caseSensitive` on every call (RC6-4: today the frame caches its creation-time snapshot). `frame_is_exact(frame)` becomes `session_is_exact(session)` or takes the conf value from Python; either is fine, but no per-frame cache.

### 3.8 Out of scope, named
- The SQL door's own ambiguity audit (`crates/repark-core/src/column_resolution/`) keeps its mechanism. `spark.sql()` frames are stamped at spawn like any other; the two mechanisms are not asked to agree in this unit.
- Display names as metadata (`repark.display`, retiring `_display_names`/`_engine_names`): a later unit. `resolve` takes `displays` by argument so this unit does not depend on it.
- RC5-5 (200-filter segfault), RC5-6 (SQL-path lambda rows), RC5-7 (parent-Column sort order on case twins), RC3-5 (Unicode-version case pairs): separate cards, as the verifier already said.

## 4. Slices (guided rounds; one round, one commit each, in order)

### S0 — the replay that gates everything (Muse, worker, no product edits)
Build `/tmp/oc-worker/direct/wo/attr-id-1/replay/` from the six verifier corpora (`cs2-verify-evidence/probe*.py`, `reverify-cs2-evidence`, `reverify2-cs2-evidence`, `reverify3-cs2-evidence`, `reverify4-cs2/corpus`, `reverify5-cs2`) plus `python/repark/tests/casesens_2_spark_oracle.json`. One driver, `replay.py <engine> <out-dir>`, in the `cs_probe5.py` shape, every cell keyed. Record three answer sets: `spark.json` (through `/tmp/oc-worker/_lib/jvm-lock.sh /tmp/sparkenv/bin/python`, once), `main.json` (a fresh `make develop` of `origin/main`), `pr881.json` (head `121c4bd0`). Print the counts: cells, cells where main ≠ Spark, cells where 881 ≠ Spark, cells where 881 ≠ main. Time the whole replay on both builds. HALT if fewer than 600 cells assemble, or if any corpus needs Spark for more than 10 % of its cells and Spark is unavailable.

### S1 — the Rust core (Opus 5.5 executor; this is the design-heavy round)
- `crates/repark-core/src/session/df_guards/attr_id.rs`: `AttrId`, `stamp`, `attribute_ids`, `resolve`, the join re-mint helper, re-exported through `frame_names` (≤3 `pub use` lines in `case_bind.rs`; `case_bind.rs` stays ≤ its current count).
- `crates/repark-python/src/dataframe_names.rs`: three pyfunctions — `stamp_attribute_ids(frame) -> PyDataFrame`, `attribute_ids(frame) -> list[str | None]`, `resolve_display_name(frame, written, qualifier, displays, exact) -> (str, list[int])` with the first element `"bound" | "ambiguous" | "missing"`. `requalify_join_sides` gains the re-mint.
- Propagation pins, written **first**, one per row of §3.5, each asserting the ids by position after the node: Column, Alias(Column), Cast (fresh), arithmetic (fresh), Filter/Limit/Sort/Distinct/SubqueryAlias (same), Join (left same, colliding right re-minted), Union (whatever DataFusion does, then what RePark guarantees), Aggregate keys (same), temp-view round trip (same). A `to verify` row whose test contradicts §2 is a HALT with the measured behaviour, not a silent change of rule.
- `resolve` pins over `(id, Data, s)` under both rules: one hit; two hits one id (twins of one attribute); two hits two ids; qualified hit; qualified miss; folded-duplicate keys.
- Mutations recorded in the ledger: M1 `stamp` mints one id for every field → the ambiguity pins go red; M2 join re-mint disabled → the self-join pins go red; M3 `resolve` ignores ids (groups by display only) → the same-attribute-twin pins go red; M4 Cast override removed → the cast-twin pin goes red.
- No facade change in S1. Gate: `cargo test -p repark-core --lib` (FULL), `cargo test -p repark-python --lib`, clippy `--all-targets -- -D warnings -A clippy::disallowed_methods`, fmt, `check_lib_rs.sh`, `check_rust_file_size.py`, `check_map_md.sh --base origin/main`, the comment ban.

### S2 — the seam (Muse, guided)
`_spawn` calls `stamp_attribute_ids` on every `inner` before wrapping it; the seven `DataFrame(` sites either go through `_spawn` or call stamp themselves. `Column` gains `_attr_id`, set at the three bind sites named in §3.6. No name API changes yet. **Gate: the S0 replay is byte-identical to `pr881.json`** (0 changed cells) and the replay time is within 1.2× of S0's 881 timing. If stamping changes a cell, that cell names a place where a projection was silently load-bearing; HALT with the cell.

### S3 — the cutover, one family per round (Muse, guided; S3a…S3e)
Each round moves one family to `resolve` and deletes the helpers that family alone used, in the same commit:
- S3a `select` / `__getitem__` / `_column_of` / `_rebind_stable_name_column`;
- S3b `filter(str)` and `orderBy` / `sort` (the `sort_names.rs` twin logic and `child_sort_target` go; `predicate_names.rs` keeps its parse and calls `resolve` for each name);
- S3c `withColumn(s)` / `withColumnRenamed` / `withColumnsRenamed` (folded-duplicate keys stay on `refuse_folded_duplicate_keys`);
- S3d `fillna` / `dropna` / `dropDuplicates` / `drop` (fan-out = every position in `Bound`);
- S3e qualified names on join children and `alias()` frames (`resolve_qualified_display_names` retires).
Gate per round: the S0 replay classified cell by cell — every RC S1 cell of that family EQUAL to Spark; 0 cells moved away from Spark against `main.json`; every other change either matches Spark or is a named residue. HALT if a round needs an "unknown identity" branch of any kind: that is the smell this unit exists to remove.

### S4 — delete the encodings (Muse, guided)
Remove `_origin_map` and its 15 propagation sites, `_engine_origin`, `_NEW_ATTRIBUTE`, `_new_attribute`, `_fresh_outputs`, `_origin_not_emitted`, `twin_identities`, `same_source_fields`, `_shared_origin_column`, `_distinct_attributes`, `_twin_identities`, `__REPARK_QCOL_`, `_origin_plan_id`/`_origin_field`; shrink the engine names to `__repark_sel_{n}`; lower the `core.py` EXCEPTIONS and CAP-1 rows by the shrink; ledger clauses PROVEN; registry paragraph. Gate: the full replay, the full `casesens_1`/`casesens_2`/`diffprobe` facade files, the crate lib sweeps, and a `grep` proving none of the deleted names remains.

### Then: #881
Branch `feat/attr-id-1` from `main`, not from `feat/casesens-2-s4`. When S4 is green, rebase the #881 charter commits (`5b730b0c`, `730530e1`, `63acd9bb`, `4f927d2c`) onto it; the four fold commits (`0487afe7`, `15a7bb9d`, `b89a7d7f`, `3ac80920`) are re-judged by the replay rather than replayed: `predicate_names.rs` parsing and the lambda scoping (RC4-1, RC4-6, RC5-2) are kept and re-pointed at `resolve`; the origin dedupe, `is_bare_bind`, the alias-source tracking and the `sort_names.rs` twin logic are dropped because `resolve` covers them. #881 closes in favour of the new PR, or is force-updated to the rebased stack; the owner decides which.

## 5. Halt rules (every slice)
1. A §2 "to verify" test contradicts the expected propagation.
2. A cell that is EQUAL on `main.json` moves away from Spark.
3. Any code path needs to treat a missing id as anything but a bug.
4. The replay slows by more than 1.2× against S0's 881 timing.
5. A family cannot be moved without changing an EQUAL `casesens_1` pin.

## 6. Sizes and tiers (judgement, not measured)
S0 ≈ 1 Muse round. S1 ≈ 350–450 Rust lines with tests, one Opus 5.5 round at high, plus one Opus verifier pass on the propagation pins. S2 ≈ 60 Python lines, one Muse round. S3 ≈ 5 Muse rounds, net negative in `core.py`/`written_names.py`. S4 ≈ 1–2 Muse rounds, net −400 to −600 lines. One Opus verifier at the end over the whole stack, with the S0 replay as its corpus. Expected total: 9–10 executor rounds, 2 Opus verifier passes; against the five re-verify cycles #881 has already consumed.

## 7. Owner decisions before S1
- OD-1: new branch from `main` and re-judge the folds (recommended), or rebuild inside #881.
- OD-2: `repark.display` metadata (retiring the display overlay) is out of this unit (recommended) or in.
- OD-3: the four pre-existing cards from §3.8 are filed now, separately.

## 7a. Owner decisions made (2026-09-30)

The owner adopted the recommendations as written ("Go with your recommendations on the document"):

- **OD-1 ADOPTED:** branch `feat/attr-id-1` from `main`; the four #881 fold commits are re-judged by the S0 replay, not replayed. #881 stays on hold and is not folded again.
- **OD-2 ADOPTED:** display-name metadata (`repark.display`) is out of this unit; `resolve` takes `displays` by argument.
- **OD-3 ADOPTED:** the four pre-existing findings are filed now as separate mid-term cards, in a docs PR of their own: `task/roadmap/mid-term/plan-depth-1-card-2026-09-30.md` (RC5-5), `sql-lambda-scope-1-card-2026-09-30.md` (RC5-6), `sort-parent-column-1-card-2026-09-30.md` (RC5-7), `unicode-case-version-1-card-2026-09-30.md` (RC3-5 / R-CS2-17).

S0 may start on this ruling; S1 goes to an Opus 5.5 executor at high with this file as its work order.

## 8. Risks
- **Plan-time cost**: a pass-through projection per spawn whenever the root is not a projection. RC4-8 already flagged +50 % on string filters; the 1.2× halt in S2 is the guard, and the optimizer removes trivial projections at execution.
- **Views and caching**: `cache()`, `checkpoint()`, `createOrReplaceTempView` materialise plans; a materialised scan must keep ids (the S1 temp-view pin covers the read-back; add a cache pin in S2).
- **The SQL door**: `spark.sql("SELECT v, v FROM t")` yields two fields with distinct ids after stamp (both fresh at the root) although Spark would give them the same `exprId`. This matters only when a DataFrame name API is then applied to that frame; record it as a dated residue if the replay shows a cell, since the SQL door's identity is out of scope here.

## 9. S1 outcome and orchestrator rulings (2026-09-30)
S1 concluded on `feat/attr-id-1` with three commits: ee3ce129 (pins), 4570d134 (core) and ce7fe8e2 (union ids; M1–M4). The gate is green. Measured Union behaviour: DataFusion intersects the ids of the inputs that carry a column, so `stamp` re-stamps every Union root position from the first input's id.

S1's questions, and the orchestrator's rulings:
- **Q1: user `alias()`.** Spark mints a new attribute for an alias. In S3, the facade gives a user alias a fresh id through the alias's own metadata. §3.5's "alias of a column inherits" then applies only to RePark's internal re-projections. The S0 replay decides the cells, and a cell that disagrees is a HALT with the cell.
- **Q2: the id leaking into output.** `repark.attr` must never reach a written file or an export. S2 strips it at every write sink (parquet, CSV, JSON, Iceberg, s3a) and every export (`toArrow`, `toPandas`, `collect` schemas, `_repr`/`schema` field metadata). S2 pins that no `repark.attr` key survives in any written file's schema or in any exported Arrow schema.
- **Q3: USING joins.** S2 adds the re-mint to `join_on_keys`, pinned with a self-join through USING.
- **Q4: the frame's join qualifiers.** S3e passes the facade's Python-held join qualifiers into `resolve`.

Next: one Opus verifier pass on the S1 propagation pins, per §6. It runs after the #889 re-verify, because Opus runs one at a time.

### 9a. S2b outcome: where ids live (2026-09-30, claude-opus-5-5)
**Diagnosis.** S2's 23 neighbour failures (g4b_semi_join 18, casesens_1 3, CSV/JSON round trip 2) have one cause, reproduced in Rust on DataFusion 54.1 alone (`crates/repark-core/src/session/tests/attr_id_seam.rs`). `stamp` sets ids through `Alias::with_metadata`. DataFusion's physical planner drops an `Alias`'s metadata unless the aliased expression is a literal (`datafusion-physical-expr-54.1.0/src/planner.rs:123-136`), so the stamp's pass-through Projection over a clean source (S2's providers are born clean) yields a keyed logical field and a clean physical one. The Aggregate planner compares the two including field metadata (`datafusion-54.1.0/src/physical_planner.rs:1007-1068`) and fails. No optimizer rule is involved: `OptimizeProjections` keeps the stamp Projection because its aliases carry metadata. Every failing family plans an Aggregate over a stamped frame: `count()` after a semi join, the case-sensitivity pins' aggregates, and `inferSchema`'s `_promote_csv_string_types` probe `agg(sum(CASE … TRY_CAST …))`.

**Choice: the second direction.** Ids live on unoptimized and analyzed logical plans only. `StripAttributeIds` (`df_guards/attr_id.rs`) is the first optimizer rule of every core session (`repark_strip_attribute_ids`, ahead of DataFusion's list). It drops `repark.attr` from every `Alias`'s metadata and every outer-reference field, subqueries included, and recomputes each touched node's schema with DataFusion's own `recompute_schema` (a Union takes `intersect_metadata_for_union` of its inputs). Logical metadata then derives only from the sources, as the physical plan's does, so DataFusion's check is not weakened, and a source that carries a foreign `repark.attr` keeps it on both sides. The first direction is not reachable without changing DataFusion: a Column's physical field is its input's field, and nothing but a literal can carry alias metadata into the physical plan.

**Why an optimizer rule and not an analyzer rule.** The first cut was an analyzer rule, and it broke two S2 binding pins: the Spark SQL door analyzes every statement eagerly (`repark_functions::analyze_eagerly`, called from `repark-spark/src/spark_ast.rs`) and hands the facade the analyzed plan, so an analyzer-stage strip erases the ids of every `spark.sql()` frame and of the facade's join shape (SQL over two temp views, §2). The optimizer runs only when a plan executes.

**Why §3.4 and §3.6 still hold.** `attribute_ids` and `resolve` read `DataFrame::schema()`, the frame's unoptimized (or eagerly analyzed) plan, which the optimizer never rewrites; a parent Column binds against the child frame's unoptimized plan. The only optimized plan the facade re-wraps is the cache read-back (`register_collected_memtable`), whose MemTable is born clean (S2's R-5), so no resolution needs ids after optimization. The stamp Projections stay in the optimized plan as trivial `c AS c` aliases (measured; `OptimizeProjections` keeps them), so the replay timing bar still guards their cost.

### 9b. S1b outcome: structural freshness (2026-09-30, claude-opus-5-5)
**§3.2 amended.** "Never rewrite below the root" now reads "never rewrite a node that was the root of a stamped frame". A same-op Window or Aggregate below the root is read to classify its outputs, and is not rewritten; the ids go on the root Projection or on the pass-through Projection `stamp` adds.

**R1 as built.** `computed_outputs(node)` answers by position which outputs a same-op node computed. It skips a Filter/Sort/Limit chain (HAVING, ORDER BY, LIMIT over the aggregate). A Window computes every output past its input width and passes its input's answers through (so stacked Windows chain). An Aggregate computes its grouping id, every aggregate value, and every group key that is not a bare `Column` over a non-computed input position. Any other node computes nothing. An Aggregate or Window root mints every computed position; a Projection root keeps a column only when it references a non-computed input position. No metadata is read to decide same-op: a Window or Aggregate that was a stamped frame's root always sits under that stamp's pass-through Projection, because it has at least one computed output to mint, except an Aggregate with no computed output, where both readings keep every id. The Filter/Sort/Limit skip goes one step past "directly below" for SQL `HAVING`; it is safe for the same reason. What R1 does not reach, and stays with the SQL door's own mechanism per §3.8: a SQL `ORDER BY` root over a Projection over an Aggregate, and a nested SQL subquery whose inner Projection copies an id through a cast.

**VA1-5.** An id is `a` + a 16-hex per-process prefix (a `RandomState` hash of the process id, fixed once) + a 12-hex counter. `AttrId::is_native` accepts only this process's prefix and length, and `stamp` keeps only native ids, so a foreign id on a source is re-minted once while a RePark-stamped frame keeps its ids across a re-stamp. Measured: DataFusion's default parquet read (`skip_metadata`) already drops field metadata, so the key reaches a plan only through a `skip_metadata(false)` read or an Arrow import that carries it.

**S2b measured (2026-09-30).** Two more changes were needed. The strip turns an emptied alias over a same-named column back into the bare column, so the stamp's pass-through Projections fall to `OptimizeProjections` and a stamped frame optimizes to its unstamped twin's plan (without it DataFusion's `push_down_leaf_projections` fails on a stamped struct access, even on stock DataFusion). And `_bound_attr_id` stopped re-running the analyzer on every bind (22 of 82 s of the `r3` corpus). Replay after both: 4 judged cells differ from `main.json` (error text only, 0 cells move away from Spark: `r2.{F,T}_oj_twinjoin_filt`, `r3.{F,T}_ob_agg_max`), median 597.27 s against the 597 s bar. Both residues come from the stamp's `Alias(Column)` wrappers in the unoptimized plan, which the optimizer cannot reach; removing them needs the ids carried without a wrapper (for example on the source provider's schema, which keeps the logical and physical plans in agreement), which is a design change, not an S2b fix.

## 9c. Orchestrator rulings on the S2b and S1b halt (2026-09-30)
S2b and S1b are committed: e5db526e (the `StripAttributeIds` optimizer rule; DataFusion 54.1 drops alias metadata physically unless the aliased expression is a literal) and eeffdc43 (structural freshness and per-process id prefixes). The neighbour sweep passed 2159 of 2159 on both parts.
- **Q1 is accepted as a dated residue.** Four replay cells change their error text only: `r2.{F,T}_oj_twinjoin_filt` and `r3.{F,T}_ob_agg_max`. All four were already non-EQUAL on main: Spark gives AMBIGUOUS_REFERENCE or rows, and main errors. They stay errors on head, so no cell moved away from Spark. S3b (orderBy/filter) is expected to close them, and S3b's gate names them.
- **Q2 is re-measured by S3a.** The Part B median was 606 s against the 597 s bar, at load 8.5 against 6.5. S3a first re-runs the replay 3 times on a quieter box. If the median is still above 597 s, S3a applies the executor's cut: `DataFrame.__init__` returns the same handle when the frame is already stamped.
- **Q3 is accepted.** An ORDER BY root over an aggregate, and a nested subquery with an inner cast projection, stay with the SQL door (§3.8).

## 9d. The S3a halt (2026-09-30 evening)
S3a found 5,252 changed cells against main: 0 moved away from Spark, 3,925 FIXED and 1,208 named residues. The neighbour sweep was 2,182 green. It halted on timing: a 635.6 s median against the 597 s bar. Cause: FIXED cells now execute instead of raising.
- **Ruling:** the timing bar is like for like, over cells whose outcome class is unchanged against main, at most 1.2x. The time spent on FIXED cells is reported but carries no bar.
- **Confirmed:** R5 and R9/R12-qualified are S3e-owned residues.

## §9e S3b halt H-1 rulings (orchestrator, 2026-10-01)

The S3b head (15c1ea17) closes all four §9c cells and fixes 7,426 cells, but 179 main-equal cells move away from Spark, and 24 S3a gains regress. The halt was correct. Rulings by bucket:

- **B1 (103 cells, plus the 24 `j_cross` S3a regressions; the 24 return one side's rows where Spark refuses, which is a wrong answer):** the walker takes S3a's uniqueness guard. Bind a marked reference only when the bound id has a **unique engine field**. Otherwise leave the token to the shaped hook, which refuses with AMBIGUOUS_REFERENCE as main does. Use one shared guard function for select and filter, not a copy.
- **B2-sort (60 cells), Q1:** Spark's sort resolves against the output first and then against the child's input scope (missing-reference resolution). So a sort key with **0 hits among the output displays is not refused**. It falls through to the engine exactly as main does. The resolve rule decides only when there is at least one hit among the outputs: one id binds, and more than one is AMBIGUOUS_REFERENCE.
- **B2-lambda (5 cells), Q3:** the **tokenizer owns lambda scope**, because it owns the text, as §8 already says ("the lambda scoping (RC4-1, RC4-6, RC5-2) is kept"). A lambda-bound variable (`X -> …`, `(a, b) -> …`) is never a column token inside its lambda body under either case rule. #881's fold commits carry this scoping: port the scoping logic from `pull/881/head` (fetch it), not the origin dedupe. Keep the parsing where it is.
- **B3 (8 cells) and B4 (3 cells), Q2:** Spark's order for a dotted name. A dotted token `a.b…` is qualifier plus name **only when `a` matches a relation qualifier that the plan carries** under the live case rule. Every other dotted token (a struct field access, or a facade-held qualifier that the plan lost) takes **main's path unchanged**: the old bare-ident tokenization and main's raise. Facade-held qualifiers are S3e's (Q4). If a token matches both a plan qualifier and a struct column, measure it against Spark and pin it.
- **Gate unchanged:** 0 cells away from Spark against `main.json`; S3a's gains do not regress (0 of the 203); the four §9c cells stay FIXED; timing, sweep and gate.sh.

## §9f S3c halt Q1 ruling (orchestrator, 2026-10-01)

The S3c brief said "a renamed column keeps its id". **Live Spark 4.1.2 contradicts this.**
- **The measurement:** the orchestrator measured it independently with `rename_probe.py`. `spark.range(10).withColumnRenamed("id","v").select("v","v")` binds `filter("v > 5")` (4 rows). After `withColumnRenamed("v","w")` or `withColumnsRenamed({"v":"w"})`, `filter("w > 5")` and `select("w")` raise AMBIGUOUS_REFERENCE under both case rules.
- **The ruling:** a rename mints **one fresh id per renamed position**, exactly as an alias does in S3a (Q1). The singular rename keeps its `Column.alias` route. The plural rename's native plain alias (`core.py` about 3185, which inherited the id through stamp) moves to the fresh-id alias.
- **Pin:** renamed twins followed by `filter`, `select(str)` and `select(F.col)` all give AMBIGUOUS_REFERENCE under both rules.
- **Mutation:** "the rename keeps the id" turns those pins red. This replaces the brief's "rename mints a fresh id" mutation.
