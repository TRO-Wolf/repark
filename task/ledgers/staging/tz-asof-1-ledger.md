# Unit ledger — WO TZ-ASOF-1 · an expression named like its ORDER BY key plans

**Date:** 2026-09-26 · **Branch:** `feat/tz-asof-1` · **Base:** `4c5c2be8`
(`origin/main`) · **Commit:** the branch commit (rebased) ·
**Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Scoreboard cell `E-TZ-TIMESTAMP-AS-OF` runs `SELECT CAST(committed_at
AS STRING) FROM t.snapshots ORDER BY committed_at`, feeds the first row back as a
`TIMESTAMP AS OF` literal, and expects `rows [[1]]`. On main the snapshots query
refuses `Error during planning: Projections require unique expression names but
the expression "CAST(hc.ns.y.ts AS Utf8View)" at position 0 and "hc.ns.y.ts" at
position 1 have the same name`, so the cell cannot run. The same refusal hits
every `SELECT CAST(col AS STRING) … ORDER BY col` shape on the Spark door.

**Root cause (verified against DataFusion 54.1 sources).** DataFusion names
`Expr::Cast` after its inner expression (`expr.rs`, `SchemaDisplay` for `Cast`
renders only the inner expression, by design for Postgres/Spark consistency), so
`CAST(sc.ns.y.ts AS Utf8View)` carries the schema name `sc.ns.y.ts`. When an
ORDER BY key is missing from the projection, `LogicalPlanBuilder::sort_with_limit`
(`datafusion-expr` `builder.rs`) appends the missing sort columns to the inner
projection, sorts, and projects them away again — but the intermediate
`project()` runs `validate_unique_names`, which fails on the two `sc.ns.y.ts`
fields before the sort is ever built. RePark never sees a plan to repair; the
fix must land before `statement_to_plan`. `SELECT DISTINCT … ORDER BY
<un-projected>` fails on a second DataFusion rule in the same function
(`ambiguous_distinct_check`: ORDER BY expressions must appear in the select
list). Shapes that already plan (`SELECT id + 1 … ORDER BY id`) leak qualified
DataFusion names (`sc.ns.y.id + Int64(1)`) because the root plan node is a `Sort`,
not the `Projection` that `SparkProjectionDisplay` rewrites.

**Fix.** `crates/repark-spark/src/normalize/sort_key_projection.rs` (called from
`spark_ast.rs` `execute_passthrough_inner`, after `apply_spark_order_by_defaults`
so moved keys keep Spark null placement, before the lowering rewrites so the
namer sees user casts): when a single-`SELECT` query (DISTINCT or not) carries an
`ORDER BY` key that is not projected — resolving bare/compound identifiers
against select aliases first, then select-list column references, mirroring
DataFusion's merged-schema resolution, with ordinals always projected — the
statement is rewritten to select from a derived table that projects every select
item under an internal `__repark_sort_sel_<i>` alias plus every un-projected key
under `__repark_sort_key_<k>`, with the outer query carrying the remapped
`ORDER BY` (options travel with each key), the original `LIMIT`/`OFFSET`/`FETCH`,
and one quoted outer alias per select item holding Spark's display name
(explicit alias verbatim; bare column as written, matching `display_rewrite`;
`CAST(col)` → the column; `CAST(<non-column> AS T)` → `CAST(<child> AS T)`;
binary expressions parenthesized; function calls lower-cased with unqualified
arguments). The derived-table alias is `__repark_sort`. On a `DISTINCT` select
the rewrite fires only when every `ORDER BY` key binds a select item (fold r1,
verifier V-003 — a hidden key inside the dedup would duplicate rows; see R-4). Anything outside the known set — set operations,
stars, `ExprWithAliases`, `SELECT INTO`, `TOP`, `EXCLUDE`, `PREWHERE`,
`CONNECT BY`, Hive `CLUSTER/DISTRIBUTE/SORT BY`, non-`Expressions` orderings,
`WITH FILL`, locks, `FOR`/`SETTINGS`/`FORMAT`/pipe clauses, unmapped cast
targets or operators, wildcard function arguments, windowed or filtered calls,
subquery/case/predicate expressions, ambiguous alias matches, out-of-range
ordinals, a `__repark_sort` collision with user text, and the case-sensitive
door (whose `strict_single_table` guard only recognizes a bare single table) —
bails with the statement untouched, so every such shape keeps exactly today's
behavior, failure or not. When the rewrite fires and the rewritten statement
then fails anywhere from lowering through the plan guards, the passthrough retries once
with the rewrite disabled and returns that outcome, so the rewrite can only turn
failures into successes, never change a failure (caught by
`u9_map::a_map_operand_refuses_comparison_ordering_and_distinct_as_spark_does`, whose
MAP-ordering refusal quoted the internal key name before the fallback).
No user-visible name changes except the measured Spark-equal display names
listed in C-009 (fold r1, verifier V-004 — the parity goal itself; main leaked
qualified names on shapes that already planned), plus the one measured
Spark-equal key binding listed there (fold r2, verifier V-006 — an unqualified
key also binds a qualified select item on its last segment, so the sort runs
on the output column like Spark's).

## Plan

- [x] Reproduce every brief shape on the branch; measure Spark's names from
  `/tmp/oc-worker/direct/probes/tz-order-spark-2026-09-26.json`.
- [x] Verify timestamp-to-string rendering follows Spark's rule (trailing
  fractional zeros trimmed); no rendering fix needed.
- [x] Land the rewrite + wire it into the passthrough.
- [x] Pin every shape `==` (columns and rows) in `tz_asof_1.rs` (Rust) and
  `test_tz_asof_1.py` (facade), plus the no-change neighbors.
- [x] Registry row, map lockstep, replay EQUAL, gates, commit, hand-back.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `SELECT CAST(ts AS STRING) FROM y ORDER BY ts [DESC]` and `… LIMIT 1` plan with columns `[ts/string]` and Spark's ordered rows. | Rust + facade pins `==` Spark's `cast_order`, `cast_order_desc`, `limit_cast_order`. | PROVEN | `tz_asof_1.rs::cast_over_its_order_key_*` (3), `test_tz_asof_1.py::test_cast_over_its_order_key_*` (3) |
| C-002 | The alias shapes keep today's answers: `AS ts` → `ts` with alias-sorted rows, `AS t2` → `t2`. | Rust + facade pins `==` Spark's `cast_alias_same_name`, `cast_alias_other`. | PROVEN | `tz_asof_1.rs::cast_aliased_*` (2), `test_tz_asof_1.py::test_cast_aliased_*` (2) |
| C-003 | Expression projections show Spark's display names: `id + 1` → `(id + 1)`, `upper(s)` → `upper(s)`, with Spark's rows. | Rust + facade pins `==` Spark's `expr_order`, `upper_order`. | PROVEN | `tz_asof_1.rs::arithmetic_projection_names_the_paren_form`, `::function_projection_names_the_call_form`, facade twins |
| C-004 | Two casts over two keys plan with columns `ts`, `id` and Spark's rows. | Rust + facade pins `==` Spark's `two_casts`. | PROVEN | `tz_asof_1.rs::two_casts_over_two_keys_answer_like_spark`, facade twin |
| C-005 | `SELECT DISTINCT CAST(ts AS STRING) … ORDER BY ts` answers columns `[ts/string]` with the distinct rows (the key binds the output column; the dedup sees only select items). | Rust + facade pins `==` Spark's `distinct_order_unselected`. | PROVEN | `tz_asof_1.rs::distinct_cast_with_unprojected_key_answers`, facade twin; M3 reds with DataFusion's original refusal |
| C-006 | `SELECT CAST(max(ts) AS STRING) … GROUP BY s ORDER BY max(ts)` answers columns `[CAST(max(ts) AS STRING)/string]` with Spark's rows. | Rust + facade pins `==` Spark's `agg_order`. | PROVEN | `tz_asof_1.rs::cast_of_aggregate_names_the_full_cast`, facade twin |
| C-007 | `SELECT CAST(committed_at AS STRING) FROM y.snapshots ORDER BY committed_at` plans with columns `[committed_at/string]`; RePark's timestamp-to-string rendering trims trailing fractional zeros like Spark (`.56`, full micros kept, whole seconds bare). | Rust + facade pins; rendering pins on literal instants. | PROVEN | `tz_asof_1.rs::snapshots_cast_orders_and_trims_like_spark`, `test_tz_asof_1.py::test_snapshots_cast_orders_and_trims_like_spark` + `::test_timestamp_rendering_trims_like_spark`; no rendering fix needed |
| C-008 | Scoreboard cell `E-TZ-TIMESTAMP-AS-OF` replays EQUAL on every `obs` key. | Harness replay vs `out/spark-edge.json`. | PROVEN | `target/probe-tz-asof-1/replay.json` EQUAL on `obs.rows`; facade twin `test_timestamp_as_of_cell_shape_answers_first_snapshot` |
| C-009 | Shapes outside the trigger keep byte-identical behavior: projected keys, stars, unions, ordinals, the case-sensitive door, and unnameable select items. No user-visible name changes except the measured Spark-equal display names on three shapes that already planned on main (fold r1, verifier V-004 — rows unchanged, names now `==` Spark): `SELECT CAST(id AS STRING) FROM sc.ns.z ORDER BY st.a DESC NULLS LAST` shows `id` not `sc.ns.z.id` (probe `nulls`), `SELECT CAST(ts AS STRING) FROM sc.ns.z ORDER BY st.a` shows `ts` not `sc.ns.z.ts` (probe `struct_order_cast`), and `SELECT id + 1 FROM sc.ns.y ORDER BY id` shows `(id + 1)` not `sc.ns.y.id + Int64(1)` (WO `expr_order`). A fourth already-planned shape changes both name and rows (fold r2, verifier V-006, r2 probe `field_item_bare_key`): an unqualified key binds a qualified item on its last segment, so `SELECT st.s FROM sc.ns.z ORDER BY s, ts` shows `s` with rows x, y, z where main and the rewrite-off door showed `sc.ns.z.st[s]` with y, z, x — `==` Spark. | Neighbor pins asserting today's names/rows/errors; probe diffs for the named shapes. | PROVEN | `tz_asof_1.rs::projected_keys_stars_and_unions_keep_todays_names` and `::unqualified_key_binds_a_qualified_items_last_segment`, facade twins, 13 inline AST unit tests, probe `target/verify/repark.json` EQUAL to `spark.json` on `nulls`/`struct_order_cast` (names) and `arithmetic_projection_names_the_paren_form` for `(id + 1)`, plus the retry fallback in `spark_ast.rs` (any post-rewrite failure replays the passthrough once with the rewrite off) held by `u9_map::a_map_operand_refuses_comparison_ordering_and_distinct_as_spark_does`, which reds quoting `__repark_sort_key_0` without it |
| C-010 | A compound (multi-part) `ORDER BY` key binds to a select item only when the item's qualifier chain matches in full; an unqualified item never matches, and a compound key whose last segment clashes with a select column without a full match bails the rewrite. `SELECT s FROM z ORDER BY st.s, ts` keeps main's answer `b, a, b` (verifier finding V-001, probe `struct_field_name_clash`). | AST bail pins + Rust and facade row pins `==` Spark. | PROVEN | `sort_key_projection.rs::compound_key_clashing_with_an_item_name_bails` + `::compound_key_without_a_name_clash_stays_a_hidden_key`, `tz_asof_1.rs::struct_field_key_never_binds_a_same_named_column`, `test_tz_asof_1.py::test_struct_field_key_never_binds_a_same_named_column` |
| C-011 | An unqualified bare-identifier `ORDER BY` key equal to a select item's display name binds to the output column like Spark: `SELECT CAST(id AS STRING) FROM z ORDER BY id` answers `10, 2, 3` and `… DESC` answers `3, 2, 10` (string order, verifier finding V-002, probes `cast_id_order_id`/`cast_id_order_id_desc`); a qualified key keeps binding to the column. | AST binding pins + Rust and facade row pins `==` Spark. | PROVEN | `sort_key_projection.rs::bare_key_matching_a_display_name_binds_the_output_column` + `::qualified_key_keeps_binding_the_column`, `tz_asof_1.rs::bare_key_matching_the_display_name_sorts_the_output_column`, facade twin |
| C-012 | On `SELECT DISTINCT` the rewrite bails unless every `ORDER BY` key binds a select item by name, so a hidden key can never join the dedup: `SELECT DISTINCT s FROM z ORDER BY ts` keeps main's refusal `For SELECT DISTINCT, ORDER BY expressions sc.ns.z.ts must appear in select list` byte for byte where Spark refuses `[UNRESOLVED_COLUMN.WITH_SUGGESTION]` (verifier finding V-003, probes `distinct_dup`/`distinct_cast_dup`); `DISTINCT CAST(ts AS STRING) … ORDER BY ts` still answers through the C-011 binding. | AST bail pins + byte-for-byte refusal pins in Rust and on the facade. | PROVEN | `sort_key_projection.rs::distinct_with_a_key_that_cannot_bind_bails`, `tz_asof_1.rs::distinct_with_an_unbindable_key_keeps_the_refusal`, facade twin |

## Mutation record (2026-09-26)

Each line was broken, the named tests ran, and the file was restored.

| # | Mutation | Red |
|---|---|---|
| M1 | outer alias leaks the internal `__repark_sort_sel_0` name instead of Spark's display name | 5 name pins red (`cast_over_its_order_key_*`, `cast_aliased_apart`, `cast_of_aggregate`, `arithmetic_projection`), restored green |
| M2 | the derived table drops the projected sort keys | `cast_over_its_order_key_answers_like_spark` reds with `UNRESOLVED_COLUMN __repark_sort_key_0`, restored green |
| M3 | `DISTINCT` bails out of the rewrite | `distinct_cast_with_unprojected_key_answers` reds with DataFusion's original `must appear in select list` refusal, restored green |

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: tz-asof-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause is walked against Spark 4.1.2's measured answer on the same door; the six failing shapes refused red on the base tree with DataFusion's unique-names/DISTINCT texts and answer green after, the Rust module pins columns, types and rows, the facade suite pins the same shapes through the product surface, and the harness replay holds the cell EQUAL.
      artifacts: [crates/repark-spark/src/tests/tz_asof_1.rs, python/repark/tests/test_tz_asof_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Every brief shape is pinned (cast asc/desc/limit, both alias spellings, arithmetic, function call, two casts, DISTINCT, aggregate cast, snapshots read, three rendering instants, the AS OF leg), with the no-rewrite neighbors asserting today's names and the AST unit tests pinning ordinals, projected keys, stars, unions, unnameable items, alias keys and marker collisions.
      artifacts: [python/repark/tests/test_tz_asof_1.py, crates/repark-spark/src/normalize/sort_key_projection.rs]
    - id: AT-3
      status: ATTACKED
      evidence: "Every failure stays byte-identical to today: any post-rewrite failure retries the passthrough once with the rewrite disabled and returns that outcome, held by the u9_map MAP-ordering pin (reds quoting the internal key without the fallback); bails preserve today's errors (out-of-range ordinals, alias keys, marker collisions) at AST level."
      artifacts: [crates/repark-spark/src/spark_ast.rs, crates/repark-spark/src/tests/u9_map.rs]
    - id: AT-4
      status: N/A
      justification: "No shared or mutable state, no concurrency: one pure AST rewrite per statement, and the retry replays planning sequentially on failure only."
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, deserialization or path handling; SQL text only.
    - id: AT-6
      status: ATTACKED
      evidence: No stored format changes; the snapshots committed-at read and the TIMESTAMP AS OF leg guard the metadata shape the cell reads, on both doors' harnesses.
      artifacts: [crates/repark-spark/src/tests/tz_asof_1.rs, python/repark/tests/test_tz_asof_1.py]
    - id: AT-7
      status: N/A
      justification: No performance claim; one AST walk per ORDER BY query plus error-path-only double planning, both cold relative to execution.
    - id: AT-8
      status: ATTACKED
      evidence: No new dependency, no crate edge, no ceiling touched (new module files only, router/lib untouched); no comments per the lane ban; clippy pedantic and rustfmt clean; the retry is safe because the rewrite fires only on SELECT statements, never on DML, so no write can execute twice.
      artifacts: [crates/repark-spark/src/normalize/sort_key_projection.rs, crates/repark-spark/src/spark_ast.rs]
    - id: AT-9
      status: ATTACKED
      evidence: "Refusal texts are pinned, not presumed: the MAP-ordering text is byte-identical through the fallback, and M2/M3 red with the exact unresolved-column and DISTINCT texts."
      artifacts: [crates/repark-spark/src/tests/u9_map.rs, crates/repark-spark/src/tests/tz_asof_1.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Each pin asserts exact names, types and rows; M1 breaks the outer alias (5 name pins red), M2 drops the key projection (unresolved-column red), M3 drops DISTINCT from the trigger (original DISTINCT refusal red), all restored green.
      artifacts: [crates/repark-spark/src/tests/tz_asof_1.rs, python/repark/tests/test_tz_asof_1.py]
  complete: true
```

## Residues

| # | Residue |
|---|---|
| R-1 | Dated 2026-09-26 (deliberate scope cut, no card): on the case-sensitive Spark door (`spark.sql.caseSensitive=true`) the rewrite stays off, so the colliding shapes keep today's planning refusal there; the bypass keeps `strict_single_table`'s bare-table guard exact. |
| R-2 | Dated 2026-09-26 (deliberate scope cut, no card): the native door (`repark.sql`) never reaches the Spark passthrough, so `SELECT CAST(ts AS STRING) … ORDER BY ts` still refuses there; native wants ANSI display names, a separate unit. |
| R-3 | Dated 2026-09-26 (pre-existing, not introduced here): the same select list shows Spark's name with `ORDER BY` and the qualified leak without it (`ts` vs `sc.ns.y.ts`); the rewrite fixes only the sort path per R1, and the leak stays pinned as expected divergence elsewhere. |
| R-4 | Dated 2026-09-26, superseded by fold r1 (verifier V-003, MEASURED): `DISTINCT … ORDER BY <un-projected>` deduping over (select items, sort keys) answered `b, b, a` — duplicates in a DISTINCT — where Spark refuses `[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name 'ts' cannot be resolved. Did you mean one of the following? ['s']. SQLSTATE: 42703` and main refuses `Error during planning: For SELECT DISTINCT, ORDER BY expressions sc.ns.z.ts must appear in select list`. The rewrite now bails on a `DISTINCT` select unless every key binds a select item, keeping main's text byte for byte. |
