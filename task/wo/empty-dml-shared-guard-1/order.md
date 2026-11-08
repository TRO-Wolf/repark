# EMPTY-DML-SHARED-GUARD-1 — build brief (SKELETON, not launchable)

**Status:** skeleton drafted 2026-11-08 by the orchestrator; rulings filled the same day. Launchable: the Step 0.3 table landed and confirms the class.

**Tier:** Claude Opus 5.5 (`claude-opus-5-5`), effort high. Verify your own model id from your transcript
before the first action; Opus 5.0 is banned (owner, 2026-10-17).
**Base:** `origin/main` at the SHA the launch prompt names (expected `fdc61f26` or its successor).
**Branch:** `fix/empty-dml-shared-guard-1`. **Workspace:** a scratch clone, never the live checkout.
**This lane never asks a question.** Everything it needs is below. If the code contradicts this brief,
stop in a clean state and hand back with the contradiction and the evidence.

## 1. Rulings carried (verbatim once given)

The owner's reply of 2026-11-08 to the Step 0.1 table, verbatim: **"Defaults."** The adopted defaults:

- R1: the Step 0.3 probe on merged main is approved; if it confirms the class, this unit runs ahead of the queue.
- R3: the hardening fixes (this guard, AUD-022/023, CTAS) ship as v1.5.6 when green; the v2 label is
  re-dated separately by a docs PR naming the released wheel that contains the audited SHA.
- R5: the upstream DataFusion issue is filed only after this guard merges, with owner-approved text. This
  unit files nothing upstream.
- R7: acceptance = every blocking finding resolved on the reviewed candidate; one PASS cancels no unresolved
  finding. This unit is design-heavy, so Opus 5.5 and Muse MAX verify the same head **in parallel**.
- Standing: one implementation lane at a time (2026-11-02); a reachable panic, wrong read or wrong write
  blocks the label whether or not an inventory listed it (owner gate statement); DML-reach rule and
  special-values row per family (2026-11-05); carding over fixing for engine-wide properties (2026-11-02).

## 2. The defect class (from the Step 0.3 table)

DataFusion 54.1.0 plans `LogicalPlan::Dml(Delete | Update)` by rebuilding the predicate and assignments
from the **optimized** input (`extract_dml_filters`, `extract_update_assignments` in
`datafusion/core/src/physical_planner.rs`). When `EliminateFilter` and `PropagateEmptyRelation` fold the
input to an `EmptyRelation`, the rebuild yields no filters (and, for UPDATE, no assignments), and the
Iceberg provider receives a statement indistinguishable from an unrestricted one.

Measured (PR #1271, branch `docs/step-0-3-probe-tables` at `88b9ab27`, `task/wo/empty-dml-shared-guard-1/evidence/`;
identical on main `fdc61f26` and PyPI 1.5.5): DELETE `WHERE false`, `NULL`, `st = CAST(NULL AS STRUCT<x: INT>)`,
`arr[1] = 1 AND 1 = 0` delete every row on the Python native door (`repark.sql()`, v2-cow) and the facade
(`spark.sql()`, v2-cow, v2-mor, v3); on the Python native door also `1 = 0`, `prim_col = NULL`, `id IS NULL`
(the facade answers those correctly through its plain route). UPDATE on the same predicates rewrites files and,
on v3, advances every row's `_last_updated_sequence_number`. Facade `DELETE … WHERE id IS NULL` commits an
empty snapshot. Read every row labelled `ROWS-DIFFER`, `EXTRA-COMMIT`, `LINEAGE-DIFFER`,
`ACCEPTED-WHERE-SPARK-REFUSED` in `empty-dml-table.md`; those are the cells this unit closes.

Controls that must stay as they are: `c-true`, `c-no-where`, `c-identity-update`, and `c-selective`
(`id = 1` deletes or updates exactly one row on every route and kind measured).

Missing evidence: Python native door on v2-mor, v3 and UUID tables. That door cannot create them (the clerk
found that `repark.sql()` plans through DataFusion directly, not through `crates/repark-sql`'s router, so
neither `WITH (...)` nor ALTER property spellings reach it). Your pins create those tables through the
Rust APIs or the facade inside the same session and then run the DML on the native door.

Behaviour against Spark's measured oracle: where Spark commits an empty snapshot for a no-op DML, or raises
`INTERNAL_ERROR` on `UPDATE … WHERE NULL`, or `FILTER_NOT_BOOLEAN` on `DELETE … WHERE NULL` (v2-mor), this unit
writes nothing and commits nothing; each such cell is recorded as a declared divergence row by the clerk.

## 3. Owning layer and seat

- **Owning layer:** DataFusion physical planning of `LogicalPlan::Dml`, reached through RePark's query planner.
- **Seat:** `StackQueryPlanner::create_physical_plan` (`crates/repark-core/src/stack/planner.rs`), installed on
  every core session by `context_with_df_54_1_rule_guards` (`crates/repark-core/src/session/df_guards.rs:84`).
  `SessionState::create_physical_plan` optimizes first and then hands the optimized plan to this planner, so
  the seat sees exactly the plan that will execute.
- **Behaviour:** a `Dml(Delete | Update)` node anywhere in the plan whose input, past any `Projection`, is an
  `EmptyRelation` with `produce_one_row == false` plans as "matches nothing": it returns the same output
  schema the DML node declares (the `count` column) with a zero count and never calls the provider. The
  mechanism (a logical rewrite before `DefaultPhysicalPlanner`, or an equivalent) is the builder's choice; the
  hand-back states which and why.
- **Pin that the seat is universal:** every session either door builds carries `StackQueryPlanner`
  (native SQL session, Spark facade session, the Python native session, and any session built for the
  `F.expr` context). Enumerate them from code and pin each.

## 4. Routes the brief requires you to enumerate (hand-back table, with file:line)

0. The Python native session (`crates/repark-python/src/session_runtime.rs`, `repark.sql()`), which plans through
   DataFusion directly and never enters `crates/repark-sql`'s router; this is where `WHERE 1 = 0` wipes a table.
1. SQL routes that reach the physical planner: native `router.rs` `execute_identity_or_delegate`
   (`crates/repark-sql/src/router.rs:237`) → `delegate` → `PreExecute`; the Spark passthrough in
   `crates/repark-spark/src/spark_ast.rs`.
2. Routes handled before physical planning, which this unit does not change: the metadata-delete door,
   `try_allowed_delete_in` (`crates/repark-iceberg/src/write/predicate_dml.rs:141`), `try_allowed_update_in`
   and `try_allowed_plain_identity_or_update` (`crates/repark-iceberg/src/write/predicate_dml/plain.rs:185,190`).
   For each, state which Step 0.3 control rows it serves.
3. Any direct-provider or public Rust path outside the session that calls `delete_from` or `update` on the
   Iceberg provider. State whether it can receive an empty predicate.
4. Plan wrappers and their existing refusals: `EXPLAIN ANALYZE` of a write and prepared writes stay
   **refusals** (`crates/repark-sql/tests/ansi_explain_dml_write.rs:122,138,153,187`). A refused wrapper is
   never turned into a successful no-op because its inner plan is empty. Pin this.

## 5. Constraints

- Empty filters alone prove nothing; empty assignments alone prove nothing. `extract_update_assignments`
  drops identity assignments, so `UPDATE t SET x = x WHERE p` is a control, not a trigger.
- Each door's result schema and affected-row report are preserved (native returns a `count` of 0; record the
  facade's shape from the table and preserve it).
- No fork-side defense in this unit (`assignments.is_empty()` refusal was withdrawn, planning §2.11).
- No change to the optimizer rule list, to `PreExecute::guard`, or to any route in §4 item 2.
- Not in the unit: struct naming, `F.expr` context retirement, any parity item, any ratchet or gate policy.
- `skip_empty_dml` on the parked branch `wip/struct-domain-8e929978` is superseded, not cherry-picked.
- No code comments (owner ruling 2026-08-26). Reasons go in the touched directory's `map.md` via the clerk.

## 6. Coverage claim (bounded; this is the clause)

A zero-row optimized DELETE or UPDATE input never becomes an unrestricted provider write, on every route in
§4 item 1. Not a proof of DML correctness; dropped conjuncts that do not fold to an empty relation are the
Step 4.3 DML-reach invariant's job and are outside this clause.

## 7. Domain table and pins

- The Step 0.3 empty-DML table becomes a table-driven test on both doors, debug and release profiles,
  `v2-cow`, `v2-mor`, `v3`. Each cell asserts rows, snapshot id unchanged where no write is meant, data
  files unchanged, and on v3 `_row_id` and `_last_updated_sequence_number` unchanged. Red on base, green on head.
- The guard's own before-and-after state check ships here; the general DML-reach generator is Step 4.3.
- The deletes corpus family's special-values row (`docs/corpus/deletes-rule-2026-11-03.md`,
  `docs/corpus/deletes-cells-2026-11-03.jsonl`): the Step 0.3 predicates added as that family's
  special-values row, measured cells only.
- DML-reach rule (2026-11-05): the same predicates measured in DELETE and UPDATE on both doors; INSERT-source
  reach is a control (an empty INSERT source inserting nothing is correct).
- Every assertion, including controls, traces to a measured Spark cell in the table (oracle-control rule).

## 8. Gates

- The full facade tree (`python/repark/tests`) against the head wheel, debug and release; the Rust workspace
  tests for `repark-core`, `repark-sql`, `repark-spark`, `repark-iceberg`; the parity harness; `make ci`.
- Hooks installed and firing in the clone; output to files, tails of 40 lines or fewer.
- CI green before any verifier.

## 9. Records and hand-back

- The builder commits code and tests with its own author line per AGENTS.md (`claude-opus-5-5`) and writes
  `handback-notes.md` (untracked): one line per touched file with purpose, claim and the test that pins it.
- A Haiku clerk then writes the `map.md` rows, the ledger and the registry text on the same branch.
- Hand-back: model id first; the §4 route table; the mechanism and why; red-before/green-after evidence per
  door and profile; the gate list with exit codes; anything outside the clause, carded with its measured cell.
- Session bounds: hand back at 150 turns or 300k context.

## 10. Critique (R7: Opus 5.5 and Muse MAX in parallel on one head, after CI is green)

Verifier standing claims: "the same expression in DML" (2026-11-05); the seat is universal across sessions;
no refused wrapper became a no-op; identity UPDATE unchanged. One delta, one re-check, then park.
