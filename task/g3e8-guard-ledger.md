# Unit ledger — G3-E8 (guard-first half): the DELETE/UPDATE subquery-predicate valve

**Unit:** G3-E8, V2 Engine Hardening campaign · **Date:** 2026-08-11 ·
**Branch:** `hardening/g3e8-dml-subquery-guard` (base `396ffdd` = origin/main) ·
**Defect class:** silent data loss (whole-table DELETE / whole-table UPDATE)

**This unit does NOT fix the defect.** It (a) localizes it with evidence, (b) closes the data-loss
window with a refuse-loud valve in **both** SQL doors, and (c) records the live-Spark oracle now so
the fix unit inherits its goldens. The fix is a separate unit; §7 hands it everything it needs.

| Artifact | Path | Role |
|---|---|---|
| Recon report | `LocalRepark/planning/hardening/G3E8-RECON.md` | the layer, the mechanism, the code cites, the fix-home recommendation |
| Spark-door valve | [`crates/repark-spark/src/normalize.rs`](../crates/repark-spark/src/normalize.rs) | `refuse_dml_subquery_predicate` + `refuse_dml_subquery_predicate_in_statement` + `DmlSubqueryVerb` + the detector |
| Spark-door wiring | [`crates/repark-spark/src/spark_ast.rs`](../crates/repark-spark/src/spark_ast.rs) (**authoritative** — the executing parse) + [`…/router.rs`](../crates/repark-spark/src/router.rs) (the early, order-only call in the `Delete`/`Update` arms) | see D-8 |
| ANSI-door valve | [`crates/repark-sql/src/guards.rs`](../crates/repark-sql/src/guards.rs) | re-implemented (door→door product edges are banned); reads the parsed `Statement` |
| ANSI-door wiring | [`crates/repark-sql/src/router.rs`](../crates/repark-sql/src/router.rs) | the shared `DELETE`/`UPDATE` arm: G3-E8 → BUG-001 → `delegate` |
| Spark-door pins | [`crates/repark-spark/src/tests/dml.rs`](../crates/repark-spark/src/tests/dml.rs) (**10**), [`…/tests/normalize.rs`](../crates/repark-spark/src/tests/normalize.rs) (**2**) | refuse families, adjacent negatives, guard order, the FROM-less bypass family + its negative, CTE-prefixed loud-today, detector, statement valve |
| ANSI-door pins | [`crates/repark-sql/src/guards/tests.rs`](../crates/repark-sql/src/guards/tests.rs) (**6**) | detector, message, rendered target, end-to-end refuse + rows-untouched, valve ORDER vs BUG-001, router-vs-session parse dialect |
| Cross-door pin | [`crates/repark-sql/tests/cross_door.rs`](../crates/repark-sql/tests/cross_door.rs) (**1**) | the two doors' RENDERED refusals must be byte-identical (the D-1 duplication's drift net) |
| Differential corpus | [`python/repark/tests/test_dml_subquery_parity.py`](../python/repark/tests/test_dml_subquery_parity.py) | 10 rows (8 split + 2 controls) + lifecycle helper + classifier |
| Record driver | [`python/repark/tests/_record_dml_subquery_goldens.py`](../python/repark/tests/_record_dml_subquery_goldens.py) | live Spark+Iceberg re-derivation |
| Maps (lockstep) | `crates/repark-spark/src/map.md`, `…/src/tests/map.md`, `crates/repark-sql/src/map.md`, `…/src/guards/map.md`, `python/repark/tests/map.md` | |
| This ledger | `task/g3e8-guard-ledger.md` | linked from [`task/map.md`](map.md) |

---

## 1. Recon summary (full report: `LocalRepark/planning/hardening/G3E8-RECON.md`)

**The predicate is lost above the fork, not inside it.**
`datafusion-54.1.0/src/physical_planner.rs::extract_dml_filters` (L2199) recovers the `WHERE`
clause for `TableProvider::delete_from` / `::update` by walking the **optimized** `Dml.input` plan
for `Filter` / `TableScan.filters` nodes. The optimizer has by then decorrelated every
`IN` / `NOT IN` / `EXISTS` / `NOT EXISTS` / `ANY` / `ALL` / correlated predicate into a
`LeftSemi` / `LeftAnti` / `LeftMark` / `Inner` **join**, which that walk skips — with no channel to
say "there was a predicate I could not represent". It returns an empty (or partial) filter vector;
the fork reads empty as *no `WHERE` clause* and matches **every row**. A **fail-open at the
DataFusion→TableProvider seam.**

The fork is explicitly NOT at fault: `crates/integrations/datafusion/src/physical_plan/delete.rs`
documents that it refuses inexact Iceberg-predicate pushdown here *because it would over-delete*,
and evaluates the exact filter it is handed. The intake's hypothesis (a fail-open
expression→Iceberg-predicate conversion) is **disproven** — `convert_filters_to_predicate` is used
on the scan path only, where inexactness is safe.

**Both doors are affected** (the ANSI door's `_ => delegate` arm rides the identical DataFusion
path). The intake only observed the facade.

**Fix home: RePark, not the fork** — recommended path is to lower subquery DML onto the
RePark-owned `MERGE INTO` executor (proven unaffected, §2 row 36), with an upstream
`extract_dml_filters` completeness check filed opportunistically. Sketch in the recon §5.

---

## 2. Statement-form matrix (executed; `tgt={1a,2b,3c}`, `keys={(2,…)}`)

> **This table is CANONICAL** (panel L2 M5, 2026-08-11). The condensed views in
> `G3E8-ACTOR-REPORT.md` §3 and `G3E8-RECON.md` §3 are re-bucketings OF this table and state their
> mapping; where any of them disagrees, this one is right. Corrections landed in the 2026-08-11
> fix pass are marked **[fix pass]**.

Legend: ❌ silently wrong (GUARDED) · ⚠️ correct today but guarded anyway (see D-3) ·
🟡 already errors loud (not guarded) · ✅ correct and NOT guarded.

**Totals after the fix pass — 44 rows:** ❌ **24** (17 DELETE + 7 UPDATE; the FROM-less family
counts as one row covering four spellings) · ❌-class-but-was-filed-🟡 **2** (L1 N-2) · ⚠️ **6** ·
🟡 **5** · ✅ **7**. Derivation: parse this section's table and count column 4 —
`python3 -c "…split('## 2. Statement-form matrix')[1].split('## 3. Guard decisions')[0]…"`; the run
that produced these numbers is in the fix-pass log set. A row is one *form family*, not one
executed statement, which is why the report's condensed view (§3 there) has different totals and
must state its mapping.

| Form | Pre-guard result | Spark | Class | Guarded? | Evidence |
|---|---|---|---|---|---|
| `DELETE … WHERE id = 2` | `{1,3}` | `{1,3}` | ✅ | no | `g3e8_non_subquery_dml_still_executes` |
| `DELETE … WHERE id IN (1,2)` / `BETWEEN` / `LIKE … OR` | correct | correct | ✅ | no | same |
| `DELETE … WHERE` (absent) | empties | empties | ✅ | no | same (`DELETE FROM t` row) |
| `UPDATE … SET … WHERE id = 2` / `IN (1,3)` | correct | correct | ✅ | no | same |
| `DELETE … id IN (SELECT …)` | **`{}`** | `{1,3}` | ❌ | **yes** | `g3e8_delete_in_subquery_refuses_and_leaves_every_row` |
| `DELETE … id IN (SELECT … FROM <temp view>)` | **`{}`** | `{1,3}` | ❌ | yes | **[fix pass]** now PINNED in `g3e8_delete_subquery_family_all_refuse` (the `FROM src` row); pre-guard `{}` re-executed under the neutered valve, §10.3 |
| **`DELETE <table> WHERE id IN (SELECT …)` (FROM-less; also lower-case / `NOT IN` / `EXISTS`)** | **`{}`** | *(Spark rejects the FROM-less spelling)* | ❌ | **yes [fix pass]** | the panel's live BYPASS (L1 M-1): the router's Databricks parse rejects the form, so it reached DataFusion through the passthrough's own parse. `g3e8_fromless_delete_subquery_family_all_refuse` |
| **`DELETE <table> WHERE id = 2` (FROM-less, no subquery)** | `{1,3}` | *(Spark rejects the spelling)* | ✅ | **no [fix pass]** | must keep executing — `g3e8_fromless_non_subquery_delete_still_executes` |
| `DELETE … id NOT IN (SELECT …)` | **`{}`** | `{2}` | ❌ | yes | `g3e8_delete_subquery_family_all_refuse` |
| `DELETE … NOT (id IN (SELECT …))` | **`{}`** | `{2}` | ❌ | yes | same |
| `DELETE … id = 1 OR id IN (SELECT …)` | **`{}`** | `{3}` | ❌ | yes | same |
| `DELETE … id > 1 AND id IN (SELECT …)` | **`{1}`** (partial over-delete) | `{1,3}` | ❌ | yes | same |
| `DELETE … EXISTS (… correlated)` | **`{}`** | `{1,3}` | ❌ | yes | same |
| `DELETE … NOT EXISTS (… correlated)` | **`{}`** | `{2}` | ❌ | yes | same |
| `DELETE … id IN (SELECT … WHERE k.name = t.name)` (correlated IN) | **`{}`** | `{1,3}` | ❌ | yes | same |
| `DELETE … id > ANY (SELECT …)` | **`{}`** | `{1,2}` | ❌ | yes | same |
| `DELETE … id > ALL (SELECT …)` | **`{}`** | `{1,2}` | ❌ | yes | same |
| `DELETE … id IN (SELECT … FROM (SELECT …) x)` (nested) | **`{}`** | `{1,3}` | ❌ | yes | same |
| `DELETE … id = (SELECT max(k.id) … WHERE k.name = t.name)` (correlated AGGREGATE scalar) | **`{}`** | `{1,3}` | ❌ | yes | same |
| **`DELETE … NOT EXISTS (SELECT 1 FROM keys)` (UNCORRELATED)** | **`{}`** | `{1,2,3}` — `keys` is non-empty, so `NOT EXISTS` is false for every row and Spark deletes **nothing** | ❌ | **yes [fix pass]** | L1 M-4. Pre-guard executed under the neutered valve (§10.3): `rows=[]`. Pin: `g3e8_delete_subquery_family_all_refuse` |
| **`DELETE … EXISTS (SELECT 1 FROM keys WHERE id = 999)` (EMPTY result)** | **`{}`** | `{1,2,3}` — the subquery is empty, so `EXISTS` is false everywhere | ❌ | **yes [fix pass]** | L1 M-4. Pre-guard `rows=[]` (§10.3). Same pin |
| **`DELETE … id IN (SELECT max(id) FROM keys)` (uncorrelated AGGREGATE in IN)** | **`{}`** | `{1,3}` | ❌ | **yes [fix pass]** | L1 M-4. Pre-guard `rows=[]` (§10.3). Same pin |
| `UPDATE … WHERE id IN (SELECT …)` | **all 3 → 'z'** | id=2 only | ❌ | yes | `g3e8_update_subquery_family_all_refuse` |
| **`UPDATE … WHERE NOT EXISTS (SELECT 1 FROM keys)` (uncorrelated)** | **all 3 → 'z'** | nothing changes | ❌ | **yes [fix pass]** | pre-guard `[(1,z),(2,z),(3,z)]` (§10.3). `g3e8_update_subquery_family_all_refuse` |
| **`UPDATE … WHERE id IN (SELECT max(id) FROM keys)`** | **all 3 → 'z'** | id=2 only | ❌ | **yes [fix pass]** | pre-guard `[(1,z),(2,z),(3,z)]` (§10.3). Same pin |
| `UPDATE … WHERE id NOT IN (SELECT …)` | **all 3 → 'z'** | ids 1,3 | ❌ | yes | same |
| `UPDATE … WHERE EXISTS (… correlated)` | **all 3 → 'z'** | id=2 | ❌ | yes | same |
| `UPDATE … WHERE id = (SELECT max(k.id) … correlated)` | **all 3 → 'z'** | id=2 | ❌ | yes | same |
| `UPDATE … SET c=(SELECT …) WHERE id IN (SELECT …)` | **all 3 → 'K'** | id=2 | ❌ | yes | same |
| `DELETE … id = (SELECT max(id) FROM keys)` (uncorrelated scalar) | `{1,3}` | `{1,3}` | ⚠️ | **yes (deliberate over-refusal)** | recon round 2/3; D-3. **[fix pass]** pin: `g3e8_delete_subquery_family_all_refuse` (the `id = (SELECT max(id) …)` row) — L2 N9 asked for the actual pin name, this is it |
| `DELETE … id > (SELECT max(id) …)` / `id <> (SELECT max(id) …)` (uncorrelated scalar) | `{1,2}` / `{2}` — correct | correct | ⚠️ | yes | recon round 3; **[fix pass]** both spellings re-executed under the neutered valve (§10.3) AND pinned in `g3e8_delete_subquery_family_all_refuse` |
| `DELETE … (SELECT count(*) FROM keys) > 0` / `> 99` | `{}` (count=1 > 0 → all match) / unchanged — correct | correct | ⚠️ | yes | recon round 3; **[fix pass]** `> 99` re-executed (unchanged, §10.3); both pinned in `g3e8_delete_subquery_family_all_refuse` |
| `UPDATE … WHERE id = (SELECT max(id) …)` (uncorrelated scalar) | correct | correct | ⚠️ | yes | recon round 2; pinned in `g3e8_update_subquery_family_all_refuse` |
| `UPDATE … SET c = (SELECT max(name) …)` **with a subquery-free `WHERE`** | correct (only the matched row) | *(Spark rejects the surface)* | ⚠️ | **no** | `g3e8_update_set_subquery_without_where_subquery_still_executes`; D-4 |
| `UPDATE … SET c = (SELECT max(name) …)` **with NO `WHERE` at all** | all 3 → `'K'` — correct (genuine match-all) | *(Spark rejects the surface)* | ⚠️ | **no** | **[fix pass]** the row was previously folded into the one above as "± WHERE" and had no pin of its own; now the second half of `g3e8_update_set_subquery_without_where_subquery_still_executes` |
| `UPDATE … SET c = (SELECT k.name … WHERE k.id=t.id)` (correlated) | loud `Invalid (non-executable) plan after Analyzer` | — | 🟡 | no | recon round 5; **[fix pass]** re-executed §10.4 (the message text differs from the recon's transcript — recorded as observed, not as remembered) |
| `UPDATE … SET c = (SELECT max(k.name) … WHERE k.id=t.id)` | loud `UPDATE operation on table 'ice.sales.tgt'` (a DataFusion planner refusal) | — | 🟡 | no | recon round 5; **[fix pass]** re-executed §10.4 |
| `DELETE … WHERE name = (SELECT k.name … WHERE k.id=t.id)` | **G3-E8 refusal** (the valve fires before the loud plan error) | — | ❌-class, valved | **yes [fix pass]** | L1 N-2: its subquery is in the `WHERE`, so the valve reaches it first — re-classified from 🟡 with executed evidence (§10.4). It was never "already loud" from the user's seat once the valve landed |
| `DELETE FROM t AS x WHERE x.id IN (SELECT …)` | **G3-E8 refusal** (was: loud `No field named x.id`) | — | ❌-class, valved | **yes [fix pass]** | L1 N-2, same reason, same evidence (§10.4). DataFusion still has no aliased DELETE target — the user simply never reaches that error now |
| `UPDATE … SET … FROM k WHERE …` | loud `UPDATE … FROM is not supported` | — | 🟡 | no | recon round 6 |
| `DELETE … USING k WHERE …` | loud `Using clause not supported` | — | 🟡 | no | recon round 6 |
| `INSERT INTO t SELECT … WHERE id IN (SELECT …)` | correct | correct | ✅ | **no** | `g3e8_insert_and_merge_with_subqueries_still_execute` |
| `MERGE INTO t USING (SELECT …) s ON … WHEN MATCHED THEN DELETE` | correct | correct | ✅ | **no** | same |
| **`WITH c AS (…) DELETE FROM t WHERE …`** (CTE-prefixed DML, subquery or not) | loud `NotImplemented: Query DELETE … not implemented yet`; nothing written | — | 🟡 | **no — un-valved by construction** | **[fix pass]** L1 N-1. sqlparser models this as a `Query` with a `SetExpr::Delete` body, so it reaches NEITHER door's `Delete` arm and the valve never sees it. Safe only because DataFusion refuses to plan the shape. Pinned LOUD-TODAY by `g3e8_cte_prefixed_dml_is_loud_today_and_writes_nothing` so the day DataFusion learns the shape, the pin reds instead of a second silent window opening |

**ANSI door**: the same probes reproduce identically (empty table / all-rows-updated) —
`crates/repark-sql/src/guards/tests.rs::dml_subquery_valve_refuses_end_to_end_and_writes_nothing`
is the post-guard pin (six statements: `IN`, uncorrelated `NOT EXISTS`, correlated `NOT EXISTS`,
`UPDATE … IN`, the FROM-less `IN`, and `IN (SELECT max(id) …)`, plus the non-subquery negative);
the pre-guard behaviour is in the recon report §3. **[fix pass]** the ANSI door has never had the
Spark door's bypass — it parses with the same dialect it delegates with, and
`router_parse_dialect_matches_the_session_default` is now the pin that keeps that true.

---

## 3. Guard decisions, with rationale

**D-1 — Both doors, re-implemented, not shared. REWRITTEN in the 2026-08-11 fix pass (L1 M-3).**
The defect is in a DataFusion path both doors delegate to. Guarding only the facade would leave
`repark.sql()` emptying tables.

The original text presented a two-way choice — share the code, or duplicate it — and called the
duplication forced. That dichotomy was **false**, and saying so is the correction: `repark-sql` may
not take a *product* edge to `repark-spark` (`scripts/check_crate_dag.py` — the row is DEV-ONLY and
says so: *"A `normal` edge here is the forbidden door → door product edge"*), but a third option
existed and had a precedent in this very repo: **hoist the detector + the message into a foundation
crate both doors already reach**, which is exactly what BUG-001 did
(`repark_iceberg::write::refuse_mor_unpartitioned_multi_spec_dml`, with a thin per-door resolution
wrapper).

Why the hoist did NOT land in this fix pass, stated as a cost rather than a principle:
`repark-common` (the only tier-0 crate) depends on `thiserror` and nothing else, and the detector
needs `sqlparser`'s `Expr` + `Visitor`. Hoisting therefore means putting a DataFusion/sqlparser
dependency into the foundation crate AND promoting the `repark-spark → repark-common` edge from
`dev` to `normal` in the DAG SSOT — an architecture change with its own review, inside a
guard-first unit whose subject is a data-loss window. **Named as the follow-up, not as impossible.**

What DID land instead is the mitigation the duplication actually needed: a cross-door pin that
compares the two doors' **rendered** refusal strings for the same statement
(`crates/repark-sql/tests/cross_door.rs::cross_door_g3e8_refusals_render_identically`, on the
dev-only test edge the DAG table exists to permit). Template identity was already pinned per door;
rendered identity — including the target — was not, and that is precisely where the two copies had
already drifted (L1 M-3(1): the ANSI copy read its target from scrubbed text and rendered a quoted
target as blanks; fixed in the same pass).

Cost, restated honestly: one duplicated message, one pin that reds the moment the copies drift, and
a named follow-up to remove the duplication.

One more sentence of the original is retracted (L1 N-4): it said *"the corpus needle asserts
through either door"*. It does not. The differential corpus
(`python/repark/tests/test_dml_subquery_parity.py`) drives the **facade**, i.e. the Spark door,
only — no Python row touches `repark-sql`. The ANSI door's needle is asserted in Rust:
`guards/tests.rs::dml_subquery_valve_refuses_end_to_end_and_writes_nothing` for the message and the
rows-untouched claim, and the cross-door pin for the two doors' equality.

**D-2 — Detect "a `Query` node under the predicate", not an enumeration of `Expr` variants.**
An enumeration (`InSubquery` | `Exists` | `Subquery` | `AnyOp` | `AllOp`) is a list that goes stale
on a sqlparser bump — and this is a data-loss valve, so staleness means silent loss. The
sqlparser `Visitor::pre_visit_query` hook fires for any `Query` reachable from the expression, and
the ONLY way a `Query` is reachable from a `WHERE` expression is a subquery. Fail-closed by
construction. Pinned against 14 spellings including `CASE`-buried and function-argument positions
(`g3e8_subquery_detector_fires_on_every_spelling_and_no_other`).

**D-3 — The uncorrelated scalar subquery is over-refused ON PURPOSE. FLAGGED.
BOUNDARY REWRITTEN in the 2026-08-11 fix pass (L1 M-4).**
`DELETE … WHERE id = (SELECT max(id) FROM keys)` is **correct today** and this unit refuses it
anyway. The true reason: its *correlated aggregate* twin —
`DELETE … WHERE id = (SELECT max(k.id) FROM keys k WHERE k.name = t.name)` — is the **same parse
tree** and **empties the table**. Correlation is a semantic property requiring full name resolution
against the target's schema and every enclosing scope; deciding it from the parse tree is precisely
the class of heuristic that produced the BUG-001 alias under-refuse. For a silent-data-loss defect
the asymmetry is decisive: over-refusing costs a user one `MERGE INTO` rewrite; under-refusing
costs a table.

> **The correction the panel forced, and it matters more than the decision above.**
> The earlier text framed the safe/unsafe line as **correlated vs uncorrelated**. That framing is
> WRONG, and a fix unit that inherited it would inherit a landmine. Executed counter-examples, all
> **uncorrelated** and all of which **emptied the table** pre-guard (§10.3):
>
> | Uncorrelated spelling | pre-guard | Spark |
> |---|---|---|
> | `NOT EXISTS (SELECT 1 FROM keys)` | `{}` | deletes nothing |
> | `EXISTS (SELECT 1 FROM keys WHERE id = 999)` | `{}` | deletes nothing |
> | `id IN (SELECT max(id) FROM keys)` | `{}` | `{1,3}` |
> | `id IN (SELECT id FROM keys)` | `{}` | `{1,3}` |
>
> …while other uncorrelated spellings (`id = (SELECT max(id) …)`, `id > / <> (SELECT max(id) …)`,
> `(SELECT count(*) …) > n`) are correct today. **The line is per-SHAPE**: what matters is whether
> the optimizer decorrelates the predicate into a join (lost) or leaves it as a `Filter` over a
> scalar (survives) — and "uncorrelated" predicts neither. Every ⚠️ row in §2 is a per-spelling
> claim with its own executed result, and re-enabling ANY of them requires re-executing THAT
> spelling. See §7 item 4.

The over-refused spellings are enumerated in the matrix above (⚠️ rows) and are the first thing the
fix unit should re-enable — **one at a time, each against live behavior.**

**D-4 — `UPDATE … SET col = (SELECT …)` assignments are NOT guarded.** Assignment subqueries are
never *silently* wrong: uncorrelated is correct (verified with a distinguishing value — row 2's
name became `'K'`), and both correlated forms fail loud at plan time. Guarding them would be pure
over-refusal of working surface, which the brief forbids. Pinned by
`g3e8_update_set_subquery_without_where_subquery_still_executes` — if a later change starts gating
assignments, that pin reds and the decision is re-made rather than drifted.
*Separate observation, out of scope:* Spark **rejects** subqueries in `UPDATE … SET` outright, so
repark is more permissive here. That is a parity divergence, not data loss; it is recorded in the
recon report and is NOT a registry row from this unit.

**D-5 — Guard order: P11 → G3-E8 → BUG-001, on BOTH doors. SCOPE CORRECTED + the ANSI door
REORDERED in the 2026-08-11 fix pass (L1 M-2).** The read-only-catalog refusal is more fundamental
("you cannot write here at all") and stays first. The G3-E8 valve is a pure sync AST walk; the
BUG-001 valve is `async` and loads the target's Iceberg metadata (a network round-trip on Glue /
S3 Tables). Cheap-before-expensive, and both are data-loss valves so either message is honest.

**What was wrong.** As delivered, that rationale was true of the SPARK door only. The ANSI door ran
its BUG-001 valve at the **router head, before the parse** the G3-E8 valve needs — so on that door
the order was BUG-001 → G3-E8, and a statement tripping both got the *other* message. One stated
rationale, two behaviours.

**What the fix pass did.** The preferred remedy in the disposition — make the code match the claim —
was proportionate, so it landed: `guards::refuse_mor_multi_spec_dml` moved out of the ANSI router
head into the shared `Statement::Delete | Statement::Update` arm, immediately after the G3-E8 call.
Consequences, stated rather than glossed:
- the multi-statement refuse still runs FIRST (it is a text guard in `run_text_guards`), so the
  BUG-010 ordering rule is untouched;
- the BUG-001 valve now sees the post-rewrite SQL and only fires for statements that PARSE as
  `DELETE`/`UPDATE`. A DML statement that fails to parse now returns the parse error instead of the
  MoR refusal — which is the more informative error, and the MoR valve's own wrapper pins
  (`mor_valve_wrapper_passes_what_it_cannot_or_must_not_gate`) and its end-to-end pin
  (`tests::mor_unpartitioned_multi_spec_dml_refuses`) are both unaffected (re-run green, §10.5).

Pinned on both doors: `tests::dml::g3e8_subquery_valve_precedes_the_mor_multi_spec_valve` (Spark)
and `guards::tests::mor_valve_runs_after_the_g3e8_valve` (ANSI). Each builds a table that trips
BOTH and asserts the G3-E8 message wins — with a control proving the BUG-001 valve still fires on
the non-subquery spelling (so neither pin can pass by the hazard not existing).

**D-8 — The valve attaches at the EXECUTING parse (new, 2026-08-11; L1 M-1 — the live BYPASS).**
The Spark door has two parses: the router's `DatabricksDialect` parse
(`parse_single_normalized`) and the session-dialect parse inside
`spark_ast::execute_passthrough`, which is the one that gets PLANNED. Any form the first rejects
falls through `execute_unparsable_fallthrough` into the second — Spark's FROM-less
`DELETE <table> WHERE …` is the live example, and it emptied the table with the router valve in
place (repro transcript: §10.2). The valve's authoritative call is therefore
`refuse_dml_subquery_predicate_in_statement` inside `execute_passthrough`.

**The router arms keep an early call. That is a deliberate deviation from the disposition's
"single-home preferred", and the reason is ORDER, not safety.** The passthrough attachment sits
*downstream* of the BUG-001 valve in `execute_delete`/`execute_update`; a single home there would
have flipped D-5's order on the Spark door — i.e. it would have fixed the bypass by breaking the
claim F-B was simultaneously making true, and would have spent an Iceberg metadata round-trip
before every G3-E8 refusal. Both call sites invoke ONE implementation (the detector and the message
have a single home in `normalize.rs`), so the duplication is of the CALL, not of the rule. Deleting
the router call would not reopen the hole; deleting the passthrough call would.

**D-6 — The refusal message content.** Names the defect class (`subquery predicates are silently
mis-executed today`), the mechanism in one clause, the consequence (`delete/update EVERY row`), the
defect id (`G3-E8`), the workaround (`MERGE INTO … WHEN MATCHED THEN DELETE / UPDATE SET …`, the
dbt adapter's proven vehicle), and that support returns with the fix. The needle
`subquery predicates are silently mis-executed` is what the corpus and both doors' pins assert —
never a bare "it raised something".

**D-7 — ANSI pins live in `guards/tests.rs`, not `tests.rs`.** `crates/repark-sql/src/tests.rs` is
at 1556/1600 against `scripts/check_rust_file_size.py`; adding the pins there would have forced a
ceiling **raise**, which that table only allows downward without a stated reason. `guards/tests.rs`
is the guard's own home per `crates/repark-sql/src/map.md` ("Add a guard → `guards.rs` +
`guards/tests.rs`") and already has the memory-catalog harness, so the end-to-end row lives there
too. No ceiling was touched.

---

## 4. Gate output (verbatim tails; exits captured as `cmd > log 2>&1; echo $?`)

> **These are the ACTOR's tails (2026-08-11 morning).** The fix pass re-ran both gates against the
> corrected tree — **§10.8 is the current gate output**, and it is where the corrected test counts
> live. §4c's "1331 tests" was wrong in both directions (the pre-unit baseline is **1321**, the
> post-unit total was **1332** before the fix pass and is **1340** now); the derivations are in
> §10.8. §4d/§4e are live-Spark runs that the fix pass did NOT repeat — see deviation V-11.

### 4a. `make verify`

```
$ make verify        # = make ci + make test
cargo fmt --check
cargo clippy --locked --workspace --all-targets -- -D warnings -A clippy::disallowed_methods
cargo clippy --locked --workspace --lib --bins --exclude repark-python -- -D clippy::disallowed_methods …
cargo clippy --locked -p repark-python --lib -- -D clippy::unwrap_used -D clippy::expect_used …
crate-dag: 20 internal edges clean (4 dev, 15 normal, 1 optional) across 9 of 9 mapped crates
lib-rs: 9 crate roots clean (no inline test modules; ceilings held)
rust-file-size: 181 files clean (default ceiling 1500; 13 exceptions)
lib-py: 54 files clean (ceilings held; no-stub rule held)
manifest: 12 components (9 delivered, 3 planned) agree with the workspace, the gates, the doc
          index, the status document and the crate maps
parity-live dual-wire: OK (maturin@1.14.1, extras=[ml-ext, numpy, pandas, polars, record],
          uv-run=[--locked, --no-sync])
cargo check --locked --workspace                       Finished
uvx ruff@0.15.22 check .                               All checks passed!
uvx ruff@0.15.22 format --check .                      249 files already formatted
uv lock --locked                                       Resolved 29 packages
uvx taplo@0.9.3 format --check / lint                  OK
uvx typos@1.47.2                                       OK
cargo test --locked --workspace
    33 x "test result: ok"  —  1332 tests passed, 0 failed, 0 ignored
EXIT=0
```

### 4b. `make py-test-facade`

```
$ make py-test-facade      # re-run AFTER the final router refactor; rebuilds the native module
uv sync --locked --extra numpy --extra pandas --extra polars --extra ml-ext --no-install-package repark
cd python/repark && … maturin@1.14.1 develop        Installed repark-0.0.0
… pytest python/repark/tests -q
........................................................................ [100%]
2648 passed, 46 skipped, 37 warnings in 96.05s (0:01:36)
EXIT=0
```

A follow-up `make ci` (covering the two markdown files added after `make verify` started) also
returned **EXIT=0**.

### 4c. `cargo test --workspace` (the landmine sweep, pre-`verify`)

```
33 × "test result: ok" — 1331 tests passed, 0 failed
EXIT=0
```

### 4d. Record mode (live Spark oracle)

```
$ JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
    PYTHONPATH=python/repark-parity/src \
    .venv/bin/python python/repark/tests/_record_dml_subquery_goldens.py
record warehouse = /tmp/repark-dml-subquery-record-…
Iceberg GAV      = org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0
[G3-E8] control_delete_without_subquery [content] PASS
[G3-E8] control_update_without_subquery [content] PASS
[G3-E8] delete_in_subquery [split] PASS
[G3-E8] delete_not_in_subquery [split] PASS
[G3-E8] delete_not_in_subquery_with_null_key [split] PASS
[G3-E8] delete_exists_correlated [split] PASS
[G3-E8] delete_not_exists_correlated [split] PASS
[G3-E8] delete_correlated_in_subquery [split] PASS
[G3-E8] update_in_subquery [split] PASS
[G3-E8] update_not_in_subquery_with_null_key [split] PASS
[G3-E8] lifecycle cleanup PASS (tables=[])

record mode: 10 rows re-derived, 0 mismatch(es)
EXIT=0
```

### 4e. Live oracle values, emitted verbatim

The driver above *verifies*; to make the "recorded" claim falsifiable from the oracle's own mouth,
a scratch emitter (never committed) printed each row's live Spark half. Every value below equals
the committed golden:

```
[LIVE] control_delete_without_subquery       schema=[('id','int64',True),('name','string',True)] rows={'id': [1, 3],       'name': ['a', 'c']}
[LIVE] control_update_without_subquery       …                                                   rows={'id': [1, 2, 3],    'name': ['a', 'z', 'c']}
[LIVE] delete_in_subquery                    …                                                   rows={'id': [1, 3],       'name': ['a', 'c']}
[LIVE] delete_not_in_subquery                …                                                   rows={'id': [2],          'name': ['b']}
[LIVE] delete_not_in_subquery_with_null_key  …                                                   rows={'id': [1, 2, 3],    'name': ['a', 'b', 'c']}
[LIVE] delete_exists_correlated              …                                                   rows={'id': [1, 3],       'name': ['a', 'c']}
[LIVE] delete_not_exists_correlated          …                                                   rows={'id': [2],          'name': ['b']}
[LIVE] delete_correlated_in_subquery         …                                                   rows={'id': [1, 3],       'name': ['a', 'c']}
[LIVE] update_in_subquery                    …                                                   rows={'id': [1, 2, 3],    'name': ['a', 'z', 'c']}
[LIVE] update_not_in_subquery_with_null_key  …                                                   rows={'id': [1, 2, 3],    'name': ['a', 'b', 'c']}
```

**The NULL trap, as live Spark resolved it:** with a NULL in the subquery result, `NOT IN` is
UNKNOWN for every row, so Spark matched **nothing** — both `*_with_null_key` rows come back with
all three rows intact. Recorded, not reasoned. `NOT EXISTS` over the same data is NULL-safe and
deletes two rows; the corpus pins the contrast.

**JVM coordination protocol.** Before every record run: `pgrep -af 'pyspark|SparkSubmit'`, ignoring
the standing containerized cluster on this host (`deploy.master`, `deploy.worker`, `HistoryServer`,
`HiveThriftServer2`, `CoarseGrainedExecutorBackend`, `spark-daemon` — long-lived infrastructure,
not a lane's record driver). Both record runs and the emitter ran with the lane **clear**; no other
local driver was observed. The protocol is also written into the record driver's docstring so the
next lane does not have to rediscover which processes matter.

---

## 5. Provocation transcripts (docs/testing.md "Gate provocation proofs")

> **§5a / §5b / §5d were RE-RUN on 2026-08-11** (panel L2 M1/M2: the originals merged per-binary
> result lines, carried a stale ANSI test-name transcript, and double-counted the clean re-run as
> "8 + 3 + 28"). The replacements — real commands, real per-binary output, real exit codes — are in
> **§10.5**, against the shipped post-fix code. They are not reproduced twice; this section is the
> index.
>
> | Provocation | Where it lives now | Re-run? |
> |---|---|---|
> | 5a — neuter the valve, everything reds | §10.5 (a) - (d) | YES, 2026-08-11, both doors + cross-door + facade corpus |
> | 5b — the classifier is reachable in both arms | §10.5 (e) | YES, 2026-08-11 |
> | 5c — golden perturbation (live Spark) | below, unchanged | **NO** — needs the JVM oracle; no golden or recipe changed in the fix pass (deviation V-11) |
> | 5d — clean re-runs after restore | §10.5 (f), with the sha-verified restore in §10.6 | YES, 2026-08-11 |

### 5c. Golden perturbation — the recorded half is really the oracle's

*(ACTOR transcript, 2026-08-11 morning; NOT re-run in the fix pass — see V-11.)*

`delete_not_in_subquery_with_null_key`'s golden was changed to the *intuitive* (wrong) answer
`{2}`, and the record driver reported live Spark's actual answer:

```
[G3-E8] delete_not_in_subquery_with_null_key [split] MISMATCH
    live schema     = [('id', 'int64', True), ('name', 'string', True)]
    recorded schema = [('id', 'int64', True), ('name', 'string', True)]
    live rows       = {'id': [1, 2, 3], 'name': ['a', 'b', 'c']}
    recorded rows   = {'id': [2], 'name': ['b']}
record mode: 10 rows re-derived, 1 mismatch(es)
EXIT=1
```

Reverted; clean re-run `record mode: 10 rows re-derived, 0 mismatch(es)` / `EXIT=0`.

## 6. Registry rows — READY TO PASTE, **not** landed

**Do not edit `docs/spark-sql-iceberg-parity.md` from this unit.** The orchestrator lands these
after the PR merges (pins must resolve on `main` first). Intent is **BACKLOG-to-FIX**: these
document a refused surface with a live defect behind it, not a settled divergence.

---

- **repark** — `DELETE FROM t WHERE <predicate containing a subquery>` and
  `UPDATE t SET … WHERE <predicate containing a subquery>` are **refused** with a `Plan` error
  naming defect G3-E8, on both SQL doors. Every subquery spelling is refused: `IN`, `NOT IN`,
  `EXISTS`, `NOT EXISTS`, `ANY`, `ALL`, correlated forms, nested subqueries, and scalar `(SELECT …)`
  — including the uncorrelated scalar spelling, which executes correctly today and is refused
  deliberately (its correlated twin is syntactically identical and empties the table), and
  including Spark's FROM-less `DELETE <table> WHERE …` spelling. Subqueries in an `UPDATE … SET`
  assignment, in `INSERT … SELECT`, and in a `MERGE INTO … USING (…)` source are unaffected. Not
  covered: CTE-prefixed `WITH … DELETE/UPDATE`, which this engine cannot plan at all today (loud
  `NotImplemented`, writes nothing).
- **Apache Spark** — runs all of them, deleting/updating exactly the matching rows.
  *(oracle: recorded — PySpark 4.1.2 + `iceberg-spark-runtime-4.1_2.13:1.11.0`, zulu-17,
  `local[2]`, ANSI on, `spark.sql.shuffle.partitions=2`, 2026-08-11.)*
- **Pin** — `python/repark/tests/test_dml_subquery_parity.py::test_dml_subquery_row[delete_in_subquery]`,
  `…[delete_not_in_subquery]`, `…[delete_exists_correlated]`, `…[delete_not_exists_correlated]`,
  `…[delete_correlated_in_subquery]`, `…[update_in_subquery]`;
  `crates/repark-spark/src/tests/dml.rs::g3e8_delete_subquery_family_all_refuse`,
  `crates/repark-spark/src/tests/dml.rs::g3e8_update_subquery_family_all_refuse`,
  `crates/repark-spark/src/tests/dml.rs::g3e8_fromless_delete_subquery_family_all_refuse`,
  `crates/repark-sql/src/guards/tests.rs::dml_subquery_valve_refuses_end_to_end_and_writes_nothing`,
  `crates/repark-sql/tests/cross_door.rs::cross_door_g3e8_refusals_render_identically`.
- **Rationale** — DEFECT, refused pending fix (not a settled absence). Until G3-E8's fix unit lands,
  executing these silently deleted or rewrote **every row** of the target
  (`extract_dml_filters` recovers no filter from a decorrelated semi-join and the fork reads an
  empty filter list as "no WHERE clause"). The valve converts silent data loss into a loud refusal
  with a `MERGE INTO` workaround. **Delete this row when the fix lands** — it is a temporary
  disclosure, and the corpus rows flip from `split` to `content` in the same change.

---

- **repark** — `DELETE`/`UPDATE` with `NOT IN (SELECT …)` where the subquery result contains
  `NULL`: refused (same G3-E8 valve).
- **Apache Spark** — SQL three-valued logic: `x NOT IN (…, NULL)` is UNKNOWN for **every** row, so
  Spark matches nothing and the table is unchanged. `NOT EXISTS` over the same data is NULL-safe
  and does match. *(oracle: recorded, same environment.)*
- **Pin** — `python/repark/tests/test_dml_subquery_parity.py::test_dml_subquery_row[delete_not_in_subquery_with_null_key]`,
  `…[update_not_in_subquery_with_null_key]`,
  `python/repark/tests/test_dml_subquery_parity.py::test_dml_subquery_row_set_covers_the_g3e8_budget`
  (the name-gated NULL-trap coverage pin).
- **Rationale** — the fix unit's most likely silent-wrong-answer trap. Recorded now so "we
  implemented subquery DELETE" cannot ship with the intuitive-but-wrong semantics. Keep this row
  after the fix lands (flip the repark half to "matches Spark"), because the behaviour is
  surprising enough to be re-broken.

---

## 7. What the fix unit inherits

1. **The layer + mechanism**, with code cites: `G3E8-RECON.md` §2, and the recommendation to lower
   subquery DML onto the RePark-owned MERGE executor rather than fight `extract_dml_filters`.
2. **Goldens already recorded** for 8 spellings (§4d/§4e). When the fix lands, each split row flips
   to `kind="content"`, `repark_error_needle=None`; the CONVERGED classifier prints that exact
   instruction, and §5b proves it fires.
3. **A regression net**: removing the valve without the fix reds **13 Rust pins** across both doors
   and the cross-door binary, plus **10 Python rows** — re-measured in the 2026-08-11 fix pass with
   per-binary transcripts (§10.5), superseding the earlier "5 Rust pins" figure.
4. **The over-refused list** (matrix ⚠️ rows) — the first spellings to re-enable, each with a
   recorded correct-today result to check against. **Rewritten 2026-08-11 (L1 M-4):** nothing here
   is re-enabled by inheritance. `correlated` is NOT the axis — §2 and D-3 list uncorrelated
   spellings that empty the table and uncorrelated spellings that are correct, side by side. To
   re-enable a spelling: (a) execute THAT spelling against live behavior with the valve off, (b)
   compare against the live-Spark answer, (c) land a pin for it, (d) only then narrow the valve.
   A per-shape claim may not be generalized into a rule; the fix unit inherits a MAP of executed
   results, not a licence.
5. **The unguarded-but-adjacent surfaces** (`INSERT … SELECT`, `MERGE`, `UPDATE … SET (SELECT …)`
   with and without a `WHERE`, non-subquery DML incl. the FROM-less spelling) with passing pins,
   so the fix can be checked for not regressing them.
6. **The attachment rule** (D-8): a DML guard belongs at the parse the executor plans. The two
   known un-valved attachments are named and pinned rather than left to be rediscovered — the
   FROM-less family (now valved) and CTE-prefixed `WITH … DELETE` (loud today, pinned loud).

---

## 8. Deviations — FLAGGED with true reasons

| # | Deviation | True reason |
|---|---|---|
| V-1 | The guard **over-refuses** uncorrelated scalar subqueries in a DELETE/UPDATE `WHERE`, which work correctly today. The brief says "do not over-refuse forms that work". | The same parse tree, once correlated, empties the table (matrix row 15, evidence-backed). Correlated-vs-not is not decidable from the parse tree without full name resolution. For a data-loss defect the fail-safe side is the refusing side. Every over-refused spelling is enumerated (matrix ⚠️ rows) so the fix unit can re-enable them precisely. Recorded as D-3. |
| V-2 | The brief scoped the guard to "the router's Delete AND Update arms"; the ANSI door (`repark-sql`) was also guarded. | The brief's own clause "and any other DML passthrough arm recon shows affected" — recon proved the ANSI door's `_ => delegate` arm empties tables identically. A facade-only guard would have left `repark.sql()` losing data. Recorded as D-1. |
| V-3 | The brief placed the ANSI/second-door pins nowhere specific; the ANSI pins landed in `crates/repark-sql/src/guards/tests.rs`, not the door's e2e `tests.rs`. | `tests.rs` is at 1556/1600 against `scripts/check_rust_file_size.py`; adding there would have forced a **ceiling raise**, which that SSOT ratchets downward and permits upward only **with a stated reason in the commit that raises it** (its own words — a convention, not a mechanical check, which is precisely why it should not be spent on test placement). `guards/tests.rs` is the guard's declared home in `crates/repark-sql/src/map.md`. Recorded as D-7. **[fix pass, L2 N2]** the qualifier was missing from this row; the file is still at 1556 (`rust-file-size: 181 files clean` in §10.8) and no ceiling was touched by the fix pass either — the six ANSI pins all live in `guards/tests.rs`. |
| V-4 | The brief asked for a subquery walk over "predicate/assignment expressions"; assignments are **not** walked. | Assignment subqueries are never silently wrong (correct, or a loud plan error — matrix rows 27-30). Guarding them would over-refuse working surface for no data-loss benefit. Pinned so the decision is re-made, not drifted. Recorded as D-4. |
| V-5 | The goldens were **authored from the mechanism and then verified** against live Spark by the committed driver, rather than emitted by a first run. | Honest provenance note. Discharged in two ways: the driver's byte-for-byte verification (schema name/type/nullability then values) would have failed on any wrong value and printed the live one, and §4e records the oracle's own emitted values for all 10 rows from a separate live run. §5c perturbs a golden and shows the driver reporting live Spark's answer. Nothing in the corpus is unverified against the live oracle. |
| V-6 | The corpus is 10 rows, of which **8** are SPLIT (the brief said "6–10 SPLIT rows"). | In range; the extra 2 rows are equality **controls**, added per the corpus-lane budget rule ("an all-disclosure corpus cannot tell agreement from a broken comparator"). Budget pin encodes both bounds. |
| V-7 | Router arms extracted into `execute_delete` / `execute_update` rather than edited in place. | **RENUMBERED 2026-08-11 (L2 M3).** This row previously carried the no-registry/no-commit deviation here while the actor report used V-7 for the router extraction — one id, two meanings. The report's meaning wins (it is the one a downstream reader cites). Reason: adding the valve pushed `execute_inner` to 104 lines and clippy `too_many_lines` (limit 100) went red. The router module already uses this extraction pattern for the same reason, and the extraction carried the valve-order rationale into a banner doc rather than a comment buried in a match arm. |
| V-8 | No registry edit; no commit; no push. | **Was V-7 before the 2026-08-11 renumber.** Per the brief's hard rules and the corpus-lane §4 rule (pins must resolve on `main` first). §6 carries the paste-ready rows. |
| V-9 | The G3-E8 valve is CALLED from two places on the Spark door (the router arms and the passthrough), where the disposition preferred a single home. | 2026-08-11 fix pass. One implementation, two call sites: the passthrough call is the load-bearing one (it is the executing parse), the router call exists so the cheap sync valve keeps winning the D-5 order against the async BUG-001 valve. A single home in the passthrough would have flipped that order on the Spark door in the same pass that F-B was making the order true on the ANSI door. Stated in D-8. |
| V-10 | The detector + message were NOT hoisted to a foundation crate (F-C(3) allowed the hoist "if the diff stays proportionate"). | 2026-08-11 fix pass. `repark-common` depends on `thiserror` alone; the detector needs sqlparser's `Expr`/`Visitor`. The hoist means a DataFusion dependency in tier 0 plus promoting `repark-spark → repark-common` from `dev` to `normal` in `scripts/check_crate_dag.py` — an architecture change, not a guard fix. The disposition's minimum landed instead (the cross-door rendered-equality pin), and D-1 now names the hoist as the follow-up with this cost stated. |
| V-11 | The live-Spark provocations (§5c golden perturbation, §4d record-mode run, §4e live emitter) were NOT re-run in the fix pass. | 2026-08-11. The dispositions asked for the neuter + clean-run provocations to be re-captured; the live-oracle ones were not named, and nothing this pass touched can change them: the fix pass edited exactly three things in the corpus module — an `import re`, the control-row subquery check (F-G), and the GAV test's docstring (F-Q) — and touched no `spark=` golden, no `dml_sql`, and no `read_sql`. Said here rather than implied by silence: §4d/§4e/§5c are the ACTOR's transcripts, unchanged, and are labelled as such. |

---

## 9. Known limitations (not defects of this unit)

- The valve is **syntactic**. A subquery hidden behind a view, a UDF, or a prepared-statement
  parameter is not visible in the `WHERE` parse tree; none of those surfaces exists in this engine
  today, but the fix unit should not assume the valve is a semantic guarantee.
- The valve is **statement-shaped**: it fires on `Statement::Delete` / `Statement::Update` at the
  executing parse. DML that sqlparser models as something else does not reach it — today that is
  CTE-prefixed `WITH … DELETE/UPDATE` (a `Query` with a `SetExpr::Delete` body), which DataFusion
  refuses to plan at all. Loud, writes nothing, and pinned as loud so the gap cannot reopen
  quietly (§2 last row; added 2026-08-11).
- The ANSI door's router parse (`PARSER_DIALECT`) and the parse `delegate` plans
  (`create_logical_plan`, which reads the session's `sql_parser.dialect`) are two parses that agree
  today. `router_parse_dialect_matches_the_session_default` is the pin that keeps that a checked
  fact; if a DataFusion bump ever separates them, every guard in that arm inherits the Spark
  door's bypass class until they are re-attached (added 2026-08-11).
- Partitioned targets, merge-on-read mode, branch/tag targets and Glue/S3 Tables catalogs were not
  exercised. The predicate is lost **above** the provider, before mode selection, so there is no
  reason to expect a different verdict — but it is untested, and said so here rather than implied.
- The `ANY`/`ALL` loss route (a `Filter` over mark columns dropped by
  `predicate_is_on_target_multi`'s qualifier check, rather than a join with no filter at all) is
  **inferred from the DataFusion source, not instrumented**; the *outcome* (table emptied) is
  executed evidence. Nothing in this unit observed which of the two routes ran (L2 N7).

---

## 10. Panel fix pass (2026-08-11)

A two-lens adversarial panel reviewed the delivery above. **Lens 1** (code) found one live guard
**BYPASS** plus three more MAJORs; **lens 2** (record) found the transcripts and counts were not
trustworthy. The orchestrator accepted everything and dispatched a fix pass (F-A … F-X). This
section is that pass: what changed, what it was verified with, and where it deviates.

Fixer: Claude (Opus 5), same worktree, same branch, uncommitted. Nothing committed or pushed;
`Cargo.lock`, `uv.lock`, `.github/` and the fork are untouched.

### 10.1 Finding → action

| Lens id | Finding | Action | Where it landed |
|---|---|---|---|
| **L1 M-1** (F-A) | **Live BYPASS.** `DELETE ice.sales.tgt WHERE id IN (SELECT …)` (FROM-less) fails the router's `DatabricksDialect` parse → `execute_unparsable_fallthrough` → `spark_ast::execute_passthrough` re-parses under the session dialect and **emptied the table**. The valve was attached to a parse the executor does not use. | Valve now runs at the EXECUTING parse: `normalize::refuse_dml_subquery_predicate_in_statement`, called from `execute_passthrough`. Router arms keep an EARLY call for valve order only (V-9). Five spellings pinned. | `crates/repark-spark/src/{normalize,spark_ast,router,lib}.rs`; pins `g3e8_fromless_delete_subquery_family_all_refuse`, `g3e8_fromless_non_subquery_delete_still_executes`; repro §10.2 |
| **L1 M-2** (F-B) | D-5's order rationale ("cheap sync before async metadata") was true of the Spark door only; the ANSI door ran BUG-001 at the router head, i.e. FIRST. | **Reordered the ANSI door** so the code matches the claim: `refuse_mor_multi_spec_dml` moved into the shared `Delete`/`Update` arm, after the G3-E8 call. Order pinned on both doors. | `crates/repark-sql/src/{router,guards}.rs`; pin `mor_valve_runs_after_the_g3e8_valve`; D-5 rewritten |
| **L1 M-3** (F-C) | (1) The ANSI refusal derived its target from **scrubbed text**, so a quoted target rendered as blanks. (2) No pin could see the two doors' RENDERED messages drift. (3) D-1's share-or-duplicate dichotomy was false. | (1) Target now read from the parsed statement, both doors. (2) Cross-door rendered-equality pin on the dev-only test edge. (3) D-1 rewritten; hoist to a foundation crate named as the follow-up with its true cost (V-10). | `crates/repark-sql/src/guards.rs`; pins `dml_subquery_refusal_renders_a_usable_target_for_every_spelling`, `cross_door_g3e8_refusals_render_identically`; D-1 rewritten |
| **L1 M-4** (F-D) | Three subquery spellings were missing from the matrix, and V-1/D-3 drew the safe/unsafe boundary at **correlated vs uncorrelated** — which is false. | The three spellings executed pre-guard (§10.3), added to §2 as ❌, pinned in the refuse families on both verbs. D-3 gains a counter-example table and the per-SHAPE rule; §7 item 4 rewritten so nothing is re-enabled by inheritance. | §2, D-3, §7; `g3e8_delete_subquery_family_all_refuse`, `g3e8_update_subquery_family_all_refuse`, ANSI detector + e2e pins |
| **L1 N-1** (F-E) | CTE-prefixed `WITH … DELETE` is un-valved (it parses as a `Query`, not a `Delete`). | Executed: it is a LOUD `NotImplemented` today and writes nothing. Pinned AS loud, so the gap cannot reopen silently. Named in §2, §9 and the map Debug row. | `g3e8_cte_prefixed_dml_is_loud_today_and_writes_nothing`; `crates/repark-spark/src/map.md` |
| **L1 N-2** (F-F) | Two 🟡 "already loud" rows have their subquery in the `WHERE`, so the valve fires first — the classification described pre-guard behaviour, not shipped behaviour. | Re-executed (§10.4) and re-classified in §2 as valved, with the executed message. | §2 |
| **L1 N-3** (F-G) | The budget pin's control check was a literal `"(SELECT"` scan with one hard-coded whitespace spelling. | Replaced with `re.search(r"\(\s*SELECT", …, re.IGNORECASE)`. | `python/repark/tests/test_dml_subquery_parity.py` |
| **L1 N-4** (F-H) | D-1 claimed "the corpus needle asserts through either door". | Corrected: the corpus is the FACADE (Spark door) only; the ANSI door's needle is asserted in Rust (`guards/tests.rs` + the cross-door pin). | §10.9 note + D-1 rewrite |
| **L1 N-5** (F-I) | The recon's fix-option space omitted a fourth option. | Added to `G3E8-RECON.md` as a dated corrections section (custom `QueryPlanner` via `SessionStateBuilder::with_query_planner` intercepting `LogicalPlan::Dml` — one implementation, both doors). | planning-side |
| **L2 M1/M2** (F-J) | The neuter + clean-run transcripts merged per-binary result lines, quoted a stale ANSI test name, and double-counted the clean run as "8 + 3 + 28". | **Every provocation re-run** against shipped code, per-binary, with captured exits. §5 is now an index; the transcripts are §10.5. Double-count removed. | §5, §10.5 |
| **L2 M3** (F-K) | V-7 meant two different things in the ledger and the report. | Renumbered: V-7 = router extraction (the report's meaning), V-8 = no registry/commit/push. | §8 + report corrections |
| **L2 M4** (F-L) | "39-row matrix executed on both doors" — the ANSI door had 6 probes. | Corrected in the report §2 and the recon §3 heading (dated corrections sections). | planning-side |
| **L2 M5** (F-L) | Three matrices, three totals, no declared source. | §2 declared **CANONICAL**; the report's condensed view states its re-bucketing and fixes its counts. | §2 + report corrections |
| **L2 M6** (F-M) | `dml.rs` pin count stated 6, actual 7. | Recounted after the additions: **10** in `dml.rs`, **2** in `normalize.rs`; the artifact table and `crates/repark-spark/src/tests/map.md` now carry the real numbers with the derivation command. | artifact table, `tests/map.md` |
| **L2 M7** (F-N) | "1331/2648 before and after" was neither before nor after. | Re-derived (§10.8): Rust **1321 → 1340**, facade **2634 → 2648**. | §10.8, §4 note |
| **L2 N1** (F-O) | The residue-grep claim had no real command/output. | Real command + true output, with the 3 pre-existing hits named. | §10.6 |
| **L2 N2** (F-P) | V-3's ceiling wording. | "only allows downward" → "only allows downward **without a stated reason**". | §8 / D-7 |
| **L2 N3** (F-Q) | The GAV pin's docstring claimed more than the test checks (inherited CP-8 tautology). | Docstring narrowed to what it actually asserts; the mechanical fix DEFERRED to W-2b's single-home GAV helper (in flight — deliberately not collided with). | corpus module + §10.7 CP-8 |
| **L2 N4** (F-R) | No explicit CP null report. | §10.7 is the CP-1..CP-12 table. | §10.7 |
| **L2 N5** (F-S) | No cross-door rendered-equality pin. | Landed (same as F-C(2)). | `cross_door.rs` |
| **L2 N6** (F-T) | "the five-element pin" over-described what the pins assert. | Corrected in the report's corrections section: the Spark-door family pins assert five message elements; the ANSI e2e and the corpus rows assert the unique needle. | planning-side |
| **L2 N7** (F-U) | §7's limits omitted the ANY/ALL inference. | §9 now says the ANY/ALL loss ROUTE is inferred from source, not instrumented; the outcome is executed. | §9 + report corrections |
| **L2 N8** (F-V) | ❌ forms without a refusal pin; ⚠️/✅ rows with no pin at all. | Temp-view spelling pinned; `>`/`<>` scalar, `count(*) > 99`, and the SET-assignment-without-WHERE form all pinned rather than annotated. Nothing is left "verified by recon, unpinned". | `g3e8_delete_subquery_family_all_refuse`, `g3e8_update_set_subquery_without_where_subquery_still_executes` |
| **L2 N9** (F-W) | The uncorrelated-scalar row cited "recon round 2/3" instead of its pin. | §2 now names the pin. | §2 |
| **L2 N11/N12** (F-X) | "five reds are exactly the refuse pins"; the ANSI footnote said six forms. | Both corrected — the neutered run's reds are now enumerated per binary (§10.5), and the ANSI e2e pin's coverage is stated exactly (§2 footnote). | §2, §10.5 |
| L2 N10 | Charter preservation. | ORCHESTRATOR action, not fixer scope. | — |

### 10.2 The bypass — before and after

**BEFORE (the repro).** The pin was written first and run against the delivered code. The rows
assertion comes first on purpose, so the failure output shows the table's contents rather than
"it did not raise":

```
$ cargo test -p repark-spark --lib -- g3e8_fromless --nocapture   # (log 01, pre-fix)
running 2 tests

thread 'tests::dml::g3e8_fromless_delete_subquery_family_all_refuse' (765303) panicked at crates/repark-spark/src/tests/dml.rs:810:9:
assertion `left == right` failed: a FROM-less subquery DELETE must not touch a row, sql="DELETE ice.sales.tgt WHERE id IN (SELECT id FROM ice.sales.keys)", outcome=Ok(())
  left: []
 right: [(1, "a"), (2, "b"), (3, "c")]
test tests::dml::g3e8_fromless_delete_subquery_family_all_refuse ... FAILED
test tests::dml::g3e8_fromless_non_subquery_delete_still_executes ... ok

test result: FAILED. 1 passed; 1 failed; 0 ignored; 0 measured; 363 filtered out; finished in 0.13s
EXIT=101
```

`outcome=Ok(())` and `left: []` are the finding in one line: the statement **succeeded** and the
table came back **empty**, with the guard in place.

**AFTER (the same command, post-fix).** The whole `g3e8` filter, so the negatives are visible too:

```
$ cargo test -p repark-spark --lib -- g3e8            # (log 19, post-fix, post-restore)
running 12 tests
test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 355 filtered out; finished in 1.45s
EXIT=0
```

with the individual lines (log 08, same set):

```
test tests::normalize::g3e8_statement_valve_covers_both_verbs_and_renders_the_parsed_target ... ok
test tests::normalize::g3e8_subquery_detector_fires_on_every_spelling_and_no_other ... ok
test tests::dml::g3e8_subquery_valve_precedes_the_mor_multi_spec_valve ... ok
test tests::dml::g3e8_delete_in_subquery_refuses_and_leaves_every_row ... ok
test tests::dml::g3e8_fromless_non_subquery_delete_still_executes ... ok
test tests::dml::g3e8_cte_prefixed_dml_is_loud_today_and_writes_nothing ... ok
test tests::dml::g3e8_update_set_subquery_without_where_subquery_still_executes ... ok
test tests::dml::g3e8_fromless_delete_subquery_family_all_refuse ... ok
test tests::dml::g3e8_insert_and_merge_with_subqueries_still_execute ... ok
test tests::dml::g3e8_update_subquery_family_all_refuse ... ok
test tests::dml::g3e8_non_subquery_dml_still_executes ... ok
test tests::dml::g3e8_delete_subquery_family_all_refuse ... ok
```

The five spellings the disposition named: F1 (`IN`), F7 (lower-case), F8 (`NOT IN`), F9
(correlated `EXISTS`) all refuse inside `g3e8_fromless_delete_subquery_family_all_refuse`; F10
(FROM-less, non-subquery) still executes and deletes exactly the matched row —
`g3e8_fromless_non_subquery_delete_still_executes`, green in BOTH runs above, i.e. the fix did not
buy the refusal by breaking the form.

### 10.3 Pre-guard evidence for the added matrix rows (executed, valve neutered)

A scratch probe (never committed; removed and sha-verified in §10.6) ran each spelling through the
real engine with the valve neutered, printed the outcome and read the table back:

```
$ cargo test -p repark-spark --lib -- zz_probe_preguard --nocapture      # (log 12, NEUTERED)
[PREGUARD] CHANGED   rows=[] ok=true sql=DELETE FROM ice.sales.tgt WHERE NOT EXISTS (SELECT 1 FROM ice.sales.keys)
[PREGUARD] CHANGED   rows=[] ok=true sql=DELETE FROM ice.sales.tgt WHERE EXISTS (SELECT 1 FROM ice.sales.keys WHERE id = 999)
[PREGUARD] CHANGED   rows=[] ok=true sql=DELETE FROM ice.sales.tgt WHERE id IN (SELECT max(id) FROM ice.sales.keys)
[PREGUARD] CHANGED   rows=[(1, "z"), (2, "z"), (3, "z")] ok=true sql=UPDATE ice.sales.tgt SET name = 'z' WHERE NOT EXISTS (SELECT 1 FROM ice.sales.keys)
[PREGUARD] CHANGED   rows=[(1, "z"), (2, "z"), (3, "z")] ok=true sql=UPDATE ice.sales.tgt SET name = 'z' WHERE id IN (SELECT max(id) FROM ice.sales.keys)
[PREGUARD] CHANGED   rows=[] ok=true sql=DELETE ice.sales.tgt WHERE id IN (SELECT id FROM ice.sales.keys)
[PREGUARD] CHANGED   rows=[] ok=true sql=delete ice.sales.tgt where id in (select id from ice.sales.keys)
[PREGUARD] CHANGED   rows=[] ok=true sql=DELETE ice.sales.tgt WHERE id NOT IN (SELECT id FROM ice.sales.keys)
[PREGUARD] CHANGED   rows=[] ok=true sql=DELETE ice.sales.tgt WHERE EXISTS (SELECT 1 FROM ice.sales.keys k WHERE k.id = ice.sales.tgt.id)
[PREGUARD] CHANGED   rows=[(1, "a"), (3, "c")] ok=true sql=DELETE ice.sales.tgt WHERE id = 2
[PREGUARD] CHANGED   rows=[] ok=true sql=DELETE FROM ice.sales.tgt WHERE id IN (SELECT id FROM src WHERE id = 2)
[PREGUARD] CHANGED   rows=[(1, "a"), (2, "b")] ok=true sql=DELETE FROM ice.sales.tgt WHERE id > (SELECT max(id) FROM ice.sales.keys)
[PREGUARD] CHANGED   rows=[(2, "b")] ok=true sql=DELETE FROM ice.sales.tgt WHERE id <> (SELECT max(id) FROM ice.sales.keys)
[PREGUARD] UNCHANGED rows=[(1, "a"), (2, "b"), (3, "c")] ok=true sql=DELETE FROM ice.sales.tgt WHERE (SELECT count(*) FROM ice.sales.keys) > 99
EXIT=0
```

Reading it: the three **uncorrelated** spellings L1 M-4 named all emptied the table (`rows=[]`)
while committing successfully (`ok=true`) — which is why "correlated vs uncorrelated" cannot be
the boundary. The FROM-less family (rows 6-9) is the bypass, reproduced statement by statement.
Row 10 is F10, correct. Row 11 is the temp-view spelling (recon row 4), also emptied. The last
three are ⚠️ rows re-verified as correct-today: `id > (SELECT max(id))` leaves `{1,2}`,
`id <> …` leaves `{2}`, and `count(*) > 99` changes nothing — each the right answer.

### 10.4 The two re-classified 🟡 rows (F-F), executed

```
$ cargo test -p repark-spark --lib -- zz_probe_p4 --nocapture       # (log 22, valve LIVE)
[P4] G3-E8  rows=[(1, "a"), (2, "b"), (3, "c")]
     sql=DELETE FROM ice.sales.tgt WHERE name = (SELECT k.name FROM ice.sales.keys k WHERE k.id = ice.sales.tgt.id)
     msg=Error during planning: DELETE with a subquery in its WHERE clause is refused on `ice.sales.tgt`: subquery predicates are silently mis-executed today — …
[P4] G3-E8  rows=[(1, "a"), (2, "b"), (3, "c")]
     sql=DELETE FROM ice.sales.tgt AS x WHERE x.id IN (SELECT id FROM ice.sales.keys)
     msg=Error during planning: DELETE with a subquery in its WHERE clause is refused on `ice.sales.tgt`: subquery predicates are silently mis-executed today — …
[P4] OTHER  rows=[(1, "a"), (2, "b"), (3, "c")]
     sql=UPDATE ice.sales.tgt SET name = (SELECT k.name FROM ice.sales.keys k WHERE k.id = ice.sales.tgt.id)
     msg=Invalid (non-executable) plan after Analyzer
[P4] OTHER  rows=[(1, "a"), (2, "b"), (3, "c")]
     sql=UPDATE ice.sales.tgt SET name = (SELECT max(k.name) FROM ice.sales.keys k WHERE k.id = ice.sales.tgt.id)
     msg=UPDATE operation on table 'ice.sales.tgt'
[P4] OTHER  rows=[(1, "a"), (2, "b"), (3, "c")]
     sql=UPDATE ice.sales.tgt SET name = 'z' FROM ice.sales.keys k WHERE k.id = ice.sales.tgt.id
     msg=This feature is not implemented: UPDATE ... FROM is not supported
[P4] OTHER  rows=[(1, "a"), (2, "b"), (3, "c")]
     sql=DELETE FROM ice.sales.tgt USING ice.sales.keys k WHERE k.id = ice.sales.tgt.id
     msg=Error during planning: Using clause not supported
```

The two rows whose subquery sits in the `WHERE` now return the G3-E8 refusal; the two `SET`-
assignment rows and the `FROM`/`USING` rows keep their own loud errors, so the re-classification is
exactly two rows wide. (The `SET max(k.name)` row's message differs from the recon's transcript —
recorded here as observed.)

### 10.5 Re-run provocations — real commands, per-binary output, captured exits

The valve was neutered in BOTH doors by an early `return Ok(())` after the detector, marked
`PROVOCATION`, in the ONE place each door implements it. Every command below is
`cmd > log 2>&1; echo $?`.

**(a) Spark door, neutered.**

```
$ cargo test -p repark-spark --lib -- g3e8                          # (log 13, NEUTERED)
running 12 tests
test tests::normalize::g3e8_subquery_detector_fires_on_every_spelling_and_no_other ... FAILED
test tests::normalize::g3e8_statement_valve_covers_both_verbs_and_renders_the_parsed_target ... FAILED
test tests::dml::g3e8_subquery_valve_precedes_the_mor_multi_spec_valve ... FAILED
test tests::dml::g3e8_delete_in_subquery_refuses_and_leaves_every_row ... FAILED
test tests::dml::g3e8_fromless_delete_subquery_family_all_refuse ... FAILED
test tests::dml::g3e8_update_subquery_family_all_refuse ... FAILED
test tests::dml::g3e8_delete_subquery_family_all_refuse ... FAILED
test tests::dml::g3e8_fromless_non_subquery_delete_still_executes ... ok
test tests::dml::g3e8_cte_prefixed_dml_is_loud_today_and_writes_nothing ... ok
test tests::dml::g3e8_update_set_subquery_without_where_subquery_still_executes ... ok
test tests::dml::g3e8_insert_and_merge_with_subqueries_still_execute ... ok
test tests::dml::g3e8_non_subquery_dml_still_executes ... ok
test result: FAILED. 5 passed; 7 failed; 0 ignored; 0 measured; 356 filtered out; finished in 0.85s
EXIT=101
```

**7 red, 5 green.** The reds are the 6 refuse/detector pins plus the guard-ORDER pin (which needs
the refusal to win). The 5 greens are the adjacent negatives — non-subquery DML, the FROM-less
negative, `INSERT`/`MERGE` with subqueries, the `SET`-assignment form, and CTE-prefixed DML — which
is what proves they pin working surface rather than the guard.

**(b) ANSI door, neutered.** (The `guards` filter, which is where the ANSI pins now live — L2 M2
asked for exactly this run.)

```
$ cargo test -p repark-sql --lib -- guards                          # (log 14, NEUTERED)
running 31 tests
test guards::tests::dml_subquery_refusal_names_its_verb_and_target ... FAILED
test guards::tests::dml_subquery_refusal_renders_a_usable_target_for_every_spelling ... FAILED
test guards::tests::dml_subquery_valve_fires_on_every_spelling_and_no_other ... FAILED
test guards::tests::router_parse_dialect_matches_the_session_default ... ok
test guards::tests::mor_valve_wrapper_passes_what_it_cannot_or_must_not_gate ... ok
test guards::tests::mor_valve_runs_after_the_g3e8_valve ... FAILED
test guards::tests::dml_subquery_valve_refuses_end_to_end_and_writes_nothing ... FAILED
test result: FAILED. 26 passed; 5 failed; 0 ignored; 0 measured; 183 filtered out; finished in 0.13s
EXIT=101
```

(The listing is filtered to the G3-E8-relevant lines; the `test result` line is the binary's own,
unedited. The other 26 guard pins are untouched by the neutering, which is the point.)

**(c) Cross-door binary, neutered.**

```
$ cargo test -p repark-sql --test cross_door                        # (log 15, NEUTERED)
running 9 tests
test cross_door_g3e8_refusals_render_identically ... FAILED
test result: FAILED. 8 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.53s
EXIT=101
```

**Regression net = 7 + 5 + 1 = 13 Rust pins**, across three separate test binaries. (The earlier
"8 + 3 + 28" figure double-counted the ANSI binary's whole `guards` filter as if it were G3-E8
pins; it is not, and it is not summed here.)

**(d) Facade corpus, neutered engine.** The native module was rebuilt from the neutered source
first, so this is the engine, not a mock:

```
$ (cd python/repark && VIRTUAL_ENV=…/.venv uvx maturin@1.14.1 develop)     # (log 16)
📦 Built wheel for abi3 Python ≥ 3.12 to /tmp/.tmpnJE7jv/repark-0.0.0-cp312-abi3-linux_x86_64.whl
🛠 Installed repark-0.0.0
MATURIN_EXIT=0

$ PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest \
      python/repark/tests/test_dml_subquery_parity.py -q                   # (log 17, NEUTERED)
FAILED …::test_dml_subquery_row[delete_in_subquery]
FAILED …::test_dml_subquery_row[delete_not_in_subquery]
FAILED …::test_dml_subquery_row[delete_not_in_subquery_with_null_key]
FAILED …::test_dml_subquery_row[delete_exists_correlated]
FAILED …::test_dml_subquery_row[delete_not_exists_correlated]
FAILED …::test_dml_subquery_row[delete_correlated_in_subquery]
FAILED …::test_dml_subquery_row[update_in_subquery]
FAILED …::test_dml_subquery_row[update_not_in_subquery_with_null_key]
FAILED …::test_refusal_leaves_every_row_untouched
FAILED …::test_lifecycle_cleanup_after_refused_dml
10 failed, 4 passed in 2.16s
EXIT=1
```

All 8 split rows red plus the two refusal-behaviour pins; the 2 equality controls, the budget pin
and the GAV pin stay green — the corpus discriminates.

**(e) The classifier, both arms.** The neutered engine takes the **regression** arm live and
verbatim (the log above, `test_dml_subquery_row[delete_in_subquery]`):

```
E  AssertionError: delete_in_subquery: repark no longer refuses (the statement committed) but the
   result does NOT match the recorded Spark golden — this is a regression or a partial fix, not a
   clean convergence, and it is exactly the silent-data-loss shape G3-E8 named. Re-derive both
   halves in record mode (see this module's docstring) before flipping the pin. …
python/repark/tests/test_dml_subquery_parity.py:496: AssertionError
```

The **CONVERGED** arm cannot be reached by neutering (a neutered engine deletes everything), so it
was driven by replacing the lifecycle helper's return with the RECORDED Spark golden — the exact
shape of a landed fix. Scratch file, outside the repo tree, deleted after the run:

```
$ PYTHONPATH=python/repark-parity/src:python/repark/tests .venv/bin/python -m pytest \
      …/zz_classifier_arms.py -q -p no:cacheprovider -s                    # (log 18)

--- CONVERGED ARM ---
delete_in_subquery: repark and Spark have CONVERGED — repark now runs this subquery predicate and produces the RECORDED SPARK result, so the G3-E8 split disclosure is stale. Do not delete the row: flip it to kind='content', clear repark_error_needle, and record the convergence (the underlying fix has landed). …
.
--- REGRESSION ARM ---
delete_in_subquery: repark no longer refuses (the statement committed) but the result does NOT match the recorded Spark golden — this is a regression or a partial fix, not a clean convergence …
.
2 passed in 0.32s
EXIT=0
```

**(f) Clean re-runs after the restore** (three binaries, three separate commands, three exits):

```
$ cargo test -p repark-spark --lib -- g3e8                         # (log 19)
running 12 tests
test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 355 filtered out; finished in 1.45s
EXIT=0

$ cargo test -p repark-sql --lib -- guards                         # (log 20)
running 31 tests
test result: ok. 31 passed; 0 failed; 0 ignored; 0 measured; 183 filtered out; finished in 0.24s
EXIT=0

$ cargo test -p repark-sql --test cross_door                       # (log 21)
running 9 tests
test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.54s
EXIT=0
```

The facade corpus's clean re-run is inside the `make py-test-facade` gate (§10.8), which rebuilt
the native module from the restored source.

### 10.6 Restore, sha-verified

The two neutered files were restored from byte copies taken before the edit, and the scratch probe
was removed from `tests/dml.rs` the same way:

```
$ sha256sum crates/repark-spark/src/normalize.rs crates/repark-sql/src/guards.rs \
            crates/repark-spark/src/tests/dml.rs           # BEFORE neutering
35a1ce97ae99e0eec254b61940b9d49e0a9222b65de025918eecc3b00ac40fba  crates/repark-spark/src/normalize.rs
88dfdcb7370f5a6645021761e1ab5d5e1b508a6b3156dcfeb134416a658f02b2  crates/repark-sql/src/guards.rs
dc881da390e48da149639def8b6f04474bf0df66eca2566d12319e829528faf8  crates/repark-spark/src/tests/dml.rs

$ sha256sum …same three files…                             # AFTER restore
35a1ce97ae99e0eec254b61940b9d49e0a9222b65de025918eecc3b00ac40fba  crates/repark-spark/src/normalize.rs
88dfdcb7370f5a6645021761e1ab5d5e1b508a6b3156dcfeb134416a658f02b2  crates/repark-sql/src/guards.rs
dc881da390e48da149639def8b6f04474bf0df66eca2566d12319e829528faf8  crates/repark-spark/src/tests/dml.rs

$ diff sha-before.txt sha-after.txt ; echo $?
0
```

(`dml.rs` carries the same sha after the §10.4 probe too — that probe was appended to and restored
from a second byte copy taken at the post-fix state.)

**Residue grep (F-O / L2 N1) — the real command and its true output:**

```
$ grep -rni provocation crates/ python/
crates/repark-python/src/lib.rs:57:/// module-scoped `disallowed_methods` expectation and the P-4/P-5 provocation record).
crates/repark-python/src/exceptions.rs:5://! (provocations P-4/P-5, p3c ledger). Re-exported at the crate root — `crate::…Exception`
python/repark/tests/_record_merge_differential_goldens.py:181:    """Provocation: after the error row, the Spark catalog must not list the error table."""
GREP_EXIT=0
```

All three hits are **PRE-EXISTING** and belong to other units (P-4/P-5 in the PyO3 bindings ledger,
and the MERGE corpus's own recorded provocation). None of the three files is touched by this unit
(`git status --porcelain` lists neither). The marker this pass actually used was the upper-case
`PROVOCATION`, and it is gone:

```
$ grep -rn "PROVOCATION" crates/ python/
GREP_EXIT=1        # no matches
```

### 10.7 CP-1 … CP-12 null report (F-R / L2 N4)

| CP | What it guards | This unit |
|---|---|---|
| CP-1 | No fabricated/rounded numbers | CLEAR after this pass — every count in this ledger is either a pasted tool line or has its derivation command beside it (§10.8) |
| CP-2 | No silently relaxed assertion | CLEAR — no existing assertion was weakened; the ANSI unit pins were re-shaped to take a `Statement` (same coverage plus the target), and 8 pins were added |
| CP-3 | No `#[ignore]` / `--skip` / commented-out test | CLEAR — none added; `grep -rn "#\[ignore\]" crates/` unchanged |
| CP-4 | Every gate is provoked | CLEAR — §10.5, re-run against shipped code. **Row updated for F-A/F-B**: the neuter now reds 13 Rust pins across 3 binaries + 10 Python rows (was: 5 pins, one merged transcript) |
| CP-5 | No vacuous pin (`assert!(is_ok())` as a whole body) | CLEAR — every refusal pin asserts the guard's OWN message tokens; every negative asserts the resulting ROWS |
| CP-6 | Lockstep `map.md` | CLEAR — 7 maps updated in this pass (`repark-spark/src`, `…/src/tests`, `repark-sql/src`, `…/src/guards`, `…/src/router`, `repark-sql/tests`, plus the earlier `python/repark/tests` + `task`) |
| CP-7 | No destructive/outward-facing operation | CLEAR — AWS untouched; every test uses a temp-dir memory catalog |
| CP-8 | No tautological pin | **NOT CLEAR — INHERITED.** `test_iceberg_gav_pin_is_exact_spark_minor` asserts a constant against itself. Docstring narrowed to what it really checks; the mechanical fix belongs to W-2b's single-home GAV helper, which is in flight in another lane and must not be collided with. Carried forward, not closed |
| CP-9 | Landmine sweep (tests asserting the wrong behaviour) | CLEAR — re-swept in this pass: no test anywhere asserts the pre-guard (data-loss) behaviour; the two string-transform cases in `test_f1_sql_expander.py` never execute DML |
| CP-10 | No lockfile / CI / fork edits | CLEAR — `git status --porcelain` carries no `Cargo.lock`, `uv.lock`, `.github/`, or fork path |
| CP-11 | Guard order is pinned, not assumed | CLEAR after this pass. **Row updated for F-B**: the order is now the same on both doors AND pinned on both (`g3e8_subquery_valve_precedes_the_mor_multi_spec_valve`, `mor_valve_runs_after_the_g3e8_valve`), each with a control proving the second valve still fires |
| CP-12 | No git identity written, nothing committed/pushed | CLEAR — no `git config` was run; the tree is uncommitted and unpushed |

### 10.8 Gate output after the fix pass

```
$ make verify        # = make ci + make test                        # (log 23)
crate-dag: 20 internal edges clean (4 dev, 15 normal, 1 optional) across 9 of 9 mapped crates
lib-rs: 9 crate roots clean (no inline test modules; ceilings held)
rust-file-size: 181 files clean (default ceiling 1500; 13 exceptions)
lib-py: 54 files clean (ceilings held; no-stub rule held)
manifest: 12 components (9 delivered, 3 planned) agree with the workspace, the gates, the doc
          index, the status document and the crate maps
parity-live dual-wire: OK (maturin@1.14.1, extras=['ml-ext', 'numpy', 'pandas', 'polars', 'record'],
          uv-run=['--locked', '--no-sync'])
uvx ruff@0.15.22 check .          /  format --check .
uv lock --locked                  Resolved 29 packages in 2ms
uvx taplo@0.9.3 format --check / lint
uvx typos@1.47.2
EXIT=0
```

```
$ make py-test-facade                                               # (log 24)
2648 passed, 46 skipped, 37 warnings in 95.95s (0:01:35)
EXIT=0
```

**Counts, each with its derivation (F-N / L2 M7 / F-M / L2 M6).**

```
$ grep -c "^test result: ok" 23-make-verify.log
33
$ grep -oE "^test result: ok\. [0-9]+ passed" 23-make-verify.log | awk '{s+=$4} END {print s}'
1340
$ git diff -U0 -- 'crates/**/*.rs' | grep -cE "^\+\s*#\[(tokio::)?test\]"
19          # test attributes ADDED by the whole unit (0 removed) → baseline 1340 - 19 = 1321
$ pytest python/repark/tests/test_dml_subquery_parity.py --collect-only -q | tail -1
14 tests collected          # → facade baseline 2648 - 14 = 2634
```

| Count | Value |
|---|---|
| Rust workspace tests, BEFORE the unit | **1321** (derived above) |
| Rust workspace tests, AFTER the unit + fix pass | **1340** (33 binaries, all `ok`) |
| Rust pins this unit contributes | **19** |
| — `crates/repark-spark/src/tests/dml.rs` | **10** |
| — `crates/repark-spark/src/tests/normalize.rs` | **2** |
| — `crates/repark-sql/src/guards/tests.rs` | **6** |
| — `crates/repark-sql/tests/cross_door.rs` | **1** |
| Facade tests, BEFORE the unit | **2634** |
| Facade tests, AFTER | **2648** (46 skipped, unchanged) |
| Python rows this unit contributes | **14** (10 parametrized corpus rows + 4 meta pins) |

Re-derive the per-file split with:
`grep -nE "^(async )?fn (g3e8|dml_subquery|mor_valve_runs|router_parse_dialect|cross_door_g3e8)" <file>`
(minus the two helper fns `g3e8_setup` / `g3e8_seed` in `dml.rs`).

### 10.9 Held under attack — carried verbatim from both lenses

**Lens 1 (code).** *(The lens-1 items that survived its own attack are the ones its dispositions
did not name; the fix pass leaves them exactly as delivered.)* The detector rule ("a `Query` node
under the predicate", not an `Expr`-variant enumeration) held; the deliberate over-refusal decision
(D-3's asymmetry argument) held as a DECISION even though its stated boundary did not; the
adjacent-negative set held; the refusal message's content (defect class, mechanism, consequence,
id, workaround, "support returns") held; `INSERT … SELECT` / `MERGE` unaffected held; the fix-home
recommendation (RePark, not the fork) held.

**Lens 2 (record), verbatim:**

> Held-under-attack: gates §4a-§4d all reproduced; V-1..V-7 rationales all TRUE; CP null-report
> all-clear except inherited CP-8; conductor ban respected; hygiene ready; no valve bypass on
> either door.

One correction the fixer owes that list, since it is carried verbatim: **"no valve bypass on either
door" was FALSE for the Spark door** — lens 1 found and this pass fixed exactly that (§10.2). It is
true of the ANSI door, before and after, and is now pinned
(`router_parse_dialect_matches_the_session_default`).

### 10.10 Deviations from the dispositions — FLAGGED

| Disposition | Deviation | True reason |
|---|---|---|
| **F-A** "single-home preferred" | The valve is CALLED from two places on the Spark door (passthrough + router arms), one implementation. | Ordering. The passthrough is downstream of the BUG-001 valve; a single home there would flip D-5's order on the Spark door in the very pass that F-B is making that order true on the ANSI door, and would spend an Iceberg metadata round-trip before every G3-E8 refusal. Recorded as D-8 / V-9. Safety does not depend on the router call — deleting it changes only which message a doubly-hazardous statement gets. |
| **F-C(3)** "hoist detector+message to repark-common IF the diff stays proportionate" | NOT hoisted. | `repark-common` depends on `thiserror` alone; the detector needs sqlparser. The hoist = a DataFusion dep in tier 0 + promoting `repark-spark → repark-common` from `dev` to `normal` in the DAG SSOT. Disproportionate inside a guard unit. The disposition's stated minimum landed instead (equality pin + honest D-1 + named follow-up). V-10. |
| **F-J** "re-run the provocations" | The LIVE-Spark provocations (§4d record mode, §4e emitter, §5c golden perturbation) were NOT re-run. | The dispositions named the neuter and clean-run provocations; the live ones need the JVM oracle and nothing this pass touched can move them — the corpus module's only edits were `import re`, the control-row subquery regex, and the GAV docstring. Labelled as ACTOR transcripts in §5 rather than presented as fresh. V-11. |
| **F-D** "add the three missing spellings … as ❌ with executed evidence" | Also added their UPDATE twins and re-executed four ⚠️ rows that were not asked for. | Cheap, same probe run, and it is what turns §7 item 4 from a prose warning into a table a fix unit can act on. No deviation in substance — a superset. |

## Landing note (L-1, 2026-08-12)

§6 BACKLOG rows classified **LANDED** as registry G3-E8 / G3-E8-NULL (no live-mirror — DML
lifecycle is not a single-shot Disclosure). The FIX unit remains queued.
