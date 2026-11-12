# SIGNED-ZERO-NORMALIZE-1: order, part B part 1 (SIGNED-ZERO-KEYS, flat keys) — DRAFT

**Status:** Draft, filed 2026-11-12; questions Q1, Q2 and Q4–Q9 await the owner.

Drafted by `claude-opus-5-5` (2026-11-12). Nothing in it is built. Questions Q1, Q2 and Q4–Q9 in
[questions.md](evidence/part-b/questions.md) are NOT RULED, and their defaults are marked "(default, Qn)" below. Q3 is
resolved on main by #1307, with no change in part 1.

**Grade:** B (guided).

**Builder:** Claude Opus 5.5 high (`claude-opus-5-5`). The 2026-11-12 window has every agent on Opus 5.5 high.

**Review:** two independent Opus 5.5 high verifiers, in parallel, on the same head and the same tables (R7,
design-heavy; ruling 2026-11-12). The cap is one build, one critique, one delta and one delta re-check. The
scoped-review rule (2026-11-09) and the named-seat rule (2026-11-10) bind.

**Release:** v1.5.7. This part is G3 of ICE-V2-FLAT-AWS-LABEL-1, so it blocks the label and the release.

**Branch:** `fix/signed-zero-normalize-1-part-b`, at main `276dc503` (#1307 on top of `ae247767`, the step-0 base).

**Split** (rulings 2026-11-11 Q3 and Q4): PR B comes in two parts.
- **Part 1** is this order: flat FLOAT and DOUBLE keys. Its bars are 1.05x (no float keys) and 1.20x (float keys).
- **Part 2** is a later order, the nested-float write conditions (§9).

**Design source.**
- [sketch.md](sketch.md) §1, §3, §4, §6–§8 (seat 2, the UDF,
  interactions, cost, doors), as amended here. Where they disagree, this order governs.
- Part A's [order.md](order.md) and its evidence.
- This step's [step0/](evidence/part-b/step0/table.md) and [inventory.md](evidence/part-b/inventory.md).

## 1. Rulings this order carries (owner, verbatim)

- **2026-11-08, "G yes and defaults"** (sketch §12, defaults):
  - PR A then PR B;
  - perf ≤1.20 with the native key-wrap-off fallback as an owner carve-out;
  - two per-door switches, both on;
  - canonical positive NaN for the UDF;
  - min/max sign and zero tie order declared;
  - the truth column is Spark SQL semantics, with Spark-on-Iceberg zero-only answers declared `PRUNED-WRONG`;
  - `nullif` and other function-internal float comparisons carded as `FUNC-FLOAT-EQ-1`.
- **2026-11-09, "Go with defaults"** (unit C ruling 6): the insertion inventory binds every analyzer-seat brief. The
  third of the review-cap memo's additions, **seat-2 idempotence, is part B's.**
- **2026-11-10, "Q2–Q7 as pasted but they have my full go ahead and sign off":** the table-completeness check, the
  named-seat rule (Q3), and the "consumers of the pairing" inventory row (Q4).
- **2026-11-11, "Go with the defaults"** (label path forward):
  - **Q3:** nested-float SELECT answers become a card; the write conditions block, as **PR B part 2**.
  - **Q4:** PR B comes in **two parts**, with bars of **1.05x (no float keys)** and **1.20x (float keys)** against PR
    A's merged head.
- **2026-11-11, scoped label (defaults on Fable's recommendations):** float grouping and join keys are **in scope**, so
  part B's flat keys (part 1) gate the label.
- **2026-11-12:** the review is two independent Opus 5.5 high verifiers in parallel (R7, design-heavy). For a window
  today, every agent is Opus 5.5 high.

## 2. Rebase and premises (item 1, before code)

Step 0 measured main `ae247767`, which includes part A (`04d84ab9`), #1303, #1304 (LISTVIEW-INNER-JOIN-KEY-1, the
key-name join path) and #1310. The branch is now on main `276dc503`, which adds only #1307 (the flat matrix's float
seeds keep the sign; test files only). Item 1:

1. **Cites.** Re-verify every `file:line` that [inventory.md](evidence/part-b/inventory.md) and sketch §3/§4 cite, on the branch. Record
   any seat that moved. A seat whose premise no longer holds is a HALT.
2. **Re-measure.** Run the step-0 probes (`step0/szb_probe.py`, all three parts) on the branch's base build. The
   RePark legs must reproduce `step0/raw/repark-ae247767-*.jsonl` label for label. The allowed differences are a
   cell whose recorded rows vary in order only, and the floatkeys `matrix` cells, which move with #1307's seeds
   (item 4).
3. **Add the missing cells** named NOT MEASURED in the completeness check (`step0/table.md` §7):
   - **D16 spill:** float keys under a memory limit small enough that `spill_count > 0` on both doors, including the
     facade.
   - **D14 plan shape:** one EXPLAIN per §4 node kind.
   - **`Distinct::On`:** native, plan shape only.
4. **The seed finding (Q3) is resolved on main by #1307; no change in part 1.** Re-run on `276dc503`
   ([q3-rerun/](evidence/part-b/q3-rerun/table.md)): with the matrix's own seeds, all 28 floatkeys cells are non-EQUAL (22
   WRONG-READ, 2 SIGN-ONLY+WRONG-READ, 4 WRONG-WRITE), label for label the same as the signed variant. The FLOAT
   column's seed, `CAST('-0.0' AS DOUBLE)` stored into `FLOAT`, keeps the sign on both engines. Part 1's PR still
   drops the 28 float cells from `KNOWN_OPEN` (`known_open_unit`, `_flat_matrix_cells.py:288`), or they XPASS
   under strict xfail once the keys are fixed.

## 3. Clause: part 1's measured cells

The clause is the step-0 table, [step0/table.md](evidence/part-b/step0/table.md), measured on main `ae247767`'s release build against
Spark 4.1.2. Truth (ruling G): Spark SQL semantics.
- **Reads:** Spark over inline `VALUES` views.
- **Writes:** Spark on one-file Iceberg tables.
- **Native rows:** Spark's `spark-sql` answer.

The labels are part A's `label_of`, unchanged.

**The totals:**

| | cells | EQUAL | WRONG-READ | SIGN-ONLY+WRONG-READ | WRONG-WRITE | ORDER-DIFFER | refusals and acceptances |
|---|---|---|---|---|---|---|---|
| RePark, all | 1150 | 434 | 502 | 32 | 78 | 21 | 83 |
| float cells, attributable (decimal twin EQUAL, or no twin) | 660 | 92 | 438 | 30 | 74 | 18 | 8 |
| float cells, confounded (decimal twin not EQUAL) | 78 | 0 | 28 | 0 | 0 | 2 | 48 |
| decimal control | 356 | 314 | 14 | 0 | 0 | 1 | 27 |
| floatkeys on `ae247767` (bare seeds, as the matrix was / signed seeds) | 28 / 28 | 28 / 0 | 0 / 22 | 0 / 2 | 0 / 4 | 0 / 0 | 0 / 0 |
| floatkeys on `276dc503` (the matrix's #1307 seeds) | 28 | 0 | 22 | 2 | 4 | 0 | 0 |

**Attributable float cells by door and site group** (computed from `step0/labels.jsonl`; DOUBLE and FLOAT together; WR = WRONG-READ, SO+WR = SIGN-ONLY+WRONG-READ, WW = WRONG-WRITE, OD = ORDER-DIFFER, EQ = EQUAL, AWSR = ACCEPTED-WHERE-SPARK-REFUSED, BR = BOTH-REFUSED):

| door | group | WR | SO+WR | WW | OD | EQ | AWSR | BR | total |
|---|---|---|---|---|---|---|---|---|---|
| spark-sql | compare | 20 |  |  |  | 2 |  |  | 22 |
| spark-sql | distinct | 8 | 2 |  |  | 4 |  |  | 14 |
| spark-sql | group-by | 14 | 4 |  |  | 2 |  |  | 20 |
| spark-sql | join | 54 |  |  |  | 6 |  |  | 60 |
| spark-sql | min-max |  | 2 |  |  |  |  |  | 2 |
| spark-sql | nan-payload | 5 |  |  | 1 |  |  |  | 6 |
| spark-sql | order-by | 2 |  |  | 6 |  |  |  | 8 |
| spark-sql | partition | 6 |  |  |  | 2 |  |  | 8 |
| spark-sql | prepared | 6 |  |  |  |  |  |  | 6 |
| spark-sql | set-op | 10 | 2 |  |  | 6 | 1 |  | 19 |
| spark-sql | subquery | 20 |  |  |  | 4 |  |  | 24 |
| spark-sql | temp-view | 2 |  |  |  |  |  |  | 2 |
| spark-sql | window | 12 |  |  |  |  |  |  | 12 |
| spark-sql | write |  |  | 72 |  | 24 | 6 |  | 102 |
| f-expr | compare | 8 |  |  |  | 2 |  |  | 10 |
| f-expr | distinct | 4 |  |  |  |  |  |  | 4 |
| f-expr | group-by | 4 | 2 |  |  |  |  |  | 6 |
| f-expr | join | 24 |  |  |  |  |  |  | 24 |
| f-expr | order-by |  |  |  | 2 |  |  |  | 2 |
| f-expr | window | 2 |  |  |  |  |  |  | 2 |
| typed | compare | 18 |  |  |  |  |  |  | 18 |
| typed | distinct | 8 | 2 |  |  | 2 |  |  | 12 |
| typed | group-by | 10 | 2 |  |  |  |  |  | 12 |
| typed | join | 34 |  |  |  | 2 |  |  | 36 |
| typed | min-max |  | 2 |  |  |  |  |  | 2 |
| typed | order-by | 2 |  |  | 4 |  |  |  | 6 |
| typed | set-op | 8 | 2 |  |  | 6 |  |  | 16 |
| typed | window | 12 |  |  |  |  |  |  | 12 |
| native | compare | 22 |  |  |  | 2 |  |  | 24 |
| native | distinct | 6 | 2 |  |  | 4 |  |  | 12 |
| native | group-by | 16 | 4 |  |  | 2 |  |  | 22 |
| native | join | 54 |  |  |  | 6 |  |  | 60 |
| native | min-max |  | 2 |  |  |  |  |  | 2 |
| native | nan-payload | 5 |  |  | 1 |  |  |  | 6 |
| native | order-by |  |  |  | 4 |  |  |  | 4 |
| native | prepared | 6 |  |  |  |  |  |  | 6 |
| native | set-op | 10 | 2 |  |  | 6 |  | 1 | 19 |
| native | subquery | 20 |  |  |  | 4 |  |  | 24 |
| native | window | 6 |  |  |  |  |  |  | 6 |
| native | write |  |  | 2 |  | 6 |  |  | 8 |

**The clause, stated as checkable propositions.** After part 1, on the release and ci builds:

1. **Every attributable float cell reaches the truth.**
   - Every one of the 438 WRONG-READ, the 24 SIGN-ONLY+WRONG-READ outside MM-1, the 74 WRONG-WRITE and the 6 m1 MERGE
     ACCEPTED-WHERE-SPARK-REFUSED attributable float cells is EQUAL; for m1, RePark now raises
     `MERGE_CARDINALITY_VIOLATION` (default, Q7). The seventh ACCEPTED-WHERE-SPARK-REFUSED cell, SO-recursive, is out
     of the clause: Spark refuses `UNION` in a recursive CTE.
   - So are the 14 wrong orders the classifier labels ORDER-DIFFER on tie-broken sorts: OB-1, OB-3 and NN-4 (the other
     4 ORDER-DIFFER cells are OB-4, declared).
   - So are the SQ-notin-ieee and WR-insert-notin-ieee rows (default, Q1).
2. **The declared rows stay declared, and get their registry text** (sketch §6 wording):
   - MM-1 (SIGN-ONLY: min/max of zeros);
   - OB-4 (ORDER-DIFFER: zero ties with no tie-breaker);
   - SQ-notin and WR-insert-notin under Spark's default plan (`NAAJ-WRONG`, default Q1).
3. **The controls do not move:**
   - every EQUAL float cell (92), including SO-unionall (stored signs survive a `UNION ALL`), PT-1 (stored values
     through an identity partition), CM-9 (part A's simple `CASE` over a zero) and the cells EQUAL by seed (DI-5,
     DI-7, JN-keylist/un, JN-filter/un, JN-nonequi/un, SO-intersect/up, WR-merge-notmatched/un, WR-insert-notin/up);
   - all 356 decimal-control cells, whatever their label;
   - part A's tables and pins (#1298's facade pin, P1–P4, P6);
   - Fable's AR01 and AR02 (`array_intersect` and `array_except` keep the zeros distinct, as Spark does).
4. **The confounded float cells move only on their float pairing** (table §4 lists them with their twin):
   - INTERSECT ALL / EXCEPT ALL: Q2;
   - WI-2 `f-expr`, and native WI-2/3/5 and OB-1/2: Q6, judged with the NULL row excluded;
   - GB-11 typed pivot: `PIVOT-EMPTY-CELL-COUNT-1`;
   - CM-11 spark-sql and GB-10 spark-sql: the Spark-door refusals, carded;
   - the native MERGE, subquery-DML and CTAS refusals: the native door cannot express them.

   Each of these cells either reaches its decimal twin's label, or keeps its label with the float pairing proved by
   EXPLAIN (one wrapper per key).
5. **The floatkeys cells,** with the matrix's own seeds (#1307; Q3 resolved), are all EQUAL. Table §8 maps each one
   to the part 1 cells on the same site and build.

### 3.1 Table-completeness check (Q3, 2026-11-10)

`step0/table.md` §7 lists every named seat with its cells and disposition:
- the brief's key sites, doors, types and values;
- sketch §3's node list;
- sketch §6's interactions;
- the review-cap memo's seat-2 idempotence.

NOT MEASURED seats are added in item 1 (§2.3). Summary of the dispositions:

| Named seat | Cells | Disposition |
|---|---|---|
| GROUP BY: column, computed, key list, HAVING, grouping sets (ROLLUP, CUBE, GROUPING SETS), group output value, repartition-then-group, temp view, identity partition | GB-1…GB-10, TV-1, PT-2, PT-4 | clause |
| DISTINCT: `SELECT DISTINCT`, single and multi distinct aggregates, `dropDuplicates` | DI-1…DI-7 | clause (DI-5, DI-7 controls) |
| Joins: inner, left, right, full, semi, anti, USING, NATURAL, key list, two float keys, `<=>`, comma, sort-merge, cross-column, non-equi filter | JN-* | clause |
| Subqueries: `IN`, `EXISTS`, `NOT EXISTS`, scalar | SQ-* | clause. `NOT IN`: Q1. `= ANY`: out (Spark refuses) |
| Set operations: `UNION`, `INTERSECT`, `EXCEPT`, both-sign dedup; `UNION ALL` | SO-* | clause; `UNION ALL` is a control; `ALL` multisets: Q2 |
| Window `PARTITION BY` and `ORDER BY` (rank, dense_rank, RANGE peers, row_number) | WI-1…WI-6 | clause |
| ORDER BY with a tie-breaker, TopK, descending; without a tie-breaker | OB-1…OB-3; OB-4 | clause; OB-4 declared |
| min / max | MM-1 | declared |
| Non-literal comparisons, `BETWEEN`, `IN` items, simple `CASE` (D8, left to B by part A's F2), aggregate `FILTER` | CM-1…CM-8, CM-10…CM-12 | clause; CM-9 is part A's control |
| Native `PREPARE` / `EXECUTE` (seat-2 idempotence) | PR-1, PR-2 | clause + one-wrapper EXPLAIN pin |
| MERGE `ON` (update, insert, cardinality), DELETE/UPDATE with `IN`/`EXISTS`, INSERT `NOT IN`, CTAS from group/distinct | WR-* | clause (Q1, Q7) |
| NaN payloads in every key kind | NN-1…NN-6 | clause |
| The flat matrix's floatkeys | table §8, [q3-rerun/](evidence/part-b/q3-rerun/table.md) | clause; the seeds were fixed on main by #1307 (Q3 resolved) |
| `Distinct::On`, window distinct args, recursive `UNION`, `TableScan.filters` untouched, spill, dynamic filters | — | plan-shape pins, D16 (item 1), cost gates |

## 4. Design

Sketch §1 (seat 2's row), §3, §4, §6 and §7 apply, with these bindings.

1. **The UDF `__repark_normalize_zero__`** (sketch §4), in `crates/repark-core/src/session/df_guards/signed_zero/udf.rs`.
   - **Registration:** registered in `context_with_df_54_1_rule_guards` beside `register_repark_isnan`
     (`df_guards.rs:100`).
   - **Signature:** one `Float32`/`Float64` argument; the return type, field and nullability equal the input's.
     `Immutable`.
   - **Values:** NULL passes through. Any NaN becomes the canonical positive NaN (ruling G). `-0.0` becomes `+0.0`.
     Everything else is identity, bit for bit.
   - **Kernel:** the fast path returns the input `ArrayRef` unchanged when no `-0.0` and no non-canonical NaN is
     present (no allocation); otherwise one allocation and one pass.
   - **`simplify`** collapses a directly nested call.
   - **Helper:** share one normalisation helper with `is_duplicated.rs:146` and `freq_items.rs:153`, homed in core or
     common.
2. **Seat 2, `signed_zero_keys`** (sketch §3), in `signed_zero/keys.rs`. It is appended after the loop in
   `unnest_safe_optimizer_rules` (`df_guards.rs:117-142`), so it runs last in every pass.
   - **What it wraps:** each float key position in sketch §3's node list and `inventory.md` §2's "part B seat" rows.
     That includes null-aware anti joins (default, Q1).
   - **What it never wraps:** a literal (it canonicalises a literal in place instead), a function argument, a plain
     projection value, or `TableScan.filters`.
   - **Names:** `NamePreserver` aliases only in output-schema positions (`Projection`, `Aggregate` group and aggregate
     expressions, `Window` expressions). Each `GroupingSet` member is aliased under its own saved name, not the set
     (part A's B1 lesson).
   - **Fast exit:** skip any node whose schema has no float key position before walking its expressions. This is
     the 1.05x bar's guard.
3. **Idempotence** (review-cap memo §7; inventory §3): the three mechanisms of sketch §1, with one pin per re-entry
   path:
   - several passes;
   - `PREPARE` then `EXECUTE`;
   - views and temporary views;
   - `F.expr` fragments;
   - CSE-shared keys;
   - a group output read by a parent.
4. **The two per-door switches** (ruling G, "both on"), as `literals` (seats 1/1b) and `keys` (seat 2) (default, Q5).
   - They form a door-internal policy, set by the door's `SessionExtension`, both `true` on the Spark door and on
     native. There is no user config.
   - Every rule is installed on every door and reads the policy at rewrite time, so the rule-list pins stay one list.
   - A switch that is off returns `Transformed::no` at the root.
5. **The native fallback carve-out** (ruling G: "perf ≤1.20 with the native key-wrap-off fallback as an owner
   carve-out"). If the float-key bar fails **on native only**, set native `keys = false`. Native float keys then
   compare by totalOrder, declared, and the Spark door stays on. A Spark-door failure is a HALT, not a carve-out.
6. **Interactions** (sketch §6):
   - Stored values are never normalised, except group, distinct and set-operation outputs (Spark normalises those,
     WR-ctas-*).
   - The MERGE cardinality change is in the clause (default, Q7).
   - No new predicate reaches an Iceberg evaluator.
7. **Out of part 1:**
   - nested floats (part 2, §9);
   - `FUNC-FLOAT-EQ-1`, `COLLECTION-FLOAT-EQ-1`, `DIST-SIGNED-ZERO-1`, `NAN-PAYLOAD-1` outside key positions;
   - the cards of Q8.

## 5. Inventories (item 1 confirms; drafted in [inventory.md](evidence/part-b/inventory.md))

- **Insertion inventory** (2026-11-09): inventory §1, twenty rows, each answered for seat 2.
- **Type-variant inventory:** part A's §5 holds for keys. The new questions are `Float32` keys through `unwrap_cast` and
  a FLOAT key joined to a DOUBLE key (coercion casts the FLOAT side, so the wrap goes outside the cast). Item 1
  measures one cell of the latter.
- **Consumers of the pairing** (Q4, 2026-11-10): inventory §2.

## 6. Pins

Tests go in the same commit as the code they pin. Every expected value comes from a measured cell.

- **P5, key sites (facade).** The step-0 clause, table-driven over all four doors, against the recorded Spark oracle
  JSONL (`step0/raw/spark-*.jsonl`). Builds are single-sign (`up`, `un`). The decimal control column is carried.
  Per-cell captures catch pyo3's `PanicException`.
- **P5r, key sites (Rust, `repark-core`).** A core session over a MemTable. One case per sketch §3 node kind on
  `Float32` and `Float64`, with ±0, NaN of both signs, NULL and 1.5. Expected sets come from the truth.
- **P6, plan shape and names.**
  - One `__repark_normalize_zero__` per float key after a full optimize, on every node kind.
  - Optimizing twice gives an identical plan.
  - `EXPLAIN EXECUTE` of PR-1 and PR-2 shows one wrapper per key.
  - No wrapper in `TableScan.filters`.
  - Output names, `printSchema` and qualifiers are unchanged for every clause cell (GB-*, DI-*, the f-expr and typed
    forms, and part A's D6-3…D6-6, whose values are now part B's; part A's F5).
- **P7, NaN and stored bits.**
  - NN-1…NN-6 reach the truth.
  - SO-unionall and PT-1 keep stored signs.
  - Part A's D11 rows still store `-0.0` bit for bit.
  - The UDF unit tests: fast path pointer-equal output, null buffer kept, `f32` and `f64`, scalar input, NaN payload
    and sign canonicalised.
- **P8, performance:** §8, recorded in the evidence.
- **Floatkeys:** `_flat_matrix_cells.py` with main's seeds (#1307, unchanged by part 1), and its 28 float cells out
  of `KNOWN_OPEN`; its local twin (step0 `floatkeys` part, variants `matrix` and `signed`) is green.
- **Mutation record** (part A's item 5 shape):

  | Mutation | Turns red |
  |---|---|
  | Seat 2 off | the key rows (P5, P5r) |
  | Idempotence mechanisms (i) and (ii) off | the one-wrapper pins |
  | The UDF's NaN canonicalisation off | NN-* |
  | The UDF's zero map off | every zero key row |
  | The `keys` switch off on one door | that door's key rows only |
  | The literal-skip off (seat 2 wraps a literal) | part A's P1 or a names pin |
  | Null-aware anti-join wrapping off | SQ-notin-ieee (default, Q1) |
- **Both build profiles:** the facade gate runs in the ci profile and in release.

## 7. Halts

- A premise in `inventory.md` or sketch §3/§4 that no longer holds on the branch.
- A control cell (§3.3) that moves: any EQUAL cell newly refused or wrong, unless it is a measured S3 over-refusal
  carded with its cell id.
- Any cell outside the clause that moves, unless it moves to Spark's measured outcome and the evidence names it.
- Any change to part A's tables or pins, to unit C's, #1290's, #1292's, #1295's or #1304's pins, or to the outcome of
  their tables.
- A `-0.0` or a NaN payload stored differently from before, outside group, distinct and set-operation outputs.
- A wrapper in `TableScan.filters`, or any new predicate reaching an Iceberg evaluator.
- `UPDATE-SCHEMA-BATCH-MISMATCH-1` reproducing more often after the change than before: 50 repeats per kind, before and
  after.
- A cost gate exceeded (§8), except the native carve-out of design point 5.
- A verifier or step finding that Spark's answer for a clause cell depends on its physical plan, other than Q1's
  `NOT IN`. That would need a truth ruling.

## 8. Cost gates

**Bars** (ruling 2026-11-11 Q4): **1.05x with no float keys** and **1.20x with float keys**.

**Base and head:**
- the base is a release build of `04d84ab9` (PR A's merged head);
- the head is the part 1 head;
- `ae247767` is reported beside the base as a diagnostic (default, Q4).

**Method:**
- like-for-like medians, interleaved per run (base, head, base, head …);
- a fresh process and warehouse each run;
- at least five runs;
- inside `flock -x /tmp/oc-worker/build-slots/measure.lock` around the build wrapper, with nothing else running.

| Gate | Workload | Bar |
|---|---|---|
| **N1** (no float keys, planning) | 200 `EXPLAIN`s after 10 warm-ups of a wide plan: 300 non-float columns plus float columns used only as values, with `GROUP BY`, a join, a window and `ORDER BY` on integer and string keys. This is part A's case (b) method; it measures seat 2's walk and its fast exit | ≤ 1.05 |
| **N2** (no float keys, end to end) | the C-012 set (PERF-1's like-for-like set), and part A's case (a) statements | ≤ 1.05 |
| **F1** (float keys, slow path) | E5's shape: 10M rows, a quarter `-0.0`. `GROUP BY d`; `JOIN … ON d` with `count(*)` (record the count); `count(*) OVER (PARTITION BY d)`; `SELECT DISTINCT d`; `ORDER BY d LIMIT 10`; on the Spark door and on native | ≤ 1.20 |
| **F2** (float keys, fast path) | the same five on 10M rows with no `-0.0` and canonical NaN only. Expected near 1.0. It gates at the same bar | ≤ 1.20 |

- A native-only F1/F2 failure takes the carve-out (design point 5). It is recorded in the evidence, and the native
  rows of P5 become declared totalOrder rows.
- A Spark-door failure is a HALT.
- Lost hash-join dynamic-filter pruning and lost sort elision on float keys are reported, with the probe-side rows
  scanned for F1's join.

## 9. Part 2 (a later order): nested-float write conditions

Part 2 gets its own order after part 1 merges, on the same lane (ruling 2026-11-11 Q3, Q4).

**Scope:** floats nested inside struct, array and map values, where they decide which rows a write touches:
- `DELETE` and `UPDATE` `WHERE` conditions;
- MERGE `ON` and `WHEN` conditions;
- `INSERT … SELECT` filters and joins over such values.

These block the label. The design would extend the normalisation through a `key_by_position`-style recursion
(`repark-functions/src/collection/key_by_position.rs:243`) into nested comparisons and keys.

**Bar:** the 1.05x / 1.20x pair against part 1's merged head.

**Out of part 2:** nested-float SELECT answers (Fable's N01–N12) are a card, `NESTED-FLOAT-KEY-1`, not this unit. So are
the map-key classes N07 (GROUP BY on a map refuses) and N08 (a map constructor with `-0.0` and `0.0` keys).
