# STRUCT-COMPARE review memo: PR #1292, F-1 and the join-key question (2026-11-10)

**From:** the frontier planning agent (Claude Fable 5.2, `claude-fable-5-2`). Guidance only; I built nothing and
changed nothing in any repository or clone.
**For:** the Opus 5.5 orchestrator (`claude-opus-5-5`) and the owner.
**Answers:** `Handoff — STRUCT-COMPARE-BY-POSITION-1 review and the join-key question 2026-11-10.md`, §7.
**Status of every recommendation below:** a recommendation. None is a ruling until the owner says so.

---

## 0. The short version

1. **F-1 is one member of a family, and the family is larger than the handoff lists.** I measured 120 cells on
   Spark 4.1.2, the head build, main and the released 1.5.6 wheel. Besides key-list joins and `nullif`, two more
   forms still pair struct fields by name and select the wrong rows: **`BETWEEN`** and the **simple `CASE x WHEN y`**.
   A `DELETE` through each of `BETWEEN`, `nullif` and simple `CASE` removed the wrong row on head, on main and on
   1.5.6 (§1.3).
2. **My recommendation for Q1 is B, widened.** Merge #1292 as verified, after a records-only commit. Open one new
   unit for the whole family (key-list joins, `BETWEEN`, simple `CASE`, `nullif`), table first, blocking v1.5.7.
   **This differs from the orchestrator's default, which is A.** The owner should answer Q1 by letter, because
   "defaults" would be ambiguous between us. The reasons are in §2.
3. **The join-key premise in my brief was wrong, and I confirmed it in the source.** DataFusion 54.1 hands each
   join key to the rule on its own, never as an equality. The fix is a join arm in `align_node`, with four
   conditions the builder must not miss (§3): skip set-operation joins, refuse a `FULL` key-list join whose key
   names differ, stop marking a rewritten join as `USING`, and stay out of the shown-key problem that PR #1000
   already failed on three times.
4. **The inventory rule should gain a row**, and the row should be read from the engine source, not from a list of
   spellings someone thought of (§5).
5. **Inventory row 7 is wrong because its probe used the wrong spelling**, `PREPARE … FROM`. `PREPARE … AS` is
   accepted. This does not weaken unit C's reasoning (§7).

Everything here reproduces on the released 1.5.6 wheel: 1.5.6 and main agree on 117 of the 120 cells. The three
that differ are unit C's refusals of a reordered struct under `INTERSECT`, `EXCEPT` and `UNION`, which merged after
the release; 1.5.6 answers those three.

Three Sonnet agents checked this memo on 2026-11-10 at the owner's request; §12 has what they found and what I
corrected.

---

## 1. What I measured

### 1.1 Method

- **Legs:** Spark 4.1.2 (the oracle venv, under the Spark lock); the head release build in
  `lane-empty-dml-guard-1` (`647fd64a`); main, from the Opus verifier's `base-site`; and the PyPI 1.5.6 wheel.
- **Fixture:** temporary views. `lk(lid, s STRUCT<a,b>)` holds `(1,{1,2})`, `(2,{2,1})`, `(3,NULL)`.
  `rk(rid, s STRUCT<b,a>)` holds `(10,{b:1,a:2})`, `(30,{b:7,a:7})`, `(40,NULL)`. By position, `lid` 1 matches
  `rid` 10. By name, `lid` 2 does.
- **Control:** the six `JOIN … ON lk.s = rk.s` cells are EQUAL on head and wrong on main, as the handoff says. So
  the head build I used behaves as the reviewed head.
- **Files:** the memo probe folder (not committed): `probe.py`, `dml.py`, `cmp.py`, the four raw
  legs, and `cells.txt` with every cell's Spark and head answer.

### 1.2 Joins on a key list (the F-1 family)

"Key list" means `USING (…)`, `NATURAL JOIN`, and DataFrame `join(other, "s")` or `join(other, ["s"])`.

| Shape | Spark | Head (= main = 1.5.6) |
|---|---|---|
| inner, left, semi, anti; SQL and DataFrame | pairs by position | pairs by name: wrong rows |
| right | pairs by position; the shown key is the **right** key and carries the right side's field names (`struct<b:int,a:int>`) | pairs by name; shows the left key, `NULL` on right-only rows |
| **full**, reordered names (`USING`, `NATURAL`, DataFrame) | **refuses**: `DATATYPE_MISMATCH.DATA_DIFF_TYPES` on `coalesce(s, s)` | answers |
| reordered and widened key (`STRUCT<b BIGINT, a INT>`) | refuses: `BINARY_OP_DIFF_TYPES` on `(s = s)` | answers by name |
| unrelated names (`STRUCT<x,y>`) | answers by position | refuses ("Unsupported CAST") |
| two struct keys; an `INT` key with a struct key; `ARRAY<STRUCT>` key | by position on every key | by name |
| three tables chained by `USING`; DataFrame join then join again | answers one row | returns no rows |
| a view over a `USING` join; a frame built on a `USING` join, then `select`, `filter` or `cache` | by position | by name, no error |
| `INTERSECT`, `EXCEPT` with a reordered struct | refuses | head and main refuse (unit C); **1.5.6 answers**, since unit C merged after the release |

Three things in that table are **not** part of F-1, and all three reproduce with a plain `INT` key:

- **Right and full joins show the left key.** `SELECT * FROM li RIGHT JOIN ri USING (k)` gives `k = NULL` on the
  right-only row where Spark gives 3. This is known and open: PR #1000 (`fix/using-per-side-keys-1`, open, last
  updated 2026-10-08), the design card `task/roadmap/mid-term/using-side-key-design-1-card-2026-10-08.md` ("open …
  Not scheduled"), and `task/ledgers/staging/attr-id-1-ledger.md:1135`. The card records three failed verifier
  rounds on #1000.
- **`WHERE k = 1` after `USING (k)` refuses** with `AMBIGUOUS_REFERENCE` where Spark answers. `GROUP BY k` and
  `SELECT k` answer.
- **`SELECT *` column order.** Spark puts the key columns first (`k, lid, rid`). RePark keeps the left table's
  order (`lid, k, rid`). I did not find this recorded in the files I searched; it belongs on the same design card.

### 1.3 Comparisons that are not a comparison expression

`A` is `named_struct('a',1,'b',2)`, `B` is `named_struct('b',1,'a',2)` (equal to `A` by position), `C` is
`named_struct('b',2,'a',1)` (equal to `A` by name).

| Construct | Spark | Head (= main = 1.5.6) | Label |
|---|---|---|---|
| `nullif(A, B)`; column form | `NULL`; row 1 | returns `A`; row 2 | **WRONG** |
| `CASE A WHEN B …`; column form | `hit`; row 1 | `miss`; row 2 | **WRONG** |
| `A BETWEEN B AND B`; column form | `true`; row 1 | `false`; row 2 | **WRONG** |
| `greatest(A, B)`, `least(A, B)` | refuses, `DATA_DIFF_TYPES` | answers by name | accepted where Spark refuses |
| `array_position`, `arrays_overlap`, `array_union`, `array_intersect`, `array_except` | refuses, `ARRAY_FUNCTION_DIFF_TYPES` or `BINARY_ARRAY_DIFF_TYPES` | answers | accepted where Spark refuses |
| `coalesce`, `if`, `CASE … THEN A ELSE B`, `array(A, B)`, `VALUES (A), (B)` | refuses | answers | accepted where Spark refuses (the D4 card) |
| `array_contains`, `array_append`, `array_prepend`, `array_insert`, `concat` of arrays | refuses | refuses, same class | both refuse |
| `array_remove`, `nvl`, `nvl2` | refuses | refuses, another class or an internal text | both refuse, class differs |
| `map(A,1,B,2)`, `map_concat` | refuses | no refusal; reading the result fails with Python's `TypeError: unhashable type: 'dict'` | not a clean refusal (see the note below) |
| `map(A, 1)[B]` | 1 | 1 | EQUAL |
| `lid … WHERE s = ANY (SELECT s FROM rk)` | `PARSE_SYNTAX_ERROR` | answers, by name | accepted where Spark refuses |
| a comparison inside `FILTER (WHERE …)`, a window `CASE WHEN`, `ORDER BY`, `GROUP BY` | answers | same | EQUAL |
| a comparison inside a lambda (`filter(array(A), x -> x = B)`) | keeps the element | head keeps it; **main and 1.5.6 drop it** | EQUAL on head only; #1292 fixed it |
| `equal_null`, four-argument `decode`, `map_contains_key`, `max_by` | answers or refuses | routine not implemented | function gaps |

**Note on the two map cells.** The error is a Python one, not an engine refusal, so RePark planned and ran both
statements. My reading, not a measurement: the facade builds a Python `dict` whose keys are structs, and a struct
arrives as an unhashable `dict`. If so, any map with a struct key cannot be collected, whatever the field names.
That deserves its own cell and card.

**Write reach, measured.** On a local Iceberg table `t(id, s STRUCT<a,b>)` with rows `(1,{1,2})` and `(2,{2,1})`:

| `DELETE FROM t WHERE …` | Spark's `SELECT` twin selects | Head deletes | Main and 1.5.6 delete |
|---|---|---|---|
| `s BETWEEN B AND B` | id 1 | **id 2** | id 2 |
| `nullif(s, B) IS NULL` | id 1 | **id 2** | id 2 |
| `CASE s WHEN B THEN true ELSE false END` | id 1 | **id 2** | id 2 |
| `s = B` (control) | id 1 | id 1 | id 2 |

The oracle here is the derived one the owner ruled on 2026-11-09 (the rows the `SELECT` twin selects); I did not
run Spark's own `DELETE`. The raw output is `dml-results.txt` in the probe folder. I measured copy-on-write on format v2 only.

**Why these three escape the rule.** The rule looks for a comparison `BinaryExpr`, an `IN` list, an `IN (subquery)`
and `element_at` (`struct_position.rs:211-230`, `:267-304`). DataFusion's `type_coercion` has separate arms that
call `comparison_coercion` for `Between` (`type_coercion.rs:625-647`), for `Case` (`:694`, `coerce_case_expression`
at `:1026`) and for `SetComparison` (`:511-531`), and `nullif` declares `Signature::comparable(2, …)`
(`datafusion-functions-54.1.0/src/core/nullif.rs:82`).

---

## 2. Question 1: A or B for F-1

### 2.1 Recommendation: B, with the follow-up unit covering the whole family

1. **A records-only commit on #1292**, then the owner's merge word. Product code stays at the verified `647fd64a`.
2. **One new unit**, say `STRUCT-COMPARE-SEAT-FAMILY-1`, rated S1 with write reach, **blocking v1.5.7**. Its clause
   is §4's "in the unit" rows: key-list joins, `BETWEEN`, simple `CASE` and `nullif`.
3. **C-116 stays open** until that unit merges. Its row is narrowed in the records commit: closed for the comparison
   operators, `IN` list, `IN (subquery)` and `ON`-condition joins; open for the four forms above.

### 2.2 Why B

- **The work is a unit, not a delta.** A delta fixes findings against a table that exists. This fix has no table
  yet, and it carries four design decisions (§3.3) plus three new expression arms. That is the size of #1290, which
  ran as its own unit and merged in a day.
- **One build either way.** `BETWEEN`, simple `CASE` and `nullif` are S1 with a measured wrong `DELETE`, so they
  must be fixed before v1.5.7 whatever happens to F-1. They live in the same file and use the same rename. Option A
  as the handoff frames it (joins and `nullif` in the delta) would still leave a second build for the other two.
- **B caps the downside.** #1292 holds 1,291 corrected cells and a clean double verification. If a family-sized
  delta fails its re-check, the cap parks the whole pull request again, as with #1267 and #1283. Under B a failure
  parks only the small unit.
- **B is what the adopted rules say.** F-1 is pre-existing (head = main = 1.5.6, measured) and outside the three
  tables. The scoped-review rule says such a finding is carded, blocks the release if S1, and "never opens a
  round". The R7 amendment allows the merge when its three conditions hold, and all three can be met here.
- **The release claim is the same.** v1.5.7 is not tagged with the blocker open under either option.

### 2.3 The case for A, fairly

- The order's design point 6 binds my brief's §4.5.4, which names `a.join(b, 'k')`. That seat was contracted and
  not delivered. Calling it "outside the clause" because the table omitted it rewards the omission.
- One pull request would then close C-116 in one step.

If the owner prefers A, I recommend two conditions: the delta is the **whole family** of §4, table first; and a
fallback is agreed now, that a failed delta re-check splits the delta onto its own pull request and #1292 merges
at `647fd64a`.

### 2.4 "The order named the seat, the table missed it": should that be class (b)?

**Recommendation: yes, from the next unit on, as two short rules.** I recommend the owner does not apply them
backwards to #1292, for the reasons in §2.2.

> **Table-completeness check.** Before a build opens, the orchestrator lists every seat and door the order names
> and marks each against the step-0 table's seat axis. A named seat with no cells is added to the table, or struck
> from the order in writing. A verifier's first step repeats the check.
>
> **Named-seat rule.** A seat the order names is inside the clause whether or not the table carries it. A wrong
> cell on a named seat found at review is class (b) for that seat and is fixed in the unit's one delta. Other
> spellings that share its mechanism but that the order does not name stay class (c).

The first rule is the one that matters. It would have caught this before the build, at the cost of one read.

### 2.5 My part in this

Brief §4.5.4 states that `map_expressions` "visits the `on` pairs as `Eq` binary expressions". That was wrong, and
I wrote it from the source comment, not the code. The comment at `tree_node.rs:562-564` says each expression is
`left-on = right-on`; the code beneath it maps the left and the right key separately. The brief did say "prove it"
and named the seat, but a wrong premise stated with confidence made the proof look optional.

---

## 3. Question 2: the join-key design

### 3.1 The premise, checked

- **`map_expressions` on a join** (`datafusion-expr-54.1.0/src/logical_plan/tree_node.rs:565-586`) runs
  `(on, filter).map_elements(f)`. Each key reaches the closure alone. `apply_expressions` does the same (`:435-437`).
- **`coerce_join`** (`datafusion-optimizer-54.1.0/src/analyzer/type_coercion.rs:192-218`) then calls
  `coerce_binary_op(lhs, left_schema, Operator::Eq, rhs, right_schema)` per pair. That is where the by-name pairing
  happens.
- **Why `ON` works today.** `join_on` reduces its conditions into the join's `filter` (`builder.rs:991-997`), so
  the rule sees an ordinary `Eq`. The optimizer extracts equi-keys later. The orchestrator's reading is right.

### 3.2 Who puts pairs into `Join.on` before the analyzer

| Producer | Where | Notes |
|---|---|---|
| SQL `USING`, `NATURAL` | `datafusion-sql-54.1.0/src/relation/join.rs:130-171` → `join_using`, `builder.rs:1171-1240` | hashable keys go to `on` with `JoinConstraint::Using`; a non-hashable key becomes an `Eq` filter, which the existing arm already sees |
| DataFrame `join(other, "s")` and `["s"]` | `crates/repark-core/src/session/df_guards/case_bind.rs:618-689` | builds `on` pairs with `LogicalPlanBuilder::join` (`:652-659`), then projects the first copy of each key (`:669-684`), which is the left key on every join type |
| `lateral_join` with key names | `crates/repark-python/src/subquery.rs:167-187` | `on` pairs with `JoinConstraint::On` |
| `INTERSECT`, `EXCEPT` | `intersect_or_except`, `builder.rs:1416-1421` | semi and anti joins with `NullEquality::NullEqualsNull` |

`df.join(other, condition)` builds a filter, not `on` pairs (`crates/repark-python/src/dataframe/mod.rs:592-609`).

### 3.3 Recommendation: a join arm in `align_node`, with four conditions

Add an arm for `LogicalPlan::Join` that treats each `(left, right)` key pair as the `=` arm treats its operands,
and renames the right key to the left key's names. I recommend this over rewriting the pairs into an `Eq` filter:
the arm leaves the plan's shape alone, and the head already runs a renamed right key through every join type on
the `ON` path (six EQUAL cells in §1.1).

1. **Skip set-operation joins.** Unit C's rule recognises an `INTERSECT` or `EXCEPT` join by
   `NullEquality::NullEqualsNull` and by each key being a bare column (`struct_setop.rs:125-141`, `:187-189`). A
   renamed right key is no longer a bare column, so the arm would **silently switch off unit C's refusal**. The arm
   acts only on `NullEqualsNothing` joins. Unit C's pins and my two `J-setop` cells guard it.
2. **Type each key against its own side.** The left key reads `join.left.schema()` and the right key
   `join.right.schema()`, as `coerce_join` does. `align_node` merges the input schemas today
   (`struct_position.rs:196-199`), and a key list uses the same unqualified name on both sides.
3. **A rewritten `USING` join stops being marked `USING`.** `LogicalPlan::using_columns` raises an internal error
   on any key that is not a column (`plan.rs:499-511`), and `get_as_join_column` sees through one `Cast` only
   (`expr.rs:2035-2044`). Setting `join_constraint` to `On` on a join the arm rewrote avoids that. The only other
   readers of the constraint are the builder, the SQL planner and the unparser. The cells that prove it are the
   "frame built on a `USING` join" and view cells of §1.2, which answer without error today.
4. **A `FULL` key-list join whose key names differ refuses** with Spark's `DATA_DIFF_TYPES` on `coalesce(s, s)`.
   Spark refuses on all three doors (measured), and the unit's own rule is that an acceptance cannot be listed as
   known. This is a refusal only. It builds no `coalesce`, so it does not reopen D4.

Smaller points for the brief:

- A clash (reordered and widened, or different arity) refuses with `BINARY_OP_DIFF_TYPES`, rendered as Spark does:
  `(s = s)`.
- **Duplicate-named keys** must be paired positionally on both sides or refused. They must never reach
  `struct_coercion`, whose debug assertion the handoff reports at `binary.rs:1326`. Measure in the ci profile.
- **`join_using` can store a pair swapped.** When only the right side's key type is hashable, it pushes
  `(right column, left column)` (`builder.rs:1202-1212`). The arm must not assume the first key of a pair belongs
  to the left input: a key that does not resolve against its own side is tried against the other, or the pair is
  left alone. When no key is hashable there is no key-list join at all, only a cross join under an `Eq` filter
  (`builder.rs:1226-1230`), which the existing arm already sees. (Added after the source check of §12.)
- `has_struct_candidate` must learn about joins, and cheaply: a key whose type holds a struct. Re-run the cost
  table with a join-heavy query.
- `rewire_join_right` (`df_guards/subquery.rs:160-171`) and the attribute-id strip (`df_guards/attr_id.rs:708-727`)
  pass `on` through; check that each tolerates a renamed key if it runs after the seat.

### 3.4 Output semantics

- **Inner, left, semi, anti.** Spark shows the left key with the left side's names. RePark already does, because
  the arm never touches the left key. Measured schema on both: `s:struct<a:int,b:int>`.
- **Right.** Spark shows the right key with the **right** side's names. RePark shows the left key. That is PR
  #1000's defect and it exists for `INT` keys. **Keep it out of this unit.** The design card records three failed
  rounds there. In the table, prove pairing with cells that project `lid, rid`, and label the star and bare-key
  cells against their `INT` twin (kind invariance), pointing at the card.
- **Full.** See condition 4. With equal names it behaves as its `INT` twin (PR #1000 again).
- **`NATURAL` over several struct columns.** Spark pairs each by position and lists them first in left order
  (`J-natural-two`, `J-natural-mixed`). The arm handles each pair independently.

### 3.5 Cells to measure before the build (I did not)

- A key-list join in case-sensitive mode, and with names that differ only by case.
- A reorder one level down (`STRUCT<p STRUCT<a,b>>` against `STRUCT<p STRUCT<b,a>>`), and inside a map value.
- A `MAP` key column in `USING`. Spark's answer decides whether `map_ordering::refuse_in_expr` applies to keys.
- `INSERT … SELECT` over a key-list join on v2 copy-on-write, v2 merge-on-read and v3 (the write-reach cell).
- `MERGE … USING src ON` where the source is itself a key-list join.
- Every list kind of the type-variant inventory as a key.

---

## 4. Question 3: the comparison-seat inventory

The reliable way to list the family is from the source: every caller of `comparison_coercion` in
`type_coercion.rs`, every function with a comparable signature, and every plan node that stores operand pairs
outside an expression. For 54.1 that gives the rows below.

| Construct | Measured on head | In C-116's clause? | Where it goes |
|---|---|---|---|
| `Join.on` key lists (`USING`, `NATURAL`, DataFrame key names, lateral key names) | by name, wrong rows | yes: C-116's text says "join key" | **the unit**, rewrite (§3) |
| `BETWEEN` (`Expr::Between`) | by name, wrong `DELETE` | yes: Spark defines it as `>=` and `<=` | **the unit**, rewrite |
| simple `CASE x WHEN y` (`Expr::Case` with an operand) | by name, wrong `DELETE` | yes: it is `x = y` | **the unit**, rewrite |
| `nullif` | by name, wrong `DELETE` | yes: it is `a = b` | **the unit**, rewrite (§6) |
| `= ANY (subquery)` (`Expr::SetComparison`) | accepted and by name; Spark has no such syntax | no | card: the door accepts non-Spark syntax, same class as `PREPARE` |
| `greatest`, `least` | accepted by name; Spark refuses | no: this is type unification | the D4 card, with these rows added |
| `array_position`, `arrays_overlap`, `array_union`, `array_intersect`, `array_except` | accepted; Spark refuses | no | the D4 card |
| `coalesce`, `if`, `CASE` branches, `array(…)`, `VALUES` rows | accepted; Spark refuses | no (D4, already ruled) | the D4 card |
| `array_contains`, `array_append`, `array_prepend`, `array_insert`, `concat` of arrays | both refuse | — | nothing |
| `array_remove`, `nvl`, `nvl2` | both refuse; RePark's class or text differs | — | one S3 card for the refusal text |
| `map(…)` constructor, `map_concat` with struct keys | Spark refuses; RePark runs, then the result cannot be read (§1.3 note) | no | the D4 card for the acceptance; a separate card for reading a struct-keyed map |
| `element_at`, map subscript | handled by the unit | yes | done |
| comparisons inside a lambda, `FILTER (WHERE …)`, a window, `ORDER BY`, `GROUP BY` | EQUAL | yes | done; keep as control cells |
| `MERGE ON`, `WHEN MATCHED AND` | EQUAL in the committed tables | yes | done |
| `UNION`, `INTERSECT`, `EXCEPT`, recursive CTE terms | refused by unit C; Spark answers `UNION` by position | no | `STRUCT-SETOP-BY-POSITION-1`, as planned |
| `equal_null`, four-argument `decode`, `map_contains_key`, `max_by` | not implemented | — | function-gap cards; `equal_null` and `decode` are comparison seats the day they are built |
| `PIVOT … FOR x IN (…)` | not supported by the door | — | a comparison seat the day it is built |
| `DISTINCT ON` | not Spark syntax; keys of one relation, no pair | — | nothing |
| `LATERAL … USING` | accepted; Spark refuses `UNSUPPORTED_FEATURE.LATERAL_JOIN_USING` | no | S3 card |

**On the D4 rows.** They return a by-name value, or a boolean, where Spark raises. The owner carded D4 on
2026-11-09 and I am not reopening that. I recommend the card gains the measured rows above, because the earlier
list (CASE, IF, coalesce, array) is about half of what the engine accepts.

---

## 5. Question 4: the insertion-inventory rule

**Recommendation: yes, add a standing row, and make it source-driven.**

> **Consumers of the pairing.** Every brief for a rule that pre-empts an engine coercion lists, from the pinned
> source and before the build: (1) every caller of that coercion in the analyzer (for DataFusion 54.1 comparison
> coercion: the `BinaryExpr`, `InList`, `InSubquery`, `SetComparison`, `Between` and `Case` arms and
> `coerce_join`); (2) every function whose signature reaches it; (3) every plan node that stores operand pairs
> outside an expression (`Join.on`, set-operation inputs, recursive terms). Each entry is a seat in the step-0
> table or is named out of clause with its card.

The existing inventory asks where evidence is created, moved or destroyed **before** the seat. This row asks who
**consumes** the pairing after it. Both parks this week came from the second question, answered from memory.

One more lesson belongs with it, from §7: **a claim that a door does not reach something needs more than one
spelling.** Row 7's absence claim rested on a single statement that was a syntax error for an unrelated reason.

---

## 6. Question 5: `nullif`

**Recommendation: in the family unit (or in the delta, if the owner picks A). Rename the second argument; do not
rewrite to `CASE`.**

- `nullif` returns its first argument's type, so renaming the second argument to the first's names cannot change
  the output schema.
- A rewrite to `CASE WHEN a = b` would change the generated column name and evaluate `a` twice.
- The renamed argument will show in generated names on the `selectExpr` door exactly as F-2 shows today. Fix F-2
  in the same unit, so one name-preservation fix covers every arm.
- **`BETWEEN` and simple `CASE`** can reuse the `=` arm by expanding only when a struct pair is present:
  `BETWEEN` into `>=` and `<=`, as Spark defines it, and each `WHEN` operand renamed to the operand's names. A
  plan with no struct pair is left untouched, which keeps the cost gate where it is.
- **Not measured:** Spark's refusal class for each of the three when the pair is reordered and widened, or differs
  in arity. I expect `BINARY_OP_DIFF_TYPES`, since Spark defines all three through `=`, `>=` and `<=`; the table
  must say.

---

## 7. Question 6: the `PREPARE` claim

- **Why row 7 was wrong.** The step-0 probe (cell 04376) is `PREPARE s1 FROM SELECT …`. That spelling is a syntax
  error on both engines. The Opus verifier used `PREPARE p1 AS SELECT 1`, which RePark accepts and Spark refuses.
- **Unit C.** Its belt reasoning stands. The belt exists because the reach exists, and this measurement confirms
  the reach on the Spark door. `EXECUTE` pairs by position on head, per the verifier's cells.
- **This unit.** Correct row 7 to "yes, reached; `EXECUTE` plans through the session analyzer; measured by
  position". That edit is documentation and can ride the records commit.
- **The family unit.** Give every new seat a `PREPARE … AS` and `EXECUTE` cell. Row 7's original worry was that
  `PREPARE` runs the optimizer alone, and a join arm is a new place for that to matter.
- **A card.** The Spark door accepts `PREPARE` and `EXECUTE` where Spark gives `PARSE_SYNTAX_ERROR`. Whether that
  is a kept extension or a refusal is the owner's to rule, at S3.

---

## 8. Question 7: ordering

**Under B (recommended):**

1. **Records commit on #1292, documentation only.**
   - The pull request text and the ledger state the claim as the three measured tables, and name the open family.
   - C-116 narrowed as in §2.1. A new ledger row and card for the family: S1, write reach, blocks v1.5.7, with
     §1.2 and §1.3 as its measured table.
   - Inventory row 7 corrected. F-6's lost `map.md` facts restored.
   - No test and no product change, so the verdict on `647fd64a` stands. This follows the #1283 precedent, where
     the F-K pin was kept out of the records commit for the same reason.
2. **The owner's merge word, then the merge.**
3. **Clerk work, alongside:** the family unit's step-0 table. Start from my 120 cells and add the axes of §3.5,
   the three doors, the six join types, both case settings, the DML seats on v2 copy-on-write, v2 merge-on-read
   and v3, `PREPARE … AS`, and the ci profile.
4. **The family unit:** one Opus 5.5 build, then Opus 5.5 and Muse MAX verifiers in parallel, one delta, one
   re-check. Its brief carries both inventories and the new row of §5. Its scope also takes the leftover items of
   #1292's review: F-2, F-4 and A-1 (product), F-7 and O-4 (tests). F-3 is carded.
5. **Records clerk, then `SIGNED-ZERO-NORMALIZE-1`**, and the rest of the planned order unchanged.

**Under A:** the same steps 3 and 4, done as the delta on #1292, table first, with the fallback of §2.3 agreed
beforehand. Step 1 happens after the delta instead of before the merge.

---

## 9. Decisions for the owner

| # | Decision | My recommendation | The orchestrator's default |
|---|---|---|---|
| Q1 | F-1: fix in #1292 (A), or merge and open a blocking unit (B) | **B**, with the unit covering key-list joins, `BETWEEN`, simple `CASE` and `nullif` | A |
| Q2 | Where the minor review items go | F-6 in the records commit; F-2, F-4, A-1, F-7, O-4 in the family unit; F-3 carded | fold all but F-3 into the delta |
| Q3 | Adopt the table-completeness check and the named-seat rule (§2.4), from the next unit on | yes | not asked |
| Q4 | Adopt the "consumers of the pairing" inventory row (§5) | yes | asked, no default given |
| Q5 | A `FULL` key-list join with reordered struct names refuses as Spark does (§3.3, condition 4) | yes | not asked |
| Q6 | Right and full shown-key behaviour stays with PR #1000's design card, out of this work | yes | not asked |
| Q7 | Extend the D4 card with the measured acceptances of §4; D4 itself unchanged | yes | not asked |

**Q1 needs a letter.** My default and the orchestrator's differ, so "defaults" alone would not settle it. For Q2 to
Q7, my default is my recommendation.

---

## 10. What I did not do

- **No Glue, no S3, no Iceberg on the Spark leg.** The join and function cells ran on temporary views. The `DELETE`
  cells ran on a local Iceberg table on the RePark legs only.
- **No merge-on-read, no v3, no `UPDATE` or `MERGE`** for the three new wrong-write forms.
- **No `INSERT … SELECT`** over a key-list join. The handoff's write-reach statement for F-1 is unmeasured by me.
- **No ci-profile run**, so nothing here speaks to debug assertions.
- **`PIVOT`:** RePark does not support the statement, and my Spark cell failed at run time for a reason I did not
  chase. It is not a measurement.
- **The native door** is untouched by any of this and still pairs by name by design; I ran no native cells.
- **The design in §3.3 is a reading of the source plus measured behaviour, not a prototype.** Condition 3 in
  particular should be confirmed by the builder's first cell, not taken from me.

---

## 11. Evidence

- **My probe:** the memo probe folder (not committed).
- **DataFusion 54.1.0, local cargo registry:**
  - `datafusion-expr-54.1.0/src/logical_plan/tree_node.rs:435-437`, `:562-586`
  - `datafusion-expr-54.1.0/src/logical_plan/builder.rs:991-997`, `:1171-1240`, `:1416-1421`
  - `datafusion-expr-54.1.0/src/logical_plan/plan.rs:489-522`
  - `datafusion-expr-54.1.0/src/expr.rs:2035-2044`
  - `datafusion-optimizer-54.1.0/src/analyzer/type_coercion.rs:192-218`, `:511-531`, `:625-647`, `:694`, `:1026`
  - `datafusion-sql-54.1.0/src/relation/join.rs:130-171`
  - `datafusion-functions-54.1.0/src/core/nullif.rs:82`, `greatest.rs:69`
- **The branch at `647fd64a`, read in the build clone:**
  - `crates/repark-spark/src/normalize/struct_position.rs:20-369`
  - `crates/repark-core/src/session/df_guards/case_bind.rs:618-689`
  - `crates/repark-core/src/session/df_guards/struct_setop.rs:120-160`, `:187-189`
  - `crates/repark-core/src/session/df_guards/subquery.rs:160-171`, `attr_id.rs:708-727`
  - `crates/repark-python/src/subquery.rs:160-195`
  - `crates/repark-python/src/dataframe/mod.rs:592` and `:609` (search hits only; I did not read the function)
  - `task/wo/struct-compare-by-position-1/evidence/inventory.md`; `evidence/step0/table.md:4760`
  - `task/ledgers/staging/attr-id-1-ledger.md:1135`
  - `task/roadmap/mid-term/using-side-key-design-1-card-2026-10-08.md` (lines 5, 10 and 28-49 only)
- **Read in full:** the handoff, and the Opus verdict `verify-cmp-opus-out/verdict.md`. I did not read the Muse
  verdict; I took its findings from the handoff.

---

## 12. Verification by three Sonnet agents (2026-11-10)

The owner asked for a quick check. Each agent was told to try to refute the memo. None of them re-ran `dml.py`.

| Agent | What it did | Result |
|---|---|---|
| Source reader | opened the DataFusion 54.1 and RePark files behind ten claims of §1.3 and §3 | nine confirmed; one confirmed with a refinement |
| Independent measurer | wrote its own fixture with other values; ran seven behaviours on Spark, head and 1.5.6 | seven confirmed |
| Auditor | compared §1's tables with the four raw legs; checked the record citations | two wrong cells and one loose phrase, all about which builds a column described |

**What changed in this memo as a result:**

- **§3.3 gained a point:** `join_using` can store a key pair swapped. The source reader found it.
- **§1.3, the lambda row:** the comparison inside a lambda is right on head only. Main and 1.5.6 get it wrong; I
  had put it under a column headed "head = main = 1.5.6".
- **§1.2, the set-operation row:** 1.5.6 answers; only head and main refuse.
- **§0:** the three cells where 1.5.6 and main differ include a `UNION`, so "set-operation refusals" now names all
  three.

**What the checks confirmed without change:** the join-key premise; where the by-name pairing happens; that a
renamed key would make unit C's rule stop refusing; that the rule has no arm for `BETWEEN`, simple `CASE`,
`nullif` or join keys; the wrong rows on key-list joins, `BETWEEN`, simple `CASE` and `nullif` on head and 1.5.6;
Spark's refusal of the `FULL` key-list join; the `INT`-key right join; PR #1000, the design card and its three
failed verdicts; the `PREPARE … FROM` spelling of cell 04376; and the 1,291 count.

**Still resting on my measurement alone:** the `DELETE` table of §1.3 (re-run once more, same result, raw output
saved); the `SELECT *` column order after `USING`, because the measurer's key column happened to be first; and
the cells the measurer did not repeat, namely `greatest`, `least`, the array functions, `= ANY` and the map cells.
The auditor checked those against my raw files, which tests my transcription, not the measurement.

---

## 13. Paste-ready prompt

> Read `<frontier-folder>/STRUCT-COMPARE review memo 2026-11-10.md` in full. It recommends
> B for Q1 and differs from your default, so put Q1 to the owner by letter with both positions, and Q2 to Q7 of its
> §9 beside it. Start nothing on #1292 until the owner answers. Whatever the answer, the family (key-list joins,
> `BETWEEN`, simple `CASE`, `nullif`) needs a measured step-0 table before any build; the memo's §1 and its probe
> folder are the starting cells, and §3.3 lists four conditions the join arm must meet.
