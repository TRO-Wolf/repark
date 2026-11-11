# STRUCT-COMPARE-SEAT-FAMILY-1: key-list joins, BETWEEN, simple CASE and nullif pair struct fields by position

**Grade:** B (guided). **Builder:** Claude Opus 5.5 (`claude-opus-5-5`).
**Review:** Opus 5.5 and Muse MAX in parallel, on the same head and the same table (R7, design-heavy). The cap is one
build, one critique, one delta and one delta re-check. The scoped-review rule of 2026-11-09 binds, and so does the
named-seat rule of 2026-11-10.
**Closes:** ledger C-118 (S1, write reach). With it, C-116 closes. Card:
[struct-compare-seat-family-1-card-2026-11-10.md](../../roadmap/mid-term/struct-compare-seat-family-1-card-2026-11-10.md).
**Release:** blocks v1.5.7.

**Design source.** The frontier review memo of 2026-11-10, copied as
[evidence/frontier-review-memo-2026-11-10.md](evidence/frontier-review-memo-2026-11-10.md). Its §3.3, §3.4 and §6
apply **as amended below**. Where this order and the memo disagree, this order governs.

The base is #1292's rule, `struct_operands_by_position` (`crates/repark-spark/src/normalize/struct_position.rs`),
merged as `68f70b0f`.

## Rulings this order carries (owner, verbatim)

- **"on 1 lets go with Fables recommendation"** (2026-11-10): option B. #1292 merged as verified, and this unit
  carries the whole family, table first, blocking v1.5.7.
- **"Q2–Q7 as pasted but they have my full go ahead and sign off, merge when CI is green"** (2026-11-10):
  - Q2: #1292's F-2, F-4, A-1, F-7 and O-4 are in this unit. F-3 is carded.
  - Q3: the table-completeness check and the named-seat rule bind from this unit on. The check is below.
  - Q4: the "consumers of the pairing" inventory row binds.
  - Q5: a `FULL` key-list join with reordered struct names refuses with `DATA_DIFF_TYPES`, as Spark does.
  - Q6: the right and full shown-key behaviour stays with the PR #1000 design card, out of this unit.
  - Q7: D4's acceptances (`greatest`, `least`, the array functions, `map(…)`) stay on the D4 card, out of this unit.
- **Still binding:**
  - **"default"** (2026-11-09): a struct-literal `DELETE` that Spark 4.1.2 with Iceberg 1.11.0 cannot run is held to
    the derived oracle.
  - **"defaults"** (2026-11-09): D3 and D4, D5, the shared walker with a refusing fallthrough, and the type-variant
    inventory.
  - The insertion-inventory rule.
  - The scoped-review rule.
  - R7.

## Clause: the measured table

The clause is the family step-0 table, measured on 2026-11-10:
- 2274 cells per leg, on Spark 4.1.2, on #1292's head (release) and on main `b64a5326`;
- the three doors (SQL, typed DataFrame, string DataFrame);
- six join types;
- both case settings;
- `PREPARE … AS` / `EXECUTE`;
- DML on v2 copy-on-write, v2 merge-on-read and v3.

It reproduces the memo's 120 cells. Build item 1 commits it under `evidence/step0/`, with the additions below.

**Wrong on head before this unit:**

| Seat | Wrong cells |
|---|---|
| Key-list joins | 280 |
| `nullif` | 104 |
| Simple `CASE` | 53 |
| `BETWEEN` / `NOT BETWEEN` | 48 |

Per format, the DML is wrong in 17 predicate cells, 2 `INSERT … SELECT` cells and 2 `MERGE` cells, plus 1 derived
cell. The typed `Column.between` already pairs by position on head: the typed door builds it as `>=` and `<=`.

### Table-completeness check (Q3; orchestrator, 2026-11-10)

| Named seat | Table group | Cells | Disposition |
|---|---|---|---|
| SQL `JOIN … USING`, six join types | `J-USING` | 270 | carried |
| SQL `NATURAL JOIN`: inner, left, right, full | `J-NATURAL` | 120 | carried |
| SQL `NATURAL` semi and anti | — | 0 | **add** (item 1) if Spark parses them; else one row each recording the refusal |
| DataFrame `join(other, "s")` and `join(other, ["s"])`, six types | `J-DF` | 540 | carried |
| DataFrame `lateralJoin` with key names | `J-LATERAL` | 15 | **struck from the clause**: no wrong cell; RePark answers where Spark refuses, which is card `LATERAL-USING-ACCEPT-1` |
| `BETWEEN`: SQL (select, where, `CASE WHEN`) and string door | `F-BETWEEN` | 102 | carried |
| `BETWEEN`: typed door | `F-BETWEEN` | 34 | carried, as a control: EQUAL on head |
| `NOT BETWEEN`: SQL | `F-NOT_BETWEEN` | 34 | carried |
| `NOT BETWEEN`: string door | — | 0 | **add** (item 1) |
| Simple `CASE`, one and two `WHEN`s: SQL and string door | `F-CASE1`, `F-CASE2` | 170 | carried |
| Simple `CASE`: typed door | — | — | **struck in writing**: the typed API has no simple-`CASE` form |
| `nullif`, both argument orders, three doors | `F-NULLIF_LR`, `F-NULLIF_RL` | 408 | carried |
| List kinds as join keys | `K-KIND` | 128 | carried |
| A `MAP` key column in `USING` (memo §3.5) | — | 0 | **add** (item 1). Spark's answer decides whether `map_ordering::refuse_in_expr` applies to keys |
| `PREPARE … AS` / `EXECUTE`, per seat | `P-PREPARE` | 23 | carried; item 1 adds one `EXECUTE` cell for each seat that has none |
| DML predicates, `INSERT … SELECT` and `MERGE … USING (join)`, three formats | `D-PRED`, `D-INSERT`, `D-MERGE` | 186 | carried |
| Controls: `ON` joins, reordered set operations, `INT`-key twins | `C-ON`, `S-SETOP`, `INT` shape | — | carried |

## Design

The base is #1292's design; the rule, the walker binding and the three seats are unchanged. Add these arms to
`align_node`:

1. **Key-list joins** (memo §3.3). On `LogicalPlan::Join`, treat each `(left, right)` key pair as the `=` arm treats
   its operands, and rename the right key to the left key's names. The arm must meet four conditions:
   1. **Act only on `NullEqualsNothing` joins.** `INTERSECT` and `EXCEPT` joins (`NullEqualsNull`, bare-column keys,
      `struct_setop.rs`) are left alone, so unit C's refusal stays on.
   2. **Type each key against its own side's schema.** `join_using` can store a pair swapped
      (`builder.rs:1202-1212`). Resolve each key against its own side; if neither assignment resolves, leave the pair
      alone.
   3. **A rewritten join's `join_constraint` becomes `On`.** `using_columns` errors on a non-column key. The builder's
      first cell confirms this, and confirms that a DataFrame or view built on a rewritten `USING` join, followed by
      `select("*")`, `.columns` and `filter`, still matches Spark's column list. Halt if it does not.
   4. **A `FULL` key-list join whose key names differ refuses** with `DATA_DIFF_TYPES` on `coalesce(s, s)`, as Spark
      does (Q5). It builds no `coalesce`.

   Further requirements:
   - A clash (reordered and widened, or a different arity) refuses with `BINARY_OP_DIFF_TYPES`.
   - Duplicate-named keys are paired positionally or refused, and never reach `struct_coercion`.
   - `has_struct_candidate` learns about join keys cheaply.
   - `rewire_join_right` and the attribute-id strip must tolerate a renamed key.
2. **`BETWEEN` and `NOT BETWEEN`** (`Expr::Between`). Rename `low` and `high` to the operand's names by position, and
   keep the `Between` node.
   - *Amendment to memo §6:* the memo proposes expanding into `>=` and `<=`, but that changes the generated column
     name of an unaliased `BETWEEN`. Expand only if renaming cannot reach a cell, and say so in the hand-back.
   - A clash refuses with Spark's measured class.
3. **Simple `CASE x WHEN y`** (`Expr::Case` with an operand). Rename each `WHEN` operand to the `CASE` operand's names
   by position. Leave the `THEN` and `ELSE` branches alone: their type unification is D4's.
4. **`nullif(a, b)`.** Rename `b` to `a`'s names by position (memo §6). Do not rewrite it to `CASE`.
5. **Name preservation (#1292's F-2), for every arm.**
   - Generated column names on every door must not show `__repark_struct_by_position__`, and must match Spark's
     wherever base already matched Spark.
   - The cells that prove it are an unaliased `selectExpr`, `F.expr`, a SQL select-list item, and `df.columns` per
     arm.
6. **Review leftovers from #1292:**
   - F-4 and A-1: Spark's class without DataFusion's prefix or schema-error text;
   - F-7: unit C's negative text checks also watch the `Unpaired` arm;
   - O-4: a targeted pin that fails when the `DuplicateFieldOperands` registration is removed, at both sites.
7. **Out of the unit:**
   - the right and full shown key and the `SELECT *` order after `USING` (PR #1000's card, Q6);
   - the native door, which keeps its recorded divergence;
   - `= ANY (subquery)`, `LATERAL … USING` and D4's acceptances, which are carded;
   - `UNION` by position (`STRUCT-SETOP-BY-POSITION-1`).

## Inventories (item 1, before the arms)

- **Insertion inventory** (the 2026-11-09 rule). Re-answer #1292's rows for the new arms. Row 7 is "reached" (#1292
  records).
- **Consumers of the pairing** (Q4), read from the pinned DataFusion 54.1.0 source:
  1. every caller of comparison coercion in `type_coercion.rs`: `BinaryExpr`, `InList`, `InSubquery`,
     `SetComparison`, `Between`, `Case`, `coerce_join`;
  2. every function whose signature reaches it (`Signature::comparable` and kin: `nullif`, `greatest`, `least`, and
     any other);
  3. every plan node that stores operand pairs outside an expression: `Join.on`, set-operation inputs, recursive
     terms.

  Each entry is a seat in the table, or it is named out of clause with its card. A consumer the memo did not list is
  a finding for the hand-back. If it is a wrong answer inside C-116's text, it is a HALT for a ruling.
- **Type-variant inventory** (2026-11-09) for join keys, from the `K-KIND` rows.

## Pins

- A Rust table-driven pin over the table's `sql` cells, both case settings, with DML read-back and the derived-oracle
  `DELETE`s.
- A facade pin for the typed and string doors, the `K-KIND` rows and the controls.
- The `INT`-twin cells for the right and full shown key, pinned as their `INT` twin's outcome.
- `PREPARE … AS` / `EXECUTE` cells.
- Name-preservation cells (design point 5).
- The leftover pins (design point 6).
- **Both profiles:** the facade pin and the full facade gate run in the **ci profile** (debug assertions) and in
  release, with a release no-panic pass. #1292's CI red came from a debug-only assertion.
- Tests go in the **same commit** as the code they pin. Every expected value comes from a table cell.

## Halts

- A cell needing a read beneath more than one cast.
- A base-EQUAL cell newly refused, unless it is a measured S3 over-refusal the builder cards with its cell id.
- Any cell outside the clause that moves, unless it moves to Spark's measured outcome and is named in the evidence.
- Any change to the outcome of #1292's three tables, or of unit C's, #1290's or #1292's pins.
- A `struct_pairing_doors.rs` change.
- Unit C's `INTERSECT` / `EXCEPT` refusal turning off on any cell.
- Condition 3 failing (a `USING`-built frame's column list changes).
- The cost bar exceeded: ≤ 1.05x median of 3 on each case:
  - (a) the table's `sql` statements on the facade;
  - (b) #1292's planning-only probe;
  - (c) a join-heavy planning probe: `EXPLAIN` of a 20-way `USING` join over struct-free keys.
