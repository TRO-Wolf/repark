# Card STRUCT-COMPARE-SEAT-FAMILY-1: key-list joins, BETWEEN, simple CASE and nullif pair struct fields by name where Spark pairs them by position

**Date:** 2026-11-10. **Filed by:** the records clerk (`claude-haiku-5-5`), from the frontier review memo of 2026-11-10 (sections 0 to 3, 6 and 7) and the Opus 5.5 verifier's FAIL on PR #1292 (F-1 and F-6).

**Status:** filed 2026-11-10, ruled 2026-11-10 (owner, verbatim: "on 1 lets go with Fables recommendation"): option B of the frontier review memo of 2026-11-10. Severity S1 with write reach; blocks v1.5.7; not dispatched. Ledger row C-118 in [ice-v2-label-1-ledger.md](../../ledgers/staging/ice-v2-label-1-ledger.md) stays OPEN until this unit merges. Ledger row C-116 is narrowed to the four forms of C-118 and stays open until this unit merges. Pre-existing on 1.5.6 and head.

**Clause:** four forms pair struct fields by name where Spark pairs them by position, on the Spark SQL door, the DataFrame typed door and the DataFrame string doors: key-list joins (`USING`, `NATURAL JOIN`, DataFrame `join(other, "s")` and `join(other, ["s"])`, and lateral key names), `BETWEEN`, simple `CASE x WHEN y`, and `nullif`.

**Why:** A `DELETE` through `BETWEEN`, `nullif` or simple `CASE` removes the wrong row on head, on main and on 1.5.6. Key-list joins return wrong rows on inner, left, right, semi and anti joins, and a `FULL` key-list join answers where Spark refuses. `INSERT ... SELECT` and `MERGE` over a key-list join write wrong rows on v2 copy-on-write, v2 merge-on-read and v3. The oracle is the derived one the owner ruled on 2026-11-09: the rows Spark's `SELECT` with the same predicate selects.

**Measured so far** (the frontier review memo of 2026-11-10, sections 1.2 and 1.3):

- Key-list joins: inner, left, semi and anti joins pair by name where Spark pairs by position. A `FULL` join with reordered names refuses on Spark and answers on head. Right and full shown keys are out of this unit.
- `BETWEEN`, `nullif` and simple `CASE` on a column: head returns the wrong row where Spark answers from the other one. On a local v2 table, `DELETE` through each removes id 2 where Spark's `SELECT` twin selects id 1, on head, main and 1.5.6.

**Step 0:** measured 2026-11-10, 2274 cells per leg on Spark 4.1.2, head and main, across the three doors, six join types, both case settings, `PREPARE ... AS` / `EXECUTE`, and DML on v2 copy-on-write, v2 merge-on-read and v3. It reproduces the memo's cells. On head it finds 280 wrong key-list join cells, 104 for `nullif`, 53 for simple `CASE` and 48 for `BETWEEN` / `NOT BETWEEN`, plus per format 17 predicate, 2 `INSERT ... SELECT` and 2 `MERGE` wrong writes. The typed `Column.between` already pairs by position on head. The unit's build commits the table as evidence, and the order binds it as the clause.

**Design notes from the memo, for the unit's order to bind** (memo sections 3.3, 3.4 and 6):

- A join arm in `align_node` treats each `(left, right)` key pair as the `=` arm treats its operands, and renames the right key to the left key's names. Four conditions:
  1. Skip set-operation joins (`NullEqualsNull`), or unit C's refusal switches off. The arm acts only on `NullEqualsNothing` joins.
  2. Type each key against its own side, and allow for `join_using` storing a pair swapped.
  3. A rewritten `USING` join stops being marked `USING`, and the builder's first cell confirms it.
  4. A `FULL` key-list join whose key names differ refuses with `DATA_DIFF_TYPES`.
- `BETWEEN` and simple `CASE` expand only when a struct pair is present.
- `nullif` renames its second argument.
- The right and full shown-key behaviour stays with [using-side-key-design-1-card-2026-10-08.md](using-side-key-design-1-card-2026-10-08.md) (PR #1000) and out of this unit.

**Also in scope, from #1292's review** (one line each):

- F-2: the `selectExpr` output name leaks the rename UDF.
- F-4: a schema-error text on `l.s = struct(r.s.b, r.s.a)`.
- A-1: the "Error during planning:" prefix.
- F-7: unit C's negative check no longer watches the `Unpaired` arm.
- O-4: no targeted pin on the `DuplicateFieldOperands` registration.

**Gates:** the builder's facade gate runs in both the ci and release profiles. Every new seat gets a `PREPARE ... AS` and `EXECUTE` cell (memo section 7).

**Close:** the Status line of this card and ledger row C-118 are updated on merge, and C-116 closes with them.

**2026-11-11, closed by PR #1295** (squash `68b09d18`). The round 1 verifiers (Opus 5.5 and Muse MAX) returned PASS, and the delta re-check returned PASS from both, with no blocking finding. C-118 and C-116 are closed. The non-blocking findings are carded on 2026-11-11: STRUCT-POSITION-GROUPING-SETS-1, NOT-BETWEEN-DML-CSE-1, STRUCT-COMPARE-FAMILY-RESIDUE-1, MAP-NESTED-ORDERING-ON-1, USING-SCALAR-STRUCT-KEY-CLASS-1, STRUCT-SETOP-CONTROL-CLASS-1, USING-CHAIN-AMBIGUOUS-1, NATURAL-EXISTENCE-JOIN-ACCEPT-1, MAP-ORDERING-FORMS-1, STRUCT-POSITION-ERROR-NAMES-1, STRUCT-CASE-NAME-CAST-1, STRUCT-TYPED-FORM-SCHEMA-1 and FACADE-DUP-FIELD-ROW-1. LISTVIEW-INNER-JOIN-KEY-1 is carded as S1 and ruled to fix by the owner on 2026-11-11.
